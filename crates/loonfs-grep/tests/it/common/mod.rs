//! The composition a host performs to serve grep over an embedded runtime.
//!
//! Nothing in `loonfs` knows about grep, so these tests wire it the way the
//! reference server and the CLI do: runtime handles for the filesystem, one
//! service holding the query-side block cache, and a worker over grep's own
//! keyspace.

#![allow(dead_code)]

use loonfs::{FsAdmin, FsReader, MaintenanceJob, MaintenanceStepConclusion, SharedObjectStore};
use loonfs_api::v0::{GrepIndexLifecycle, GrepIndexStatusResponse};
use loonfs_api::{ChangeSeq, GrepRequest, GrepResponse, NamespaceId};
use loonfs_grep::root::GrepLifecycle;
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBlockCache, GrepDisableOutcome, GrepEnableOutcome, GrepError,
    GrepMaintenanceJob, GrepService, GrepWorker, NamespaceReads,
    DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
use std::sync::Arc;

pub(crate) struct GrepHost {
    pub(crate) store: SharedObjectStore,
    pub(crate) reader: FsReader,
    pub(crate) admin: FsAdmin,
    pub(crate) service: GrepService,
    pub(crate) worker: GrepWorker<SharedObjectStore>,
    pub(crate) block_cache: Arc<GrepBlockCache>,
}

impl GrepHost {
    pub(crate) async fn new(store: &SharedObjectStore, actor: &str) -> Self {
        let reader = FsReader::builder_with_store(store.clone())
            .build()
            .await
            .expect("build reader");
        let admin = FsAdmin::builder_with_store(store.clone())
            .actor_id(actor)
            .build()
            .await
            .expect("build admin");
        let block_cache = Arc::new(GrepBlockCache::new(DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES));
        Self {
            store: store.clone(),
            reader: reader.clone(),
            admin: admin.clone(),
            service: GrepService::with_block_cache(Arc::clone(&block_cache)),
            worker: GrepWorker::with_block_cache(
                store.clone(),
                reader,
                admin,
                Arc::clone(&block_cache),
            ),
            block_cache,
        }
    }

    /// The query the server's `POST .../query/grep` performs: this host's
    /// service and store for grep's own segments, its reader for the
    /// filesystem.
    pub(crate) async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse, GrepError> {
        grep_with(
            &self.service,
            &self.reader,
            &self.store,
            namespace_id,
            request,
        )
        .await
    }

    /// Enables grep and advances the index to the namespace's current
    /// sequence without a maintenance runner.
    ///
    /// The target is captured before the first step, so concurrent writes do
    /// not keep the test running. Unlike the CLI command, this helper has no
    /// step or time budget and returns the first error.
    pub(crate) async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndexStatusResponse, GrepError> {
        let lifecycle = match self.worker.enable(namespace_id).await? {
            GrepEnableOutcome::Enabled { state } | GrepEnableOutcome::AlreadyEnabled { state } => {
                state
            }
            GrepEnableOutcome::Superseded => {
                return Err(GrepError::PublicationConflict {
                    object_key: loonfs_grep::keyspace::root_key(namespace_id),
                })
            }
        };
        let target_seq = match &lifecycle {
            GrepLifecycle::Disabled => None,
            GrepLifecycle::Backfilling { target_seq, .. } => Some(*target_seq),
            GrepLifecycle::Active { .. } => Some(
                NamespaceReads::new(&self.reader, namespace_id)
                    .head_seq()
                    .await?,
            ),
        };
        if let Some(target_seq) = target_seq {
            self.catch_up_grep_index(namespace_id, target_seq).await?;
        }
        self.grep_index_status(namespace_id).await
    }

    /// Runs the index job's bounded steps until the index has built through
    /// `target_seq`, or until a step settles short of it.
    pub(crate) async fn catch_up_grep_index(
        &self,
        namespace_id: &NamespaceId,
        target_seq: ChangeSeq,
    ) -> Result<GrepLifecycle, GrepError> {
        let job = GrepMaintenanceJob::new(self.worker.clone(), GramIndexBuildPolicy::default());
        loop {
            let lifecycle = self.worker.lifecycle(namespace_id).await?;
            if GrepIndexLifecycle::from(&lifecycle).is_built_through(target_seq) {
                return Ok(lifecycle);
            }
            match job
                .step(namespace_id, None)
                .await
                .map_err(GrepError::Runtime)?
                .conclusion
            {
                MaintenanceStepConclusion::Progressed | MaintenanceStepConclusion::Superseded => {}
                // Nothing this loop does next would move the index, so the
                // caller sees where it stopped rather than a spin.
                _ => return self.worker.lifecycle(namespace_id).await,
            }
        }
    }

    pub(crate) async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndexStatusResponse, GrepError> {
        match self.worker.disable(namespace_id).await? {
            GrepDisableOutcome::Disabled | GrepDisableOutcome::NotEnabled => {
                self.grep_index_status(namespace_id).await
            }
            GrepDisableOutcome::Superseded => Err(GrepError::PublicationConflict {
                object_key: loonfs_grep::keyspace::root_key(namespace_id),
            }),
        }
    }

    pub(crate) async fn grep_index_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndexStatusResponse, GrepError> {
        let root = self.worker.root_state(namespace_id).await?;
        let (lifecycle, next_run_ordinal, reorganize_pending) = match &root {
            Some(root) => (
                GrepIndexLifecycle::from(root.lifecycle()),
                root.index().next_run_ordinal,
                root.index().reorganize.is_some(),
            ),
            None => (GrepIndexLifecycle::Disabled, 0, false),
        };
        Ok(GrepIndexStatusResponse {
            namespace_id: namespace_id.clone(),
            lifecycle,
            next_run_ordinal,
            reorganize_pending,
        })
    }
}

/// Durable control objects these tests assert on directly.
///
/// They read the same bytes the runtime writes, decoded through the
/// envelope codec `loonfs-api` publishes — the durable format is a
/// specified artifact, so a test can check it without reaching into the
/// engine that produced it.
pub(crate) mod control {
    use loonfs::SharedObjectStore;
    use loonfs_api::wire::control::{
        decode_control_object, CheckpointRecordState, ControlObjectKind, HeadState,
        MetadataRootState,
    };
    use loonfs_api::{CheckpointId, NamespaceId};
    use loonfs_objectstore::keys;

    async fn control_bytes(store: &SharedObjectStore, object_key: &str) -> Option<Vec<u8>> {
        store
            .get(object_key, None)
            .await
            .expect("read control object")
            .map(|body| body.to_vec())
    }

    pub(crate) async fn head(store: &SharedObjectStore, namespace_id: &NamespaceId) -> HeadState {
        let bytes = control_bytes(store, &keys::wal_head(namespace_id))
            .await
            .expect("namespace head exists");
        decode_control_object::<HeadState>(&bytes, ControlObjectKind::WalHead)
            .expect("decode namespace head")
            .state
    }

    pub(crate) async fn metadata_root(
        store: &SharedObjectStore,
        namespace_id: &NamespaceId,
    ) -> MetadataRootState {
        let bytes = control_bytes(store, &keys::metadata_root(namespace_id))
            .await
            .expect("metadata root exists");
        decode_control_object::<MetadataRootState>(&bytes, ControlObjectKind::MetadataRoot)
            .expect("decode metadata root")
            .state
    }

    pub(crate) async fn checkpoint_record(
        store: &SharedObjectStore,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Option<CheckpointRecordState> {
        let bytes =
            control_bytes(store, &keys::checkpoint_record(namespace_id, checkpoint_id)).await?;
        Some(
            decode_control_object::<CheckpointRecordState>(
                &bytes,
                ControlObjectKind::CheckpointRecord,
            )
            .expect("decode checkpoint record")
            .state,
        )
    }
}

/// One composed query over caller-chosen parts, for tests that want a cold
/// service or a reader other than a host's own.
pub(crate) async fn grep_with(
    service: &GrepService,
    reader: &FsReader,
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    request: &GrepRequest,
) -> Result<GrepResponse, GrepError> {
    let reads = NamespaceReads::new(reader, namespace_id);
    service.query(request, &reads, store).await
}

/// Classifies a key by durable family instead of by its spelling, so the
/// object-key grammar stays the single place that knows the layout.
pub(crate) fn is_content_object(key: &str) -> bool {
    loonfs_objectstore::layout::parse_object_key(key).is_some_and(|parsed| {
        parsed.family() == loonfs_objectstore::layout::DurableObjectFamily::ContentBlob
    })
}
