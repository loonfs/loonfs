//! The composition a host performs to serve grep over an embedded runtime.
//!
//! Nothing in `loonfs` knows about grep, so these tests wire it the way the
//! reference server and the CLI do: runtime handles for the filesystem, one
//! service holding the query-side block cache, and a worker over grep's own
//! keyspace.

#![allow(dead_code)]

use loonfs::{FsAdmin, FsReader, SharedObjectStore};
use loonfs_api::v0::{DisableGrepIndexResponse, EnableGrepIndexResponse};
use loonfs_api::{GrepRequest, GrepResponse, NamespaceId};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepDisableOutcome, GrepEnableOutcome, GrepError, GrepIndexSnapshot,
    GrepService, GrepWorker, NamespaceReads,
};

pub(crate) struct GrepHost {
    pub(crate) store: SharedObjectStore,
    pub(crate) reader: FsReader,
    pub(crate) admin: FsAdmin,
    pub(crate) service: GrepService,
    pub(crate) worker: GrepWorker<SharedObjectStore>,
}

impl GrepHost {
    pub(crate) async fn new(store: &SharedObjectStore, actor: &str, version: &str) -> Self {
        let reader = FsReader::builder_with_store(store.clone())
            .build()
            .await
            .expect("build reader");
        let admin = FsAdmin::builder_with_store(store.clone())
            .actor_id(actor)
            .actor_version(version)
            .build()
            .await
            .expect("build admin");
        Self {
            store: store.clone(),
            reader: reader.clone(),
            admin: admin.clone(),
            service: GrepService::new(),
            worker: GrepWorker::new(store.clone(), reader, admin, version),
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

    /// Enables grep and drives the backfill to quiescence, the way a host
    /// with no driver runtime does.
    pub(crate) async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGrepIndexResponse, GrepError> {
        let already_enabled = match self.worker.enable(namespace_id).await? {
            GrepEnableOutcome::Enabled { .. } => false,
            GrepEnableOutcome::AlreadyEnabled { .. } => true,
            GrepEnableOutcome::Superseded => {
                return Err(GrepError::PublicationConflict {
                    object_key: loonfs_grep::keyspace::root_key(namespace_id),
                })
            }
        };
        let built_through_seq = self
            .worker
            .run_to_quiescence(namespace_id, GramIndexBuildPolicy::default())
            .await?;
        Ok(EnableGrepIndexResponse {
            namespace_id: namespace_id.clone(),
            built_through_seq,
            already_enabled,
        })
    }

    pub(crate) async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGrepIndexResponse, GrepError> {
        match self.worker.disable(namespace_id).await? {
            GrepDisableOutcome::Disabled => Ok(DisableGrepIndexResponse {
                namespace_id: namespace_id.clone(),
                was_enabled: true,
            }),
            GrepDisableOutcome::NotEnabled => Ok(DisableGrepIndexResponse {
                namespace_id: namespace_id.clone(),
                was_enabled: false,
            }),
            GrepDisableOutcome::Superseded => Err(GrepError::PublicationConflict {
                object_key: loonfs_grep::keyspace::root_key(namespace_id),
            }),
        }
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
        let bytes = control_bytes(store, &keys::wal_head(namespace_id.as_str()))
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
        let bytes = control_bytes(store, &keys::metadata_root(namespace_id.as_str()))
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
        let bytes = control_bytes(
            store,
            &keys::checkpoint_record(namespace_id.as_str(), checkpoint_id.as_str()),
        )
        .await?;
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
    let snapshot = GrepIndexSnapshot::from_grep_root(&**store, namespace_id, service).await;
    service.query(request, &snapshot, &reads, store).await
}
