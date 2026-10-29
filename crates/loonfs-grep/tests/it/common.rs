//! The composition a host performs to serve grep over an embedded runtime.
//!
//! Nothing in `loonfs` knows about grep, so these tests wire it the way the
//! reference server and the CLI do: runtime handles for the filesystem, one
//! service holding the query-side block cache, and a worker over grep's own
//! keyspace.

use loonfs::{LoonFs, Maintenance, ReadOnly, SharedObjectStore, Writable};
use loonfs_grep::manifest::GrepIndexStatus;
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBlockCache, GrepBuildOutcome, GrepDisableOutcome, GrepEnableOutcome,
    GrepError, GrepPassBudget, GrepService, GrepWorker, NamespaceReads,
    DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
use loonfs_test_support::clock::ManualClock;
use loonfs_types::api::v0::{self, GrepIndex};
use loonfs_types::{ChangeSeq, GrepRequest, GrepResponse, NamespaceId, PageRequest};
use std::sync::Arc;

pub(crate) struct GrepHost {
    pub(crate) store: SharedObjectStore,
    pub(crate) reader: LoonFs<ReadOnly>,
    /// The runtime that creates and deletes the worker's backfill snapshots.
    pub(crate) writer: LoonFs<Writable>,
    pub(crate) maintenance: Maintenance,
    pub(crate) service: GrepService,
    pub(crate) worker: GrepWorker<SharedObjectStore>,
    pub(crate) block_cache: Arc<GrepBlockCache>,
}

impl GrepHost {
    pub(crate) async fn new(store: &SharedObjectStore, actor: &str) -> Self {
        Self::with_reader(store, actor, |builder| builder).await
    }

    /// `configure` adjusts the reader the service and worker read through.
    pub(crate) async fn with_reader(
        store: &SharedObjectStore,
        actor: &str,
        configure: impl FnOnce(loonfs::LoonFsBuilder<ReadOnly>) -> loonfs::LoonFsBuilder<ReadOnly>,
    ) -> Self {
        let reader = configure(LoonFs::builder_with_store(store.clone()).read_only())
            .build()
            .await
            .expect("build reader");
        let writer = LoonFs::builder_with_store(store.clone())
            .writer_id(loonfs_test_support::ids::writer_id(actor))
            .build()
            .await
            .expect("build maintenance");
        let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id(actor));
        let block_cache = default_block_cache();
        Self {
            store: store.clone(),
            reader: reader.clone(),
            maintenance,
            service: GrepService::new(Arc::clone(&block_cache), reader.monotonic_timer()),
            worker: GrepWorker::new(
                store.clone(),
                reader,
                writer.clone(),
                GrepPassBudget::default(),
            ),
            writer,
            block_cache,
        }
    }

    /// Runs a grep request with this host's grep service, object store, and
    /// filesystem reader.
    pub(crate) async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
        page: PageRequest<String>,
    ) -> Result<GrepResponse, GrepError> {
        grep_with(
            &self.service,
            &self.reader,
            &self.store,
            namespace_id,
            request,
            page,
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
    ) -> Result<GrepIndex, GrepError> {
        let lifecycle = match self.worker.enable(namespace_id).await? {
            GrepEnableOutcome::Enabled { state } | GrepEnableOutcome::AlreadyEnabled { state } => {
                state
            }
            GrepEnableOutcome::Superseded => {
                return Err(GrepError::PublicationConflict {
                    object_key: loonfs_grep::keyspace::hint_key(namespace_id),
                })
            }
        };
        let captured_seq = match &lifecycle {
            GrepIndexStatus::Disabled {} => None,
            GrepIndexStatus::Backfilling { captured_seq, .. } => Some(*captured_seq),
            GrepIndexStatus::Active { .. } => Some(
                NamespaceReads::new(self.reader.namespace(namespace_id))
                    .head()
                    .await?
                    .head_seq,
            ),
        };
        if let Some(captured_seq) = captured_seq {
            self.catch_up_grep_index(namespace_id, captured_seq).await?;
        }
        self.worker.get_grep_index(namespace_id).await
    }

    /// Runs bounded build steps until the index has built through
    /// `captured_seq`, or until a step settles short of it.
    pub(crate) async fn catch_up_grep_index(
        &self,
        namespace_id: &NamespaceId,
        captured_seq: ChangeSeq,
    ) -> Result<GrepIndexStatus, GrepError> {
        loop {
            let lifecycle = self.worker.lifecycle(namespace_id).await?;
            if v0::GrepIndexStatus::from(&lifecycle).is_built_through(captured_seq) {
                return Ok(lifecycle);
            }
            match self
                .worker
                .build_step(namespace_id, GramIndexBuildPolicy::default())
                .await?
            {
                GrepBuildOutcome::Published { .. }
                | GrepBuildOutcome::BackfillRestarted { .. }
                | GrepBuildOutcome::Superseded => {}
                // Nothing this loop does next would move the index, so the
                // caller sees where it stopped rather than a spin.
                GrepBuildOutcome::UpToDate { .. } | GrepBuildOutcome::NotEnabled => {
                    return self.worker.lifecycle(namespace_id).await
                }
            }
        }
    }

    pub(crate) async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndex, GrepError> {
        match self.worker.disable(namespace_id).await? {
            GrepDisableOutcome::Disabled | GrepDisableOutcome::NotEnabled => {
                self.worker.get_grep_index(namespace_id).await
            }
            GrepDisableOutcome::Superseded => Err(GrepError::PublicationConflict {
                object_key: loonfs_grep::keyspace::hint_key(namespace_id),
            }),
        }
    }
}

/// Durable control objects these tests assert on directly.
///
/// They read the same bytes the runtime writes, decoded through the
/// envelope codec `loonfs-types` publishes — the durable format is a
/// specified artifact, so a test can check it without reaching into the
/// engine that produced it.
pub(crate) mod control {
    use loonfs::control::NamespaceReadState;
    use loonfs::SharedObjectStore;
    use loonfs_objectstore::keys;
    use loonfs_types::format::control::{decode_control_object, ControlObjectKind, PinPayload};
    use loonfs_types::{NamespaceId, PinId};

    async fn control_bytes(store: &SharedObjectStore, object_key: &str) -> Option<Vec<u8>> {
        store
            .get(object_key, None)
            .await
            .expect("read control object")
            .map(|body| body.to_vec())
    }

    pub(crate) async fn head(
        store: &SharedObjectStore,
        namespace_id: &NamespaceId,
    ) -> NamespaceReadState {
        loonfs_core::test_support::namespace_store(store, namespace_id)
            .load_read_state()
            .await
            .expect("namespace state")
    }

    pub(crate) async fn metadata_manifest(
        store: &SharedObjectStore,
        namespace_id: &NamespaceId,
    ) -> loonfs::control::LoadedManifest {
        loonfs_core::test_support::namespace_store(store.as_ref(), namespace_id)
            .load_current_manifest()
            .await
            .expect("current manifest")
    }

    pub(crate) async fn pin(
        store: &SharedObjectStore,
        namespace_id: &NamespaceId,
        checkpoint_id: &PinId,
    ) -> Option<PinPayload> {
        let bytes = control_bytes(store, &keys::pin(namespace_id, checkpoint_id)).await?;
        Some(
            decode_control_object::<PinPayload>(&bytes, ControlObjectKind::Pin)
                .expect("decode pin")
                .into_payload(),
        )
    }
}

/// Returns a worker over `store` whose writer reads `clock` as its wall
/// clock, so a collection pass measures object ages at the clock's reading.
pub(crate) async fn collector_at(
    store: &SharedObjectStore,
    clock: Arc<ManualClock>,
) -> GrepWorker<SharedObjectStore> {
    let reader = LoonFs::builder_with_store(store.clone())
        .read_only()
        .build()
        .await
        .expect("build the collector's reader");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("grep-collector"))
        .wall_clock(clock)
        .build()
        .await
        .expect("build the collector's writer");
    GrepWorker::new(store.clone(), reader, writer, GrepPassBudget::default())
}

/// Returns an empty grep block cache with the default budget.
pub(crate) fn default_block_cache() -> Arc<GrepBlockCache> {
    Arc::new(GrepBlockCache::new(
        loonfs::engine::DecodedBlockCacheConfig::new(DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES),
    ))
}

/// One composed query over caller-chosen parts, for tests that want a cold
/// service or a reader other than a host's own.
pub(crate) async fn grep_with(
    service: &GrepService,
    reader: &LoonFs<ReadOnly>,
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    request: &GrepRequest,
    page: PageRequest<String>,
) -> Result<GrepResponse, GrepError> {
    let reads = NamespaceReads::new(reader.namespace(namespace_id));
    service.query(request, page, &reads, store).await
}

pub(crate) fn observation() -> loonfs::engine::Observation {
    loonfs::engine::Observation::now(Arc::new(loonfs_types::StdMonotonicTimer::default()))
}

/// Returns the page of at most `matches` matches at `cursor`, or the first
/// such page when `cursor` is `None`.
pub(crate) fn page_request(cursor: Option<String>, matches: u32) -> PageRequest<String> {
    PageRequest {
        limit: loonfs_types::EffectiveLimit::from_requested(Some(matches))
            .expect("test page limits should be within the pagination policy"),
        cursor,
    }
}
