//! The composition a host performs to serve grep over an embedded runtime.
//!
//! Nothing in `loonfs` knows about grep, so these tests wire it the way the
//! reference server and the CLI do: runtime handles for the filesystem, one
//! service holding the query-side block cache, and a worker over grep's own
//! segments.

use loonfs::publish::{CommitCandidate, CommitIntent, FilesystemOperation};
use loonfs::{
    CommitId, DestinationBehavior, LoonFs, MaintainMetadataOptions, Maintenance, ReadOnly,
    SharedObjectStore, Writable,
};
use loonfs_grep::entry::GrepIndexStatus;
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBlockCache, GrepBuildOutcome, GrepDisableOutcome, GrepEnableOutcome,
    GrepError, GrepProgress, GrepReorganizeOutcome, GrepService, GrepStepBudget, GrepWorker,
    NamespaceReads, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES, DEFAULT_GREP_PUBLISH_INTERVAL_MS,
};
use loonfs_types::GrepIndex;
use loonfs_types::{AbsolutePath, ChangeSeq, GrepRequest, GrepResponse, NamespaceId, PageRequest};
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
    ///
    /// The reader checks for a newer manifest on every read, so a query
    /// reads the grep entry that a step just published. (A reader with the
    /// default interval reads it within a second.)
    pub(crate) async fn with_reader(
        store: &SharedObjectStore,
        actor: &str,
        configure: impl FnOnce(loonfs::LoonFsBuilder<ReadOnly>) -> loonfs::LoonFsBuilder<ReadOnly>,
    ) -> Self {
        let reader = configure(
            LoonFs::builder_with_store(store.clone())
                .read_only()
                .config(loonfs::RuntimeConfig {
                    manifest_revalidation_interval_ms: 0,
                    ..loonfs::RuntimeConfig::default()
                }),
        )
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
            maintenance: maintenance.clone(),
            service: GrepService::new(Arc::clone(&block_cache)),
            worker: GrepWorker::new(
                store.clone(),
                reader,
                writer.clone(),
                maintenance.clone(),
                GrepStepBudget::default(),
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
                panic!("enabling grep lost its publication, which a test must arrange on purpose")
            }
        };
        let captured_seq = match &lifecycle {
            GrepIndexStatus::Disabled {} => None,
            GrepIndexStatus::Backfilling { captured_seq, .. } => Some(*captured_seq),
            GrepIndexStatus::Active { .. } => Some(
                NamespaceReads::new(self.reader.clone(), namespace_id)
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

    /// Runs steps the way the CLI's wait does, until the index has built
    /// through `captured_seq` or a step settles short of it, and then
    /// publishes the run's pending change.
    pub(crate) async fn catch_up_grep_index(
        &self,
        namespace_id: &NamespaceId,
        captured_seq: ChangeSeq,
    ) -> Result<GrepIndexStatus, GrepError> {
        let mut steps = self
            .worker
            .steps(namespace_id, DEFAULT_GREP_PUBLISH_INTERVAL_MS);
        loop {
            let status = match steps.pending_status() {
                Some(status) => status.clone(),
                None => self.worker.lifecycle(namespace_id).await?,
            };
            if loonfs_types::GrepIndexStatus::from(&status).is_built_through(captured_seq) {
                break;
            }
            match steps.step(GramIndexBuildPolicy::default()).await? {
                GrepProgress::Built(
                    GrepBuildOutcome::Indexed { .. }
                    | GrepBuildOutcome::BackfillRestarted { .. }
                    | GrepBuildOutcome::Superseded,
                )
                | GrepProgress::Reorganized(
                    GrepReorganizeOutcome::UnitWritten { .. }
                    | GrepReorganizeOutcome::Abandoned
                    | GrepReorganizeOutcome::Superseded,
                ) => {}
                // Nothing this loop does next would move the index, so the
                // caller sees where it stopped rather than a spin.
                GrepProgress::Built(
                    GrepBuildOutcome::UpToDate { .. } | GrepBuildOutcome::NotEnabled,
                )
                | GrepProgress::Reorganized(
                    GrepReorganizeOutcome::NotNeeded { .. } | GrepReorganizeOutcome::NotEnabled,
                ) => break,
            }
        }
        steps.publish().await?;
        self.worker.lifecycle(namespace_id).await
    }

    pub(crate) async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndex, GrepError> {
        match self.worker.disable(namespace_id).await? {
            GrepDisableOutcome::Disabled | GrepDisableOutcome::NotEnabled => {
                self.worker.get_grep_index(namespace_id).await
            }
            GrepDisableOutcome::Superseded => {
                panic!("disabling grep lost its publication, which a test must arrange on purpose")
            }
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

    /// Returns grep's entry in the namespace's current manifest, or `None`
    /// where grep was never enabled.
    pub(crate) async fn grep_entry(
        store: &SharedObjectStore,
        namespace_id: &NamespaceId,
    ) -> Option<loonfs_grep::entry::GrepEntry> {
        metadata_manifest(store, namespace_id)
            .await
            .payload()
            .extensions
            .get(&loonfs_grep::entry::grep_extension_name())
            .map(|entry| loonfs_grep::entry::GrepEntry::decode(entry).expect("decode grep's entry"))
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

/// Publishes `entry` as grep's entry in the namespace's next manifest, with
/// `next_run_no` as the next run number, whatever grep would make of it.
pub(crate) async fn publish_grep_entry(
    maintenance: &Maintenance,
    namespace_id: &NamespaceId,
    entry: loonfs_types::format::manifest::ExtensionEntry,
    next_run_no: loonfs_types::RunNo,
) {
    let basis = maintenance
        .load_extension_basis(namespace_id)
        .await
        .expect("load the basis");
    let published = maintenance
        .publish_extension(&basis, &loonfs_grep::entry::grep_extension_name(), |_| {
            (entry.clone(), next_run_no)
        })
        .await
        .expect("publish grep's entry");
    assert!(
        matches!(
            published,
            loonfs::engine::ExtensionPublication::Published(_)
        ),
        "no other publication races a test's own"
    );
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
    let reads = NamespaceReads::new(reader.clone(), namespace_id);
    service.query(request, page, &reads, store).await
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

/// Folds the tail through `maintenance`, then advances the floor to the head
/// through a runtime whose wall clock is one default grace window ahead, so
/// the advance can pass every commit made before the call.
///
/// That runtime is separate from the test's others and has no writer session
/// for the namespace, so the advance creates a floor object at the WAL tip,
/// folds it, and claims no writer epoch. A writer session for the namespace
/// in another runtime isn't fenced. Its next commit collides with the floor
/// object and plans again against the new floor, so a test can keep writing
/// through that session after the advance.
pub(crate) async fn fold_wal_and_advance_retention(
    store: &SharedObjectStore,
    maintenance: &Maintenance,
    namespace_id: &NamespaceId,
) -> ChangeSeq {
    maintenance
        .maintain_metadata_with_options(
            namespace_id,
            &MaintainMetadataOptions {
                max_wal_tail_objects: std::num::NonZeroU64::MIN,
            },
        )
        .await
        .expect("fold wal");
    loonfs::test_support::maintenance_past_the_grace_window(store.clone())
        .await
        .advance_retention_floor(namespace_id)
        .await
        .expect("advance retention")
        .retention_floor_seq
}

/// Returns `paths` in the order a grep walk reads their files: by the
/// content value of each file's current revision, which is its content ID
/// and size, and then by inode ID.
///
/// Content IDs are random, so a test that needs one file before another
/// learns the order after it writes the files.
pub(crate) async fn in_walk_order<const N: usize>(
    namespace: &loonfs::Namespace<loonfs::Writable>,
    paths: [&str; N],
) -> [String; N] {
    let mut files = Vec::with_capacity(N);
    for path in paths {
        let entry = namespace.stat(path).await.expect("stat the file");
        let content_ref = entry.content_ref().expect("a file has content");
        files.push((
            (
                content_ref.content_id.clone(),
                content_ref.size_bytes,
                entry.inode_id,
            ),
            path.to_owned(),
        ));
    }
    files.sort();
    files
        .into_iter()
        .map(|(_, path)| path)
        .collect::<Vec<_>>()
        .try_into()
        .expect("one path for each file")
}

/// Publishes `count` files under `/{prefix}-{index}.txt` that refer to one
/// upload of `content`, in one publication.
pub(crate) async fn publish_same_content_files(
    namespace: &loonfs::Namespace<Writable>,
    prefix: &str,
    count: usize,
    content: &[u8],
) {
    let prepared = namespace
        .prepare_content(content)
        .await
        .expect("prepare shared content");
    let content_ref = prepared.content_ref().clone();
    let candidates = (0..count)
        .map(|index| {
            CommitCandidate::prepared(
                CommitIntent::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(format!("/{prefix}-{index:04}.txt"))
                            .expect("batch path"),
                        content: loonfs_types::FileContent::Ref(content_ref.clone()),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
                vec![prepared.clone()],
            )
        })
        .collect::<Vec<_>>();
    // Every candidate is admitted before the publisher's worker can take
    // any of them, so they coalesce into one publication.
    let submissions = candidates
        .into_iter()
        .map(|candidate| namespace.commit_candidate(candidate));
    for result in futures::future::join_all(submissions).await {
        result.expect("publish batch file");
    }
}
