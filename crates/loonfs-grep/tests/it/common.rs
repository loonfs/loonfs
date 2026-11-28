//! The composition a host performs to serve grep over an embedded runtime.
//!
//! Nothing in `loonfs` knows about grep, so these tests wire it the way the
//! reference server and the CLI do: runtime handles for the filesystem, one
//! service holding the query-side block cache, and a worker over grep's own
//! segments.

use loonfs::publish::{CommitCandidate, CommitIntent, FilesystemOperation};
use loonfs::test_support::OffsetWallClock;
use loonfs::{
    CommitId, DestinationBehavior, LoonFs, MaintainMetadataOptions, Namespace, ReadOnly,
    SharedObjectStore, Writable,
};
use loonfs_grep::entry::GrepIndexStatus;
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBlockCache, GrepBuildOutcome, GrepDisableOutcome, GrepEnableOutcome,
    GrepError, GrepProgress, GrepReorganizeOutcome, GrepService, GrepStepBudget, GrepSteps,
    GrepWorker, NamespaceReads, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
    DEFAULT_GREP_PUBLISH_INTERVAL_MS,
};
use loonfs_types::GrepIndex;
use loonfs_types::{
    AbsolutePath, CommitSeq, GrepRequest, GrepResponse, GrepScope, NamespaceId, PageRequest,
};
use std::sync::{Arc, Mutex};

/// The writable runtime of each store that a test opened, with its wall
/// clock.
type SharedWriters = Mutex<Vec<(SharedObjectStore, LoonFs<Writable>, Arc<OffsetWallClock>)>>;

static SHARED_WRITERS: SharedWriters = Mutex::new(Vec::new());

/// Returns the one writable runtime of `store`, and its wall clock, building
/// them on first use.
///
/// The helpers that write a namespace and the grep hosts on one store share
/// this runtime, as one process that writes and indexes the store does, so
/// a grep step's publication and a test's commit publish through one writer
/// session of the namespace and never claim the namespace from each other.
pub(crate) async fn shared_writer(
    store: &SharedObjectStore,
) -> (LoonFs<Writable>, Arc<OffsetWallClock>) {
    if let Some((_, writer, clock)) = SHARED_WRITERS
        .lock()
        .expect("the shared writers lock")
        .iter()
        .find(|(shared, _, _)| Arc::ptr_eq(shared, store))
    {
        return (writer.clone(), Arc::clone(clock));
    }
    let clock = Arc::new(OffsetWallClock::default());
    let writer = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("grep-test-writer"))
        .wall_clock(Arc::clone(&clock) as Arc<dyn loonfs::WallClock>)
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("build the store's writer");
    let mut writers = SHARED_WRITERS.lock().expect("the shared writers lock");
    if let Some((_, writer, clock)) = writers
        .iter()
        .find(|(shared, _, _)| Arc::ptr_eq(shared, store))
    {
        return (writer.clone(), Arc::clone(clock));
    }
    writers.push((store.clone(), writer.clone(), Arc::clone(&clock)));
    (writer, clock)
}

/// Makes `writer` the one writable runtime of `store` (see
/// [`shared_writer`]), for a test that builds its writer with settings of its
/// own. Call it before any grep host on `store` is built.
pub(crate) fn share_writer(store: &SharedObjectStore, writer: &LoonFs<Writable>) {
    let mut writers = SHARED_WRITERS.lock().expect("the shared writers lock");
    assert!(
        !writers
            .iter()
            .any(|(shared, _, _)| Arc::ptr_eq(shared, store)),
        "the store's writer is shared before any grep host on the store is built"
    );
    writers.push((
        store.clone(),
        writer.clone(),
        Arc::new(OffsetWallClock::default()),
    ));
}

/// Opens `namespace_id` on the one writable runtime of `store` (see
/// [`shared_writer`]), which a grep host on `store` registered.
pub(crate) fn writer_session(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> Namespace<Writable> {
    let writers = SHARED_WRITERS.lock().expect("the shared writers lock");
    let (_, writer, _) = writers
        .iter()
        .find(|(shared, _, _)| Arc::ptr_eq(shared, store))
        .expect("a grep host on the store registered its writer");
    writer
        .open_namespace(namespace_id)
        .expect("open the namespace's writer session")
}

pub(crate) struct GrepHost {
    pub(crate) store: SharedObjectStore,
    pub(crate) reader: LoonFs<ReadOnly>,
    /// The store's writable runtime (see [`shared_writer`]), which the
    /// worker publishes through.
    pub(crate) writer: LoonFs<Writable>,
    /// The writable runtime again, which the tests run their maintenance
    /// calls through.
    pub(crate) maintenance: LoonFs<Writable>,
    pub(crate) service: GrepService,
    pub(crate) worker: GrepWorker<SharedObjectStore>,
    pub(crate) block_cache: Arc<GrepBlockCache>,
}

impl GrepHost {
    pub(crate) async fn new(store: &SharedObjectStore) -> Self {
        Self::with_reader(store, |builder| builder).await
    }

    /// `configure` adjusts the reader the service and worker read through.
    ///
    /// The reader checks for a newer manifest on every read, so a query
    /// reads the grep entry that a step just published. (A reader with the
    /// default interval reads it within a second.)
    pub(crate) async fn with_reader(
        store: &SharedObjectStore,
        configure: impl FnOnce(loonfs::LoonFsBuilder<ReadOnly>) -> loonfs::LoonFsBuilder<ReadOnly>,
    ) -> Self {
        let reader = configure(
            LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
                .read_only()
                .config(loonfs::RuntimeConfig {
                    manifest_revalidation_interval_ms: 0,
                    ..loonfs::RuntimeConfig::default()
                }),
        )
        .build()
        .await
        .expect("build reader");
        let (writer, _) = shared_writer(store).await;
        let maintenance = writer.clone();
        let block_cache = default_block_cache();
        Self {
            store: store.clone(),
            reader: reader.clone(),
            maintenance: maintenance.clone(),
            service: GrepService::new(Arc::clone(&block_cache)),
            worker: GrepWorker::new(store.clone(), reader, GrepStepBudget::default()),
            writer,
            block_cache,
        }
    }

    /// Returns a host over the same store and runtimes whose worker draws
    /// its steps from `budget`.
    pub(crate) fn with_budget(&self, budget: &GrepStepBudget) -> Self {
        let block_cache = default_block_cache();
        Self {
            store: self.store.clone(),
            reader: self.reader.clone(),
            writer: self.writer.clone(),
            maintenance: self.maintenance.clone(),
            service: GrepService::new(Arc::clone(&block_cache)),
            worker: GrepWorker::new(self.store.clone(), self.reader.clone(), budget.clone()),
            block_cache,
        }
    }

    /// Returns the worker's steps for `namespace_id`, which publish through
    /// the writer session of the store's one writable runtime (see
    /// [`writer_session`]), so the worker publishes as the namespace's
    /// writer.
    pub(crate) fn steps_of(
        &self,
        namespace_id: &NamespaceId,
        publish_interval_ms: u64,
    ) -> GrepSteps<'_, SharedObjectStore> {
        self.worker.steps(
            &writer_session(&self.store, namespace_id),
            publish_interval_ms,
        )
    }

    /// Enables grep on `namespace_id` through the namespace's writer
    /// session, as [`Self::steps_of`] publishes.
    pub(crate) async fn enable_of(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs_grep::Result<GrepEnableOutcome> {
        self.worker
            .enable(&writer_session(&self.store, namespace_id))
            .await
    }

    /// Disables grep on `namespace_id` through the namespace's writer
    /// session, as [`Self::steps_of`] publishes.
    pub(crate) async fn disable_of(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs_grep::Result<GrepDisableOutcome> {
        self.worker
            .disable(&writer_session(&self.store, namespace_id))
            .await
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
        let lifecycle = match self.enable_of(namespace_id).await? {
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
        captured_seq: CommitSeq,
    ) -> Result<GrepIndexStatus, GrepError> {
        let mut steps = self.steps_of(namespace_id, DEFAULT_GREP_PUBLISH_INTERVAL_MS);
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
        match self.disable_of(namespace_id).await? {
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
/// envelope codec `loonfs-types` publishes. The durable format is a
/// specified artifact, so a test can check it without reaching into the
/// engine that produced it.
pub(crate) mod control {
    use loonfs::control::NamespaceReadState;
    use loonfs::SharedObjectStore;
    use loonfs_types::NamespaceId;

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
}

/// Publishes `entry` as grep's entry in the namespace's next manifest,
/// whatever grep would make of it.
pub(crate) async fn publish_grep_entry(
    maintenance: &LoonFs<Writable>,
    namespace_id: &NamespaceId,
    entry: loonfs_types::format::manifest::ExtensionEntry,
) {
    let basis = maintenance
        .open_namespace(namespace_id)
        .expect("open namespace")
        .load_extension_basis()
        .await
        .expect("load the basis");
    let published = maintenance
        .open_namespace(namespace_id)
        .expect("open namespace")
        .publish_extension(&basis, &loonfs_grep::entry::grep_extension_name(), entry)
        .await
        .expect("publish grep's entry");
    assert!(
        matches!(
            published,
            loonfs::engine::ExtensionPublication::Published { .. }
        ),
        "no other publication races a test's own"
    );
}

/// Builds a writer over `store` that publishes each commit at once, creates
/// `namespace_id`, and opens the namespace. The writer ID is the namespace ID
/// followed by `-writer`.
pub(crate) async fn writable_namespace(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> (LoonFs<Writable>, Namespace<Writable>) {
    let (writer, _) = shared_writer(store).await;
    writer
        .create_namespace(namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    (writer, namespace)
}

pub(crate) fn request(pattern: &str) -> GrepRequest {
    GrepRequest {
        pattern: pattern.to_owned(),
        case_insensitive: false,
        scope: GrepScope::Namespace,
        allow_stale: false,
        allow_scan: false,
    }
}

pub(crate) async fn drive_worker_step(
    host: &GrepHost,
    namespace_id: &NamespaceId,
    policy: GramIndexBuildPolicy,
) {
    host.steps_of(namespace_id, 0)
        .build_step(policy)
        .await
        .expect("grep build step");
    host.steps_of(namespace_id, 0)
        .reorganize_step(policy)
        .await
        .expect("grep reorganize step");
}

pub(crate) async fn drive_worker_to_current(
    host: &GrepHost,
    namespace_id: &NamespaceId,
    policy: GramIndexBuildPolicy,
) {
    for _ in 0..512 {
        let build = host
            .steps_of(namespace_id, 0)
            .build_step(policy)
            .await
            .expect("grep build step");
        let reorganize = host
            .steps_of(namespace_id, 0)
            .reorganize_step(policy)
            .await
            .expect("grep reorganize step");
        if matches!(build, GrepBuildOutcome::UpToDate { .. })
            && matches!(reorganize, GrepReorganizeOutcome::NotNeeded { .. })
        {
            return;
        }
    }
    panic!("grep worker backlog must drain");
}

/// Returns an empty grep block cache with the default budget.
pub(crate) fn default_block_cache() -> Arc<GrepBlockCache> {
    Arc::new(GrepBlockCache::new(
        loonfs::engine::DecodedBlockCacheConfig::new(DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES),
    ))
}

/// One composed query over caller-chosen parts, for tests that want a cold
/// service or a reader other than a host's own. A query follows the worker's
/// publication of the index, which doesn't move the runtime's cached head, so
/// it reads `strong`.
pub(crate) async fn grep_with(
    service: &GrepService,
    reader: &LoonFs<ReadOnly>,
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    request: &GrepRequest,
    page: PageRequest<String>,
) -> Result<GrepResponse, GrepError> {
    let reads = NamespaceReads::new(reader.clone(), namespace_id).strong();
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

/// Folds the tail, then advances the floor to the head with the wall clock
/// of the store's writer one default grace window ahead, so the advance can
/// pass every commit made before the call. Both run as the namespace's
/// writer session in the store's writable runtime (see [`shared_writer`]),
/// so a test can keep writing through that session after the advance.
pub(crate) async fn fold_wal_and_advance_retention(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> CommitSeq {
    let (writer, clock) = shared_writer(store).await;
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    namespace
        .maintain_metadata_with_options(&MaintainMetadataOptions {
            max_wal_tail_objects: std::num::NonZeroU64::MIN,
        })
        .await
        .expect("fold wal");
    clock
        .past_the_grace_window(namespace.advance_retention_floor())
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
