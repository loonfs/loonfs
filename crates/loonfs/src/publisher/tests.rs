//! Publisher batching, shutdown, retry, and observability tests.

#![allow(clippy::panic)]
// Publisher tests use panic in async result helpers for precise diagnostics.

#[path = "publication_clock.rs"]
mod publication_clock;

use super::*;
use crate::config::ReadConfig;
use crate::content_tokens::ContentTokenError;
use crate::fs::WriterIdentity;
use crate::metrics::{DefaultMetricsRecorder, MetricValue, RuntimeInstruments};
use crate::publish::{CommitRequest, ContentPreparationError, FilesystemOperation};
use crate::{
    ErrorCode, MetadataCache, SharedObjectStore as SharedStore, TraceMode, TraceStoreKind,
};
use async_trait::async_trait;
use bytes::Bytes;
use loonfs_core::test_support::append_wal_objects;
use loonfs_core::MutationContext;
use loonfs_objectstore::keys::{metadata_manifest_prefix, wal_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use loonfs_test_support::ids::first_page;
use loonfs_test_support::stores::{
    delegate_object_store, BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
    OperationContext, OperationKind, RecordingStore,
};
use loonfs_types::format::wal::decode_wal_object_envelope_zstd;
use loonfs_types::{AbsolutePath, ActorId, ChangeSeq, DestinationBehavior};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Condvar;
use tempfile::tempdir;
use tokio::time::timeout;

fn is_publication(bytes: &[u8]) -> bool {
    decode_wal_object_envelope_zstd(bytes).is_ok_and(|wal| !wal.payload().records.is_empty())
        || loonfs_types::format::manifest::decode_namespace_manifest_json(bytes)
            .is_ok_and(|manifest| manifest.payload().status.is_deleted())
}

fn is_fold(operation: &OperationContext<'_>) -> bool {
    match operation.kind() {
        OperationKind::Put {
            bytes,
            mode: PutMode::CreateIfAbsent,
        } => loonfs_types::format::manifest::decode_namespace_manifest_json(bytes).is_ok_and(
            |manifest| {
                manifest.payload().folded_wal_no.0 > 0 && !manifest.payload().status.is_deleted()
            },
        ),
        _ => false,
    }
}

fn blocking_fold_store<S>(inner: S, prefix: String) -> BlockingStore<S> {
    BlockingStore::matching(inner, move |operation| {
        operation.key().starts_with(&prefix) && is_fold(operation)
    })
}

fn blocking_publication_store(
    root: impl AsRef<Path>,
    namespace_id: &NamespaceId,
) -> BlockingStore<LocalFsStore> {
    let prefix = loonfs_objectstore::keys::namespace_prefix(namespace_id);
    BlockingStore::matching(
        LocalFsStore::new(root.as_ref()).expect("store"),
        move |operation| {
            operation.key().starts_with(&prefix)
                && matches!(operation.kind(), OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent } if is_publication(bytes))
        },
    )
}

#[derive(Debug)]
struct PanicWalPutStore {
    inner: LocalFsStore,
    namespace_prefix: String,
    gate: Arc<PanicGate>,
}

#[derive(Debug)]
struct PanicGate {
    state: Mutex<PanicGateState>,
    cvar: Condvar,
}

#[derive(Debug)]
struct PanicGateState {
    armed: bool,
    entered: bool,
    released: bool,
}

impl PanicWalPutStore {
    fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            namespace_prefix: loonfs_objectstore::keys::namespace_prefix(namespace_id),
            gate: Arc::new(PanicGate {
                state: Mutex::new(PanicGateState {
                    armed: false,
                    entered: false,
                    released: false,
                }),
                cvar: Condvar::new(),
            }),
        }
    }

    fn arm_blocking_panic(&self) {
        let mut state = self.gate.lock_state();
        state.armed = true;
        state.entered = false;
        state.released = false;
    }

    async fn wait_until_blocked(&self) {
        let gate = self.gate.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = gate.lock_state();
            while !state.entered {
                state = gate
                    .cvar
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        })
        .await
        .expect("wait for blocked WAL put");
    }

    fn release_into_panic(&self) {
        let mut state = self.gate.lock_state();
        state.released = true;
        self.gate.cvar.notify_all();
    }
}

impl PanicGate {
    // The injected panic poisons this mutex by design; later store calls
    // must keep working, so recover instead of unwrapping.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, PanicGateState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl ObjectStore for PanicWalPutStore {
    delegate_object_store!(self => self.inner; except put);

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key.starts_with(&self.namespace_prefix)
            && matches!(mode, PutMode::CreateIfAbsent)
            && is_publication(&bytes)
        {
            let gate = self.gate.clone();
            tokio::task::spawn_blocking(move || {
                let mut state = gate.lock_state();
                if state.armed {
                    state.armed = false;
                    state.entered = true;
                    gate.cvar.notify_all();
                    while !state.released {
                        state = gate
                            .cvar
                            .wait(state)
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                    }
                    panic!("injected publish task panic");
                }
            })
            .await
            .expect("WAL put gate task");
        }
        self.inner.put(key, bytes, mode).await
    }
}

fn lost_wal_put_ack_store(
    root: impl AsRef<Path>,
    namespace_id: &NamespaceId,
) -> FailStore<LocalFsStore> {
    let prefix = wal_prefix(namespace_id);
    FailStore::matching(LocalFsStore::new(root.as_ref()).expect("store"), move |operation| {
        operation.key().starts_with(&prefix) && matches!(operation.kind(), OperationKind::Put { bytes, mode: PutMode::CreateIfAbsent } if is_publication(bytes))
    }, InjectedError::Transport("lost WAL put acknowledgement".to_owned())).apply_then_fail()
}

fn test_runtime_core(store: SharedStore) -> RuntimeCore {
    RuntimeCore::open(
        store,
        ReadConfig {
            max_read_content_bytes: None,
            manifest_revalidation_interval_ms: 1000,
            metadata_lsm_policy: loonfs_core::MetadataLsmPolicy::default(),
            trace_mode: TraceMode::Remote,
            trace_store_kind: TraceStoreKind::LocalFs,
        },
        MetadataCache::default(),
        None,
        RuntimeInstruments::new(None),
        Arc::new(loonfs_types::StdMonotonicTimer::default()),
        Arc::new(loonfs_core::time::SystemWallClock),
    )
}

fn test_writer_bits() -> Arc<WriterBits> {
    Arc::new(WriterBits {
        inline_content: crate::InlineContentPolicy::default(),
        identity: WriterIdentity::new("writer-a".to_owned()).expect("valid writer identity"),
        wal_fold_permits: tokio::sync::Semaphore::new(crate::config::DEFAULT_MAX_CONCURRENT_FOLDS),
        wal_folds_waiting: AtomicUsize::new(0),
        compaction_permits: tokio::sync::Semaphore::new(
            crate::config::DEFAULT_MAX_CONCURRENT_COMPACTIONS,
        ),
        compactor_epochs: tokio::sync::Mutex::default(),
    })
}

/// A runtime core plus the writer bits a standalone publisher publishes
/// under. The caller keeps both alive; the publisher holds the bits weakly.
struct TestRuntime {
    core: RuntimeCore,
    bits: Arc<WriterBits>,
}

fn test_runtime(store: SharedStore) -> TestRuntime {
    TestRuntime {
        core: test_runtime_core(store),
        bits: test_writer_bits(),
    }
}

async fn test_writer(store: SharedStore) -> crate::LoonFs<crate::Writable> {
    test_writer_with_interval(store, crate::config::DEFAULT_MIN_PUBLISH_INTERVAL_MS).await
}

async fn test_writer_with_interval(
    store: SharedStore,
    min_publish_interval_ms: u64,
) -> crate::LoonFs<crate::Writable> {
    crate::LoonFs::builder_with_store(store)
        .writer_id("writer-a")
        .min_publish_interval_ms(min_publish_interval_ms)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(TraceStoreKind::LocalFs)
        .build()
        .await
        .expect("build writer")
}

async fn test_writer_with_cache(
    store: SharedStore,
    metadata_cache: MetadataCache,
    recorder: Arc<DefaultMetricsRecorder>,
) -> crate::LoonFs<crate::Writable> {
    crate::LoonFs::builder_with_store(store)
        .writer_id("writer-a")
        .min_publish_interval_ms(0)
        .metadata_cache(metadata_cache)
        .metrics_recorder(recorder)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(TraceStoreKind::LocalFs)
        .build()
        .await
        .expect("build writer")
}

fn counter(recorder: &DefaultMetricsRecorder, name: &str) -> u64 {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name(name)
        .next()
        .unwrap_or_else(|| panic!("no `{name}` counter registered"));
    match entry.value {
        MetricValue::Counter(value) => value,
        ref other => panic!("expected a counter, found {other:?}"),
    }
}

/// Bootstraps `namespaces` under `writer` and publishes one directory into
/// each, in order, so the cache's recency order is the namespace order.
/// The sessions stay in the table while the caller holds the handles.
async fn publish_once_into_each(
    writer: &crate::LoonFs<crate::Writable>,
    namespaces: &[NamespaceId],
) -> Vec<crate::Namespace<crate::Writable>> {
    let mut namespace_writers = Vec::new();
    for namespace_id in namespaces {
        writer
            .create_namespace(namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("bootstrap");
        let namespace = writer.open_namespace(namespace_id).expect("open namespace");
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                "seed", "docs",
            )))
            .await
            .expect("commit");
        namespace_writers.push(namespace);
    }
    namespace_writers
}

fn test_namespaces(count: usize) -> Vec<NamespaceId> {
    (0..count)
        .map(|index| NamespaceId::parse(format!("ns-{index:02}")).expect("valid namespace id"))
        .collect()
}

/// Bootstraps a namespace under the identity the standalone publisher will
/// publish with, so its first publication continues the writer session the
/// bootstrap left behind — the same continuity a writer handle has.
async fn create_namespace(runtime: &TestRuntime, namespace_id: &NamespaceId) {
    runtime
        .core
        .writer_engine(&runtime.bits.identity, namespace_id)
        .bootstrap_namespace(
            &loonfs_test_support::test_actor(),
            &loonfs_core::CreateNamespaceOptions {
                access: loonfs_types::NamespaceAccess::Unrestricted {},
                allow_existing: false,
            },
        )
        .await
        .expect("bootstrap");
}

/// Pacing for standalone test publishers, long enough that
/// `wait_past_publish_pacing` outlasting it is meaningful.
const TEST_STANDALONE_PACING: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
struct ManualMonotonicTimer(AtomicU64);

impl ManualMonotonicTimer {
    fn set(&self, now_ms: u64) {
        self.0.store(now_ms, AtomicOrdering::Release);
    }
}

impl loonfs_objectstore::timing::MonotonicTimer for ManualMonotonicTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.0.load(AtomicOrdering::Acquire)
    }
}

fn publisher_state(
    publisher: &NamespacePublisher,
) -> std::sync::MutexGuard<'_, NamespacePublisherState> {
    publisher
        .state
        .lock()
        .expect("namespace publisher mutex should not be poisoned")
}

/// True while exactly one worker owns the publisher's queue.
fn single_live_worker(publisher: &NamespacePublisher) -> bool {
    publisher_state(publisher)
        .worker
        .as_ref()
        .is_some_and(|worker| !*worker.liveness.borrow())
}

/// Yields until the publisher's queue holds at least `expected` candidates
/// the worker has not taken yet.
async fn wait_for_queued_candidates(publisher: &NamespacePublisher, expected: usize) {
    while queued_candidates(&publisher_state(publisher)) < expected {
        tokio::task::yield_now().await;
    }
}

async fn wait_for_fold_waiters(writer: &crate::LoonFs<crate::Writable>, expected: usize) {
    timeout(Duration::from_secs(10), async {
        while writer.mode.bits.wal_folds_waiting.load(Ordering::SeqCst) != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fold waiters should reach the expected count");
}

/// Yields until all expected callers are waiting on one in-flight commit ID.
async fn wait_for_commit_waiters(
    publisher: &NamespacePublisher,
    commit_id: &CommitId,
    expected: usize,
) {
    loop {
        let ready = publisher_state(publisher)
            .in_flight
            .get(commit_id)
            .is_some_and(|request| request.waiters.len() >= expected);
        if ready {
            return;
        }
        tokio::task::yield_now().await;
    }
}

/// Yields until a delete sits at the tail of the publisher's queue.
async fn wait_for_queued_delete(publisher: &NamespacePublisher) {
    loop {
        if matches!(
            publisher_state(publisher).queue.back(),
            Some(WorkItem::Delete(_))
        ) {
            return;
        }
        tokio::task::yield_now().await;
    }
}

/// Queued delete items and the waiters they hold, worker-untaken.
fn queued_delete_shape(state: &NamespacePublisherState) -> (usize, usize) {
    state
        .queue
        .iter()
        .fold((0, 0), |(items, waiters), item| match item {
            WorkItem::Delete(pending) => (items + 1, waiters + pending.waiters.len()),
            _ => (items, waiters),
        })
}

/// Yields until the queued deletes hold at least `expected` waiters.
async fn wait_for_queued_delete_waiters(publisher: &NamespacePublisher, expected: usize) {
    while queued_delete_shape(&publisher_state(publisher)).1 < expected {
        tokio::task::yield_now().await;
    }
}

fn spawn_delete(
    publisher: &NamespacePublisher,
    options: DeleteNamespaceOptions,
) -> tokio::task::JoinHandle<DeleteResult> {
    let publisher = publisher.clone();
    tokio::spawn(async move { publisher.submit_delete(options).await })
}

/// Bounded: a stranded delete waiter is a hang, and a hang must fail
/// the test.
async fn settle_delete(handle: tokio::task::JoinHandle<DeleteResult>, label: &str) -> DeleteResult {
    timeout(Duration::from_secs(10), handle)
        .await
        .unwrap_or_else(|_| panic!("{label} must settle, not hang"))
        .unwrap_or_else(|err| panic!("{label} task failed: {err}"))
}

/// A publisher with no owning registry, exercising the unowned-task
/// fallback the production paths reserve for a dropped registry. The
/// caller keeps `runtime` alive; the publisher holds its bits weakly.
fn standalone_publisher(namespace_id: &NamespaceId, runtime: &TestRuntime) -> NamespacePublisher {
    let registry = PublisherRegistry::new(
        runtime.core.clone(),
        Arc::downgrade(&runtime.bits),
        tokio::runtime::Handle::current(),
        TEST_STANDALONE_PACING,
        crate::PublicationLimits::default(),
    );
    NamespacePublisher::new(namespace_id.clone(), &registry)
}

#[allow(clippy::disallowed_methods)]
async fn wait_past_publish_pacing() {
    // Deliberate wall-clock wait past the per-namespace CAS pacing
    // interval. A work loop that were not single-flight would let a
    // racing second task release a queued delete after exactly that
    // interval, so outlasting it proves the delete is ordered behind
    // the sealed batch, not merely paced behind it.
    tokio::time::sleep(TEST_STANDALONE_PACING + Duration::from_millis(300)).await;
}

/// One directory creation directly under the root, named by the directory
/// the test wants: the cheapest mutation that is distinct per name.
pub(super) fn create_directory_request(
    commit_id: impl Into<String>,
    directory_name: impl AsRef<str>,
) -> CommitRequest {
    CommitRequest::single(
        CommitId::parse(commit_id.into()).expect("valid commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(format!("/{}", directory_name.as_ref()))
                .expect("valid absolute path"),
            parents: false,
        },
    )
}

fn admit_commit(
    publisher: &NamespacePublisher,
    namespace_id: &NamespaceId,
    request: CommitRequest,
) -> oneshot::Receiver<CommitResult> {
    try_admit_commit(publisher, namespace_id, request).expect("admit mutation")
}

fn try_admit_commit(
    publisher: &NamespacePublisher,
    namespace_id: &NamespaceId,
    request: CommitRequest,
) -> Result<oneshot::Receiver<CommitResult>, CoreError> {
    let candidate = CommitCandidate::new(request);
    try_admit_candidate(publisher, namespace_id, candidate)
}

fn try_admit_candidate(
    publisher: &NamespacePublisher,
    namespace_id: &NamespaceId,
    candidate: CommitCandidate,
) -> Result<oneshot::Receiver<CommitResult>, CoreError> {
    try_admit_prepared_candidate(publisher, namespace_id, PreparedCandidate::new(candidate)?)
}

fn try_admit_prepared_candidate(
    publisher: &NamespacePublisher,
    namespace_id: &NamespaceId,
    candidate: PreparedCandidate,
) -> Result<oneshot::Receiver<CommitResult>, CoreError> {
    let commit_id = candidate.candidate.commit_id().clone();
    publisher.check_admission(&publisher.lock_state())?;
    let permit = publisher
        .admission
        .acquire_candidate(namespace_id, &candidate)?;
    let semantic_identity = candidate.candidate.semantic_identity(namespace_id)?;
    let (sender, receiver) = oneshot::channel();
    let admission = publisher.admit(
        commit_id,
        candidate,
        semantic_identity,
        AdmittedWaiter::new(sender, &permit),
        publisher.timer.monotonic_now_ms(),
    )?;
    assert!(matches!(admission, SubmissionAdmission::OwnOutcome));
    Ok(receiver)
}

async fn recv_commit(receiver: oneshot::Receiver<CommitResult>, label: &str) -> Commit {
    receiver
        .await
        .unwrap_or_else(|err| panic!("{label} receiver dropped: {err}"))
        .unwrap_or_else(|err| panic!("{label} failed: {err}"))
}

#[tokio::test]
async fn publisher_splits_batches_at_the_wal_bound_in_admission_order() {
    for extra_bytes in [0, 1] {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("bounded").expect("namespace");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::prefix(wal_prefix(&namespace_id)),
        ));
        let runtime = test_runtime(store.clone());
        create_namespace(&runtime, &namespace_id).await;
        let mut publisher = standalone_publisher(&namespace_id, &runtime);
        publisher.min_publish_interval = Duration::ZERO;
        recv_commit(
            admit_commit(
                &publisher,
                &namespace_id,
                create_directory_request("warmup", "warmup"),
            ),
            "warmup",
        )
        .await;
        store.reset();

        let available_bytes = MAX_WAL_OBJECT_BYTES - WAL_OBJECT_OVERHEAD_BYTES;
        let first_bound = available_bytes / 2;
        let mut responses = Vec::new();
        for (name, bound) in [
            ("first", first_bound),
            ("second", available_bytes - first_bound + extra_bytes),
        ] {
            let mut candidate =
                PreparedCandidate::new(CommitCandidate::new(create_directory_request(name, name)))
                    .expect("prepare");
            candidate.wal_record_bytes_upper_bound = bound;
            responses.push(
                try_admit_prepared_candidate(&publisher, &namespace_id, candidate).expect("admit"),
            );
        }
        let expected_batches = if extra_bytes == 0 {
            vec![2]
        } else {
            vec![1, 1]
        };
        let batches = publisher_state(&publisher)
            .queue
            .iter()
            .map(|item| match item {
                WorkItem::Batch(batch) => batch.candidates.len(),
                WorkItem::Delete(_) => panic!("expected commit batch"),
            })
            .collect::<Vec<_>>();
        assert_eq!(batches, expected_batches);
        for (response, expected_seq) in responses.into_iter().zip([ChangeSeq(2), ChangeSeq(3)]) {
            assert_eq!(
                recv_commit(response, "bounded").await.committed_seq,
                expected_seq
            );
        }
        assert_eq!(
            store.count(OperationClass::PutCreateIfAbsent),
            expected_batches.len()
        );
        publisher.wait_for_worker().await;
    }
}

#[tokio::test]
async fn publisher_splits_batches_at_the_inline_limit_without_failing_commits() {
    use loonfs_core::publish::InlineContent;
    use loonfs_types::format::wal::{
        MAX_WAL_INLINE_CONTENT_BYTES, MAX_WAL_OBJECT_INLINE_CONTENT_BYTES,
    };

    for extra_values in [0, 1] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("inline-batches").expect("namespace");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::prefix(wal_prefix(&namespace_id)),
        ));
        let runtime = test_runtime(store.clone());
        create_namespace(&runtime, &namespace_id).await;
        let mut publisher = standalone_publisher(&namespace_id, &runtime);
        publisher.min_publish_interval = Duration::ZERO;
        publisher
            .inline_content
            .inline_content_wal_object_budget_bytes = MAX_WAL_OBJECT_INLINE_CONTENT_BYTES;
        recv_commit(
            admit_commit(
                &publisher,
                &namespace_id,
                create_directory_request("warmup", "warmup"),
            ),
            "warmup",
        )
        .await;
        store.reset();
        let half = MAX_WAL_OBJECT_INLINE_CONTENT_BYTES / MAX_WAL_INLINE_CONTENT_BYTES / 2;
        let mut responses = Vec::new();
        for (name, count) in [("first", half), ("second", half + extra_values)] {
            let values: Vec<_> = (0..count)
                .map(|_| {
                    InlineContent::new(
                        namespace_id.clone(),
                        loonfs_types::ContentId::generate(),
                        Bytes::from(vec![1; MAX_WAL_INLINE_CONTENT_BYTES]),
                    )
                })
                .collect();
            let request = CommitRequest {
                commit_id: CommitId::parse(name).expect("commit"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                preconditions: Vec::new(),
                operations: values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(format!("/{name}-{index}")).expect("path"),
                        content_ref: Some(value.content_ref().clone()),
                        inline_content: None,
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    })
                    .collect(),
            };
            responses.push(
                try_admit_candidate(
                    &publisher,
                    &namespace_id,
                    CommitCandidate::with_inline_content(request, Vec::new(), values),
                )
                .expect("admit"),
            );
        }
        let expected_batches = if extra_values == 0 { 1 } else { 2 };
        assert_eq!(publisher_state(&publisher).queue.len(), expected_batches);
        for (response, expected_seq) in responses.into_iter().zip([ChangeSeq(2), ChangeSeq(3)]) {
            assert_eq!(
                recv_commit(response, "inline commit").await.committed_seq,
                expected_seq
            );
        }
        assert_eq!(
            store.count(OperationClass::PutCreateIfAbsent),
            expected_batches
        );
        publisher.wait_for_worker().await;
    }
}

#[test]
fn publisher_trace_labels_are_low_cardinality() {
    // A result label says only whether the publication succeeded. The error
    // text is caller data and must never reach a trace label, where it would
    // make the label set unbounded.
    assert_eq!(result_label(&Ok::<_, CoreError>(())).as_str(), "ok");
    assert_eq!(
        result_label(&Err::<(), _>(CoreError::Internal(
            "private error".to_owned()
        )))
        .as_str(),
        "error"
    );
}

#[tokio::test]
async fn publisher_delivery_preserves_bootstrap_namespace_exists_code() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let runtime = test_runtime(store);
    let publisher = standalone_publisher(&namespace_id, &runtime);
    let candidate = CommitCandidate::new(create_directory_request("bootstrap-error", "docs"));
    let commit_id = candidate.commit_id().clone();
    let semantic_identity = candidate
        .semantic_identity(&namespace_id)
        .expect("candidate identity");
    let (sender, receiver) = oneshot::channel();
    publisher_state(&publisher).in_flight.insert(
        commit_id.clone(),
        InFlightRequest {
            semantic_identity,
            waiters: vec![AdmittedWaiter::new(
                sender,
                &publisher
                    .admission
                    .acquire(
                        &namespace_id,
                        candidate.estimated_retained_bytes().expect("weight"),
                    )
                    .expect("admit request"),
            )],
        },
    );
    let selected_at = publisher.timer.monotonic_now_ms();
    publisher.deliver_batch_results(
        vec![commit_id],
        vec![Err(Error::Core(crate::CoreError::NamespaceExists {
            namespace_id: namespace_id.clone(),
        }))],
        selected_at,
    );

    let error = receiver
        .await
        .expect("publisher should deliver the result")
        .expect_err("bootstrap failure should remain an error");
    assert!(matches!(error, Error::Core(_)));
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
}

#[tokio::test]
async fn rejected_duplicate_joins_ready_in_flight_primary() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer(store).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let publisher = namespace.session().publisher.clone();
    let request = create_directory_request("ready-primary", "ready-primary");

    let primary = try_admit_candidate(
        &publisher,
        &namespace_id,
        CommitCandidate::new(request.clone()),
    )
    .expect("admit ready primary");
    let duplicate = try_admit_candidate(
        &publisher,
        &namespace_id,
        CommitCandidate::rejected(
            request,
            ContentPreparationError::ContentToken(vec![(
                loonfs_types::ContentId::generate(),
                ContentTokenError::Expired,
            )]),
        ),
    )
    .expect("join rejected duplicate");

    let primary = primary.await.expect("primary result channel");
    let duplicate = duplicate.await.expect("duplicate result channel");
    assert_eq!(
        duplicate.as_ref().expect("duplicate success"),
        primary.as_ref().expect("primary success")
    );
}

#[tokio::test]
async fn ready_duplicate_joins_rejected_in_flight_primary() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer(store).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let publisher = namespace.session().publisher.clone();
    let request = create_directory_request("rejected-primary", "rejected-primary");

    let primary = try_admit_candidate(
        &publisher,
        &namespace_id,
        CommitCandidate::rejected(
            request.clone(),
            ContentPreparationError::ContentToken(vec![(
                loonfs_types::ContentId::generate(),
                ContentTokenError::Expired,
            )]),
        ),
    )
    .expect("admit rejected primary");
    let duplicate = try_admit_candidate(&publisher, &namespace_id, CommitCandidate::new(request))
        .expect("join ready duplicate");

    let primary = primary.await.expect("primary result channel");
    let duplicate = duplicate.await.expect("duplicate result channel");
    let primary_error = primary.expect_err("primary preparation error");
    let duplicate_error = duplicate.expect_err("duplicate inherits preparation error");
    assert_eq!(primary_error.code(), ErrorCode::ContentNotPrepared);
    assert_eq!(duplicate_error.code(), primary_error.code());
    assert_eq!(duplicate_error.to_string(), primary_error.to_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_admits_pending_batch_while_active_publish_blocks() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    store.block_next();
    let active = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("active", "active"),
    );
    store.wait_until_blocked().await;

    let pending = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("pending", "pending"),
    );
    {
        let state = publisher_state(&publisher);
        assert!(state.worker.is_some());
        assert_eq!(queued_candidates(&state), 1);
    }

    store.release();
    let active_response = recv_commit(active, "active").await;
    let pending_response = recv_commit(pending, "pending").await;
    assert_eq!(active_response.committed_seq, ChangeSeq(1));
    assert_eq!(pending_response.committed_seq, ChangeSeq(2));

    let wal_keys = shared
        .list_prefix(&wal_prefix(
            &loonfs_types::NamespaceId::parse("demo").expect("valid namespace id"),
        ))
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_contender_waits_for_active_request_receipt() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    store.block_next();
    let active_request = create_directory_request("active", "active");
    let active_identity = CommitCandidate::new(active_request.clone())
        .semantic_identity(&namespace_id)
        .expect("active identity");
    let active = admit_commit(&publisher, &namespace_id, active_request);
    store.wait_until_blocked().await;

    let duplicate = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("active", "active"),
    );
    let conflict = {
        let publisher = publisher.clone();
        let request = create_directory_request("active", "different-active");
        tokio::spawn(async move { publisher.submit(CommitCandidate::new(request)).await })
    };
    let active_commit_id = CommitId::parse("active").expect("valid commit id");
    wait_for_commit_waiters(&publisher, &active_commit_id, 3).await;

    store.release();
    let active_response = recv_commit(active, "active").await;
    let duplicate_response = recv_commit(duplicate, "duplicate").await;
    let conflict = conflict
        .await
        .expect("conflict task")
        .expect_err("different request conflicts after the primary lands");
    assert_eq!(active_response.committed_seq, ChangeSeq(1));
    assert_eq!(duplicate_response.committed_seq, ChangeSeq(1));
    assert!(matches!(
        conflict,
        Error::Core(CoreError::CommitIdReuseConflict {
            commit_id,
            committed_seq: Some(ChangeSeq(1)),
            committed_fingerprint: Some(fingerprint),
        }) if commit_id == "active" && fingerprint == active_identity.as_str()
    ));
}

#[tokio::test]
async fn publisher_contender_retries_after_active_request_fails() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let runtime = test_runtime(store);
    create_namespace(&runtime, &namespace_id).await;
    let mut publisher = standalone_publisher(&namespace_id, &runtime);
    publisher.admission = Arc::new(PublicationAdmission::new(crate::PublicationLimits {
        max_requests: NonZeroUsize::new(1).expect("one slot"),
        ..crate::PublicationLimits::default()
    }));
    let commit_id = CommitId::parse("handoff").expect("valid commit id");
    let primary_identity = CommitCandidate::new(create_directory_request("handoff", "first"))
        .semantic_identity(&namespace_id)
        .expect("primary identity");
    publisher_state(&publisher).in_flight.insert(
        commit_id.clone(),
        InFlightRequest {
            semantic_identity: primary_identity,
            waiters: Vec::new(),
        },
    );

    let contender = {
        let publisher = publisher.clone();
        tokio::spawn(async move {
            publisher
                .submit(CommitCandidate::new(create_directory_request(
                    "handoff", "second",
                )))
                .await
        })
    };
    wait_for_commit_waiters(&publisher, &commit_id, 1).await;

    let failed_primary = publisher_state(&publisher)
        .in_flight
        .remove(&commit_id)
        .expect("in-flight primary");
    for waiter in failed_primary.waiters {
        let _ = waiter.send(Err(CoreError::Internal("primary failed".to_owned()).into()));
    }

    let response = contender
        .await
        .expect("contender task")
        .expect("contender publishes after the failed primary");
    assert_eq!(response.commit_id, commit_id);
    assert_eq!(response.committed_seq, ChangeSeq(1));
    assert_eq!(publisher.admission.used_requests(), 0);
}

#[tokio::test]
async fn publisher_contender_reports_conflict_after_retry_limit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let runtime = test_runtime(store);
    let publisher = standalone_publisher(&namespace_id, &runtime);
    let commit_id = CommitId::parse("bounded-handoff").expect("valid commit id");
    let primary_identity =
        CommitCandidate::new(create_directory_request("bounded-handoff", "first"))
            .semantic_identity(&namespace_id)
            .expect("primary identity");
    publisher_state(&publisher).in_flight.insert(
        commit_id.clone(),
        InFlightRequest {
            semantic_identity: primary_identity,
            waiters: Vec::new(),
        },
    );

    let contender = {
        let publisher = publisher.clone();
        tokio::spawn(async move {
            publisher
                .submit(CommitCandidate::new(create_directory_request(
                    "bounded-handoff",
                    "second",
                )))
                .await
        })
    };
    for _ in 0..CONTENTION_RETRY_LIMIT {
        wait_for_commit_waiters(&publisher, &commit_id, 1).await;
        let waiter = publisher_state(&publisher)
            .in_flight
            .get_mut(&commit_id)
            .expect("in-flight primary")
            .waiters
            .pop()
            .expect("contender waiter");
        let _ = waiter.send(Err(CoreError::Internal("primary failed".to_owned()).into()));
    }

    let error = contender
        .await
        .expect("contender task")
        .expect_err("retry limit reports a conflict");
    assert!(matches!(
        error,
        Error::Core(CoreError::CommitIdReuseConflict {
            commit_id: conflicting_id,
            committed_seq: None,
            committed_fingerprint: None,
        }) if conflicting_id == commit_id.as_str()
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_limit_counts_active_duplicate_contended_and_delete_requests() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    store.block_next();
    let active = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("active", "active"),
    );
    store.wait_until_blocked().await;

    let mut pending = Vec::new();
    for index in 0..crate::PublicationLimits::default()
        .max_requests_per_namespace
        .get()
        - 3
    {
        pending.push(admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request(format!("pending-{index}"), format!("pending-{index}")),
        ));
    }

    let duplicate = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("pending-0", "pending-0"),
    );
    let conflict = {
        let publisher = publisher.clone();
        let request = create_directory_request("pending-0", "different-pending");
        tokio::spawn(async move { publisher.submit(CommitCandidate::new(request)).await })
    };
    let pending_commit_id = CommitId::parse("pending-0").expect("valid commit id");
    wait_for_commit_waiters(&publisher, &pending_commit_id, 3).await;

    let overflow = try_admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("overflow", "overflow"),
    );
    assert!(matches!(overflow, Err(CoreError::CommitQueueFull)));

    assert!(matches!(
        try_admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("pending-0", "pending-0")
        ),
        Err(CoreError::CommitQueueFull)
    ));
    assert!(matches!(
        publisher.admit_delete(DeleteNamespaceOptions::default()),
        Err(CoreError::CommitQueueFull)
    ));

    store.release();
    assert_eq!(
        recv_commit(active, "active").await.committed_seq,
        ChangeSeq(1)
    );
    for (index, receiver) in pending.into_iter().enumerate() {
        assert_eq!(
            recv_commit(receiver, "pending").await.committed_seq,
            ChangeSeq(index as u64 + 2)
        );
    }
    assert_eq!(
        recv_commit(duplicate, "duplicate").await.committed_seq,
        ChangeSeq(2)
    );
    let conflict = conflict
        .await
        .expect("conflict task")
        .expect_err("different request conflicts after the primary lands");
    assert!(matches!(
        conflict,
        Error::Core(CoreError::CommitIdReuseConflict {
            commit_id,
            committed_seq: Some(ChangeSeq(2)),
            committed_fingerprint: Some(_),
        }) if commit_id == "pending-0"
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn publisher_takes_a_cold_full_batch_immediately() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    store.block_next();
    let mut receivers = Vec::new();
    for index in 0..crate::PublicationLimits::default()
        .max_requests_per_namespace
        .get()
    {
        receivers.push(admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request(format!("full-{index}"), format!("full-{index}")),
        ));
    }

    tokio::task::yield_now().await;
    {
        let state = publisher_state(&publisher);
        assert!(state.worker.is_some());
        assert!(state.queue.is_empty());
    }
    store.release();
    for (index, receiver) in receivers.into_iter().enumerate() {
        assert_eq!(
            recv_commit(receiver, "full").await.committed_seq,
            ChangeSeq(index as u64 + 1)
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cold_submission_publishes_without_a_coalescing_delay() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let runtime = test_runtime(store);
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    let receiver = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("cold", "cold"),
    );
    tokio::task::yield_now().await;
    assert!(
        publisher_state(&publisher).queue.is_empty(),
        "a cold batch must be taken immediately, not held for a coalescing timer"
    );
    let response = recv_commit(receiver, "cold").await;
    assert_eq!(response.committed_seq, ChangeSeq(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_queued_behind_a_publish_waits_out_the_pacing_interval() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let runtime = test_runtime(store.clone() as SharedStore);
    create_namespace(&runtime, &namespace_id).await;
    let mut publisher = standalone_publisher(&namespace_id, &runtime);
    let timer = Arc::new(ManualMonotonicTimer::default());
    publisher.timer = timer.clone();
    publisher.min_publish_interval = Duration::from_millis(400);

    store.block_next();
    let active = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("active", "active"),
    );
    store.wait_until_blocked().await;
    let mut queued = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("queued", "queued"),
    );
    store.release();
    recv_commit(active, "active").await;

    // `queued` was waiting when `active` settled, so it is paced.
    assert!(
        timeout(Duration::from_millis(200), &mut queued)
            .await
            .is_err(),
        "a request queued behind a publish must wait out the pacing interval"
    );
    timer.set(400);
    timeout(Duration::from_secs(10), queued)
        .await
        .expect("queued request publishes at the interval boundary")
        .expect("queued receiver")
        .expect("queued commit");
}

#[tokio::test]
async fn a_sequential_follow_up_publishes_at_once() {
    tokio::time::pause();
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let mut writer = test_writer_with_interval(store, 400).await;
    writer.mode.publisher.timer = Arc::new(ManualMonotonicTimer::default());
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for (commit_id, directory) in [("first", "first"), ("second", "second"), ("first", "first")] {
        tokio::time::timeout(
            Duration::from_millis(399),
            namespace.commit_candidate(CommitCandidate::new(create_directory_request(
                commit_id, directory,
            ))),
        )
        .await
        .expect("a request after its predecessor settled must not be paced")
        .expect("commit");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_resolves_unknown_head_outcome_by_replaying_receipt() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(lost_wal_put_ack_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared);
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    // One clean commit first, so the session holds its writer epoch. Epoch
    // acquisition is a WAL put too, and this test is about the
    // publication swap.
    let warm = recv_commit(
        admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("warm-epoch", "warm-epoch"),
        ),
        "warm-epoch",
    )
    .await;
    assert_eq!(warm.committed_seq, ChangeSeq(1));

    // The commit lands but the CAS acknowledgement is lost. The publisher
    // retries with the same commit id and replays the durable receipt
    // instead of reporting `commit_outcome_unknown` to the waiter.
    store.fail_next(1);
    let response = recv_commit(
        admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("unknown-ack", "unknown-ack"),
        ),
        "unknown-ack",
    )
    .await;
    assert_eq!(response.committed_seq, ChangeSeq(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_survives_publish_panic_and_keeps_serving() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(PanicWalPutStore::new(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    store.arm_blocking_panic();
    let doomed = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("doomed", "doomed"),
    );
    store.wait_until_blocked().await;

    // Queued behind the in-flight batch: only a worker that survives the
    // panic can ever publish this one.
    let queued = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("queued", "queued"),
    );

    store.release_into_panic();

    // The panic may have struck either side of the WAL put, so the
    // taken request reports an unknown outcome, not definite failure.
    let doomed_error = doomed
        .await
        .expect("doomed waiter is answered, not abandoned")
        .expect_err("doomed commit did not complete");
    assert_eq!(doomed_error.code(), ErrorCode::CommitOutcomeUnknown);

    let queued_response = recv_commit(queued, "queued").await;
    assert_eq!(queued_response.committed_seq, ChangeSeq(1));

    // The publisher is fully serviceable after the panic.
    let after = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("after", "after"),
    );
    assert_eq!(
        recv_commit(after, "after").await.committed_seq,
        ChangeSeq(2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_barrier_publishes_admitted_work_and_rejects_later_work() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    // A publishes and blocks at its WAL put; B queues behind it.
    store.block_next();
    let before_a = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("before-a", "before-a"),
    );
    store.wait_until_blocked().await;
    let before_b = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("before-b", "before-b"),
    );

    // The delete arrives: everything above was admitted before it,
    // everything below after it.
    let delete_task = {
        let publisher = publisher.clone();
        tokio::spawn(async move {
            publisher
                .submit_delete(DeleteNamespaceOptions::default())
                .await
        })
    };
    // Deterministic: wait until the delete has queued behind the open batch.
    wait_for_queued_delete(&publisher).await;
    let after = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("after", "after"),
    );

    store.release();

    // Admitted-before work publishes; the delete lands after it.
    assert_eq!(
        recv_commit(before_a, "before-a").await.committed_seq,
        ChangeSeq(1)
    );
    assert_eq!(
        recv_commit(before_b, "before-b").await.committed_seq,
        ChangeSeq(2)
    );
    let response = delete_task
        .await
        .expect("delete task")
        .expect("delete succeeds");
    assert_eq!(response.head_seq, ChangeSeq(2));
    let statistics = loonfs_core::control::load_namespace_statistics(store.as_ref(), &namespace_id)
        .await
        .expect("final statistics");
    assert_eq!(statistics.activity.mutations.get(), 2);
    assert_eq!(statistics.manifest.head_seq, response.head_seq);
    assert_eq!(statistics.inode_record_count, 3);

    // Admitted-after work is rejected, and the tombstone fails new
    // admissions immediately.
    let after_error = after
        .await
        .expect("after waiter answered")
        .expect_err("admitted after the delete");
    assert_eq!(after_error.code(), ErrorCode::NamespaceDeleted);
    let fast_fail = try_admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("too-late", "too-late"),
    );
    assert!(matches!(fast_fail, Err(CoreError::NamespaceDeleted { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_delete_during_inflight_delete_settles_both() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    // One publication first, so the session already holds its writer epoch:
    // the next WAL put is the delete's own tombstone swap.
    recv_commit(
        admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("seed", "seed"),
        ),
        "seed",
    )
    .await;

    store.block_next();
    let first = {
        let publisher = publisher.clone();
        tokio::spawn(async move {
            publisher
                .submit_delete(DeleteNamespaceOptions::default())
                .await
        })
    };
    store.wait_until_blocked().await;

    // Admitted while the first delete holds the head: a mutation, then a
    // second delete behind it.
    let orphan = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("orphan", "orphan"),
    );
    let second = {
        let publisher = publisher.clone();
        tokio::spawn(async move {
            publisher
                .submit_delete(DeleteNamespaceOptions::default())
                .await
        })
    };
    wait_for_queued_delete(&publisher).await;

    store.release();
    let first_response = first
        .await
        .expect("first delete task")
        .expect("first delete succeeds");
    assert_eq!(first_response.head_seq, ChangeSeq(1));

    // Bounded: a stranded waiter is a hang, and a hang must fail the test.
    let second_error = timeout(Duration::from_secs(10), second)
        .await
        .expect("the second delete must settle, not hang")
        .expect("second delete task")
        .expect_err("second delete after the tombstone");
    assert_eq!(second_error.code(), ErrorCode::NamespaceDeleted);
    let orphan_error = orphan
        .await
        .expect("orphan waiter answered")
        .expect_err("admitted behind the delete");
    assert_eq!(orphan_error.code(), ErrorCode::NamespaceDeleted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_pending_deletes_coalesce_into_one_outcome() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    recv_commit(
        admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("seed", "seed"),
        ),
        "seed",
    )
    .await;

    // A blocked publication holds the worker, so both deletes stay
    // pending at the tail together.
    store.block_next();
    let gate = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("gate", "gate"),
    );
    store.wait_until_blocked().await;

    let options = DeleteNamespaceOptions {
        expected_head_seq: Some(ChangeSeq(2)),
    };
    let first = spawn_delete(&publisher, options);
    wait_for_queued_delete_waiters(&publisher, 1).await;
    let second = spawn_delete(&publisher, options);
    wait_for_queued_delete_waiters(&publisher, 2).await;
    assert_eq!(
        queued_delete_shape(&publisher_state(&publisher)),
        (1, 2),
        "equal options coalesce into one pending delete"
    );

    store.release();
    recv_commit(gate, "gate").await;
    let first = settle_delete(first, "first delete")
        .await
        .expect("first delete succeeds");
    let second = settle_delete(second, "second delete")
        .await
        .expect("second delete succeeds");
    assert_eq!(first.head_seq, ChangeSeq(2));
    assert_eq!(second.head_seq, ChangeSeq(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_deletes_with_different_preconditions_settle_separately() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    recv_commit(
        admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("seed", "seed"),
        ),
        "seed",
    )
    .await;

    store.block_next();
    let gate = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("gate", "gate"),
    );
    store.wait_until_blocked().await;

    let stale = spawn_delete(
        &publisher,
        DeleteNamespaceOptions {
            expected_head_seq: Some(ChangeSeq(1)),
        },
    );
    wait_for_queued_delete_waiters(&publisher, 1).await;
    let unconditional = spawn_delete(&publisher, DeleteNamespaceOptions::default());
    wait_for_queued_delete_waiters(&publisher, 2).await;
    assert_eq!(
        queued_delete_shape(&publisher_state(&publisher)),
        (2, 2),
        "different options stay separate pending deletes"
    );

    store.release();
    recv_commit(gate, "gate").await;
    let stale_error = settle_delete(stale, "stale delete")
        .await
        .expect_err("the gate publication moved the head past its precondition");
    assert_eq!(stale_error.code(), ErrorCode::StaleHead);
    let deleted = settle_delete(unconditional, "unconditional delete")
        .await
        .expect("the unconditional delete lands after the stale one fails");
    assert_eq!(deleted.head_seq, ChangeSeq(2));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_pending_delete_does_not_share_the_first_deletes_success() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    recv_commit(
        admit_commit(
            &publisher,
            &namespace_id,
            create_directory_request("seed", "seed"),
        ),
        "seed",
    )
    .await;

    store.block_next();
    let gate = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("gate", "gate"),
    );
    store.wait_until_blocked().await;

    let valid = spawn_delete(
        &publisher,
        DeleteNamespaceOptions {
            expected_head_seq: Some(ChangeSeq(2)),
        },
    );
    wait_for_queued_delete_waiters(&publisher, 1).await;
    let stale = spawn_delete(
        &publisher,
        DeleteNamespaceOptions {
            expected_head_seq: Some(ChangeSeq(1)),
        },
    );
    wait_for_queued_delete_waiters(&publisher, 2).await;

    store.release();
    recv_commit(gate, "gate").await;
    let landed = settle_delete(valid, "valid delete")
        .await
        .expect("the delete whose precondition holds lands");
    assert_eq!(landed.head_seq, ChangeSeq(2));
    let stale_error = settle_delete(stale, "stale delete")
        .await
        .expect_err("a precondition the tombstone outran");
    assert_eq!(stale_error.code(), ErrorCode::NamespaceDeleted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutations_admitted_after_a_queued_delete_wait_behind_it() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    store.block_next();
    let before = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("before", "before"),
    );
    store.wait_until_blocked().await;

    let delete_task = {
        let publisher = publisher.clone();
        tokio::spawn(async move {
            publisher
                .submit_delete(DeleteNamespaceOptions::default())
                .await
        })
    };
    wait_for_queued_delete(&publisher).await;
    let after = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("after", "after"),
    );

    store.release();
    assert_eq!(
        recv_commit(before, "before").await.committed_seq,
        ChangeSeq(1)
    );
    let delete_response = delete_task
        .await
        .expect("delete task")
        .expect("delete succeeds behind the in-flight publication");
    assert_eq!(delete_response.head_seq, ChangeSeq(1));
    let after_error = after
        .await
        .expect("after waiter answered")
        .expect_err("admitted after the delete");
    assert_eq!(after_error.code(), ErrorCode::NamespaceDeleted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_batches_concurrent_distinct_commits_into_one_wal_object() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    let namespace = writer.namespace(&namespace_id);
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    // Hold the cold publication in flight so both concurrent submissions are
    // deterministically admitted to the pending batch behind it.
    store.block_next();
    let warmup = {
        let namespace_writer = namespace_writer.clone();
        tokio::spawn(async move {
            namespace_writer
                .commit_candidate(CommitCandidate::new(create_directory_request(
                    "warmup", "warmup",
                )))
                .await
        })
    };
    store.wait_until_blocked().await;

    let actor_a = ActorId::parse("user-a").expect("actor id");
    let actor_b = ActorId::parse("service-b").expect("actor id");
    let mut request_a = create_directory_request("req-a", "alpha");
    request_a.actor_id = actor_a.clone();
    let mut request_b = create_directory_request("req-b", "beta");
    request_b.actor_id = actor_b.clone();
    let response_a = {
        let namespace_writer = namespace_writer.clone();
        tokio::spawn(async move {
            namespace_writer
                .commit_candidate(CommitCandidate::new(request_a))
                .await
        })
    };
    let response_b = {
        let namespace_writer = namespace_writer.clone();
        tokio::spawn(async move {
            namespace_writer
                .commit_candidate(CommitCandidate::new(request_b))
                .await
        })
    };
    let publisher = namespace_writer.session().publisher.clone();
    wait_for_queued_candidates(&publisher, 2).await;

    store.release();
    assert_eq!(
        warmup
            .await
            .expect("warmup task")
            .expect("warmup response")
            .committed_seq,
        ChangeSeq(1)
    );
    let response_a = response_a
        .await
        .expect("response a task")
        .expect("response a");
    let response_b = response_b
        .await
        .expect("response b task")
        .expect("response b");
    let mut committed_seqs = [response_a.committed_seq, response_b.committed_seq];
    committed_seqs.sort_unstable();
    assert_eq!(committed_seqs, [ChangeSeq(2), ChangeSeq(3)]);

    // The warmup published alone; the two concurrent submissions share
    // one WAL object.
    let wal_keys = shared
        .list_prefix(&wal_prefix(
            &loonfs_types::NamespaceId::parse("demo").expect("valid namespace id"),
        ))
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 3);

    let mut batched_actors = std::collections::BTreeMap::new();
    for key in &wal_keys {
        let bytes = shared
            .get(key, None)
            .await
            .expect("read WAL object")
            .expect("WAL object exists");
        let wal_object = decode_wal_object_envelope_zstd(&bytes).expect("decode WAL object");
        if wal_object.payload().records.len() == 2 {
            for record in wal_object.into_payload().records {
                batched_actors.insert(record.commit_id.to_string(), record.committed_by);
            }
        }
    }
    assert_eq!(batched_actors.get("req-a"), Some(&actor_a));
    assert_eq!(batched_actors.get("req-b"), Some(&actor_b));

    let changes = namespace
        .list_changes(ChangeSeq(0))
        .page(first_page())
        .await
        .expect("read change feed");
    let feed_actors = changes
        .changes
        .into_iter()
        .map(|change| (change.commit_id.to_string(), change.committed_by))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(feed_actors.get("req-a"), Some(&actor_a));
    assert_eq!(feed_actors.get("req-b"), Some(&actor_b));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_batches_plain_and_prepared_mutations_together() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let upload = namespace.create_upload().await.expect("begin upload");
    let staged = namespace
        .put_upload_content(&upload.upload_id, b"hello")
        .await
        .expect("stage content");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&shared, &namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared_content = loonfs_core::content::prepare_existing_content_ref(
        &shared,
        &catalog,
        staged.content_ref().expect("staged content").clone(),
    )
    .await
    .expect("prepare existing content");

    // Hold the cold publication in flight so both concurrent submissions are
    // deterministically admitted to the pending batch behind it.
    store.block_next();
    let warmup = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::new(create_directory_request(
                    "warmup", "warmup",
                )))
                .await
        })
    };
    store.wait_until_blocked().await;

    let plain = create_directory_request("plain-mutation", "alpha");
    let prepared = CommitRequest::single(
        CommitId::parse("prepared-put").expect("valid commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/file.txt").expect("path"),
            content_ref: Some(prepared_content.content_ref().clone()),
            inline_content: None,
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    );
    let plain_response = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::new(plain))
                .await
        })
    };
    let prepared_response = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::prepared(prepared, vec![prepared_content]))
                .await
        })
    };
    let publisher = namespace.session().publisher.clone();
    wait_for_queued_candidates(&publisher, 2).await;

    store.release();
    assert_eq!(
        warmup
            .await
            .expect("warmup task")
            .expect("warmup response")
            .committed_seq,
        ChangeSeq(1)
    );
    let plain_response = plain_response
        .await
        .expect("plain task")
        .expect("plain response");
    let prepared_response = prepared_response
        .await
        .expect("prepared task")
        .expect("prepared response");
    let mut committed_seqs = [
        plain_response.committed_seq,
        prepared_response.committed_seq,
    ];
    committed_seqs.sort_unstable();
    assert_eq!(committed_seqs, [ChangeSeq(2), ChangeSeq(3)]);

    let wal_keys = shared
        .list_prefix(&wal_prefix(
            &loonfs_types::NamespaceId::parse("demo").expect("valid namespace id"),
        ))
        .await
        .expect("list wal");
    let mut record_counts = Vec::new();
    for key in &wal_keys {
        let wal_bytes = store
            .get(key, None)
            .await
            .expect("read wal")
            .expect("wal exists");
        let wal_object = decode_wal_object_envelope_zstd(&wal_bytes).expect("decode WAL object");
        record_counts.push(wal_object.payload().records.len());
    }
    record_counts.sort_unstable();
    // The warmup published alone; the concurrent pair shares a WAL object.
    assert_eq!(record_counts, vec![0, 1, 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_close_admission_refuses_new_work_while_admitted_work_drains() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let registry = writer.mode.publisher.clone();
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    // An admitted publication blocks at its WAL put...
    store.block_next();
    let active = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::new(create_directory_request(
                    "active", "active",
                )))
                .await
        })
    };
    store.wait_until_blocked().await;

    // Admission then closes, and new work is refused.
    writer.close_admission();
    let refused = namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "refused", "refused",
        )))
        .await
        .expect_err("submission after close_admission");
    assert_eq!(refused.code(), ErrorCode::ShuttingDown);

    // A publisher clone that predates the sweep also refuses directly.
    let publisher = namespace.session().publisher.clone();
    let direct = try_admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("direct", "direct"),
    );
    assert!(matches!(direct, Err(CoreError::ShuttingDown)));

    // The admitted publication still settles, and drain joins its worker.
    store.release();
    let response = active
        .await
        .expect("submit task")
        .expect("admitted commit publishes");
    assert_eq!(response.committed_seq, ChangeSeq(1));
    writer.drain().await.expect("drain settles publish tasks");
    assert!(registry
        .shared
        .lock_state()
        .sessions
        .values()
        .all(|live| publisher_state(&live.publisher).worker.is_none()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_survives_panic_and_processes_later_queue_items() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(PanicWalPutStore::new(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let registry = writer.mode.publisher.clone();
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    store.arm_blocking_panic();
    let doomed = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::new(create_directory_request(
                    "doomed", "doomed",
                )))
                .await
        })
    };
    store.wait_until_blocked().await;

    // Queued behind the doomed batch: only a worker that survives the panic
    // publishes this one, and the drain must wait for it.
    let queued = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::new(create_directory_request(
                    "queued", "queued",
                )))
                .await
        })
    };
    let publisher = namespace.session().publisher.clone();
    wait_for_queued_candidates(&publisher, 1).await;

    store.release_into_panic();
    writer.close_admission();

    let doomed_error = doomed
        .await
        .expect("doomed submit task")
        .expect_err("doomed commit did not complete");
    assert_eq!(doomed_error.code(), ErrorCode::CommitOutcomeUnknown);
    let queued_response = queued
        .await
        .expect("queued submit task")
        .expect("the surviving worker publishes queued work");
    assert_eq!(queued_response.committed_seq, ChangeSeq(1));

    let drain_error = writer
        .drain()
        .await
        .expect_err("drain surfaces the contained panic");
    assert!(
        drain_error.to_string().contains("panicked"),
        "drain reports panicked publisher tasks: {drain_error}"
    );
    assert_eq!(
        registry.shared.admission.used_requests(),
        0,
        "panic and drain refund admission"
    );
}

#[tokio::test]
async fn a_fold_reloads_the_tail_when_no_projection_is_retained() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("no-projection").expect("valid namespace id");
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("writer-a")
        .metadata_cache(
            MetadataCache::builder()
                .max_segment_bytes(0)
                .max_head_state_bytes(0)
                .build(),
        )
        .build()
        .await
        .expect("build writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    append_wal_objects(
        store.as_ref(),
        &namespace_id,
        FOLD_AT_WAL_OBJECTS - 1,
        &MutationContext {
            writer_id: loonfs_types::WriterId::parse("fold-seed").expect("valid writer id"),
            now_ms: 1_000,
        },
    )
    .await
    .expect("seed the WAL tail below the fold threshold");

    namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "cross-threshold",
            "docs",
        )))
        .await
        .expect("publish across the fold threshold");
    namespace
        .wait_for_fold()
        .await
        .expect("fold the uncached tail");

    let status = writer
        .maintenance(loonfs_test_support::ids::writer_id("fold-inspection"))
        .diagnostics(&namespace_id)
        .await
        .expect("inspect the folded namespace");
    assert!(status.current_manifest_no.is_some(), "{status:?}");
    assert!(status.wal_tail_objects < FOLD_AT_WAL_OBJECTS, "{status:?}");
    writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn a_runtime_fold_materializes_inline_content_and_reanchors_to_an_empty_tail() {
    use loonfs_core::publish::InlineContent;

    let directory = tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let namespace_id = NamespaceId::parse("inline-fold").expect("namespace");
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("writer")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    append_wal_objects(
        store.as_ref(),
        &namespace_id,
        FOLD_AT_WAL_OBJECTS - 1,
        &MutationContext {
            writer_id: loonfs_types::WriterId::parse("seed").expect("writer"),
            now_ms: 1_000,
        },
    )
    .await
    .expect("seed tail");
    let value = InlineContent::new(
        namespace_id.clone(),
        loonfs_types::ContentId::generate(),
        Bytes::from_static(b"folded inline bytes"),
    );
    let candidate = CommitCandidate::with_inline_content(
        CommitRequest::single(
            CommitId::parse("inline").expect("commit"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/inline").expect("path"),
                content_ref: Some(value.content_ref().clone()),
                inline_content: None,
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ),
        Vec::new(),
        vec![value.clone()],
    );
    namespace_writer
        .commit_candidate(candidate.clone())
        .await
        .expect("publish");
    namespace_writer.wait_for_fold().await.expect("fold");
    let folded = loonfs_core::control::load_namespace_statistics(store.as_ref(), &namespace_id)
        .await
        .expect("inline statistics");
    assert_eq!(
        folded.activity.content_bytes.get(),
        value.bytes().len() as u64
    );
    assert_eq!(folded.activity.file_revisions.get(), 1);
    assert_eq!(store.count(OperationClass::Put), 1);
    let reader = crate::LoonFs::builder_with_store(store.clone())
        .read_only()
        .build()
        .await
        .expect("fresh reader");
    let namespace = reader.namespace(&namespace_id);
    assert_eq!(
        namespace
            .read_file("/inline")
            .await
            .expect("read after fold")
            .bytes,
        value.bytes().as_ref()
    );
    assert!(store.count(OperationClass::Read) > 0);
    let input = namespace_writer
        .session()
        .publisher
        .engine
        .lock()
        .await
        .engine
        .as_ref()
        .expect("engine")
        .wal_fold_input()
        .expect("reanchored projection");
    assert_eq!(input.wal_tail_objects, 0);
    assert_eq!(
        input.tail_state.decoded_bytes(),
        loonfs_core::cache::ProjectedWalTail::default().decoded_bytes()
    );
    store.reset();
    namespace_writer
        .commit_candidate(candidate)
        .await
        .expect("folded receipt");
    assert_eq!(store.count(OperationClass::Put), 0);
    assert_eq!(
        loonfs_core::control::load_namespace_statistics(store.as_ref(), &namespace_id)
            .await
            .expect("after extraction and retry")
            .activity,
        folded.activity
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_failed_fold_reloads_the_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let failing = Arc::new(FailStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        is_fold,
        InjectedError::PermissionDenied("injected manifest write failure".to_owned()),
    ));
    let namespace_id = NamespaceId::parse("failed-fold").expect("valid namespace id");
    let writer = crate::LoonFs::builder_with_store(failing.clone())
        .writer_id("writer-a")
        .build()
        .await
        .expect("build writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    append_wal_objects(
        failing.as_ref(),
        &namespace_id,
        FOLD_AT_WAL_OBJECTS - 1,
        &MutationContext {
            writer_id: loonfs_types::WriterId::parse("fold-seed").expect("valid writer id"),
            now_ms: 1_000,
        },
    )
    .await
    .expect("seed the WAL tail below the fold threshold");

    failing.fail_next(1);
    namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "cross-threshold",
            "docs",
        )))
        .await
        .expect("publish across the fold threshold");
    namespace
        .wait_for_fold()
        .await
        .expect("settle the failed fold");

    assert_eq!(failing.attempts(), 1);
    loonfs_core::fold_wal_tail(
        failing.inner(),
        None,
        &namespace_id,
        None,
        &Deadline::start(Arc::new(
            loonfs_objectstore::timing::StdMonotonicTimer::default(),
        )),
    )
    .await
    .expect("another process folds the tail");
    namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "after-fold",
            "after-fold",
        )))
        .await
        .expect("reload the folded tail and publish");
    let publisher = writer
        .mode
        .publisher
        .live_publisher(&namespace_id)
        .expect("live session");
    let tail_objects = publisher
        .engine
        .lock()
        .await
        .engine
        .as_ref()
        .and_then(NamespaceCommitEngine::wal_tail_objects);
    assert!(
        tail_objects.is_some_and(|objects| objects < FOLD_AT_WAL_OBJECTS),
        "{tail_objects:?}"
    );
    writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn wal_folds_share_the_writer_concurrency_bound() {
    let temp_dir = tempdir().expect("tempdir");
    let namespaces = [
        NamespaceId::parse("fold-a").expect("valid namespace id"),
        NamespaceId::parse("fold-b").expect("valid namespace id"),
        NamespaceId::parse("fold-c").expect("valid namespace id"),
    ];
    let fold_c = blocking_fold_store(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        metadata_manifest_prefix(&namespaces[2]),
    );
    let fold_b = blocking_fold_store(fold_c, metadata_manifest_prefix(&namespaces[1]));
    let blocking = Arc::new(blocking_fold_store(
        fold_b,
        metadata_manifest_prefix(&namespaces[0]),
    ));
    let writer = crate::LoonFs::builder_with_store(blocking.clone())
        .writer_id("writer-a")
        .max_concurrent_folds(NonZeroUsize::new(1).expect("nonzero fold limit"))
        .build()
        .await
        .expect("build writer");
    for namespace_id in &namespaces {
        writer
            .create_namespace(namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("bootstrap");
        append_wal_objects(
            blocking.as_ref(),
            namespace_id,
            FOLD_AT_WAL_OBJECTS - 1,
            &MutationContext {
                writer_id: loonfs_types::WriterId::parse("fold-seed").expect("valid writer id"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed the WAL tail below the fold threshold");
    }
    let namespace_writers = namespaces
        .iter()
        .map(|namespace_id| writer.open_namespace(namespace_id).expect("open namespace"))
        .collect::<Vec<_>>();
    blocking.block_next();
    blocking.inner().block_next();
    blocking.inner().inner().block_next();

    for (index, namespace) in namespace_writers.iter().enumerate() {
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                format!("cross-threshold-{index}"),
                "docs",
            )))
            .await
            .expect("publish across the fold threshold");
        if index == 0 {
            blocking.wait_until_blocked().await;
        } else {
            wait_for_fold_waiters(&writer, index).await;
        }
    }
    assert_eq!(writer.mode.bits.wal_fold_permits.available_permits(), 0);
    let closing = tokio::spawn(namespace_writers[2].clone().close());
    while namespace_writers[2].session_state() != NamespaceSessionState::Closed {
        tokio::task::yield_now().await;
    }

    blocking.release();
    blocking.inner().wait_until_blocked().await;
    blocking.inner().release();
    blocking.inner().inner().wait_until_blocked().await;
    blocking.inner().inner().release();
    for namespace in &namespace_writers[..2] {
        namespace
            .wait_for_fold()
            .await
            .expect("released fold settles");
    }
    timeout(Duration::from_secs(10), closing)
        .await
        .expect("namespace close should settle")
        .expect("join namespace close")
        .expect("close namespace with a waiting fold");
    assert_eq!(writer.mode.bits.wal_folds_waiting.load(Ordering::SeqCst), 0);
    assert_eq!(writer.mode.bits.wal_fold_permits.available_permits(), 1);
    writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn a_late_fold_does_not_republish_an_already_folded_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_a = NamespaceId::parse("fold-a").expect("valid namespace id");
    let namespace_b = NamespaceId::parse("fold-b").expect("valid namespace id");
    let blocked = blocking_fold_store(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        metadata_manifest_prefix(&namespace_a),
    );
    let recording = Arc::new(RecordingStore::new(
        blocked,
        KeyPredicate::prefix(metadata_manifest_prefix(&namespace_b)),
    ));
    let writer = crate::LoonFs::builder_with_store(recording.clone())
        .writer_id("writer-a")
        .max_concurrent_folds(NonZeroUsize::new(1).expect("nonzero fold limit"))
        .build()
        .await
        .expect("build writer");
    for namespace_id in [&namespace_a, &namespace_b] {
        writer
            .create_namespace(namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("bootstrap");
        append_wal_objects(
            recording.as_ref(),
            namespace_id,
            FOLD_AT_WAL_OBJECTS - 1,
            &MutationContext {
                writer_id: loonfs_types::WriterId::parse("fold-seed").expect("valid writer id"),
                now_ms: 1_000,
            },
        )
        .await
        .expect("seed the WAL tail below the fold threshold");
    }
    let namespace_writer_a = writer.open_namespace(&namespace_a).expect("open namespace");
    let namespace_writer_b = writer.open_namespace(&namespace_b).expect("open namespace");
    recording.inner().block_next();
    namespace_writer_a
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "cross-a", "docs",
        )))
        .await
        .expect("publish across the fold threshold");
    recording.inner().wait_until_blocked().await;

    // An explicit fold takes a fold permit too, so it queues ahead of the
    // fold that the next publication starts, and folds that tail first.
    let explicit_fold = tokio::spawn({
        let maintenance =
            writer.maintenance(loonfs_test_support::ids::writer_id("late-fold-maintenance"));
        let namespace_b = namespace_b.clone();
        async move { maintenance.fold_wal(&namespace_b).await }
    });
    wait_for_fold_waiters(&writer, 1).await;
    namespace_writer_b
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "cross-b", "docs",
        )))
        .await
        .expect("publish across the fold threshold");
    wait_for_fold_waiters(&writer, 2).await;

    recording.reset();
    recording.inner().release();
    let folded = explicit_fold
        .await
        .expect("join the explicit fold")
        .expect("fold the namespace tail");
    assert_eq!(folded.outcome, crate::FoldWalOutcome::Published);
    assert_eq!(folded.manifest_head_seq, folded.target_head_seq);
    namespace_writer_a
        .wait_for_fold()
        .await
        .expect("first fold settles");
    namespace_writer_b
        .wait_for_fold()
        .await
        .expect("late fold settles");
    assert_eq!(
        recording.count(OperationClass::Put),
        1,
        "the late fold must not write a second manifest"
    );
    writer.shutdown().await.expect("shut down writer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_delete_waits_for_fold_before_evicting_the_namespace_publisher() {
    let temp_dir = tempdir().expect("tempdir");
    let blocking = Arc::new(blocking_fold_store(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        "namespaces/".to_owned(),
    ));
    let store = blocking.clone() as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer(store.clone()).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let registry = writer.mode.publisher.clone();
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    append_wal_objects(
        blocking.inner(),
        &namespace_id,
        FOLD_AT_WAL_OBJECTS - 1,
        &MutationContext {
            writer_id: loonfs_types::WriterId::parse("fold-seed").expect("valid writer id"),
            now_ms: 1_000,
        },
    )
    .await
    .expect("seed the WAL tail below the fold threshold");

    blocking.block_next();
    namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "before", "before",
        )))
        .await
        .expect("threshold-crossing commit before delete");
    blocking.wait_until_blocked().await;
    assert_eq!(registry.shared.lock_state().sessions.len(), 1);
    let publisher = namespace.session().publisher.clone();

    let mut delete = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .session()
                .submit_delete(DeleteNamespaceOptions::default())
                .await
        })
    };
    if let Ok(completed) = timeout(Duration::from_millis(100), &mut delete).await {
        blocking.release();
        panic!("delete completed while its earlier fold was parked: {completed:?}");
    }

    assert!(!matches!(
        publisher_state(&publisher).admission,
        PublisherAdmissionState::Deleted
    ));
    let manifest =
        loonfs_core::control::load_namespace_current_manifest(blocking.inner(), &namespace_id)
            .await
            .expect("manifest during fold");
    assert!(!manifest.state.envelope.payload().status.is_deleted());

    blocking.release();
    settle_delete(delete, "delete waiting for the earlier fold")
        .await
        .expect("delete namespace");
    assert!(
        registry.shared.lock_state().sessions.is_empty(),
        "a deleted session must not stay in the table while its handle is held"
    );
    let fast = namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "fast", "fast",
        )))
        .await
        .expect_err("submission through the deleted session");
    assert_eq!(fast.code(), ErrorCode::NamespaceDeleted);

    // A later open builds a fresh session whose submission still fails, now
    // on the durable tombstone instead of the fast in-memory flag.
    let reopened = writer
        .open_namespace(&namespace_id)
        .expect("reopen namespace");
    assert!(!std::ptr::eq(reopened.session(), namespace.session()));
    let late = reopened
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "late", "late",
        )))
        .await
        .expect_err("submission after delete");
    assert_eq!(late.code(), ErrorCode::NamespaceDeleted);
    writer.shutdown().await.expect("shut down after delete");
}

#[tokio::test(flavor = "current_thread")]
async fn close_admission_refuses_without_creating_publishers() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer(store.clone()).await;
    let registry = writer.mode.publisher.clone();

    writer.close_admission();
    let refused = writer
        .open_namespace(&namespace_id)
        .expect_err("closed registry refuses to open a session");
    assert_eq!(refused.code(), ErrorCode::ShuttingDown);
    assert!(registry.shared.lock_state().sessions.is_empty());
    writer.drain().await.expect("nothing to drain");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_admitted_before_close_admission_lands_terminal() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    // A publication parks at its WAL put, so the delete deterministically
    // queues behind it instead of being taken first.
    store.block_next();
    let active = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::new(create_directory_request(
                    "active", "active",
                )))
                .await
        })
    };
    store.wait_until_blocked().await;
    let publisher = namespace.session().publisher.clone();
    let delete = spawn_delete(&publisher, DeleteNamespaceOptions::default());
    wait_for_queued_delete(&publisher).await;

    // Admission closes with the delete already admitted; releasing the gate
    // lets the batch and then the delete publish.
    writer.close_admission();
    store.release();

    let response = active
        .await
        .expect("submit task")
        .expect("admitted commit publishes");
    assert_eq!(response.committed_seq, ChangeSeq(1));
    let deleted = settle_delete(delete, "delete admitted before close_admission")
        .await
        .expect("an admitted delete lands after admission closes");
    assert_eq!(deleted.head_seq, ChangeSeq(1));

    assert!(matches!(
        publisher_state(&publisher).admission,
        PublisherAdmissionState::Deleted
    ));
    let late = try_admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("late", "late"),
    )
    .expect_err("submission after the delete lands");
    assert_eq!(late.code(), ErrorCode::NamespaceDeleted);
    writer.drain().await.expect("drain settles the delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_queued_mid_publish_waits_behind_admitted_work() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    // Park the first publication at its WAL put, batch a second commit
    // behind it, then queue the delete: the queued batch must publish
    // before the delete runs, and the blocked CAS outlasts the pacing
    // interval — the interleaving where a racing second worker could run
    // the delete first.
    store.block_next();
    let before = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::new(create_directory_request(
                    "before", "before",
                )))
                .await
        })
    };
    store.wait_until_blocked().await;
    let publisher = namespace.session().publisher.clone();

    // The worker is parked in the blocked CAS, so this admission
    // deterministically queues the next batch instead of being taken.
    let second = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::new(create_directory_request(
                    "second", "second",
                )))
                .await
        })
    };
    wait_for_queued_candidates(&publisher, 1).await;

    let delete = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .session()
                .submit_delete(DeleteNamespaceOptions::default())
                .await
        })
    };
    // Deterministic: the delete has queued behind the open batch.
    wait_for_queued_delete(&publisher).await;

    // Snapshots are taken while the CAS is blocked but asserted only
    // after the gate is released: a regression then fails the test
    // instead of hanging runtime teardown on the never-released gate.
    let single_worker_while_blocked = single_live_worker(&publisher);
    // With the queued batch still blocked at its WAL put, outlast the
    // pacing interval: the delete must still not have run.
    wait_past_publish_pacing().await;
    let (deleted_while_blocked, delete_queued_while_blocked) = {
        let state = publisher_state(&publisher);
        (
            matches!(state.admission, PublisherAdmissionState::Deleted),
            matches!(state.queue.back(), Some(WorkItem::Delete(_))),
        )
    };

    // Released: the parked commit publishes, then the queued batch, and
    // only then the delete.
    store.release();
    assert!(
        single_worker_while_blocked,
        "a delete must not spawn a racing second worker"
    );
    assert!(
        !deleted_while_blocked,
        "delete executed while the queued batch was still publishing"
    );
    assert!(
        delete_queued_while_blocked,
        "delete must stay queued behind the admitted batch"
    );
    let before_response = before
        .await
        .expect("before submit task")
        .expect("parked commit publishes before the delete");
    assert_eq!(before_response.committed_seq, ChangeSeq(1));
    let second_response = second
        .await
        .expect("second submit task")
        .expect("queued batch publishes before the delete");
    assert_eq!(second_response.committed_seq, ChangeSeq(2));
    let delete_response = delete
        .await
        .expect("delete task")
        .expect("delete succeeds after the queued batch");
    assert_eq!(delete_response.head_seq, ChangeSeq(2));
    writer.shutdown().await.expect("drain settles both units");
}

#[tokio::test]
async fn maintenance_invalidation_leaves_the_writer_tail_cached() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = test_writer_with_cache(store, MetadataCache::default(), recorder.clone()).await;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let namespaces = publish_once_into_each(&writer, std::slice::from_ref(&namespace_id)).await;
    let replays = counter(&recorder, "loonfs.publisher.tail_replays");

    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    maintenance.invalidate_namespace(&namespace_id);
    namespaces[0]
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "after", "after",
        )))
        .await
        .expect("publish after invalidation");

    assert_eq!(
        counter(&recorder, "loonfs.publisher.tail_replays"),
        replays,
        "maintenance drops the read anchor, not the tail the writer publishes from"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_writer_and_its_reader_share_one_counted_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let writer = test_writer_with_cache(
        store,
        MetadataCache::default(),
        Arc::new(DefaultMetricsRecorder::new()),
    )
    .await;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let _namespaces = publish_once_into_each(&writer, std::slice::from_ref(&namespace_id)).await;
    writer
        .read_only()
        .namespace(&namespace_id)
        .stat("/docs")
        .await
        .expect("read the published directory");

    let stats = writer.metadata_cache().stats();
    assert_eq!(
        stats.wal_tail_inserts, 1,
        "the publish inserts its tail once"
    );
    assert_eq!(stats.wal_tail_misses, 0, "the read starts from that tail");
    let head_state = &writer.core.inner.head_state;
    let anchor = head_state
        .peek_anchor(&namespace_id)
        .expect("the publish seeds the anchor");
    let tail = head_state
        .get_tail(&loonfs_core::cache::WalTailProjectionCacheKey {
            namespace_id: namespace_id.clone(),
            manifest_no: anchor.basis.manifest_no(),
            head_seq: anchor.head.seq,
        })
        .expect("the anchor names the writer's tail");
    assert_eq!(
        Arc::strong_count(&tail),
        2,
        "between publishes only the cache and this test hold the tail"
    );
    writer.core.invalidate_namespace_read_cache(&namespace_id);
    assert_eq!(
        writer.metadata_cache().stats().head_state_bytes,
        tail.decoded_bytes(),
        "with the anchor gone, the cache counts the shared tail once"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writer_tails_stay_within_the_head_state_budget() {
    const NAMESPACES: usize = 8;
    const ADMITTED: usize = 2;

    let budget_bytes = one_namespace_head_state_bytes().await * ADMITTED;
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = test_writer_with_cache(
        store,
        MetadataCache::builder()
            .max_head_state_bytes(budget_bytes)
            .build(),
        recorder.clone(),
    )
    .await;
    let namespaces = test_namespaces(NAMESPACES);

    let namespace_writers = publish_once_into_each(&writer, &namespaces).await;

    let stats = writer.metadata_cache().stats();
    assert!(
        stats.head_state_bytes <= budget_bytes,
        "writer tails must fit the head-state budget of {budget_bytes}: {stats:?}"
    );
    assert!(stats.head_state_evictions > 0, "{stats:?}");
    assert_eq!(
        counter(&recorder, "loonfs.publisher.tail_replays"),
        u64::try_from(NAMESPACES).expect("small count"),
        "each session's first publish has no tail to start from"
    );

    for (namespace, replays) in [
        (&namespace_writers[NAMESPACES - 1], 0),
        (&namespace_writers[0], 1),
    ] {
        let before = counter(&recorder, "loonfs.publisher.tail_replays");
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                "again", "again",
            )))
            .await
            .expect("commit");
        assert_eq!(
            counter(&recorder, "loonfs.publisher.tail_replays") - before,
            replays,
            "only the least recently published namespace replays its tail"
        );
    }

    writer
        .shutdown()
        .await
        .expect("drain settles every publisher");
}

#[tokio::test]
async fn a_publish_past_the_publish_budget_counts_a_tail_replay() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let mut writer =
        test_writer_with_cache(store, MetadataCache::default(), recorder.clone()).await;
    let timer = Arc::new(ManualMonotonicTimer::default());
    writer.mode.publisher.timer = timer.clone();
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("bootstrap");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    for (commit_id, now_ms) in [
        ("first", 0),
        ("second", loonfs_core::limits::WAL_PUBLISH_BUDGET_MS + 1_000),
    ] {
        timer.set(now_ms);
        namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(
                commit_id, commit_id,
            )))
            .await
            .expect("commit");
    }
    assert_eq!(
        counter(&recorder, "loonfs.publisher.tail_replays"),
        2,
        "the engine drops a projection older than the publish budget and rereads the tail"
    );

    writer
        .shutdown()
        .await
        .expect("drain settles every publisher");
}

/// What one namespace's head anchor and tail weigh after a single publish, so
/// a budget can be stated in whole namespaces instead of a guessed constant.
async fn one_namespace_head_state_bytes() -> usize {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let writer = test_writer(store).await;
    let _namespace_writers = publish_once_into_each(&writer, &test_namespaces(1)).await;
    writer.metadata_cache().stats().head_state_bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_zero_head_state_limit_keeps_no_writer_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = test_writer_with_cache(
        store,
        MetadataCache::builder()
            .max_segment_bytes(0)
            .max_head_state_bytes(0)
            .build(),
        recorder.clone(),
    )
    .await;
    let registry = writer.mode.publisher.clone();
    let namespaces = test_namespaces(3);

    let namespace_writers = publish_once_into_each(&writer, &namespaces).await;
    namespace_writers[0]
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "again", "again",
        )))
        .await
        .expect("commit");

    assert_eq!(writer.metadata_cache().stats().head_state_bytes, 0);
    assert_eq!(
        counter(&recorder, "loonfs.publisher.tail_replays"),
        u64::try_from(namespaces.len() + 1).expect("small count"),
        "with nothing kept, every publish reads its tail from the store"
    );
    assert_eq!(
        registry.shared.lock_state().sessions.len(),
        namespaces.len(),
        "sessions and their publishers survive caches being off"
    );

    writer
        .shutdown()
        .await
        .expect("drain settles every publisher");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_shares_admission_and_publication_slots_after_caller_cancellation() {
    let temp_dir = tempdir().expect("tempdir");
    let a = NamespaceId::parse("a").expect("namespace");
    let b = NamespaceId::parse("b").expect("namespace");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &a));
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("bounded-writer")
        .publication_limits(crate::PublicationLimits {
            max_requests: NonZeroUsize::new(2).expect("two requests"),
            max_requests_per_namespace: NonZeroUsize::new(1).expect("one request per namespace"),
            max_concurrent_publications: NonZeroUsize::new(1).expect("one publication"),
            ..crate::PublicationLimits::default()
        })
        .build()
        .await
        .expect("writer");
    for namespace in [&a, &b] {
        writer
            .create_namespace(namespace, &loonfs_test_support::test_actor())
            .await
            .expect("bootstrap");
    }
    let registry = writer.mode.publisher.clone();
    let writer_a = writer.open_namespace(&a).expect("open namespace");
    let writer_b = writer.open_namespace(&b).expect("open namespace");
    store.block_next();
    let first = {
        let writer_a = writer_a.clone();
        tokio::spawn(async move {
            writer_a
                .commit_candidate(CommitCandidate::new(create_directory_request(
                    "first", "first",
                )))
                .await
        })
    };
    store.wait_until_blocked().await;
    let second = {
        let writer_b = writer_b.clone();
        tokio::spawn(async move {
            writer_b
                .commit_candidate(CommitCandidate::new(create_directory_request(
                    "second", "second",
                )))
                .await
        })
    };
    let publisher = writer_b.session().publisher.clone();
    wait_for_queued_candidates(&publisher, 1).await;
    assert_eq!(
        registry.shared.admission.publications.available_permits(),
        0
    );
    assert!(
        publisher.engine.lock().await.engine.is_none(),
        "waiting for a slot must not load the other namespace's engine"
    );
    first.abort();
    let _ = first.await;
    assert_eq!(
        registry.shared.admission.used_requests(),
        2,
        "disconnected work remains charged"
    );
    let error = writer_a
        .session()
        .submit_delete(DeleteNamespaceOptions::default())
        .await
        .expect_err("budget remains full");
    assert_eq!(error.code(), ErrorCode::CommitQueueFull);
    store.release();
    second
        .await
        .expect("second task")
        .expect("second publication");
    writer.shutdown().await.expect("drain");
    assert_eq!(registry.shared.admission.used_requests(), 0);
    assert_eq!(
        registry.shared.admission.publications.available_permits(),
        1
    );
}

mod inline_writer;
mod session_compaction;

impl loonfs_core::time::WallClock for ManualMonotonicTimer {
    fn now_ms(&self) -> Result<u64, crate::CoreError> {
        Ok(self.0.load(AtomicOrdering::SeqCst))
    }
}
