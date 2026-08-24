//! Publisher batching, shutdown, retry, and observability tests.

#![allow(clippy::panic)]
// Publisher tests use panic in async result helpers for precise diagnostics.

use super::*;
use crate::config::ReadConfig;
use crate::content_tokens::ContentTokenError;
use crate::fs::WriterIdentity;
use crate::maintenance_runner::MaintenanceRunner;
use crate::metrics::{DefaultMetricsRecorder, MetricValue, RuntimeInstruments};
use crate::publish::{CommitRequest, ContentPreparationError, FilesystemOperation};
use crate::{
    BeginUploadRequest, CreateNamespaceOptions, ErrorCode, RuntimeCacheConfig,
    SharedObjectStore as SharedStore, TraceMode, TraceStoreKind,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::wire::wal::decode_wal_segment_envelope_zstd;
use loonfs_api::{AbsolutePath, ActorId, ActorRef, ChangeSeq, DestinationBehavior};
use loonfs_objectstore::keys::{wal_head, wal_segment_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use std::path::Path;
use std::sync::Condvar;
use tempfile::tempdir;
use tokio::time::timeout;

fn blocking_head_cas_store(
    root: impl AsRef<Path>,
    namespace_id: &NamespaceId,
) -> BlockingStore<LocalFsStore> {
    BlockingStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::wal_head(namespace_id),
        OperationClass::CompareAndSwap,
    )
}

#[derive(Debug)]
struct PanicHeadCasStore {
    inner: LocalFsStore,
    head_key: String,
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

impl PanicHeadCasStore {
    fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            head_key: wal_head(namespace_id),
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
        .expect("wait for blocked head CAS");
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
impl ObjectStore for PanicHeadCasStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.head_key && matches!(mode, PutMode::CompareAndSwap { .. }) {
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
            .expect("head CAS gate task");
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

fn lost_head_cas_ack_store(
    root: impl AsRef<Path>,
    namespace_id: &NamespaceId,
) -> FailStore<LocalFsStore> {
    FailStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::wal_head(namespace_id),
        OperationClass::CompareAndSwap,
        InjectedError::Transport("injected lost head CAS acknowledgement".to_owned()),
    )
    .apply_then_fail()
}

fn test_read_core(store: SharedStore) -> ReadCore {
    ReadCore::open(
        store,
        ReadConfig {
            max_read_content_bytes: None,
            runtime_cache: RuntimeCacheConfig::default(),
            trace_mode: TraceMode::Remote,
            trace_store_kind: TraceStoreKind::LocalFs,
        },
        None,
        None,
        RuntimeInstruments::new(None),
    )
}

fn test_writer_bits() -> Arc<WriterBits> {
    Arc::new(WriterBits {
        identity: WriterIdentity::new("writer-a".to_owned()).expect("valid writer identity"),
        maintenance: MaintenanceRunner::new(
            crate::FsBackgroundWork::ManualOnly,
            None,
            std::num::NonZeroUsize::new(1).expect("nonzero"),
            RuntimeInstruments::new(None),
        ),
        publish_observer: None,
    })
}

/// A read core plus the writer bits a standalone publisher publishes
/// under. The caller keeps both alive; the publisher holds the bits weakly.
struct TestRuntime {
    core: ReadCore,
    bits: Arc<WriterBits>,
}

fn test_runtime(store: SharedStore) -> TestRuntime {
    TestRuntime {
        core: test_read_core(store),
        bits: test_writer_bits(),
    }
}

async fn test_writer(store: SharedStore) -> crate::FsWriter {
    test_writer_with_interval(store, crate::config::DEFAULT_MIN_PUBLISH_INTERVAL_MS).await
}

async fn test_writer_with_interval(
    store: SharedStore,
    min_publish_interval_ms: u64,
) -> crate::FsWriter {
    crate::FsWriter::builder_with_store(store)
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
    runtime_cache: RuntimeCacheConfig,
    recorder: Arc<DefaultMetricsRecorder>,
) -> crate::FsWriter {
    crate::FsWriter::builder_with_store(store)
        .writer_id("writer-a")
        .min_publish_interval_ms(0)
        .runtime_cache(runtime_cache)
        .metrics_recorder(recorder)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(TraceStoreKind::LocalFs)
        .build()
        .await
        .expect("build writer")
}

fn gauge(recorder: &DefaultMetricsRecorder, name: &str) -> i64 {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name(name)
        .next()
        .unwrap_or_else(|| panic!("no `{name}` gauge registered"));
    match entry.value {
        MetricValue::Gauge(value) => value,
        ref other => panic!("expected a gauge, found {other:?}"),
    }
}

fn retained_projections(registry: &PublisherRegistry) -> RetainedProjectionTotals {
    registry.shared.lock_state().projections.totals()
}

/// The namespaces whose projections the registry still holds, least
/// recently published first.
fn retained_namespaces(registry: &PublisherRegistry) -> Vec<NamespaceId> {
    registry
        .shared
        .lock_state()
        .projections
        .entries
        .iter()
        .map(|entry| entry.namespace_id.clone())
        .collect()
}

/// Bootstraps `namespaces` under `writer` and publishes one directory into
/// each, in order, so the registry's retention order is the namespace order.
async fn publish_once_into_each(writer: &crate::FsWriter, namespaces: &[NamespaceId]) {
    let registry = writer.publisher();
    for namespace_id in namespaces {
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("bootstrap");
        registry
            .submit_candidate(
                namespace_id.clone(),
                CommitCandidate::new(create_directory_request("seed", "docs")),
            )
            .await
            .expect("commit");
    }
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
        .bootstrap_namespace(loonfs_core::BootstrapOptions {
            allow_existing: false,
        })
        .await
        .expect("bootstrap");
}

/// Pacing for standalone test publishers, long enough that
/// `wait_past_cas_pacing` outlasting it is meaningful.
const TEST_STANDALONE_PACING: Duration = Duration::from_secs(1);

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
        .is_some_and(|worker| !worker.liveness.is_finished())
}

/// Yields until the publisher's queue holds at least `expected` candidates
/// the worker has not taken yet.
async fn wait_for_queued_candidates(publisher: &NamespacePublisher, expected: usize) {
    while queued_candidates(&publisher_state(publisher)) < expected {
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
    NamespacePublisher::new(
        namespace_id.clone(),
        runtime.core.clone(),
        Arc::downgrade(&runtime.bits),
        Weak::new(),
        TEST_STANDALONE_PACING,
        runtime.core.trace_mode(),
        runtime.core.trace_store_kind(),
    )
}

#[allow(clippy::disallowed_methods)]
async fn wait_past_cas_pacing() {
    // Deliberate wall-clock wait past the per-namespace CAS pacing
    // interval. A work loop that were not single-flight would let a
    // racing second task release a queued delete after exactly that
    // interval, so outlasting it proves the delete is ordered behind
    // the sealed batch, not merely paced behind it.
    tokio::time::sleep(TEST_STANDALONE_PACING + Duration::from_millis(300)).await;
}

/// One directory creation directly under the root, named by the directory
/// the test wants: the cheapest mutation that is distinct per name.
fn create_directory_request(
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

#[allow(clippy::disallowed_methods)]
// This timestamp is used only by publisher wait metrics.
fn try_admit_candidate(
    publisher: &NamespacePublisher,
    namespace_id: &NamespaceId,
    candidate: CommitCandidate,
) -> Result<oneshot::Receiver<CommitResult>, CoreError> {
    let commit_id = candidate.commit_id().clone();
    let semantic_identity = candidate.semantic_identity(namespace_id)?;
    let (sender, receiver) = oneshot::channel();
    publisher.admit(
        commit_id,
        candidate,
        semantic_identity,
        sender,
        Instant::now(),
    )?;
    Ok(receiver)
}

async fn recv_commit(receiver: oneshot::Receiver<CommitResult>, label: &str) -> ApiCommitResponse {
    receiver
        .await
        .unwrap_or_else(|err| panic!("{label} receiver dropped: {err}"))
        .unwrap_or_else(|err| panic!("{label} failed: {err}"))
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
    assert_eq!(usize_to_u64(7), 7);
}

#[tokio::test]
#[allow(clippy::disallowed_methods)]
// Monotonic time is used only by publisher wait metrics in this test.
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
            waiters: vec![sender],
        },
    );
    let selected_at = Instant::now();
    publisher.deliver_batch_results(
        vec![BatchCandidate {
            commit_id,
            candidate,
            enqueued_at: selected_at,
        }],
        vec![Err(RuntimeError::Bootstrap(
            crate::BootstrapNamespaceError::NamespaceAlreadyExists {
                namespace_id: namespace_id.clone(),
            },
        ))],
        selected_at,
    );

    let error = receiver
        .await
        .expect("publisher should deliver the result")
        .expect_err("bootstrap failure should remain an error");
    assert!(matches!(error, RuntimeError::Bootstrap(_)));
    assert_eq!(error.code(), ErrorCode::NamespaceExists);
}

#[tokio::test]
async fn rejected_duplicate_joins_ready_in_flight_primary() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer(store).await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    let registry = writer.publisher();
    let publisher = registry.publisher_for(&namespace_id).expect("publisher");
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
                loonfs_api::ContentId::generate(),
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
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    let registry = writer.publisher();
    let publisher = registry.publisher_for(&namespace_id).expect("publisher");
    let request = create_directory_request("rejected-primary", "rejected-primary");

    let primary = try_admit_candidate(
        &publisher,
        &namespace_id,
        CommitCandidate::rejected(
            request.clone(),
            ContentPreparationError::ContentToken(vec![(
                loonfs_api::ContentId::generate(),
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

/// Admission races a blocked active publication with a pending batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_admits_pending_batch_while_active_publish_blocks() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
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
        .list_prefix(&wal_segment_prefix(
            &loonfs_api::NamespaceId::parse("demo").expect("valid namespace id"),
        ))
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 2);
}

/// Duplicate and conflicting admissions race an active publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_duplicate_active_request_joins_while_conflict_fails() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
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

    let duplicate = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("active", "active"),
    );
    let conflict = try_admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("active", "different-active"),
    );
    // Both claims are still in flight, so there is no receipt to name.
    assert!(matches!(
        conflict,
        Err(CoreError::CommitIdReuseConflict {
            commit_id,
            committed_seq: None,
            committed_fingerprint: None,
        }) if commit_id == "active"
    ));

    store.release();
    let active_response = recv_commit(active, "active").await;
    let duplicate_response = recv_commit(duplicate, "duplicate").await;
    assert_eq!(active_response.committed_seq, ChangeSeq(1));
    assert_eq!(duplicate_response.committed_seq, ChangeSeq(1));
}

/// Distinct and duplicate admissions race a full pending batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_pending_batch_full_rejects_distinct_but_allows_duplicate() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
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

    let mut pending = Vec::with_capacity(MAX_BATCH_CANDIDATES);
    for index in 0..MAX_BATCH_CANDIDATES {
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
    let conflict = try_admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("pending-0", "different-pending"),
    );
    assert!(matches!(
        conflict,
        Err(CoreError::CommitIdReuseConflict {
            commit_id,
            committed_seq: None,
            committed_fingerprint: None,
        }) if commit_id == "pending-0"
    ));

    let overflow = try_admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("overflow", "overflow"),
    );
    assert!(matches!(overflow, Err(CoreError::CommitQueueFull)));

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
}

/// A cold namespace takes whatever has batched — here a full batch
/// admitted before the publish task first runs — immediately, with no
/// coalescing wait in front of the first publication.
#[tokio::test(flavor = "current_thread")]
async fn publisher_takes_a_cold_full_batch_immediately() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    store.block_next();
    let mut receivers = Vec::with_capacity(MAX_BATCH_CANDIDATES);
    for index in 0..MAX_BATCH_CANDIDATES {
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

/// A submission to a cold namespace is taken immediately: after one
/// poll of the publish task there is no open batch parked behind a
/// timer, so a lone candidate never waits out a coalescing window.
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

/// Follow-up submissions inside the pacing interval coalesce and
/// publish no earlier than the interval boundary — the timer gives a
/// deterministic lower bound.
#[tokio::test]
async fn hot_submissions_wait_out_the_pacing_interval() {
    tokio::time::pause();
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer_with_interval(store.clone(), 400).await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    let registry = writer.publisher();

    registry
        .submit_candidate(
            namespace_id.clone(),
            CommitCandidate::new(create_directory_request("warmup", "warmup")),
        )
        .await
        .expect("warmup commit");

    let hot = tokio::spawn({
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::new(create_directory_request("hot", "hot")),
                )
                .await
        }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(399)).await;
    tokio::task::yield_now().await;
    assert!(
        !hot.is_finished(),
        "a follow-up publication must remain paced before the interval boundary"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    hot.await
        .expect("join hot publication")
        .expect("hot commit");
}

/// Receipt replay races a lost head compare-and-swap acknowledgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_resolves_unknown_head_outcome_by_replaying_receipt() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(lost_head_cas_ack_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared);
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    // One clean commit first, so the session holds its writer epoch. Epoch
    // acquisition is a head compare-and-swap too, and this test is about the
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

/// Queued publication races a publication whose panic the worker contains.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_survives_publish_panic_and_keeps_serving() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(PanicHeadCasStore::new(temp_dir.path(), &namespace_id));
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

    // The panic may have struck either side of the head CAS, so the
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

/// Delete admission races active, pending, and later mutation work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_barrier_publishes_admitted_work_and_rejects_later_work() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    // A publishes and blocks at its head CAS; B queues behind it.
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

/// A second delete races the first one's head compare-and-swap.
///
/// Both callers must be answered. The second delete is a queue item of its
/// own, so the tombstone sweep settles it exactly like any other work that
/// queued behind a landed delete — where the sealed-batch design stranded
/// its waiters forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_delete_during_inflight_delete_settles_both() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let runtime = test_runtime(shared.clone());
    create_namespace(&runtime, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &runtime);

    // One publication first, so the session already holds its writer epoch:
    // the next head compare-and-swap is the delete's own tombstone swap.
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

/// Two deletes pending at the queue tail with equal options are one
/// request: one queue item, and both callers share its outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_pending_deletes_coalesce_into_one_outcome() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
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

/// Deletes with different preconditions are different requests: each is
/// its own queue item, and each settles against its own options.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_deletes_with_different_preconditions_settle_separately() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
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

/// A stale delete queued behind a valid one settles as
/// `namespace_deleted` — it never inherits the other delete's success.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_pending_delete_does_not_share_the_first_deletes_success() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
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

/// Commit admission races a delete already queued behind an in-flight
/// publication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mutations_admitted_after_a_queued_delete_wait_behind_it() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
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

/// Concurrent submissions race to share one pending publication batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_batches_concurrent_distinct_commits_into_one_wal_segment() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    let registry = writer.publisher();

    // Hold the cold publication in flight so both concurrent submissions are
    // deterministically admitted to the pending batch behind it.
    store.block_next();
    let warmup = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::new(create_directory_request("warmup", "warmup")),
                )
                .await
        })
    };
    store.wait_until_blocked().await;

    let actor_a = ActorRef::user(ActorId::parse("user-a").expect("actor id"));
    let actor_b = ActorRef::service(ActorId::parse("service-b").expect("actor id"));
    let mut request_a = create_directory_request("req-a", "alpha");
    request_a.actor = actor_a.clone();
    let mut request_b = create_directory_request("req-b", "beta");
    request_b.actor = actor_b.clone();
    let response_a = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(namespace_id, CommitCandidate::new(request_a))
                .await
        })
    };
    let response_b = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(namespace_id, CommitCandidate::new(request_b))
                .await
        })
    };
    let publisher = registry.publisher_for(&namespace_id).expect("publisher");
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
    // one segment.
    let wal_keys = shared
        .list_prefix(&wal_segment_prefix(
            &loonfs_api::NamespaceId::parse("demo").expect("valid namespace id"),
        ))
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 2);

    let mut batched_actors = std::collections::BTreeMap::new();
    for key in &wal_keys {
        let bytes = shared
            .get(key, None)
            .await
            .expect("read WAL segment")
            .expect("WAL segment exists");
        let segment = decode_wal_segment_envelope_zstd(&bytes).expect("decode WAL segment");
        if segment.payload.records.len() == 2 {
            for record in segment.payload.records {
                batched_actors.insert(record.commit_id.to_string(), record.committed_by);
            }
        }
    }
    assert_eq!(batched_actors.get("req-a"), Some(&actor_a));
    assert_eq!(batched_actors.get("req-b"), Some(&actor_b));

    let changes = writer
        .reader()
        .list_changes(
            &namespace_id,
            ChangeSeq(0),
            crate::ListChangesOptions::default(),
        )
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

/// A content-free submission and a submission carrying prepared content
/// race to share one publication batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_batches_plain_and_prepared_mutations_together() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    let upload = writer
        .create_upload(&namespace_id, BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    let staged = writer
        .put_upload_content(&namespace_id, upload.upload_id(), b"hello")
        .await
        .expect("stage content");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&shared, &namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared_content =
        loonfs_core::content::prepare_existing_content_ref(&shared, &catalog, staged.content_ref)
            .await
            .expect("prepare existing content");
    let registry = writer.publisher();

    // Hold the cold publication in flight so both concurrent submissions are
    // deterministically admitted to the pending batch behind it.
    store.block_next();
    let warmup = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::new(create_directory_request("warmup", "warmup")),
                )
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
            content_ref: prepared_content.content_ref().clone(),
            behavior: DestinationBehavior::NoReplace,
            expected_revision_no: None,
        },
    );
    let plain_response = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(namespace_id, CommitCandidate::new(plain))
                .await
        })
    };
    let prepared_response = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::prepared(prepared, vec![prepared_content]),
                )
                .await
        })
    };
    let publisher = registry.publisher_for(&namespace_id).expect("publisher");
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
        .list_prefix(&wal_segment_prefix(
            &loonfs_api::NamespaceId::parse("demo").expect("valid namespace id"),
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
        let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode wal segment");
        record_counts.push(segment.payload.records.len());
    }
    record_counts.sort_unstable();
    // The warmup published alone; the concurrent pair shares a segment.
    assert_eq!(record_counts, vec![1, 2]);
}

/// Admission closure races an already-blocked publication draining.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_close_admission_refuses_new_work_while_admitted_work_drains() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    let registry = writer.publisher();

    // An admitted publication blocks at its head CAS...
    store.block_next();
    let active = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::new(create_directory_request("active", "active")),
                )
                .await
        })
    };
    store.wait_until_blocked().await;

    // Admission then closes, and new work is refused.
    registry.close_admission();
    let refused = registry
        .submit_candidate(
            namespace_id.clone(),
            CommitCandidate::new(create_directory_request("refused", "refused")),
        )
        .await
        .expect_err("submission after close_admission");
    assert_eq!(refused.code(), ErrorCode::ShuttingDown);

    // A publisher clone that predates the sweep also refuses directly.
    let publisher = registry
        .shared
        .lock_state()
        .publishers
        .get(&namespace_id)
        .expect("active publisher exists")
        .clone();
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
    registry.drain().await.expect("drain settles publish tasks");
    assert!(registry
        .shared
        .lock_state()
        .publishers
        .values()
        .all(|publisher| publisher_state(publisher).worker.is_none()));
}

/// Registry drain races a contained panic and the queue behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_survives_panic_and_processes_later_queue_items() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(PanicHeadCasStore::new(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    let registry = writer.publisher();

    store.arm_blocking_panic();
    let doomed = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::new(create_directory_request("doomed", "doomed")),
                )
                .await
        })
    };
    store.wait_until_blocked().await;

    // Queued behind the doomed batch: only a worker that survives the panic
    // publishes this one, and the drain must wait for it.
    let queued = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::new(create_directory_request("queued", "queued")),
                )
                .await
        })
    };
    let publisher = registry
        .shared
        .lock_state()
        .publishers
        .get(&namespace_id)
        .expect("publisher exists while blocked")
        .clone();
    wait_for_queued_candidates(&publisher, 1).await;

    store.release_into_panic();
    registry.close_admission();

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

    let drain_error = registry
        .drain()
        .await
        .expect_err("drain surfaces the contained panic");
    assert!(
        drain_error.to_string().contains("panicked"),
        "drain reports panicked publisher tasks: {drain_error}"
    );
}

/// Publisher eviction races the delete barrier's terminal transition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successful_delete_evicts_the_namespace_publisher() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer(store.clone()).await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    let registry = writer.publisher();

    registry
        .submit_candidate(
            namespace_id.clone(),
            CommitCandidate::new(create_directory_request("before", "before")),
        )
        .await
        .expect("commit before delete");
    assert_eq!(registry.shared.lock_state().publishers.len(), 1);

    registry
        .submit_delete(namespace_id.clone(), DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");
    assert!(
        registry.shared.lock_state().publishers.is_empty(),
        "a terminal publisher must not stay in the map"
    );

    // A later submission builds a fresh publisher and still fails, now
    // on the durable tombstone instead of the fast in-memory flag.
    let late = registry
        .submit_candidate(
            namespace_id.clone(),
            CommitCandidate::new(create_directory_request("late", "late")),
        )
        .await
        .expect_err("submission after delete");
    assert_eq!(late.code(), ErrorCode::NamespaceDeleted);
    registry.close_admission();
    registry.drain().await.expect("drain after delete");
}

#[tokio::test(flavor = "current_thread")]
async fn close_admission_refuses_without_creating_publishers() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer(store.clone()).await;
    let registry = writer.publisher();

    registry.close_admission();
    let refused = registry
        .submit_candidate(
            namespace_id.clone(),
            CommitCandidate::new(create_directory_request("nope", "nope")),
        )
        .await
        .expect_err("closed registry refuses commits");
    assert_eq!(refused.code(), ErrorCode::ShuttingDown);
    let refused_delete = registry
        .submit_delete(namespace_id, DeleteNamespaceOptions::default())
        .await
        .expect_err("closed registry refuses deletes");
    assert_eq!(refused_delete.code(), ErrorCode::ShuttingDown);
    assert!(registry.shared.lock_state().publishers.is_empty());
    registry.drain().await.expect("nothing to drain");
}

/// A delete admitted before the shutdown sweep is the worker's to finish, so
/// it still lands after admission closes — and it lands terminally. Deleted
/// outranks closed: the publisher answers later work with the namespace's
/// tombstone, not with the shutdown that raced it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_delete_admitted_before_close_admission_lands_terminal() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    let registry = writer.publisher();

    // A publication parks at its head CAS, so the delete deterministically
    // queues behind it instead of being taken first.
    store.block_next();
    let active = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::new(create_directory_request("active", "active")),
                )
                .await
        })
    };
    store.wait_until_blocked().await;
    let publisher = registry
        .shared
        .lock_state()
        .publishers
        .get(&namespace_id)
        .cloned()
        .expect("publisher exists once a publish is in flight");
    let delete = spawn_delete(&publisher, DeleteNamespaceOptions::default());
    wait_for_queued_delete(&publisher).await;

    // Admission closes with the delete already admitted; releasing the gate
    // lets the batch and then the delete publish.
    registry.close_admission();
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
    registry.drain().await.expect("drain settles the delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_queued_mid_publish_waits_behind_admitted_work() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    let registry = writer.publisher();

    // Park the first publication at its head CAS, batch a second commit
    // behind it, then queue the delete: the queued batch must publish
    // before the delete runs, and the blocked CAS outlasts the pacing
    // interval — the interleaving where a racing second worker could run
    // the delete first.
    store.block_next();
    let before = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::new(create_directory_request("before", "before")),
                )
                .await
        })
    };
    store.wait_until_blocked().await;
    let publisher = registry
        .shared
        .lock_state()
        .publishers
        .get(&namespace_id)
        .cloned()
        .expect("publisher exists once a publish is in flight");

    // The worker is parked in the blocked CAS, so this admission
    // deterministically queues the next batch instead of being taken.
    let second = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::new(create_directory_request("second", "second")),
                )
                .await
        })
    };
    wait_for_queued_candidates(&publisher, 1).await;

    let delete = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_delete(namespace_id, DeleteNamespaceOptions::default())
                .await
        })
    };
    // Deterministic: the delete has queued behind the open batch.
    wait_for_queued_delete(&publisher).await;

    // Snapshots are taken while the CAS is blocked but asserted only
    // after the gate is released: a regression then fails the test
    // instead of hanging runtime teardown on the never-released gate.
    let single_worker_while_blocked = single_live_worker(&publisher);
    // With the queued batch still blocked at its head CAS, outlast the
    // pacing interval: the delete must still not have run.
    wait_past_cas_pacing().await;
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
    registry.close_admission();
    registry.drain().await.expect("drain settles both units");
}

/// Retained publish-tail projections are bounded across namespaces, not only
/// one at a time: a writer that publishes to far more namespaces than the
/// budget admits keeps a projection for the most recently published ones and
/// nothing for the rest. The publishers stay — each one carries the writer
/// session that nothing may evict — but they carry no tail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_tail_projections_stay_within_the_namespace_count_cap() {
    const NAMESPACES: usize = 12;
    const RETAINED: usize = 3;

    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = test_writer_with_cache(
        store,
        RuntimeCacheConfig {
            max_cached_namespaces: RETAINED,
            ..RuntimeCacheConfig::default()
        },
        recorder.clone(),
    )
    .await;
    let registry = writer.publisher();
    let namespaces = test_namespaces(NAMESPACES);

    publish_once_into_each(&writer, &namespaces).await;

    let totals = retained_projections(&registry);
    assert_eq!(
        totals.projections, RETAINED,
        "the count cap bounds retained projections across namespaces"
    );
    assert!(
        totals.rows > 0 && totals.decoded_bytes > 0,
        "the surviving projections must be real ones: {totals:?}"
    );
    assert_eq!(
        retained_namespaces(&registry),
        namespaces[NAMESPACES - RETAINED..].to_vec(),
        "eviction takes the least recently published namespaces first"
    );
    assert_eq!(
        registry.shared.lock_state().publishers.len(),
        NAMESPACES,
        "bounding the projections must not evict a publisher or its session"
    );

    assert_eq!(
        gauge(&recorder, "loonfs.publisher.retained_projections"),
        i64::try_from(RETAINED).expect("small count"),
        "the gauge reports what the registry retains"
    );
    assert_eq!(
        gauge(&recorder, "loonfs.publisher.retained_projection_bytes"),
        i64::try_from(totals.decoded_bytes).expect("small byte total"),
    );

    registry.close_admission();
    registry
        .drain()
        .await
        .expect("drain settles every publisher");
}

/// The row and decoded-byte budgets bound the publish side in aggregate, the
/// same meaning the read-side projection cache gives them. A projection that
/// fits the per-projection ceiling on its own is still evicted once the
/// namespaces together outgrow the budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_tail_projections_stay_within_the_shared_byte_budget() {
    const NAMESPACES: usize = 8;
    const ADMITTED: usize = 2;

    let budget_bytes = one_projection_decoded_bytes().await * ADMITTED;
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = test_writer_with_cache(
        store,
        RuntimeCacheConfig {
            max_cached_wal_tail_projection_decoded_bytes: budget_bytes,
            ..RuntimeCacheConfig::default()
        },
        recorder.clone(),
    )
    .await;
    let registry = writer.publisher();
    let namespaces = test_namespaces(NAMESPACES);

    publish_once_into_each(&writer, &namespaces).await;

    let totals = retained_projections(&registry);
    assert!(
        totals.decoded_bytes <= budget_bytes,
        "retained projections must fit the shared byte budget of {budget_bytes}: {totals:?}"
    );
    assert!(
        (1..=ADMITTED).contains(&totals.projections),
        "the budget admits {ADMITTED} projections of this size, no more: {totals:?}"
    );
    assert_eq!(
        gauge(&recorder, "loonfs.publisher.retained_projection_bytes"),
        i64::try_from(totals.decoded_bytes).expect("small byte total"),
    );

    registry.close_admission();
    registry
        .drain()
        .await
        .expect("drain settles every publisher");
}

/// What one namespace's tail projection weighs after a single publish, so a
/// budget can be stated in whole projections instead of a guessed constant.
async fn one_projection_decoded_bytes() -> usize {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let writer = test_writer(store).await;
    publish_once_into_each(&writer, &test_namespaces(1)).await;
    let totals = retained_projections(&writer.publisher());
    assert_eq!(totals.projections, 1, "one publish retains one projection");
    totals.decoded_bytes
}

/// Caches off is the one mode that keeps nothing: every publish drops its
/// projection where it was built, so the registry has none to account for
/// and none to evict.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_runtime_caches_retain_no_tail_projections() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer =
        test_writer_with_cache(store, RuntimeCacheConfig::disabled(), recorder.clone()).await;
    let registry = writer.publisher();
    let namespaces = test_namespaces(3);

    publish_once_into_each(&writer, &namespaces).await;

    assert_eq!(
        retained_projections(&registry),
        RetainedProjectionTotals::default(),
        "a diagnostic run retains nothing to bound"
    );
    assert_eq!(gauge(&recorder, "loonfs.publisher.retained_projections"), 0);
    assert_eq!(
        registry.shared.lock_state().publishers.len(),
        namespaces.len(),
        "publishers and their sessions survive caches being off"
    );

    registry.close_admission();
    registry
        .drain()
        .await
        .expect("drain settles every publisher");
}

/// A landed delete takes its namespace's projection out of the accounting
/// with the publisher itself: nothing stays charged to an id that can never
/// rebind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_landed_delete_forgets_the_namespace_projection() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer =
        test_writer_with_cache(store, RuntimeCacheConfig::default(), recorder.clone()).await;
    let registry = writer.publisher();
    let namespaces = test_namespaces(2);

    publish_once_into_each(&writer, &namespaces).await;
    assert_eq!(retained_projections(&registry).projections, 2);

    registry
        .submit_delete(namespaces[0].clone(), DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");

    assert_eq!(
        retained_namespaces(&registry),
        vec![namespaces[1].clone()],
        "the deleted namespace leaves no accounting behind"
    );
    assert_eq!(gauge(&recorder, "loonfs.publisher.retained_projections"), 1);

    registry.close_admission();
    registry
        .drain()
        .await
        .expect("drain settles every publisher");
}

/// A namespace that is publishing keeps its engine, so a sweep skips it —
/// and must keep it accounted rather than record an eviction that never
/// happened. The next publish sweeps again and the skipped namespace loses
/// its projection then.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_skipped_eviction_leaves_the_namespace_accounted() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = test_writer_with_cache(
        store,
        RuntimeCacheConfig {
            max_cached_namespaces: 1,
            ..RuntimeCacheConfig::default()
        },
        recorder.clone(),
    )
    .await;
    let registry = writer.publisher();
    let namespaces = test_namespaces(2);
    let (busy, other) = (namespaces[0].clone(), namespaces[1].clone());

    publish_once_into_each(&writer, &namespaces[..1]).await;
    assert_eq!(retained_namespaces(&registry), vec![busy.clone()]);

    // Standing in for a publication in flight: the engine is held, so the
    // sweep the next publish runs cannot take it.
    let busy_publisher = registry
        .shared
        .lock_state()
        .publishers
        .get(&busy)
        .expect("the published namespace has a publisher")
        .clone();
    let held_engine = busy_publisher.engine.lock().await;

    writer
        .create_namespace(&other, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
    registry
        .submit_candidate(
            other.clone(),
            CommitCandidate::new(create_directory_request("seed", "docs")),
        )
        .await
        .expect("commit while the other engine is held");

    let skipped = retained_projections(&registry);
    assert_eq!(
        retained_namespaces(&registry),
        vec![busy.clone(), other.clone()],
        "a skipped eviction must not be recorded as one"
    );
    assert_eq!(
        gauge(&recorder, "loonfs.publisher.retained_projections"),
        i64::try_from(skipped.projections).expect("small count"),
        "the gauge reports the overshoot rather than hiding it"
    );

    drop(held_engine);
    registry
        .submit_candidate(
            other.clone(),
            CommitCandidate::new(create_directory_request("second", "more")),
        )
        .await
        .expect("commit once the engine is free");

    assert_eq!(
        retained_namespaces(&registry),
        vec![other],
        "the next sweep evicts what the previous one skipped"
    );

    registry.close_admission();
    registry
        .drain()
        .await
        .expect("drain settles every publisher");
}
