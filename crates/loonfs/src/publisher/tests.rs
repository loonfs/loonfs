//! Publisher batching, shutdown, retry, and observability tests.

#![allow(clippy::panic)]
// Publisher tests use panic in async result helpers for precise diagnostics.

use super::*;
use crate::background::BackgroundWork;
use crate::config::FsConfig;
use crate::content_tokens::ContentTokenError;
use crate::publish::{ContentPreparationError, NamespaceMutation};
use crate::{
    BeginUploadRequest, CreateNamespaceOptions, ErrorCode, RuntimeCacheConfig,
    SharedObjectStore as SharedStore, TraceMode, TraceStoreKind,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::v0::{CommitOp, CommitRequest};
use loonfs_api::wire::wal::decode_wal_segment_envelope_zstd;
use loonfs_api::{AbsolutePath, ChangeSeq, DestinationBehavior, InodeId};
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

fn blocking_head_cas_store(
    root: impl AsRef<Path>,
    namespace_id: &NamespaceId,
) -> BlockingStore<LocalFsStore> {
    BlockingStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::wal_head(namespace_id.as_str()),
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
            head_key: wal_head(namespace_id.as_str()),
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

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

fn lost_head_cas_ack_store(
    root: impl AsRef<Path>,
    namespace_id: &NamespaceId,
) -> FailStore<LocalFsStore> {
    FailStore::new(
        LocalFsStore::new(root.as_ref()).expect("store"),
        KeyPredicate::wal_head(namespace_id.as_str()),
        OperationClass::CompareAndSwap,
        InjectedError::Transport("injected lost head CAS acknowledgement".to_owned()),
    )
    .apply_then_fail()
}

fn test_fs(store: SharedStore) -> FsCore {
    FsCore::open_with_background(
        store,
        FsConfig {
            writer_id: "writer-a".to_owned(),
            writer_version: "test".to_owned(),
            // The publisher under test is itself the coalescer; direct
            // windows would only add latency to these tests.
            min_publish_interval_ms: 0,
            max_read_content_bytes: None,
            runtime_cache: RuntimeCacheConfig::default(),
            trace_mode: TraceMode::Remote,
            trace_store_kind: TraceStoreKind::LocalFs,
        },
        BackgroundWork::inert(),
        None,
        None,
    )
    .expect("open runtime")
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
        .writer_version("test")
        .min_publish_interval_ms(min_publish_interval_ms)
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(TraceStoreKind::LocalFs)
        .build()
        .await
        .expect("build writer")
}

async fn create_namespace(fs: &FsCore, namespace_id: &NamespaceId) {
    fs.create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("bootstrap");
}

/// Pacing for standalone test publishers, long enough that
/// `wait_past_cas_pacing` outlasting it is meaningful.
const TEST_STANDALONE_PACING: Duration = Duration::from_secs(1);

/// A publisher with no owning registry, exercising the unowned-task
/// fallback the production paths reserve for a dropped registry. The
/// caller keeps `fs` alive; the publisher holds it weakly.
fn standalone_publisher(namespace_id: &NamespaceId, fs: &FsCore) -> NamespacePublisher {
    NamespacePublisher::new(
        namespace_id.clone(),
        Arc::downgrade(&fs.inner),
        Weak::new(),
        TEST_STANDALONE_PACING,
        fs.inner.config.trace_mode.as_str(),
        fs.inner.config.trace_store_kind.as_str(),
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

fn create_directory_request(
    commit_id: impl Into<String>,
    display_name: impl AsRef<str>,
) -> CommitRequest {
    CommitRequest {
        commit_id: CommitId::parse(commit_id.into()).expect("valid commit id"),
        preconditions: Vec::new(),
        ops: vec![CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse(display_name.as_ref())
                .expect("valid display name"),
        }],
        message: None,
    }
}

fn admit_commit(
    publisher: &NamespacePublisher,
    namespace_id: &NamespaceId,
    request: CommitRequest,
) -> oneshot::Receiver<CommitResult> {
    try_admit_commit(publisher, namespace_id, request).expect("admit commit")
}

fn try_admit_commit(
    publisher: &NamespacePublisher,
    namespace_id: &NamespaceId,
    request: CommitRequest,
) -> Result<oneshot::Receiver<CommitResult>, CoreError> {
    let candidate = NamespaceMutationCandidate::commit(request);
    try_admit_candidate(publisher, namespace_id, candidate)
}

fn try_admit_candidate(
    publisher: &NamespacePublisher,
    namespace_id: &NamespaceId,
    candidate: NamespaceMutationCandidate,
) -> Result<oneshot::Receiver<CommitResult>, CoreError> {
    let commit_id = candidate.commit_id().clone();
    let semantic_identity = candidate.semantic_identity(namespace_id)?;
    let operation_class = operation_class(&semantic_identity);
    let (sender, receiver) = oneshot::channel();
    publisher.admit(
        commit_id,
        candidate,
        semantic_identity,
        sender,
        operation_class,
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
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let commit = NamespaceMutationCandidate::commit(create_directory_request(
        "commit-trace",
        "private-name",
    ));
    let path = NamespaceMutationCandidate::path(PathMutationIntent::CreateDir {
        commit_id: CommitId::parse("path-trace").expect("valid commit id"),
        message: None,
        absolute_path: AbsolutePath::parse("/private/path").expect("path"),
        parents: false,
    });

    assert_eq!(
        operation_class(&commit.semantic_identity(&namespace_id).expect("identity")),
        "explicit_commit"
    );
    assert_eq!(
        operation_class(&path.semantic_identity(&namespace_id).expect("identity")),
        "path_mutation"
    );
    assert_eq!(result_label(&Ok::<_, CoreError>(())), "ok");
    assert_eq!(
        result_label(&Err::<(), _>(CoreError::Internal(
            "private error".to_owned()
        ))),
        "error"
    );
    assert_eq!(usize_to_u64(7), 7);
}

#[tokio::test]
async fn rejected_duplicate_joins_ready_in_flight_primary() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer(store).await;
    create_namespace(writer.core(), &namespace_id).await;
    let registry = writer.publisher();
    let publisher = registry.publisher_for(&namespace_id).expect("publisher");
    let intent = PathMutationIntent::CreateDir {
        commit_id: CommitId::parse("ready-primary").expect("valid commit id"),
        message: None,
        absolute_path: AbsolutePath::parse("/ready-primary").expect("path"),
        parents: false,
    };

    let primary = try_admit_candidate(
        &publisher,
        &namespace_id,
        NamespaceMutationCandidate::path(intent.clone()),
    )
    .expect("admit ready primary");
    let duplicate = try_admit_candidate(
        &publisher,
        &namespace_id,
        NamespaceMutationCandidate::rejected(
            NamespaceMutation::Path(intent),
            ContentPreparationError::ContentToken(ContentTokenError::Expired),
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
    create_namespace(writer.core(), &namespace_id).await;
    let registry = writer.publisher();
    let publisher = registry.publisher_for(&namespace_id).expect("publisher");
    let intent = PathMutationIntent::CreateDir {
        commit_id: CommitId::parse("rejected-primary").expect("valid commit id"),
        message: None,
        absolute_path: AbsolutePath::parse("/rejected-primary").expect("path"),
        parents: false,
    };

    let primary = try_admit_candidate(
        &publisher,
        &namespace_id,
        NamespaceMutationCandidate::rejected(
            NamespaceMutation::Path(intent.clone()),
            ContentPreparationError::ContentToken(ContentTokenError::Expired),
        ),
    )
    .expect("admit rejected primary");
    let duplicate = try_admit_candidate(
        &publisher,
        &namespace_id,
        NamespaceMutationCandidate::path(intent),
    )
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
    let fs = test_fs(shared.clone());
    create_namespace(&fs, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &fs);

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
        let state = publisher
            .state
            .lock()
            .expect("namespace publisher mutex poisoned");
        assert!(state.publishing);
        assert_eq!(
            state
                .batch
                .as_ref()
                .expect("pending batch")
                .candidates
                .len(),
            1
        );
    }

    store.release();
    let active_response = recv_commit(active, "active").await;
    let pending_response = recv_commit(pending, "pending").await;
    assert_eq!(active_response.committed_seq, ChangeSeq(1));
    assert_eq!(pending_response.committed_seq, ChangeSeq(2));

    let wal_keys = shared
        .list_prefix(&wal_segment_prefix("demo"))
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
    let fs = test_fs(shared.clone());
    create_namespace(&fs, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &fs);

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
    assert!(matches!(
        conflict,
        Err(CoreError::CommitIdReuseConflict(commit_id)) if commit_id == "active"
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
    let fs = test_fs(shared.clone());
    create_namespace(&fs, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &fs);

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
        Err(CoreError::CommitIdReuseConflict(commit_id)) if commit_id == "pending-0"
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
    let fs = test_fs(shared.clone());
    create_namespace(&fs, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &fs);

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
        let state = publisher
            .state
            .lock()
            .expect("namespace publisher mutex poisoned");
        assert!(state.publishing);
        assert!(state.batch.is_none());
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
    let fs = test_fs(store);
    create_namespace(&fs, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &fs);

    let receiver = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("cold", "cold"),
    );
    tokio::task::yield_now().await;
    {
        let state = publisher
            .state
            .lock()
            .expect("namespace publisher mutex poisoned");
        assert!(
            state.batch.is_none(),
            "a cold batch must be taken immediately, not held for a coalescing timer"
        );
    }
    let response = recv_commit(receiver, "cold").await;
    assert_eq!(response.committed_seq, ChangeSeq(1));
}

/// Follow-up submissions inside the pacing interval coalesce and
/// publish no earlier than the interval boundary — the timer gives a
/// deterministic lower bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hot_submissions_wait_out_the_pacing_interval() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer_with_interval(store.clone(), 400).await;
    create_namespace(writer.core(), &namespace_id).await;
    let registry = writer.publisher();

    let warmup_started = Instant::now();
    registry
        .submit_commit(
            namespace_id.clone(),
            create_directory_request("warmup", "warmup"),
        )
        .await
        .expect("warmup commit");

    registry
        .submit_commit(namespace_id.clone(), create_directory_request("hot", "hot"))
        .await
        .expect("hot commit");
    let elapsed = warmup_started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(400),
        "a follow-up publication must wait out the pacing interval, took {elapsed:?}"
    );
}

/// Receipt replay races a lost head compare-and-swap acknowledgement.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_resolves_unknown_head_outcome_by_replaying_receipt() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(lost_head_cas_ack_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let fs = test_fs(shared);
    create_namespace(&fs, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &fs);

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
    assert_eq!(response.committed_seq, ChangeSeq(1));
}

/// Queued publication races recovery from a panicked publish task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_survives_publish_task_panic_and_keeps_serving() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(PanicHeadCasStore::new(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let fs = test_fs(shared.clone());
    create_namespace(&fs, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &fs);

    store.arm_blocking_panic();
    let doomed = admit_commit(
        &publisher,
        &namespace_id,
        create_directory_request("doomed", "doomed"),
    );
    store.wait_until_blocked().await;

    // Queued behind the in-flight batch: only the abort guard's respawn
    // can ever publish this one.
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
    let fs = test_fs(shared.clone());
    create_namespace(&fs, &namespace_id).await;
    let publisher = standalone_publisher(&namespace_id, &fs);

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
    // Deterministic: wait until the delete has sealed the open batch.
    loop {
        let sealed = {
            let state = publisher
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            state.pending_delete.is_some()
        };
        if sealed {
            break;
        }
        tokio::task::yield_now().await;
    }
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

/// Concurrent submissions race to share one pending publication batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_batches_concurrent_distinct_commits_into_one_wal_segment() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer_with_interval(store.clone(), 400).await;
    create_namespace(writer.core(), &namespace_id).await;
    let registry = writer.publisher();

    // Warm the namespace: a cold one publishes its first submission
    // immediately, so truly concurrent submissions could split across
    // two publications. Inside the pacing interval the open batch
    // deterministically holds both.
    registry
        .submit_commit(
            namespace_id.clone(),
            create_directory_request("warmup", "warmup"),
        )
        .await
        .expect("warmup commit");

    let request_a = CommitRequest {
        commit_id: CommitId::parse("req-a").expect("valid commit id"),
        preconditions: Vec::new(),
        ops: vec![CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse("alpha").expect("valid display name"),
        }],
        message: None,
    };
    let request_b = CommitRequest {
        commit_id: CommitId::parse("req-b").expect("valid commit id"),
        preconditions: Vec::new(),
        ops: vec![CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse("beta").expect("valid display name"),
        }],
        message: None,
    };

    let (response_a, response_b) = tokio::join!(
        registry.submit_commit(namespace_id.clone(), request_a),
        registry.submit_commit(namespace_id.clone(), request_b)
    );
    assert_eq!(response_a.expect("response a").committed_seq, ChangeSeq(2));
    assert_eq!(response_b.expect("response b").committed_seq, ChangeSeq(3));

    // The warmup published alone; the two concurrent submissions share
    // one segment.
    let wal_keys = store
        .list_prefix(&wal_segment_prefix("demo"))
        .await
        .expect("list wal");
    assert_eq!(wal_keys.len(), 2);
}

/// Explicit and path-intent submissions race to share one publication batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publisher_batches_explicit_commit_and_path_intent_together() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let writer = test_writer_with_interval(store.clone(), 400).await;
    create_namespace(writer.core(), &namespace_id).await;
    let upload = writer
        .begin_upload(
            &namespace_id,
            BeginUploadRequest {
                mode: None,
                content_ref: None,
            },
        )
        .await
        .expect("begin upload");
    let staged = writer
        .upload_content(&namespace_id, &upload.upload_id, b"hello")
        .await
        .expect("stage content");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared_content =
        loonfs_core::content::prepare_existing_content_ref(&store, &catalog, staged.content_ref)
            .await
            .expect("prepare existing content");
    let registry = writer.publisher();

    // Warm the namespace so the pacing interval deterministically holds
    // the two concurrent submissions in one batch.
    registry
        .submit_commit(
            namespace_id.clone(),
            create_directory_request("warmup", "warmup"),
        )
        .await
        .expect("warmup commit");

    let explicit = CommitRequest {
        commit_id: CommitId::parse("explicit-commit").expect("valid commit id"),
        preconditions: Vec::new(),
        ops: vec![CommitOp::CreateDirectory {
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse("alpha").expect("valid display name"),
        }],
        message: None,
    };
    let path_intent = PathMutationIntent::PutFile {
        commit_id: CommitId::parse("path-put").expect("valid commit id"),
        message: None,
        absolute_path: AbsolutePath::parse("/file.txt").expect("path"),
        content_ref: prepared_content.content_ref().clone(),
        behavior: DestinationBehavior::NoReplace,
    };
    let (explicit_response, path_response) = tokio::join!(
        registry.submit_commit(namespace_id.clone(), explicit),
        registry.submit_path_intent_with_prepared_content(
            namespace_id.clone(),
            path_intent,
            vec![prepared_content]
        )
    );
    assert_eq!(
        explicit_response.expect("explicit response").committed_seq,
        ChangeSeq(2)
    );
    assert_eq!(
        path_response.expect("path response").committed_seq,
        ChangeSeq(3)
    );

    let wal_keys = store
        .list_prefix(&wal_segment_prefix("demo"))
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
    create_namespace(writer.core(), &namespace_id).await;
    let registry = writer.publisher();

    // An admitted publication blocks at its head CAS...
    store.block_next();
    let active = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_commit(namespace_id, create_directory_request("active", "active"))
                .await
        })
    };
    store.wait_until_blocked().await;

    // ...then admission closes. New work is refused at the front door.
    registry.close_admission();
    let refused = registry
        .submit_commit(
            namespace_id.clone(),
            create_directory_request("refused", "refused"),
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

    // The admitted publication still settles, and drain joins its task.
    store.release();
    let response = active
        .await
        .expect("submit task")
        .expect("admitted commit publishes");
    assert_eq!(response.committed_seq, ChangeSeq(1));
    registry.drain().await.expect("drain settles publish tasks");
    assert!(registry.shared.lock_state().tasks.is_empty());
}

/// Registry drain races panic recovery and respawned queued work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_drain_surfaces_panics_and_settles_respawned_work() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(PanicHeadCasStore::new(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    create_namespace(writer.core(), &namespace_id).await;
    let registry = writer.publisher();

    store.arm_blocking_panic();
    let doomed = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_commit(namespace_id, create_directory_request("doomed", "doomed"))
                .await
        })
    };
    store.wait_until_blocked().await;

    // Queued behind the blocked batch: only the abort guard's respawn
    // publishes this one, and the drain must join that respawn.
    let queued = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_commit(namespace_id, create_directory_request("queued", "queued"))
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
    loop {
        let queued_admitted = {
            let state = publisher
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            state
                .batch
                .as_ref()
                .is_some_and(|batch| !batch.candidates.is_empty())
        };
        if queued_admitted {
            break;
        }
        tokio::task::yield_now().await;
    }

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
        .expect("respawned task publishes queued work");
    assert_eq!(queued_response.committed_seq, ChangeSeq(1));

    let drain_error = registry
        .drain()
        .await
        .expect_err("drain surfaces the panicked task");
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
    create_namespace(writer.core(), &namespace_id).await;
    let registry = writer.publisher();

    registry
        .submit_commit(
            namespace_id.clone(),
            create_directory_request("before", "before"),
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
        .submit_commit(
            namespace_id.clone(),
            create_directory_request("late", "late"),
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
        .submit_commit(
            namespace_id.clone(),
            create_directory_request("nope", "nope"),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_queued_mid_publish_waits_behind_the_sealed_batch() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_head_cas_store(temp_dir.path(), &namespace_id));
    let shared = store.clone() as SharedStore;
    let writer = test_writer(shared.clone()).await;
    create_namespace(writer.core(), &namespace_id).await;
    let registry = writer.publisher();

    // Park the first publication at its head CAS, batch a second commit
    // behind it, then queue the delete: the sealed batch must publish
    // before the delete runs, and the blocked CAS outlasts the pacing
    // interval — the interleaving where a racing second task could run
    // the delete first.
    store.block_next();
    let before = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_commit(namespace_id, create_directory_request("before", "before"))
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

    // The publish task is parked in the blocked CAS, so this admission
    // deterministically opens the next batch instead of being taken.
    let second = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_commit(namespace_id, create_directory_request("second", "second"))
                .await
        })
    };
    loop {
        let batch_open = {
            let state = publisher
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            state
                .batch
                .as_ref()
                .is_some_and(|batch| !batch.candidates.is_empty())
        };
        if batch_open {
            break;
        }
        tokio::task::yield_now().await;
    }

    let delete = {
        let registry = registry.clone();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_delete(namespace_id, DeleteNamespaceOptions::default())
                .await
        })
    };
    // Deterministic: the delete has sealed the open batch.
    loop {
        let sealed = {
            let state = publisher
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            state.pending_delete.is_some()
        };
        if sealed {
            break;
        }
        tokio::task::yield_now().await;
    }

    // Snapshots are taken while the CAS is blocked but asserted only
    // after the gate is released: a regression then fails the test
    // instead of hanging runtime teardown on the never-released gate.
    let unfinished_tasks_while_blocked = registry
        .shared
        .lock_state()
        .tasks
        .iter()
        .filter(|task| !task.is_finished())
        .count();
    // With the sealed batch still blocked at its head CAS, outlast the
    // pacing interval: the delete must still not have run.
    wait_past_cas_pacing().await;
    let (deleted_while_blocked, delete_queued_while_blocked) = {
        let state = publisher
            .state
            .lock()
            .expect("namespace publisher mutex poisoned");
        (state.deleted, state.pending_delete.is_some())
    };

    // Released: the parked commit publishes, then the sealed batch, and
    // only then the delete.
    store.release();
    assert_eq!(
        unfinished_tasks_while_blocked, 1,
        "a delete must not spawn a racing second publish task"
    );
    assert!(
        !deleted_while_blocked,
        "delete executed while the sealed batch was still publishing"
    );
    assert!(
        delete_queued_while_blocked,
        "delete must stay queued behind the sealed batch"
    );
    let before_response = before
        .await
        .expect("before submit task")
        .expect("parked commit publishes before the delete");
    assert_eq!(before_response.committed_seq, ChangeSeq(1));
    let second_response = second
        .await
        .expect("second submit task")
        .expect("sealed batch publishes before the delete");
    assert_eq!(second_response.committed_seq, ChangeSeq(2));
    let delete_response = delete
        .await
        .expect("delete task")
        .expect("delete succeeds after the sealed batch");
    assert_eq!(delete_response.head_seq, ChangeSeq(2));
    registry.close_admission();
    registry.drain().await.expect("drain settles both units");
}
