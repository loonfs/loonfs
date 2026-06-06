use loon_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loon_api::{CommitId, NamespaceId};
use loon_core::commit::{CommitHeadPublishError, SemanticMutationIdentity};
use loonfs::{CoreError, Fs, NamespaceMutationCandidate, PathMutationIntent, RuntimeError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, Notify};
use tokio::time::{Duration, Instant};
use tracing::Instrument;

type CommitResult = Result<ApiCommitResponse, CoreError>;

const MAX_BATCH_CANDIDATES: usize = 1024;
const COALESCING_DELAY: Duration = Duration::from_millis(100);
const MIN_NAMESPACE_CAS_INTERVAL: Duration = Duration::from_secs(1);
const HEAD_CAS_RETRY_LIMIT: usize = 8;

#[allow(clippy::disallowed_methods)]
async fn coalescing_delay() {
    // Publisher batching intentionally uses a short async timer to coalesce requests.
    tokio::time::sleep(COALESCING_DELAY).await;
}

#[derive(Clone)]
pub(crate) struct PublisherRegistry {
    inner: Arc<Mutex<HashMap<NamespaceId, NamespacePublisher>>>,
    fs: Arc<Fs>,
}

impl PublisherRegistry {
    pub(crate) fn new(fs: Arc<Fs>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            fs,
        }
    }

    pub(crate) async fn submit_commit(
        &self,
        namespace_id: NamespaceId,
        request: ApiCommitRequest,
    ) -> CommitResult {
        self.submit_candidate(namespace_id, NamespaceMutationCandidate::Commit(request))
            .await
    }

    pub(crate) async fn submit_path_intent(
        &self,
        namespace_id: NamespaceId,
        intent: PathMutationIntent,
    ) -> CommitResult {
        self.submit_candidate(namespace_id, NamespaceMutationCandidate::Path(intent))
            .await
    }

    async fn submit_candidate(
        &self,
        namespace_id: NamespaceId,
        candidate: NamespaceMutationCandidate,
    ) -> CommitResult {
        let publisher = {
            let mut publishers = self
                .inner
                .lock()
                .expect("publisher registry mutex poisoned");
            publishers
                .entry(namespace_id.clone())
                .or_insert_with(|| NamespacePublisher::new(namespace_id.clone(), self.fs.clone()))
                .clone()
        };
        publisher.submit(candidate).await
    }
}

#[derive(Clone)]
struct NamespacePublisher {
    namespace_id: NamespaceId,
    fs: Arc<Fs>,
    state: Arc<Mutex<NamespacePublisherState>>,
}

struct NamespacePublisherState {
    batch: Option<OpenBatch>,
    in_flight: HashMap<CommitId, InFlightRequest>,
    publishing: bool,
    next_allowed_cas_at: Instant,
}

struct OpenBatch {
    candidates: Vec<BatchCandidate>,
    notify: Arc<Notify>,
}

#[derive(Clone)]
struct BatchCandidate {
    commit_id: CommitId,
    candidate: NamespaceMutationCandidate,
    operation_class: &'static str,
    enqueued_at: Instant,
}

struct InFlightRequest {
    semantic_identity: SemanticMutationIdentity,
    waiters: Vec<oneshot::Sender<CommitResult>>,
}

impl NamespacePublisher {
    fn new(namespace_id: NamespaceId, fs: Arc<Fs>) -> Self {
        Self {
            namespace_id,
            fs,
            state: Arc::new(Mutex::new(NamespacePublisherState {
                batch: None,
                in_flight: HashMap::new(),
                publishing: false,
                next_allowed_cas_at: Instant::now(),
            })),
        }
    }

    async fn submit(&self, candidate: NamespaceMutationCandidate) -> CommitResult {
        let commit_id = candidate.commit_id().clone();
        let operation_class = operation_class(&candidate);
        let enqueued_at = Instant::now();
        let semantic_identity = candidate.semantic_identity(&self.namespace_id)?;
        let (sender, receiver) = oneshot::channel();
        self.admit(
            commit_id,
            candidate,
            semantic_identity,
            sender,
            operation_class,
            enqueued_at,
        )?;
        receiver
            .await
            .unwrap_or_else(|_| Err(CoreError::Store("publisher task stopped".to_owned())))
    }

    fn admit(
        &self,
        commit_id: CommitId,
        candidate: NamespaceMutationCandidate,
        semantic_identity: SemanticMutationIdentity,
        waiter: oneshot::Sender<CommitResult>,
        operation_class: &'static str,
        enqueued_at: Instant,
    ) -> Result<(), CoreError> {
        let mut should_spawn = false;
        let mut notify_full = None;
        {
            let mut state = self
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            if let Some(existing) = state.in_flight.get_mut(&commit_id) {
                if existing.semantic_identity != semantic_identity {
                    return Err(CoreError::CommitIdReuseConflict(commit_id.to_string()));
                }
                existing.waiters.push(waiter);
                self.trace_enqueue(operation_class, pending_queue_depth(&state), "duplicate");
                return Ok(());
            }

            if state.batch.is_none() {
                should_spawn = !state.publishing;
                state.batch = Some(OpenBatch {
                    candidates: Vec::new(),
                    notify: Arc::new(Notify::new()),
                });
            }

            let (batch_len, batch_notify) = {
                let batch = state.batch.as_mut().expect("open batch should exist");
                if batch.candidates.len() >= MAX_BATCH_CANDIDATES {
                    self.trace_enqueue(operation_class, batch.candidates.len(), "full");
                    return Err(CoreError::CommitQueueFull);
                }
                batch.candidates.push(BatchCandidate {
                    commit_id: commit_id.clone(),
                    candidate: candidate.clone(),
                    operation_class,
                    enqueued_at,
                });
                (batch.candidates.len(), batch.notify.clone())
            };
            self.trace_enqueue(operation_class, batch_len, "new");
            state.in_flight.insert(
                commit_id,
                InFlightRequest {
                    semantic_identity,
                    waiters: vec![waiter],
                },
            );
            if batch_len >= MAX_BATCH_CANDIDATES {
                notify_full = Some(batch_notify);
            }
        }

        if should_spawn {
            self.spawn_publish_task();
        }
        if let Some(notify) = notify_full {
            notify.notify_one();
        }
        Ok(())
    }

    fn spawn_publish_task(&self) {
        let publisher = self.clone();
        tokio::spawn(async move {
            publisher.publish_open_batch().await;
        });
    }

    async fn publish_open_batch(self) {
        let collect_started = Instant::now();
        let (notify, already_full, queue_depth_start) = {
            let state = self
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            let Some(batch) = state.batch.as_ref() else {
                return;
            };
            (
                batch.notify.clone(),
                batch.candidates.len() >= MAX_BATCH_CANDIDATES,
                batch.candidates.len(),
            )
        };

        if !already_full {
            tokio::select! {
                _ = coalescing_delay() => {}
                _ = notify.notified() => {}
            }
        }
        let queue_depth_end = {
            let state = self
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            state
                .batch
                .as_ref()
                .map_or(0, |batch| batch.candidates.len())
        };
        tracing::info!(
            phase = "batch_collect",
            mode = self.trace_mode(),
            store_kind = self.trace_store_kind(),
            batch_size = usize_to_u64(queue_depth_end),
            queue_depth_start = usize_to_u64(queue_depth_start),
            queue_depth_end = usize_to_u64(queue_depth_end),
            collect_ms = elapsed_ms_since(collect_started),
            "publisher.batch_collect"
        );

        loop {
            let sleep_until = {
                let state = self
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                state.next_allowed_cas_at
            };
            let now = Instant::now();
            if sleep_until <= now {
                break;
            }
            tokio::time::sleep_until(sleep_until).await;
        }

        let candidates = {
            let mut state = self
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            state.publishing = true;
            let Some(batch) = state.batch.take() else {
                state.publishing = false;
                return;
            };
            state.next_allowed_cas_at = Instant::now() + MIN_NAMESPACE_CAS_INTERVAL;
            batch.candidates
        };
        let selected_at = Instant::now();
        for candidate in &candidates {
            tracing::info!(
                phase = "wait_for_batch",
                mode = self.trace_mode(),
                store_kind = self.trace_store_kind(),
                operation_class = candidate.operation_class,
                result = "ok",
                wait_ms = elapsed_ms_from(candidate.enqueued_at, selected_at),
                "publisher.wait_for_batch"
            );
        }

        let publish_span = tracing::info_span!(
            "publisher.batch_publish",
            phase = "batch_publish",
            mode = self.trace_mode(),
            store_kind = self.trace_store_kind(),
            batch_size = usize_to_u64(candidates.len()),
            result = tracing::field::Empty,
            retry_count = tracing::field::Empty
        );
        let (results, retry_count) = async {
            let mut results = Vec::new();
            let mut retry_count = 0_u64;
            for attempt in 0..HEAD_CAS_RETRY_LIMIT {
                let batch_candidates = candidates
                    .iter()
                    .map(|candidate| candidate.candidate.clone())
                    .collect::<Vec<_>>();
                let namespace_id = self.namespace_id.clone();
                let fs = self.fs.clone();
                results = tokio::task::spawn_blocking(move || {
                    fs.publish_namespace_mutations_batch(&namespace_id, batch_candidates)
                        .into_iter()
                        .map(|result| result.map_err(runtime_error_to_core))
                        .collect()
                })
                .await
                .unwrap_or_else(|err| {
                    vec![Err(CoreError::Store(err.to_string())); candidates.len()]
                });
                if !results.iter().any(is_head_publish_stale) {
                    break;
                }
                if attempt + 1 == HEAD_CAS_RETRY_LIMIT {
                    break;
                }
                retry_count += 1;
                self.wait_for_next_cas_token().await;
            }
            (results, retry_count)
        }
        .instrument(publish_span.clone())
        .await;
        publish_span.record("result", batch_result_label(&results));
        publish_span.record("retry_count", retry_count);
        drop(publish_span);

        self.complete_batch(candidates, results, selected_at);
    }

    async fn wait_for_next_cas_token(&self) {
        loop {
            let sleep_until = {
                let state = self
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                state.next_allowed_cas_at
            };
            let now = Instant::now();
            if sleep_until <= now {
                let mut state = self
                    .state
                    .lock()
                    .expect("namespace publisher mutex poisoned");
                state.next_allowed_cas_at = Instant::now() + MIN_NAMESPACE_CAS_INTERVAL;
                break;
            }
            tokio::time::sleep_until(sleep_until).await;
        }
    }

    fn complete_batch(
        &self,
        candidates: Vec<BatchCandidate>,
        results: Vec<CommitResult>,
        selected_at: Instant,
    ) {
        let mut deliveries = Vec::new();
        let mut wait_traces = Vec::new();
        let should_spawn_next = {
            let mut state = self
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            state.publishing = false;
            let mut results = results.into_iter();
            for candidate in candidates {
                let result = results.next().unwrap_or_else(|| {
                    Err(CoreError::Store(
                        "publisher returned too few batch results".to_owned(),
                    ))
                });
                wait_traces.push((
                    candidate.operation_class,
                    result_label(&result),
                    elapsed_ms_since(selected_at),
                ));
                if let Some(in_flight) = state.in_flight.remove(&candidate.commit_id) {
                    for waiter in in_flight.waiters {
                        deliveries.push((waiter, result.clone()));
                    }
                }
            }
            state
                .batch
                .as_ref()
                .is_some_and(|batch| !batch.candidates.is_empty())
        };

        for (operation_class, result, wait_ms) in wait_traces {
            tracing::info!(
                phase = "wait_for_result",
                mode = self.trace_mode(),
                store_kind = self.trace_store_kind(),
                operation_class,
                result,
                wait_ms,
                "publisher.wait_for_result"
            );
        }

        for (waiter, result) in deliveries {
            let _ = waiter.send(result);
        }

        if should_spawn_next {
            self.spawn_publish_task();
        }
    }

    fn trace_enqueue(
        &self,
        operation_class: &'static str,
        queue_depth: usize,
        reason: &'static str,
    ) {
        tracing::info!(
            phase = "enqueue",
            mode = self.trace_mode(),
            store_kind = self.trace_store_kind(),
            operation_class,
            queue_depth = usize_to_u64(queue_depth),
            reason,
            "publisher.enqueue"
        );
    }

    fn trace_mode(&self) -> &'static str {
        self.fs.config().trace_mode.as_str()
    }

    fn trace_store_kind(&self) -> &'static str {
        self.fs.config().trace_store_kind.as_str()
    }
}

fn is_head_publish_stale(result: &CommitResult) -> bool {
    matches!(
        result,
        Err(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
    )
}

fn runtime_error_to_core(error: RuntimeError) -> CoreError {
    match error {
        RuntimeError::Core(error) => error,
        RuntimeError::Bootstrap(error) => CoreError::Store(error.to_string()),
        RuntimeError::Config(message) => CoreError::Store(message),
    }
}

fn operation_class(candidate: &NamespaceMutationCandidate) -> &'static str {
    match candidate {
        NamespaceMutationCandidate::Commit(_) => "explicit_commit",
        NamespaceMutationCandidate::Path(_) => "path_mutation",
    }
}

fn pending_queue_depth(state: &NamespacePublisherState) -> usize {
    state
        .batch
        .as_ref()
        .map_or(0, |batch| batch.candidates.len())
}

fn result_label<T, E>(result: &Result<T, E>) -> &'static str {
    if result.is_ok() {
        "ok"
    } else {
        "error"
    }
}

fn batch_result_label(results: &[CommitResult]) -> &'static str {
    if results.iter().all(Result::is_ok) {
        "ok"
    } else {
        "error"
    }
}

fn elapsed_ms_since(start: Instant) -> u64 {
    duration_ms(start.elapsed())
}

fn elapsed_ms_from(start: Instant, end: Instant) -> u64 {
    duration_ms(end.saturating_duration_since(start))
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // Publisher tests use panic in async result helpers for precise diagnostics.

    use super::*;
    use crate::config::{RuntimeCacheConfigOverrides, ServerConfig, StoreConfig};
    use loon_api::v0::{CommitOp, CommitRequest};
    use loon_api::wire::wal::decode_wal_segment_envelope_zstd;
    use loon_api::{ChangeSeq, InodeId};
    use loon_core::content::store_bytes_as_content;
    use loon_core::publish::PathMutationIntent;
    use loon_core::PutFileBehavior;
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::namespace_head;
    use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
    use loonfs::{
        CreateNamespaceOptions, Fs, FsConfig, RuntimeCacheConfig, SharedObjectStore as SharedStore,
        TraceMode, TraceStoreKind,
    };
    use std::path::Path;
    use std::sync::Condvar;
    use tempfile::tempdir;

    #[derive(Debug)]
    struct BlockingHeadCasStore {
        inner: LocalFsStore,
        head_key: String,
        gate: Arc<HeadCasGate>,
    }

    #[derive(Debug)]
    struct HeadCasGate {
        state: Mutex<HeadCasGateState>,
        cvar: Condvar,
    }

    #[derive(Debug)]
    struct HeadCasGateState {
        blocks_remaining: usize,
        entered: usize,
        released: bool,
    }

    impl BlockingHeadCasStore {
        fn new(root: impl AsRef<Path>, namespace_id: &NamespaceId) -> Self {
            Self {
                inner: LocalFsStore::new(root.as_ref()).expect("store"),
                head_key: namespace_head(namespace_id.as_str()),
                gate: Arc::new(HeadCasGate {
                    state: Mutex::new(HeadCasGateState {
                        blocks_remaining: 0,
                        entered: 0,
                        released: false,
                    }),
                    cvar: Condvar::new(),
                }),
            }
        }

        fn arm_next_head_cas(&self) {
            let mut state = self.gate.state.lock().expect("head gate mutex poisoned");
            state.blocks_remaining = 1;
            state.released = false;
        }

        async fn wait_for_blocked_head_cas(&self) {
            let gate = self.gate.clone();
            tokio::task::spawn_blocking(move || {
                let mut state = gate.state.lock().expect("head gate mutex poisoned");
                while state.entered < 1 {
                    state = gate.cvar.wait(state).expect("head gate mutex poisoned");
                }
            })
            .await
            .expect("wait for blocked head CAS");
        }

        fn release_head_cas(&self) {
            let mut state = self.gate.state.lock().expect("head gate mutex poisoned");
            state.released = true;
            self.gate.cvar.notify_all();
        }
    }

    impl ObjectStore for BlockingHeadCasStore {
        fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key)
        }

        fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
            self.inner.get(key, range)
        }

        fn put(
            &self,
            key: &str,
            bytes: &[u8],
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if key == self.head_key && matches!(mode, PutMode::CompareAndSwap { .. }) {
                let mut state = self.gate.state.lock().expect("head gate mutex poisoned");
                if state.blocks_remaining > 0 {
                    state.blocks_remaining -= 1;
                    state.entered += 1;
                    self.gate.cvar.notify_all();
                    while !state.released {
                        state = self
                            .gate
                            .cvar
                            .wait(state)
                            .expect("head gate mutex poisoned");
                    }
                }
            }
            self.inner.put(key, bytes, mode)
        }

        fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key)
        }

        fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
            self.inner.list_prefix(prefix)
        }
    }

    fn test_config(root: &Path) -> Arc<ServerConfig> {
        Arc::new(ServerConfig {
            bind: "127.0.0.1:0".to_owned(),
            auth_token: None,
            writer_id: "writer-a".to_owned(),
            writer_version: "test".to_owned(),
            lease_duration_ms: 60_000,
            runtime_cache: RuntimeCacheConfigOverrides::default(),
            store: StoreConfig::LocalFs {
                root: root.display().to_string(),
                key_prefix: None,
            },
        })
    }

    fn test_fs(store: SharedStore, config: &ServerConfig) -> Arc<Fs> {
        Arc::new(
            Fs::open(
                store,
                FsConfig {
                    writer_id: config.writer_id.clone(),
                    writer_version: config.writer_version.clone(),
                    lease_duration_ms: config.lease_duration_ms,
                    runtime_cache: RuntimeCacheConfig::default(),
                    trace_mode: TraceMode::Remote,
                    trace_store_kind: TraceStoreKind::LocalFs,
                },
            )
            .expect("open runtime"),
        )
    }

    fn create_namespace(fs: &Fs, namespace_id: &NamespaceId) {
        fs.create_namespace(namespace_id, CreateNamespaceOptions::default())
            .expect("bootstrap");
    }

    fn create_dir_request(
        commit_id: impl Into<String>,
        display_name: impl Into<String>,
    ) -> CommitRequest {
        CommitRequest {
            commit_id: CommitId::try_new(commit_id.into()).expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: display_name.into(),
            }],
            message: None,
            annotations: None,
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
        let commit_id = request.commit_id.clone();
        let candidate = NamespaceMutationCandidate::Commit(request);
        let operation_class = operation_class(&candidate);
        let semantic_identity = candidate.semantic_identity(namespace_id)?;
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

    async fn recv_commit(
        receiver: oneshot::Receiver<CommitResult>,
        label: &str,
    ) -> ApiCommitResponse {
        receiver
            .await
            .unwrap_or_else(|err| panic!("{label} receiver dropped: {err}"))
            .unwrap_or_else(|err| panic!("{label} failed: {err}"))
    }

    #[test]
    fn publisher_trace_labels_are_low_cardinality() {
        let commit =
            NamespaceMutationCandidate::Commit(create_dir_request("commit-trace", "private-name"));
        let path = NamespaceMutationCandidate::Path(PathMutationIntent::CreateDir {
            commit_id: CommitId::parse("path-trace").expect("valid commit id"),
            absolute_path: "/private/path".to_owned(),
        });

        assert_eq!(operation_class(&commit), "explicit_commit");
        assert_eq!(operation_class(&path), "path_mutation");
        assert_eq!(result_label(&Ok::<_, CoreError>(())), "ok");
        assert_eq!(
            result_label(&Err::<(), _>(CoreError::Store("private error".to_owned()))),
            "error"
        );
        assert_eq!(usize_to_u64(7), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_admits_pending_batch_while_active_publish_blocks() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let config = test_config(temp_dir.path());
        let fs = test_fs(shared.clone(), &config);
        create_namespace(&fs, &namespace_id);
        let publisher = NamespacePublisher::new(namespace_id.clone(), fs);

        store.arm_next_head_cas();
        let active = admit_commit(
            &publisher,
            &namespace_id,
            create_dir_request("active", "active"),
        );
        store.wait_for_blocked_head_cas().await;

        let pending = admit_commit(
            &publisher,
            &namespace_id,
            create_dir_request("pending", "pending"),
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

        store.release_head_cas();
        let active_response = recv_commit(active, "active").await;
        let pending_response = recv_commit(pending, "pending").await;
        assert_eq!(active_response.committed_seq, ChangeSeq(1));
        assert_eq!(pending_response.committed_seq, ChangeSeq(2));

        let wal_keys = shared
            .list_prefix("namespaces/demo/wal/")
            .expect("list wal");
        assert_eq!(wal_keys.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_duplicate_active_request_joins_while_conflict_fails() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let config = test_config(temp_dir.path());
        let fs = test_fs(shared.clone(), &config);
        create_namespace(&fs, &namespace_id);
        let publisher = NamespacePublisher::new(namespace_id.clone(), fs);

        store.arm_next_head_cas();
        let active = admit_commit(
            &publisher,
            &namespace_id,
            create_dir_request("active", "active"),
        );
        store.wait_for_blocked_head_cas().await;

        let duplicate = admit_commit(
            &publisher,
            &namespace_id,
            create_dir_request("active", "active"),
        );
        let conflict = try_admit_commit(
            &publisher,
            &namespace_id,
            create_dir_request("active", "different-active"),
        );
        assert!(matches!(
            conflict,
            Err(CoreError::CommitIdReuseConflict(commit_id)) if commit_id == "active"
        ));

        store.release_head_cas();
        let active_response = recv_commit(active, "active").await;
        let duplicate_response = recv_commit(duplicate, "duplicate").await;
        assert_eq!(active_response.committed_seq, ChangeSeq(1));
        assert_eq!(duplicate_response.committed_seq, ChangeSeq(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_pending_batch_full_rejects_distinct_but_allows_duplicate() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let config = test_config(temp_dir.path());
        let fs = test_fs(shared.clone(), &config);
        create_namespace(&fs, &namespace_id);
        let publisher = NamespacePublisher::new(namespace_id.clone(), fs);

        store.arm_next_head_cas();
        let active = admit_commit(
            &publisher,
            &namespace_id,
            create_dir_request("active", "active"),
        );
        store.wait_for_blocked_head_cas().await;

        let mut pending = Vec::with_capacity(MAX_BATCH_CANDIDATES);
        for index in 0..MAX_BATCH_CANDIDATES {
            pending.push(admit_commit(
                &publisher,
                &namespace_id,
                create_dir_request(format!("pending-{index}"), format!("pending-{index}")),
            ));
        }

        let duplicate = admit_commit(
            &publisher,
            &namespace_id,
            create_dir_request("pending-0", "pending-0"),
        );
        let conflict = try_admit_commit(
            &publisher,
            &namespace_id,
            create_dir_request("pending-0", "different-pending"),
        );
        assert!(matches!(
            conflict,
            Err(CoreError::CommitIdReuseConflict(commit_id)) if commit_id == "pending-0"
        ));

        let overflow = try_admit_commit(
            &publisher,
            &namespace_id,
            create_dir_request("overflow", "overflow"),
        );
        assert!(matches!(overflow, Err(CoreError::CommitQueueFull)));

        store.release_head_cas();
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

    #[tokio::test(flavor = "current_thread")]
    async fn publisher_full_batch_does_not_wait_on_missed_full_notification() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let config = test_config(temp_dir.path());
        let fs = test_fs(shared.clone(), &config);
        create_namespace(&fs, &namespace_id);
        let publisher = NamespacePublisher::new(namespace_id.clone(), fs);

        store.arm_next_head_cas();
        let mut receivers = Vec::with_capacity(MAX_BATCH_CANDIDATES);
        for index in 0..MAX_BATCH_CANDIDATES {
            receivers.push(admit_commit(
                &publisher,
                &namespace_id,
                create_dir_request(format!("full-{index}"), format!("full-{index}")),
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
        store.release_head_cas();
        for (index, receiver) in receivers.into_iter().enumerate() {
            assert_eq!(
                recv_commit(receiver, "full").await.committed_seq,
                ChangeSeq(index as u64 + 1)
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_batches_concurrent_distinct_commits_into_one_wal_segment() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let config = Arc::new(ServerConfig {
            bind: "127.0.0.1:0".to_owned(),
            auth_token: None,
            writer_id: "writer-a".to_owned(),
            writer_version: "test".to_owned(),
            lease_duration_ms: 60_000,
            runtime_cache: RuntimeCacheConfigOverrides::default(),
            store: StoreConfig::LocalFs {
                root: temp_dir.path().display().to_string(),
                key_prefix: None,
            },
        });
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let fs = test_fs(store.clone(), &config);
        create_namespace(&fs, &namespace_id);
        let registry = PublisherRegistry::new(fs);

        let request_a = CommitRequest {
            commit_id: CommitId::parse("req-a").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "alpha".to_owned(),
            }],
            message: None,
            annotations: None,
        };
        let request_b = CommitRequest {
            commit_id: CommitId::parse("req-b").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "beta".to_owned(),
            }],
            message: None,
            annotations: None,
        };

        let (response_a, response_b) = tokio::join!(
            registry.submit_commit(namespace_id.clone(), request_a),
            registry.submit_commit(namespace_id.clone(), request_b)
        );
        assert_eq!(response_a.expect("response a").committed_seq, ChangeSeq(1));
        assert_eq!(response_b.expect("response b").committed_seq, ChangeSeq(2));

        let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
        assert_eq!(wal_keys.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_batches_explicit_commit_and_path_intent_together() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let config = Arc::new(ServerConfig {
            bind: "127.0.0.1:0".to_owned(),
            auth_token: None,
            writer_id: "writer-a".to_owned(),
            writer_version: "test".to_owned(),
            lease_duration_ms: 60_000,
            runtime_cache: RuntimeCacheConfigOverrides::default(),
            store: StoreConfig::LocalFs {
                root: temp_dir.path().display().to_string(),
                key_prefix: None,
            },
        });
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let fs = test_fs(store.clone(), &config);
        create_namespace(&fs, &namespace_id);
        let content =
            store_bytes_as_content(store.as_ref(), &namespace_id, b"hello").expect("stage content");
        let registry = PublisherRegistry::new(fs);

        let explicit = CommitRequest {
            commit_id: CommitId::parse("explicit-commit").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "alpha".to_owned(),
            }],
            message: None,
            annotations: None,
        };
        let path_intent = PathMutationIntent::PutFile {
            commit_id: CommitId::parse("path-put").expect("valid commit id"),
            absolute_path: "/file.txt".to_owned(),
            content_ref: content.content_ref,
            behavior: PutFileBehavior::CreateOnly,
        };

        let (explicit_response, path_response) = tokio::join!(
            registry.submit_commit(namespace_id.clone(), explicit),
            registry.submit_path_intent(namespace_id.clone(), path_intent)
        );
        assert_eq!(
            explicit_response.expect("explicit response").committed_seq,
            ChangeSeq(1)
        );
        assert_eq!(
            path_response.expect("path response").committed_seq,
            ChangeSeq(2)
        );

        let wal_keys = store.list_prefix("namespaces/demo/wal/").expect("list wal");
        assert_eq!(wal_keys.len(), 1);
        let wal_bytes = store
            .get(&wal_keys[0], None)
            .expect("read wal")
            .expect("wal exists");
        let segment = decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode wal segment");
        assert_eq!(segment.payload.records.len(), 2);
    }
}
