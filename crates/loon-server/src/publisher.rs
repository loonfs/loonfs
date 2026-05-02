use crate::config::ServerConfig;
use loon_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loon_api::{payload_checksum_sha256, NamespaceId};
use loon_core::{
    commit::CommitHeadPublishError, publish_namespace_mutations_batch, CoreError, MutationContext,
    NamespaceMutationCandidate, PathMutationIntent, PlannedNamespaceMutation,
};
use loon_objectstore::ObjectStore;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, Notify};
use tokio::time::{Duration, Instant};

type SharedStore = Arc<dyn ObjectStore + Send + Sync>;
type CommitResult = Result<ApiCommitResponse, CoreError>;

const MAX_BATCH_CANDIDATES: usize = 1024;
const COALESCING_DELAY: Duration = Duration::from_millis(100);
const MIN_NAMESPACE_CAS_INTERVAL: Duration = Duration::from_secs(1);
const HEAD_CAS_RETRY_LIMIT: usize = 8;

#[derive(Clone)]
pub(crate) struct PublisherRegistry {
    inner: Arc<Mutex<HashMap<NamespaceId, NamespacePublisher>>>,
    store: SharedStore,
    config: Arc<ServerConfig>,
}

impl PublisherRegistry {
    pub(crate) fn new(store: SharedStore, config: Arc<ServerConfig>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            store,
            config,
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
                .or_insert_with(|| {
                    NamespacePublisher::new(
                        namespace_id.clone(),
                        self.store.clone(),
                        self.config.clone(),
                    )
                })
                .clone()
        };
        publisher.submit(candidate).await
    }
}

#[derive(Clone)]
struct NamespacePublisher {
    namespace_id: NamespaceId,
    store: SharedStore,
    config: Arc<ServerConfig>,
    state: Arc<Mutex<NamespacePublisherState>>,
}

struct NamespacePublisherState {
    batch: Option<OpenBatch>,
    in_flight: HashMap<String, InFlightRequest>,
    publishing: bool,
    next_allowed_cas_at: Instant,
}

struct OpenBatch {
    candidates: Vec<BatchCandidate>,
    notify: Arc<Notify>,
}

#[derive(Clone)]
struct BatchCandidate {
    request_id: String,
    candidate: NamespaceMutationCandidate,
}

struct InFlightRequest {
    fingerprint: String,
    waiters: Vec<oneshot::Sender<CommitResult>>,
}

impl NamespacePublisher {
    fn new(namespace_id: NamespaceId, store: SharedStore, config: Arc<ServerConfig>) -> Self {
        Self {
            namespace_id,
            store,
            config,
            state: Arc::new(Mutex::new(NamespacePublisherState {
                batch: None,
                in_flight: HashMap::new(),
                publishing: false,
                next_allowed_cas_at: Instant::now(),
            })),
        }
    }

    async fn submit(&self, candidate: NamespaceMutationCandidate) -> CommitResult {
        let request_id = candidate_request_id(&candidate).to_owned();
        let fingerprint = candidate_fingerprint(&self.namespace_id, &candidate)?;
        let (sender, receiver) = oneshot::channel();
        self.admit(request_id, candidate, fingerprint, sender)?;
        receiver
            .await
            .unwrap_or_else(|_| Err(CoreError::Store("publisher task stopped".to_owned())))
    }

    fn admit(
        &self,
        request_id: String,
        candidate: NamespaceMutationCandidate,
        fingerprint: String,
        waiter: oneshot::Sender<CommitResult>,
    ) -> Result<(), CoreError> {
        let mut should_spawn = false;
        let mut notify_full = None;
        {
            let mut state = self
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            if let Some(existing) = state.in_flight.get_mut(&request_id) {
                if existing.fingerprint != fingerprint {
                    return Err(CoreError::RequestIdConflict(request_id));
                }
                existing.waiters.push(waiter);
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
                    return Err(CoreError::CommitQueueFull);
                }
                batch.candidates.push(BatchCandidate {
                    request_id: request_id.clone(),
                    candidate: candidate.clone(),
                });
                (batch.candidates.len(), batch.notify.clone())
            };
            state.in_flight.insert(
                request_id,
                InFlightRequest {
                    fingerprint,
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
        let (notify, already_full) = {
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
            )
        };

        if !already_full {
            tokio::select! {
                _ = tokio::time::sleep(COALESCING_DELAY) => {}
                _ = notify.notified() => {}
            }
        }

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
            state.next_allowed_cas_at = Instant::now() + MIN_NAMESPACE_CAS_INTERVAL;
            let Some(batch) = state.batch.take() else {
                state.publishing = false;
                return;
            };
            batch.candidates
        };

        let mut results = Vec::new();
        for attempt in 0..HEAD_CAS_RETRY_LIMIT {
            let batch_candidates = candidates
                .iter()
                .map(|candidate| candidate.candidate.clone())
                .collect::<Vec<_>>();
            let namespace_id = self.namespace_id.clone();
            let store = self.store.clone();
            let context = mutation_context(&self.config);
            results = tokio::task::spawn_blocking(move || {
                publish_namespace_mutations_batch(
                    store.as_ref(),
                    &namespace_id,
                    batch_candidates,
                    &context,
                )
            })
            .await
            .unwrap_or_else(|err| vec![Err(CoreError::Store(err.to_string())); candidates.len()]);
            if !results.iter().any(is_head_publish_stale) {
                break;
            }
            if attempt + 1 == HEAD_CAS_RETRY_LIMIT {
                break;
            }
            self.wait_for_next_cas_token().await;
        }

        self.complete_batch(candidates, results);
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

    fn complete_batch(&self, candidates: Vec<BatchCandidate>, results: Vec<CommitResult>) {
        let mut deliveries = Vec::new();
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
                if let Some(in_flight) = state.in_flight.remove(&candidate.request_id) {
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

        for (waiter, result) in deliveries {
            let _ = waiter.send(result);
        }

        if should_spawn_next {
            self.spawn_publish_task();
        }
    }
}

fn is_head_publish_stale(result: &CommitResult) -> bool {
    matches!(
        result,
        Err(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
    )
}

fn candidate_request_id(candidate: &NamespaceMutationCandidate) -> &str {
    match candidate {
        NamespaceMutationCandidate::Commit(request) => &request.request_id,
        NamespaceMutationCandidate::Planned(PlannedNamespaceMutation {
            commit_request, ..
        }) => &commit_request.request_id,
        NamespaceMutationCandidate::Path(intent) => intent.request_id(),
    }
}

fn candidate_fingerprint(
    namespace_id: &NamespaceId,
    candidate: &NamespaceMutationCandidate,
) -> Result<String, CoreError> {
    match candidate {
        NamespaceMutationCandidate::Commit(request) => semantic_fingerprint(namespace_id, request)
            .map_err(|err| CoreError::Store(err.to_string())),
        NamespaceMutationCandidate::Planned(PlannedNamespaceMutation {
            source_request_checksum_sha256: Some(source),
            ..
        }) => Ok(source.clone()),
        NamespaceMutationCandidate::Planned(PlannedNamespaceMutation {
            commit_request,
            source_request_checksum_sha256: None,
        }) => semantic_fingerprint(namespace_id, commit_request)
            .map_err(|err| CoreError::Store(err.to_string())),
        NamespaceMutationCandidate::Path(intent) => {
            intent.source_request_checksum_sha256(namespace_id)
        }
    }
}

#[derive(Serialize)]
struct SemanticCommit<'a> {
    namespace_id: &'a NamespaceId,
    request_id: &'a str,
    planned_head_seq: loon_api::ChangeSeq,
    preconditions: &'a [loon_api::v0::CommitPrecondition],
    ops: &'a [loon_api::v0::CommitOp],
    message: &'a Option<String>,
    annotations: &'a Option<loon_api::v0::CommitAnnotations>,
}

fn semantic_fingerprint(
    namespace_id: &NamespaceId,
    request: &ApiCommitRequest,
) -> Result<String, serde_json::Error> {
    payload_checksum_sha256(&SemanticCommit {
        namespace_id,
        request_id: &request.request_id,
        planned_head_seq: request.planned_head_seq,
        preconditions: &request.preconditions,
        ops: &request.ops,
        message: &request.message,
        annotations: &request.annotations,
    })
}

fn mutation_context(config: &ServerConfig) -> MutationContext {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    MutationContext {
        writer_id: config.writer_id.clone(),
        writer_version: config.writer_version.clone(),
        now_ms,
        lease_duration_ms: config.lease_duration_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StoreConfig;
    use loon_api::v0::{CommitOp, CommitRequest};
    use loon_api::{ChangeSeq, InodeId};
    use loon_core::{
        bootstrap_namespace, store_bytes_as_content, PathMutationIntent, PutFileBehavior,
    };
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::namespace_head;
    use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
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
            store: StoreConfig::LocalFs {
                root: root.display().to_string(),
                key_prefix: None,
            },
        })
    }

    fn create_dir_request(
        request_id: impl Into<String>,
        display_name: impl Into<String>,
    ) -> CommitRequest {
        CommitRequest {
            request_id: request_id.into(),
            planned_head_seq: ChangeSeq(0),
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
        let request_id = request.request_id.clone();
        let candidate = NamespaceMutationCandidate::Commit(request);
        let fingerprint = candidate_fingerprint(namespace_id, &candidate)?;
        let (sender, receiver) = oneshot::channel();
        publisher.admit(request_id, candidate, fingerprint, sender)?;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publisher_admits_pending_batch_while_active_publish_blocks() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::from("demo");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let config = test_config(temp_dir.path());
        bootstrap_namespace(
            shared.as_ref(),
            &namespace_id,
            &mutation_context(&config),
            false,
        )
        .expect("bootstrap");
        let publisher = NamespacePublisher::new(namespace_id.clone(), shared.clone(), config);

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
        let namespace_id = NamespaceId::from("demo");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let config = test_config(temp_dir.path());
        bootstrap_namespace(
            shared.as_ref(),
            &namespace_id,
            &mutation_context(&config),
            false,
        )
        .expect("bootstrap");
        let publisher = NamespacePublisher::new(namespace_id.clone(), shared.clone(), config);

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
            Err(CoreError::RequestIdConflict(request_id)) if request_id == "active"
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
        let namespace_id = NamespaceId::from("demo");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let config = test_config(temp_dir.path());
        bootstrap_namespace(
            shared.as_ref(),
            &namespace_id,
            &mutation_context(&config),
            false,
        )
        .expect("bootstrap");
        let publisher = NamespacePublisher::new(namespace_id.clone(), shared.clone(), config);

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
            Err(CoreError::RequestIdConflict(request_id)) if request_id == "pending-0"
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
        let namespace_id = NamespaceId::from("demo");
        let store = Arc::new(BlockingHeadCasStore::new(temp_dir.path(), &namespace_id));
        let shared = store.clone() as SharedStore;
        let config = test_config(temp_dir.path());
        bootstrap_namespace(
            shared.as_ref(),
            &namespace_id,
            &mutation_context(&config),
            false,
        )
        .expect("bootstrap");
        let publisher = NamespacePublisher::new(namespace_id.clone(), shared.clone(), config);

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
            store: StoreConfig::LocalFs {
                root: temp_dir.path().display().to_string(),
                key_prefix: None,
            },
        });
        let namespace_id = NamespaceId::from("demo");
        bootstrap_namespace(
            store.as_ref(),
            &namespace_id,
            &mutation_context(&config),
            false,
        )
        .expect("bootstrap");
        let registry = PublisherRegistry::new(store.clone(), config);

        let request_a = CommitRequest {
            request_id: "req-a".to_owned(),
            planned_head_seq: ChangeSeq(0),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "alpha".to_owned(),
            }],
            message: None,
            annotations: None,
        };
        let request_b = CommitRequest {
            request_id: "req-b".to_owned(),
            planned_head_seq: ChangeSeq(0),
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
            store: StoreConfig::LocalFs {
                root: temp_dir.path().display().to_string(),
                key_prefix: None,
            },
        });
        let namespace_id = NamespaceId::from("demo");
        bootstrap_namespace(
            store.as_ref(),
            &namespace_id,
            &mutation_context(&config),
            false,
        )
        .expect("bootstrap");
        let content =
            store_bytes_as_content(store.as_ref(), &namespace_id, b"hello").expect("stage content");
        let registry = PublisherRegistry::new(store.clone(), config);

        let explicit = CommitRequest {
            request_id: "explicit-commit".to_owned(),
            planned_head_seq: ChangeSeq(0),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDir {
                parent_inode: InodeId(1),
                display_name: "alpha".to_owned(),
            }],
            message: None,
            annotations: None,
        };
        let path_intent = PathMutationIntent::PutFile {
            request_id: "path-put".to_owned(),
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
        let segment =
            loon_api::decode_wal_segment_envelope_zstd(&wal_bytes).expect("decode wal segment");
        assert_eq!(segment.payload.records.len(), 2);
    }
}
