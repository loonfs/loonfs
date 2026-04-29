use crate::config::ServerConfig;
use loon_api::v0::{CommitRequest as V0CommitRequest, CommitResponse as V0CommitResponse};
use loon_api::{payload_checksum_sha256, NamespaceId};
use loon_core::{
    commit::CommitHeadPublishError, commit_operations_batch, CoreError, MutationContext,
};
use loon_objectstore::ObjectStore;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, Notify};
use tokio::time::{Duration, Instant};

type SharedStore = Arc<dyn ObjectStore + Send + Sync>;
type CommitResult = Result<V0CommitResponse, CoreError>;

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
        request: V0CommitRequest,
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
        publisher.submit(request).await
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
    request: V0CommitRequest,
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

    async fn submit(&self, request: V0CommitRequest) -> CommitResult {
        let fingerprint = semantic_fingerprint(&self.namespace_id, &request)
            .map_err(|err| CoreError::Store(err.to_string()))?;
        let (sender, receiver) = oneshot::channel();
        self.admit(request, fingerprint, sender)?;
        receiver
            .await
            .unwrap_or_else(|_| Err(CoreError::Store("publisher task stopped".to_owned())))
    }

    fn admit(
        &self,
        request: V0CommitRequest,
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
            if let Some(existing) = state.in_flight.get_mut(&request.request_id) {
                if existing.fingerprint != fingerprint {
                    return Err(CoreError::RequestIdConflict(request.request_id));
                }
                existing.waiters.push(waiter);
                return Ok(());
            }

            if state.publishing {
                return Err(CoreError::CommitQueueFull);
            }

            if state.batch.is_none() {
                should_spawn = true;
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
                    request_id: request.request_id.clone(),
                    request: request.clone(),
                });
                (batch.candidates.len(), batch.notify.clone())
            };
            state.in_flight.insert(
                request.request_id,
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
            let publisher = self.clone();
            tokio::spawn(async move {
                publisher.publish_open_batch().await;
            });
        }
        if let Some(notify) = notify_full {
            notify.notify_one();
        }
        Ok(())
    }

    async fn publish_open_batch(self) {
        let notify = {
            let state = self
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            state
                .batch
                .as_ref()
                .map(|batch| batch.notify.clone())
                .expect("publish task requires an open batch")
        };

        tokio::select! {
            _ = tokio::time::sleep(COALESCING_DELAY) => {}
            _ = notify.notified() => {}
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
            state
                .batch
                .take()
                .map(|batch| batch.candidates)
                .unwrap_or_default()
        };

        let mut results = Vec::new();
        for attempt in 0..HEAD_CAS_RETRY_LIMIT {
            let requests = candidates
                .iter()
                .map(|candidate| candidate.request.clone())
                .collect::<Vec<_>>();
            let namespace_id = self.namespace_id.clone();
            let store = self.store.clone();
            let context = mutation_context(&self.config);
            results = tokio::task::spawn_blocking(move || {
                commit_operations_batch(store.as_ref(), &namespace_id, requests, &context)
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
        {
            let mut state = self
                .state
                .lock()
                .expect("namespace publisher mutex poisoned");
            state.publishing = false;
            for (candidate, result) in candidates.into_iter().zip(results.into_iter()) {
                if let Some(in_flight) = state.in_flight.remove(&candidate.request_id) {
                    for waiter in in_flight.waiters {
                        deliveries.push((waiter, result.clone()));
                    }
                }
            }
        }

        for (waiter, result) in deliveries {
            let _ = waiter.send(result);
        }
    }
}

fn is_head_publish_stale(result: &CommitResult) -> bool {
    matches!(
        result,
        Err(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
    )
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
    request: &V0CommitRequest,
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
    use loon_core::bootstrap_namespace;
    use loon_objectstore::fs::LocalFsStore;
    use tempfile::tempdir;

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
}
