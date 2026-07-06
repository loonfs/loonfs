use crate::commit::{core_commit_fingerprint_for_v0_request, SemanticMutationIdentity};
use crate::content::ContentAdmission;
use crate::context::MutationContext;
use crate::error::{CoreError, ErrorCode, MetadataViewError, Result};
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::path::write::planner::plan_path_mutation_against_publish_view;
use crate::path::write::{
    path_intent_fingerprint_for_path_intent, PathMutationIntent, PlannedPathMutation,
    PublishPlanningSession,
};
use crate::protocol::{load_publish_metadata_view, PublishTailOptions, PublishTailProjection};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loonfs_api::wire::control::AcquiredWriter;
use loonfs_api::{CommitId, MutationResult, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

const DEFAULT_STALE_HEAD_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceMutationCandidate {
    Commit(ApiCommitRequest),
    Path(PathMutationIntent),
    PathWithContentAdmission {
        intent: PathMutationIntent,
        admissions: Vec<ContentAdmission>,
    },
}

impl NamespaceMutationCandidate {
    pub fn commit_id(&self) -> &CommitId {
        match self {
            Self::Commit(request) => &request.commit_id,
            Self::Path(intent) | Self::PathWithContentAdmission { intent, .. } => {
                intent.commit_id()
            }
        }
    }

    pub fn semantic_identity(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<SemanticMutationIdentity> {
        match self {
            Self::Commit(request) => core_commit_fingerprint_for_v0_request(namespace_id, request)
                .map(SemanticMutationIdentity::CoreCommit)
                .map_err(|err| {
                    CoreError::Internal(format!("failed to fingerprint commit request: {err}"))
                }),
            Self::Path(intent) | Self::PathWithContentAdmission { intent, .. } => {
                path_intent_fingerprint_for_path_intent(namespace_id, intent)
                    .map(SemanticMutationIdentity::PathIntent)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPolicy {
    Immediate,
    Coalesce { max_delay_ms: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishOptions {
    pub flush: FlushPolicy,
    pub stale_head_retry_limit: usize,
}

impl Default for PublishOptions {
    fn default() -> Self {
        Self {
            flush: FlushPolicy::Immediate,
            stale_head_retry_limit: DEFAULT_STALE_HEAD_RETRY_LIMIT,
        }
    }
}

/// Publishes stop being accepted once the WAL tail is far past the default
/// maintenance checkpoint threshold (4x at defaults). Reads never gate; this
/// only asks writers to wait for the maintenance a deployment failed to run
/// (format spec, "Maintenance operations").
pub(crate) const WAL_TAIL_BACKPRESSURE_SEGMENTS: u64 = 128;

#[derive(Debug, Clone)]
pub struct NamespaceCommitEnginePublishResult {
    pub results: Vec<Result<ApiCommitResponse>>,
    /// WAL tail length observed by this publish, for opportunistic
    /// maintenance scheduling. Zero when no projection was loaded.
    pub wal_tail_segments: u64,
}

#[derive(Debug, Clone)]
pub struct NamespaceCommitEngine {
    namespace_id: NamespaceId,
    publish_tail_projection: Option<PublishTailProjection>,
    /// Epoch acquired lazily on this session's first publish and reused for
    /// its lifetime; no per-publish acquisition CAS.
    acquired_writer: Option<AcquiredWriter>,
    /// Terminal fencing record. Once another session supersedes our epoch,
    /// every later publish fails with `writer_fenced` without touching the
    /// store; the session never reacquires on its own.
    fenced: Option<String>,
    /// Local monotonic source for the self-enforced publish budget.
    timer: Arc<dyn MonotonicTimer>,
}

impl NamespaceCommitEngine {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            publish_tail_projection: None,
            acquired_writer: None,
            fenced: None,
            timer: Arc::new(StdMonotonicTimer::default()),
        }
    }

    #[doc(hidden)]
    pub fn with_monotonic_timer(mut self, timer: Arc<dyn MonotonicTimer>) -> Self {
        self.timer = timer;
        self
    }

    pub fn invalidate(&mut self) {
        // Drops only the tail projection. The acquired epoch is a number
        // whose validity is re-checked against the head on every publish
        // view load, and a fenced session stays fenced.
        self.publish_tail_projection = None;
    }

    pub async fn publish_batch<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: Vec<NamespaceMutationCandidate>,
        context: &MutationContext,
    ) -> NamespaceCommitEnginePublishResult {
        self.publish_batch_with_tail_options(
            store,
            candidates,
            context,
            &PublishTailOptions::default(),
        )
        .await
    }

    pub(crate) async fn publish_batch_with_tail_options<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: Vec<NamespaceMutationCandidate>,
        context: &MutationContext,
        tail_options: &PublishTailOptions,
    ) -> NamespaceCommitEnginePublishResult {
        if candidates.is_empty() {
            return NamespaceCommitEnginePublishResult {
                results: Vec::new(),
                wal_tail_segments: 0,
            };
        }

        let candidate_count = candidates.len();
        if let Some(message) = &self.fenced {
            return NamespaceCommitEnginePublishResult {
                results: repeated_error(candidate_count, CoreError::WriterFenced(message.clone())),
                wal_tail_segments: 0,
            };
        }
        let acquired_writer = match &self.acquired_writer {
            Some(value) => value.clone(),
            None => match acquire_writer_epoch(store, &self.namespace_id, context).await {
                Ok(value) => {
                    self.acquired_writer = Some(value.clone());
                    value
                }
                Err(error) => {
                    return NamespaceCommitEnginePublishResult {
                        results: repeated_error(candidate_count, CoreError::WriterEpoch(error)),
                        wal_tail_segments: 0,
                    };
                }
            },
        };

        let (publish_view, projection) = match load_publish_metadata_view(
            store,
            &self.namespace_id,
            Some(acquired_writer),
            self.publish_tail_projection.as_ref(),
            tail_options,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                self.invalidate();
                if let CoreError::WriterFenced(message) = &error {
                    self.fenced = Some(message.clone());
                    self.acquired_writer = None;
                }
                return NamespaceCommitEnginePublishResult {
                    results: repeated_error(candidate_count, error),
                    wal_tail_segments: 0,
                };
            }
        };

        if projection.wal_tail_segments > WAL_TAIL_BACKPRESSURE_SEGMENTS {
            let wal_tail_segments = projection.wal_tail_segments;
            self.publish_tail_projection = Some(projection);
            let error = MetadataViewError::MaintenanceRequired {
                namespace_id: self.namespace_id.clone(),
                reason: format!(
                    "wal tail has {wal_tail_segments} segments; publishes resume once maintenance brings it back under {WAL_TAIL_BACKPRESSURE_SEGMENTS}"
                ),
            };
            return NamespaceCommitEnginePublishResult {
                results: repeated_error(candidate_count, CoreError::from(error)),
                wal_tail_segments,
            };
        }

        let published = crate::protocol::publish_namespace_mutations_batch_against_publish_view(
            store,
            &self.namespace_id,
            &candidates,
            context,
            &publish_view,
            self.timer.as_ref(),
        )
        .await;
        let wal_tail_segments =
            self.update_publish_tail_projection(projection, &published, tail_options);
        NamespaceCommitEnginePublishResult {
            results: published.results,
            wal_tail_segments,
        }
    }

    pub async fn publish_batch_with_tail_cache_limits<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: Vec<NamespaceMutationCandidate>,
        context: &MutationContext,
        max_tail_rows: usize,
        max_tail_decoded_bytes: Option<usize>,
    ) -> NamespaceCommitEnginePublishResult {
        let options = PublishTailOptions {
            max_tail_rows,
            max_tail_decoded_bytes,
        };
        self.publish_batch_with_tail_options(store, candidates, context, &options)
            .await
    }

    fn update_publish_tail_projection(
        &mut self,
        mut projection: PublishTailProjection,
        published: &crate::protocol::PublishBatchAgainstViewResult,
        tail_options: &PublishTailOptions,
    ) -> u64 {
        if !published.published_records.is_empty() {
            projection.wal_tail_segments = projection.wal_tail_segments.saturating_add(1);
        }
        let wal_tail_segments = projection.wal_tail_segments;
        let Some(resulting_head) = published.resulting_head.clone() else {
            if published.can_reuse_loaded_projection {
                self.publish_tail_projection = Some(projection);
            } else {
                self.invalidate();
            }
            return wal_tail_segments;
        };
        let Some(resulting_head_etag) = published.resulting_head_etag.clone() else {
            self.invalidate();
            return wal_tail_segments;
        };
        for record in &published.published_records {
            if projection
                .tail_state
                .apply_committed_wal_record_mut(record)
                .is_err()
            {
                self.invalidate();
                return wal_tail_segments;
            }
        }
        projection.head_seq = resulting_head.seq;
        projection.head_etag = resulting_head_etag;
        if projection.within_limits(tail_options) {
            self.publish_tail_projection = Some(projection);
        } else {
            self.invalidate();
        }
        wal_tail_segments
    }
}

fn repeated_error(count: usize, error: CoreError) -> Vec<Result<ApiCommitResponse>> {
    (0..count).map(|_| Err(error.clone())).collect()
}

pub struct DirectObjectStorePublisher<'a, S: ObjectStore + ?Sized> {
    store: &'a S,
}

impl<'a, S: ObjectStore + ?Sized> DirectObjectStorePublisher<'a, S> {
    pub fn new(store: &'a S) -> Self {
        Self { store }
    }

    pub async fn plan_path_intent(
        &self,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
    ) -> Result<PlannedPathMutation> {
        let (view, _projection) = load_publish_metadata_view(
            self.store,
            namespace_id,
            None,
            None,
            &PublishTailOptions::default(),
        )
        .await?;
        let session = PublishPlanningSession::new(view.head());
        let base_view = view.metadata_view();
        let metadata_view = base_view.with_overlay(session.accepted_rows(), session.head().seq);
        plan_path_mutation_against_publish_view(
            namespace_id,
            intent,
            session.head(),
            &metadata_view,
        )
        .await
    }

    pub async fn submit_path_intent(
        &self,
        namespace_id: &NamespaceId,
        intent: PathMutationIntent,
        context: &MutationContext,
        options: PublishOptions,
    ) -> Result<MutationResult> {
        let attempts = options.stale_head_retry_limit.max(1);
        let mut last_error = None;

        for attempt in 0..attempts {
            let mut results = publish_namespace_mutations_batch(
                self.store,
                namespace_id,
                vec![NamespaceMutationCandidate::Path(intent.clone())],
                context,
            )
            .await;
            let result = results.pop().unwrap_or_else(|| {
                Err(CoreError::Internal("empty path mutation batch".to_owned()))
            });
            match result {
                Ok(response) => {
                    return Ok(MutationResult {
                        namespace_id: response.namespace_id,
                        committed_seq: response.committed_seq,
                    });
                }
                Err(error) if error.code() == ErrorCode::StaleHead && attempt + 1 < attempts => {
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            CoreError::Internal("path mutation stale-head retry exhausted".to_owned())
        }))
    }

    pub async fn submit_commit_request(
        &self,
        namespace_id: &NamespaceId,
        request: ApiCommitRequest,
        context: &MutationContext,
    ) -> Result<ApiCommitResponse> {
        crate::protocol::commit_operations(self.store, namespace_id, request, context).await
    }

    pub async fn submit_commit_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<ApiCommitRequest>,
        context: &MutationContext,
    ) -> Vec<Result<ApiCommitResponse>> {
        crate::protocol::commit_operations_batch(self.store, namespace_id, requests, context).await
    }
}

pub(crate) async fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse>> {
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    engine
        .publish_batch(store, candidates, context)
        .await
        .results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::control::read_head_object;
    use futures::StreamExt;
    use loonfs_api::{ChangeSeq, InodeId, WriterEpoch};
    use loonfs_objectstore::fs::LocalFsStore;
    use loonfs_objectstore::keys::wal_segment_prefix;
    use loonfs_objectstore::ObjectStore;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::tempdir;

    fn context(writer_id: &str, writer_session_id: &str) -> MutationContext {
        MutationContext {
            writer_id: writer_id.to_owned(),
            writer_session_id: writer_session_id.to_owned(),
            writer_version: "publisher-test/0.1.0".to_owned(),
            now_ms: 1_000,
        }
    }

    fn create_dir(commit_id: &str, display_name: &str) -> NamespaceMutationCandidate {
        NamespaceMutationCandidate::Commit(ApiCommitRequest {
            commit_id: CommitId::parse(commit_id).expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![loonfs_api::v0::CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: display_name.to_owned(),
            }],
            message: None,
        })
    }

    async fn wal_segment_count(store: &LocalFsStore, namespace_id: &NamespaceId) -> usize {
        store
            .list_prefix_stream(&wal_segment_prefix(namespace_id.as_str()))
            .collect::<Vec<_>>()
            .await
            .len()
    }

    #[tokio::test]
    async fn commit_engine_is_terminally_fenced_after_takeover() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer_a = context("writer-a", "session-a");
        bootstrap_namespace(&store, &namespace_id, &writer_a, false)
            .await
            .expect("bootstrap");

        let mut engine_a = NamespaceCommitEngine::new(namespace_id.clone());
        let first = engine_a
            .publish_batch(&store, vec![create_dir("from-a-first", "alpha")], &writer_a)
            .await;
        first.results[0].as_ref().expect("writer a first commit");

        // Writer B's session acquires the epoch; A's cached epoch is now
        // superseded.
        let writer_b = context("writer-b", "session-b");
        let mut engine_b = NamespaceCommitEngine::new(namespace_id.clone());
        let takeover = engine_b
            .publish_batch(&store, vec![create_dir("from-b-first", "beta")], &writer_b)
            .await;
        takeover.results[0]
            .as_ref()
            .expect("writer b takeover commit");
        let epoch_after_takeover = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state
            .writer_epoch;

        // A is fenced terminally: both attempts fail with writer_fenced, the
        // second without ever reaching the store, and the session never
        // bumps the epoch back.
        for attempt in 0..2 {
            let fenced = engine_a
                .publish_batch(
                    &store,
                    vec![create_dir("from-a-second", "gamma")],
                    &writer_a,
                )
                .await;
            let error = fenced.results[0].as_ref().expect_err("fenced publish");
            assert_eq!(error.code(), ErrorCode::WriterFenced, "attempt {attempt}");
        }
        let head = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head.writer_epoch, epoch_after_takeover);
        assert_eq!(head.writer.expect("writer block").writer_id, "writer-b");
    }

    /// Advances an entire publish budget per reading, so every publish
    /// observes an expired budget between segment PUT and head CAS.
    #[derive(Debug)]
    struct ExpiredBudgetTimer(AtomicU64);

    impl MonotonicTimer for ExpiredBudgetTimer {
        fn monotonic_now_ms(&self) -> u64 {
            self.0
                .fetch_add(crate::protocol::PUBLISH_BUDGET_MS + 1_000, Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn publish_over_budget_abandons_the_segment_and_a_retry_rebuilds() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = context("writer-a", "session-a");
        bootstrap_namespace(&store, &namespace_id, &writer, false)
            .await
            .expect("bootstrap");
        let head_before = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;

        let mut over_budget = NamespaceCommitEngine::new(namespace_id.clone())
            .with_monotonic_timer(Arc::new(ExpiredBudgetTimer(AtomicU64::new(0))));
        let abandoned = over_budget
            .publish_batch(&store, vec![create_dir("budgeted", "alpha")], &writer)
            .await;
        let error = abandoned.results[0]
            .as_ref()
            .expect_err("over-budget publish must abandon");
        assert!(
            matches!(
                error,
                CoreError::HeadPublish(
                    crate::commit::CommitHeadPublishError::PublishBudgetExceeded { .. }
                )
            ),
            "unexpected error: {error:?}"
        );
        // Retryable exactly like a stale head, so existing retry loops
        // rebuild the commit.
        assert_eq!(error.code(), ErrorCode::StaleHead);

        // The head did not advance; the written segment is an orphan for GC.
        let head_after = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head_after.seq, head_before.seq);
        assert_eq!(head_after.visible_wal_tip, head_before.visible_wal_tip);
        assert_eq!(wal_segment_count(&store, &namespace_id).await, 1);

        // A retry with a healthy budget republishes the same commit as a
        // fresh segment; the orphan stays behind.
        let mut healthy = NamespaceCommitEngine::new(namespace_id.clone());
        let retried = healthy
            .publish_batch(&store, vec![create_dir("budgeted", "alpha")], &writer)
            .await;
        let response = retried.results[0].as_ref().expect("rebuilt publish");
        assert_eq!(response.committed_seq, ChangeSeq(1));
        assert_eq!(wal_segment_count(&store, &namespace_id).await, 2);
        let head_final = read_head_object(&store, &namespace_id)
            .await
            .expect("read head")
            .envelope
            .state;
        assert_eq!(head_final.seq, ChangeSeq(1));
        assert_eq!(head_final.writer_epoch, WriterEpoch(0));
    }
}
