use crate::commit::{core_commit_fingerprint_for_v0_request, SemanticMutationIdentity};
use crate::content::ContentAdmission;
use crate::context::MutationContext;
use crate::error::{CoreError, ErrorCode};
use crate::namespace::lease::acquire_or_renew_namespace_lease;
use crate::path::write::{
    path_intent_fingerprint_for_path_intent, PathMutationIntent, PathPlanner, PlannedPathMutation,
};
use crate::protocol::{load_publish_validation_basis, PublishTailOptions, PublishTailProjection};
use loonfs_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loonfs_api::{CommitId, MutationResult, NamespaceId};
use loonfs_objectstore::ObjectStore;

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
    ) -> Result<SemanticMutationIdentity, CoreError> {
        match self {
            Self::Commit(request) => core_commit_fingerprint_for_v0_request(namespace_id, request)
                .map(SemanticMutationIdentity::CoreCommit)
                .map_err(|err| CoreError::Store(err.to_string())),
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

#[derive(Debug, Clone)]
pub struct NamespaceCommitEnginePublishResult {
    pub results: Vec<Result<ApiCommitResponse, CoreError>>,
}

#[derive(Debug, Clone)]
pub struct NamespaceCommitEngine {
    namespace_id: NamespaceId,
    publish_tail_projection: Option<PublishTailProjection>,
}

impl NamespaceCommitEngine {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            publish_tail_projection: None,
        }
    }

    pub fn invalidate(&mut self) {
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
            };
        }

        let candidate_count = candidates.len();
        if let Err(error) =
            acquire_or_renew_namespace_lease(store, &self.namespace_id, context).await
        {
            return NamespaceCommitEnginePublishResult {
                results: repeated_error(candidate_count, CoreError::Lease(error)),
            };
        }

        let (basis, projection) = match load_publish_validation_basis(
            store,
            &self.namespace_id,
            self.publish_tail_projection.as_ref(),
            tail_options,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                self.invalidate();
                return NamespaceCommitEnginePublishResult {
                    results: repeated_error(candidate_count, error),
                };
            }
        };

        let published = crate::protocol::publish_namespace_mutations_batch_against_basis(
            store,
            &self.namespace_id,
            &candidates,
            context,
            &basis,
        )
        .await;
        self.update_publish_tail_projection(projection, &published, tail_options);
        NamespaceCommitEnginePublishResult {
            results: published.results,
        }
    }

    pub async fn publish_batch_with_tail_limits<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: Vec<NamespaceMutationCandidate>,
        context: &MutationContext,
        max_wal_tail_segments: u64,
        max_tail_rows: usize,
        max_tail_decoded_bytes: Option<usize>,
    ) -> NamespaceCommitEnginePublishResult {
        let options = PublishTailOptions {
            max_wal_tail_segments,
            max_tail_rows,
            max_tail_decoded_bytes,
        };
        self.publish_batch_with_tail_options(store, candidates, context, &options)
            .await
    }

    fn update_publish_tail_projection(
        &mut self,
        mut projection: PublishTailProjection,
        published: &crate::protocol::PublishBatchAgainstBasisResult,
        tail_options: &PublishTailOptions,
    ) {
        let Some(resulting_head) = published.resulting_head.clone() else {
            if published.can_reuse_loaded_projection {
                self.publish_tail_projection = Some(projection);
            } else {
                self.invalidate();
            }
            return;
        };
        let Some(resulting_head_etag) = published.resulting_head_etag.clone() else {
            self.invalidate();
            return;
        };
        if resulting_head.current_manifest_id != Some(projection.manifest_id) {
            self.invalidate();
            return;
        }
        for record in &published.published_records {
            if projection
                .tail_state
                .apply_committed_wal_record_mut(record)
                .is_err()
            {
                self.invalidate();
                return;
            }
        }
        projection.head_seq = resulting_head.seq;
        projection.head_etag = resulting_head_etag;
        projection.wal_tail_segments = projection.wal_tail_segments.saturating_add(1);
        if projection.within_limits(tail_options) {
            self.publish_tail_projection = Some(projection);
        } else {
            self.invalidate();
        }
    }
}

fn repeated_error(count: usize, error: CoreError) -> Vec<Result<ApiCommitResponse, CoreError>> {
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
    ) -> Result<PlannedPathMutation, CoreError> {
        PathPlanner::new(self.store)
            .plan_against_basis(namespace_id, intent)
            .await
    }

    pub async fn submit_path_intent(
        &self,
        namespace_id: &NamespaceId,
        intent: PathMutationIntent,
        context: &MutationContext,
        options: PublishOptions,
    ) -> Result<MutationResult, CoreError> {
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
            let result = results
                .pop()
                .unwrap_or_else(|| Err(CoreError::Store("empty path mutation batch".to_owned())));
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
            CoreError::Store("path mutation stale-head retry exhausted".to_owned())
        }))
    }

    pub async fn submit_commit_request(
        &self,
        namespace_id: &NamespaceId,
        request: ApiCommitRequest,
        context: &MutationContext,
    ) -> Result<ApiCommitResponse, CoreError> {
        crate::protocol::commit_operations(self.store, namespace_id, request, context).await
    }

    pub async fn submit_commit_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<ApiCommitRequest>,
        context: &MutationContext,
    ) -> Vec<Result<ApiCommitResponse, CoreError>> {
        crate::protocol::commit_operations_batch(self.store, namespace_id, requests, context).await
    }
}

pub(crate) async fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    engine
        .publish_batch(store, candidates, context)
        .await
        .results
}
