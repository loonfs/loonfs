use crate::commit::{semantic_commit_fingerprint_for_v0_request, SemanticCommitFingerprint};
use crate::content::ContentValidationKey;
use crate::context::MutationContext;
use crate::error::{CoreError, CoreErrorKind};
use crate::path::intent::PathMutationIntent;
use crate::path::planner::{
    semantic_commit_fingerprint_for_path_intent, PathPlanner, PlannedPathMutation,
};
use crate::{
    load_namespace_head_identity, load_namespace_lease_control, load_verified_namespace_basis,
    BasisLoadError, VerifiedNamespaceBasis,
};
use loon_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loon_api::{CommitId, MutationResult, NamespaceId};
use loon_objectstore::ObjectStore;
use std::sync::Arc;

const DEFAULT_STALE_HEAD_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceMutationCandidate {
    Commit(ApiCommitRequest),
    Path(PathMutationIntent),
}

impl NamespaceMutationCandidate {
    pub fn commit_id(&self) -> &CommitId {
        match self {
            Self::Commit(request) => &request.commit_id,
            Self::Path(intent) => intent.commit_id(),
        }
    }

    pub fn semantic_commit_fingerprint(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<SemanticCommitFingerprint, CoreError> {
        match self {
            Self::Commit(request) => {
                semantic_commit_fingerprint_for_v0_request(namespace_id, request)
                    .map_err(|err| CoreError::Store(err.to_string()))
            }
            Self::Path(intent) => semantic_commit_fingerprint_for_path_intent(namespace_id, intent),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmBasisEvent {
    Disabled,
    ColdLoaded,
    Reused,
    InvalidatedThenColdLoaded,
}

#[derive(Debug, Clone)]
pub struct NamespaceCommitEnginePublishResult {
    pub results: Vec<Result<ApiCommitResponse, CoreError>>,
    pub warm_basis_event: WarmBasisEvent,
    pub warm_basis_update: WarmBasisUpdate,
}

#[derive(Debug, Clone)]
pub enum WarmBasisUpdate {
    NoChange,
    Retained(VerifiedNamespaceBasis),
    Advanced(VerifiedNamespaceBasis),
    Invalidated,
}

impl WarmBasisUpdate {
    pub fn basis(&self) -> Option<&VerifiedNamespaceBasis> {
        match self {
            Self::Retained(basis) | Self::Advanced(basis) => Some(basis),
            Self::NoChange | Self::Invalidated => None,
        }
    }

    pub fn is_advanced(&self) -> bool {
        matches!(self, Self::Advanced(_))
    }

    pub fn is_invalidated(&self) -> bool {
        matches!(self, Self::Invalidated)
    }
}

#[derive(Debug, Clone)]
pub struct NamespaceCommitEngine {
    namespace_id: NamespaceId,
    basis: Option<Arc<VerifiedNamespaceBasis>>,
}

impl NamespaceCommitEngine {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            basis: None,
        }
    }

    pub fn invalidate(&mut self) {
        self.basis = None;
    }

    pub fn publish_batch<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: Vec<NamespaceMutationCandidate>,
        context: &MutationContext,
    ) -> NamespaceCommitEnginePublishResult {
        self.publish_batch_with_content_validation_hints(store, candidates, context, &[])
    }

    pub fn publish_batch_with_content_validation_hints<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: Vec<NamespaceMutationCandidate>,
        context: &MutationContext,
        trusted_content_validations: &[ContentValidationKey],
    ) -> NamespaceCommitEnginePublishResult {
        if candidates.is_empty() {
            return NamespaceCommitEnginePublishResult {
                results: Vec::new(),
                warm_basis_event: WarmBasisEvent::Disabled,
                warm_basis_update: self
                    .basis
                    .as_deref()
                    .cloned()
                    .map(WarmBasisUpdate::Retained)
                    .unwrap_or(WarmBasisUpdate::NoChange),
            };
        }

        let candidate_count = candidates.len();
        if let Err(error) =
            crate::acquire_or_renew_namespace_lease(store, &self.namespace_id, context)
        {
            self.invalidate();
            return NamespaceCommitEnginePublishResult {
                results: repeated_error(candidate_count, CoreError::Lease(error)),
                warm_basis_event: WarmBasisEvent::Disabled,
                warm_basis_update: WarmBasisUpdate::Invalidated,
            };
        }

        let (basis, warm_basis_event) = match self.basis_for_publish(store) {
            Ok(value) => value,
            Err(error) => {
                self.invalidate();
                return NamespaceCommitEnginePublishResult {
                    results: repeated_error(candidate_count, CoreError::Basis(error)),
                    warm_basis_event: WarmBasisEvent::Disabled,
                    warm_basis_update: WarmBasisUpdate::Invalidated,
                };
            }
        };

        let published = crate::protocol::publish_namespace_mutations_batch_against_basis(
            store,
            &self.namespace_id,
            &candidates,
            context,
            &basis,
            trusted_content_validations,
        );
        if should_retry_reused_warm_rejection(warm_basis_event, &published) {
            self.invalidate();
            let cold_basis = match load_verified_namespace_basis(store, &self.namespace_id) {
                Ok(value) => value,
                Err(error) => {
                    return NamespaceCommitEnginePublishResult {
                        results: repeated_error(candidate_count, CoreError::Basis(error)),
                        warm_basis_event: WarmBasisEvent::InvalidatedThenColdLoaded,
                        warm_basis_update: WarmBasisUpdate::Invalidated,
                    };
                }
            };
            let retried = crate::protocol::publish_namespace_mutations_batch_against_basis(
                store,
                &self.namespace_id,
                &candidates,
                context,
                &cold_basis,
                trusted_content_validations,
            );
            return self.finish_publish_result(retried, WarmBasisEvent::InvalidatedThenColdLoaded);
        }
        self.finish_publish_result(published, warm_basis_event)
    }

    fn finish_publish_result(
        &mut self,
        published: crate::protocol::PublishBatchAgainstBasisResult,
        warm_basis_event: WarmBasisEvent,
    ) -> NamespaceCommitEnginePublishResult {
        let warm_basis_update = match published.basis_promotion {
            crate::protocol::BasisPromotion::Unchanged(basis) => {
                self.basis = Some(Arc::new(basis.clone()));
                WarmBasisUpdate::Retained(basis)
            }
            crate::protocol::BasisPromotion::Advanced(basis) => {
                self.basis = Some(Arc::new(basis.clone()));
                WarmBasisUpdate::Advanced(basis)
            }
            crate::protocol::BasisPromotion::NotCacheable => {
                self.invalidate();
                WarmBasisUpdate::Invalidated
            }
        };

        NamespaceCommitEnginePublishResult {
            results: published.results,
            warm_basis_event,
            warm_basis_update,
        }
    }

    fn basis_for_publish<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
    ) -> Result<(VerifiedNamespaceBasis, WarmBasisEvent), BasisLoadError> {
        let mut invalidated = false;
        if let Some(cached) = self.basis.clone() {
            match load_namespace_head_identity(store, &self.namespace_id) {
                Ok(identity) if identity.head_etag == cached.head_etag => {
                    match load_namespace_lease_control(store, &self.namespace_id) {
                        Ok(lease) => {
                            let mut basis = cached.as_ref().clone();
                            basis.lease = lease.state;
                            return Ok((basis, WarmBasisEvent::Reused));
                        }
                        Err(_) => {
                            self.invalidate();
                            invalidated = true;
                        }
                    }
                }
                Ok(_) | Err(_) => {
                    self.invalidate();
                    invalidated = true;
                }
            }
        }

        let basis = load_verified_namespace_basis(store, &self.namespace_id)?;
        let event = if invalidated {
            WarmBasisEvent::InvalidatedThenColdLoaded
        } else {
            WarmBasisEvent::ColdLoaded
        };
        Ok((basis, event))
    }
}

fn should_retry_reused_warm_rejection(
    warm_basis_event: WarmBasisEvent,
    published: &crate::protocol::PublishBatchAgainstBasisResult,
) -> bool {
    warm_basis_event == WarmBasisEvent::Reused
        && matches!(
            published.basis_promotion,
            crate::protocol::BasisPromotion::Unchanged(_)
        )
        && published.results.iter().any(Result::is_err)
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

    pub fn plan_path_intent(
        &self,
        namespace_id: &NamespaceId,
        intent: &PathMutationIntent,
    ) -> Result<PlannedPathMutation, CoreError> {
        PathPlanner::new(self.store).plan_against_basis(namespace_id, intent)
    }

    pub fn submit_path_intent(
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
            );
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
                Err(error)
                    if error.kind() == CoreErrorKind::StaleHead && attempt + 1 < attempts =>
                {
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            CoreError::Store("path mutation stale-head retry exhausted".to_owned())
        }))
    }

    pub fn submit_commit_request(
        &self,
        namespace_id: &NamespaceId,
        request: ApiCommitRequest,
        context: &MutationContext,
    ) -> Result<ApiCommitResponse, CoreError> {
        crate::protocol::commit_operations(self.store, namespace_id, request, context)
    }

    pub fn submit_commit_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<ApiCommitRequest>,
        context: &MutationContext,
    ) -> Vec<Result<ApiCommitResponse, CoreError>> {
        crate::protocol::commit_operations_batch(self.store, namespace_id, requests, context)
    }
}

pub fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    publish_namespace_mutations_batch_with_content_validation_hints(
        store,
        namespace_id,
        candidates,
        context,
        &[],
    )
}

pub fn publish_namespace_mutations_batch_with_content_validation_hints<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
    trusted_content_validations: &[ContentValidationKey],
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    engine
        .publish_batch_with_content_validation_hints(
            store,
            candidates,
            context,
            trusted_content_validations,
        )
        .results
}
