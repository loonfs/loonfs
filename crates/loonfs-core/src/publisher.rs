use crate::commit::{
    core_commit_fingerprint_for_v0_request, CommitHeadPublishError, SemanticMutationIdentity,
};
use crate::context::MutationContext;
use crate::error::{CoreError, ErrorCode};
use crate::namespace::basis::{
    load_verified_namespace_basis, probe_namespace_head_etag, BasisLoadError,
    NamespaceHeadEtagProbe, VerifiedNamespaceBasis, VerifiedNamespaceBasisWeight,
};
use crate::namespace::control::load_namespace_lease_control;
use crate::namespace::lease::acquire_or_renew_namespace_lease;
use crate::path::write::{
    path_intent_fingerprint_for_path_intent, PathMutationIntent, PathPlanner, PlannedPathMutation,
};
use loonfs_api::v0::{CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse};
use loonfs_api::wire::control::LeaseState;
use loonfs_api::{CommitId, ContentRef, MutationResult, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

const DEFAULT_STALE_HEAD_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceMutationCandidate {
    Commit(ApiCommitRequest),
    Path(PathMutationIntent),
    AdmittedPath {
        intent: PathMutationIntent,
        admitted_content_refs: Vec<ContentRef>,
    },
}

impl NamespaceMutationCandidate {
    pub fn commit_id(&self) -> &CommitId {
        match self {
            Self::Commit(request) => &request.commit_id,
            Self::Path(intent) | Self::AdmittedPath { intent, .. } => intent.commit_id(),
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
            Self::Path(intent) | Self::AdmittedPath { intent, .. } => {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasisReuseEvent {
    Disabled,
    ColdLoaded,
    ReusedAfterHeadEtagMatch,
    InvalidatedThenColdLoaded,
}

#[derive(Debug, Clone)]
pub struct NamespaceCommitEnginePublishResult {
    pub results: Vec<Result<ApiCommitResponse, CoreError>>,
    pub basis_reuse_event: BasisReuseEvent,
    pub verified_basis_cache_update: VerifiedBasisCacheUpdate,
}

#[derive(Debug, Clone)]
pub enum VerifiedBasisCacheUpdate {
    NoChange,
    ReusableAfterHeadEtagMatch(Arc<VerifiedNamespaceBasis>),
    AdvancedAfterHeadCas(Arc<VerifiedNamespaceBasis>),
    Invalidated,
}

impl VerifiedBasisCacheUpdate {
    pub fn verified_basis_to_cache(&self) -> Option<Arc<VerifiedNamespaceBasis>> {
        match self {
            Self::ReusableAfterHeadEtagMatch(basis) | Self::AdvancedAfterHeadCas(basis) => {
                Some(Arc::clone(basis))
            }
            Self::NoChange | Self::Invalidated => None,
        }
    }

    pub fn is_advanced(&self) -> bool {
        matches!(self, Self::AdvancedAfterHeadCas(_))
    }

    pub fn is_invalidated(&self) -> bool {
        matches!(self, Self::Invalidated)
    }
}

#[derive(Debug, Clone)]
pub struct NamespaceCommitEngine {
    namespace_id: NamespaceId,
    basis: Option<CachedVerifiedBasis>,
}

#[derive(Debug, Clone)]
struct CachedVerifiedBasis {
    basis: Arc<VerifiedNamespaceBasis>,
    head_etag_reuse_token: String,
    weight: VerifiedNamespaceBasisWeight,
}

impl CachedVerifiedBasis {
    fn from_arc(basis: Arc<VerifiedNamespaceBasis>) -> Self {
        Self {
            head_etag_reuse_token: basis.head_etag.clone(),
            weight: basis.weight(),
            basis,
        }
    }

    fn matches_head_etag_probe(&self, probe: &NamespaceHeadEtagProbe) -> bool {
        self.head_etag_reuse_token == probe.head_etag
    }

    fn weight(&self) -> VerifiedNamespaceBasisWeight {
        self.weight
    }

    fn basis_to_reuse_with_refreshed_lease(&self, lease: LeaseState) -> VerifiedNamespaceBasis {
        let mut basis = self.basis.as_ref().clone();
        basis.lease = lease;
        basis
    }

    #[cfg(test)]
    fn verified_basis(&self) -> &VerifiedNamespaceBasis {
        self.basis.as_ref()
    }
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

    pub fn cached_basis_weight(&self) -> Option<VerifiedNamespaceBasisWeight> {
        self.basis.as_ref().map(CachedVerifiedBasis::weight)
    }

    pub async fn publish_batch<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        candidates: Vec<NamespaceMutationCandidate>,
        context: &MutationContext,
    ) -> NamespaceCommitEnginePublishResult {
        if candidates.is_empty() {
            return NamespaceCommitEnginePublishResult {
                results: Vec::new(),
                basis_reuse_event: BasisReuseEvent::Disabled,
                verified_basis_cache_update: VerifiedBasisCacheUpdate::NoChange,
            };
        }

        let candidate_count = candidates.len();
        if let Err(error) =
            acquire_or_renew_namespace_lease(store, &self.namespace_id, context).await
        {
            self.invalidate();
            return NamespaceCommitEnginePublishResult {
                results: repeated_error(candidate_count, CoreError::Lease(error)),
                basis_reuse_event: BasisReuseEvent::Disabled,
                verified_basis_cache_update: VerifiedBasisCacheUpdate::Invalidated,
            };
        }

        let (basis, basis_reuse_event) = match self.basis_for_publish(store).await {
            Ok(value) => value,
            Err(error) => {
                self.invalidate();
                return NamespaceCommitEnginePublishResult {
                    results: repeated_error(candidate_count, CoreError::Basis(error)),
                    basis_reuse_event: BasisReuseEvent::Disabled,
                    verified_basis_cache_update: VerifiedBasisCacheUpdate::Invalidated,
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
        if should_retry_reused_warm_stale_head(basis_reuse_event, &published) {
            self.invalidate();
            let cold_basis = match load_verified_namespace_basis(store, &self.namespace_id).await {
                Ok(value) => value,
                Err(error) => {
                    return NamespaceCommitEnginePublishResult {
                        results: repeated_error(candidate_count, CoreError::Basis(error)),
                        basis_reuse_event: BasisReuseEvent::InvalidatedThenColdLoaded,
                        verified_basis_cache_update: VerifiedBasisCacheUpdate::Invalidated,
                    };
                }
            };
            let retried = crate::protocol::publish_namespace_mutations_batch_against_basis(
                store,
                &self.namespace_id,
                &candidates,
                context,
                &cold_basis,
            )
            .await;
            return self.finish_publish_result(retried, BasisReuseEvent::InvalidatedThenColdLoaded);
        }
        self.finish_publish_result(published, basis_reuse_event)
    }

    fn finish_publish_result(
        &mut self,
        published: crate::protocol::PublishBatchAgainstBasisResult,
        basis_reuse_event: BasisReuseEvent,
    ) -> NamespaceCommitEnginePublishResult {
        let verified_basis_cache_update = match published.basis_promotion {
            crate::protocol::BasisPromotion::Unchanged(basis) => {
                self.basis = Some(CachedVerifiedBasis::from_arc(Arc::clone(&basis)));
                VerifiedBasisCacheUpdate::ReusableAfterHeadEtagMatch(basis)
            }
            crate::protocol::BasisPromotion::Advanced(basis) => {
                self.basis = Some(CachedVerifiedBasis::from_arc(Arc::clone(&basis)));
                VerifiedBasisCacheUpdate::AdvancedAfterHeadCas(basis)
            }
            crate::protocol::BasisPromotion::NotCacheable => {
                self.invalidate();
                VerifiedBasisCacheUpdate::Invalidated
            }
        };

        NamespaceCommitEnginePublishResult {
            results: published.results,
            basis_reuse_event,
            verified_basis_cache_update,
        }
    }

    async fn basis_for_publish<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
    ) -> Result<(VerifiedNamespaceBasis, BasisReuseEvent), BasisLoadError> {
        let mut invalidated = false;
        if let Some(cached) = self.basis.clone() {
            match probe_namespace_head_etag(store, &self.namespace_id).await {
                Ok(probe) if cached.matches_head_etag_probe(&probe) => {
                    // A matching ETag does not make the cache authoritative; it
                    // only proves the durable head object is unchanged since
                    // this basis was reconstructed and verified.
                    match load_namespace_lease_control(store, &self.namespace_id).await {
                        Ok(lease) => {
                            let basis = cached.basis_to_reuse_with_refreshed_lease(lease.state);
                            return Ok((basis, BasisReuseEvent::ReusedAfterHeadEtagMatch));
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

        let basis = load_verified_namespace_basis(store, &self.namespace_id).await?;
        let event = if invalidated {
            BasisReuseEvent::InvalidatedThenColdLoaded
        } else {
            BasisReuseEvent::ColdLoaded
        };
        Ok((basis, event))
    }
}

/// A reused warm basis is only probed for freshness before the publish, so a
/// racing writer can still advance the head between the probe and our
/// compare-and-swap. That stale-head loss says nothing about the candidates
/// themselves; retrying them once against a cold-loaded basis keeps the warm
/// cache transparent to callers. Rejections decided against an etag-matched
/// basis are as authoritative as a cold load and stand without a retry.
fn should_retry_reused_warm_stale_head(
    basis_reuse_event: BasisReuseEvent,
    published: &crate::protocol::PublishBatchAgainstBasisResult,
) -> bool {
    basis_reuse_event == BasisReuseEvent::ReusedAfterHeadEtagMatch
        && published.results.iter().any(|result| {
            matches!(
                result,
                Err(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
            )
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::MetadataState;
    use crate::namespace::basis::NamespaceHeadEtagProbe;
    use loonfs_api::wire::control::{HeadState, NamespaceDescriptorState};
    use loonfs_api::{ChangeSeq, ContentStoreId, FenceToken};

    #[test]
    fn cached_verified_basis_refreshes_only_lease_state() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let content_store_id = ContentStoreId::parse("cs_00000000000000000000000000000001")
            .expect("valid content store id");
        let mut head = HeadState::initial(namespace_id.clone());
        head.seq = ChangeSeq(3);
        let original_lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(1),
            lease_expires_at_ms: 10,
        };
        let basis = VerifiedNamespaceBasis {
            namespace_descriptor: NamespaceDescriptorState {
                namespace_id: namespace_id.clone(),
                content_store_id: content_store_id.clone(),
            },
            content_store_id,
            head: head.clone(),
            head_etag: "etag-a".to_owned(),
            lease: original_lease.clone(),
            metadata_state: MetadataState::default(),
        };
        let cached = CachedVerifiedBasis::from_arc(Arc::new(basis.clone()));

        assert!(cached.matches_head_etag_probe(&NamespaceHeadEtagProbe {
            head_etag: "etag-a".to_owned(),
        }));
        assert!(!cached.matches_head_etag_probe(&NamespaceHeadEtagProbe {
            head_etag: "etag-b".to_owned(),
        }));

        let refreshed_lease = LeaseState {
            namespace_id,
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(1),
            lease_expires_at_ms: 20,
        };
        let refreshed = cached.basis_to_reuse_with_refreshed_lease(refreshed_lease.clone());

        assert_eq!(cached.verified_basis().lease, original_lease);
        assert_eq!(refreshed.lease, refreshed_lease);
        assert_eq!(refreshed.head, basis.head);
        assert_eq!(refreshed.head_etag, basis.head_etag);
        assert_eq!(refreshed.metadata_state, basis.metadata_state);
        assert_eq!(refreshed.namespace_descriptor, basis.namespace_descriptor);
        assert_eq!(refreshed.content_store_id, basis.content_store_id);
    }
}
