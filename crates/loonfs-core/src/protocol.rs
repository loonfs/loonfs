use crate::commit::{
    build_commit_plan, commit_request_from_v0, core_commit_fingerprint, materialize_commit,
    prepare_commit_head_publish, publish_commit_head, resolve_restore_content_refs,
    wal_payload_from_materialized_commit, CommitExecutionContext, CommitIdentitySource, CommitOp,
    CommitRequest as CoreCommitRequest, CommitValidationContext, MaterializedCommit,
    PreparedCommit, SemanticMutationIdentity,
};
use crate::context::MutationContext;
use crate::engine::{BeginDirectPutUploadTargetResponse, DirectPutUploadTarget};
use crate::error::CoreError;
use crate::metadata::{CommitReceiptRecord, MetadataState};
use crate::namespace::basis::{
    load_verified_namespace_basis, BasisLoadError, VerifiedNamespaceBasis,
};
use crate::namespace::catalog::{
    load_namespace_content_store_id, namespace_initialization_state, NamespaceInitializationError,
    NamespaceInitializationState,
};
use crate::namespace::control::{
    load_content_store_descriptor_control, load_namespace_descriptor_control,
    load_namespace_head_control, load_namespace_lease_control,
};
use crate::namespace::lease::acquire_or_renew_namespace_lease;
use crate::path::write::{
    path_intent_fingerprint_for_path_intent, PathMutationIntent, PublishPlanningSession,
};
use crate::publisher::NamespaceMutationCandidate;
use crate::storage::content::{
    validate_durable_content_reference, write_immutable_object, ContentValidationTracker,
};
use crate::wal::{load_validated_wal_chain, prepare_wal_segment, WalChainLoadRequest};
use bytes::Bytes;
use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, ChangesResponse, CommitDelta,
    CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse, CommittedChange,
    CompleteUploadRequest, CompleteUploadResponse, UploadContentResponse, UploadMode,
};
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, CompletedUpload, ControlObjectKind,
    UploadSessionEnvelope, UploadSessionState,
};
use loonfs_api::wire::wal::{WalCommitDelta, WalDelta};
use loonfs_api::{
    generate_upload_id, validate_upload_id, ChangeSeq, CommitId, ContentRef, ContentRefKind,
    ContentStoreId, NameKey, NamespaceId,
};
use loonfs_objectstore::keys::{content_blob, namespace_descriptor, upload_session};
use loonfs_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::Instrument;

const UPLOAD_SESSION_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct PublishBatchAgainstBasisResult {
    pub(crate) results: Vec<Result<ApiCommitResponse, CoreError>>,
    pub(crate) basis_promotion: BasisPromotion,
}

#[derive(Debug, Clone)]
pub(crate) enum BasisPromotion {
    Unchanged(Arc<VerifiedNamespaceBasis>),
    Advanced(Arc<VerifiedNamespaceBasis>),
    NotCacheable,
}

impl PublishBatchAgainstBasisResult {
    fn unchanged(
        results: Vec<Result<ApiCommitResponse, CoreError>>,
        basis: &VerifiedNamespaceBasis,
    ) -> Self {
        Self {
            results,
            basis_promotion: BasisPromotion::Unchanged(Arc::new(basis.clone())),
        }
    }

    fn advanced(
        results: Vec<Result<ApiCommitResponse, CoreError>>,
        basis: VerifiedNamespaceBasis,
    ) -> Self {
        Self {
            results,
            basis_promotion: BasisPromotion::Advanced(Arc::new(basis)),
        }
    }

    fn not_cacheable(results: Vec<Result<ApiCommitResponse, CoreError>>) -> Self {
        Self {
            results,
            basis_promotion: BasisPromotion::NotCacheable,
        }
    }
}

#[derive(Debug, Clone)]
struct LoadedUploadSessionObject {
    object_key: String,
    metadata: ObjectMetadata,
    envelope: UploadSessionEnvelope,
}

#[derive(Debug, Clone)]
struct InBatchRequest {
    primary_index: usize,
    semantic_identity: SemanticMutationIdentity,
}

struct CandidateCoreRequest {
    request: CoreCommitRequest,
    identity_source: CommitIdentitySource,
}

pub(crate) async fn begin_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: BeginUploadRequest,
    context: &MutationContext,
) -> Result<BeginUploadResponse, CoreError> {
    ensure_upload_namespace_available(store, namespace_id).await?;
    let mode = request.mode.unwrap_or_default();
    if mode == UploadMode::DirectPut {
        return Err(CoreError::InvalidUploadContent(
            "direct_put requires a presigned URL issuer".to_owned(),
        ));
    }
    let upload_id = create_upload_session(
        store,
        namespace_id,
        UploadMode::ServiceProxied,
        None,
        context,
    )
    .await?;
    Ok(BeginUploadResponse {
        namespace_id: namespace_id.clone(),
        upload_id,
        mode: UploadMode::ServiceProxied,
        direct_put: None,
    })
}

pub(crate) async fn begin_direct_put_upload_target<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_ref: ContentRef,
    context: &MutationContext,
) -> Result<BeginDirectPutUploadTargetResponse, CoreError> {
    ensure_upload_namespace_available(store, namespace_id).await?;
    ensure_direct_put_content_ref_supported(&content_ref)?;
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let object_key = content_blob(content_store_id.as_str(), &content_ref.digest)
        .map_err(|err| CoreError::InvalidUploadContent(err.to_string()))?;
    let upload_id = create_upload_session(
        store,
        namespace_id,
        UploadMode::DirectPut,
        Some(content_ref.clone()),
        context,
    )
    .await?;
    Ok(BeginDirectPutUploadTargetResponse {
        namespace_id: namespace_id.clone(),
        upload_id,
        target: DirectPutUploadTarget {
            content_ref,
            object_key,
        },
    })
}

fn ensure_direct_put_content_ref_supported(content_ref: &ContentRef) -> Result<(), CoreError> {
    if content_ref.kind != ContentRefKind::WholeFileV0 {
        return Err(CoreError::InvalidUploadContent(
            "direct_put only supports whole_file_v0 content refs".to_owned(),
        ));
    }
    Ok(())
}

async fn create_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    mode: UploadMode,
    direct_put_content_ref: Option<ContentRef>,
    context: &MutationContext,
) -> Result<String, CoreError> {
    let upload_id = generate_upload_id();
    let state = UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        mode,
        direct_put_content_ref,
        staged_content_ref: None,
        completed: None,
        created_at_ms: context.now_ms,
    };
    let envelope = UploadSessionEnvelope::from_state(
        ControlObjectKind::UploadSession,
        &context.writer_version,
        state,
    )
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let encoded =
        encode_control_object(&envelope).map_err(|err| CoreError::Store(err.to_string()))?;
    let object_key = upload_session(namespace_id.as_str(), &upload_id);
    store
        .put_if_absent(&object_key, Bytes::from(encoded))
        .await
        .map_err(|err| CoreError::Store(err.to_string()))?;
    Ok(upload_id)
}

async fn ensure_upload_namespace_available<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(), CoreError> {
    match namespace_initialization_state(store, namespace_id).await {
        Ok(NamespaceInitializationState::Complete) => {
            let descriptor = load_namespace_descriptor_control(store, namespace_id)
                .await
                .map_err(|error| {
                    CoreError::Basis(BasisLoadError::LoadNamespaceDescriptor(error))
                })?;
            load_content_store_descriptor_control(store, &descriptor.state.content_store_id)
                .await
                .map_err(|error| {
                    CoreError::Basis(BasisLoadError::LoadContentStoreDescriptor(error))
                })?;
            load_namespace_head_control(store, namespace_id)
                .await
                .map_err(|error| CoreError::Basis(BasisLoadError::LoadHead(error)))?;
            load_namespace_lease_control(store, namespace_id)
                .await
                .map_err(|error| CoreError::Basis(BasisLoadError::LoadLease(error)))?;
            Ok(())
        }
        Ok(NamespaceInitializationState::Absent) => {
            Err(CoreError::Basis(BasisLoadError::LoadNamespaceDescriptor(
                crate::namespace::control::ControlObjectLoadError::MissingObject {
                    object_key: namespace_descriptor(namespace_id.as_str()),
                },
            )))
        }
        Ok(NamespaceInitializationState::Partial) => {
            Err(CoreError::NamespacePartiallyInitialized {
                namespace_id: namespace_id.clone(),
            })
        }
        Err(error) => Err(map_upload_namespace_initialization_error(error)),
    }
}

fn map_upload_namespace_initialization_error(error: NamespaceInitializationError) -> CoreError {
    match error {
        NamespaceInitializationError::InvalidNamespaceId(error) => {
            CoreError::InvalidNamespaceId(error)
        }
        NamespaceInitializationError::LoadNamespaceDescriptor(error) => {
            CoreError::Basis(BasisLoadError::LoadNamespaceDescriptor(error))
        }
        NamespaceInitializationError::LoadContentStoreDescriptor(error) => {
            CoreError::Basis(BasisLoadError::LoadContentStoreDescriptor(error))
        }
        NamespaceInitializationError::InspectNamespaceDescriptor(_)
        | NamespaceInitializationError::InspectNamespaceHead(_)
        | NamespaceInitializationError::InspectNamespaceLease(_) => {
            CoreError::Store(error.to_string())
        }
    }
}

pub(crate) async fn upload_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
    bytes: &[u8],
    context: &MutationContext,
) -> Result<UploadContentResponse, CoreError> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let content_ref = ContentRef::whole_file_v0(bytes);
    let object_key = content_blob(content_store_id.as_str(), &content_ref.digest)
        .map_err(|err| CoreError::Store(err.to_string()))?;

    for _attempt in 0..UPLOAD_SESSION_RETRY_LIMIT {
        let loaded = read_upload_session_object(store, namespace_id, upload_id).await?;
        if loaded.envelope.state.completed.is_some() {
            return Err(CoreError::UploadAlreadyCompleted {
                upload_id: upload_id.to_owned(),
            });
        }
        if loaded.envelope.state.mode == UploadMode::DirectPut {
            return Err(CoreError::InvalidUploadContent(
                "direct_put sessions must be completed after using the presigned URL".to_owned(),
            ));
        }

        if let Some(existing) = &loaded.envelope.state.staged_content_ref {
            if existing == &content_ref {
                return Ok(UploadContentResponse {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.to_owned(),
                    content_ref,
                });
            }
            return Err(CoreError::UploadContentConflict {
                upload_id: upload_id.to_owned(),
            });
        }

        write_immutable_object(store, &object_key, bytes).await?;

        let mut next_state = loaded.envelope.state.clone();
        next_state.staged_content_ref = Some(content_ref.clone());

        let envelope = UploadSessionEnvelope::from_state(
            ControlObjectKind::UploadSession,
            &context.writer_version,
            next_state,
        )
        .map_err(|err| CoreError::Store(err.to_string()))?;
        let encoded =
            encode_control_object(&envelope).map_err(|err| CoreError::Store(err.to_string()))?;
        let expected_etag = loaded
            .metadata
            .etag
            .as_deref()
            .ok_or_else(|| CoreError::Store("missing upload session etag".to_owned()))?;

        match store
            .compare_and_swap(&loaded.object_key, expected_etag, Bytes::from(encoded))
            .await
        {
            Ok(_) => {
                return Ok(UploadContentResponse {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.to_owned(),
                    content_ref,
                });
            }
            Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => continue,
            Err(err) => return Err(CoreError::Store(err.to_string())),
        }
    }

    Err(CoreError::Store(
        "upload session compare-and-swap retry exhausted".to_owned(),
    ))
}

pub(crate) async fn complete_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
    request: &CompleteUploadRequest,
    context: &MutationContext,
) -> Result<CompleteUploadResponse, CoreError> {
    for _attempt in 0..UPLOAD_SESSION_RETRY_LIMIT {
        let loaded = read_upload_session_object(store, namespace_id, upload_id).await?;
        if let Some(completed) = &loaded.envelope.state.completed {
            if completed.content_ref == request.content_ref {
                return Ok(CompleteUploadResponse {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.to_owned(),
                    content_ref: completed.content_ref.clone(),
                    validated_content_token: None,
                });
            }
            return Err(CoreError::UploadAlreadyCompleted {
                upload_id: upload_id.to_owned(),
            });
        }

        let staged_content_ref = match loaded.envelope.state.staged_content_ref.clone() {
            Some(content_ref) => content_ref,
            None => stage_direct_put_content_ref(store, namespace_id, &loaded, request).await?,
        };
        if staged_content_ref != request.content_ref {
            return Err(CoreError::InvalidUploadContent(
                "completed content ref does not match staged content".to_owned(),
            ));
        }

        let mut next_state = loaded.envelope.state.clone();
        if next_state.staged_content_ref.is_none() {
            next_state.staged_content_ref = Some(staged_content_ref);
        }
        next_state.completed = Some(CompletedUpload {
            content_ref: request.content_ref.clone(),
        });
        let envelope = UploadSessionEnvelope::from_state(
            ControlObjectKind::UploadSession,
            &context.writer_version,
            next_state,
        )
        .map_err(|err| CoreError::Store(err.to_string()))?;
        let encoded =
            encode_control_object(&envelope).map_err(|err| CoreError::Store(err.to_string()))?;
        let expected_etag = loaded
            .metadata
            .etag
            .as_deref()
            .ok_or_else(|| CoreError::Store("missing upload session etag".to_owned()))?;

        match store
            .compare_and_swap(&loaded.object_key, expected_etag, Bytes::from(encoded))
            .await
        {
            Ok(_) => {
                return Ok(CompleteUploadResponse {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.to_owned(),
                    content_ref: request.content_ref.clone(),
                    validated_content_token: None,
                });
            }
            Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => continue,
            Err(err) => return Err(CoreError::Store(err.to_string())),
        }
    }

    Err(CoreError::Store(
        "upload session compare-and-swap retry exhausted".to_owned(),
    ))
}

async fn stage_direct_put_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    loaded: &LoadedUploadSessionObject,
    request: &CompleteUploadRequest,
) -> Result<ContentRef, CoreError> {
    if loaded.envelope.state.mode != UploadMode::DirectPut {
        return Err(CoreError::InvalidUploadContent(
            "upload content has not been staged".to_owned(),
        ));
    }

    let Some(expected) = &loaded.envelope.state.direct_put_content_ref else {
        return Err(CoreError::InvalidUploadContent(
            "direct_put session is missing its target content ref".to_owned(),
        ));
    };
    if expected != &request.content_ref {
        return Err(CoreError::InvalidUploadContent(
            "completed content ref does not match direct_put target".to_owned(),
        ));
    }

    // Bytes bypassed the LoonFS server; completion is the authority point where
    // the server proves the object is durable and matches the signed content ref.
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    validate_durable_content_reference(store, &content_store_id, &request.content_ref)
        .await
        .map_err(|err| CoreError::InvalidUploadContent(err.to_string()))?;
    Ok(request.content_ref.clone())
}

pub(crate) async fn commit_operations<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: ApiCommitRequest,
    context: &MutationContext,
) -> Result<ApiCommitResponse, CoreError> {
    publish_namespace_mutations_batch(
        store,
        namespace_id,
        vec![NamespaceMutationCandidate::Commit(request)],
        context,
    )
    .await
    .pop()
    .unwrap_or_else(|| Err(CoreError::Store("empty commit batch".to_owned())))
}

pub(crate) async fn commit_operations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    requests: Vec<ApiCommitRequest>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    publish_namespace_mutations_batch(
        store,
        namespace_id,
        requests
            .into_iter()
            .map(NamespaceMutationCandidate::Commit)
            .collect(),
        context,
    )
    .await
}

pub(crate) async fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    commit_namespace_mutations_batch(store, namespace_id, candidates, context).await
}

async fn commit_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    if candidates.is_empty() {
        return Vec::new();
    }
    if let Err(error) = acquire_or_renew_namespace_lease(store, namespace_id, context).await {
        return (0..candidates.len())
            .map(|_| Err(CoreError::Lease(error.clone())))
            .collect();
    }
    let basis = match load_verified_namespace_basis(store, namespace_id)
        .instrument(tracing::info_span!(
            "loon.phase",
            phase = "load_basis_for_publish"
        ))
        .await
    {
        Ok(basis) => basis,
        Err(error) => {
            return (0..candidates.len())
                .map(|_| Err(CoreError::Basis(error.clone())))
                .collect()
        }
    };
    publish_namespace_mutations_batch_against_basis(
        store,
        namespace_id,
        &candidates,
        context,
        &basis,
    )
    .await
    .results
}

pub(crate) async fn publish_namespace_mutations_batch_against_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: &[NamespaceMutationCandidate],
    context: &MutationContext,
    basis: &VerifiedNamespaceBasis,
) -> PublishBatchAgainstBasisResult {
    if candidates.is_empty() {
        return PublishBatchAgainstBasisResult::unchanged(Vec::new(), basis);
    }
    let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
    if basis.head.namespace_id != *namespace_id {
        return PublishBatchAgainstBasisResult::not_cacheable(
            (0..candidates.len())
                .map(|_| {
                    Err(CoreError::Store(
                        "publish basis namespace mismatch".to_owned(),
                    ))
                })
                .collect(),
        );
    }
    let mut outcomes: Vec<Option<Result<ApiCommitResponse, CoreError>>> =
        (0..candidates.len()).map(|_| None).collect();
    let mut session = PublishPlanningSession::new(basis);
    let mut accepted: Vec<(usize, MaterializedCommit)> = Vec::new();
    let mut in_batch_requests: HashMap<CommitId, InBatchRequest> = HashMap::new();
    let mut aliases: Vec<(usize, usize)> = Vec::new();
    let mut content_validation = ContentValidationTracker::default();

    let prepare_span = tracing::info_span!(
        "publisher.batch_prepare",
        phase = "batch_prepare",
        batch_size,
        accepted_count = tracing::field::Empty
    );
    async {
        for (index, candidate) in candidates.iter().enumerate() {
            let candidate_request = {
                let _span = tracing::info_span!("loon.phase", phase = "prepare_commit").entered();
                prepare_candidate_request(
                    namespace_id,
                    basis,
                    &session,
                    candidate,
                    context,
                    index,
                    &mut outcomes,
                    &mut in_batch_requests,
                    &mut aliases,
                )
            };
            let Some(candidate_request) = candidate_request else {
                continue;
            };
            let validation = CommitValidationContext {
                head: session.head().clone(),
                lease: basis.lease.clone(),
                now_ms: context.now_ms,
                metadata_state: session.metadata_state(),
            };
            let request = candidate_request.request;
            let resolved_restore_content_refs = resolve_restore_content_refs(&request, &validation);
            let content_result = match candidate {
                NamespaceMutationCandidate::Commit(_)
                | NamespaceMutationCandidate::Path(PathMutationIntent::PutFile { .. }) => {
                    validate_commit_content_references(
                        store,
                        &basis.content_store_id,
                        &request,
                        &resolved_restore_content_refs,
                        &mut content_validation,
                    )
                    .await
                }
                NamespaceMutationCandidate::AdmittedPath {
                    admitted_content_refs,
                    ..
                } => ensure_commit_content_refs_admitted(&request, admitted_content_refs),
                NamespaceMutationCandidate::Path(_) => Ok(()),
            };
            if let Err(error) = content_result {
                outcomes[index] = Some(Err(error));
                continue;
            }
            let plan = {
                let _span =
                    tracing::info_span!("loon.phase", phase = "build_commit_plan").entered();
                match build_commit_plan(&request, &validation) {
                    Ok(plan) => plan,
                    Err(error) => {
                        outcomes[index] = Some(Err(error.into()));
                        continue;
                    }
                }
            };
            let prepared = {
                let _span =
                    tracing::info_span!("loon.phase", phase = "PreparedCommit::prepare").entered();
                match PreparedCommit::prepare(
                    request,
                    plan.clone(),
                    candidate_request.identity_source,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        outcomes[index] = Some(Err(CoreError::Store(format!(
                            "commit preparation failed: {error}"
                        ))));
                        continue;
                    }
                }
            };
            let materialized = {
                let _span =
                    tracing::info_span!("loon.phase", phase = "materialize_commit").entered();
                materialize_commit(prepared)
            };
            let preview = {
                let _span = tracing::info_span!(
                    "loon.phase",
                    phase = "wal_payload_from_materialized_commit"
                )
                .entered();
                match wal_payload_from_materialized_commit(&materialized) {
                    Ok(payload) => payload,
                    Err(error) => {
                        outcomes[index] = Some(Err(error.into()));
                        continue;
                    }
                }
            };
            let applied = {
                let _span = tracing::info_span!("loon.phase", phase = "apply_committed_wal_record")
                    .entered();
                session.apply_accepted_commit(&preview, &plan)
            };
            match applied {
                Ok(()) => accepted.push((index, materialized)),
                Err(error) => outcomes[index] = Some(Err(error.into())),
            }
        }
    }
    .instrument(prepare_span.clone())
    .await;
    prepare_span.record(
        "accepted_count",
        u64::try_from(accepted.len()).unwrap_or(u64::MAX),
    );
    drop(prepare_span);

    if accepted.is_empty() {
        return PublishBatchAgainstBasisResult::unchanged(
            finish_batch_outcomes_with_aliases(outcomes, &aliases),
            basis,
        );
    }
    let records = accepted
        .iter()
        .map(|(_, record)| record.clone())
        .collect::<Vec<_>>();
    let accepted_count = u64::try_from(records.len()).unwrap_or(u64::MAX);
    let wal_span = tracing::info_span!(
        "publisher.batch_write_wal",
        phase = "batch_write_wal",
        batch_size,
        accepted_count,
        wal_segment_count = 1_u64,
        key_class = "wal_segment",
        result = tracing::field::Empty
    );
    let wal_result: Result<_, CoreError> = {
        let _span = wal_span.enter();
        match prepare_wal_segment(
            namespace_id.clone(),
            basis.head.visible_wal_tip.clone(),
            &records,
            &context.writer_version,
        ) {
            Ok(wal) => match store
                .put_if_absent(&wal.object_key, Bytes::copy_from_slice(&wal.encoded_bytes))
                .await
            {
                Ok(_) => Ok(wal),
                Err(error) => Err(CoreError::WalWrite(error.to_string())),
            },
            Err(error) => Err(CoreError::Store(format!("wal build failed: {error:?}"))),
        }
    };
    wal_span.record("result", if wal_result.is_ok() { "ok" } else { "error" });
    drop(wal_span);
    let wal = match wal_result {
        Ok(wal) => wal,
        Err(error) => {
            fail_outcomes_contingent_on_unpublished_batch(&mut outcomes, &accepted, &error);
            return PublishBatchAgainstBasisResult::not_cacheable(
                finish_batch_outcomes_with_aliases(outcomes, &aliases),
            );
        }
    };

    let last_plan = &records
        .last()
        .expect("non-empty accepted records")
        .prepared
        .plan;
    let head_publish =
        prepare_commit_head_publish(&basis.head, last_plan, &wal, &context.writer_version);
    let head_publish = match head_publish {
        Ok(value) => value,
        Err(error) => {
            let error = CoreError::Store(format!("head publish preparation failed: {error:?}"));
            fail_outcomes_contingent_on_unpublished_batch(&mut outcomes, &accepted, &error);
            return PublishBatchAgainstBasisResult::not_cacheable(
                finish_batch_outcomes_with_aliases(outcomes, &aliases),
            );
        }
    };
    let head_cas_span = tracing::info_span!(
        "publisher.batch_cas_head",
        phase = "batch_cas_head",
        batch_size,
        accepted_count,
        key_class = "namespace_head",
        result = tracing::field::Empty
    );
    let head_metadata_result = {
        let _span = head_cas_span.enter();
        publish_commit_head(store, &basis.head_etag, &head_publish).await
    };
    head_cas_span.record(
        "result",
        if head_metadata_result.is_ok() {
            "ok"
        } else {
            "error"
        },
    );
    drop(head_cas_span);
    let head_metadata = match head_metadata_result {
        Ok(metadata) => metadata,
        Err(error) => {
            fail_outcomes_contingent_on_unpublished_batch(&mut outcomes, &accepted, &error.into());
            return PublishBatchAgainstBasisResult::not_cacheable(
                finish_batch_outcomes_with_aliases(outcomes, &aliases),
            );
        }
    };

    let promoted_basis = head_metadata.etag.map(|head_etag| VerifiedNamespaceBasis {
        namespace_descriptor: basis.namespace_descriptor.clone(),
        content_store_id: basis.content_store_id.clone(),
        head: head_publish.resulting_head.clone(),
        head_etag,
        lease: basis.lease.clone(),
        metadata_state: session.into_metadata_state(),
    });

    for (accepted_index, (outcome_index, record)) in accepted.into_iter().enumerate() {
        outcomes[outcome_index] = Some(Ok(ApiCommitResponse {
            namespace_id: namespace_id.clone(),
            commit_id: record.prepared.request.commit_id,
            committed_seq: wal.envelope.payload.records[accepted_index].seq,
            results: record.results,
        }));
    }
    let results = finish_batch_outcomes_with_aliases(outcomes, &aliases);
    match promoted_basis {
        Some(basis) => PublishBatchAgainstBasisResult::advanced(results, basis),
        None => PublishBatchAgainstBasisResult::not_cacheable(results),
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_candidate_request(
    namespace_id: &NamespaceId,
    basis: &VerifiedNamespaceBasis,
    session: &PublishPlanningSession,
    candidate: &NamespaceMutationCandidate,
    context: &MutationContext,
    index: usize,
    outcomes: &mut [Option<Result<ApiCommitResponse, CoreError>>],
    in_batch_requests: &mut HashMap<CommitId, InBatchRequest>,
    aliases: &mut Vec<(usize, usize)>,
) -> Option<CandidateCoreRequest> {
    let conversion_context = CommitExecutionContext {
        namespace_id: namespace_id.clone(),
        writer_id: context.writer_id.clone(),
        writer_fence_token: basis.head.active_fence_token,
    };
    match candidate {
        NamespaceMutationCandidate::Commit(request) => {
            if let Err(error) = validate_commit_id(&request.commit_id) {
                outcomes[index] = Some(Err(error));
                return None;
            }
            let request = match commit_request_from_v0(conversion_context, request.clone()) {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(error.into()));
                    return None;
                }
            };
            let semantic_identity = match core_commit_fingerprint(&request) {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(CoreError::Store(error.to_string())));
                    return None;
                }
            };
            let semantic_identity = SemanticMutationIdentity::CoreCommit(semantic_identity);
            if !record_primary_request_or_complete_idempotent(
                namespace_id,
                &basis.metadata_state,
                outcomes,
                in_batch_requests,
                aliases,
                index,
                &request.commit_id,
                &semantic_identity,
            ) {
                return None;
            }
            Some(CandidateCoreRequest {
                request,
                identity_source: CommitIdentitySource::CoreCommitRequest,
            })
        }
        NamespaceMutationCandidate::Path(intent)
        | NamespaceMutationCandidate::AdmittedPath { intent, .. } => {
            if let Err(error) = validate_commit_id(intent.commit_id()) {
                outcomes[index] = Some(Err(error));
                return None;
            }
            let path_intent_fingerprint =
                match path_intent_fingerprint_for_path_intent(namespace_id, intent) {
                    Ok(value) => value,
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        return None;
                    }
                };
            let semantic_identity =
                SemanticMutationIdentity::PathIntent(path_intent_fingerprint.clone());
            let commit_id = intent.commit_id().clone();
            if !record_primary_request_or_complete_idempotent(
                namespace_id,
                &basis.metadata_state,
                outcomes,
                in_batch_requests,
                aliases,
                index,
                &commit_id,
                &semantic_identity,
            ) {
                return None;
            }
            let planned = match session.plan_path_mutation(namespace_id, intent) {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(error));
                    return None;
                }
            };
            let request = match commit_request_from_v0(conversion_context, planned.commit_request) {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(error.into()));
                    return None;
                }
            };
            Some(CandidateCoreRequest {
                request,
                identity_source: CommitIdentitySource::PathIntent(planned.path_intent_fingerprint),
            })
        }
    }
}

fn validate_commit_id(commit_id: &CommitId) -> Result<(), CoreError> {
    CommitId::parse(commit_id.as_str())
        .map(|_| ())
        .map_err(CoreError::InvalidCommitId)
}

#[allow(clippy::too_many_arguments)]
fn record_primary_request_or_complete_idempotent(
    namespace_id: &NamespaceId,
    visible_metadata_state: &MetadataState,
    outcomes: &mut [Option<Result<ApiCommitResponse, CoreError>>],
    in_batch_requests: &mut HashMap<CommitId, InBatchRequest>,
    aliases: &mut Vec<(usize, usize)>,
    index: usize,
    commit_id: &CommitId,
    semantic_identity: &SemanticMutationIdentity,
) -> bool {
    if let Some(existing) = find_commit_receipt(visible_metadata_state, commit_id) {
        outcomes[index] = Some(
            if existing.semantic_commit_fingerprint != semantic_identity.as_str() {
                Err(CoreError::CommitIdReuseConflict(commit_id.to_string()))
            } else {
                Ok(commit_response_from_commit_receipt(namespace_id, existing))
            },
        );
        return false;
    }
    if let Some(existing) = in_batch_requests.get(commit_id) {
        if existing.semantic_identity != *semantic_identity {
            outcomes[index] = Some(Err(CoreError::CommitIdReuseConflict(commit_id.to_string())));
        } else {
            aliases.push((index, existing.primary_index));
        }
        return false;
    }
    in_batch_requests.insert(
        commit_id.clone(),
        InBatchRequest {
            primary_index: index,
            semantic_identity: semantic_identity.clone(),
        },
    );
    true
}

pub(crate) async fn list_changes_after<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after_seq: ChangeSeq,
) -> Result<ChangesResponse, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id).await?;
    if after_seq < basis.head.retention_floor_seq {
        return Err(CoreError::RebootstrapRequired {
            after_seq,
            retention_floor_seq: basis.head.retention_floor_seq,
        });
    }
    if after_seq >= basis.head.seq {
        return Ok(ChangesResponse {
            namespace_id: namespace_id.clone(),
            after_seq,
            through_seq: basis.head.seq,
            changes: Vec::new(),
        });
    }

    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: basis.head.retention_floor_seq,
            head_seq: basis.head.seq,
            visible_tip: basis.head.visible_wal_tip.clone(),
            stop_after_seq: Some(after_seq),
        },
    )
    .await
    .map_err(|error| CoreError::Basis(BasisLoadError::WalChainLoad(error)))?;
    let mut changes = Vec::new();
    for segment in wal_chain.segments() {
        for record in segment.records() {
            if record.seq > after_seq {
                changes.push(CommittedChange {
                    seq: record.seq,
                    commit_id: record.commit_id.clone(),
                    message: record.message.clone(),
                    annotations: record.annotations.clone(),
                    ops: record.results.clone(),
                    deltas: record
                        .deltas
                        .iter()
                        .map(commit_delta_from_wal)
                        .collect::<Result<Vec<_>, _>>()?,
                });
            }
        }
    }

    Ok(ChangesResponse {
        namespace_id: namespace_id.clone(),
        after_seq,
        through_seq: basis.head.seq,
        changes,
    })
}

fn commit_delta_from_wal(delta: &WalCommitDelta) -> Result<CommitDelta, CoreError> {
    let semantic_op_index = delta.semantic_op_index;
    Ok(match &delta.delta {
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind,
        } => CommitDelta::CreateInode {
            semantic_op_index,
            delta_index: *delta_index,
            inode_id: *inode_id,
            inode_kind: inode_kind.clone(),
        },
        WalDelta::BindDirentry {
            delta_index,
            parent_inode,
            name_key,
            display_name,
            child_inode,
        } => CommitDelta::BindDirentry {
            semantic_op_index,
            delta_index: *delta_index,
            parent_inode: *parent_inode,
            name_key: NameKey::try_new(name_key.clone()).map_err(|err| {
                CoreError::NamespaceCorrupt(format!("invalid WAL name_key: {err}"))
            })?,
            display_name: display_name.clone(),
            child_inode: *child_inode,
        },
        WalDelta::UnbindDirentry {
            delta_index,
            parent_inode,
            name_key,
            child_inode,
            bind_seq,
            bind_delta_index,
        } => CommitDelta::UnbindDirentry {
            semantic_op_index,
            delta_index: *delta_index,
            parent_inode: *parent_inode,
            name_key: NameKey::try_new(name_key.clone()).map_err(|err| {
                CoreError::NamespaceCorrupt(format!("invalid WAL name_key: {err}"))
            })?,
            child_inode: *child_inode,
            bind_seq: *bind_seq,
            bind_delta_index: *bind_delta_index,
        },
        WalDelta::AppendFileRevision {
            delta_index,
            inode_id,
            revision_no,
            content_ref,
        } => CommitDelta::AppendFileRevision {
            semantic_op_index,
            delta_index: *delta_index,
            inode_id: *inode_id,
            revision_no: *revision_no,
            content_ref: content_ref.clone(),
        },
        WalDelta::TombstoneSubtree {
            delta_index,
            root_inode,
        } => CommitDelta::TombstoneSubtree {
            semantic_op_index,
            delta_index: *delta_index,
            root_inode: *root_inode,
        },
    })
}

async fn validate_commit_content_references<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    request: &CoreCommitRequest,
    resolved_restore_content_refs: &[Option<ContentRef>],
    content_validation: &mut ContentValidationTracker,
) -> Result<(), CoreError> {
    let mut content_refs = Vec::new();
    for (index, op) in request.ops.iter().enumerate() {
        match op {
            CommitOp::CreateFile { content_ref, .. }
            | CommitOp::ReplaceFile { content_ref, .. } => {
                content_refs.push(content_ref);
            }
            CommitOp::RestoreRevision { .. } => {
                if let Some(content_ref) = resolved_restore_content_refs
                    .get(index)
                    .and_then(|content_ref| content_ref.as_ref())
                {
                    content_refs.push(content_ref);
                }
            }
            _ => {}
        }
    }

    if content_refs.is_empty() {
        return Ok(());
    }

    for content_ref in content_refs {
        content_validation
            .ensure_validated(store, content_store_id, content_ref)
            .await?;
    }

    Ok(())
}

fn ensure_commit_content_refs_admitted(
    request: &CoreCommitRequest,
    admitted_content_refs: &[ContentRef],
) -> Result<(), CoreError> {
    for op in &request.ops {
        match op {
            CommitOp::CreateFile { content_ref, .. }
            | CommitOp::ReplaceFile { content_ref, .. } => {
                if !admitted_content_refs
                    .iter()
                    .any(|admitted| admitted == content_ref)
                {
                    return Err(CoreError::InvalidUploadContent(
                        "content ref was not admitted before publish".to_owned(),
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

async fn read_upload_session_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
) -> Result<LoadedUploadSessionObject, CoreError> {
    NamespaceId::parse(namespace_id.as_str()).map_err(CoreError::from)?;
    validate_upload_id(upload_id).map_err(CoreError::InvalidUploadId)?;
    let object_key = upload_session(namespace_id.as_str(), upload_id);
    let metadata = store
        .head(&object_key)
        .await
        .map_err(|err| CoreError::Store(err.to_string()))?
        .ok_or_else(|| CoreError::UploadNotFound {
            upload_id: upload_id.to_owned(),
        })?;
    let encoded = store
        .get(&object_key, None)
        .await
        .map_err(|err| CoreError::Store(err.to_string()))?
        .ok_or_else(|| CoreError::UploadNotFound {
            upload_id: upload_id.to_owned(),
        })?;
    let envelope: UploadSessionEnvelope =
        decode_control_object(&encoded, ControlObjectKind::UploadSession).map_err(|err| {
            CoreError::Store(format!("invalid upload session `{object_key}`: {err}"))
        })?;
    if envelope.state.namespace_id != *namespace_id {
        return Err(CoreError::Store(format!(
            "upload session namespace mismatch for `{object_key}`"
        )));
    }
    if envelope.state.upload_id != upload_id {
        return Err(CoreError::Store(format!(
            "upload session id mismatch for `{object_key}`"
        )));
    }

    Ok(LoadedUploadSessionObject {
        object_key,
        metadata,
        envelope,
    })
}

fn find_commit_receipt<'a>(
    metadata_state: &'a crate::metadata::MetadataState,
    commit_id: &CommitId,
) -> Option<&'a CommitReceiptRecord> {
    metadata_state.find_commit_receipt(commit_id)
}

fn commit_response_from_commit_receipt(
    namespace_id: &NamespaceId,
    record: &CommitReceiptRecord,
) -> ApiCommitResponse {
    ApiCommitResponse {
        namespace_id: namespace_id.clone(),
        commit_id: record.commit_id.clone(),
        committed_seq: record.committed_seq,
        results: record.results.clone(),
    }
}

/// Fails every outcome that was contingent on this batch publishing durably.
///
/// The accepted candidates take the batch error: they never committed. So do
/// rejections recorded after the first acceptance, because their verdicts
/// were decided against session state advanced by tentatively accepted
/// candidates — state that never became durable. Reporting them would hand a
/// client a definitive semantic error (path conflict, missing path, stale
/// revision, ...) it correctly treats as non-retryable, for a precondition
/// that was never durably true (format.md section 3.1.5).
///
/// Rejections recorded before any acceptance were decided against the
/// durable basis alone and stand. Idempotent `Ok` completions replay durable
/// commit receipts and stand. Alias slots stay unfilled here and inherit
/// their primary's final outcome.
fn fail_outcomes_contingent_on_unpublished_batch(
    outcomes: &mut [Option<Result<ApiCommitResponse, CoreError>>],
    accepted: &[(usize, MaterializedCommit)],
    error: &CoreError,
) {
    let Some(first_accepted_index) = accepted.first().map(|(index, _)| *index) else {
        return;
    };
    for (index, _) in accepted {
        outcomes[*index] = Some(Err(error.clone()));
    }
    for outcome in outcomes.iter_mut().skip(first_accepted_index + 1) {
        if matches!(outcome, Some(Err(_))) {
            *outcome = Some(Err(error.clone()));
        }
    }
}

fn finish_batch_outcomes(
    outcomes: Vec<Option<Result<ApiCommitResponse, CoreError>>>,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    outcomes
        .into_iter()
        .map(|outcome| {
            outcome.unwrap_or_else(|| Err(CoreError::Store("missing batch outcome".to_owned())))
        })
        .collect()
}

fn finish_batch_outcomes_with_aliases(
    mut outcomes: Vec<Option<Result<ApiCommitResponse, CoreError>>>,
    aliases: &[(usize, usize)],
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    for (alias_index, primary_index) in aliases {
        let primary_outcome = outcomes
            .get(*primary_index)
            .and_then(Clone::clone)
            .unwrap_or_else(|| Err(CoreError::Store("missing primary batch outcome".to_owned())));
        outcomes[*alias_index] = Some(primary_outcome);
    }
    finish_batch_outcomes(outcomes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use loonfs_api::{ChangeSeq, InodeId};

    #[test]
    fn invalid_wal_delta_name_key_is_namespace_corrupt() {
        let delta = WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::BindDirentry {
                delta_index: 0,
                parent_inode: InodeId(1),
                name_key: "bad/key".to_owned(),
                display_name: "file.txt".to_owned(),
                child_inode: InodeId(2),
            },
        };

        let error = commit_delta_from_wal(&delta).expect_err("invalid durable WAL name key");

        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    }

    #[test]
    fn invalid_wal_unbind_name_key_is_namespace_corrupt() {
        let delta = WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::UnbindDirentry {
                delta_index: 0,
                parent_inode: InodeId(1),
                name_key: "bad/key".to_owned(),
                child_inode: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            },
        };

        let error = commit_delta_from_wal(&delta).expect_err("invalid durable WAL name key");

        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    }
}
