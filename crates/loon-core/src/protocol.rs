use crate::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::commit::{
    build_commit_plan, commit_request_from_v0, materialize_commit, prepare_commit_head_publish,
    publish_commit_head, resolve_restore_content_refs, semantic_commit_fingerprint,
    wal_payload_from_materialized_commit, CommitExecutionContext, CommitFingerprintSource,
    CommitOp, CommitRequest as CoreCommitRequest, CommitValidationContext, MaterializedCommit,
    PreparedCommit, SemanticCommitFingerprint,
};
use crate::content::{validate_durable_content_reference, write_immutable_object};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::metadata::{CommitReceiptRecord, MetadataState};
use crate::namespace::catalog::load_namespace_content_store_id;
use crate::publisher::NamespaceMutationCandidate;
use crate::wal::{prepare_wal_segment, StoredWalObject};
use loon_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitRequest as ApiCommitRequest,
    CommitResponse as ApiCommitResponse, CommittedChange, CompleteUploadRequest,
    CompleteUploadResponse, UploadContentResponse, UploadMode,
};
use loon_api::{
    decode_wal_segment_envelope_zstd, generate_upload_id, validate_upload_id, ChangeSeq, CommitId,
    CompletedUpload, ContentRef, ControlObjectKind, HeadState, NamespaceId, UploadSessionEnvelope,
    UploadSessionState,
};
use loon_objectstore::keys::{content_blob, upload_session};
use loon_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError};
use std::collections::HashMap;

const UPLOAD_SESSION_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone)]
struct LoadedUploadSessionObject {
    object_key: String,
    metadata: ObjectMetadata,
    envelope: UploadSessionEnvelope,
}

#[derive(Debug, Clone)]
struct InBatchRequest {
    primary_index: usize,
    semantic_commit_fingerprint: SemanticCommitFingerprint,
}

struct CandidateCoreRequest {
    request: CoreCommitRequest,
    fingerprint_source: CommitFingerprintSource,
}

pub fn begin_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<BeginUploadResponse, CoreError> {
    let _basis = load_verified_namespace_basis(store, namespace_id)?;
    let upload_id = generate_upload_id();
    let state = UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
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
    let encoded = serde_json::to_vec(&envelope).map_err(|err| CoreError::Store(err.to_string()))?;
    let object_key = upload_session(namespace_id.as_str(), &upload_id);
    store
        .put_if_absent(&object_key, &encoded)
        .map_err(|err| CoreError::Store(err.to_string()))?;

    Ok(BeginUploadResponse {
        namespace_id: namespace_id.clone(),
        upload_id,
        mode: UploadMode::ServiceProxied,
    })
}

pub fn upload_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
    bytes: &[u8],
    context: &MutationContext,
) -> Result<UploadContentResponse, CoreError> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id)?;
    let content_ref = ContentRef::whole_file_v0(bytes);
    let object_key = content_blob(content_store_id.as_str(), &content_ref.digest)
        .map_err(|err| CoreError::Store(err.to_string()))?;

    for _attempt in 0..UPLOAD_SESSION_RETRY_LIMIT {
        let loaded = read_upload_session_object(store, namespace_id, upload_id)?;
        if loaded.envelope.state.completed.is_some() {
            return Err(CoreError::UploadAlreadyCompleted {
                upload_id: upload_id.to_owned(),
            });
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

        write_immutable_object(store, &object_key, bytes)?;

        let mut next_state = loaded.envelope.state.clone();
        next_state.staged_content_ref = Some(content_ref.clone());

        let envelope = UploadSessionEnvelope::from_state(
            ControlObjectKind::UploadSession,
            &context.writer_version,
            next_state,
        )
        .map_err(|err| CoreError::Store(err.to_string()))?;
        let encoded =
            serde_json::to_vec(&envelope).map_err(|err| CoreError::Store(err.to_string()))?;
        let expected_etag = loaded
            .metadata
            .etag
            .as_deref()
            .ok_or_else(|| CoreError::Store("missing upload session etag".to_owned()))?;

        match store.compare_and_swap(&loaded.object_key, expected_etag, &encoded) {
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

pub fn complete_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
    request: &CompleteUploadRequest,
    context: &MutationContext,
) -> Result<CompleteUploadResponse, CoreError> {
    for _attempt in 0..UPLOAD_SESSION_RETRY_LIMIT {
        let loaded = read_upload_session_object(store, namespace_id, upload_id)?;
        if let Some(completed) = &loaded.envelope.state.completed {
            if completed.content_ref == request.content_ref {
                return Ok(CompleteUploadResponse {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.to_owned(),
                    content_ref: completed.content_ref.clone(),
                });
            }
            return Err(CoreError::UploadAlreadyCompleted {
                upload_id: upload_id.to_owned(),
            });
        }

        let Some(staged_content_ref) = loaded.envelope.state.staged_content_ref.clone() else {
            return Err(CoreError::InvalidUploadContent(
                "upload content has not been staged".to_owned(),
            ));
        };
        if staged_content_ref != request.content_ref {
            return Err(CoreError::InvalidUploadContent(
                "completed content ref does not match staged content".to_owned(),
            ));
        }
        let content_store_id = load_namespace_content_store_id(store, namespace_id)?;
        validate_durable_content_reference(store, &content_store_id, &request.content_ref)?;

        let mut next_state = loaded.envelope.state.clone();
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
            serde_json::to_vec(&envelope).map_err(|err| CoreError::Store(err.to_string()))?;
        let expected_etag = loaded
            .metadata
            .etag
            .as_deref()
            .ok_or_else(|| CoreError::Store("missing upload session etag".to_owned()))?;

        match store.compare_and_swap(&loaded.object_key, expected_etag, &encoded) {
            Ok(_) => {
                return Ok(CompleteUploadResponse {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.to_owned(),
                    content_ref: request.content_ref.clone(),
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

pub fn commit_operations<S: ObjectStore + ?Sized>(
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
    .pop()
    .unwrap_or_else(|| Err(CoreError::Store("empty commit batch".to_owned())))
}

pub fn commit_operations_batch<S: ObjectStore + ?Sized>(
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
}

pub(crate) fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    commit_namespace_mutations_batch(store, namespace_id, candidates, context)
}

fn commit_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    if candidates.is_empty() {
        return Vec::new();
    }
    if let Err(error) = crate::acquire_or_renew_namespace_lease(store, namespace_id, context) {
        return (0..candidates.len())
            .map(|_| Err(CoreError::Lease(error.clone())))
            .collect();
    }
    let basis = match load_verified_namespace_basis(store, namespace_id) {
        Ok(basis) => basis,
        Err(error) => {
            return (0..candidates.len())
                .map(|_| Err(CoreError::Basis(error.clone())))
                .collect()
        }
    };

    let mut outcomes: Vec<Option<Result<ApiCommitResponse, CoreError>>> =
        (0..candidates.len()).map(|_| None).collect();
    let mut current_head = basis.head.clone();
    let mut current_metadata_state = basis.metadata_state.clone();
    let mut accepted: Vec<(usize, MaterializedCommit)> = Vec::new();
    let mut in_batch_requests: HashMap<CommitId, InBatchRequest> = HashMap::new();
    let mut aliases: Vec<(usize, usize)> = Vec::new();

    for (index, candidate) in candidates.into_iter().enumerate() {
        let Some(candidate_request) = prepare_candidate_request(
            store,
            namespace_id,
            &basis,
            &current_head,
            &current_metadata_state,
            candidate,
            context,
            index,
            &mut outcomes,
            &mut in_batch_requests,
            &mut aliases,
        ) else {
            continue;
        };
        let validation = CommitValidationContext {
            head: current_head.clone(),
            lease: basis.lease.clone(),
            now_ms: context.now_ms,
            metadata_state: current_metadata_state.clone(),
        };
        let request = candidate_request.request;
        let resolved_restore_content_refs = resolve_restore_content_refs(&request, &validation);
        if let Err(error) = validate_commit_content_references(
            store,
            namespace_id,
            &request,
            &resolved_restore_content_refs,
        ) {
            outcomes[index] = Some(Err(error));
            continue;
        }
        let plan = match build_commit_plan(&request, &validation) {
            Ok(plan) => plan,
            Err(error) => {
                outcomes[index] = Some(Err(error.into()));
                continue;
            }
        };
        let prepared = match PreparedCommit::prepare(
            request,
            plan.clone(),
            candidate_request.fingerprint_source,
        ) {
            Ok(value) => value,
            Err(error) => {
                outcomes[index] = Some(Err(CoreError::Store(format!(
                    "commit preparation failed: {error}"
                ))));
                continue;
            }
        };
        let materialized = match materialize_commit(prepared) {
            Ok(value) => value,
            Err(error) => {
                outcomes[index] = Some(Err(CoreError::Store(format!(
                    "commit materialization failed: {error}"
                ))));
                continue;
            }
        };
        let preview = match wal_payload_from_materialized_commit(&materialized) {
            Ok(payload) => payload,
            Err(error) => {
                outcomes[index] = Some(Err(error.into()));
                continue;
            }
        };
        match current_metadata_state.apply_committed_wal_record(&preview) {
            Ok(applied) => {
                current_metadata_state = applied.metadata_state;
                current_head.seq = plan.assigned_seq;
                current_head.next_inode_id = plan.resulting_next_inode_id;
                accepted.push((index, materialized));
            }
            Err(error) => outcomes[index] = Some(Err(error.into())),
        }
    }

    if accepted.is_empty() {
        return finish_batch_outcomes_with_aliases(outcomes, &aliases);
    }
    let records = accepted
        .iter()
        .map(|(_, record)| record.clone())
        .collect::<Vec<_>>();
    let wal = match prepare_wal_segment(
        namespace_id.clone(),
        basis.head.visible_wal_tip.clone(),
        &records,
        &context.writer_version,
    ) {
        Ok(wal) => wal,
        Err(error) => {
            let message = format!("wal build failed: {error:?}");
            for (index, _) in accepted {
                outcomes[index] = Some(Err(CoreError::Store(message.clone())));
            }
            return finish_batch_outcomes_with_aliases(outcomes, &aliases);
        }
    };
    match store.put_if_absent(&wal.object_key, &wal.encoded_bytes) {
        Ok(_) => {}
        Err(err) => {
            for (index, _) in accepted {
                outcomes[index] = Some(Err(CoreError::WalWrite(err.to_string())));
            }
            return finish_batch_outcomes_with_aliases(outcomes, &aliases);
        }
    }

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
            let message = format!("head publish preparation failed: {error:?}");
            for (index, _) in accepted {
                outcomes[index] = Some(Err(CoreError::Store(message.clone())));
            }
            return finish_batch_outcomes_with_aliases(outcomes, &aliases);
        }
    };
    if let Err(error) = publish_commit_head(store, &basis.head_etag, &head_publish) {
        for (index, _) in accepted {
            outcomes[index] = Some(Err(error.clone().into()));
        }
        return finish_batch_outcomes_with_aliases(outcomes, &aliases);
    }

    for (accepted_index, (outcome_index, record)) in accepted.into_iter().enumerate() {
        outcomes[outcome_index] = Some(Ok(ApiCommitResponse {
            namespace_id: namespace_id.clone(),
            commit_id: record.prepared.request.commit_id,
            committed_seq: wal.envelope.payload.records[accepted_index].seq,
            results: record.ops.into_iter().map(|op| op.result).collect(),
        }));
    }
    finish_batch_outcomes_with_aliases(outcomes, &aliases)
}

#[allow(clippy::too_many_arguments)]
fn prepare_candidate_request<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: &crate::VerifiedNamespaceBasis,
    current_head: &HeadState,
    current_metadata_state: &MetadataState,
    candidate: NamespaceMutationCandidate,
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
            let request = match commit_request_from_v0(conversion_context, request) {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(error.into()));
                    return None;
                }
            };
            let semantic_fingerprint = match semantic_commit_fingerprint(&request) {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(CoreError::Store(error.to_string())));
                    return None;
                }
            };
            if !record_primary_request_or_complete_idempotent(
                namespace_id,
                &basis.metadata_state,
                outcomes,
                in_batch_requests,
                aliases,
                index,
                &request.commit_id,
                &semantic_fingerprint,
            ) {
                return None;
            }
            Some(CandidateCoreRequest {
                request,
                fingerprint_source: CommitFingerprintSource::ComputeFromRequest,
            })
        }
        NamespaceMutationCandidate::Path(intent) => {
            if let Err(error) = validate_commit_id(intent.commit_id()) {
                outcomes[index] = Some(Err(error));
                return None;
            }
            let semantic_fingerprint = match intent.semantic_commit_fingerprint(namespace_id) {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(error));
                    return None;
                }
            };
            let commit_id = intent.commit_id().clone();
            if !record_primary_request_or_complete_idempotent(
                namespace_id,
                &basis.metadata_state,
                outcomes,
                in_batch_requests,
                aliases,
                index,
                &commit_id,
                &semantic_fingerprint,
            ) {
                return None;
            }
            let planned = match crate::services::plan_path_mutation_against_state(
                store,
                namespace_id,
                &intent,
                current_head,
                current_metadata_state,
                &basis.content_store_id,
            ) {
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
                fingerprint_source: CommitFingerprintSource::TrustedPrecomputed(
                    planned.semantic_commit_fingerprint,
                ),
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
    semantic_commit_fingerprint: &SemanticCommitFingerprint,
) -> bool {
    if let Some(existing) = find_commit_receipt(visible_metadata_state, commit_id) {
        outcomes[index] = Some(
            if existing.semantic_commit_fingerprint_sha256 != semantic_commit_fingerprint.as_str() {
                Err(CoreError::CommitIdReuseConflict(commit_id.to_string()))
            } else {
                Ok(commit_response_from_commit_receipt(namespace_id, existing))
            },
        );
        return false;
    }
    if let Some(existing) = in_batch_requests.get(commit_id) {
        if existing.semantic_commit_fingerprint != *semantic_commit_fingerprint {
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
            semantic_commit_fingerprint: semantic_commit_fingerprint.clone(),
        },
    );
    true
}

pub fn list_changes_after<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after_seq: ChangeSeq,
) -> Result<ChangesResponse, CoreError> {
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    if after_seq < basis.head.retention_floor_seq {
        return Err(CoreError::RebootstrapRequired {
            after_seq,
            retention_floor_seq: basis.head.retention_floor_seq,
        });
    }
    if after_seq >= basis.head.seq {
        return Ok(ChangesResponse {
            namespace_id: namespace_id.clone(),
            from_exclusive_seq: after_seq,
            through_seq: basis.head.seq,
            changes: Vec::new(),
        });
    }

    let wal_objects = load_wal_range(store, namespace_id, after_seq, basis.head.seq, &basis.head)?;
    let mut changes = Vec::new();
    for wal_object in wal_objects {
        let envelope = decode_wal_segment_envelope_zstd(&wal_object.encoded_bytes)
            .map_err(|err| CoreError::Store(err.to_string()))?;
        for record in envelope.payload.records {
            if record.seq > after_seq {
                changes.push(CommittedChange {
                    seq: record.seq,
                    commit_id: record.commit_id,
                    message: record.message,
                    annotations: record.annotations,
                    ops: record.results,
                });
            }
        }
    }

    Ok(ChangesResponse {
        namespace_id: namespace_id.clone(),
        from_exclusive_seq: after_seq,
        through_seq: basis.head.seq,
        changes,
    })
}

fn validate_commit_content_references<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: &CoreCommitRequest,
    resolved_restore_content_refs: &[Option<ContentRef>],
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

    let content_store_id = load_namespace_content_store_id(store, namespace_id)?;
    for content_ref in content_refs {
        validate_durable_content_reference(store, &content_store_id, content_ref)?;
    }

    Ok(())
}

fn read_upload_session_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
) -> Result<LoadedUploadSessionObject, CoreError> {
    NamespaceId::parse(namespace_id.as_str()).map_err(CoreError::from)?;
    validate_upload_id(upload_id).map_err(CoreError::InvalidUploadId)?;
    let object_key = upload_session(namespace_id.as_str(), upload_id);
    let metadata = store
        .head(&object_key)
        .map_err(|err| CoreError::Store(err.to_string()))?
        .ok_or_else(|| CoreError::UploadNotFound {
            upload_id: upload_id.to_owned(),
        })?;
    let encoded = store
        .get(&object_key, None)
        .map_err(|err| CoreError::Store(err.to_string()))?
        .ok_or_else(|| CoreError::UploadNotFound {
            upload_id: upload_id.to_owned(),
        })?;
    let envelope: UploadSessionEnvelope =
        serde_json::from_slice(&encoded).map_err(|err| CoreError::Store(err.to_string()))?;
    if envelope.kind != ControlObjectKind::UploadSession {
        return Err(CoreError::Store(format!(
            "unexpected upload session kind for `{object_key}`"
        )));
    }
    if !envelope
        .has_valid_payload_checksum()
        .map_err(|err| CoreError::Store(err.to_string()))?
    {
        return Err(CoreError::Store(format!(
            "upload session checksum mismatch for `{object_key}`"
        )));
    }
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

fn load_wal_range<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_seq_exclusive: ChangeSeq,
    through_seq_inclusive: ChangeSeq,
    head: &loon_api::HeadState,
) -> Result<Vec<crate::wal::StoredWalObject>, CoreError> {
    if through_seq_inclusive <= from_seq_exclusive {
        return Ok(Vec::new());
    }
    let prefix = format!("namespaces/{}/wal/", namespace_id.as_str());
    let mut pointer =
        head.visible_wal_tip
            .clone()
            .ok_or_else(|| BasisLoadError::MissingWalObject {
                prefix: prefix.clone(),
                seq: through_seq_inclusive,
            })?;
    let mut out = Vec::new();
    loop {
        if pointer.end_seq <= from_seq_exclusive {
            break;
        }
        let encoded_bytes = store
            .get(&pointer.object_key, None)
            .map_err(|err| BasisLoadError::ReadWal {
                object_key: pointer.object_key.clone(),
                message: err.to_string(),
            })?
            .ok_or_else(|| BasisLoadError::MissingWalObjectAfterList {
                object_key: pointer.object_key.clone(),
            })?;
        let envelope = decode_wal_segment_envelope_zstd(&encoded_bytes)
            .map_err(|err| CoreError::Store(err.to_string()))?;
        let prev = envelope.payload.prev_visible_segment.clone();
        out.push(StoredWalObject {
            object_key: pointer.object_key,
            encoded_bytes,
        });
        let Some(prev) = prev else {
            break;
        };
        pointer = prev;
    }
    out.reverse();
    Ok(out)
}

fn find_commit_receipt<'a>(
    metadata_state: &'a crate::metadata::MetadataState,
    commit_id: &CommitId,
) -> Option<&'a CommitReceiptRecord> {
    metadata_state
        .commit_receipts
        .iter()
        .filter(|record| record.commit_id == *commit_id)
        .max_by_key(|record| record.committed_seq)
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
