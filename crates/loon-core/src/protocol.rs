use crate::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::commit::{
    build_commit_plan, prepare_commit_head_publish, publish_commit_head,
    resolve_restore_content_refs, CommitOp, CommitRequest as CoreCommitRequest,
    CommitValidationContext, Precondition,
};
use crate::content::{validate_durable_content_reference, write_immutable_object};
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::metadata::RequestReceiptRecord;
use crate::namespace::catalog::load_namespace_content_store_id;
use crate::wal::{prepare_wal_segment, PreparedWalRecord, StoredWalObject};
use loon_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitOp as V0CommitOp, CommitOpResult,
    CommitPrecondition as V0CommitPrecondition, CommitRequest as V0CommitRequest,
    CommitResponse as V0CommitResponse, CommittedChange, CompleteUploadRequest,
    CompleteUploadResponse, UploadContentResponse, UploadMode,
};
use loon_api::{
    decode_wal_segment_envelope_zstd, ChangeSeq, CompletedUpload, ContentRef, ControlObjectKind,
    InodeId, NamespaceId, UploadSessionEnvelope, UploadSessionState,
};
use loon_objectstore::keys::{content_blob, upload_session};
use loon_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError};
use std::collections::HashMap;
use uuid::Uuid;

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
    semantic_fingerprint_sha256: String,
}

pub fn begin_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<BeginUploadResponse, CoreError> {
    let _basis = load_verified_namespace_basis(store, namespace_id)?;
    let upload_id = format!("upl_{}", Uuid::new_v4().simple());
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
    request: V0CommitRequest,
    context: &MutationContext,
) -> Result<V0CommitResponse, CoreError> {
    commit_operations_with_source_checksum(store, namespace_id, request, None, context)
}

pub fn commit_operations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    requests: Vec<V0CommitRequest>,
    context: &MutationContext,
) -> Vec<Result<V0CommitResponse, CoreError>> {
    commit_operations_batch_with_source_checksums(
        store,
        namespace_id,
        requests
            .into_iter()
            .map(|request| (request, None))
            .collect(),
        context,
    )
}

pub(crate) fn commit_path_operations<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: V0CommitRequest,
    source_request_checksum_sha256: String,
    context: &MutationContext,
) -> Result<V0CommitResponse, CoreError> {
    commit_operations_with_source_checksum(
        store,
        namespace_id,
        request,
        Some(source_request_checksum_sha256),
        context,
    )
}

pub(crate) fn retry_existing_path_request<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request_id: &str,
    source_request_checksum_sha256: &str,
    context: &MutationContext,
) -> Result<Option<V0CommitResponse>, CoreError> {
    let _ = context;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let Some(receipt) = find_request_receipt(&basis.metadata_state, request_id) else {
        return Ok(None);
    };
    if receipt.semantic_fingerprint_sha256 != source_request_checksum_sha256 {
        return Err(CoreError::RequestIdConflict(request_id.to_owned()));
    }
    Ok(Some(commit_response_from_receipt(namespace_id, receipt)))
}

fn commit_operations_with_source_checksum<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: V0CommitRequest,
    source_request_checksum_sha256: Option<String>,
    context: &MutationContext,
) -> Result<V0CommitResponse, CoreError> {
    commit_operations_batch_with_source_checksums(
        store,
        namespace_id,
        vec![(request, source_request_checksum_sha256)],
        context,
    )
    .pop()
    .unwrap_or_else(|| Err(CoreError::Store("empty commit batch".to_owned())))
}

fn commit_operations_batch_with_source_checksums<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    requests: Vec<(V0CommitRequest, Option<String>)>,
    context: &MutationContext,
) -> Vec<Result<V0CommitResponse, CoreError>> {
    if requests.is_empty() {
        return Vec::new();
    }
    if let Err(error) = crate::acquire_or_renew_namespace_lease(store, namespace_id, context) {
        return (0..requests.len())
            .map(|_| Err(CoreError::Lease(error.clone())))
            .collect();
    }
    let basis = match load_verified_namespace_basis(store, namespace_id) {
        Ok(basis) => basis,
        Err(error) => {
            return (0..requests.len())
                .map(|_| Err(CoreError::Basis(error.clone())))
                .collect()
        }
    };

    let mut outcomes: Vec<Option<Result<V0CommitResponse, CoreError>>> =
        (0..requests.len()).map(|_| None).collect();
    let mut current_head = basis.head.clone();
    let mut current_metadata_state = basis.metadata_state.clone();
    let mut accepted: Vec<(usize, PreparedWalRecord)> = Vec::new();
    let mut in_batch_requests: HashMap<String, InBatchRequest> = HashMap::new();
    let mut aliases: Vec<(usize, usize)> = Vec::new();

    for (index, (request, source_request_checksum_sha256)) in requests.into_iter().enumerate() {
        let request = map_commit_request(
            namespace_id,
            &basis,
            request,
            source_request_checksum_sha256,
            context,
        );
        let semantic_fingerprint = match request.semantic_fingerprint_sha256() {
            Ok(value) => value,
            Err(err) => {
                outcomes[index] = Some(Err(CoreError::Store(err.to_string())));
                continue;
            }
        };
        if let Some(existing) = find_request_receipt(&basis.metadata_state, &request.request_id) {
            outcomes[index] = Some(
                if existing.semantic_fingerprint_sha256 != semantic_fingerprint {
                    Err(CoreError::RequestIdConflict(request.request_id.clone()))
                } else {
                    Ok(commit_response_from_receipt(namespace_id, existing))
                },
            );
            continue;
        }
        if let Some(existing) = in_batch_requests.get(&request.request_id) {
            if existing.semantic_fingerprint_sha256 != semantic_fingerprint {
                outcomes[index] = Some(Err(CoreError::RequestIdConflict(
                    request.request_id.clone(),
                )));
            } else {
                aliases.push((index, existing.primary_index));
            }
            continue;
        }
        in_batch_requests.insert(
            request.request_id.clone(),
            InBatchRequest {
                primary_index: index,
                semantic_fingerprint_sha256: semantic_fingerprint,
            },
        );

        let validation = CommitValidationContext {
            head: current_head.clone(),
            lease: basis.lease.clone(),
            now_ms: context.now_ms,
            metadata_state: current_metadata_state.clone(),
        };
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
        let results = derive_commit_results(
            &request.ops,
            &plan.allocated_inode_ids,
            &plan.resolved_restore_content_refs,
        );
        let record = PreparedWalRecord {
            request,
            plan: plan.clone(),
            results,
        };
        let preview = match prepare_wal_segment(
            namespace_id.clone(),
            current_head.visible_wal_tip.clone(),
            std::slice::from_ref(&record),
            &context.writer_version,
        ) {
            Ok(segment) => segment.envelope.payload.records[0].clone(),
            Err(error) => {
                outcomes[index] = Some(Err(error.into()));
                continue;
            }
        };
        match current_metadata_state.apply_committed_wal_record(&preview) {
            Ok(applied) => {
                current_metadata_state = applied.metadata_state;
                current_head.seq = plan.next_seq;
                current_head.next_inode_id = plan.resulting_next_inode_id;
                accepted.push((index, record));
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
        Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => {
            match store.get(&wal.object_key, None) {
                Ok(Some(existing)) if existing == wal.encoded_bytes => {}
                Ok(_) => {
                    for (index, _) in accepted {
                        outcomes[index] = Some(Err(CoreError::WalWrite(
                            "conflicting WAL segment object already exists".to_owned(),
                        )));
                    }
                    return finish_batch_outcomes_with_aliases(outcomes, &aliases);
                }
                Err(err) => {
                    for (index, _) in accepted {
                        outcomes[index] = Some(Err(CoreError::WalWrite(err.to_string())));
                    }
                    return finish_batch_outcomes_with_aliases(outcomes, &aliases);
                }
            }
        }
        Err(err) => {
            for (index, _) in accepted {
                outcomes[index] = Some(Err(CoreError::WalWrite(err.to_string())));
            }
            return finish_batch_outcomes_with_aliases(outcomes, &aliases);
        }
    }

    let last_plan = &records.last().expect("non-empty accepted records").plan;
    let head_publish = prepare_commit_head_publish(
        &basis.head,
        last_plan,
        wal.envelope.pointer(wal.object_key.clone()),
        &context.writer_version,
    );
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
        outcomes[outcome_index] = Some(Ok(V0CommitResponse {
            namespace_id: namespace_id.clone(),
            commit_id: record.request.request_id,
            committed_seq: wal.envelope.payload.records[accepted_index].seq,
            results: record.results,
        }));
    }
    finish_batch_outcomes_with_aliases(outcomes, &aliases)
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
                    request_id: record.request_id,
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

fn derive_commit_results(
    ops: &[CommitOp],
    allocated_inode_ids: &[InodeId],
    resolved_restore_content_refs: &[Option<ContentRef>],
) -> Vec<CommitOpResult> {
    let mut allocated = allocated_inode_ids.iter().copied();
    ops.iter()
        .enumerate()
        .map(|(index, op)| {
            let op_index = u32::try_from(index).expect("commit op index should fit in u32");
            match op {
                CommitOp::CreateDir { .. } => CommitOpResult::CreateDir {
                    op_index,
                    inode_id: allocated
                        .next()
                        .expect("allocated inode ids should cover create ops"),
                },
                CommitOp::CreateFile { content_ref, .. } => CommitOpResult::CreateFile {
                    op_index,
                    inode_id: allocated
                        .next()
                        .expect("allocated inode ids should cover create ops"),
                    revision_no: loon_api::RevisionNo(1),
                    content_ref: content_ref.clone(),
                },
                CommitOp::ReplaceFile {
                    inode_id,
                    base_revision,
                    content_ref,
                } => CommitOpResult::ReplaceFile {
                    op_index,
                    inode_id: *inode_id,
                    revision_no: loon_api::RevisionNo(
                        base_revision
                            .0
                            .checked_add(1)
                            .expect("replace_file revision increment validated"),
                    ),
                    content_ref: content_ref.clone(),
                },
                CommitOp::RestoreRevision {
                    inode_id,
                    source_revision,
                    base_revision,
                } => CommitOpResult::RestoreRevision {
                    op_index,
                    inode_id: *inode_id,
                    source_revision_no: *source_revision,
                    revision_no: loon_api::RevisionNo(
                        base_revision
                            .0
                            .checked_add(1)
                            .expect("restore_revision increment validated"),
                    ),
                    content_ref: resolved_restore_content_refs[index]
                        .as_ref()
                        .expect("resolved restore content ref should be present")
                        .clone(),
                },
                CommitOp::DeleteFile { inode_id } => CommitOpResult::DeleteFile {
                    op_index,
                    inode_id: *inode_id,
                },
                CommitOp::Rename { inode_id, .. } => CommitOpResult::Rename {
                    op_index,
                    inode_id: *inode_id,
                },
                CommitOp::DeleteSubtree { root_inode } => CommitOpResult::DeleteSubtree {
                    op_index,
                    root_inode: *root_inode,
                },
            }
        })
        .collect()
}

fn map_commit_request(
    namespace_id: &NamespaceId,
    basis: &crate::VerifiedNamespaceBasis,
    request: V0CommitRequest,
    source_request_checksum_sha256: Option<String>,
    context: &MutationContext,
) -> CoreCommitRequest {
    CoreCommitRequest {
        namespace_id: namespace_id.clone(),
        request_id: request.request_id,
        writer_id: context.writer_id.clone(),
        writer_fence_token: basis.head.active_fence_token,
        planned_head_seq: request.planned_head_seq,
        source_request_checksum_sha256,
        ops: request.ops.into_iter().map(map_commit_op).collect(),
        preconditions: request
            .preconditions
            .into_iter()
            .map(map_commit_precondition)
            .collect(),
        message: request.message,
        annotations: request.annotations,
    }
}

fn map_commit_op(op: V0CommitOp) -> CommitOp {
    match op {
        V0CommitOp::CreateDir {
            parent_inode,
            display_name,
        } => CommitOp::CreateDir {
            parent_inode,
            display_name,
        },
        V0CommitOp::CreateFile {
            parent_inode,
            display_name,
            content_ref,
        } => CommitOp::CreateFile {
            parent_inode,
            display_name,
            content_ref,
        },
        V0CommitOp::ReplaceFile {
            inode_id,
            base_revision_no,
            content_ref,
        } => CommitOp::ReplaceFile {
            inode_id,
            base_revision: base_revision_no,
            content_ref,
        },
        V0CommitOp::RestoreRevision {
            inode_id,
            source_revision_no,
            base_revision_no,
        } => CommitOp::RestoreRevision {
            inode_id,
            source_revision: source_revision_no,
            base_revision: base_revision_no,
        },
        V0CommitOp::DeleteFile { inode_id } => CommitOp::DeleteFile { inode_id },
        V0CommitOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
        } => CommitOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
        },
        V0CommitOp::DeleteSubtree { root_inode } => CommitOp::DeleteSubtree { root_inode },
    }
}

fn map_commit_precondition(precondition: V0CommitPrecondition) -> Precondition {
    match precondition {
        V0CommitPrecondition::HeadSeqIs { expected_seq } => Precondition::HeadSeqIs(expected_seq),
        V0CommitPrecondition::InodeRevisionIs {
            inode_id,
            revision_no,
        } => Precondition::InodeRevisionIs {
            inode_id,
            revision: revision_no,
        },
        V0CommitPrecondition::AncestorsNotSubtreeDeleted { inode_id } => {
            Precondition::AncestorsNotSubtreeDeleted { inode_id }
        }
        V0CommitPrecondition::ChildNameAbsent {
            parent_inode,
            name_key,
        } => Precondition::ChildNameAbsent {
            parent_inode,
            name_key,
        },
    }
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

fn find_request_receipt<'a>(
    metadata_state: &'a crate::metadata::MetadataState,
    request_id: &str,
) -> Option<&'a RequestReceiptRecord> {
    metadata_state
        .request_receipts
        .iter()
        .filter(|receipt| receipt.request_id == request_id)
        .max_by_key(|receipt| receipt.committed_seq)
}

fn commit_response_from_receipt(
    namespace_id: &NamespaceId,
    receipt: &RequestReceiptRecord,
) -> V0CommitResponse {
    V0CommitResponse {
        namespace_id: namespace_id.clone(),
        commit_id: receipt.commit_id.clone(),
        committed_seq: receipt.committed_seq,
        results: receipt.results.clone(),
    }
}

fn finish_batch_outcomes(
    outcomes: Vec<Option<Result<V0CommitResponse, CoreError>>>,
) -> Vec<Result<V0CommitResponse, CoreError>> {
    outcomes
        .into_iter()
        .map(|outcome| {
            outcome.unwrap_or_else(|| Err(CoreError::Store("missing batch outcome".to_owned())))
        })
        .collect()
}

fn finish_batch_outcomes_with_aliases(
    mut outcomes: Vec<Option<Result<V0CommitResponse, CoreError>>>,
    aliases: &[(usize, usize)],
) -> Vec<Result<V0CommitResponse, CoreError>> {
    for (alias_index, primary_index) in aliases {
        let primary_outcome = outcomes
            .get(*primary_index)
            .and_then(Clone::clone)
            .unwrap_or_else(|| Err(CoreError::Store("missing primary batch outcome".to_owned())));
        outcomes[*alias_index] = Some(primary_outcome);
    }
    finish_batch_outcomes(outcomes)
}
