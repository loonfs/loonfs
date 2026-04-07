use crate::basis::{load_verified_namespace_basis, BasisLoadError};
use crate::commit::{
    build_commit_plan, prepare_commit_head_publish, publish_commit_head,
    resolve_restore_content_manifest_digests, CommitOp, CommitRequest as CoreCommitRequest,
    CommitValidationContext, Precondition,
};
use crate::content::validate_durable_content_reference;
use crate::services::{derive_commit_results, write_immutable_object, CoreError, MutationContext};
use crate::wal::prepare_wal_commit;
use loon_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitOp as V0CommitOp,
    CommitPrecondition as V0CommitPrecondition, CommitRequest as V0CommitRequest,
    CommitResponse as V0CommitResponse, CommittedChange, CompleteUploadRequest,
    CompleteUploadResponse, UploadBlockResponse, UploadMode,
};
use loon_api::{
    content_manifest_digest_sha256, decode_wal_commit_envelope_zstd, encode_content_manifest_json,
    sha256_digest, ChangeSeq, CompletedUpload, ContentBlockDescriptor, ContentManifestEnvelope,
    ContentManifestPayload, ControlObjectKind, NamespaceId, UploadSessionEnvelope,
    UploadSessionState, UploadedBlock, CONTENT_BLOCK_SIZE_BYTES,
};
use loon_objectstore::keys::{blob, content_manifest, upload_session, wal_commit};
use loon_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const UPLOAD_SESSION_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone)]
struct LoadedUploadSessionObject {
    object_key: String,
    metadata: ObjectMetadata,
    envelope: UploadSessionEnvelope,
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
        block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
        uploaded_blocks: Vec::new(),
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
        block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
        mode: UploadMode::ServiceProxied,
    })
}

pub fn upload_block<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
    block_index: u32,
    bytes: &[u8],
    context: &MutationContext,
) -> Result<UploadBlockResponse, CoreError> {
    validate_upload_block_bytes(bytes)?;
    let descriptor = UploadedBlock {
        block_index,
        content_digest_sha256: sha256_digest(bytes),
        plaintext_size_bytes: bytes.len() as u64,
    };

    for _attempt in 0..UPLOAD_SESSION_RETRY_LIMIT {
        let loaded = read_upload_session_object(store, namespace_id, upload_id)?;
        if let Some(completed) = &loaded.envelope.state.completed {
            let _ = completed;
            return Err(CoreError::UploadAlreadyCompleted {
                upload_id: upload_id.to_owned(),
            });
        }

        if let Some(existing) = loaded
            .envelope
            .state
            .uploaded_blocks
            .iter()
            .find(|block| block.block_index == block_index)
        {
            if existing == &descriptor {
                return Ok(UploadBlockResponse {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.to_owned(),
                    block_index,
                    content_digest_sha256: descriptor.content_digest_sha256,
                    plaintext_size_bytes: descriptor.plaintext_size_bytes,
                });
            }
            return Err(CoreError::UploadBlockConflict {
                upload_id: upload_id.to_owned(),
                block_index,
            });
        }

        write_immutable_object(
            store,
            &blob(namespace_id.as_str(), &descriptor.content_digest_sha256),
            bytes,
        )?;

        let mut next_state = loaded.envelope.state.clone();
        next_state.uploaded_blocks.push(descriptor.clone());
        next_state
            .uploaded_blocks
            .sort_by_key(|block| block.block_index);

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
                return Ok(UploadBlockResponse {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.to_owned(),
                    block_index,
                    content_digest_sha256: descriptor.content_digest_sha256,
                    plaintext_size_bytes: descriptor.plaintext_size_bytes,
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
            if completed.file_size_bytes == request.file_size_bytes
                && completed.file_digest_sha256 == request.file_digest_sha256
            {
                return Ok(CompleteUploadResponse {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.to_owned(),
                    content_manifest_digest: completed.content_manifest_digest.clone(),
                });
            }
            return Err(CoreError::UploadAlreadyCompleted {
                upload_id: upload_id.to_owned(),
            });
        }

        let blocks = build_ordered_block_descriptors(&loaded.envelope.state.uploaded_blocks)?;
        let (actual_size, actual_digest) =
            verify_uploaded_block_bytes(store, namespace_id, &blocks)?;
        if actual_size != request.file_size_bytes {
            return Err(CoreError::InvalidUploadBlock(format!(
                "file size mismatch: expected {}, actual {}",
                request.file_size_bytes, actual_size
            )));
        }
        if actual_digest != request.file_digest_sha256 {
            return Err(CoreError::InvalidUploadBlock(format!(
                "file digest mismatch: expected `{}`, actual `{}`",
                request.file_digest_sha256, actual_digest
            )));
        }

        let manifest = ContentManifestEnvelope::from_payload(ContentManifestPayload {
            namespace_id: namespace_id.clone(),
            file_size_bytes: actual_size,
            file_digest_sha256: actual_digest.clone(),
            block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
            blocks,
        })
        .map_err(|err| CoreError::Store(err.to_string()))?;
        let manifest_digest = content_manifest_digest_sha256(&manifest)
            .map_err(|err| CoreError::Store(err.to_string()))?;
        let manifest_bytes = encode_content_manifest_json(&manifest)
            .map_err(|err| CoreError::Store(err.to_string()))?;
        write_immutable_object(
            store,
            &content_manifest(namespace_id.as_str(), &manifest_digest),
            &manifest_bytes,
        )?;

        let mut next_state = loaded.envelope.state.clone();
        next_state.completed = Some(CompletedUpload {
            file_size_bytes: actual_size,
            file_digest_sha256: actual_digest,
            content_manifest_digest: manifest_digest.clone(),
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
                    content_manifest_digest: manifest_digest,
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
    let Some(existing) =
        find_existing_commit_payload_by_request_id(store, namespace_id, request_id)?
    else {
        return Ok(None);
    };

    if existing.source_request_checksum_sha256.as_deref() != Some(source_request_checksum_sha256) {
        return Err(CoreError::RequestIdConflict(request_id.to_owned()));
    }

    let request = request_from_payload(&existing);
    commit_operations_with_source_checksum(
        store,
        namespace_id,
        request,
        existing.source_request_checksum_sha256.clone(),
        context,
    )
    .map(Some)
}

fn commit_operations_with_source_checksum<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: V0CommitRequest,
    source_request_checksum_sha256: Option<String>,
    context: &MutationContext,
) -> Result<V0CommitResponse, CoreError> {
    crate::acquire_or_renew_namespace_lease(store, namespace_id, context)?;
    let basis = load_verified_namespace_basis(store, namespace_id)?;
    let request = map_commit_request(
        namespace_id,
        &basis,
        request,
        source_request_checksum_sha256,
        context,
    );
    let next_seq = request
        .planned_head_seq
        .0
        .checked_add(1)
        .map(ChangeSeq)
        .ok_or(crate::commit::CommitValidationError::SeqOverflow)?;
    let wal_key = wal_commit(namespace_id.as_str(), next_seq.0, &request.request_id);
    let request_checksum = request
        .request_checksum_sha256()
        .map_err(|err| CoreError::Store(err.to_string()))?;

    if let Some(existing) = load_existing_commit_payload(store, &wal_key)? {
        if existing.request_checksum_sha256 != request_checksum {
            return Err(CoreError::RequestIdConflict(request.request_id.clone()));
        }
        if basis.head.seq >= existing.seq {
            return Ok(commit_response_from_payload(&existing));
        }
    }

    let validation = CommitValidationContext {
        head: basis.head.clone(),
        lease: basis.lease.clone(),
        now_ms: context.now_ms,
        metadata_state: basis.metadata_state.clone(),
    };
    let resolved_restore_content_manifest_digests =
        resolve_restore_content_manifest_digests(&request, &validation)
            .map_err(CoreError::CommitValidation)?;
    validate_commit_content_references(
        store,
        namespace_id,
        &request,
        &resolved_restore_content_manifest_digests,
    )?;
    let plan = build_commit_plan(&request, &validation)?;
    debug_assert_eq!(
        plan.resolved_restore_content_manifest_digests,
        resolved_restore_content_manifest_digests
    );
    let results = derive_commit_results(
        &request.ops,
        &plan.allocated_inode_ids,
        &plan.resolved_restore_content_manifest_digests,
    );
    let wal = prepare_wal_commit(&request, &plan, results.clone(), &context.writer_version)?;
    let _applied = basis
        .metadata_state
        .apply_committed_wal_ops(plan.next_seq, &wal.envelope.payload.ops)?;

    match store.put_if_absent(&wal.object_key, &wal.encoded_bytes) {
        Ok(_) => {}
        Err(ObjectStoreError::PreconditionFailed | ObjectStoreError::Conflict) => {
            let existing =
                load_existing_commit_payload(store, &wal.object_key)?.ok_or_else(|| {
                    CoreError::Store("commit WAL disappeared after conflict".to_owned())
                })?;
            if existing.request_checksum_sha256 != request_checksum {
                return Err(CoreError::RequestIdConflict(request.request_id.clone()));
            }
            if basis.head.seq >= existing.seq {
                return Ok(commit_response_from_payload(&existing));
            }
        }
        Err(err) => return Err(CoreError::WalWrite(err.to_string())),
    }

    let head_publish = prepare_commit_head_publish(&basis.head, &plan, &context.writer_version)?;
    publish_commit_head(store, &basis.head_etag, &head_publish)?;

    Ok(V0CommitResponse {
        namespace_id: namespace_id.clone(),
        commit_id: request.request_id,
        committed_seq: head_publish.resulting_head.seq,
        results,
    })
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

    let wal_objects = load_wal_range(store, namespace_id, after_seq, basis.head.seq)?;
    let mut changes = Vec::with_capacity(wal_objects.len());
    for wal_object in wal_objects {
        let envelope = decode_wal_commit_envelope_zstd(&wal_object.encoded_bytes)
            .map_err(|err| CoreError::Store(err.to_string()))?;
        changes.push(CommittedChange {
            seq: envelope.payload.seq,
            commit_id: envelope.payload.commit_id,
            request_id: envelope.payload.request_id,
            message: envelope.payload.message,
            annotations: envelope.payload.annotations,
            ops: envelope.payload.results,
        });
    }

    Ok(ChangesResponse {
        namespace_id: namespace_id.clone(),
        from_exclusive_seq: after_seq,
        through_seq: basis.head.seq,
        changes,
    })
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

fn request_from_payload(payload: &loon_api::WalCommitPayload) -> V0CommitRequest {
    V0CommitRequest {
        request_id: payload.request_id.clone(),
        planned_head_seq: payload.base_head_seq,
        preconditions: payload
            .preconditions
            .iter()
            .cloned()
            .map(map_wal_precondition)
            .collect(),
        ops: payload.ops.iter().cloned().map(map_wal_op).collect(),
        message: payload.message.clone(),
        annotations: payload.annotations.clone(),
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
            content_manifest_digest,
        } => CommitOp::CreateFile {
            parent_inode,
            display_name,
            content_manifest_digest,
        },
        V0CommitOp::ReplaceFile {
            inode_id,
            base_revision_no,
            content_manifest_digest,
        } => CommitOp::ReplaceFile {
            inode_id,
            base_revision: base_revision_no,
            content_manifest_digest,
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

fn map_wal_op(op: loon_api::WalOp) -> V0CommitOp {
    match op {
        loon_api::WalOp::CreateDir {
            parent_inode,
            display_name,
            ..
        } => V0CommitOp::CreateDir {
            parent_inode,
            display_name,
        },
        loon_api::WalOp::CreateFile {
            parent_inode,
            display_name,
            content_manifest_digest,
            ..
        } => V0CommitOp::CreateFile {
            parent_inode,
            display_name,
            content_manifest_digest,
        },
        loon_api::WalOp::ReplaceFile {
            inode_id,
            base_revision,
            content_manifest_digest,
            ..
        } => V0CommitOp::ReplaceFile {
            inode_id,
            base_revision_no: base_revision,
            content_manifest_digest,
        },
        loon_api::WalOp::RestoreRevision {
            inode_id,
            source_revision_no,
            base_revision,
            ..
        } => V0CommitOp::RestoreRevision {
            inode_id,
            source_revision_no,
            base_revision_no: base_revision,
        },
        loon_api::WalOp::DeleteFile { inode_id, .. } => V0CommitOp::DeleteFile { inode_id },
        loon_api::WalOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
            ..
        } => V0CommitOp::Rename {
            inode_id,
            new_parent_inode,
            new_display_name,
        },
        loon_api::WalOp::DeleteSubtree { root_inode, .. } => {
            V0CommitOp::DeleteSubtree { root_inode }
        }
    }
}

fn map_wal_precondition(precondition: loon_api::WalPrecondition) -> V0CommitPrecondition {
    match precondition {
        loon_api::WalPrecondition::HeadSeqIs(expected_seq) => {
            V0CommitPrecondition::HeadSeqIs { expected_seq }
        }
        loon_api::WalPrecondition::InodeRevisionIs { inode_id, revision } => {
            V0CommitPrecondition::InodeRevisionIs {
                inode_id,
                revision_no: revision,
            }
        }
        loon_api::WalPrecondition::AncestorsNotSubtreeDeleted { inode_id } => {
            V0CommitPrecondition::AncestorsNotSubtreeDeleted { inode_id }
        }
        loon_api::WalPrecondition::ChildNameAbsent {
            parent_inode,
            name_key,
        } => V0CommitPrecondition::ChildNameAbsent {
            parent_inode,
            name_key,
        },
    }
}

fn validate_commit_content_references<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: &CoreCommitRequest,
    resolved_restore_content_manifest_digests: &[Option<String>],
) -> Result<(), CoreError> {
    for (index, op) in request.ops.iter().enumerate() {
        match op {
            CommitOp::CreateFile {
                content_manifest_digest,
                ..
            }
            | CommitOp::ReplaceFile {
                content_manifest_digest,
                ..
            } => {
                validate_durable_content_reference(store, namespace_id, content_manifest_digest)?;
            }
            CommitOp::RestoreRevision {
                inode_id,
                source_revision,
                ..
            } => {
                let content_manifest_digest = resolved_restore_content_manifest_digests
                    .get(index)
                    .and_then(|digest| digest.as_deref())
                    .ok_or_else(|| {
                        CoreError::Store(format!(
                            "missing resolved restore manifest digest for inode {:?} source revision {:?}",
                            inode_id, source_revision
                        ))
                    })?;
                validate_durable_content_reference(store, namespace_id, content_manifest_digest)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_upload_block_bytes(bytes: &[u8]) -> Result<(), CoreError> {
    if bytes.is_empty() {
        return Err(CoreError::InvalidUploadBlock(
            "block bytes must not be empty".to_owned(),
        ));
    }
    if bytes.len() > CONTENT_BLOCK_SIZE_BYTES as usize {
        return Err(CoreError::InvalidUploadBlock(format!(
            "block exceeds {} bytes",
            CONTENT_BLOCK_SIZE_BYTES
        )));
    }
    Ok(())
}

fn build_ordered_block_descriptors(
    uploaded_blocks: &[UploadedBlock],
) -> Result<Vec<ContentBlockDescriptor>, CoreError> {
    if uploaded_blocks.is_empty() {
        return Ok(Vec::new());
    }

    let mut previous_index = None;
    let mut descriptors = Vec::with_capacity(uploaded_blocks.len());
    for (position, block) in uploaded_blocks.iter().enumerate() {
        let expected_index = u32::try_from(position)
            .map_err(|_| CoreError::InvalidUploadBlock("block index overflow".to_owned()))?;
        if block.block_index != expected_index {
            return Err(CoreError::InvalidUploadBlock(format!(
                "missing or out-of-order block index: expected {}, got {}",
                expected_index, block.block_index
            )));
        }
        if previous_index == Some(block.block_index) {
            return Err(CoreError::InvalidUploadBlock(format!(
                "duplicate block index {}",
                block.block_index
            )));
        }
        let is_last = position + 1 == uploaded_blocks.len();
        if !is_last && block.plaintext_size_bytes != CONTENT_BLOCK_SIZE_BYTES {
            return Err(CoreError::InvalidUploadBlock(format!(
                "non-final block {} must be exactly {} bytes",
                block.block_index, CONTENT_BLOCK_SIZE_BYTES
            )));
        }
        if block.plaintext_size_bytes == 0 || block.plaintext_size_bytes > CONTENT_BLOCK_SIZE_BYTES
        {
            return Err(CoreError::InvalidUploadBlock(format!(
                "block {} has invalid size {}",
                block.block_index, block.plaintext_size_bytes
            )));
        }
        descriptors.push(ContentBlockDescriptor {
            content_digest_sha256: block.content_digest_sha256.clone(),
            plaintext_size_bytes: block.plaintext_size_bytes,
        });
        previous_index = Some(block.block_index);
    }
    Ok(descriptors)
}

fn verify_uploaded_block_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    blocks: &[ContentBlockDescriptor],
) -> Result<(u64, String), CoreError> {
    let mut total_size = 0u64;
    let mut hasher = Sha256::new();

    for block in blocks {
        let block_key = blob(namespace_id.as_str(), &block.content_digest_sha256);
        let bytes = store
            .get(&block_key, None)
            .map_err(|err| CoreError::Store(err.to_string()))?
            .ok_or_else(|| {
                CoreError::InvalidUploadBlock(format!("missing durable block `{block_key}`"))
            })?;
        let actual_size = bytes.len() as u64;
        if actual_size != block.plaintext_size_bytes {
            return Err(CoreError::InvalidUploadBlock(format!(
                "block `{}` size mismatch: expected {}, actual {}",
                block.content_digest_sha256, block.plaintext_size_bytes, actual_size
            )));
        }
        let actual_digest = sha256_digest(&bytes);
        if actual_digest != block.content_digest_sha256 {
            return Err(CoreError::InvalidUploadBlock(format!(
                "block `{}` digest mismatch",
                block.content_digest_sha256
            )));
        }
        total_size = total_size.saturating_add(actual_size);
        hasher.update(&bytes);
    }

    Ok((total_size, format!("sha256:{:x}", hasher.finalize())))
}

fn read_upload_session_object<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
) -> Result<LoadedUploadSessionObject, CoreError> {
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

fn load_existing_commit_payload<S: ObjectStore + ?Sized>(
    store: &S,
    wal_key: &str,
) -> Result<Option<loon_api::WalCommitPayload>, CoreError> {
    let Some(bytes) = store
        .get(wal_key, None)
        .map_err(|err| CoreError::Store(err.to_string()))?
    else {
        return Ok(None);
    };
    let envelope =
        decode_wal_commit_envelope_zstd(&bytes).map_err(|err| CoreError::Store(err.to_string()))?;
    Ok(Some(envelope.payload))
}

fn find_existing_commit_payload_by_request_id<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request_id: &str,
) -> Result<Option<loon_api::WalCommitPayload>, CoreError> {
    let prefix = format!("namespaces/{}/wal/", namespace_id.as_str());
    let mut matching_keys: Vec<String> = store
        .list_prefix(&prefix)
        .map_err(|err| CoreError::Store(err.to_string()))?
        .into_iter()
        .filter(|key| wal_request_id_from_key(&prefix, key) == Some(request_id))
        .collect();
    matching_keys.sort();

    match matching_keys.as_slice() {
        [] => Ok(None),
        [only] => load_existing_commit_payload(store, only),
        _ => Err(CoreError::Store(format!(
            "multiple WAL commits found for request id `{request_id}`"
        ))),
    }
}

fn commit_response_from_payload(payload: &loon_api::WalCommitPayload) -> V0CommitResponse {
    V0CommitResponse {
        namespace_id: payload.namespace_id.clone(),
        commit_id: payload.commit_id.clone(),
        committed_seq: payload.seq,
        results: payload.results.clone(),
    }
}

fn load_wal_range<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    from_seq_exclusive: ChangeSeq,
    through_seq_inclusive: ChangeSeq,
) -> Result<Vec<crate::wal::StoredWalObject>, CoreError> {
    let prefix = format!("namespaces/{}/wal/", namespace_id.as_str());
    let listed = store
        .list_prefix(&prefix)
        .map_err(|err| BasisLoadError::ListWal {
            prefix: prefix.clone(),
            message: err.to_string(),
        })?;

    let mut wal_by_seq = std::collections::BTreeMap::new();
    for object_key in listed {
        let Some(seq) = wal_seq_from_key(&prefix, &object_key) else {
            return Err(BasisLoadError::InvalidWalObjectKey { object_key }.into());
        };
        if seq <= from_seq_exclusive || seq > through_seq_inclusive {
            continue;
        }
        if let Some(existing) = wal_by_seq.insert(seq, object_key.clone()) {
            return Err(BasisLoadError::DuplicateWalSeq {
                seq,
                first: existing,
                second: object_key,
            }
            .into());
        }
    }

    let mut out = Vec::new();
    let mut expected = from_seq_exclusive.0.saturating_add(1);
    while expected <= through_seq_inclusive.0 {
        let seq = ChangeSeq(expected);
        let object_key =
            wal_by_seq
                .remove(&seq)
                .ok_or_else(|| BasisLoadError::MissingWalObject {
                    prefix: prefix.clone(),
                    seq,
                })?;
        let encoded_bytes = store
            .get(&object_key, None)
            .map_err(|err| BasisLoadError::ReadWal {
                object_key: object_key.clone(),
                message: err.to_string(),
            })?
            .ok_or_else(|| BasisLoadError::MissingWalObjectAfterList {
                object_key: object_key.clone(),
            })?;
        out.push(crate::wal::StoredWalObject {
            object_key,
            encoded_bytes,
        });
        expected = expected.saturating_add(1);
    }

    Ok(out)
}

fn wal_seq_from_key(prefix: &str, object_key: &str) -> Option<ChangeSeq> {
    let suffix = object_key.strip_prefix(prefix)?;
    let (seq_part, _) = suffix.split_once('-')?;
    let seq = seq_part.parse::<u64>().ok()?;
    Some(ChangeSeq(seq))
}

fn wal_request_id_from_key<'a>(prefix: &str, object_key: &'a str) -> Option<&'a str> {
    let suffix = object_key.strip_prefix(prefix)?;
    let suffix = suffix.strip_suffix(".cbor.zst")?;
    let (_, request_id) = suffix.split_once('-')?;
    Some(request_id)
}
