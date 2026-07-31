//! Durable upload sessions: begin, stage, complete, and abort content
//! uploads, including direct-put targets that move bytes past the server.
//!
//! A session is `open`, then `completed` or `aborted`, and both of those are
//! final. The compare-and-swap that lands one of them is the serialization
//! point for the whole upload: provider state is cleaned strictly after the
//! durable transition, never before it, so whichever transition wins is what
//! happened and the loser reports a terminal error instead of undoing
//! anything.

use crate::context::MutationContext;
use crate::control_update::{
    read_upload_session_state, update_upload_session, UploadSessionUpdate,
};
use crate::engine::{
    BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse,
    DirectMultipartUploadTarget, DirectPutUploadTarget, MultipartPartTarget, MultipartPartTargets,
};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::{
    COMPLETED_UPLOAD_RECEIPT_WINDOW_MS, CONTENTION_RETRY_LIMIT, MAX_MULTIPART_PARTS,
    MAX_MULTIPART_PART_BYTES, MAX_SIGNED_PARTS_PER_REQUEST, MIN_MULTIPART_PART_BYTES,
    UPLOAD_SESSION_LEASE_MS,
};
use crate::namespace::catalog::load_namespace_content_store_id;
use crate::namespace::control::load_namespace_head_control;
use crate::storage::content::{
    abort_unpublished_multipart_upload, delete_unpublished_content_object,
    identify_streamed_payload, stage_bytes_under_content_id, stage_streamed_under_content_id,
    verify_durable_content_checksum,
};
use crate::storage::content_admission::{
    CompletedUploadReceipt, ContentAdmission, PreparedContent,
};
use bytes::Bytes;
use loonfs_api::v0::{
    AbortUploadResponse, BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest,
    CompleteUploadResponse, CompletedUploadPart, DirectMultipartContentClaim,
    DirectMultipartUploadOptions, DirectPutContentClaim, UploadContentResponse, UploadMode,
    UploadPartChecksumClaim, UploadSessionStatus, UploadStatusResponse,
};
use loonfs_api::wire::control::{
    encode_control_object, ControlObjectKind, NamespaceState, UploadSessionEnvelope,
    UploadSessionLifecycle, UploadSessionState,
};
use loonfs_api::{
    ChecksumAlgorithm, ContentId, ContentRef, ContentRefKind, ContentStoreId, NamespaceId,
    StorageChecksum, UploadId,
};
use loonfs_objectstore::keys::{content_blob, upload_session};
use loonfs_objectstore::{
    ByteStream, MultipartCompletion, MultipartPart, ObjectStore, PROVIDER_MULTIPART_PART_BYTES,
};

pub(crate) async fn begin_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: BeginUploadRequest,
    context: &MutationContext,
) -> Result<BeginUploadResponse> {
    ensure_upload_namespace_available(store, namespace_id).await?;
    let mode = request.mode.unwrap_or_default();
    if mode != UploadMode::ServiceProxied {
        return Err(CoreError::InvalidUploadContent(format!(
            "{} requires a presigned URL issuer",
            upload_mode_name(mode)
        )));
    }
    let upload_id = create_upload_session(
        store,
        namespace_id,
        NewUploadSession::service_proxied(),
        context,
    )
    .await?;
    Ok(BeginUploadResponse {
        namespace_id: namespace_id.clone(),
        upload_id,
        mode: UploadMode::ServiceProxied,
        direct_put: None,
        direct_multipart: None,
    })
}

fn upload_mode_name(mode: UploadMode) -> &'static str {
    match mode {
        UploadMode::ServiceProxied => "service_proxied",
        UploadMode::DirectPut => "direct_put",
        UploadMode::DirectMultipart => "direct_multipart",
    }
}

/// Mints the content identity a direct upload will write to, and the
/// reference that names it.
///
/// The client declares only what it can know — how many bytes and what they
/// hash to. Identity is the server's, so a caller can never aim a presigned
/// write at an object it chose. The reference returned here is the one the
/// signed write, the completion check, and the later commit all name.
pub(crate) async fn begin_direct_put_upload_target<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    claim: DirectPutContentClaim,
    context: &MutationContext,
) -> Result<BeginDirectPutUploadTargetResponse> {
    ensure_upload_namespace_available(store, namespace_id).await?;
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let content_id = ContentId::generate();
    let content_ref = direct_put_content_ref(content_id.clone(), &claim)?;
    let object_key = content_blob(content_store_id.as_str(), &content_id);
    let upload_id = create_upload_session(
        store,
        namespace_id,
        NewUploadSession::direct_put(content_ref.clone()),
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

/// Mints the content identity a direct multipart upload will assemble into,
/// and opens the provider upload that will assemble it.
///
/// Nothing is claimed here. The session is opened for a payload whose
/// length and digest the client may not know yet — reading from a pipe, or
/// simply unwilling to read a large file twice — so all that is settled is
/// the geometry. What the object turns out to be is claimed at completion,
/// which is where it was always verified.
///
/// The provider upload is created before the session record, so the record
/// is complete from birth and every later step — signing a part, completing,
/// cleaning up — reads one durable object that already knows everything. A
/// session record that fails to land takes the provider upload down with it,
/// so the only thing a failure here can leave behind is nothing.
pub(crate) async fn begin_direct_multipart_upload_target<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    options: DirectMultipartUploadOptions,
    context: &MutationContext,
) -> Result<BeginDirectMultipartUploadTargetResponse> {
    ensure_upload_namespace_available(store, namespace_id).await?;
    let part_size_bytes = multipart_part_size(options.part_size_bytes)?;
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let content_id = ContentId::generate();
    let object_key = content_blob(content_store_id.as_str(), &content_id);

    let provider_upload_id = store
        .create_multipart_upload(&object_key)
        .await
        .map_err(|err| CoreError::store(&object_key, &err))?;
    let session = NewUploadSession::direct_multipart(
        content_id.clone(),
        &provider_upload_id,
        part_size_bytes,
    );
    let upload_id = match create_upload_session(store, namespace_id, session, context).await {
        Ok(upload_id) => upload_id,
        Err(error) => {
            abort_unpublished_multipart_upload(
                store,
                &content_store_id,
                &content_id,
                &provider_upload_id,
            )
            .await;
            return Err(error);
        }
    };

    Ok(BeginDirectMultipartUploadTargetResponse {
        namespace_id: namespace_id.clone(),
        upload_id,
        target: DirectMultipartUploadTarget {
            object_key,
            part_size_bytes,
        },
    })
}

/// Settles the part geometry one multipart session is opened with.
///
/// The bounds are the providers': no non-final part below 5 MiB, none above
/// 5 GiB. The size a client picks is also what bounds its object, since a
/// provider accepts at most [`MAX_MULTIPART_PARTS`] of them.
fn multipart_part_size(requested: Option<u64>) -> Result<u64> {
    let part_size_bytes = requested.unwrap_or(PROVIDER_MULTIPART_PART_BYTES);
    if !(MIN_MULTIPART_PART_BYTES..=MAX_MULTIPART_PART_BYTES).contains(&part_size_bytes) {
        return Err(CoreError::InvalidUploadContent(format!(
            "part_size_bytes must be between {MIN_MULTIPART_PART_BYTES} and \
             {MAX_MULTIPART_PART_BYTES} bytes"
        )));
    }
    Ok(part_size_bytes)
}

/// Resolves the parts a client asked to be authorized against the session
/// that owns them.
///
/// The server signs a part, it does not remember one: nothing durable is
/// written here and nothing is read back later. Part bookkeeping stays with
/// the client all the way to completion, exactly as it does in the
/// provider's own multipart API.
pub(crate) async fn direct_multipart_part_targets<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    requested: &[UploadPartChecksumClaim],
) -> Result<MultipartPartTargets> {
    if requested.is_empty() {
        return Err(CoreError::InvalidUploadContent(
            "a part-signing request names at least one part".to_owned(),
        ));
    }
    if requested.len() > MAX_SIGNED_PARTS_PER_REQUEST {
        return Err(CoreError::InvalidUploadContent(format!(
            "a part-signing request names at most {MAX_SIGNED_PARTS_PER_REQUEST} parts"
        )));
    }
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let session = read_upload_session_state(store, namespace_id, upload_id).await?;
    if let Some(error) = terminal_session_error(&session.state, upload_id.clone()) {
        return Err(error);
    }
    let provider_upload_id = multipart_session_upload(&session)?;

    let mut parts = Vec::with_capacity(requested.len());
    for claim in requested {
        // The only bound is the provider's own part-number range: the
        // session never learned how long the payload would be, so there is
        // no part count to check against.
        if claim.part_number == 0 || claim.part_number > MAX_MULTIPART_PARTS {
            return Err(CoreError::InvalidUploadContent(format!(
                "part {} is outside the provider's 1..={MAX_MULTIPART_PARTS} part range",
                claim.part_number
            )));
        }
        parts.push(MultipartPartTarget {
            part_number: claim.part_number,
            checksum: crc64nvme_claim(&claim.crc64nvme)?,
        });
    }

    Ok(MultipartPartTargets {
        object_key: content_blob(content_store_id.as_str(), &session.content_id),
        provider_upload_id: provider_upload_id.to_owned(),
        parts,
    })
}

/// The provider upload a multipart session is bound to.
fn multipart_session_upload(session: &UploadSessionState) -> Result<&str> {
    if session.mode != UploadMode::DirectMultipart {
        return Err(CoreError::InvalidUploadContent(
            "this upload session is not a direct_multipart upload".to_owned(),
        ));
    }
    session
        .provider_multipart_upload_id
        .as_deref()
        .ok_or_else(|| {
            CoreError::InvalidUploadContent(
                "direct_multipart session is missing its provider upload".to_owned(),
            )
        })
}

/// Turns a client's multipart claim into the reference the assembled object
/// is bound to.
///
/// There is no `whole_file_sha256`: nobody trustworthy hashes these bytes.
/// The client's own digest is not evidence, and the provider assembles the
/// object without ever computing a SHA-256 over it — so the CRC-64/NVME it
/// does compute is the whole of the reference's evidence, and completion
/// reads it back rather than believing the claim.
fn direct_multipart_content_ref(
    content_id: ContentId,
    claim: &DirectMultipartContentClaim,
) -> Result<ContentRef> {
    let content_ref = ContentRef {
        kind: ContentRefKind::BlobV1,
        content_id,
        size_bytes: claim.size_bytes,
        storage_checksum: crc64nvme_claim(&claim.crc64nvme)?,
        whole_file_sha256: None,
    };
    content_ref
        .validate()
        .map_err(|err| CoreError::InvalidUploadContent(err.to_string()))?;
    Ok(content_ref)
}

fn crc64nvme_claim(value: &str) -> Result<StorageChecksum> {
    let checksum = StorageChecksum {
        algorithm: ChecksumAlgorithm::Crc64nvme,
        value: value.to_owned(),
    };
    let width = ChecksumAlgorithm::Crc64nvme.value_bytes() * 2;
    if checksum.value.len() != width
        || !checksum
            .value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CoreError::InvalidUploadContent(format!(
            "crc64nvme must be {width} lowercase hex characters"
        )));
    }
    Ok(checksum)
}

/// Turns a client's part bookkeeping into what the provider assembles from.
fn multipart_parts(parts: &[CompletedUploadPart]) -> Result<Vec<MultipartPart>> {
    let mut previous = 0;
    parts
        .iter()
        .map(|part| {
            if part.part_number <= previous {
                return Err(CoreError::InvalidUploadContent(
                    "completion lists each part once, in ascending part order".to_owned(),
                ));
            }
            previous = part.part_number;
            if part.etag.trim().is_empty() {
                return Err(CoreError::InvalidUploadContent(format!(
                    "part {} carries no etag",
                    part.part_number
                )));
            }
            Ok(MultipartPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
                checksum: crc64nvme_claim(&part.crc64nvme)?,
            })
        })
        .collect()
}

/// Turns a client's claim into the reference the write is bound to.
///
/// The digest is the client's, but it stops being a client claim the moment
/// it is signed into the provider write: the provider refuses any body that
/// does not hash to it, and completion re-checks the stored object against
/// it. That is why the resulting reference may carry `whole_file_sha256`.
fn direct_put_content_ref(
    content_id: ContentId,
    claim: &DirectPutContentClaim,
) -> Result<ContentRef> {
    let storage_checksum = StorageChecksum {
        algorithm: ChecksumAlgorithm::Sha256,
        value: claim.sha256.clone(),
    };
    let content_ref = ContentRef {
        kind: ContentRefKind::BlobV1,
        content_id,
        size_bytes: claim.size_bytes,
        whole_file_sha256: Some(storage_checksum.value.clone()),
        storage_checksum,
    };
    content_ref
        .validate()
        .map_err(|err| CoreError::InvalidUploadContent(err.to_string()))?;
    Ok(content_ref)
}

/// What a session is opened with: everything decided before any byte moves.
struct NewUploadSession {
    mode: UploadMode,
    /// The content object this session will write, allocated up front.
    content_id: ContentId,
    /// What the client promised, for the mode that promises anything.
    claimed_checksum: Option<StorageChecksum>,
    /// The reference a direct write is signed against.
    direct_put_content_ref: Option<ContentRef>,
    /// The provider upload a direct multipart session assembles through.
    provider_multipart_upload_id: Option<String>,
    /// The part geometry a direct multipart session was opened with.
    multipart_part_size_bytes: Option<u64>,
}

impl NewUploadSession {
    fn service_proxied() -> Self {
        Self {
            mode: UploadMode::ServiceProxied,
            content_id: ContentId::generate(),
            claimed_checksum: None,
            direct_put_content_ref: None,
            provider_multipart_upload_id: None,
            multipart_part_size_bytes: None,
        }
    }

    fn direct_put(content_ref: ContentRef) -> Self {
        Self {
            mode: UploadMode::DirectPut,
            content_id: content_ref.content_id.clone(),
            claimed_checksum: Some(content_ref.storage_checksum.clone()),
            direct_put_content_ref: Some(content_ref),
            provider_multipart_upload_id: None,
            multipart_part_size_bytes: None,
        }
    }

    /// A multipart session records identity, the provider handle, and the
    /// geometry — and nothing about the payload, which it has not been told.
    fn direct_multipart(
        content_id: ContentId,
        provider_upload_id: &str,
        part_size_bytes: u64,
    ) -> Self {
        Self {
            mode: UploadMode::DirectMultipart,
            content_id,
            claimed_checksum: None,
            direct_put_content_ref: None,
            provider_multipart_upload_id: Some(provider_upload_id.to_owned()),
            multipart_part_size_bytes: Some(part_size_bytes),
        }
    }
}

async fn create_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    session: NewUploadSession,
    context: &MutationContext,
) -> Result<UploadId> {
    let upload_id = UploadId::generate();
    let state = UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        mode: session.mode,
        content_id: session.content_id,
        claimed_checksum: session.claimed_checksum,
        direct_put_content_ref: session.direct_put_content_ref,
        provider_multipart_upload_id: session.provider_multipart_upload_id,
        multipart_part_size_bytes: session.multipart_part_size_bytes,
        staged_content_ref: None,
        created_at_ms: context.now_ms,
        state: UploadSessionLifecycle::Open {
            expires_at_ms: context.now_ms.saturating_add(UPLOAD_SESSION_LEASE_MS),
        },
    };
    let envelope = UploadSessionEnvelope::from_state(ControlObjectKind::UploadSession, state)
        .map_err(|err| {
            CoreError::Internal(format!("failed to build upload session envelope: {err}"))
        })?;
    let encoded = encode_control_object(&envelope).map_err(|err| {
        CoreError::Internal(format!("failed to encode upload session envelope: {err}"))
    })?;
    let object_key = upload_session(namespace_id.as_str(), upload_id.as_str());
    store
        .put_if_absent(&object_key, Bytes::from(encoded))
        .await
        .map_err(|err| CoreError::store(&object_key, &err))?;
    Ok(upload_id)
}

/// Admits an upload session only for a namespace that exists and still
/// serves writes. The head is the whole existence check: absent means the
/// namespace was never created, and the tombstone refuses.
async fn ensure_upload_namespace_available<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<()> {
    let head = load_namespace_head_control(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: namespace_id.clone(),
        });
    }
    Ok(())
}

/// How a terminal session answers an operation that needed it open.
///
/// A completed session is a conflict the caller can reason about; an aborted
/// one reports the same absence the eventual physical deletion does, because
/// it will never select content again.
fn terminal_session_error(
    state: &UploadSessionLifecycle,
    upload_id: UploadId,
) -> Option<CoreError> {
    match state {
        UploadSessionLifecycle::Open { .. } => None,
        UploadSessionLifecycle::Completed { .. } => {
            Some(CoreError::UploadAlreadyCompleted { upload_id })
        }
        UploadSessionLifecycle::Aborted { .. } => Some(CoreError::UploadNotFound { upload_id }),
    }
}

/// Stages bytes into a service-proxied session.
///
/// The bytes land under the identity the session allocated when it began,
/// so re-sending the same bytes to the same session writes the same object
/// rather than minting a second one. Two *different* sessions carrying
/// identical bytes still get their own objects; sessions are where retry
/// idempotency lives now, not the key space.
pub(crate) async fn upload_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    bytes: &[u8],
) -> Result<UploadContentResponse> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;

    update_upload_session(
        store,
        namespace_id,
        upload_id,
        CONTENTION_RETRY_LIMIT,
        |mut state| {
            let content_store_id = content_store_id.clone();
            let namespace_id = namespace_id.clone();
            let upload_id = upload_id.to_owned();
            async move {
                if let Some(error) = terminal_session_error(&state.state, upload_id.clone()) {
                    return Err(error);
                }
                if state.mode != UploadMode::ServiceProxied {
                    return Err(CoreError::InvalidUploadContent(format!(
                        "{} sessions must be completed after using the presigned URLs",
                        upload_mode_name(state.mode)
                    )));
                }

                let content_ref = ContentRef::blob_v1(state.content_id.clone(), bytes);
                if let Some(existing) = &state.staged_content_ref {
                    if existing == &content_ref {
                        return Ok(UploadSessionUpdate::Noop(UploadContentResponse {
                            namespace_id,
                            upload_id,
                            content_ref,
                        }));
                    }
                    return Err(CoreError::UploadContentConflict { upload_id });
                }

                let stored = stage_bytes_under_content_id(
                    store,
                    content_store_id,
                    state.content_id.clone(),
                    bytes,
                )
                .await?;
                state.staged_content_ref = Some(stored.content_ref.clone());

                Ok(UploadSessionUpdate::Replace {
                    next: Box::new(state),
                    outcome: UploadContentResponse {
                        namespace_id,
                        upload_id,
                        content_ref: stored.content_ref,
                    },
                })
            }
        },
    )
    .await
}

/// Stages a streamed payload into a service-proxied session.
///
/// The bytes are hashed as they are forwarded and never held whole, which
/// is the only difference from [`upload_content`]. That difference forces
/// the shape: the write cannot happen inside a retried compare-and-swap
/// closure, because a stream can only be read once. So the session is read,
/// the payload is written, and only then is the record swapped — and the
/// swap is where an idempotent re-send is told from a conflicting one, by
/// comparing the digest this write produced against the one the session
/// recorded. The store consumes the whole body before it reports a refused
/// precondition, so that digest exists either way.
pub(crate) async fn upload_streamed_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    body: ByteStream,
) -> Result<UploadContentResponse> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let loaded = read_upload_session_state(store, namespace_id, upload_id).await?;
    if let Some(error) = terminal_session_error(&loaded.state, upload_id.clone()) {
        return Err(error);
    }
    if loaded.mode != UploadMode::ServiceProxied {
        return Err(CoreError::InvalidUploadContent(format!(
            "{} sessions must be completed after using the presigned URLs",
            upload_mode_name(loaded.mode)
        )));
    }

    // A session that has already staged content must not have its object
    // rewritten while the answer is being worked out. Reading the body
    // without writing it decides the question: the same bytes are one
    // upload arriving twice, and different bytes are a conflict either way.
    if let Some(staged) = &loaded.staged_content_ref {
        let content_ref = identify_streamed_payload(loaded.content_id.clone(), body).await?;
        if staged != &content_ref {
            return Err(CoreError::UploadContentConflict {
                upload_id: upload_id.clone(),
            });
        }
        return Ok(UploadContentResponse {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            content_ref,
        });
    }

    let staged =
        stage_streamed_under_content_id(store, content_store_id, loaded.content_id.clone(), body)
            .await?;

    update_upload_session(
        store,
        namespace_id,
        upload_id,
        CONTENTION_RETRY_LIMIT,
        |mut state| {
            let namespace_id = namespace_id.clone();
            let upload_id = upload_id.to_owned();
            let content_ref = staged.content_ref.clone();
            let already_present = staged.already_present;
            async move {
                if let Some(error) = terminal_session_error(&state.state, upload_id.clone()) {
                    return Err(error);
                }
                let response = UploadContentResponse {
                    namespace_id,
                    upload_id: upload_id.clone(),
                    content_ref: content_ref.clone(),
                };
                match &state.staged_content_ref {
                    Some(existing) if existing == &content_ref => {
                        Ok(UploadSessionUpdate::Noop(response))
                    }
                    Some(_) => Err(CoreError::UploadContentConflict { upload_id }),
                    // Nothing is recorded yet, so an occupied key holds
                    // bytes this session never acknowledged writing.
                    None if already_present => Err(CoreError::UploadContentConflict { upload_id }),
                    None => {
                        state.staged_content_ref = Some(content_ref);
                        Ok(UploadSessionUpdate::Replace {
                            next: Box::new(state),
                            outcome: response,
                        })
                    }
                }
            }
        },
    )
    .await
}

/// Completes an upload: verify the bytes, then make the completion durable.
///
/// The order is the contract. Verification happens before the
/// compare-and-swap, so nothing is ever recorded as completed on the
/// strength of a provider response alone; and the terminal states are
/// checked before any provider call, so a completion arriving after an abort
/// fails without touching the object the abort is cleaning up.
pub(crate) async fn complete_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    request: &CompleteUploadRequest,
    context: &MutationContext,
) -> Result<CompletedUpload> {
    let now_ms = context.now_ms;
    let loaded = read_upload_session_state(store, namespace_id, upload_id).await?;
    // An aborted session answers the same absence its physical deletion
    // will, before anything about the request's shape is examined.
    if matches!(loaded.state, UploadSessionLifecycle::Aborted { .. }) {
        return Err(CoreError::UploadNotFound {
            upload_id: upload_id.clone(),
        });
    }
    let requested = completion_target_ref(&loaded, request)?;
    if let Some(completed) = completed_outcome(
        &loaded.state,
        namespace_id,
        content_store_id,
        upload_id,
        Some(&requested),
        now_ms,
    )? {
        return Ok(completed);
    }

    let verified =
        match completion_outcome(store, content_store_id, &loaded, request, &requested).await? {
            CompletionOutcome::Verified(content_ref) => content_ref,
            // The bytes that landed are not the bytes that were promised, and
            // the provider upload that could have produced them is consumed.
            // Nothing can rescue this session, so it stops here rather than
            // waiting for its lease to pass: aborting is what deletes the wrong
            // object and releases the provider state.
            CompletionOutcome::Unusable(reason) => {
                if let Err(error) =
                    abort_upload(store, namespace_id, content_store_id, upload_id, context).await
                {
                    tracing::warn!(
                        namespace_id = %namespace_id,
                        upload_id = %upload_id,
                        error = %error,
                        "failed to abandon an upload session whose completion did not verify"
                    );
                }
                return Err(CoreError::InvalidUploadContent(reason));
            }
        };

    update_upload_session(
        store,
        namespace_id,
        upload_id,
        CONTENTION_RETRY_LIMIT,
        |mut state| {
            let namespace_id = namespace_id.clone();
            let content_store_id = content_store_id.clone();
            let upload_id = upload_id.to_owned();
            let verified = verified.clone();
            async move {
                // A racing abort or a peer's completion may have landed
                // between the read above and this swap. Whatever the durable
                // record says now is what happened.
                if let Some(completed) = completed_outcome(
                    &state.state,
                    &namespace_id,
                    &content_store_id,
                    &upload_id,
                    Some(&verified),
                    now_ms,
                )? {
                    return Ok(UploadSessionUpdate::Noop(completed));
                }

                state.staged_content_ref = Some(verified.clone());
                state.state = UploadSessionLifecycle::Completed {
                    completed_at_ms: now_ms,
                    content_ref: verified.clone(),
                };
                let outcome = completed_upload(
                    &namespace_id,
                    &content_store_id,
                    &upload_id,
                    &verified,
                    now_ms,
                    now_ms,
                );
                Ok(UploadSessionUpdate::Replace {
                    next: Box::new(state),
                    outcome,
                })
            }
        },
    )
    .await
}

/// Aborts an upload session, then cleans up what it was writing.
///
/// The durable transition comes first and the provider work strictly after
/// it, so a crash in between leaves an object that the next garbage
/// collection pass reclaims from the aborted record — never an object
/// deleted out from under a session that is still open. Repeating an abort
/// is a success that reports the first abort's stamp.
pub(crate) async fn abort_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    context: &MutationContext,
) -> Result<AbortUploadResponse> {
    let now_ms = context.now_ms;
    let (response, abandoned) = update_upload_session(
        store,
        namespace_id,
        upload_id,
        CONTENTION_RETRY_LIMIT,
        |mut state| {
            let namespace_id = namespace_id.clone();
            let upload_id = upload_id.to_owned();
            async move {
                let aborted = |aborted_at_ms| AbortUploadResponse {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.clone(),
                    aborted_at_ms,
                };
                match state.state {
                    UploadSessionLifecycle::Aborted { aborted_at_ms } => {
                        let abandoned = AbandonedUpload::of(&state);
                        Ok(UploadSessionUpdate::Noop((
                            aborted(aborted_at_ms),
                            abandoned,
                        )))
                    }
                    // Completion is final in the other direction: the
                    // content may already be published, so an abort cannot
                    // quietly succeed over it.
                    UploadSessionLifecycle::Completed { .. } => {
                        Err(CoreError::UploadAlreadyCompleted { upload_id })
                    }
                    UploadSessionLifecycle::Open { .. } => {
                        let abandoned = AbandonedUpload::of(&state);
                        state.state = UploadSessionLifecycle::Aborted {
                            aborted_at_ms: now_ms,
                        };
                        Ok(UploadSessionUpdate::Replace {
                            next: Box::new(state),
                            outcome: (aborted(now_ms), abandoned),
                        })
                    }
                }
            }
        },
    )
    .await?;

    abandoned.release(store, content_store_id).await;
    Ok(response)
}

/// The provider state one terminated session owned.
///
/// It travels out of the compare-and-swap that made the session terminal so
/// cleanup runs strictly after the durable transition — the ordering that
/// makes a crash in between cost a repeat rather than a lost object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbandonedUpload {
    content_id: ContentId,
    provider_multipart_upload_id: Option<String>,
}

impl AbandonedUpload {
    pub(crate) fn of(state: &UploadSessionState) -> Self {
        Self {
            content_id: state.content_id.clone(),
            provider_multipart_upload_id: state.provider_multipart_upload_id.clone(),
        }
    }

    /// Releases everything the session left behind, provider upload first so
    /// the object it might still assemble cannot outlive the deletion.
    pub(crate) async fn release<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        content_store_id: &ContentStoreId,
    ) {
        if let Some(provider_upload_id) = &self.provider_multipart_upload_id {
            abort_unpublished_multipart_upload(
                store,
                content_store_id,
                &self.content_id,
                provider_upload_id,
            )
            .await;
        }
        delete_unpublished_content_object(store, content_store_id, &self.content_id).await;
    }
}

/// Reads one session, minting a fresh receipt when it is completed.
///
/// This read is the reason a lost commit response is cheap: the completed
/// session is durable, so the receipt it hands back is as good as the one
/// the completion returned, and the bytes never move again.
pub(crate) async fn read_upload_status<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    now_ms: u64,
) -> Result<(UploadStatusResponse, Option<CompletedUploadReceipt>)> {
    let loaded = read_upload_session_state(store, namespace_id, upload_id).await?;
    let (status, receipt) = match loaded.state {
        UploadSessionLifecycle::Open { expires_at_ms } => {
            (UploadSessionStatus::Open { expires_at_ms }, None)
        }
        UploadSessionLifecycle::Aborted { aborted_at_ms } => {
            (UploadSessionStatus::Aborted { aborted_at_ms }, None)
        }
        UploadSessionLifecycle::Completed {
            completed_at_ms,
            content_ref,
        } => (
            UploadSessionStatus::Completed {
                completed_at_ms,
                content_ref: content_ref.clone(),
                validated_content_token: None,
            },
            receipt_within_window(
                namespace_id,
                content_store_id,
                &content_ref,
                completed_at_ms,
                now_ms,
            ),
        ),
    };
    Ok((
        UploadStatusResponse {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            status,
        },
        receipt,
    ))
}

/// What a completed upload hands back: the wire response, the in-process
/// admission a same-process publication uses, and the receipt a remote one
/// carries back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedUpload {
    /// Wire response for the completion or its idempotent replay.
    pub response: CompleteUploadResponse,
    /// Admission for a publication in this process, which needs no token.
    pub prepared: PreparedContent,
    /// Receipt for a publication elsewhere, or `None` once the session has
    /// stopped minting them.
    pub receipt: Option<CompletedUploadReceipt>,
}

fn completed_upload(
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    content_ref: &ContentRef,
    completed_at_ms: u64,
    now_ms: u64,
) -> CompletedUpload {
    CompletedUpload {
        response: CompleteUploadResponse {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            content_ref: content_ref.clone(),
            validated_content_token: None,
        },
        prepared: PreparedContent::from_admission(ContentAdmission::for_durable_content_write(
            content_store_id.clone(),
            content_ref.clone(),
        )),
        receipt: receipt_within_window(
            namespace_id,
            content_store_id,
            content_ref,
            completed_at_ms,
            now_ms,
        ),
    }
}

/// Mints a receipt only while the completed session is still inside its
/// receipt window.
///
/// The window is what makes content reclamation decidable: past it no new
/// receipt exists, so no new metadata reference to this content can appear
/// (`limits::CONTENT_RECLAMATION_GRACE_MS`).
fn receipt_within_window(
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
    completed_at_ms: u64,
    now_ms: u64,
) -> Option<CompletedUploadReceipt> {
    (now_ms.saturating_sub(completed_at_ms) < COMPLETED_UPLOAD_RECEIPT_WINDOW_MS).then(|| {
        CompletedUploadReceipt::for_completed_session(
            namespace_id.clone(),
            content_store_id.clone(),
            content_ref.clone(),
        )
    })
}

/// Answers a completion against a session that has already reached a
/// terminal state: a replay of the same content succeeds idempotently,
/// anything else is the terminal error for that state.
fn completed_outcome(
    state: &UploadSessionLifecycle,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    expected: Option<&ContentRef>,
    now_ms: u64,
) -> Result<Option<CompletedUpload>> {
    match state {
        UploadSessionLifecycle::Open { .. } => Ok(None),
        UploadSessionLifecycle::Aborted { .. } => Err(CoreError::UploadNotFound {
            upload_id: upload_id.clone(),
        }),
        UploadSessionLifecycle::Completed {
            completed_at_ms,
            content_ref,
        } => {
            if expected.is_some_and(|expected| expected != content_ref) {
                return Err(CoreError::UploadAlreadyCompleted {
                    upload_id: upload_id.clone(),
                });
            }
            Ok(Some(completed_upload(
                namespace_id,
                content_store_id,
                upload_id,
                content_ref,
                *completed_at_ms,
                now_ms,
            )))
        }
    }
}

/// What a completion attempt established about the session's content.
enum CompletionOutcome {
    /// The object at the session's key is the object it promised.
    Verified(ContentRef),
    /// The session can never produce the content it promised. The caller
    /// makes the session terminal and reports the reason.
    Unusable(String),
}

/// The reference a completion is about, decided from the session and the
/// request alone — no provider call, no durable write.
///
/// Which side names it depends on which side knew it first. A proxied or
/// `direct_put` session was handed a reference before any byte moved, so
/// the request names it back and mismatches are the caller's error. A
/// `direct_multipart` session was never told one, so the request carries
/// the claim instead and the reference is assembled here, over the identity
/// the session has held since it opened. Either way the client cannot
/// choose the identity.
fn completion_target_ref(
    session: &UploadSessionState,
    request: &CompleteUploadRequest,
) -> Result<ContentRef> {
    match session.mode {
        UploadMode::ServiceProxied | UploadMode::DirectPut => {
            if request.multipart.is_some() || request.multipart_parts.is_some() {
                return Err(CoreError::InvalidUploadContent(format!(
                    "{} completion carries no multipart claim",
                    upload_mode_name(session.mode)
                )));
            }
            request.content_ref.clone().ok_or_else(|| {
                CoreError::InvalidUploadContent(format!(
                    "completing a {} upload requires its content ref",
                    upload_mode_name(session.mode)
                ))
            })
        }
        UploadMode::DirectMultipart => {
            if request.content_ref.is_some() {
                return Err(CoreError::InvalidUploadContent(
                    "direct_multipart completion names no content ref: the server owns the \
                     identity and reports it back"
                        .to_owned(),
                ));
            }
            let claim = request.multipart.as_ref().ok_or_else(|| {
                CoreError::InvalidUploadContent(
                    "completing a direct_multipart upload requires the assembled object's \
                     size and crc64nvme"
                        .to_owned(),
                )
            })?;
            direct_multipart_content_ref(session.content_id.clone(), claim)
        }
    }
}

/// Establishes the content reference a completion may freeze.
///
/// A proxied session already wrote and checked its bytes, so the staged
/// reference is the answer. A direct session's bytes bypassed the server,
/// so this is where the server learns what actually landed: it verifies
/// rather than trusts, because provider enforcement is not uniform across
/// the family we support and a random object id says nothing about its
/// contents. One checksum-bearing HEAD settles size and bytes together
/// without a download.
async fn completion_outcome<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    session: &UploadSessionState,
    request: &CompleteUploadRequest,
    requested: &ContentRef,
) -> Result<CompletionOutcome> {
    if let Some(staged) = &session.staged_content_ref {
        if staged != requested {
            return Err(CoreError::InvalidUploadContent(
                "completed content ref does not match staged content".to_owned(),
            ));
        }
        return Ok(CompletionOutcome::Verified(staged.clone()));
    }

    match session.mode {
        UploadMode::ServiceProxied => Err(CoreError::InvalidUploadContent(
            "upload content has not been staged".to_owned(),
        )),
        UploadMode::DirectPut => {
            let expected = expected_direct_put_target(session, requested)?;
            match verify_durable_content_checksum(store, content_store_id, expected).await {
                Ok(()) => Ok(CompletionOutcome::Verified(expected.clone())),
                Err(err) => {
                    // The id is random and still open, so the object nothing
                    // can name is safe to remove and would otherwise leak.
                    delete_unpublished_content_object(
                        store,
                        content_store_id,
                        &requested.content_id,
                    )
                    .await;
                    Err(CoreError::InvalidUploadContent(err.to_string()))
                }
            }
        }
        UploadMode::DirectMultipart => {
            assemble_multipart_upload(store, content_store_id, session, request, requested).await
        }
    }
}

fn expected_direct_put_target<'a>(
    session: &'a UploadSessionState,
    requested: &ContentRef,
) -> Result<&'a ContentRef> {
    let expected = session.direct_put_content_ref.as_ref().ok_or_else(|| {
        CoreError::InvalidUploadContent(
            "direct_put session is missing its target content ref".to_owned(),
        )
    })?;
    if expected != requested {
        return Err(CoreError::InvalidUploadContent(
            "completed content ref does not match the direct_put target".to_owned(),
        ));
    }
    Ok(expected)
}

/// Asks the provider to assemble the parts a client uploaded, then proves
/// what it assembled.
///
/// The read-back is the load-bearing check, not a formality: AWS S3 treats
/// the whole-object checksum as a precondition and refuses a wrong one, but
/// Cloudflare R2 accepts it, assembles the object anyway, and reports the
/// true checksum instead. One provider enforces, one only witnesses, so
/// LoonFS witnesses for itself on both.
///
/// A completion whose response was lost reconciles here too. Replaying the
/// provider's completion is useless — S3 answers success with no checksum,
/// R2 answers `NoSuchUpload` while the object sits there correct — so a
/// consumed upload is not read as failure. The object at the key is the
/// evidence, and it answers the same way on both providers.
async fn assemble_multipart_upload<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    session: &UploadSessionState,
    request: &CompleteUploadRequest,
    expected: &ContentRef,
) -> Result<CompletionOutcome> {
    let provider_upload_id = multipart_session_upload(session)?;
    let parts = request.multipart_parts.as_deref().ok_or_else(|| {
        CoreError::InvalidUploadContent(
            "completing a direct_multipart upload requires the list of parts it uploaded"
                .to_owned(),
        )
    })?;
    let parts = multipart_parts(parts)?;
    let object_key = content_blob(content_store_id.as_str(), &expected.content_id);

    match store
        .complete_multipart_upload(
            &object_key,
            provider_upload_id,
            &parts,
            &expected.storage_checksum,
        )
        .await
    {
        // Either the provider assembled the object on this call, or it had
        // already consumed the upload. Both questions are answered by the
        // same read of the object, so neither needs its own path.
        Ok(MultipartCompletion::Assembled | MultipartCompletion::UnknownUpload) => {}
        Err(err) => {
            // The provider refused to assemble. On AWS S3 that includes a
            // whole-object checksum that does not match the parts, which is
            // a wrong upload and not a transient one, so this is where the
            // session stops.
            return Ok(CompletionOutcome::Unusable(format!(
                "multipart completion failed: {}",
                err.message()
            )));
        }
    }

    match verify_durable_content_checksum(store, content_store_id, expected).await {
        Ok(()) => Ok(CompletionOutcome::Verified(expected.clone())),
        Err(err) => Ok(CompletionOutcome::Unusable(err.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use loonfs_api::v0::BeginUploadRequest;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    const BYTES: &[u8] = b"terminal states\n";

    fn context(now_ms: u64) -> MutationContext {
        MutationContext {
            writer_id: "upload-test".to_owned(),
            now_ms,
        }
    }

    /// One store with a namespace and one open, staged session in it.
    async fn staged_session(
        store: &LocalFsStore,
        context: &MutationContext,
    ) -> (NamespaceId, ContentStoreId, UploadId, ContentRef, String) {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        bootstrap_namespace(store, &namespace_id, context, false)
            .await
            .expect("bootstrap");
        let begin = begin_upload(store, &namespace_id, BeginUploadRequest::default(), context)
            .await
            .expect("begin upload");
        let staged = upload_content(store, &namespace_id, &begin.upload_id, BYTES)
            .await
            .expect("stage upload");
        let content_store_id = load_namespace_content_store_id(store, &namespace_id)
            .await
            .expect("content store id");
        let content_key = content_blob(content_store_id.as_str(), &staged.content_ref.content_id);
        (
            namespace_id,
            content_store_id,
            begin.upload_id,
            staged.content_ref,
            content_key,
        )
    }

    async fn complete(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        content_store_id: &ContentStoreId,
        upload_id: &UploadId,
        content_ref: &ContentRef,
        context: &MutationContext,
    ) -> Result<CompletedUpload> {
        complete_upload(
            store,
            namespace_id,
            content_store_id,
            upload_id,
            &CompleteUploadRequest::for_content_ref(content_ref.clone()),
            context,
        )
        .await
    }

    /// An aborted session is logically absent: it will never select content,
    /// which is the same thing the eventual physical deletion says. A
    /// completion arriving afterwards must not resurrect it — and must not
    /// touch the object the abort already cleaned up.
    #[tokio::test]
    async fn a_completion_after_an_abort_fails_terminally_and_touches_nothing() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, content_store_id, upload_id, content_ref, content_key) =
            staged_session(&store, &setup).await;

        abort_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &context(2_000),
        )
        .await
        .expect("abort");
        assert!(store.head(&content_key).await.expect("head").is_none());

        let error = complete(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &content_ref,
            &context(3_000),
        )
        .await
        .expect_err("an aborted session cannot complete");
        assert!(matches!(error, CoreError::UploadNotFound { .. }));

        let state = read_upload_session_state(&store, &namespace_id, &upload_id)
            .await
            .expect("session still readable");
        assert!(matches!(
            state.state,
            UploadSessionLifecycle::Aborted {
                aborted_at_ms: 2_000
            }
        ));
        assert!(store.head(&content_key).await.expect("head").is_none());
    }

    /// Completion is terminal in the other direction. An abort cannot
    /// quietly succeed over it, because by then the content may already be
    /// published and deleting it would break a live reference.
    #[tokio::test]
    async fn an_abort_after_completion_is_refused_and_keeps_the_content() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, content_store_id, upload_id, content_ref, content_key) =
            staged_session(&store, &setup).await;
        complete(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &content_ref,
            &context(2_000),
        )
        .await
        .expect("complete");

        let error = abort_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &context(3_000),
        )
        .await
        .expect_err("a completed session cannot be aborted");
        assert!(matches!(error, CoreError::UploadAlreadyCompleted { .. }));
        assert!(
            store.head(&content_key).await.expect("head").is_some(),
            "a refused abort must not clean up published-able content"
        );
    }

    /// Aborting twice is a success that reports the abort that stands, so a
    /// client retrying a lost response learns the same thing both times.
    #[tokio::test]
    async fn a_repeated_abort_reports_the_first_stamp() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, content_store_id, upload_id, _content_ref, _content_key) =
            staged_session(&store, &setup).await;

        let first = abort_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &context(2_000),
        )
        .await
        .expect("first abort");
        let second = abort_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &context(9_000),
        )
        .await
        .expect("repeated abort");

        assert_eq!(first.aborted_at_ms, 2_000);
        assert_eq!(second, first);
    }

    /// Bytes may only be staged into the one live state.
    #[tokio::test]
    async fn staging_into_a_terminal_session_is_refused() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, content_store_id, upload_id, content_ref, _content_key) =
            staged_session(&store, &setup).await;
        complete(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &content_ref,
            &context(2_000),
        )
        .await
        .expect("complete");
        let error = upload_content(&store, &namespace_id, &upload_id, BYTES)
            .await
            .expect_err("a completed session takes no more bytes");
        assert!(matches!(error, CoreError::UploadAlreadyCompleted { .. }));

        let aborted = begin_upload(&store, &namespace_id, BeginUploadRequest::default(), &setup)
            .await
            .expect("begin a second upload");
        abort_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &aborted.upload_id,
            &context(3_000),
        )
        .await
        .expect("abort");
        let error = upload_content(&store, &namespace_id, &aborted.upload_id, BYTES)
            .await
            .expect_err("an aborted session takes no more bytes");
        assert!(matches!(error, CoreError::UploadNotFound { .. }));
    }

    /// A receipt exists for exactly one state. An open session has nothing
    /// durable to attest yet, and an aborted one never will.
    #[tokio::test]
    async fn only_a_completed_session_mints_a_receipt() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, content_store_id, upload_id, content_ref, _content_key) =
            staged_session(&store, &setup).await;

        let (open, receipt) =
            read_upload_status(&store, &namespace_id, &content_store_id, &upload_id, 1_500)
                .await
                .expect("status of an open session");
        assert!(matches!(open.status, UploadSessionStatus::Open { .. }));
        assert!(receipt.is_none(), "an open session attests nothing");

        complete(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &content_ref,
            &context(2_000),
        )
        .await
        .expect("complete");
        let (completed, receipt) =
            read_upload_status(&store, &namespace_id, &content_store_id, &upload_id, 2_500)
                .await
                .expect("status of a completed session");
        assert!(matches!(
            completed.status,
            UploadSessionStatus::Completed { .. }
        ));
        assert_eq!(
            receipt.expect("a completed session mints").content_ref(),
            &content_ref
        );

        // A second session, aborted, to check the other terminal state.
        let begin = begin_upload(&store, &namespace_id, BeginUploadRequest::default(), &setup)
            .await
            .expect("begin second upload");
        abort_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &begin.upload_id,
            &context(3_000),
        )
        .await
        .expect("abort");
        let (aborted, receipt) = read_upload_status(
            &store,
            &namespace_id,
            &content_store_id,
            &begin.upload_id,
            3_500,
        )
        .await
        .expect("status of an aborted session");
        assert!(matches!(
            aborted.status,
            UploadSessionStatus::Aborted { .. }
        ));
        assert!(receipt.is_none(), "an aborted session attests nothing");
    }

    /// Re-minting is what makes a lost publish response cheap, and its
    /// window is what makes content reclamation decidable: the session hands
    /// out fresh receipts for as long as its content is protected, then
    /// stops.
    #[tokio::test]
    async fn a_completed_session_re_mints_until_its_receipt_window_closes() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, content_store_id, upload_id, content_ref, _content_key) =
            staged_session(&store, &setup).await;
        let completed_at_ms = 2_000;
        complete(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &content_ref,
            &context(completed_at_ms),
        )
        .await
        .expect("complete");

        // Long after the first receipt would have expired, the durable
        // session still answers with a usable one.
        let much_later = completed_at_ms + COMPLETED_UPLOAD_RECEIPT_WINDOW_MS - 1;
        let (_, receipt) = read_upload_status(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            much_later,
        )
        .await
        .expect("status inside the receipt window");
        assert_eq!(receipt.expect("still minting").content_ref(), &content_ref);

        let past = completed_at_ms + COMPLETED_UPLOAD_RECEIPT_WINDOW_MS;
        let (status, receipt) =
            read_upload_status(&store, &namespace_id, &content_store_id, &upload_id, past)
                .await
                .expect("status past the receipt window");
        assert!(matches!(status, UploadStatusResponse { .. }));
        assert!(
            receipt.is_none(),
            "past the window no receipt exists, which is what lets content GC decide"
        );

        // The same rule governs a very late idempotent completion replay.
        let replay = complete(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &content_ref,
            &context(past),
        )
        .await
        .expect("replay still succeeds");
        assert_eq!(replay.response.content_ref, content_ref);
        assert!(replay.receipt.is_none());
    }
}
