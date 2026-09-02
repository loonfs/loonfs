//! Durable upload sessions for starting, staging, completing, and aborting
//! content uploads, including direct-to-provider transfers.
//!
//! A session starts open and ends as either completed or aborted. The
//! compare-and-swap that records a terminal state determines the result.
//! Provider cleanup runs only after that durable transition.
//!
//! Every content object is created through a session. Before metadata
//! references the object, the session record gives garbage collection a
//! durable owner and status.

use crate::context::MutationContext;
use crate::control_update::{
    create_control_object_under_generated_id, load_upload_session_state, update_upload_session,
    UploadSessionUpdate,
};
use crate::error::{CoreError, Result};
use crate::limits::{
    COMPLETED_UPLOAD_ADMISSION_WINDOW_MS, COMPLETED_UPLOAD_RECEIPT_WINDOW_MS, MAX_MULTIPART_PARTS,
    MAX_MULTIPART_PART_BYTES, MAX_SIGNED_PARTS_PER_REQUEST, MIN_MULTIPART_PART_BYTES,
    UPLOAD_SESSION_LEASE_MS,
};
use crate::namespace::catalog::{load_namespace_content_store_id, VerifiedNamespaceCatalogEntry};
use crate::namespace::control::load_namespace_head_control;
use crate::storage::content::{
    abort_unpublished_multipart_upload, delete_unpublished_content_object,
    identify_streamed_payload, stage_bytes_under_content_id, stage_streamed_under_content_id,
    verify_durable_content_checksum, DurableContentValidationError, StreamedPayloadKind,
};
use crate::storage::content_admission::{CompletedUploadReceipt, PreparedContent};
use bytes::Bytes;
use loonfs_api::options::DirectMultipartUploadOptions;
use loonfs_api::v0::{
    BeginUploadResponse, CompleteMultipartUploadRequest, CompletedUploadPart, UploadContentClaim,
    UploadContentResponse, UploadMode, UploadPartChecksumClaim, UploadSession, UploadSessionStatus,
};
use loonfs_api::wire::control::{
    encode_control_state, ControlObjectKind, ProxiedStaging, UploadSessionMode,
    UploadSessionRecordStatus, UploadSessionState,
};
use loonfs_api::{
    Checksum, ChecksumAlgorithm, ContentId, ContentRef, ContentRefKind, ContentStoreId,
    NamespaceId, UploadId,
};
use loonfs_objectstore::keys::{content_blob, upload_session};
use loonfs_objectstore::{
    ByteStream, MultipartCompletion, MultipartPart, ObjectStore, PROVIDER_MULTIPART_PART_BYTES,
};
use std::num::NonZeroU64;

/// Internal response for preparing a direct_put session before URL signing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginDirectPutUploadTargetResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: UploadId,
    pub object_key: String,
}

/// Internal multipart target used by the server before signing part URLs.
///
/// The session has an object identity but no content reference yet because
/// the payload size and checksum are supplied at completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectMultipartUploadTarget {
    pub object_key: String,
    pub part_size_bytes: u64,
    pub checksum_algorithm: ChecksumAlgorithm,
}

const DIRECT_MULTIPART_CHECKSUM_ALGORITHM: ChecksumAlgorithm = ChecksumAlgorithm::Crc64nvme;

/// Internal response for preparing a direct_multipart session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginDirectMultipartUploadTargetResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: UploadId,
    pub target: DirectMultipartUploadTarget,
}

/// Completion data after the request has been decoded for the stored upload
/// mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedUploadCompletion {
    /// The session already contains all required content information.
    KnownContent,
    /// Content claim supplied when a direct PUT completes.
    DirectPut { content: UploadContentClaim },
    /// Multipart content information supplied at completion.
    Multipart(CompleteMultipartUploadRequest),
}

impl ResolvedUploadCompletion {
    /// Returns the upload mode this completion describes.
    pub fn mode(&self) -> UploadMode {
        match self {
            Self::KnownContent => UploadMode::ServiceProxied,
            Self::DirectPut { .. } => UploadMode::DirectPut,
            Self::Multipart(_) => UploadMode::DirectMultipart,
        }
    }
}

/// One part a server integration is about to sign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartTarget {
    pub part_number: u32,
    pub checksum: Checksum,
}

/// Everything a server integration needs to sign one wave of part uploads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartTargets {
    pub object_key: String,
    pub provider_upload_id: String,
    pub parts: Vec<MultipartPartTarget>,
}

pub(crate) async fn begin_service_proxied_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<BeginUploadResponse> {
    ensure_upload_namespace_available(store, namespace_id).await?;
    let upload_id = create_upload_session(
        store,
        namespace_id,
        NewUploadSession::service_proxied(),
        context,
    )
    .await?;
    Ok(BeginUploadResponse::ServiceProxied {
        namespace_id: namespace_id.clone(),
        upload_id,
    })
}

/// Starts a direct PUT session and assigns its content identity.
pub(crate) async fn begin_direct_put_upload_target<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checksum_algorithm: ChecksumAlgorithm,
    context: &MutationContext,
) -> Result<BeginDirectPutUploadTargetResponse> {
    ensure_upload_namespace_available(store, namespace_id).await?;
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let content_id = ContentId::generate();
    let object_key = content_blob(&content_store_id, &content_id);
    let upload_id = create_upload_session(
        store,
        namespace_id,
        NewUploadSession::direct_put(content_id, checksum_algorithm),
        context,
    )
    .await?;
    Ok(BeginDirectPutUploadTargetResponse {
        namespace_id: namespace_id.clone(),
        upload_id,
        object_key,
    })
}

/// Creates a multipart upload session and the corresponding provider upload.
///
/// Size and checksum are supplied at completion because they may be unknown
/// when streaming begins. The provider upload is created first and then
/// stored in the session record. If the session record cannot be written,
/// the provider upload is aborted.
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
    let object_key = content_blob(&content_store_id, &content_id);

    let provider_upload_id = store
        .create_multipart_upload(&object_key)
        .await
        .map_err(|err| CoreError::store(&object_key, &err))?;
    let session = NewUploadSession::direct_multipart(
        content_id.clone(),
        &provider_upload_id,
        part_size_bytes,
        DIRECT_MULTIPART_CHECKSUM_ALGORITHM,
    );
    let upload_id = match create_upload_session(store, namespace_id, session, context).await {
        Ok(upload_id) => upload_id,
        Err(error) => {
            let _ = abort_unpublished_multipart_upload(
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
            part_size_bytes: part_size_bytes.get(),
            checksum_algorithm: DIRECT_MULTIPART_CHECKSUM_ALGORITHM,
        },
    })
}

/// Validates the multipart part size against provider limits.
///
/// The selected part size also bounds the maximum object size because the
/// provider accepts at most [`MAX_MULTIPART_PARTS`] parts.
fn multipart_part_size(requested: Option<u64>) -> Result<NonZeroU64> {
    let part_size_bytes = requested.unwrap_or(PROVIDER_MULTIPART_PART_BYTES);
    NonZeroU64::new(part_size_bytes)
        .filter(|size| (MIN_MULTIPART_PART_BYTES..=MAX_MULTIPART_PART_BYTES).contains(&size.get()))
        .ok_or_else(|| {
            CoreError::InvalidUploadContent(format!(
                "part_size_bytes must be between {MIN_MULTIPART_PART_BYTES} and \
                 {MAX_MULTIPART_PART_BYTES} bytes"
            ))
        })
}

/// Validates requested multipart parts and returns the data needed to sign
/// them.
///
/// Part state remains client-managed. This method writes no durable state.
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
    let session = load_upload_session_state(store, namespace_id, upload_id).await?;
    if let Some(error) = terminal_session_error(&session.status, upload_id.clone()) {
        return Err(error);
    }
    let (provider_upload_id, checksum_algorithm) = multipart_session_upload(&session)?;

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
            checksum: validate_upload_checksum(&claim.checksum, checksum_algorithm)?.clone(),
        });
    }

    Ok(MultipartPartTargets {
        object_key: content_blob(&content_store_id, &session.content_id),
        provider_upload_id: provider_upload_id.to_owned(),
        parts,
    })
}

/// Returns the provider upload ID for a direct multipart session.
///
/// Other upload modes return an invalid-upload error.
fn multipart_session_upload(session: &UploadSessionState) -> Result<(&str, ChecksumAlgorithm)> {
    match &session.mode {
        UploadSessionMode::DirectMultipart {
            provider_upload_id,
            checksum_algorithm,
            ..
        } => Ok((provider_upload_id, *checksum_algorithm)),
        UploadSessionMode::ServiceProxied { .. } | UploadSessionMode::DirectPut { .. } => {
            Err(CoreError::InvalidUploadContent(
                "this upload session is not a direct_multipart upload".to_owned(),
            ))
        }
    }
}

/// Builds a content reference from a client's upload claim.
///
fn claimed_content_ref(
    content_id: ContentId,
    claim: &UploadContentClaim,
    required_algorithm: ChecksumAlgorithm,
) -> Result<ContentRef> {
    let content_ref = ContentRef {
        kind: ContentRefKind::BlobV1,
        content_id,
        size_bytes: claim.size_bytes,
        checksum: validate_upload_checksum(&claim.checksum, required_algorithm)?.clone(),
    };
    content_ref
        .validate()
        .map_err(|err| CoreError::InvalidUploadContent(err.to_string()))?;
    Ok(content_ref)
}

fn validate_upload_checksum(
    checksum: &Checksum,
    required_algorithm: ChecksumAlgorithm,
) -> Result<&Checksum> {
    if checksum.algorithm != required_algorithm {
        return Err(CoreError::InvalidUploadContent(format!(
            "checksum algorithm `{}` does not match the session requirement `{required_algorithm}`",
            checksum.algorithm
        )));
    }
    checksum
        .validate()
        .map_err(|error| CoreError::InvalidUploadContent(error.to_string()))?;
    Ok(checksum)
}

/// Turns a client's part bookkeeping into what the provider assembles from.
fn multipart_parts(
    parts: &[CompletedUploadPart],
    checksum_algorithm: ChecksumAlgorithm,
) -> Result<Vec<MultipartPart>> {
    if parts.is_empty() {
        return Err(CoreError::InvalidUploadContent(
            "completion must include at least one uploaded part".to_owned(),
        ));
    }
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
                checksum: validate_upload_checksum(&part.checksum, checksum_algorithm)?.clone(),
            })
        })
        .collect()
}

/// Values fixed when an upload session is created.
struct NewUploadSession {
    /// The content object this session will write, allocated up front.
    content_id: ContentId,
    /// How the bytes will reach it.
    mode: UploadSessionMode,
}

impl NewUploadSession {
    fn service_proxied() -> Self {
        Self {
            content_id: ContentId::generate(),
            mode: UploadSessionMode::ServiceProxied {
                staging: ProxiedStaging::Idle,
            },
        }
    }

    fn direct_put(content_id: ContentId, checksum_algorithm: ChecksumAlgorithm) -> Self {
        Self {
            content_id,
            mode: UploadSessionMode::DirectPut { checksum_algorithm },
        }
    }

    /// A multipart session records identity, the provider handle, and the
    /// geometry — and nothing about the payload, which it has not been told.
    fn direct_multipart(
        content_id: ContentId,
        provider_upload_id: &str,
        part_size_bytes: NonZeroU64,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> Self {
        Self {
            content_id,
            mode: UploadSessionMode::DirectMultipart {
                provider_upload_id: provider_upload_id.to_owned(),
                part_size_bytes,
                checksum_algorithm,
            },
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
        content_id: session.content_id,
        created_at_ms: context.now_ms,
        mode: session.mode,
        status: UploadSessionRecordStatus::Open {
            expires_at_ms: context.now_ms.saturating_add(UPLOAD_SESSION_LEASE_MS),
        },
    };
    let object_key = upload_session(namespace_id, &upload_id);
    let encoded =
        encode_control_state(ControlObjectKind::UploadSession, &state).map_err(|error| {
            CoreError::Codec {
                object_key: object_key.clone(),
                message: error.to_string(),
            }
        })?;
    create_control_object_under_generated_id(store, &object_key, Bytes::from(encoded)).await?;
    Ok(upload_id)
}

/// Verifies that the namespace exists and accepts writes.
///
/// A missing head means the namespace was not created. A deleted head
/// rejects the upload.
async fn ensure_upload_namespace_available<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<()> {
    let head = load_namespace_head_control(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?
        .state;
    crate::namespace::control::ensure_namespace_live(&head)?;
    Ok(())
}

/// Converts a terminal upload status into the error returned by an operation
/// that requires an open session.
fn terminal_session_error(
    status: &UploadSessionRecordStatus,
    upload_id: UploadId,
) -> Option<CoreError> {
    match status {
        UploadSessionRecordStatus::Open { .. } => None,
        UploadSessionRecordStatus::Completed { .. } => {
            Some(CoreError::UploadAlreadyCompleted { upload_id })
        }
        UploadSessionRecordStatus::Aborted { .. } => Some(CoreError::UploadNotFound { upload_id }),
    }
}

/// What a staging request found when it asked for the right to write.
enum StagingSlot {
    /// The request holds the claim and is the only one that may write.
    Claimed,
    /// The session already staged content, so nothing is written. The
    /// caller decides from this reference whether its bytes are the same
    /// upload arriving twice or a conflicting one.
    AlreadyStaged(ContentRef),
}

/// Takes the exclusive right to write this session's content object.
///
/// A proxied session writes one object key, and past the store's multipart
/// threshold the create-only condition on that write cannot be part of the
/// write (see [`stage_streamed_under_content_id`]). Two requests that both
/// found the key absent would therefore both assemble over it, and the one
/// that lost the record compare-and-swap would leave its bytes behind under
/// the winner's digest. This compare-and-swap is what stops that: exactly
/// one request holds the claim, so only one ever writes.
///
/// A session that already staged content needs no claim, because nothing
/// will be written — the caller answers from the reference instead.
async fn claim_staging_slot<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Result<StagingSlot> {
    update_upload_session(store, namespace_id, upload_id, |mut state| {
        let upload_id = upload_id.to_owned();
        async move {
            if let Some(error) = terminal_session_error(&state.status, upload_id.clone()) {
                return Err(error);
            }
            let UploadSessionMode::ServiceProxied { staging } = &mut state.mode else {
                return Err(CoreError::Internal(
                    "staging slot requested from a direct upload session".to_owned(),
                ));
            };
            match staging {
                ProxiedStaging::Idle => {
                    *staging = ProxiedStaging::Claimed;
                    Ok(UploadSessionUpdate::Replace {
                        next: Box::new(state),
                        outcome: StagingSlot::Claimed,
                    })
                }
                ProxiedStaging::Claimed => Err(CoreError::UploadContentConflict { upload_id }),
                ProxiedStaging::Staged(content_ref) => Ok(UploadSessionUpdate::Noop(
                    StagingSlot::AlreadyStaged(content_ref.clone()),
                )),
            }
        }
    })
    .await
}

/// Gives the staging claim back after a write that will never be recorded.
///
/// Best effort on purpose. The claim is bounded by the session's own lease,
/// so a release lost to a crash costs the session the rest of that lease and
/// nothing more; failing the caller's request over it would replace a
/// recoverable error with a worse one.
async fn release_staging_claim<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) {
    let released = update_upload_session(store, namespace_id, upload_id, |mut state| async move {
        if !matches!(state.status, UploadSessionRecordStatus::Open { .. }) {
            return Ok(UploadSessionUpdate::Noop(()));
        }
        let UploadSessionMode::ServiceProxied { staging } = &mut state.mode else {
            return Ok(UploadSessionUpdate::Noop(()));
        };
        if !matches!(staging, ProxiedStaging::Claimed) {
            return Ok(UploadSessionUpdate::Noop(()));
        }
        *staging = ProxiedStaging::Idle;
        Ok(UploadSessionUpdate::Replace {
            next: Box::new(state),
            outcome: (),
        })
    })
    .await;
    if let Err(error) = released {
        tracing::warn!(
            namespace_id = %namespace_id,
            upload_id = %upload_id,
            error = %error,
            "failed to release the staging claim of a failed upload; \
             the session cannot stage again until its lease passes"
        );
    }
}

async fn read_open_proxied_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
) -> Result<(ContentStoreId, UploadSessionState)> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let session = load_upload_session_state(store, namespace_id, upload_id).await?;
    if let Some(error) = terminal_session_error(&session.status, upload_id.clone()) {
        return Err(error);
    }
    if !matches!(session.mode, UploadSessionMode::ServiceProxied { .. }) {
        return Err(CoreError::InvalidUploadContent(format!(
            "{} sessions must be completed after using the presigned URLs",
            upload_mode(&session.mode).as_str()
        )));
    }
    Ok((content_store_id, session))
}

pub(crate) enum ProxiedPayload<'a> {
    Bytes(&'a [u8]),
    Stream(ByteStream),
}

/// Stores bytes in a service-proxied upload session.
///
/// Retries to the same session use the same object identity. Matching bytes
/// are idempotent; different bytes return a content conflict. Separate
/// sessions always use separate object identities.
pub(crate) async fn upload_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    bytes: &[u8],
) -> Result<UploadContentResponse> {
    upload_proxied_content(store, namespace_id, upload_id, ProxiedPayload::Bytes(bytes)).await
}

pub(crate) async fn upload_streamed_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    body: ByteStream,
) -> Result<UploadContentResponse> {
    upload_proxied_content(store, namespace_id, upload_id, ProxiedPayload::Stream(body)).await
}

pub(crate) async fn upload_proxied_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    payload: ProxiedPayload<'_>,
) -> Result<UploadContentResponse> {
    let (content_store_id, loaded) =
        read_open_proxied_session(store, namespace_id, upload_id).await?;

    // The claim is what makes the write exclusive, so it is taken before any
    // byte is written and released by the same swap that records the result.
    match claim_staging_slot(store, namespace_id, upload_id).await? {
        StagingSlot::AlreadyStaged(staged) => {
            let content_ref = match payload {
                ProxiedPayload::Bytes(bytes) => {
                    ContentRef::blob_v1(loaded.content_id.clone(), bytes)
                }
                ProxiedPayload::Stream(body) => {
                    identify_streamed_payload(loaded.content_id.clone(), body).await?
                }
            };
            if staged != content_ref {
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
        StagingSlot::Claimed => {}
    }

    let staged = match payload {
        ProxiedPayload::Bytes(bytes) => {
            stage_bytes_under_content_id(store, content_store_id, loaded.content_id.clone(), bytes)
                .await
                .map(|stored| (stored.into_content_ref(), false))
        }
        ProxiedPayload::Stream(body) => stage_streamed_under_content_id(
            store,
            content_store_id,
            loaded.content_id.clone(),
            body,
            StreamedPayloadKind::Request,
        )
        .await
        .map(|staged| (staged.content_ref, staged.already_present)),
    };
    let (content_ref, already_present) = match staged {
        Ok(staged) => staged,
        Err(error) => {
            release_staging_claim(store, namespace_id, upload_id).await;
            return Err(error);
        }
    };

    record_staged_content(store, namespace_id, upload_id, content_ref, already_present).await
}

/// Records a staging result and releases its claim in the same
/// compare-and-swap.
///
/// `already_present` means the create-only object write found an existing
/// object. The session's claim prevents concurrent writers, so that object
/// can only come from an earlier attempt that did not record its result.
/// Matching content is an idempotent retry; different content is a conflict.
async fn record_staged_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    content_ref: ContentRef,
    already_present: bool,
) -> Result<UploadContentResponse> {
    update_upload_session(store, namespace_id, upload_id, |mut state| {
        let namespace_id = namespace_id.clone();
        let upload_id = upload_id.to_owned();
        let content_ref = content_ref.clone();
        async move {
            if let Some(error) = terminal_session_error(&state.status, upload_id.clone()) {
                return Err(error);
            }
            let UploadSessionMode::ServiceProxied { staging } = &mut state.mode else {
                return Err(CoreError::Internal(
                    "staged content recorded for a direct upload session".to_owned(),
                ));
            };
            let response = UploadContentResponse {
                namespace_id,
                upload_id: upload_id.clone(),
                content_ref: content_ref.clone(),
            };
            if already_present && !matches!(staging, ProxiedStaging::Staged(_)) {
                return Err(CoreError::UploadContentConflict { upload_id });
            }
            match staging {
                ProxiedStaging::Staged(existing) => {
                    if existing == &content_ref {
                        Ok(UploadSessionUpdate::Noop(response))
                    } else {
                        Err(CoreError::UploadContentConflict { upload_id })
                    }
                }
                ProxiedStaging::Idle | ProxiedStaging::Claimed => {
                    *staging = ProxiedStaging::Staged(content_ref);
                    Ok(UploadSessionUpdate::Replace {
                        next: Box::new(state),
                        outcome: response,
                    })
                }
            }
        }
    })
    .await
}

/// Completes an upload by verifying the content and then recording the
/// terminal state.
///
/// Verification happens before the compare-and-swap. Terminal session state
/// is checked before provider access, so a completion cannot race an abort
/// and read an object being cleaned up.
pub(crate) async fn complete_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    completion: ResolvedUploadCompletion,
    context: &MutationContext,
) -> Result<CompletedUpload> {
    complete_upload_for_mode(
        store,
        namespace_id,
        content_store_id,
        upload_id,
        |_| Ok(completion),
        context,
    )
    .await
}

/// Loads the session, decodes completion data for its mode, and completes the
/// upload. Decoding happens before provider access or durable writes.
pub(crate) async fn complete_upload_for_mode<S, F>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    resolve: F,
    context: &MutationContext,
) -> Result<CompletedUpload>
where
    S: ObjectStore + ?Sized,
    F: FnOnce(UploadMode) -> std::result::Result<ResolvedUploadCompletion, String>,
{
    let now_ms = context.now_ms;
    let loaded = load_upload_session_state(store, namespace_id, upload_id).await?;
    // An aborted session answers the same absence its physical deletion
    // will, before anything about the request's shape is examined.
    if matches!(loaded.status, UploadSessionRecordStatus::Aborted { .. }) {
        return Err(CoreError::UploadNotFound {
            upload_id: upload_id.clone(),
        });
    }
    let mode = upload_mode(&loaded.mode);
    let completion = resolve(mode).map_err(CoreError::InvalidUploadContent)?;
    let plan = completion_plan(&loaded, &completion)?;
    // The aborted status was rejected above; the compare-and-swap caller also uses this helper.
    if let Some(completed) = replay_terminal_completion(
        &loaded.status,
        namespace_id,
        content_store_id,
        upload_id,
        plan.expected_completed_content(),
        mode,
        now_ms,
    )? {
        return Ok(completed);
    }

    let verified = match completion_outcome(store, content_store_id, plan).await? {
        CompletionOutcome::Verified(content_ref) => content_ref,
        // The provider upload was already consumed, so this request cannot
        // establish what consumed it. Reject only this claim: a retry with
        // the claim that describes the assembled object may still recover
        // a completion whose response was lost.
        CompletionOutcome::Rejected(reason) => {
            return Err(CoreError::InvalidUploadContent(reason));
        }
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

    freeze_completed_session(
        store,
        namespace_id,
        content_store_id,
        upload_id,
        &verified,
        now_ms,
    )
    .await
}

/// Stores a verified content reference as the session's completed state.
///
/// All upload modes use this transition. Completion makes the content
/// eligible for publication and later garbage collection.
async fn freeze_completed_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    verified: &ContentRef,
    now_ms: u64,
) -> Result<CompletedUpload> {
    update_upload_session(store, namespace_id, upload_id, |mut state| {
        let namespace_id = namespace_id.clone();
        let content_store_id = content_store_id.clone();
        let upload_id = upload_id.to_owned();
        let verified = verified.clone();
        async move {
            if let Some(completed) = replay_terminal_completion(
                &state.status,
                &namespace_id,
                &content_store_id,
                &upload_id,
                Some(&verified),
                upload_mode(&state.mode),
                now_ms,
            )? {
                return Ok(UploadSessionUpdate::Noop(completed));
            }

            state.status = UploadSessionRecordStatus::Completed {
                completed_at_ms: now_ms,
                content_ref: verified.clone(),
            };
            let outcome = completed_upload(
                &namespace_id,
                &content_store_id,
                &upload_id,
                &verified,
                upload_mode(&state.mode),
                now_ms,
                now_ms,
            );
            Ok(UploadSessionUpdate::Replace {
                next: Box::new(state),
                outcome,
            })
        }
    })
    .await
}

/// The session one in-process staging write fills, from the identity it
/// allocated to the record that will hold its outcome.
struct OwnedStagingSession {
    upload_id: UploadId,
    content_id: ContentId,
}

/// Stores in-process bytes through an upload session and returns prepared
/// content.
///
/// The session is written before the content object so garbage collection
/// always has a durable owner for the object. The operation performs one
/// content write and two sequential control-object writes.
pub(crate) async fn stage_owned_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    bytes: &[u8],
    context: &MutationContext,
) -> Result<PreparedContent> {
    let session = open_owned_staging_session(store, catalog, context).await?;
    let stored = stage_bytes_under_content_id(
        store,
        catalog.content_store_id().clone(),
        session.content_id,
        bytes,
    )
    .await?;
    complete_owned_staging(
        store,
        catalog,
        &session.upload_id,
        stored.into_content_ref(),
        context,
    )
    .await
}

/// Stages a payload this runtime forwards under a session that owns it.
///
/// The streaming twin of [`stage_owned_bytes`]: the bytes are hashed on
/// their way to the store rather than held, and everything about ownership
/// is identical.
pub(crate) async fn stage_owned_stream<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    body: ByteStream,
    payload_kind: StreamedPayloadKind,
    context: &MutationContext,
) -> Result<PreparedContent> {
    let session = open_owned_staging_session(store, catalog, context).await?;
    let content_store_id = catalog.content_store_id().clone();
    let staged = stage_streamed_under_content_id(
        store,
        content_store_id,
        session.content_id,
        body,
        payload_kind,
    )
    .await?;
    if staged.already_present {
        // The identity is 128 fresh random bits and this session has made no
        // earlier attempt, so an occupied key is corruption rather than a
        // replay, and it fails loudly.
        return Err(CoreError::Internal(format!(
            "content object `{}` already holds bytes under a freshly minted identity",
            content_blob(catalog.content_store_id(), &staged.content_ref.content_id)
        )));
    }
    complete_owned_staging(
        store,
        catalog,
        &session.upload_id,
        staged.content_ref,
        context,
    )
    .await
}

/// Creates the internal upload session that owns an in-process content
/// write.
///
/// Its ID is not exposed. Garbage collection is its only later reader.
async fn open_owned_staging_session<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    context: &MutationContext,
) -> Result<OwnedStagingSession> {
    // Do not recheck namespace availability here. The catalog came from the
    // namespace head, and publication performs the final admission check. If
    // the namespace is deleted first, garbage collection removes the
    // unreferenced completed session and content.
    let session = NewUploadSession::service_proxied();
    let content_id = session.content_id.clone();
    let upload_id = create_upload_session(store, catalog.namespace_id(), session, context).await?;
    Ok(OwnedStagingSession {
        upload_id,
        content_id,
    })
}

/// Completes an in-process staging session using the content reference
/// produced by the write.
///
/// No additional verification is needed because this process computed the
/// checksum while writing. On failure, the open session eventually expires
/// and garbage collection removes its object.
async fn complete_owned_staging<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    upload_id: &UploadId,
    content_ref: ContentRef,
    context: &MutationContext,
) -> Result<PreparedContent> {
    Ok(freeze_completed_session(
        store,
        catalog.namespace_id(),
        catalog.content_store_id(),
        upload_id,
        &content_ref,
        context.now_ms,
    )
    .await?
    .prepared)
}

/// Marks an upload session aborted, then cleans up its provider state and
/// unpublished content.
///
/// Cleanup starts only after the durable transition. Repeating an abort is
/// idempotent and returns the original abort timestamp.
pub(crate) async fn abort_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    context: &MutationContext,
) -> Result<UploadSession> {
    let now_ms = context.now_ms;
    let (response, abandoned) =
        update_upload_session(store, namespace_id, upload_id, |mut state| {
            let namespace_id = namespace_id.clone();
            let upload_id = upload_id.to_owned();
            async move {
                let mode = upload_mode(&state.mode);
                let abandoned = AbandonedUpload::of(&state);
                let aborted = |aborted_at_ms| UploadSession {
                    namespace_id: namespace_id.clone(),
                    upload_id: upload_id.clone(),
                    mode,
                    status: UploadSessionStatus::Aborted { aborted_at_ms },
                };
                match state.status {
                    UploadSessionRecordStatus::Aborted { aborted_at_ms } => Ok(
                        UploadSessionUpdate::Noop((aborted(aborted_at_ms), abandoned)),
                    ),
                    // Completion is final in the other direction: the
                    // content may already be published, so an abort cannot
                    // quietly succeed over it.
                    UploadSessionRecordStatus::Completed { .. } => {
                        Err(CoreError::UploadAlreadyCompleted { upload_id })
                    }
                    UploadSessionRecordStatus::Open { .. } => {
                        state.status = UploadSessionRecordStatus::Aborted {
                            aborted_at_ms: now_ms,
                        };
                        Ok(UploadSessionUpdate::Replace {
                            next: Box::new(state),
                            outcome: (aborted(now_ms), abandoned),
                        })
                    }
                }
            }
        })
        .await?;

    if !abandoned.release(store, content_store_id).await {
        tracing::debug!(
            namespace_id = %namespace_id,
            upload_id = %upload_id,
            "upload cleanup remains for garbage collection"
        );
    }
    Ok(response)
}

/// Provider resources owned by a terminated upload session.
///
/// The compare-and-swap returns this value so cleanup runs only after the
/// terminal state is durable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbandonedUpload {
    content_id: ContentId,
    provider_multipart_upload_id: Option<String>,
}

impl AbandonedUpload {
    pub(crate) fn of(state: &UploadSessionState) -> Self {
        let provider_multipart_upload_id = match &state.mode {
            UploadSessionMode::DirectMultipart {
                provider_upload_id, ..
            } => Some(provider_upload_id.clone()),
            UploadSessionMode::ServiceProxied { .. } | UploadSessionMode::DirectPut { .. } => None,
        };
        Self {
            content_id: state.content_id.clone(),
            provider_multipart_upload_id,
        }
    }

    /// Releases everything the session left behind, provider upload first so
    /// the object it might still assemble cannot outlive the deletion.
    ///
    /// `true` confirms both cleanup steps. A failed provider abort stops
    /// before object deletion so an in-flight assembly cannot resurrect the
    /// object after it was deleted.
    #[must_use]
    pub(crate) async fn release<S: ObjectStore + ?Sized>(
        &self,
        store: &S,
        content_store_id: &ContentStoreId,
    ) -> bool {
        if let Some(provider_upload_id) = &self.provider_multipart_upload_id {
            if !abort_unpublished_multipart_upload(
                store,
                content_store_id,
                &self.content_id,
                provider_upload_id,
            )
            .await
            {
                return false;
            }
        }
        delete_unpublished_content_object(store, content_store_id, &self.content_id).await
    }
}

/// Returns an upload session and a new receipt when the upload is complete.
///
/// A caller that lost the original completion response can recover the
/// receipt without uploading the content again.
pub(crate) async fn get_upload_status<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    now_ms: u64,
) -> Result<(UploadSession, Option<CompletedUploadReceipt>)> {
    let loaded = load_upload_session_state(store, namespace_id, upload_id).await?;
    let mode = upload_mode(&loaded.mode);
    let (status, receipt) = match loaded.status {
        UploadSessionRecordStatus::Open { expires_at_ms, .. } => {
            (UploadSessionStatus::Open { expires_at_ms }, None)
        }
        UploadSessionRecordStatus::Aborted { aborted_at_ms } => {
            (UploadSessionStatus::Aborted { aborted_at_ms }, None)
        }
        UploadSessionRecordStatus::Completed {
            completed_at_ms,
            content_ref,
        } => (
            completed_status(&content_ref, completed_at_ms),
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
        UploadSession {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            mode,
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
    pub response: UploadSession,
    /// Admission for a publication in this process, which needs no token but
    /// expires at the completed session's final admission horizon.
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
    mode: UploadMode,
    completed_at_ms: u64,
    now_ms: u64,
) -> CompletedUpload {
    CompletedUpload {
        response: UploadSession {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            mode,
            status: completed_status(content_ref, completed_at_ms),
        },
        prepared: PreparedContent::for_completed_upload(
            namespace_id.clone(),
            content_store_id.clone(),
            content_ref.clone(),
            completed_at_ms.saturating_add(COMPLETED_UPLOAD_ADMISSION_WINDOW_MS),
        ),
        receipt: receipt_within_window(
            namespace_id,
            content_store_id,
            content_ref,
            completed_at_ms,
            now_ms,
        ),
    }
}

fn completed_status(content_ref: &ContentRef, completed_at_ms: u64) -> UploadSessionStatus {
    UploadSessionStatus::Completed {
        completed_at_ms,
        content_ref: content_ref.clone(),
        content_token: None,
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
/// terminal status: a replay of the same content succeeds idempotently,
/// anything else is the terminal error for that status.
fn replay_terminal_completion(
    status: &UploadSessionRecordStatus,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    expected: Option<&ContentRef>,
    mode: UploadMode,
    now_ms: u64,
) -> Result<Option<CompletedUpload>> {
    match status {
        UploadSessionRecordStatus::Open { .. } => Ok(None),
        UploadSessionRecordStatus::Aborted { .. } => Err(CoreError::UploadNotFound {
            upload_id: upload_id.clone(),
        }),
        UploadSessionRecordStatus::Completed {
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
                mode,
                *completed_at_ms,
                now_ms,
            )))
        }
    }
}

/// What a completion attempt established about the session's content.
enum CompletionOutcome {
    /// The stored object matches the completion claim.
    Verified(ContentRef),
    /// This claim does not match the evidence left by an upload that was
    /// already consumed. Another claim may still recover the session.
    Rejected(String),
    /// The upload is invalid and must be aborted.
    Unusable(String),
}

/// Information needed to verify an upload before completion.
enum CompletionPlan<'a> {
    /// Content previously staged through the server.
    Proxied { staged: Option<&'a ContentRef> },
    /// Directly uploaded content that must be checked at the provider.
    DirectPut { requested: ContentRef },
    /// Parts the provider must assemble and verify.
    DirectMultipart {
        requested: ContentRef,
        provider_upload_id: &'a str,
        checksum_algorithm: ChecksumAlgorithm,
        parts: &'a [CompletedUploadPart],
    },
}

impl CompletionPlan<'_> {
    /// Content a repeated multipart completion must match.
    fn expected_completed_content(&self) -> Option<&ContentRef> {
        match self {
            Self::Proxied { .. } => None,
            Self::DirectPut { requested, .. } | Self::DirectMultipart { requested, .. } => {
                Some(requested)
            }
        }
    }
}

fn upload_mode(mode: &UploadSessionMode) -> UploadMode {
    match mode {
        UploadSessionMode::ServiceProxied { .. } => UploadMode::ServiceProxied,
        UploadSessionMode::DirectPut { .. } => UploadMode::DirectPut,
        UploadSessionMode::DirectMultipart { .. } => UploadMode::DirectMultipart,
    }
}

/// Validates a completion request against the session mode.
fn completion_plan<'a>(
    session: &'a UploadSessionState,
    completion: &'a ResolvedUploadCompletion,
) -> Result<CompletionPlan<'a>> {
    let session_mode = upload_mode(&session.mode);
    let completion_mode = completion.mode();
    if completion_mode != session_mode {
        return Err(CoreError::InvalidUploadContent(format!(
            "completion mode `{}` does not match stored upload mode `{}`",
            completion_mode.as_str(),
            session_mode.as_str()
        )));
    }

    match (&session.mode, completion) {
        (UploadSessionMode::ServiceProxied { staging }, ResolvedUploadCompletion::KnownContent) => {
            Ok(CompletionPlan::Proxied {
                staged: match staging {
                    ProxiedStaging::Staged(content_ref) => Some(content_ref),
                    ProxiedStaging::Idle | ProxiedStaging::Claimed => None,
                },
            })
        }
        (
            UploadSessionMode::DirectPut { checksum_algorithm },
            ResolvedUploadCompletion::DirectPut { content },
        ) => Ok(CompletionPlan::DirectPut {
            requested: claimed_content_ref(
                session.content_id.clone(),
                content,
                *checksum_algorithm,
            )?,
        }),
        (
            UploadSessionMode::DirectMultipart {
                provider_upload_id,
                checksum_algorithm,
                ..
            },
            ResolvedUploadCompletion::Multipart(CompleteMultipartUploadRequest { content, parts }),
        ) => Ok(CompletionPlan::DirectMultipart {
            requested: claimed_content_ref(
                session.content_id.clone(),
                content,
                *checksum_algorithm,
            )?,
            provider_upload_id,
            checksum_algorithm: *checksum_algorithm,
            parts,
        }),
        _ => Err(CoreError::Internal(
            "upload completion mode should match the session mode".to_owned(),
        )),
    }
}

/// Verifies the uploaded object and returns the content reference that may
/// be recorded as completed.
///
/// Proxied uploads use the reference computed while staging. Direct uploads
/// verify the provider-stored object because the bytes bypassed this server.
/// A checksum-bearing metadata request verifies size and checksum without
/// downloading the object.
async fn completion_outcome<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    plan: CompletionPlan<'_>,
) -> Result<CompletionOutcome> {
    match plan {
        CompletionPlan::Proxied { staged } => {
            let staged = staged.ok_or_else(|| {
                CoreError::InvalidUploadContent("upload content has not been staged".to_owned())
            })?;
            Ok(CompletionOutcome::Verified(staged.clone()))
        }
        CompletionPlan::DirectPut { requested } => {
            match verify_durable_content_checksum(store, content_store_id, &requested).await {
                Ok(()) => Ok(CompletionOutcome::Verified(requested)),
                Err(err) => Ok(CompletionOutcome::Unusable(content_failure_reason(err)?)),
            }
        }
        CompletionPlan::DirectMultipart {
            requested,
            provider_upload_id,
            checksum_algorithm,
            parts,
        } => {
            assemble_multipart_upload(
                store,
                content_store_id,
                provider_upload_id,
                checksum_algorithm,
                parts,
                &requested,
            )
            .await
        }
    }
}

/// Asks the provider to assemble the uploaded parts, then verifies the
/// resulting object.
///
/// Provider behavior differs: some reject an incorrect whole-object checksum,
/// while others assemble the object and report the actual checksum. LoonFS
/// therefore verifies the stored object after every completion.
///
/// The same verification also recovers from a lost completion response. An
/// unknown or already-consumed provider upload is accepted only when the
/// object at the target key passes verification.
async fn assemble_multipart_upload<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    provider_upload_id: &str,
    checksum_algorithm: ChecksumAlgorithm,
    parts: &[CompletedUploadPart],
    expected: &ContentRef,
) -> Result<CompletionOutcome> {
    let parts = multipart_parts(parts, checksum_algorithm)?;
    let object_key = content_blob(content_store_id, &expected.content_id);

    let completion = match store
        .complete_multipart_upload(&object_key, provider_upload_id, &parts, &expected.checksum)
        .await
    {
        Ok(completion @ (MultipartCompletion::Assembled | MultipartCompletion::UnknownUpload)) => {
            completion
        }
        Err(err) => {
            // The object was not assembled on this call. The provider may
            // have refused the parts, or the call may not have completed at
            // all: a refusal arrives as the same transport failure as a lost
            // response, so this cannot tell them apart and does not guess.
            // Reporting the store failure leaves the session open and the
            // object, if the provider did assemble one, in place — which is
            // what a repeated completion reconciles from.
            return Err(CoreError::store(&object_key, &err));
        }
    };

    match verify_durable_content_checksum(store, content_store_id, expected).await {
        Ok(()) => Ok(CompletionOutcome::Verified(expected.clone())),
        Err(err) => {
            let reason = content_failure_reason(err)?;
            Ok(match completion {
                // This call consumed the provider upload, so a confirmed
                // mismatch makes the session unusable.
                MultipartCompletion::Assembled => CompletionOutcome::Unusable(reason),
                // An earlier completion or abort consumed the upload. A
                // mismatch rejects this request but is not evidence that a
                // different completion claim cannot describe the object.
                MultipartCompletion::UnknownUpload => CompletionOutcome::Rejected(reason),
            })
        }
    }
}

/// Classifies a content-verification failure.
///
/// A confirmed absence, length mismatch, or checksum mismatch makes the
/// upload unusable. A storage access failure is returned unchanged so the
/// session remains open and completion can be retried.
fn content_failure_reason(error: DurableContentValidationError) -> Result<String> {
    match error {
        DurableContentValidationError::Store { .. } => Err(CoreError::DurableContent(error)),
        error => Ok(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::PutMode;
    use tempfile::tempdir;

    const BYTES: &[u8] = b"terminal states\n";

    fn context(now_ms: u64) -> MutationContext {
        MutationContext {
            writer_id: loonfs_api::WriterId::parse("upload-test").expect("writer id"),
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
        let begin = begin_service_proxied_upload(store, &namespace_id, context)
            .await
            .expect("begin upload");
        let staged = upload_content(store, &namespace_id, begin.upload_id(), BYTES)
            .await
            .expect("stage upload");
        let content_store_id = load_namespace_content_store_id(store, &namespace_id)
            .await
            .expect("content store id");
        let content_key = content_blob(&content_store_id, &staged.content_ref.content_id);
        (
            namespace_id,
            content_store_id,
            begin.upload_id().clone(),
            staged.content_ref,
            content_key,
        )
    }

    async fn complete(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        content_store_id: &ContentStoreId,
        upload_id: &UploadId,
        context: &MutationContext,
    ) -> Result<CompletedUpload> {
        complete_upload(
            store,
            namespace_id,
            content_store_id,
            upload_id,
            ResolvedUploadCompletion::KnownContent,
            context,
        )
        .await
    }

    #[test]
    fn multipart_validation_uses_the_algorithm_frozen_in_the_session() {
        let session = UploadSessionState {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            upload_id: UploadId::parse("upl_00000000000000000000000000000001").expect("upload id"),
            content_id: ContentId::parse("con_00000000000000000000000000000001")
                .expect("content id"),
            created_at_ms: 1,
            mode: UploadSessionMode::DirectMultipart {
                provider_upload_id: "provider-upload".to_owned(),
                part_size_bytes: NonZeroU64::new(8 * 1024 * 1024).expect("part size"),
                // Deliberately not the process default. This models a
                // session reopened after that default changed.
                checksum_algorithm: ChecksumAlgorithm::Crc32c,
            },
            status: UploadSessionRecordStatus::Open { expires_at_ms: 2 },
        };
        let (_, required_algorithm) =
            multipart_session_upload(&session).expect("multipart session");
        assert_eq!(required_algorithm, ChecksumAlgorithm::Crc32c);

        let content = UploadContentClaim {
            size_bytes: 7,
            checksum: Checksum::crc32c(b"payload"),
        };
        let content_ref =
            claimed_content_ref(session.content_id.clone(), &content, required_algorithm)
                .expect("the stored algorithm accepts the content claim");
        assert_eq!(content_ref.checksum.algorithm, ChecksumAlgorithm::Crc32c);

        let part = CompletedUploadPart {
            part_number: 1,
            etag: "etag".to_owned(),
            checksum: Checksum::crc32c(b"payload"),
        };
        assert_eq!(
            multipart_parts(&[part], required_algorithm)
                .expect("the stored algorithm accepts the part")[0]
                .checksum
                .algorithm,
            ChecksumAlgorithm::Crc32c
        );

        let wrong_part = CompletedUploadPart {
            part_number: 1,
            etag: "etag".to_owned(),
            checksum: Checksum::sha256(b"payload"),
        };
        assert!(multipart_parts(&[wrong_part], required_algorithm).is_err());
        let wrong_signing_claim = UploadPartChecksumClaim {
            part_number: 1,
            checksum: Checksum::sha256(b"payload"),
        };
        assert!(
            validate_upload_checksum(&wrong_signing_claim.checksum, required_algorithm).is_err()
        );
        let wrong_content = UploadContentClaim {
            size_bytes: 7,
            checksum: Checksum::sha256(b"payload"),
        };
        assert!(
            claimed_content_ref(session.content_id, &wrong_content, required_algorithm).is_err()
        );
    }

    #[tokio::test]
    async fn a_direct_put_requires_the_session_algorithm_and_replays_completion() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        let begin = begin_direct_put_upload_target(
            &store,
            &namespace_id,
            ChecksumAlgorithm::Sha256,
            &setup,
        )
        .await
        .expect("begin direct put");
        let content_store_id = load_namespace_content_store_id(&store, &namespace_id)
            .await
            .expect("content store id");

        let wrong_algorithm = complete_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &begin.upload_id,
            ResolvedUploadCompletion::DirectPut {
                content: UploadContentClaim {
                    size_bytes: BYTES.len() as u64,
                    checksum: Checksum::crc32c(BYTES),
                },
            },
            &context(2_000),
        )
        .await
        .expect_err("a different checksum algorithm is invalid content");
        assert!(matches!(
            wrong_algorithm,
            CoreError::InvalidUploadContent(_)
        ));
        let open = load_upload_session_state(&store, &namespace_id, &begin.upload_id)
            .await
            .expect("session remains readable");
        assert!(matches!(
            open.status,
            UploadSessionRecordStatus::Open { .. }
        ));

        store
            .put(
                &begin.object_key,
                Bytes::from_static(BYTES),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("write direct-put bytes");
        let completion = ResolvedUploadCompletion::DirectPut {
            content: UploadContentClaim {
                size_bytes: BYTES.len() as u64,
                checksum: Checksum::sha256(BYTES),
            },
        };
        let first = complete_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &begin.upload_id,
            completion.clone(),
            &context(3_000),
        )
        .await
        .expect("complete direct put");
        let replay = complete_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &begin.upload_id,
            completion,
            &context(3_000),
        )
        .await
        .expect("replay direct-put completion");
        assert_eq!(replay, first);
    }

    #[tokio::test]
    async fn provider_abort_failure_stops_before_object_delete() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        bootstrap_namespace(&store, &namespace_id, &context(1_000), false)
            .await
            .expect("bootstrap");
        let content_store_id = load_namespace_content_store_id(&store, &namespace_id)
            .await
            .expect("content store id");
        let content_id = ContentId::generate();
        let content_key = content_blob(&content_store_id, &content_id);
        store
            .put(
                &content_key,
                Bytes::from_static(BYTES),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("write unpublished content");
        let abandoned = AbandonedUpload {
            content_id,
            provider_multipart_upload_id: Some("unsupported-provider-upload".to_owned()),
        };

        assert!(
            !abandoned.release(&store, &content_store_id).await,
            "an unsupported provider abort is incomplete cleanup"
        );
        assert!(
            store
                .head(&content_key)
                .await
                .expect("head content")
                .is_some(),
            "object deletion waits until provider abort succeeds"
        );
    }

    #[tokio::test]
    async fn a_completion_after_an_abort_fails_terminally_and_touches_nothing() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, content_store_id, upload_id, _content_ref, content_key) =
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
            &context(3_000),
        )
        .await
        .expect_err("an aborted session cannot complete");
        assert!(matches!(error, CoreError::UploadNotFound { .. }));

        let state = load_upload_session_state(&store, &namespace_id, &upload_id)
            .await
            .expect("session still readable");
        assert!(matches!(
            state.status,
            UploadSessionRecordStatus::Aborted {
                aborted_at_ms: 2_000
            }
        ));
        assert!(store.head(&content_key).await.expect("head").is_none());
    }

    #[tokio::test]
    async fn a_failed_direct_put_completion_aborts_and_retries_as_terminal() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        let begin = begin_direct_put_upload_target(
            &store,
            &namespace_id,
            ChecksumAlgorithm::Sha256,
            &setup,
        )
        .await
        .expect("begin direct put");
        store
            .put(
                &begin.object_key,
                Bytes::from(vec![b'x'; BYTES.len()]),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("write mismatched direct-put bytes");
        let content_store_id = load_namespace_content_store_id(&store, &namespace_id)
            .await
            .expect("content store id");

        let error = complete_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &begin.upload_id,
            ResolvedUploadCompletion::DirectPut {
                content: UploadContentClaim {
                    size_bytes: BYTES.len() as u64,
                    checksum: Checksum::sha256(BYTES),
                },
            },
            &context(2_000),
        )
        .await
        .expect_err("mismatched bytes cannot complete");
        assert!(matches!(error, CoreError::InvalidUploadContent(_)));
        let state = load_upload_session_state(&store, &namespace_id, &begin.upload_id)
            .await
            .expect("session remains readable");
        assert!(matches!(
            state.status,
            UploadSessionRecordStatus::Aborted {
                aborted_at_ms: 2_000
            }
        ));
        assert!(store
            .head(&begin.object_key)
            .await
            .expect("head mismatched content")
            .is_none());

        let retry = complete_upload(
            &store,
            &namespace_id,
            &content_store_id,
            &begin.upload_id,
            ResolvedUploadCompletion::DirectPut {
                content: UploadContentClaim {
                    size_bytes: BYTES.len() as u64,
                    checksum: Checksum::sha256(BYTES),
                },
            },
            &context(3_000),
        )
        .await
        .expect_err("an aborted direct put stays terminal");
        assert!(matches!(retry, CoreError::UploadNotFound { .. }));
    }

    #[tokio::test]
    async fn an_abort_after_completion_is_refused_and_keeps_the_content() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, content_store_id, upload_id, _content_ref, content_key) =
            staged_session(&store, &setup).await;
        complete(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
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

        assert_eq!(first.mode, UploadMode::ServiceProxied);
        assert!(matches!(
            &first.status,
            UploadSessionStatus::Aborted {
                aborted_at_ms: 2_000
            }
        ));
        assert_eq!(second, first);
    }

    #[tokio::test]
    async fn staging_into_a_terminal_session_is_refused() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, content_store_id, upload_id, _content_ref, _content_key) =
            staged_session(&store, &setup).await;
        complete(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &context(2_000),
        )
        .await
        .expect("complete");
        let error = upload_content(&store, &namespace_id, &upload_id, BYTES)
            .await
            .expect_err("a completed session takes no more bytes");
        assert!(matches!(error, CoreError::UploadAlreadyCompleted { .. }));

        let aborted = begin_service_proxied_upload(&store, &namespace_id, &setup)
            .await
            .expect("begin a second upload");
        abort_upload(
            &store,
            &namespace_id,
            &content_store_id,
            aborted.upload_id(),
            &context(3_000),
        )
        .await
        .expect("abort");
        let error = upload_content(&store, &namespace_id, aborted.upload_id(), BYTES)
            .await
            .expect_err("an aborted session takes no more bytes");
        assert!(matches!(error, CoreError::UploadNotFound { .. }));
    }

    #[tokio::test]
    async fn only_a_completed_session_mints_a_receipt() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, content_store_id, upload_id, content_ref, _content_key) =
            staged_session(&store, &setup).await;

        let (open, receipt) =
            get_upload_status(&store, &namespace_id, &content_store_id, &upload_id, 1_500)
                .await
                .expect("status of an open session");
        assert!(matches!(open.status, UploadSessionStatus::Open { .. }));
        assert!(receipt.is_none(), "an open session attests nothing");

        complete(
            &store,
            &namespace_id,
            &content_store_id,
            &upload_id,
            &context(2_000),
        )
        .await
        .expect("complete");
        let (completed, receipt) =
            get_upload_status(&store, &namespace_id, &content_store_id, &upload_id, 2_500)
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
        let begin = begin_service_proxied_upload(&store, &namespace_id, &setup)
            .await
            .expect("begin second upload");
        abort_upload(
            &store,
            &namespace_id,
            &content_store_id,
            begin.upload_id(),
            &context(3_000),
        )
        .await
        .expect("abort");
        let (aborted, receipt) = get_upload_status(
            &store,
            &namespace_id,
            &content_store_id,
            begin.upload_id(),
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

    #[tokio::test]
    async fn a_completed_session_keeps_its_content_ref_after_token_minting_closes() {
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
            &context(completed_at_ms),
        )
        .await
        .expect("complete");

        // Long after the first receipt would have expired, the durable
        // session still answers with a usable one.
        let much_later = completed_at_ms + COMPLETED_UPLOAD_RECEIPT_WINDOW_MS - 1;
        let (_, receipt) = get_upload_status(
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
            get_upload_status(&store, &namespace_id, &content_store_id, &upload_id, past)
                .await
                .expect("status past the receipt window");
        let completed = match status.status {
            UploadSessionStatus::Completed {
                content_ref,
                content_token,
                ..
            } => Some((content_ref, content_token)),
            _ => None,
        };
        assert_eq!(completed, Some((content_ref.clone(), None)));
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
            &context(past),
        )
        .await
        .expect("replay still succeeds");
        assert_eq!(replay.response.content_ref(), Some(&content_ref));
        assert!(replay.response.content_token().is_none());
        assert!(replay.receipt.is_none());
    }
}
