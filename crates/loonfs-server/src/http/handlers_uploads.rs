//! Upload session handlers plus the presign and content-token helpers
//! backing them.

use super::error::ApiResponseError;
use super::{
    authorize, AppJson, AppPath, AppState, NamespaceIdPath, OptionalAppJson, UploadBodyStream,
};
use crate::config::ServerConfig;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use loonfs::content_tokens::{mint_content_token, CompletedUploadReceipt, ContentTokenError};
use loonfs::publish::PreparedContent;
use loonfs::FsWriter;
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::ErrorCode;
use loonfs_api::{
    v0::{
        AbortUploadResponse, BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest,
        CompleteUploadResponse, DirectMultipartUpload, DirectPutUpload, ObjectTransferAccess,
        SignUploadPartsRequest, SignUploadPartsResponse, SignedUploadPart, UploadContentResponse,
        UploadMode, UploadSessionStatus, UploadStatusResponse, ValidatedContentToken,
    },
    ContentRef, NamespaceId, UploadId, FEATURE_UPLOADS_DIRECT_MULTIPART,
    FEATURE_UPLOADS_DIRECT_PUT,
};
use loonfs_objectstore::{
    presign::{ObjectTransferIssuer, PresignedPartRequest, PresignedPutRequest},
    ObjectStoreError,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DIRECT_PUT_URL_TTL: Duration = Duration::from_secs(15 * 60);

/// Lifetime of one part-upload capability. Longer than a whole-object PUT's
/// because a client works through a large file part by part and may not
/// reach a late wave for a while, and short enough that an issued part URL
/// is not a standing write capability.
const MULTIPART_PART_URL_TTL: Duration = Duration::from_secs(60 * 60);

pub(super) enum PutContentPreparation {
    Absent,
    Ready(Vec<PreparedContent>),
    Rejected(ContentTokenError),
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct UploadPathParams {
    upload_id: String,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/uploads",
        tag = "uploads",
        summary = "Begin upload",
        description = "Starts an upload session for content that may later be attached to a file. Service-proxied uploads send bytes through the server; direct-put uploads return object-store presigned credentials.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body = BeginUploadRequest,
        responses(
            (status = 200, description = "Upload session started", body = BeginUploadResponse),
            (status = 400, description = "Invalid upload request", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "Requested upload mode is unsupported", body = ApiError)
        )
    )
)]
pub(super) async fn begin_upload(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    OptionalAppJson(request): OptionalAppJson<BeginUploadRequest>,
) -> Result<Json<BeginUploadResponse>, ApiResponseError> {
    let namespace_id = namespace.into_id()?;
    let request = request.unwrap_or_default();
    match request.mode.unwrap_or_default() {
        UploadMode::DirectPut => {
            return begin_direct_put_upload(state, namespace_id, request).await
        }
        UploadMode::DirectMultipart => {
            return begin_direct_multipart_upload(state, namespace_id, request).await
        }
        UploadMode::ServiceProxied => {}
    }

    let response = state
        .writer
        .begin_upload(&namespace_id, request)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

async fn begin_direct_put_upload(
    state: AppState,
    namespace_id: NamespaceId,
    request: BeginUploadRequest,
) -> Result<Json<BeginUploadResponse>, ApiResponseError> {
    let Some(issuer) = state.transfer_issuer.as_ref() else {
        return Err(ApiResponseError::not_supported(
            FEATURE_UPLOADS_DIRECT_PUT,
            "direct_put requires an object store that can presign create-only, \
             checksum-bound uploads; this deployment's endpoint cannot",
        ));
    };
    let Some(claim) = request.content else {
        return Err(ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            "invalid upload content: direct_put requires a content claim at begin_upload",
        ));
    };

    let prepared = state
        .writer
        .begin_direct_put_upload_target(&namespace_id, claim)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    let content_ref = prepared.target.content_ref;
    let signed = issuer
        .presign_put(
            PresignedPutRequest {
                object_key: &prepared.target.object_key,
                content_ref: &content_ref,
                expires_in: DIRECT_PUT_URL_TTL,
            },
            direct_put_presign_time(),
        )
        .map_err(direct_put_issuer_error)?;

    Ok(Json(BeginUploadResponse {
        namespace_id: prepared.namespace_id,
        upload_id: prepared.upload_id,
        mode: UploadMode::DirectPut,
        direct_put: Some(DirectPutUpload {
            content_ref,
            access: ObjectTransferAccess::PresignedUrl {
                method: signed.method,
                url: signed.url,
                headers: signed.headers,
                expires_at_ms: signed.expires_at_ms,
            },
        }),
        direct_multipart: None,
    }))
}

async fn begin_direct_multipart_upload(
    state: AppState,
    namespace_id: NamespaceId,
    request: BeginUploadRequest,
) -> Result<Json<BeginUploadResponse>, ApiResponseError> {
    if state.transfer_issuer.is_none() {
        return Err(ApiResponseError::not_supported(
            FEATURE_UPLOADS_DIRECT_MULTIPART,
            "direct_multipart requires an object store that can presign checksum-bound \
             part uploads and run the provider's multipart control operations; this \
             deployment's endpoint cannot",
        ));
    }
    // A multipart begin declares nothing about the payload, so an absent
    // `multipart` object simply takes the server's default geometry.
    let options = request.multipart.unwrap_or_default();

    let prepared = state
        .writer
        .begin_direct_multipart_upload_target(&namespace_id, options)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;

    Ok(Json(BeginUploadResponse {
        namespace_id: prepared.namespace_id,
        upload_id: prepared.upload_id,
        mode: UploadMode::DirectMultipart,
        direct_put: None,
        direct_multipart: Some(DirectMultipartUpload {
            part_size_bytes: prepared.target.part_size_bytes,
        }),
    }))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/uploads/{upload_id}/parts",
        tag = "uploads",
        summary = "Sign multipart parts",
        description = "Returns one short-lived, checksum-bound upload capability per requested part of an open direct_multipart session. Asking again for a part is how a client retries it.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("upload_id" = String, Path, description = "Upload session id")
        ),
        request_body = SignUploadPartsRequest,
        responses(
            (status = 200, description = "Part capabilities issued", body = SignUploadPartsResponse),
            (status = 400, description = "Invalid part request", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or upload not found", body = ApiError),
            (status = 409, description = "Upload already completed", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "Direct multipart upload is unsupported", body = ApiError)
        )
    )
)]
pub(super) async fn sign_upload_parts(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    path: AppPath<UploadPathParams>,
    AppJson(request): AppJson<SignUploadPartsRequest>,
) -> Result<Json<SignUploadPartsResponse>, ApiResponseError> {
    let namespace_id = namespace.into_id()?;
    let UploadPathParams { upload_id } = path.into_params()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let Some(issuer) = state.transfer_issuer.as_ref() else {
        return Err(ApiResponseError::not_supported(
            FEATURE_UPLOADS_DIRECT_MULTIPART,
            "this deployment cannot presign multipart part uploads",
        ));
    };

    let targets = state
        .writer
        .direct_multipart_part_targets(&namespace_id, &upload_id, &request.parts)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    let parts = sign_parts(issuer.as_ref(), &targets)?;

    Ok(Json(SignUploadPartsResponse {
        namespace_id,
        upload_id,
        parts,
    }))
}

fn sign_parts(
    issuer: &dyn ObjectTransferIssuer,
    targets: &loonfs::uploads::MultipartPartTargets,
) -> Result<Vec<SignedUploadPart>, ApiResponseError> {
    let signing_time = direct_put_presign_time();
    targets
        .parts
        .iter()
        .map(|part| {
            let signed = issuer
                .presign_multipart_part(
                    PresignedPartRequest {
                        object_key: &targets.object_key,
                        provider_upload_id: &targets.provider_upload_id,
                        part_number: part.part_number,
                        part_checksum: &part.checksum,
                        expires_in: MULTIPART_PART_URL_TTL,
                    },
                    signing_time,
                )
                .map_err(direct_put_issuer_error)?;
            Ok(SignedUploadPart {
                part_number: part.part_number,
                access: ObjectTransferAccess::PresignedUrl {
                    method: signed.method,
                    url: signed.url,
                    headers: signed.headers,
                    expires_at_ms: signed.expires_at_ms,
                },
            })
        })
        .collect()
}

fn direct_put_issuer_error(error: ObjectStoreError) -> ApiResponseError {
    match error {
        ObjectStoreError::InvalidContentRef(message) => {
            ApiResponseError::new(StatusCode::BAD_REQUEST, ErrorCode::InvalidRequest, &message)
        }
        error => ApiResponseError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::ServerError,
            &error.to_string(),
        ),
    }
}

#[allow(clippy::disallowed_methods)]
fn direct_put_presign_time() -> SystemTime {
    // Issuing a short-lived transfer capability enters wall time at this HTTP
    // boundary so core replay stays deterministic.
    SystemTime::now()
}

/// Prepares the request's content proofs against the content refs its put
/// operations name. One prepared proof covers every operation that names its
/// ref; tokens covering a ref no operation puts are ignored.
pub(super) async fn content_preparation_for_puts(
    writer: &FsWriter,
    config: &ServerConfig,
    namespace_id: &NamespaceId,
    content_refs: &[&ContentRef],
    tokens: &[ValidatedContentToken],
    now_ms: u64,
) -> Result<PutContentPreparation, ApiResponseError> {
    let mut prepared_content = Vec::new();
    let mut first_error = None;
    let matching_tokens = tokens
        .iter()
        .filter(|token| content_refs.contains(&&token.content_ref))
        .cloned()
        .collect::<Vec<_>>();
    for token in &matching_tokens {
        match writer
            .prepare_content_token(namespace_id, config.content_token_secret(), token, now_ms)
            .await
            .map_err(|error| ApiResponseError::runtime_for_namespace(namespace_id, error))?
        {
            Ok(prepared) => prepared_content.push(prepared),
            Err(error) => {
                tracing::debug!(
                    namespace_id = %namespace_id,
                    content_id = %token.content_ref.content_id,
                    error = %error,
                    "content token rejected during put preparation"
                );
                first_error.get_or_insert(error);
            }
        }
    }

    if !prepared_content.is_empty() {
        Ok(PutContentPreparation::Ready(prepared_content))
    } else if let Some(error) = first_error {
        Ok(PutContentPreparation::Rejected(error))
    } else {
        Ok(PutContentPreparation::Absent)
    }
}

fn content_token_error(error: ContentTokenError) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCode::ServerError,
        &format!("failed to mint content token: {error}"),
    )
}

#[allow(clippy::disallowed_methods)]
pub(super) fn current_unix_ms() -> Result<u64, ApiResponseError> {
    // Request timestamps enter wall time at this HTTP API boundary so core
    // replay stays deterministic.
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            ApiResponseError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::ServerError,
                &format!("system time is before unix epoch: {error}"),
            )
        })?;
    u64::try_from(duration.as_millis()).map_err(|error| {
        ApiResponseError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::ServerError,
            &format!("system time overflowed milliseconds: {error}"),
        )
    })
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        put,
        path = "/v0/namespaces/{namespace}/uploads/{upload_id}/content",
        tag = "uploads",
        summary = "Upload content",
        description = "Uploads bytes into a service-proxied upload session and returns the content reference for the stored object.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("upload_id" = String, Path, description = "Upload session id")
        ),
        request_body(content = Vec<u8>, content_type = "application/octet-stream"),
        responses(
            (status = 200, description = "Upload content accepted", body = UploadContentResponse),
            (status = 400, description = "Invalid upload content", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or upload not found", body = ApiError),
            (status = 409, description = "Upload content conflict", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 413, description = "Body exceeds the advertised `upload.max_content_bytes` limit", body = ApiError),
            (status = 503, description = "The server is at its concurrent proxied-upload limit; retry shortly", body = ApiError)
        )
    )
)]
/// Forwards a proxied upload's body straight into object storage.
///
/// The body is never held: it is hashed and written a piece at a time, so
/// the server's memory cost tracks the transfer's part size rather than the
/// object's length. The reference this produces is the same one the
/// buffered path produced — `storage_checksum` is the SHA-256 this server
/// computed over the complete payload, and `whole_file_sha256` carries it —
/// because this server is the trusted party that hashed the bytes.
///
/// A failure has two possible authors. The store may have refused the
/// write, or the body may have ended early — past the byte cap, or with a
/// broken connection — and only the second is the client's. The stream
/// records which, so the client is told the truth rather than a blanket
/// storage error.
pub(super) async fn upload_content(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    path: AppPath<UploadPathParams>,
    body: UploadBodyStream,
) -> Result<Json<UploadContentResponse>, ApiResponseError> {
    let namespace_id = namespace.into_id()?;
    let UploadPathParams { upload_id } = path.into_params()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let (stream, outcome) = body.into_stream();
    match state
        .writer
        .upload_streamed_content(&namespace_id, &upload_id, stream)
        .await
    {
        Ok(response) => Ok(Json(response)),
        Err(error) => Err(outcome
            .into_rejection()
            .unwrap_or_else(|| ApiResponseError::runtime_for_namespace(&namespace_id, error))),
    }
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/uploads/{upload_id}/complete",
        tag = "uploads",
        summary = "Complete upload",
        description = "Completes an upload session once the caller confirms the expected content reference. The response may include a short-lived validation token for a following file write.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("upload_id" = String, Path, description = "Upload session id")
        ),
        request_body = CompleteUploadRequest,
        responses(
            (status = 200, description = "Upload completed", body = CompleteUploadResponse),
            (status = 400, description = "Invalid completion request", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or upload not found", body = ApiError),
            (status = 409, description = "Upload completion conflict", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn complete_upload(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    path: AppPath<UploadPathParams>,
    AppJson(request): AppJson<CompleteUploadRequest>,
) -> Result<Json<CompleteUploadResponse>, ApiResponseError> {
    let namespace_id = namespace.into_id()?;
    let UploadPathParams { upload_id } = path.into_params()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let completed = state
        .writer
        .complete_upload_prepared(&namespace_id, &upload_id, &request)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    let mut response = completed.response;
    response.validated_content_token = mint_receipt(&state.config, completed.receipt.as_ref())?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/v0/namespaces/{namespace}/uploads/{upload_id}",
        tag = "uploads",
        summary = "Read upload session",
        description = "Reads one upload session. A completed session answers with a freshly minted validation token for its content, so a client that lost a commit response can commit again without re-uploading anything.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("upload_id" = String, Path, description = "Upload session id")
        ),
        responses(
            (status = 200, description = "Upload session state", body = UploadStatusResponse),
            (status = 400, description = "Invalid upload id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or upload not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn read_upload_status(
    State(state): State<AppState>,
    headers: HeaderMap,
    namespace: NamespaceIdPath,
    path: AppPath<UploadPathParams>,
) -> Result<Json<UploadStatusResponse>, ApiResponseError> {
    // A completed session answers with a freshly minted content-validation
    // token, which is a capability to commit that content. Authorization
    // runs before the id is even parsed, as everywhere else.
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let UploadPathParams { upload_id } = path.into_params()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let (mut response, receipt) = state
        .writer
        .read_upload_status(&namespace_id, &upload_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    if let UploadSessionStatus::Completed {
        validated_content_token,
        ..
    } = &mut response.status
    {
        *validated_content_token = mint_receipt(&state.config, receipt.as_ref())?;
    }
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        path = "/v0/namespaces/{namespace}/uploads/{upload_id}/abort",
        tag = "uploads",
        summary = "Abort upload",
        description = "Ends an upload session without selecting content and deletes the object it was writing. Repeating it succeeds; a session that already completed cannot be aborted.",
        params(
            ("namespace" = String, Path, description = "Namespace id"),
            ("upload_id" = String, Path, description = "Upload session id")
        ),
        responses(
            (status = 200, description = "Upload aborted", body = AbortUploadResponse),
            (status = 400, description = "Invalid upload id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or upload not found", body = ApiError),
            (status = 409, description = "Upload already completed", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError)
        )
    )
)]
pub(super) async fn abort_upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    namespace: NamespaceIdPath,
    path: AppPath<UploadPathParams>,
) -> Result<Json<AbortUploadResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let UploadPathParams { upload_id } = path.into_params()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let response = state
        .writer
        .abort_upload(&namespace_id, &upload_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

/// Signs a receipt the core layer already proved is mintable.
///
/// Core decides *whether* a receipt exists — only a durable completed
/// session inside its receipt window yields one — and the server owns the
/// secret that signs it, so neither side can mint on its own.
fn mint_receipt(
    config: &ServerConfig,
    receipt: Option<&CompletedUploadReceipt>,
) -> Result<Option<String>, ApiResponseError> {
    let Some(receipt) = receipt else {
        return Ok(None);
    };
    let token = mint_content_token(config.content_token_secret(), receipt, current_unix_ms()?)
        .map_err(content_token_error)?;
    Ok(Some(token))
}

fn parse_upload_id(value: &str) -> Result<UploadId, ApiResponseError> {
    UploadId::parse(value).map_err(|error| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &error.to_string(),
        )
    })
}
