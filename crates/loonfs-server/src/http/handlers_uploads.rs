//! Upload session handlers plus the presign and content-token helpers
//! backing them.

use super::error::{status_for_core_error_code, ApiResponseError};
use super::{
    authorize, AppPath, AppQuery, AppState, NamespaceIdPath, NoQuery, UploadBodyBytes,
    UploadBodyStream, UploadControlJson, MAX_COMPLETION_BODY_BYTES, MAX_UPLOAD_CONTROL_BODY_BYTES,
};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use loonfs::content_tokens::{
    mint_content_token, CompletedUploadReceipt, ContentToken, ContentTokenError,
};
use loonfs::publish::PreparedContent;
use loonfs::uploads::ResolvedUploadCompletion;
use loonfs::FsWriter;
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::ErrorCode;
use loonfs_api::{
    options::DirectMultipartUploadOptions,
    v0::{
        BeginUploadRequest, BeginUploadResponse, CompleteMultipartUploadRequest,
        CompleteUploadRequest, ObjectTransferAccess, SignUploadPartsRequest,
        SignUploadPartsResponse, SignedUploadPart, UploadContentResponse, UploadMode,
        UploadSessionResponse, UploadSessionStatus,
    },
    ContentId, ContentRef, NamespaceId, UploadId, FEATURE_UPLOADS_DIRECT_MULTIPART,
    FEATURE_UPLOADS_DIRECT_PUT, LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES,
};
use loonfs_objectstore::{
    presign::{DirectMultipartIssuer, PresignedPartRequest, PresignedPutRequest},
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
    /// Every supplied token was rejected, each paired with the content ref
    /// it was supplied for. Never empty.
    Rejected(Vec<(ContentId, ContentTokenError)>),
}

/// The deployment material allowed to verify and mint content tokens.
/// Keeping it separate means upload helpers cannot reach unrelated server
/// configuration while handling a proof.
#[derive(Clone, Copy)]
pub(super) struct ContentTokenVerifier<'a> {
    secret: &'a str,
}

impl<'a> ContentTokenVerifier<'a> {
    pub(super) fn new(secret: &'a str) -> Self {
        Self { secret }
    }

    async fn prepare(
        self,
        writer: &FsWriter,
        namespace_id: &NamespaceId,
        token: &ContentToken,
        now_ms: u64,
    ) -> Result<Result<PreparedContent, ContentTokenError>, ApiResponseError> {
        writer
            .prepare_content_token(namespace_id, self.secret, token, now_ms)
            .await
            .map_err(|error| ApiResponseError::runtime_for_namespace(namespace_id, error))
    }

    fn mint_receipt(
        self,
        receipt: Option<&CompletedUploadReceipt>,
    ) -> Result<Option<ContentToken>, ApiResponseError> {
        let Some(receipt) = receipt else {
            return Ok(None);
        };
        let token = mint_content_token(self.secret, receipt, current_unix_ms()?)
            .map_err(content_token_error)?;
        Ok(Some(token))
    }
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct UploadPathParams {
    upload_id: String,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_upload",
        path = "/v0/namespaces/{namespace_id}/uploads",
        tag = "uploads",
        summary = "Begin upload",
        description = "Starts an upload session for content that may later be attached to a file. Service-proxied uploads send bytes through the server; direct-put uploads return object-store presigned credentials.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        request_body = BeginUploadRequest,
        responses(
            (status = 200, description = "Upload session started", body = BeginUploadResponse),
            (status = 400, description = "Invalid upload request", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 413, description = "Body exceeds the 1 MiB upload-control limit", body = ApiError),
            (status = 501, description = "Requested upload mode is unsupported", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_upload(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    query: AppQuery<NoQuery>,
    UploadControlJson(request): UploadControlJson<
        BeginUploadRequest,
        MAX_UPLOAD_CONTROL_BODY_BYTES,
    >,
) -> Result<Json<BeginUploadResponse>, ApiResponseError> {
    let namespace_id = namespace_id_path.into_id()?;
    query.into_params()?;
    // Decoding the body settled which transport this is and that it carries
    // that transport's fields and no other's, so there is nothing left here
    // to check before dispatching on it.
    match request {
        BeginUploadRequest::DirectPut { size_bytes } => {
            begin_direct_put_upload(state, namespace_id, size_bytes).await
        }
        BeginUploadRequest::DirectMultipart { part_size_bytes } => {
            begin_direct_multipart_upload(state, namespace_id, part_size_bytes).await
        }
        BeginUploadRequest::ServiceProxied {} => {
            let response = state
                .writer
                .begin_upload(&namespace_id, request)
                .await
                .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
            Ok(Json(response))
        }
    }
}

async fn begin_direct_put_upload(
    state: AppState,
    namespace_id: NamespaceId,
    size_bytes: Option<u64>,
) -> Result<Json<BeginUploadResponse>, ApiResponseError> {
    let Some(issuer) = state
        .direct_transfers
        .as_ref()
        .and_then(|transfers| transfers.put.as_ref())
    else {
        return Err(ApiResponseError::not_supported(
            FEATURE_UPLOADS_DIRECT_PUT,
            "direct_put requires an object store that can presign create-only uploads and \
             report a durable full-object checksum; this deployment's endpoint cannot",
        ));
    };
    let max_content_bytes = issuer.max_content_bytes();
    if let Some(size_bytes) = size_bytes.filter(|size_bytes| *size_bytes > max_content_bytes) {
        return Err(ApiResponseError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::ContentTooLarge,
            &format!(
                "this deployment's provider accepts at most {max_content_bytes} bytes in one \
                 direct_put request, and this request reports {}; check the \
                 `{LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES}` capability limit and use \
                 `direct_multipart` for larger content when \
                 `{FEATURE_UPLOADS_DIRECT_MULTIPART}` is advertised",
                size_bytes,
            ),
        )
        .with_param("/size_bytes"));
    }

    let checksum_algorithm = issuer.stored_checksum_algorithm();
    let prepared = state
        .writer
        .begin_direct_put_upload_target(&namespace_id, checksum_algorithm)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    let signed = issuer
        .presign_put(
            PresignedPutRequest {
                object_key: &prepared.object_key,
                expires_in: DIRECT_PUT_URL_TTL,
            },
            presign_time(),
        )
        .await
        .map_err(presign_issuer_error)?;

    Ok(Json(BeginUploadResponse::DirectPut {
        namespace_id: prepared.namespace_id,
        upload_id: prepared.upload_id,
        checksum_algorithm,
        access: ObjectTransferAccess::PresignedUrl {
            method: signed.method,
            url: signed.url,
            headers: signed.headers,
            expires_at_ms: signed.expires_at_ms,
        },
    }))
}

async fn begin_direct_multipart_upload(
    state: AppState,
    namespace_id: NamespaceId,
    part_size_bytes: Option<u64>,
) -> Result<Json<BeginUploadResponse>, ApiResponseError> {
    if state
        .direct_transfers
        .as_ref()
        .is_none_or(|transfers| transfers.multipart.is_none())
    {
        return Err(ApiResponseError::not_supported(
            FEATURE_UPLOADS_DIRECT_MULTIPART,
            "direct_multipart requires an object store that can presign checksum-bound \
             part uploads and run the provider's multipart control operations; this \
             deployment's endpoint cannot",
        ));
    }
    // Use the server's default when no part size is requested.
    let prepared = state
        .writer
        .begin_direct_multipart_upload_target(
            &namespace_id,
            DirectMultipartUploadOptions { part_size_bytes },
        )
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_invalid_request_param("/part_size_bytes")
        })?;

    Ok(Json(BeginUploadResponse::DirectMultipart {
        namespace_id: prepared.namespace_id,
        upload_id: prepared.upload_id,
        part_size_bytes: prepared.target.part_size_bytes,
        checksum_algorithm: prepared.target.checksum_algorithm,
    }))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "sign_upload_parts",
        path = "/v0/namespaces/{namespace_id}/uploads/{upload_id}/parts",
        tag = "uploads",
        summary = "Sign multipart parts",
        description = "Returns one short-lived, checksum-bound upload capability per requested part of an open direct_multipart session. Asking again for a part is how a client retries it.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
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
            (status = 413, description = "Body exceeds the 1 MiB upload-control limit", body = ApiError),
            (status = 501, description = "Direct multipart upload is unsupported", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn sign_upload_parts(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    path: AppPath<UploadPathParams>,
    query: AppQuery<NoQuery>,
    UploadControlJson(request): UploadControlJson<
        SignUploadPartsRequest,
        MAX_UPLOAD_CONTROL_BODY_BYTES,
    >,
) -> Result<Json<SignUploadPartsResponse>, ApiResponseError> {
    let namespace_id = namespace_id_path.into_id()?;
    let UploadPathParams { upload_id } = path.into_params()?;
    query.into_params()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let Some(issuer) = state
        .direct_transfers
        .as_ref()
        .and_then(|transfers| transfers.multipart.as_ref())
    else {
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
    let parts = sign_parts(issuer.as_ref(), &targets).await?;

    Ok(Json(SignUploadPartsResponse {
        namespace_id,
        upload_id,
        parts,
    }))
}

async fn sign_parts(
    issuer: &dyn DirectMultipartIssuer,
    targets: &loonfs::uploads::MultipartPartTargets,
) -> Result<Vec<SignedUploadPart>, ApiResponseError> {
    let signing_time = presign_time();
    let mut signed_parts = Vec::with_capacity(targets.parts.len());
    for part in &targets.parts {
        let signed = issuer
            .presign_multipart_part(
                PresignedPartRequest {
                    object_key: &targets.object_key,
                    provider_upload_id: &targets.provider_upload_id,
                    part_number: part.part_number,
                    checksum: &part.checksum,
                    expires_in: MULTIPART_PART_URL_TTL,
                },
                signing_time,
            )
            .await
            .map_err(presign_issuer_error)?;
        signed_parts.push(SignedUploadPart {
            part_number: part.part_number,
            access: ObjectTransferAccess::PresignedUrl {
                method: signed.method,
                url: signed.url,
                headers: signed.headers,
                expires_at_ms: signed.expires_at_ms,
            },
        });
    }
    Ok(signed_parts)
}

pub(super) fn presign_issuer_error(error: ObjectStoreError) -> ApiResponseError {
    let message = error.public_message();
    match error {
        ObjectStoreError::InvalidContentRef(_) => {
            ApiResponseError::new(StatusCode::BAD_REQUEST, ErrorCode::InvalidRequest, &message)
                .with_param("/content")
        }
        // The status comes from the registry so this handler cannot drift
        // from the status the rest of the server serves for the code.
        ObjectStoreError::PermissionDenied { .. } => ApiResponseError::new(
            status_for_core_error_code(ErrorCode::StoragePermissionDenied),
            ErrorCode::StoragePermissionDenied,
            &message,
        ),
        _ => ApiResponseError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::ServerError,
            &message,
        ),
    }
}

#[allow(clippy::disallowed_methods)]
pub(super) fn presign_time() -> SystemTime {
    // Issuing a short-lived transfer capability enters wall time at this HTTP
    // boundary so core replay stays deterministic.
    SystemTime::now()
}

/// Prepares the request's content proofs against the content refs its put
/// operations name. One prepared proof covers every operation that names its
/// ref; tokens covering a ref no operation puts are ignored.
///
/// A rejected token is reported whether or not a sibling token verified.
/// Coverage is still decided per ref — a put whose ref no proof admits is
/// refused by the commit engine — but a token this deployment did not mint,
/// or minted for another namespace or content store, is worth saying out
/// loud even when the request it arrived in went on to publish.
pub(super) async fn content_preparation_for_puts(
    writer: &FsWriter,
    verifier: ContentTokenVerifier<'_>,
    namespace_id: &NamespaceId,
    content_refs: &[&ContentRef],
    tokens: &[ContentToken],
    now_ms: u64,
) -> Result<PutContentPreparation, ApiResponseError> {
    let mut prepared_content = Vec::new();
    let mut rejections = Vec::new();
    for token in tokens
        .iter()
        .filter(|token| content_refs.contains(&&token.content_ref))
    {
        match verifier
            .prepare(writer, namespace_id, token, now_ms)
            .await?
        {
            Ok(prepared) => prepared_content.push(prepared),
            Err(error) => {
                let content_id = token.content_ref.content_id.clone();
                if is_forged_content_token(&error) {
                    tracing::warn!(
                        namespace_id = %namespace_id,
                        content_id = %content_id,
                        error = %error,
                        "content token was not minted by this deployment for this namespace \
                         and content store"
                    );
                } else {
                    tracing::debug!(
                        namespace_id = %namespace_id,
                        content_id = %content_id,
                        error = %error,
                        "content token rejected during put preparation"
                    );
                }
                rejections.push((content_id, error));
            }
        }
    }

    if !prepared_content.is_empty() {
        Ok(PutContentPreparation::Ready(prepared_content))
    } else if rejections.is_empty() {
        Ok(PutContentPreparation::Absent)
    } else {
        Ok(PutContentPreparation::Rejected(rejections))
    }
}

/// Whether a rejection says the token was not this deployment's to accept.
///
/// These three are the only rejections no honest client can produce: the
/// signature is this server's own HMAC, and the namespace and content store
/// are the pair the completed session was for. Everything else — an expiry,
/// a malformed body, a ref the payload does not cover — is a client that got
/// something wrong or waited too long, so it stays at debug.
fn is_forged_content_token(error: &ContentTokenError) -> bool {
    matches!(
        error,
        ContentTokenError::BadSignature
            | ContentTokenError::NamespaceMismatch
            | ContentTokenError::ContentStoreMismatch
    )
}

fn content_token_error(error: ContentTokenError) -> ApiResponseError {
    ApiResponseError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        ErrorCode::ServerError,
        &format!("failed to mint content token: {error}"),
    )
}

fn with_content_token(
    mut response: UploadSessionResponse,
    verifier: ContentTokenVerifier<'_>,
    receipt: Option<&CompletedUploadReceipt>,
) -> Result<UploadSessionResponse, ApiResponseError> {
    if let UploadSessionStatus::Completed { content_token, .. } = &mut response.status {
        *content_token = verifier.mint_receipt(receipt)?;
    }
    Ok(response)
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
        operation_id = "put_upload_content",
        path = "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
        tag = "uploads",
        summary = "Upload content",
        description = "Uploads bytes into a service-proxied upload session and returns the content reference for the stored object.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
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
            crate::http::openapi::UnavailableResponses
        )
    )
)]
/// Forwards a proxied upload's body straight into object storage.
///
/// The body is never held: it is hashed and written a piece at a time, so
/// the server's memory cost tracks the transfer's part size rather than the
/// object's length. The reference this produces is the same one the
/// buffered path produced: its `checksum` is the SHA-256 this server
/// computed over the complete payload.
///
/// A failure has two possible authors. The store may have refused the
/// write, or the body may have ended early — past the byte cap, or with a
/// broken connection — and only the second is the client's. The stream
/// records which, so the client is told the truth rather than a blanket
/// storage error.
pub(super) async fn put_upload_content(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    path: AppPath<UploadPathParams>,
    query: AppQuery<NoQuery>,
    body: UploadBodyStream,
) -> Result<Json<UploadContentResponse>, ApiResponseError> {
    let namespace_id = namespace_id_path.into_id()?;
    let UploadPathParams { upload_id } = path.into_params()?;
    query.into_params()?;
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
        operation_id = "complete_upload",
        path = "/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete",
        tag = "uploads",
        summary = "Complete upload",
        description = "Completes an upload. The request mode must match the mode used to start the session. Direct uploads include a content claim; multipart also includes completed parts.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("upload_id" = String, Path, description = "Upload session id")
        ),
        request_body(
            content = CompleteUploadRequest,
            description = "The request mode must match the upload session."
        ),
        responses(
            (status = 200, description = "Upload completed", body = UploadSessionResponse),
            (status = 400, description = "Invalid completion request", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or upload not found", body = ApiError),
            (status = 409, description = "Upload completion conflict", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 413, description = "Completion body exceeds the advertised `upload.completion_max_body_bytes` limit", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn complete_upload(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    path: AppPath<UploadPathParams>,
    query: AppQuery<NoQuery>,
    body: UploadBodyBytes<MAX_COMPLETION_BODY_BYTES>,
) -> Result<Json<UploadSessionResponse>, ApiResponseError> {
    let namespace_id = namespace_id_path.into_id()?;
    let UploadPathParams { upload_id } = path.into_params()?;
    query.into_params()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let body = body.into_bytes();
    let completed = state
        .writer
        .complete_upload_prepared_for_mode(&namespace_id, &upload_id, |mode| {
            decode_completion_body(mode, &body)
        })
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(with_content_token(
        completed.response,
        ContentTokenVerifier::new(state.config.content_token_secret()),
        completed.receipt.as_ref(),
    )?))
}

fn decode_completion_body(
    mode: UploadMode,
    body: &[u8],
) -> std::result::Result<ResolvedUploadCompletion, String> {
    let invalid = |error: serde_json::Error| {
        format!(
            "request body is not valid JSON for {} completion: {error}",
            upload_mode_name(mode)
        )
    };
    let request = serde_json::from_slice::<CompleteUploadRequest>(body).map_err(invalid)?;
    if request.mode() != mode {
        return Err(format!(
            "completion request mode `{}` does not match stored upload mode `{}`",
            upload_mode_name(request.mode()),
            upload_mode_name(mode)
        ));
    }

    match request {
        CompleteUploadRequest::ServiceProxied {} => Ok(ResolvedUploadCompletion::KnownContent),
        CompleteUploadRequest::DirectPut { content } => {
            Ok(ResolvedUploadCompletion::DirectPut { content })
        }
        CompleteUploadRequest::DirectMultipart { content, parts } => Ok(
            ResolvedUploadCompletion::Multipart(CompleteMultipartUploadRequest { content, parts }),
        ),
    }
}

fn upload_mode_name(mode: UploadMode) -> &'static str {
    match mode {
        UploadMode::ServiceProxied => "service_proxied",
        UploadMode::DirectPut => "direct_put",
        UploadMode::DirectMultipart => "direct_multipart",
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod completion_body_tests {
    use super::*;
    use loonfs_api::{
        v0::{CompletedUploadPart, UploadContentClaim, UploadPartChecksumClaim},
        Checksum, ChecksumAlgorithm,
    };

    const CONTENT: &str =
        r#"{"size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"0123456789abcdef"}}"#;

    #[test]
    fn stored_mode_selects_a_precise_completion_schema_error() {
        let multipart_body =
            format!(r#"{{"mode":"direct_multipart","content":{CONTENT},"parts":[]}}"#);
        let direct_put_error =
            decode_completion_body(UploadMode::DirectPut, multipart_body.as_bytes())
                .expect_err("multipart mode does not match direct put");
        assert_eq!(
            direct_put_error,
            "completion request mode `direct_multipart` does not match stored upload mode \
             `direct_put`"
        );

        let multipart_error = decode_completion_body(UploadMode::DirectMultipart, b"{}")
            .expect_err("completion mode is required");
        assert_eq!(
            multipart_error,
            "request body is not valid JSON for direct_multipart completion: missing field \
             `mode` at line 1 column 2"
        );

        let missing_parts = format!(r#"{{"mode":"direct_multipart","content":{CONTENT}}}"#);
        let missing_parts_error =
            decode_completion_body(UploadMode::DirectMultipart, missing_parts.as_bytes())
                .expect_err("multipart completion needs parts");
        assert_eq!(
            missing_parts_error,
            "request body is not valid JSON for direct_multipart completion: missing field \
             `parts`"
        );
    }

    #[test]
    fn retired_completion_fields_are_unknown_in_known_content_bodies() {
        for (body, field) in [
            (
                r#"{"mode":"service_proxied","completion":"content_ref"}"#,
                "completion",
            ),
            (
                r#"{"mode":"service_proxied","content_ref":{}}"#,
                "content_ref",
            ),
        ] {
            let error = decode_completion_body(UploadMode::ServiceProxied, body.as_bytes())
                .expect_err("retired completion field");
            assert!(
                error.contains(&format!("unknown field `{field}`")),
                "wrong error for {field}: {error}"
            );
        }
    }

    #[test]
    fn maximum_multipart_completion_fits_the_completion_body_cap() {
        let checksum = Checksum {
            algorithm: ChecksumAlgorithm::Sha256,
            value: "f".repeat(64),
        };
        let quoted_etag = format!("\"{}\"", "e".repeat(254));
        assert_eq!(quoted_etag.len(), 256);
        let request = CompleteUploadRequest::DirectMultipart {
            content: UploadContentClaim {
                size_bytes: u64::MAX,
                checksum: checksum.clone(),
            },
            parts: (1..=loonfs::MAX_MULTIPART_PARTS)
                .map(|part_number| CompletedUploadPart {
                    part_number,
                    etag: quoted_etag.clone(),
                    checksum: checksum.clone(),
                })
                .collect(),
        };

        let encoded = serde_json::to_vec(&request).expect("serialize maximal completion");
        assert!(
            encoded.len() < MAX_COMPLETION_BODY_BYTES,
            "{}-byte maximal completion exceeds the {}-byte cap",
            encoded.len(),
            MAX_COMPLETION_BODY_BYTES
        );
        let decoded = serde_json::from_slice::<CompleteUploadRequest>(&encoded)
            .expect("maximal completion decodes");
        assert_eq!(decoded, request);
    }

    #[test]
    fn maximum_part_signing_request_fits_the_control_body_cap() {
        let checksum = Checksum {
            algorithm: ChecksumAlgorithm::Sha256,
            value: "f".repeat(64),
        };
        let request = SignUploadPartsRequest {
            parts: (1..=loonfs::MAX_SIGNED_PARTS_PER_REQUEST)
                .map(|part_number| UploadPartChecksumClaim {
                    part_number: part_number as u32,
                    checksum: checksum.clone(),
                })
                .collect(),
        };

        let encoded = serde_json::to_vec(&request).expect("serialize maximal part signing");
        assert!(
            encoded.len() < MAX_UPLOAD_CONTROL_BODY_BYTES,
            "{}-byte maximal part-signing request exceeds the {}-byte cap",
            encoded.len(),
            MAX_UPLOAD_CONTROL_BODY_BYTES
        );
        let decoded = serde_json::from_slice::<SignUploadPartsRequest>(&encoded)
            .expect("maximal part-signing request decodes");
        assert_eq!(decoded, request);
    }
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_upload",
        path = "/v0/namespaces/{namespace_id}/uploads/{upload_id}",
        tag = "uploads",
        summary = "Get upload session",
        description = "Returns an upload session. A completed session includes a new content token so the client can retry the commit without uploading the content again.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("upload_id" = String, Path, description = "Upload session id")
        ),
        responses(
            (status = 200, description = "Upload session state", body = UploadSessionResponse),
            (status = 400, description = "Invalid upload id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or upload not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    namespace_id_path: NamespaceIdPath,
    path: AppPath<UploadPathParams>,
    query: AppQuery<NoQuery>,
) -> Result<Json<UploadSessionResponse>, ApiResponseError> {
    // Completed sessions return a fresh token when they are still allowed to
    // mint one. Authorization runs before the upload id is parsed.
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let UploadPathParams { upload_id } = path.into_params()?;
    query.into_params()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let (response, receipt) = state
        .writer
        .get_upload_status(&namespace_id, &upload_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(with_content_token(
        response,
        ContentTokenVerifier::new(state.config.content_token_secret()),
        receipt.as_ref(),
    )?))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "abort_upload",
        path = "/v0/namespaces/{namespace_id}/uploads/{upload_id}/abort",
        tag = "uploads",
        summary = "Abort upload",
        description = "Ends an upload session without selecting content and deletes the object it was writing. Repeating it succeeds; a session that already completed cannot be aborted.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("upload_id" = String, Path, description = "Upload session id")
        ),
        responses(
            (status = 200, description = "Upload aborted", body = UploadSessionResponse),
            (status = 400, description = "Invalid upload id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace or upload not found", body = ApiError),
            (status = 409, description = "Upload already completed", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn abort_upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    namespace_id_path: NamespaceIdPath,
    path: AppPath<UploadPathParams>,
    query: AppQuery<NoQuery>,
) -> Result<Json<UploadSessionResponse>, ApiResponseError> {
    authorize(state.config.auth_policy(), &headers)?;
    let namespace_id = namespace_id_path.into_id()?;
    let UploadPathParams { upload_id } = path.into_params()?;
    query.into_params()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let response = state
        .writer
        .abort_upload(&namespace_id, &upload_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

fn parse_upload_id(value: &str) -> Result<UploadId, ApiResponseError> {
    UploadId::parse(value).map_err(|error| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            &error.to_string(),
        )
        .with_param("upload_id")
    })
}
