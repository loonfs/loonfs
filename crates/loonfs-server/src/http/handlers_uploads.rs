//! Upload session handlers plus the presign and content-token helpers
//! backing them.

use super::error::ApiResponseError;
use super::{authorize, AppJson, AppState, NamespaceIdPath, OptionalAppJson, UploadBodyBytes};
use crate::config::ServerConfig;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use loonfs::content_tokens::{
    mint_content_token, verify_content_token, ContentAdmission, ContentTokenError,
};
use loonfs::ErrorCode;
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::{
    v0::{
        BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, CompleteUploadResponse,
        DirectPutUpload, ObjectTransferAccess, UploadContentResponse, UploadMode,
        ValidatedContentToken,
    },
    ContentRef, NamespaceId, UploadId, FEATURE_UPLOADS_DIRECT_PUT,
};
use loonfs_objectstore::{presign::PresignedPutRequest, ObjectStoreError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DIRECT_PUT_URL_TTL: Duration = Duration::from_secs(15 * 60);

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
    headers: HeaderMap,
    OptionalAppJson(request): OptionalAppJson<BeginUploadRequest>,
) -> Result<Json<BeginUploadResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let request = request.unwrap_or_default();
    if request.mode.unwrap_or_default() == UploadMode::DirectPut {
        return begin_direct_put_upload(state, namespace_id, request).await;
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
            "direct_put requires a presigned URL capable object store",
        ));
    };
    let Some(content_ref) = request.content_ref else {
        return Err(ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRequest,
            "invalid upload content: direct_put requires content_ref at begin_upload",
        ));
    };

    let prepared = state
        .writer
        .begin_direct_put_upload_target(&namespace_id, content_ref)
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
    }))
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
    // Issuing a short-lived transfer capability is an explicit wall-clock boundary.
    SystemTime::now()
}

pub(super) fn content_admissions_for_put(
    config: &ServerConfig,
    namespace_id: &NamespaceId,
    content_ref: &ContentRef,
    tokens: &[ValidatedContentToken],
    now_ms: u64,
) -> Vec<ContentAdmission> {
    tokens
        .iter()
        .filter(|token| token.content_ref == *content_ref)
        .filter_map(|token| {
            verify_content_token(config.content_token_secret(), namespace_id, token, now_ms).ok()
        })
        .collect()
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
pub(super) async fn upload_content(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    AxumPath(UploadPathParams { upload_id }): AxumPath<UploadPathParams>,
    headers: HeaderMap,
    body: UploadBodyBytes,
) -> Result<Json<UploadContentResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let response = state
        .writer
        .upload_content(&namespace_id, &upload_id, &body.bytes)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
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
    AxumPath(UploadPathParams { upload_id }): AxumPath<UploadPathParams>,
    headers: HeaderMap,
    AppJson(request): AppJson<CompleteUploadRequest>,
) -> Result<Json<CompleteUploadResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = namespace.into_id()?;
    let upload_id = parse_upload_id(&upload_id)?;
    let mut response = state
        .writer
        .complete_upload(&namespace_id, &upload_id, &request)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    response.validated_content_token = Some(
        mint_content_token(
            state.config.content_token_secret(),
            &namespace_id,
            &response.content_ref,
            current_unix_ms()?,
        )
        .map_err(content_token_error)?,
    );
    Ok(Json(response))
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
