//! Direct-download handlers.

use super::error::ApiResponseError;
use super::extractors::SubjectHeaders;
use super::handlers_filesystem::{pin_requested_snapshot, reject_snapshot_with_revision};
use super::handlers_inodes::{parse_inode_id, InodeRevisionPathParams};
use super::handlers_uploads::{presign_issuer_error, presign_time};
use super::query_params::parse_revision_no;
use super::{AppJson, AppPath, AppQuery, AppState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::Json;
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::{
    v0::{
        CreateDownloadByInodeResponse, CreateDownloadRequest, CreateDownloadResponse,
        ObjectTransferAccess,
    },
    FEATURE_DOWNLOADS_DIRECT_GET,
};
use loonfs_objectstore::presign::{DirectGetIssuer, PresignedGetRequest};
use std::time::Duration;

/// Issues a short-lived download URL for a file.
///
/// This endpoint reads metadata but does not proxy the file bytes, so service-proxied
/// download limits do not apply.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_download",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/filesystem/downloads",
        tag = "filesystem",
        summary = "Begin download",
        description = "Authorizes one direct read of a file's content object and returns a short-lived presigned GET capability, the resolved revision, and the content reference the client checks the arriving bytes against. `Range` is outside the signature, so one grant serves ranged, resumed, and parallel reads. Deployments that cannot presign answer 501 `not_supported`; the proxied `GET /filesystem/content` route stays available and is capped by `download.service_proxied.max_content_bytes`.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id")
        ),
        request_body = CreateDownloadRequest,
        responses(
            (status = 200, description = "Download authorized", body = CreateDownloadResponse),
            (status = 400, description = "Invalid path, revision, snapshot id, non-snapshot checkpoint, or revision_no combined with snapshot_id", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, path, revision, or snapshot not found", body = ApiError),
            (status = 410, description = "Namespace deleted or snapshot deleted or expired", body = ApiError),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ApiError),
            (status = 501, description = "Direct download is unsupported", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_download(
    State(state): State<AppState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateDownloadRequest>,
) -> Result<Json<CreateDownloadResponse>, ApiResponseError> {
    let scoped_reader = subject.map(|subject| state.reader.as_subject(subject));
    let reader = scoped_reader.as_ref().unwrap_or(&state.reader);
    reject_snapshot_with_revision(request.snapshot_id.as_ref(), request.revision_no)?;
    let target = pin_requested_snapshot(reader, &namespace_id, request.snapshot_id).await?;
    let issuer = direct_get_issuer(&state)?;

    let download = target
        .create_download(request.path.as_str(), request.revision_no)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    let access = presigned_access(issuer, &download.object_key).await?;

    Ok(Json(CreateDownloadResponse {
        namespace_id,
        path: download.absolute_path,
        revision_no: download.revision_no,
        content_ref: download.content_ref,
        access,
    }))
}

/// Authorizes a direct read of one retained inode revision.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_download_by_inode",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
        tag = "inodes",
        summary = "Begin download by inode",
        description = "Authorizes a direct read of one retained inode revision. The request has no body and the response does not include a path.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "File inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("revision_no" = loonfs_api::RevisionNo, Path, description = "Revision number")
        ),
        responses(
            (status = 200, description = "Download authorized", body = CreateDownloadByInodeResponse),
            (status = 400, description = "Invalid inode ID or revision number", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, inode, or revision not found", body = ApiError),
            (status = 409, description = "Inode is not a file", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "Direct download is unsupported", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_download_by_inode(
    State(state): State<AppState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodeRevisionPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<CreateDownloadByInodeResponse>, ApiResponseError> {
    let scoped_reader = subject.map(|subject| state.reader.as_subject(subject));
    let reader = scoped_reader.as_ref().unwrap_or(&state.reader);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let revision_no = parse_revision_no(&path.revision_no)?;
    let issuer = direct_get_issuer(&state)?;
    let target = reader
        .create_download_by_inode(&namespace_id, inode_id, revision_no)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    let access = presigned_access(issuer, &target.object_key).await?;
    Ok(Json(CreateDownloadByInodeResponse {
        namespace_id,
        inode_id: target.inode_id,
        revision_no: target.revision_no,
        content_ref: target.content_ref,
        access,
    }))
}

fn direct_get_issuer(state: &AppState) -> Result<&dyn DirectGetIssuer, ApiResponseError> {
    state
        .direct_transfers
        .as_ref()
        .map(|transfers| transfers.get.as_ref())
        .ok_or_else(|| {
            ApiResponseError::not_supported(
                FEATURE_DOWNLOADS_DIRECT_GET,
                "direct_get requires an object store that can presign object reads; \
                 this deployment's endpoint cannot, so every read is proxied and \
                 bounded by `download.service_proxied.max_content_bytes`",
            )
        })
}

async fn presigned_access(
    issuer: &dyn DirectGetIssuer,
    object_key: &str,
) -> Result<ObjectTransferAccess, ApiResponseError> {
    let signed = issuer
        .presign_get(
            PresignedGetRequest {
                object_key,
                expires_in: Duration::from_millis(loonfs::DIRECT_TRANSFER_URL_TTL_MS),
            },
            presign_time(),
        )
        .await
        .map_err(presign_issuer_error)?;
    Ok(ObjectTransferAccess::PresignedUrl {
        method: signed.method,
        url: signed.url,
        headers: signed.headers,
        expires_at_ms: signed.expires_at_ms,
    })
}
