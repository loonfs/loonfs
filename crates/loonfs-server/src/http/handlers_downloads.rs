//! Download-grant handler: the read half of the direct transfer plane.
//!
//! A deployment that lets a client write an object straight into the bucket
//! must be able to hand that object back, and above `download.max_content_bytes`
//! it cannot do that by proxying. So the same issuer that authorizes the
//! write authorizes the read, gated on the same proof, and the two are
//! advertised together.

use super::error::ApiResponseError;
use super::handlers_filesystem::parse_revision_no;
use super::handlers_inodes::{parse_inode_id, InodeRevisionPathParams};
use super::handlers_uploads::{presign_issuer_error, presign_time};
use super::{AppJson, AppPath, AppState, NamespaceIdPath};
use axum::extract::State;
use axum::Json;
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::{
    v0::{
        BeginDownloadByInodeRequest, BeginDownloadByInodeResponse, BeginDownloadRequest,
        BeginDownloadResponse, ObjectTransferAccess,
    },
    FEATURE_DOWNLOADS_DIRECT_GET,
};
use loonfs_objectstore::presign::{DirectGetIssuer, PresignedGetRequest};
use std::time::Duration;

/// Lifetime of one read capability, the same window a whole-object write
/// gets.
///
/// It bounds a transfer, not a session: a reader that runs out of time asks
/// for another grant, which costs a few bytes of JSON and no retransfer,
/// and nothing is left holding a standing read capability on the bucket.
const DIRECT_GET_URL_TTL: Duration = Duration::from_secs(15 * 60);

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_download",
        path = "/v0/namespaces/{namespace_id}/filesystem/downloads",
        tag = "filesystem",
        summary = "Begin download",
        description = "Authorizes one direct read of a file's content object and returns a short-lived presigned GET capability, the resolved revision, and the content reference the client checks the arriving bytes against. `Range` is outside the signature, so one grant serves ranged, resumed, and parallel reads. Deployments that cannot presign answer 501 `not_supported`; the proxied `GET /filesystem/content` route stays available and is capped by `download.max_content_bytes`.",
        params(("namespace_id" = String, Path, description = "Namespace id")),
        request_body = BeginDownloadRequest,
        responses(
            (status = 200, description = "Download authorized", body = BeginDownloadResponse),
            (status = 400, description = "Invalid path or revision", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, path, or revision not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "Direct download is unsupported", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
/// Issues one short-lived read capability for the content object behind a
/// path.
///
/// The grant names one immutable content object, so a commit landing
/// afterwards neither invalidates it nor changes what it reads: the client
/// receives the bytes of the revision it asked for, and the reference in
/// the response is what it checks them against. The raw object key stays
/// server-side exactly as it does for `direct_put` — a client learns a URL
/// that expires, not an address it can revisit.
///
/// No download permit is taken, deliberately, and no `download.max_content_bytes`
/// applies. Both bound the memory a proxied read costs this process, and
/// this route reads metadata and signs a string: the file's bytes never
/// come here at all, which is the entire point.
pub(super) async fn create_download(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    AppJson(request): AppJson<BeginDownloadRequest>,
) -> Result<Json<BeginDownloadResponse>, ApiResponseError> {
    let namespace_id = namespace_id_path.into_id()?;
    // A store either authorizes direct transfers or it does not, and one
    // that does can always sign a read — so this asks for the bundle, not
    // for a direction within it.
    let issuer = direct_get_issuer(&state)?;

    let target = state
        .reader
        .direct_download_target(&namespace_id, request.path.as_str(), request.revision_no)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    let access = presigned_access(issuer, &target.object_key).await?;

    Ok(Json(BeginDownloadResponse {
        namespace_id,
        path: target.absolute_path,
        revision_no: target.revision_no,
        content_ref: target.content_ref,
        access,
    }))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_download_by_inode",
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
        tag = "inodes",
        summary = "Begin download by inode",
        description = "Authorizes a direct read of one retained inode revision. The request body is `{}` and the response does not include a path.",
        params(
            ("namespace_id" = String, Path, description = "Namespace id"),
            ("inode_id" = String, Path, description = "File inode ID", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("revision_no" = loonfs_api::RevisionNo, Path, description = "Revision number")
        ),
        request_body = BeginDownloadByInodeRequest,
        responses(
            (status = 200, description = "Download authorized", body = BeginDownloadByInodeResponse),
            (status = 400, description = "Invalid inode ID, revision number, or request body", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, inode, or revision not found", body = ApiError),
            (status = 409, description = "Inode is not a file", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "Direct download is unsupported", body = ApiError),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
/// Authorizes a direct read of one retained inode revision.
pub(super) async fn create_download_by_inode(
    State(state): State<AppState>,
    namespace_id_path: NamespaceIdPath,
    path: AppPath<InodeRevisionPathParams>,
    AppJson(_request): AppJson<BeginDownloadByInodeRequest>,
) -> Result<Json<BeginDownloadByInodeResponse>, ApiResponseError> {
    let namespace_id = namespace_id_path.into_id()?;
    let path = path.into_params()?;
    let inode_id = parse_inode_id(&path.inode_id)?;
    let revision_no = parse_revision_no(&path.revision_no)?;
    let issuer = direct_get_issuer(&state)?;
    let target = state
        .reader
        .direct_download_target_by_inode(&namespace_id, inode_id, revision_no)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    let access = presigned_access(issuer, &target.object_key).await?;
    Ok(Json(BeginDownloadByInodeResponse {
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
                 bounded by `download.max_content_bytes`",
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
                expires_in: DIRECT_GET_URL_TTL,
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
