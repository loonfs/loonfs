//! Download-grant handler: the read half of the direct transfer plane.
//!
//! A deployment that lets a client write an object straight into the bucket
//! must be able to hand that object back, and above `download.max_content_bytes`
//! it cannot do that by proxying. So the same issuer that authorizes the
//! write authorizes the read, gated on the same proof, and the two are
//! advertised together.

use super::error::ApiResponseError;
use super::handlers_uploads::{presign_issuer_error, presign_time};
use super::{AppJson, AppState, NamespaceIdPath};
use axum::extract::State;
use axum::Json;
#[cfg(feature = "openapi")]
use loonfs_api::ApiError;
use loonfs_api::{
    v0::{BeginDownloadRequest, BeginDownloadResponse, ObjectTransferAccess},
    FEATURE_DOWNLOADS_DIRECT_GET,
};
use loonfs_objectstore::presign::PresignedGetRequest;
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
        path = "/v0/namespaces/{namespace}/filesystem/downloads",
        tag = "filesystem",
        summary = "Begin download",
        description = "Authorizes one direct read of a file's content object and returns a short-lived presigned GET capability, the resolved revision, and the content reference the client checks the arriving bytes against. `Range` is outside the signature, so one grant serves ranged, resumed, and parallel reads. Deployments that cannot presign answer 501 `not_supported`; the proxied `GET /filesystem/content` route stays available and is capped by `download.max_content_bytes`.",
        params(("namespace" = String, Path, description = "Namespace id")),
        request_body = BeginDownloadRequest,
        responses(
            (status = 200, description = "Download authorized", body = BeginDownloadResponse),
            (status = 400, description = "Invalid path or revision", body = ApiError),
            (status = 401, description = "Unauthorized", body = ApiError),
            (status = 404, description = "Namespace, path, or revision not found", body = ApiError),
            (status = 410, description = "Namespace deleted", body = ApiError),
            (status = 501, description = "Direct download is unsupported", body = ApiError)
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
pub(super) async fn begin_download(
    State(state): State<AppState>,
    namespace: NamespaceIdPath,
    AppJson(request): AppJson<BeginDownloadRequest>,
) -> Result<Json<BeginDownloadResponse>, ApiResponseError> {
    let namespace_id = namespace.into_id()?;
    // A store either authorizes direct transfers or it does not, and one
    // that does can always sign a read — so this asks for the bundle, not
    // for a direction within it.
    let Some(issuer) = state
        .direct_transfers
        .as_ref()
        .map(|transfers| &transfers.get)
    else {
        return Err(ApiResponseError::not_supported(
            FEATURE_DOWNLOADS_DIRECT_GET,
            "direct_get requires an object store that can presign object reads; \
             this deployment's endpoint cannot, so every read is proxied and \
             bounded by `download.max_content_bytes`",
        ));
    };

    let target = state
        .reader
        .direct_download_target(&namespace_id, request.path.as_str(), request.revision_no)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    let signed = issuer
        .presign_get(
            PresignedGetRequest {
                object_key: &target.object_key,
                expires_in: DIRECT_GET_URL_TTL,
            },
            presign_time(),
        )
        .map_err(presign_issuer_error)?;

    Ok(Json(BeginDownloadResponse {
        namespace_id,
        absolute_path: target.absolute_path,
        revision_no: target.revision_no,
        content_ref: target.content_ref,
        access: ObjectTransferAccess::PresignedUrl {
            method: signed.method,
            url: signed.url,
            headers: signed.headers,
            expires_at_ms: signed.expires_at_ms,
        },
    }))
}
