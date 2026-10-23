//! Direct-download handlers.

use super::error::ApiResponseError;
use super::extractors::SubjectHeaders;
use super::handlers_filesystem::{read_target, reject_snapshot_with_revision};
use super::handlers_inodes::{parse_inode_id, InodePathParams, InodeRevisionPathParams};
use super::handlers_uploads::{presign_issuer_error, presign_time};
use super::query_params::{live_consistency, parse_revision_no, read_at};
use super::{AppJson, AppPath, AppQuery, BindingState, NamespaceIdPath, NoQuery, OptionalAppJson};
use axum::extract::State;
use axum::Json;
use loonfs::{DownloadOptions, ReadAt};
use loonfs_objectstore::presign::{DirectGetIssuer, PresignedGetRequest};
use loonfs_objectstore::ByteRange;
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    api::v0::{
        CreateDownloadByInodeRequest, CreateDownloadByInodeResponse, CreateDownloadRequest,
        CreateDownloadResponse, CreateRevisionDownloadByInodeRequest, DownloadRange,
        ObjectTransferAccess,
    },
    FEATURE_DOWNLOADS_DIRECT_GET,
};
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
        description = "Returns ordered, signed object ranges that cover exactly `[start_offset, size_bytes)` of the selected revision. Each range includes its signed headers in `access.headers`, and the client sends them unchanged. The client checks each range's length and the complete revision's checksum. To resume a download, a client asks for a new grant from its current offset. A deployment that can't presign URLs returns 501 `not_supported`. The proxied `GET /filesystem/content` route is still available, up to the `download.service_proxied.max_content_bytes` limit.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID.")
        ),
        request_body = CreateDownloadRequest,
        responses(
            (status = 200, description = "Download authorized", body = CreateDownloadResponse),
            (status = 400, description = "Invalid path, revision, snapshot ID, revision_no combined with snapshot_id, or start_offset at or past the file's size", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, path, revision, or snapshot not found. The ID of a deleted or expired snapshot, or of a fork pin, returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            (status = 501, description = "Direct download is unsupported", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_download(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateDownloadRequest>,
) -> Result<Json<CreateDownloadResponse>, ApiResponseError> {
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    reject_snapshot_with_revision(
        request.snapshot_id.as_ref(),
        request.revision_no,
        "/revision_no",
    )?;
    let at = read_at(
        request.snapshot_id,
        request.consistency,
        request.max_staleness_ms,
    )
    .map_err(with_consistency_field_params)?;
    let target = read_target(runtime.namespace(&namespace_id), at, "/snapshot_id").await?;
    let issuer = direct_get_issuer(&state)?;

    let options = DownloadOptions {
        start_offset: request.start_offset,
        ..DownloadOptions::default()
    };
    let download = target
        .create_download(request.path.as_str(), request.revision_no, &options)
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_body_field_param("start_offset")
        })?;
    let ranges = presigned_ranges(issuer, &download.ranges, download.start_offset).await?;

    Ok(Json(CreateDownloadResponse {
        namespace_id,
        path: download.absolute_path,
        revision_no: download.revision_no,
        content_ref: download.content_ref,
        ranges,
    }))
}

/// Authorizes a direct read of the current revision of a file inode.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_download_by_inode",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/downloads",
        tag = "inodes",
        summary = "Begin download by inode",
        description = "Authorizes a direct read of the current revision of a visible file inode, wherever the inode is bound, or of the revision that an unexpired snapshot captured. The capability reads exactly `[start_offset, size_bytes)`, as on the path route. The response doesn't include a path.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The file's inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123")
        ),
        request_body = CreateDownloadByInodeRequest,
        responses(
            (status = 200, description = "Download authorized", body = CreateDownloadByInodeResponse),
            (status = 400, description = "Invalid inode ID, snapshot ID, or start_offset, or start_offset at or past the file's size", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, visible inode, or snapshot not found. The ID of a deleted or expired snapshot, or of a fork pin, returns `snapshot_not_found`.", body = ErrorResponse),
            (status = 409, description = "Inode isn't a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            (status = 501, description = "Direct download is unsupported", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_download_by_inode(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodePathParams>,
    AppQuery(_): AppQuery<NoQuery>,
    OptionalAppJson(request): OptionalAppJson<CreateDownloadByInodeRequest>,
) -> Result<Json<CreateDownloadByInodeResponse>, ApiResponseError> {
    let request = request.unwrap_or_default();
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let at = read_at(
        request.snapshot_id,
        request.consistency,
        request.max_staleness_ms,
    )
    .map_err(with_consistency_field_params)?;
    let target = read_target(runtime.namespace(&namespace_id), at, "/snapshot_id").await?;
    let issuer = direct_get_issuer(&state)?;
    let options = DownloadOptions {
        start_offset: request.start_offset,
        ..DownloadOptions::default()
    };
    let download = target
        .create_download_by_inode(inode_id, &options)
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_body_field_param("start_offset")
        })?;
    inode_download_response(issuer, namespace_id, download).await
}

/// Authorizes a direct read of one retained inode revision.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_revision_download_by_inode",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
        tag = "inodes",
        summary = "Begin revision download by inode",
        description = "Authorizes a direct read of one retained inode revision. The capability reads exactly `[start_offset, size_bytes)`, as on the path route. The response doesn't include a path.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("inode_id" = String, Path, description = "The file's inode ID.", pattern = r"^ino_[1-9][0-9]*$", example = "ino_123"),
            ("revision_no" = loonfs_types::RevisionNo, Path, description = "The revision number.")
        ),
        request_body = CreateRevisionDownloadByInodeRequest,
        responses(
            (status = 200, description = "Download authorized", body = CreateDownloadByInodeResponse),
            (status = 400, description = "Invalid inode ID, revision number, or start_offset, start_offset at or past the revision's size, or a snapshot_id, which can't be combined with a revision", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace, inode, or revision not found", body = ErrorResponse),
            (status = 409, description = "Inode isn't a file", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            (status = 501, description = "Direct download is unsupported", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_revision_download_by_inode(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(path): AppPath<InodeRevisionPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
    OptionalAppJson(request): OptionalAppJson<CreateRevisionDownloadByInodeRequest>,
) -> Result<Json<CreateDownloadByInodeResponse>, ApiResponseError> {
    let request = request.unwrap_or_default();
    let scoped_runtime = subject.map(|subject| state.runtime.with_subject(subject));
    let runtime = scoped_runtime.as_ref().unwrap_or(&state.runtime);
    let inode_id = parse_inode_id(&path.inode_id)?;
    let revision_no = parse_revision_no(&path.revision_no)?;
    let consistency = live_consistency(request.consistency, request.max_staleness_ms)
        .map_err(with_consistency_field_params)?;
    let issuer = direct_get_issuer(&state)?;
    let options = DownloadOptions {
        at: ReadAt::Live(consistency),
        start_offset: request.start_offset,
    };
    let target = runtime
        .namespace(&namespace_id)
        .create_revision_download_by_inode_with_options(inode_id, revision_no, &options)
        .await
        .map_err(|error| {
            ApiResponseError::runtime_for_namespace(&namespace_id, error)
                .with_body_field_param("start_offset")
        })?;
    inode_download_response(issuer, namespace_id, target).await
}

/// Reports a rejected `consistency` or `max_staleness_ms` as the JSON
/// Pointer of the body field.
fn with_consistency_field_params(error: ApiResponseError) -> ApiResponseError {
    error
        .with_body_field_param("consistency")
        .with_body_field_param("max_staleness_ms")
}

async fn inode_download_response(
    issuer: &dyn DirectGetIssuer,
    namespace_id: loonfs_types::NamespaceId,
    target: loonfs::downloads::DirectDownloadByInodeTarget,
) -> Result<Json<CreateDownloadByInodeResponse>, ApiResponseError> {
    let ranges = presigned_ranges(issuer, &target.ranges, target.start_offset).await?;
    Ok(Json(CreateDownloadByInodeResponse {
        namespace_id,
        inode_id: target.inode_id,
        revision_no: target.revision_no,
        content_ref: target.content_ref,
        ranges,
    }))
}

fn direct_get_issuer(state: &BindingState) -> Result<&dyn DirectGetIssuer, ApiResponseError> {
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

async fn presigned_ranges(
    issuer: &dyn DirectGetIssuer,
    ranges: &[loonfs::downloads::GrantedRange],
    mut start_offset: u64,
) -> Result<Vec<DownloadRange>, ApiResponseError> {
    let mut granted = Vec::with_capacity(ranges.len());
    for range in ranges {
        let signed = issuer
            .presign_get(
                PresignedGetRequest {
                    object_key: &range.object_key,
                    range: ByteRange {
                        start_inclusive: range.object_start,
                        end_exclusive: range.object_start + range.length,
                    },
                    ttl: Duration::from_millis(loonfs::DIRECT_TRANSFER_URL_TTL_MS),
                },
                presign_time(),
            )
            .await
            .map_err(presign_issuer_error)?;
        granted.push(DownloadRange {
            start_offset,
            length: range.length,
            access: ObjectTransferAccess::PresignedUrl {
                method: signed.method,
                url: signed.url,
                headers: signed.headers,
                expires_at_ms: signed.expires_at_ms,
            },
        });
        start_offset += range.length;
    }
    Ok(granted)
}
