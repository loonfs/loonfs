//! Object-store implementations for uploads, commit publication, and the
//! change feed. [`NamespaceEngine`](crate::engine::NamespaceEngine) exposes
//! the supported entry points.

mod batch;
mod candidates;
mod changes;
mod publish_view;
mod uploads;

pub(crate) use self::batch::{
    publish_namespace_commits_batch_against_publish_view, PublishViewEffect,
};
pub(crate) use self::changes::list_changes_after;
pub(crate) use self::publish_view::{load_publish_metadata_view, PublishTailProjection};
pub use self::publish_view::{PublishTailOptions, PublishTailWeight};
pub(crate) use self::uploads::{
    abort_upload, begin_direct_multipart_upload_target, begin_direct_put_upload_target,
    begin_service_proxied_upload, complete_upload, complete_upload_for_mode,
    direct_multipart_part_targets, get_upload_status, stage_owned_bytes, stage_owned_stream,
    upload_content, upload_streamed_content, AbandonedUpload,
};
pub use self::uploads::{
    BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse, CompletedUpload,
    DirectMultipartUploadTarget, MultipartPartTarget, MultipartPartTargets,
    ResolvedUploadCompletion,
};

#[cfg(test)]
pub(crate) async fn begin_upload<S: loonfs_objectstore::ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &loonfs_api::NamespaceId,
    request: loonfs_api::v0::BeginUploadRequest,
    context: &crate::MutationContext,
) -> crate::error::Result<loonfs_api::v0::BeginUploadResponse> {
    assert!(matches!(
        request,
        loonfs_api::v0::BeginUploadRequest::ServiceProxied {}
    ));
    uploads::begin_service_proxied_upload(store, namespace_id, context).await
}
