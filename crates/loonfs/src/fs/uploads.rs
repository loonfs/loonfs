//! Staged uploads and direct-put targets.

use super::core::FsCore;
use crate::publish::PreparedContent;
use crate::uploads::BeginDirectPutUploadTargetResponse;
use crate::Result;
use crate::{
    BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, CompleteUploadResponse,
    ContentRef, NamespaceId, UploadContentResponse,
};
use loonfs_api::UploadId;

impl FsCore {
    /// Starts a durable upload session for a namespace.
    pub(crate) async fn begin_upload(
        &self,
        namespace_id: &NamespaceId,
        request: BeginUploadRequest,
    ) -> Result<BeginUploadResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .begin_upload(request)
            .await?)
    }

    /// Starts a direct_put upload session and returns the internal target for server-side signing.
    pub(crate) async fn begin_direct_put_upload_target(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> Result<BeginDirectPutUploadTargetResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .begin_direct_put_upload_target(content_ref)
            .await?)
    }

    /// Uploads whole-file content into an upload session.
    pub(crate) async fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        Ok(self
            .namespace_engine(namespace_id)
            .upload_content(upload_id, bytes)
            .await?)
    }

    /// Completes an upload session when the expected content ref matches.
    pub(crate) async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        let (response, _) = self
            .complete_upload_prepared(namespace_id, upload_id, request)
            .await?;
        Ok(response)
    }

    /// Completes an upload session and returns proof for later publication.
    ///
    /// Service-proxied completion performs no content-blob I/O. Direct-put
    /// completion performs one content-blob HEAD and no content-blob GET.
    pub(crate) async fn complete_upload_prepared(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<(CompleteUploadResponse, PreparedContent)> {
        let catalog = self
            .load_namespace_catalog_for_content_preparation(namespace_id)
            .await?;
        Ok(self
            .namespace_engine(namespace_id)
            .complete_upload_prepared_with_catalog(&catalog, upload_id, request)
            .await?)
    }
}
