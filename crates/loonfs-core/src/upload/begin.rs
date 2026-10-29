//! Starting an upload session: a proxied session, or a target for a direct
//! put or a direct multipart upload to the provider.

use super::abort::abort_unpublished_multipart_upload;
use super::session::{authorize_upload_subject, create_upload_session, NewUploadSession};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::limits::{MAX_MULTIPART_PART_BYTES, MIN_MULTIPART_PART_BYTES};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{ObjectStore, PROVIDER_MULTIPART_PART_BYTES};
use loonfs_types::options::DirectMultipartUploadOptions;
use loonfs_types::{
    ChecksumAlgorithm, ContentId, NamespaceAccess, NamespaceId, Subject, SubjectId, UploadSession,
};
use std::num::NonZeroU64;

/// Internal target for a new direct_put session, used by the server before
/// signing its URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectPutUploadTarget {
    pub session: UploadSession,
    pub object_key: String,
}

/// Internal multipart target used by the server before signing part URLs.
///
/// The session has an object identity but no content reference yet because
/// the payload size and checksum are supplied at completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectMultipartUploadTarget {
    pub session: UploadSession,
    pub object_key: String,
    pub part_size_bytes: u64,
    pub checksum_algorithm: ChecksumAlgorithm,
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Starts a service-proxied upload session. Direct transports use
    /// [`Self::begin_direct_put_upload_target`] or [`Self::begin_direct_multipart_upload_target`].
    pub async fn begin_upload(&self) -> Result<UploadSession> {
        let clock = self.namespace.start_clock();
        let catalog = self.namespace.ensure_upload_namespace_available().await?;
        let recorded = recorded_subject(
            &self.namespace.namespace_id,
            catalog.access(),
            self.subject.as_ref(),
        )?;
        create_upload_session(
            &self.namespace.store,
            &catalog,
            recorded,
            NewUploadSession::service_proxied(),
            &clock,
        )
        .await
    }

    /// Starts a direct PUT upload and assigns its content identity.
    pub async fn begin_direct_put_upload_target(&self) -> Result<DirectPutUploadTarget> {
        let clock = self.namespace.start_clock();
        let catalog = self.namespace.ensure_upload_namespace_available().await?;
        let recorded = recorded_subject(
            &self.namespace.namespace_id,
            catalog.access(),
            self.subject.as_ref(),
        )?;
        let content_id = ContentId::generate();
        let object_key = content_blob(&self.namespace.namespace_id, &content_id);
        let session = create_upload_session(
            &self.namespace.store,
            &catalog,
            recorded,
            NewUploadSession::direct_put(content_id, self.namespace.store.checksum_algorithm()),
            &clock,
        )
        .await?;
        Ok(DirectPutUploadTarget {
            session,
            object_key,
        })
    }

    /// Creates a direct multipart upload target with a new object identity,
    /// provider upload ID, and required part size.
    ///
    /// The final size and checksum are supplied when the upload is completed,
    /// because they may be unknown when streaming begins. The provider upload
    /// is created first and then stored in the session record. If the session
    /// record cannot be written, the provider upload is aborted.
    pub async fn begin_direct_multipart_upload_target(
        &self,
        options: DirectMultipartUploadOptions,
    ) -> Result<DirectMultipartUploadTarget> {
        let clock = self.namespace.start_clock();
        let catalog = self.namespace.ensure_upload_namespace_available().await?;
        let part_size_bytes = multipart_part_size(options.part_size_bytes)?;
        let recorded = recorded_subject(
            &self.namespace.namespace_id,
            catalog.access(),
            self.subject.as_ref(),
        )?;
        let content_id = ContentId::generate();
        let object_key = content_blob(&self.namespace.namespace_id, &content_id);
        let provider_upload_id = self
            .namespace
            .store
            .create_multipart_upload(&object_key)
            .await
            .map_err(|error| CoreError::store(&object_key, &error))?;
        let session = NewUploadSession::direct_multipart(
            content_id.clone(),
            &provider_upload_id,
            part_size_bytes,
            self.namespace.store.checksum_algorithm(),
        );
        let session =
            match create_upload_session(&self.namespace.store, &catalog, recorded, session, &clock)
                .await
            {
                Ok(session) => session,
                Err(error) => {
                    let _ = abort_unpublished_multipart_upload(
                        &self.namespace.store,
                        &self.namespace.namespace_id,
                        &content_id,
                        &provider_upload_id,
                    )
                    .await;
                    return Err(error);
                }
            };

        Ok(DirectMultipartUploadTarget {
            session,
            object_key,
            part_size_bytes: part_size_bytes.get(),
            checksum_algorithm: self.namespace.store.checksum_algorithm(),
        })
    }
}

/// Validates the multipart part size against provider limits.
///
/// The selected part size also bounds the maximum object size because the
/// provider accepts at most [`MAX_MULTIPART_PARTS`](crate::limits::MAX_MULTIPART_PARTS) parts.
fn multipart_part_size(requested: Option<u64>) -> Result<NonZeroU64> {
    let part_size_bytes = requested.unwrap_or(PROVIDER_MULTIPART_PART_BYTES);
    NonZeroU64::new(part_size_bytes)
        .filter(|size| (MIN_MULTIPART_PART_BYTES..=MAX_MULTIPART_PART_BYTES).contains(&size.get()))
        .ok_or_else(|| {
            CoreError::InvalidUploadContent(format!(
                "`part_size_bytes` must be between {MIN_MULTIPART_PART_BYTES} and \
                 {MAX_MULTIPART_PART_BYTES} bytes"
            ))
        })
}

fn recorded_subject(
    namespace_id: &NamespaceId,
    access: &NamespaceAccess,
    subject: Option<&Subject>,
) -> Result<Option<SubjectId>> {
    authorize_upload_subject(namespace_id, access, subject)?;
    Ok(match access {
        NamespaceAccess::Unrestricted {} => None,
        NamespaceAccess::Acl { .. } => subject.map(|subject| subject.subject_id.clone()),
    })
}
