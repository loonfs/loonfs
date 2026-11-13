//! Proxied uploads, where the server writes the request's bytes under the
//! session's content ID.

use super::records::UploadSessionState;
use super::session::{
    authorize_upload_subject, ensure_session_open, ensure_session_subject, session_response,
    upload_mode,
};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::store::NamespaceStore;
use crate::storage::content_staging::{
    stage_bytes_under_content_id, stage_streamed_under_content_id, ContentKeyOwner,
};
use crate::time::OperationClock;
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{ByteStream, ObjectStore, ObjectStoreError};
use loonfs_types::format::control::{UploadSessionMode, UploadSessionPayload};
use loonfs_types::{ContentRef, ContentRefKind, PoolId, Subject, UploadId, UploadSession};

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Uploads whole-file content into an upload session.
    ///
    /// Retries to the same session use the same object identity. Matching bytes
    /// are idempotent; different bytes return a content conflict. Separate
    /// sessions always use separate object identities.
    pub async fn upload_content(
        &self,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadSession> {
        let _permit = self.namespace.content_writes.acquire().await;
        self.namespace
            .upload_proxied_content(
                upload_id,
                self.subject.as_ref(),
                ProxiedPayload::Bytes(bytes),
                &self.namespace.start_clock(),
            )
            .await
    }

    /// Uploads content that arrives as a stream into an upload session,
    /// hashing it on the way through instead of keeping it in memory.
    pub async fn upload_content_stream(
        &self,
        upload_id: &UploadId,
        body: ByteStream,
    ) -> Result<UploadSession> {
        let _permit = self.namespace.content_writes.acquire().await;
        self.namespace
            .upload_proxied_content(
                upload_id,
                self.subject.as_ref(),
                ProxiedPayload::Stream(body),
                &self.namespace.start_clock(),
            )
            .await
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Writes a service-proxied session's content object and returns the open
    /// session with the uploaded reference.
    ///
    /// The write is create-only and writes no session record. The store makes
    /// the create conditional at every size, so when two requests upload to one
    /// session, only one of them creates the object. A request that finds the
    /// object already written succeeds if the stored checksum matches its bytes
    /// (a retry of the same upload), and returns a content conflict if the
    /// checksums differ.
    pub(crate) async fn upload_proxied_content(
        &self,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        payload: ProxiedPayload<'_>,
        clock: &OperationClock,
    ) -> Result<UploadSession> {
        let catalog = self.ensure_upload_namespace_available().await?;
        let pool_id = catalog.pool_id();
        authorize_upload_subject(&self.namespace_id, catalog.access(), subject)?;
        let loaded = self
            .load_open_proxied_session(&catalog, upload_id, subject, clock)
            .await?;
        let content_id = loaded.session.content_id.clone();
        let owner = ContentKeyOwner::ProxiedSession(upload_id);
        let content_ref = match payload {
            ProxiedPayload::Bytes(bytes) => {
                stage_bytes_under_content_id(&self.store, pool_id, content_id, bytes, owner)
                    .await?
                    .content_ref
            }
            ProxiedPayload::Stream(body) => {
                stage_streamed_under_content_id(
                    &self.store,
                    pool_id,
                    content_id,
                    body,
                    owner,
                    self.ensure_session_still_open(&catalog, upload_id, clock),
                )
                .await?
            }
        };
        // An abort during the write deletes the object it was writing, so the
        // bytes are reported as uploaded only if the session is still open.
        let state = self.load_upload_session_state(&catalog, upload_id).await?;
        ensure_session_open(&state, clock.request_at_ms())?;
        Ok(session_response(
            &state,
            Some(content_ref),
            self.store.checksum_algorithm(),
        ))
    }

    async fn load_open_proxied_session(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        clock: &OperationClock,
    ) -> Result<UploadSessionState> {
        let state = self.load_upload_session_state(catalog, upload_id).await?;
        ensure_session_subject(&state.session, subject)?;
        ensure_session_open(&state, clock.request_at_ms())?;
        if !matches!(state.session.mode, UploadSessionMode::ServiceProxied {}) {
            return Err(CoreError::InvalidUploadContent(format!(
                "{} sessions must be completed after using the presigned URLs",
                upload_mode(&state.session.mode).as_str()
            )));
        }
        Ok(state)
    }
}

pub(crate) enum ProxiedPayload<'a> {
    Bytes(&'a [u8]),
    Stream(ByteStream),
}

/// Builds the reference of a proxied session's content object, under the
/// prefix of `pool_id`, from the object's stored size and checksum, or
/// returns `None` if no upload has written it yet.
///
/// Only this session writes the object, with a create that the store makes
/// conditional, so the stored object contains the bytes that one request
/// uploaded.
pub(crate) async fn find_proxied_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    pool_id: &PoolId,
    session: &UploadSessionPayload,
) -> Result<Option<ContentRef>> {
    let object_key = content_blob(pool_id, &session.content_id);
    let Some(stored) = store
        .head(&object_key)
        .await
        .map_err(|error| CoreError::store(&object_key, &error))?
    else {
        return Ok(None);
    };
    let checksum = stored.checksum.ok_or_else(|| {
        CoreError::store(
            &object_key,
            &ObjectStoreError::StoredChecksumMissing {
                object_key: object_key.clone(),
            },
        )
    })?;
    Ok(Some(ContentRef {
        kind: ContentRefKind::BlobV1,
        content_id: session.content_id.clone(),
        size_bytes: stored.size_bytes,
        checksum,
    }))
}
