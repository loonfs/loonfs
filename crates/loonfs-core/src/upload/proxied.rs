//! Proxied uploads, where the server writes the request's bytes under the
//! session's content ID.

use super::records::UploadSessionState;
use super::session::{
    authorize_upload_subject, ensure_session_open, ensure_session_subject, session_response,
    upload_mode,
};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::namespace::store::NamespaceStore;
use crate::storage::content::{verify_durable_content_checksum, DurableContentValidationError};
use crate::storage::content_staging::{stage_streamed_under_content_id, StreamedPayloadKind};
use crate::time::OperationClock;
use bytes::Bytes;
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{ByteStream, ObjectStore, ObjectStoreError};
use loonfs_types::format::control::{UploadSessionMode, UploadSessionPayload};
use loonfs_types::{ContentRef, ContentRefKind, Subject, UploadId, UploadSession};

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
    /// object already written succeeds if the stored size and checksum match its
    /// bytes (a retry of the same upload), and returns a content conflict if
    /// they don't.
    pub(crate) async fn upload_proxied_content(
        &self,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        payload: ProxiedPayload<'_>,
        clock: &OperationClock,
    ) -> Result<UploadSession> {
        let catalog = self.ensure_upload_namespace_available().await?;
        authorize_upload_subject(&self.namespace_id, catalog.access(), subject)?;
        let loaded = self
            .load_open_proxied_session(upload_id, subject, clock)
            .await?;
        let session = &loaded.session;
        let content_ref = match payload {
            ProxiedPayload::Bytes(bytes) => {
                write_proxied_bytes(&self.store, session, bytes).await?
            }
            ProxiedPayload::Stream(body) => {
                let staged = stage_streamed_under_content_id(
                    &self.store,
                    session.namespace_id.clone(),
                    session.content_id.clone(),
                    body,
                    StreamedPayloadKind::Request,
                    self.ensure_session_still_open(upload_id, clock),
                )
                .await?;
                if staged.already_present {
                    ensure_stored_content_matches(&self.store, upload_id, &staged.content_ref)
                        .await?;
                }
                staged.content_ref
            }
        };
        // An abort during the write deletes the object it was writing, so the
        // bytes are reported as uploaded only if the session is still open.
        let state = self.load_upload_session_state(upload_id).await?;
        ensure_session_open(&state, clock.request_at_ms())?;
        Ok(session_response(
            &state,
            Some(content_ref),
            self.store.checksum_algorithm(),
        ))
    }

    async fn load_open_proxied_session(
        &self,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        clock: &OperationClock,
    ) -> Result<UploadSessionState> {
        let state = self.load_upload_session_state(upload_id).await?;
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

/// Writes a proxied upload's buffered bytes to the session's content object.
async fn write_proxied_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    session: &UploadSessionPayload,
    bytes: &[u8],
) -> Result<ContentRef> {
    let content_ref = ContentRef::blob_v1(
        session.namespace_id.clone(),
        session.content_id.clone(),
        bytes,
        store.checksum_algorithm(),
    );
    let object_key = content_blob(&session.namespace_id, &session.content_id);
    match store
        .put_immutable_verified(&object_key, Bytes::copy_from_slice(bytes))
        .await
    {
        Ok(_) => Ok(content_ref),
        Err(ObjectStoreError::DifferentObject { .. }) => Err(CoreError::UploadContentConflict {
            upload_id: session.upload_id.clone(),
        }),
        Err(error) => Err(CoreError::store(&object_key, &error)),
    }
}

/// Accepts a content object that an earlier request already wrote only if
/// its stored size and checksum match the bytes of this request.
async fn ensure_stored_content_matches<S: ObjectStore + ?Sized>(
    store: &S,
    upload_id: &UploadId,
    content_ref: &ContentRef,
) -> Result<()> {
    match verify_durable_content_checksum(store, content_ref).await {
        Ok(()) => Ok(()),
        Err(error @ DurableContentValidationError::Store { .. }) => {
            Err(CoreError::DurableContent(error))
        }
        Err(_) => Err(CoreError::UploadContentConflict {
            upload_id: upload_id.clone(),
        }),
    }
}

/// Builds the reference of a proxied session's content object from the
/// object's stored size and checksum, or returns `None` if no upload has
/// written it yet.
///
/// Only this session writes the object, with a create that the store makes
/// conditional, so the stored object contains the bytes that one request
/// uploaded.
pub(crate) async fn find_proxied_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    session: &UploadSessionPayload,
) -> Result<Option<ContentRef>> {
    let object_key = content_blob(&session.namespace_id, &session.content_id);
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
        owner_namespace_id: session.namespace_id.clone(),
        content_id: session.content_id.clone(),
        size_bytes: stored.size_bytes,
        checksum,
    }))
}
