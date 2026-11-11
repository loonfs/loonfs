//! Owned staging, where the writer stages content and completes its session
//! in one call.

use super::session::{create_upload_session_record, NewUploadSession};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::Result;
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::store::NamespaceStore;
use crate::storage::content_admission::PreparedContent;
use crate::storage::content_staging::{
    stage_bytes_under_content_id, stage_streamed_under_content_id, ContentKeyOwner,
};
use crate::time::OperationClock;
use loonfs_objectstore::{ByteStream, ObjectStore};
use loonfs_types::format::control::UploadSessionPayload;
use loonfs_types::{ContentRef, SubjectId};

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Stores in-process bytes as prepared content and completes the associated
    /// upload session.
    ///
    /// This combines session creation, content upload, and completion without a
    /// network round trip or receipt. It still writes the upload session's
    /// records that garbage collection needs: the session record before the
    /// content write, so collection always has a durable owner for the object,
    /// and the outcome record after it.
    pub async fn stage_owned_bytes(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        subject_id: Option<&SubjectId>,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        let _permit = self.namespace.content_writes.acquire().await;
        let catalog = self.own_catalog(catalog);
        let clock = self.namespace.start_clock();
        let session =
            open_owned_staging_session(&self.namespace.store, catalog, subject_id, &clock).await?;
        let stored = stage_bytes_under_content_id(
            &self.namespace.store,
            catalog.pool_id(),
            session.content_id.clone(),
            bytes,
            ContentKeyOwner::FreshIdentity,
        )
        .await?;
        self.namespace
            .complete_owned_staging(catalog, &session, stored.content_ref, &clock)
            .await
    }

    /// Stages a streamed payload as content a session owns, hashing it on
    /// the way through instead of keeping it in memory.
    ///
    /// The streaming twin of [`Self::stage_owned_bytes`]; ownership and cost
    /// are identical.
    pub async fn stage_owned_stream(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        subject_id: Option<&SubjectId>,
        body: ByteStream,
    ) -> Result<PreparedContent> {
        let _permit = self.namespace.content_writes.acquire().await;
        self.namespace
            .stage_owned_stream(
                self.own_catalog(catalog),
                subject_id,
                body,
                &self.namespace.start_clock(),
            )
            .await
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Stages a payload this runtime forwards under a session that owns it.
    ///
    /// The streaming twin of [`stage_owned_bytes`]: the bytes are hashed on
    /// their way to the store rather than kept in memory, and everything about ownership
    /// is identical.
    pub(crate) async fn stage_owned_stream(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        subject_id: Option<&SubjectId>,
        body: ByteStream,
        clock: &OperationClock,
    ) -> Result<PreparedContent> {
        let session = open_owned_staging_session(&self.store, catalog, subject_id, clock).await?;
        let content_ref = stage_streamed_under_content_id(
            &self.store,
            catalog.pool_id(),
            session.content_id.clone(),
            body,
            ContentKeyOwner::FreshIdentity,
            self.ensure_session_still_open(catalog, &session.upload_id, clock),
        )
        .await?;
        self.complete_owned_staging(catalog, &session, content_ref, clock)
            .await
    }
}

/// Creates the internal upload session that owns an in-process content
/// write.
async fn open_owned_staging_session<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    subject_id: Option<&SubjectId>,
    clock: &OperationClock,
) -> Result<UploadSessionPayload> {
    // Do not recheck namespace availability here. The catalog came from the
    // namespace head, and publication performs the final admission check. If
    // the namespace is deleted first, garbage collection removes the
    // unreferenced completed session and content.
    create_upload_session_record(
        store,
        catalog,
        subject_id.cloned(),
        NewUploadSession::service_proxied(),
        clock,
    )
    .await
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Completes an in-process staging session using the content reference
    /// produced by the write.
    ///
    /// No additional verification is needed because this process computed the
    /// checksum while writing. On failure, the open session eventually expires
    /// and garbage collection removes its object.
    async fn complete_owned_staging(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        session: &UploadSessionPayload,
        written: ContentRef,
        clock: &OperationClock,
    ) -> Result<PreparedContent> {
        Ok(self
            .record_completion(catalog.pool_id(), session, written, clock)
            .await?
            .prepared)
    }
}
