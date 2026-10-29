//! Aborting an upload session, and releasing the provider objects of a
//! session that never completed.

use super::records::OutcomeCreation;
use super::session::{authorize_upload_subject, ensure_session_subject, upload_mode};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{
    UploadAlreadyCompletedError, UploadOutcome, UploadSessionMode, UploadSessionPayload,
};
use loonfs_types::{ContentId, NamespaceId, UploadId, UploadSession, UploadSessionStatus};

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Aborts an upload session, then deletes the content object it owned
    /// and cleans up its provider state.
    ///
    /// Cleanup starts only after the durable transition. Terminal and
    /// idempotent: repeating it succeeds and returns the original abort
    /// timestamp, and it refuses a session that already completed, whose
    /// content may be published.
    pub async fn abort_upload(&self, upload_id: &UploadId) -> Result<UploadSession> {
        let catalog = self.namespace.load_catalog_entry().await?;
        let clock = self.namespace.start_clock();
        let namespace_id = catalog.namespace_id();
        authorize_upload_subject(namespace_id, catalog.access(), self.subject.as_ref())?;
        let loaded = self.namespace.load_upload_session_state(upload_id).await?;
        ensure_session_subject(&loaded.session, self.subject.as_ref())?;
        let aborted_at_ms = match &loaded.outcome {
            Some(outcome) => repeated_abort(outcome, upload_id)?,
            None => {
                let outcome = UploadOutcome::Aborted {
                    aborted_at_ms: clock.request_at_ms(),
                };
                match self
                    .namespace
                    .create_upload_outcome(&loaded.session, &outcome)
                    .await?
                {
                    OutcomeCreation::Created => clock.request_at_ms(),
                    OutcomeCreation::Existing(existing) => repeated_abort(&existing, upload_id)?,
                }
            }
        };

        if !AbandonedUpload::of(&loaded.session)
            .release(&self.namespace.store)
            .await
        {
            tracing::debug!(
                namespace_id = %namespace_id,
                upload_id = %upload_id,
                "upload cleanup remains for garbage collection"
            );
        }
        Ok(UploadSession {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            mode: upload_mode(&loaded.session.mode),
            status: UploadSessionStatus::Aborted { aborted_at_ms },
        })
    }
}

/// Returns the first abort's stamp for a session that already ended, or
/// `UploadAlreadyCompleted` if it completed.
fn repeated_abort(outcome: &UploadOutcome, upload_id: &UploadId) -> Result<u64> {
    outcome
        .repeated_abort()
        .map_err(
            |UploadAlreadyCompletedError| CoreError::UploadAlreadyCompleted {
                upload_id: upload_id.clone(),
            },
        )
}

/// Provider resources owned by a terminated upload session.
///
/// The caller builds this value from the session record, and runs its
/// cleanup only after the aborted outcome record exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbandonedUpload {
    pub(crate) owner_namespace_id: NamespaceId,
    pub(crate) content_id: ContentId,
    pub(crate) provider_multipart_upload_id: Option<String>,
}

impl AbandonedUpload {
    pub(crate) fn of(state: &UploadSessionPayload) -> Self {
        let provider_multipart_upload_id = match &state.mode {
            UploadSessionMode::DirectMultipart {
                provider_upload_id, ..
            } => Some(provider_upload_id.clone()),
            UploadSessionMode::ServiceProxied { .. } | UploadSessionMode::DirectPut { .. } => None,
        };
        Self {
            owner_namespace_id: state.namespace_id.clone(),
            content_id: state.content_id.clone(),
            provider_multipart_upload_id,
        }
    }

    /// Releases everything the session left behind, provider upload first so
    /// the object it might still assemble cannot outlive the deletion.
    ///
    /// `true` confirms both cleanup steps. A failed provider abort stops
    /// before object deletion so an in-flight assembly cannot resurrect the
    /// object after it was deleted.
    #[must_use]
    pub(crate) async fn release<S: ObjectStore + ?Sized>(&self, store: &S) -> bool {
        if !self.release_provider(store).await {
            return false;
        }
        delete_unpublished_content_object(store, &self.owner_namespace_id, &self.content_id).await
    }

    pub(crate) async fn release_provider<S: ObjectStore + ?Sized>(&self, store: &S) -> bool {
        if let Some(provider_upload_id) = &self.provider_multipart_upload_id {
            if !abort_unpublished_multipart_upload(
                store,
                &self.owner_namespace_id,
                &self.content_id,
                provider_upload_id,
            )
            .await
            {
                return false;
            }
        }
        true
    }
}

/// Removes the content object an upload session owned but never published.
///
/// The id is random and an upload session is the only thing that can name
/// one before publication, so exactly one session is ever talking about this
/// object and no metadata can reference it. Deleting is therefore safe and
/// keeping it would leak bytes nobody can name again. This runs strictly
/// after the durable transition that made the session terminal, and it is
/// idempotent, so a cleanup lost to a crash is repeated by the next
/// garbage-collection pass. The returned flag tells that pass whether it may
/// remove the durable session record that makes the retry possible.
#[must_use]
pub(crate) async fn delete_unpublished_content_object<S: ObjectStore + ?Sized>(
    store: &S,
    owner_namespace_id: &NamespaceId,
    content_id: &ContentId,
) -> bool {
    let object_key = content_blob(owner_namespace_id, content_id);
    match store.delete(&object_key).await {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                content_id = %content_id,
                error_class = ?error.kind(),
                "failed to remove the content object of a terminated upload session"
            );
            false
        }
    }
}

/// Abandons the provider multipart upload a terminated session opened.
///
/// Aborting is safe whatever the upload's real state: an upload that already
/// assembled its object survives the abort untouched, and one the provider
/// has never heard of succeeds anyway. So this runs strictly after the
/// durable transition without first proving what it is cleaning up. The
/// returned flag tells garbage collection whether the provider is quiescent
/// enough to proceed to object deletion and eventual session-record removal.
#[must_use]
pub(crate) async fn abort_unpublished_multipart_upload<S: ObjectStore + ?Sized>(
    store: &S,
    owner_namespace_id: &NamespaceId,
    content_id: &ContentId,
    provider_upload_id: &str,
) -> bool {
    let object_key = content_blob(owner_namespace_id, content_id);
    match store
        .abort_multipart_upload(&object_key, provider_upload_id)
        .await
    {
        Ok(()) => true,
        Err(error) => {
            tracing::warn!(
                content_id = %content_id,
                error_class = ?error.kind(),
                "failed to abandon the multipart upload of a terminated upload session"
            );
            false
        }
    }
}
