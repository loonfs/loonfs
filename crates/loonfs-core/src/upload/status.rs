//! Reading an upload session's status.

use super::complete::evidence_within_window;
use super::proxied::find_proxied_content_ref;
use super::session::{authorize_upload_subject, ensure_session_subject, session_response};
use crate::engine::NamespaceEngine;
use crate::error::Result;
use crate::storage::content_admission::CompletedUploadEvidence;
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{UploadOutcome, UploadSessionMode};
use loonfs_types::{UploadId, UploadSession};

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Returns an upload session. Completed uploads include new evidence so
    /// the caller can retry publication without uploading the content again.
    pub async fn get_upload(&self, upload_id: &UploadId) -> Result<UploadSessionView> {
        let now_ms = self.now_ms();
        let catalog = self.namespace.ensure_upload_namespace_available().await?;
        authorize_upload_subject(
            &self.namespace.namespace_id,
            catalog.access(),
            self.subject.as_ref(),
        )?;
        let loaded = self.namespace.load_upload_session_state(upload_id).await?;
        ensure_session_subject(&loaded.session, self.subject.as_ref())?;
        let evidence = match &loaded.outcome {
            Some(UploadOutcome::Completed {
                completed_at_ms,
                content_ref,
            }) => evidence_within_window(content_ref, *completed_at_ms, now_ms),
            Some(UploadOutcome::Aborted { .. }) | None => None,
        };
        let (uploaded, direct_put_object_key) = match (&loaded.outcome, &loaded.session.mode) {
            (None, UploadSessionMode::ServiceProxied {}) => (
                find_proxied_content_ref(&self.namespace.store, &loaded.session).await?,
                None,
            ),
            (None, UploadSessionMode::DirectPut { .. }) => (
                None,
                Some(content_blob(
                    &self.namespace.namespace_id,
                    &loaded.session.content_id,
                )),
            ),
            _ => (None, None),
        };
        Ok(UploadSessionView {
            session: session_response(&loaded, uploaded, self.namespace.store.checksum_algorithm()),
            evidence,
            direct_put_object_key,
        })
    }
}

/// A session with the server-side details needed for fresh capabilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadSessionView {
    pub session: UploadSession,
    /// Fresh evidence for a completed session inside its minting window.
    pub evidence: Option<CompletedUploadEvidence>,
    /// Object key of an open `direct_put` session, for the server to presign.
    pub direct_put_object_key: Option<String>,
}
