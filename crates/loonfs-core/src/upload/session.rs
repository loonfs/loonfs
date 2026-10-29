//! The checks and records that every step of an upload session uses: the
//! namespace, subject, and lease checks, the session record, and the session's
//! response.

use super::complete::completed_status;
use super::records::UploadSessionState;
use crate::authorize::{Authorizer, CommitAuthority};
use crate::control_update::create_control_object_under_generated_id;
use crate::error::{CoreError, Result};
use crate::limits::UPLOAD_SESSION_LEASE_MS;
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use bytes::Bytes;
use loonfs_objectstore::keys::upload_session;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{
    encode_control_state, ControlObjectKind, UploadOutcome, UploadSessionMode, UploadSessionPayload,
};
use loonfs_types::{
    ChecksumAlgorithm, ContentId, ContentRef, NamespaceAccess, NamespaceId, Subject, SubjectId,
    UploadId, UploadMode, UploadSession, UploadSessionStatus,
};
use std::num::NonZeroU64;

/// Values fixed when an upload session is created.
pub(crate) struct NewUploadSession {
    /// The content object this session will write, allocated up front.
    content_id: ContentId,
    /// How the bytes will reach it.
    mode: UploadSessionMode,
}

impl NewUploadSession {
    pub(crate) fn service_proxied() -> Self {
        Self {
            content_id: ContentId::generate(),
            mode: UploadSessionMode::ServiceProxied {},
        }
    }

    pub(crate) fn direct_put(content_id: ContentId, checksum_algorithm: ChecksumAlgorithm) -> Self {
        Self {
            content_id,
            mode: UploadSessionMode::DirectPut { checksum_algorithm },
        }
    }

    /// A multipart session records identity, the provider handle, and the
    /// geometry — and nothing about the payload, which it has not been told.
    pub(crate) fn direct_multipart(
        content_id: ContentId,
        provider_upload_id: &str,
        part_size_bytes: NonZeroU64,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> Self {
        Self {
            content_id,
            mode: UploadSessionMode::DirectMultipart {
                provider_upload_id: provider_upload_id.to_owned(),
                part_size_bytes,
                checksum_algorithm,
            },
        }
    }
}

pub(crate) async fn create_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    subject_id: Option<SubjectId>,
    session: NewUploadSession,
    clock: &OperationClock,
) -> Result<UploadSession> {
    let session = create_upload_session_record(store, catalog, subject_id, session, clock).await?;
    let state = UploadSessionState {
        session,
        outcome: None,
    };
    Ok(session_response(&state, None, store.checksum_algorithm()))
}

/// Writes the record that starts a new upload session, with put-if-absent.
pub(crate) async fn create_upload_session_record<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    subject_id: Option<SubjectId>,
    session: NewUploadSession,
    clock: &OperationClock,
) -> Result<UploadSessionPayload> {
    let upload_id = UploadId::generate();
    let record = UploadSessionPayload {
        namespace_id: catalog.namespace_id().clone(),
        upload_id: upload_id.clone(),
        content_id: session.content_id,
        subject_id,
        mode: session.mode,
        expires_at_ms: clock
            .request_at_ms()
            .saturating_add(UPLOAD_SESSION_LEASE_MS),
    };
    let object_key = upload_session(catalog.namespace_id(), &upload_id);
    let encoded = encode_control_state(ControlObjectKind::UploadSession, &record)
        .expect("an upload session record should encode");
    create_control_object_under_generated_id(store, &object_key, Bytes::from(encoded)).await?;
    Ok(record)
}

pub(crate) fn authorize_upload_subject(
    namespace_id: &NamespaceId,
    access: &NamespaceAccess,
    subject: Option<&Subject>,
) -> Result<()> {
    Authorizer::for_request(namespace_id, access, CommitAuthority::Subject(subject)).map(|_| ())
}

pub(crate) fn ensure_session_subject(
    session: &UploadSessionPayload,
    subject: Option<&Subject>,
) -> Result<()> {
    match (
        &session.subject_id,
        subject.map(|subject| &subject.subject_id),
    ) {
        (None, _) => Ok(()),
        (Some(_), None) => Err(CoreError::SubjectRequired {
            namespace_id: session.namespace_id.clone(),
        }),
        (Some(owner), Some(caller)) if owner == caller => Ok(()),
        (Some(_), Some(_)) => Err(CoreError::UploadNotFound {
            upload_id: session.upload_id.clone(),
        }),
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    pub(crate) async fn ensure_upload_namespace_available(
        &self,
    ) -> Result<VerifiedNamespaceCatalogEntry> {
        let manifest = self.load_current_manifest().await?;
        let head = crate::namespace::state::NamespaceReadState::from(manifest.payload());
        crate::namespace::control::ensure_namespace_live(&head.namespace_id, head.status)?;
        Ok(VerifiedNamespaceCatalogEntry::from_head(&head))
    }

    /// Checks, just before a streamed write sends its final request, that the
    /// session is still open.
    ///
    /// A session aborted after this check keeps its records for a grace period
    /// longer than the final request can take, so a later cleanup removes any
    /// object the request writes.
    pub(crate) async fn ensure_session_still_open(
        &self,
        upload_id: &UploadId,
        clock: &OperationClock,
    ) -> Result<()> {
        let state = self.load_upload_session_state(upload_id).await?;
        ensure_session_open(&state, clock.request_at_ms())
    }
}

/// Refuses an operation that needs an open session when the session is
/// completed, aborted, or open at or past its expiry.
pub(crate) fn ensure_session_open(state: &UploadSessionState, now_ms: u64) -> Result<()> {
    let upload_id = state.session.upload_id.clone();
    match &state.outcome {
        None if now_ms < state.session.expires_at_ms => Ok(()),
        Some(UploadOutcome::Completed { .. }) => {
            Err(CoreError::UploadAlreadyCompleted { upload_id })
        }
        None | Some(UploadOutcome::Aborted { .. }) => Err(CoreError::UploadNotFound { upload_id }),
    }
}

/// Builds the wire view of a session. `uploaded` is the content reference
/// an open proxied session reports once its bytes are written.
pub(crate) fn session_response(
    state: &UploadSessionState,
    uploaded: Option<ContentRef>,
    algorithm: ChecksumAlgorithm,
) -> UploadSession {
    let session = &state.session;
    let status = match &state.outcome {
        None => UploadSessionStatus::Open {
            expires_at_ms: session.expires_at_ms,
            checksum_algorithm: algorithm,
            part_size_bytes: match &session.mode {
                UploadSessionMode::DirectMultipart {
                    part_size_bytes, ..
                } => Some(part_size_bytes.get()),
                _ => None,
            },
            access: None,
            content_ref: uploaded,
        },
        Some(UploadOutcome::Completed {
            completed_at_ms,
            content_ref,
        }) => completed_status(content_ref, *completed_at_ms),
        Some(UploadOutcome::Aborted { aborted_at_ms }) => UploadSessionStatus::Aborted {
            aborted_at_ms: *aborted_at_ms,
        },
    };
    UploadSession {
        namespace_id: session.namespace_id.clone(),
        upload_id: session.upload_id.clone(),
        mode: upload_mode(&session.mode),
        status,
    }
}

pub(crate) fn upload_mode(mode: &UploadSessionMode) -> UploadMode {
    match mode {
        UploadSessionMode::ServiceProxied {} => UploadMode::ServiceProxied,
        UploadSessionMode::DirectPut { .. } => UploadMode::DirectPut,
        UploadSessionMode::DirectMultipart { .. } => UploadMode::DirectMultipart,
    }
}
