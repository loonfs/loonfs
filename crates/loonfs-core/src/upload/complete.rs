//! Completing an upload session: verifying the uploaded bytes and creating
//! the completed outcome, or returning an earlier completion again.

use super::direct::validate_upload_checksum;
use super::proxied::find_proxied_content_ref;
use super::records::OutcomeCreation;
use super::session::{
    authorize_upload_subject, ensure_session_open, ensure_session_subject, upload_mode,
};
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::limits::{COMPLETED_UPLOAD_ADMISSION_WINDOW_MS, COMPLETED_UPLOAD_RECEIPT_WINDOW_MS};
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::store::NamespaceStore;
use crate::storage::content::{verify_durable_content_checksum, DurableContentValidationError};
use crate::storage::content_admission::{CompletedUploadEvidence, PreparedContent};
use crate::time::OperationClock;
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{MultipartPart, ObjectStore};
use loonfs_types::format::control::{UploadOutcome, UploadSessionMode, UploadSessionPayload};
use loonfs_types::{
    ChecksumAlgorithm, CompleteUploadBody, CompletedUploadPart, ContentId, ContentRef,
    ContentRefKind, NamespaceId, UploadContentClaim, UploadId, UploadMode, UploadSession,
    UploadSessionStatus,
};

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Completes an upload session and returns time-bounded proof for later
    /// publication. The caller passes the catalog it already has, which
    /// must be this engine's namespace's.
    ///
    /// Verification of the content happens before the outcome record is
    /// created. An abort can still end the session between this completion's
    /// read of the session and its check of the content object, and the
    /// put-if-absent of the outcome record settles that race: if the abort's
    /// outcome is created first, the completion finds it and fails.
    pub async fn complete_upload(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        upload_id: &UploadId,
        completion: CompleteUploadBody,
    ) -> Result<CompletedUpload> {
        self.own_catalog(catalog);
        let clock = self.namespace.start_clock();
        let catalog = self.namespace.ensure_upload_namespace_available().await?;
        authorize_upload_subject(
            &self.namespace.namespace_id,
            catalog.access(),
            self.subject.as_ref(),
        )?;
        let now_ms = clock.request_at_ms();
        let loaded = self.namespace.load_upload_session_state(upload_id).await?;
        ensure_session_subject(&loaded.session, self.subject.as_ref())?;
        if !matches!(loaded.outcome, Some(UploadOutcome::Completed { .. })) {
            ensure_session_open(&loaded, now_ms)?;
        }
        let mode = upload_mode(&loaded.session.mode);
        let plan = completion_plan(&loaded.session, &completion)?;
        if let Some(outcome) = &loaded.outcome {
            return replay_terminal_completion(
                outcome,
                &self.namespace.namespace_id,
                upload_id,
                plan.expected_completed_content(),
                mode,
                now_ms,
            );
        }

        let verified = verify_completion(&self.namespace.store, plan).await?;
        self.namespace
            .record_completion(&loaded.session, verified, &clock)
            .await
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Records a verified content reference as the session's completed outcome.
    ///
    /// All upload modes use this transition. Completion makes the content
    /// eligible for publication and later garbage collection.
    pub(crate) async fn record_completion(
        &self,
        session: &UploadSessionPayload,
        verified: ContentRef,
        clock: &OperationClock,
    ) -> Result<CompletedUpload> {
        let now_ms = clock.request_at_ms();
        let outcome = UploadOutcome::Completed {
            completed_at_ms: now_ms,
            content_ref: verified.clone(),
        };
        let standing = match self.create_upload_outcome(session, &outcome).await? {
            OutcomeCreation::Created => outcome,
            OutcomeCreation::Existing(existing) => existing,
        };
        replay_terminal_completion(
            &standing,
            &session.namespace_id,
            &session.upload_id,
            Some(&verified),
            upload_mode(&session.mode),
            now_ms,
        )
    }
}

/// Builds a content reference from a client's upload claim.
///
pub(crate) fn claimed_content_ref(
    owner_namespace_id: NamespaceId,
    content_id: ContentId,
    claim: &UploadContentClaim,
    required_algorithm: ChecksumAlgorithm,
) -> Result<ContentRef> {
    Ok(ContentRef {
        kind: ContentRefKind::BlobV1,
        owner_namespace_id,
        content_id,
        size_bytes: claim.size_bytes,
        checksum: validate_upload_checksum(&claim.checksum, required_algorithm)?.clone(),
    })
}

/// Turns a client's part bookkeeping into what the provider assembles from.
pub(crate) fn multipart_parts(
    parts: &[CompletedUploadPart],
    checksum_algorithm: ChecksumAlgorithm,
) -> Result<Vec<MultipartPart>> {
    if parts.is_empty() {
        return Err(CoreError::InvalidUploadContent(
            "completion must include at least one uploaded part".to_owned(),
        ));
    }
    let mut previous = 0;
    parts
        .iter()
        .map(|part| {
            if part.part_number <= previous {
                return Err(CoreError::InvalidUploadContent(
                    "completion lists each part once, in ascending part order".to_owned(),
                ));
            }
            previous = part.part_number;
            if part.etag.trim().is_empty() {
                return Err(CoreError::InvalidUploadContent(format!(
                    "part {} carries no etag",
                    part.part_number
                )));
            }
            Ok(MultipartPart {
                part_number: part.part_number,
                etag: part.etag.clone(),
                checksum: validate_upload_checksum(&part.checksum, checksum_algorithm)?.clone(),
            })
        })
        .collect()
}

/// What a completed upload hands back: the wire response, the in-process
/// admission a same-process publication uses, and the evidence a remote one
/// returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedUpload {
    /// Wire response for the completion or its idempotent replay.
    pub response: UploadSession,
    /// Admission for a publication in this process, which needs no token but
    /// expires at the completed session's final admission horizon.
    pub prepared: PreparedContent,
    /// Evidence for a publication elsewhere, or `None` once the session has
    /// stopped minting it.
    pub evidence: Option<CompletedUploadEvidence>,
}

fn completed_upload(
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    content: &ContentRef,
    mode: UploadMode,
    completed_at_ms: u64,
    now_ms: u64,
) -> CompletedUpload {
    CompletedUpload {
        response: UploadSession {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            mode,
            status: completed_status(content, completed_at_ms),
        },
        prepared: PreparedContent::for_completed_upload(
            content.clone(),
            completed_at_ms.saturating_add(COMPLETED_UPLOAD_ADMISSION_WINDOW_MS),
            Some(upload_id.clone()),
        ),
        evidence: evidence_within_window(content, completed_at_ms, now_ms),
    }
}

pub(crate) fn completed_status(
    content_ref: &ContentRef,
    completed_at_ms: u64,
) -> UploadSessionStatus {
    UploadSessionStatus::Completed {
        completed_at_ms,
        content_ref: content_ref.clone(),
        content_token: None,
    }
}

/// Mints evidence only while the completed session is still inside its
/// receipt window.
///
/// The window is what makes content reclamation decidable: past it no new
/// evidence exists, so no new metadata reference to this content can appear
/// (`limits::CONTENT_RECLAMATION_GRACE_WINDOW_MS`).
pub(crate) fn evidence_within_window(
    content: &ContentRef,
    completed_at_ms: u64,
    now_ms: u64,
) -> Option<CompletedUploadEvidence> {
    (now_ms.saturating_sub(completed_at_ms) < COMPLETED_UPLOAD_RECEIPT_WINDOW_MS)
        .then(|| CompletedUploadEvidence::for_completed_session(content.clone(), completed_at_ms))
}

/// Resolves a completion against a session that has already ended: a replay
/// of the same content succeeds idempotently, and anything else returns the
/// error for that outcome.
fn replay_terminal_completion(
    outcome: &UploadOutcome,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    expected: Option<&ContentRef>,
    mode: UploadMode,
    now_ms: u64,
) -> Result<CompletedUpload> {
    match outcome {
        UploadOutcome::Aborted { .. } => Err(CoreError::UploadNotFound {
            upload_id: upload_id.clone(),
        }),
        UploadOutcome::Completed {
            completed_at_ms,
            content_ref,
        } => {
            if expected.is_some_and(|expected| expected != content_ref) {
                return Err(CoreError::UploadAlreadyCompleted {
                    upload_id: upload_id.clone(),
                });
            }
            Ok(completed_upload(
                namespace_id,
                upload_id,
                content_ref,
                mode,
                *completed_at_ms,
                now_ms,
            ))
        }
    }
}

/// Information needed to verify an upload before completion.
enum CompletionPlan<'a> {
    /// Content the server wrote for a service-proxied upload.
    Proxied { session: &'a UploadSessionPayload },
    /// Directly uploaded content that must be checked at the provider.
    DirectPut { requested: ContentRef },
    /// Parts the provider must assemble and verify.
    DirectMultipart {
        requested: ContentRef,
        provider_upload_id: &'a str,
        checksum_algorithm: ChecksumAlgorithm,
        parts: &'a [CompletedUploadPart],
    },
}

impl CompletionPlan<'_> {
    /// Content a repeated direct completion must match.
    fn expected_completed_content(&self) -> Option<&ContentRef> {
        match self {
            Self::Proxied { .. } => None,
            Self::DirectPut { requested, .. } | Self::DirectMultipart { requested, .. } => {
                Some(requested)
            }
        }
    }
}

/// Validates a completion request against the session mode.
fn completion_plan<'a>(
    session: &'a UploadSessionPayload,
    completion: &'a CompleteUploadBody,
) -> Result<CompletionPlan<'a>> {
    match (&session.mode, completion) {
        (UploadSessionMode::ServiceProxied {}, CompleteUploadBody::ServiceProxied {}) => {
            Ok(CompletionPlan::Proxied { session })
        }
        (
            UploadSessionMode::DirectPut { checksum_algorithm },
            CompleteUploadBody::DirectPut { content },
        ) => Ok(CompletionPlan::DirectPut {
            requested: claimed_content_ref(
                session.namespace_id.clone(),
                session.content_id.clone(),
                content,
                *checksum_algorithm,
            )?,
        }),
        (
            UploadSessionMode::DirectMultipart {
                provider_upload_id,
                checksum_algorithm,
                ..
            },
            CompleteUploadBody::DirectMultipart { content, parts },
        ) => Ok(CompletionPlan::DirectMultipart {
            requested: claimed_content_ref(
                session.namespace_id.clone(),
                session.content_id.clone(),
                content,
                *checksum_algorithm,
            )?,
            provider_upload_id,
            checksum_algorithm: *checksum_algorithm,
            parts,
        }),
        _ => Err(CoreError::InvalidUploadContent(format!(
            "completion mode `{}` does not match stored upload mode `{}`",
            completion.mode().as_str(),
            upload_mode(&session.mode).as_str()
        ))),
    }
}

/// Verifies the uploaded object and returns the content reference that may
/// be recorded as completed.
///
/// A proxied upload's reference comes from the stored size and checksum of
/// the object the server wrote. Direct uploads verify the provider-stored
/// object against the client's claim, because the bytes bypassed this
/// server. Both use one metadata request, without downloading the object.
async fn verify_completion<S: ObjectStore + ?Sized>(
    store: &S,
    plan: CompletionPlan<'_>,
) -> Result<ContentRef> {
    match plan {
        CompletionPlan::Proxied { session } => find_proxied_content_ref(store, session)
            .await?
            .ok_or_else(|| {
                CoreError::InvalidUploadContent("upload content has not been staged".to_owned())
            }),
        CompletionPlan::DirectPut { requested } => {
            verify_durable_content_checksum(store, &requested)
                .await
                .map_err(content_failure)?;
            Ok(requested)
        }
        CompletionPlan::DirectMultipart {
            requested,
            provider_upload_id,
            checksum_algorithm,
            parts,
        } => {
            assemble_multipart_upload(
                store,
                provider_upload_id,
                checksum_algorithm,
                parts,
                &requested,
            )
            .await?;
            Ok(requested)
        }
    }
}

/// Asks the provider to assemble the uploaded parts, then verifies the
/// object at the target key.
///
/// Providers differ in whether they enforce the whole-object checksum and in
/// how they respond to an upload that is already consumed, so the stored object
/// is the only evidence. The same check recovers a completion whose response
/// was lost.
async fn assemble_multipart_upload<S: ObjectStore + ?Sized>(
    store: &S,
    provider_upload_id: &str,
    checksum_algorithm: ChecksumAlgorithm,
    parts: &[CompletedUploadPart],
    expected: &ContentRef,
) -> Result<()> {
    let parts = multipart_parts(parts, checksum_algorithm)?;
    let object_key = content_blob(&expected.owner_namespace_id, &expected.content_id);
    store
        .complete_multipart_upload(&object_key, provider_upload_id, &parts, &expected.checksum)
        .await
        .map_err(|error| CoreError::store(&object_key, &error))?;
    verify_durable_content_checksum(store, expected)
        .await
        .map_err(content_failure)
}

/// Classifies a content-verification failure.
///
/// A confirmed absence, length mismatch, or checksum mismatch rejects the
/// completion claim. A storage access failure is returned unchanged. Neither
/// changes the session, so completion can be retried.
fn content_failure(error: DurableContentValidationError) -> CoreError {
    match error {
        DurableContentValidationError::Store { .. } => CoreError::DurableContent(error),
        error => CoreError::InvalidUploadContent(error.to_string()),
    }
}
