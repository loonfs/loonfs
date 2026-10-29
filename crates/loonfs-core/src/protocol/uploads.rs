//! Durable upload sessions for starting, staging, completing, and aborting
//! content uploads, including direct-to-provider transfers.
//!
//! A session starts open and ends as either completed or aborted. The
//! put-if-absent that creates its outcome record determines the result (see
//! [`crate::upload_records`]). Provider cleanup runs only after that durable
//! transition.
//!
//! Every uploaded content object is created through a session. Before
//! metadata references the object, the session record gives garbage
//! collection a durable owner and status.

use crate::authorize::{Authorizer, CommitAuthority};
use crate::context::MutationContext;
use crate::control_update::create_control_object_under_generated_id;
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::limits::{
    COMPLETED_UPLOAD_ADMISSION_WINDOW_MS, COMPLETED_UPLOAD_RECEIPT_WINDOW_MS, MAX_MULTIPART_PARTS,
    MAX_MULTIPART_PART_BYTES, MAX_SIGNED_PARTS_PER_REQUEST, MIN_MULTIPART_PART_BYTES,
    UPLOAD_SESSION_LEASE_MS,
};
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::store::NamespaceStore;
use crate::storage::content::{
    abort_unpublished_multipart_upload, delete_unpublished_content_object,
    open_content_import_reader, stage_bytes_under_content_id, stage_streamed_under_content_id,
    verify_durable_content_checksum, ContentLocation, DurableContentValidationError,
    StreamedPayloadKind,
};
use crate::storage::content_admission::{CompletedUploadEvidence, PreparedContent};
use crate::upload_records::{OutcomeCreation, UploadSessionState};
use bytes::Bytes;
use loonfs_objectstore::keys::{content_blob, upload_session};
use loonfs_objectstore::{
    ByteStream, MultipartPart, ObjectStore, ObjectStoreError, PROVIDER_MULTIPART_PART_BYTES,
};
use loonfs_types::api::v0::{
    CompleteUploadBody, CompletedUploadPart, UploadContentClaim, UploadMode,
    UploadPartChecksumClaim, UploadSession, UploadSessionStatus,
};
use loonfs_types::format::control::{
    encode_control_state, ControlObjectKind, UploadAlreadyCompletedError, UploadOutcome,
    UploadSessionMode, UploadSessionPayload,
};
use loonfs_types::options::DirectMultipartUploadOptions;
use loonfs_types::{
    Checksum, ChecksumAlgorithm, ContentId, ContentRef, ContentRefKind, NamespaceAccess,
    NamespaceId, Subject, SubjectId, UploadId,
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

/// One part a server integration is about to sign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartTarget {
    pub part_number: u32,
    pub checksum: Checksum,
}

/// Everything a server integration needs to sign one wave of part uploads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartTargets {
    pub object_key: String,
    pub provider_upload_id: String,
    pub parts: Vec<MultipartPartTarget>,
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Starts a service-proxied upload session. Direct transports use
    /// [`Self::begin_direct_put_upload_target`] or [`Self::begin_direct_multipart_upload_target`].
    pub async fn begin_upload(&self) -> Result<UploadSession> {
        let context = self.mutation_context();
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
            &context,
        )
        .await
    }

    /// Starts a direct PUT upload and assigns its content identity.
    pub async fn begin_direct_put_upload_target(&self) -> Result<DirectPutUploadTarget> {
        let context = self.mutation_context();
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
            &context,
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
        let context = self.mutation_context();
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
        let session = match create_upload_session(
            &self.namespace.store,
            &catalog,
            recorded,
            session,
            &context,
        )
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
        let now_ms = self.now_ms();
        self.namespace
            .upload_proxied_content(
                upload_id,
                self.subject.as_ref(),
                ProxiedPayload::Bytes(bytes),
                now_ms,
            )
            .await
    }

    /// Uploads content that arrives as a stream into an upload session,
    /// hashing it on the way through instead of holding it.
    pub async fn upload_content_stream(
        &self,
        upload_id: &UploadId,
        body: ByteStream,
    ) -> Result<UploadSession> {
        let _permit = self.namespace.content_writes.acquire().await;
        let now_ms = self.now_ms();
        self.namespace
            .upload_proxied_content(
                upload_id,
                self.subject.as_ref(),
                ProxiedPayload::Stream(body),
                now_ms,
            )
            .await
    }

    /// Completes an upload session and returns time-bounded proof for later
    /// publication. The caller passes the catalog it already holds, which
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
        let context = self.mutation_context();
        let catalog = self.namespace.ensure_upload_namespace_available().await?;
        authorize_upload_subject(
            &self.namespace.namespace_id,
            catalog.access(),
            self.subject.as_ref(),
        )?;
        let now_ms = context.now_ms;
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
            .record_completion(&loaded.session, verified, now_ms)
            .await
    }

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
        let context = self.mutation_context();
        let session =
            open_owned_staging_session(&self.namespace.store, catalog, subject_id, &context)
                .await?;
        let stored = stage_bytes_under_content_id(
            &self.namespace.store,
            catalog.namespace_id().clone(),
            session.content_id.clone(),
            bytes,
        )
        .await?;
        self.namespace
            .complete_owned_staging(&session, stored.content_ref, &context)
            .await
    }

    /// Stages a streamed payload as content a session owns, hashing it on
    /// the way through instead of holding it.
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
                StreamedPayloadKind::Request,
                &self.mutation_context(),
            )
            .await
    }

    /// Imports existing content under a fresh identity owned by this
    /// namespace.
    ///
    /// A content reference locates bytes but does not identify the namespace
    /// whose upload session keeps them alive. This streams the source bytes
    /// chunk by chunk into a new local upload session, verifying the claimed
    /// size and checksum against what was staged, so collection in the source
    /// namespace cannot invalidate a later publication here. `owner_location`
    /// is where the owner's view finds the bytes; without one, they are read
    /// from the content object.
    pub async fn import_content_ref(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        subject_id: Option<&SubjectId>,
        content_ref: &ContentRef,
        owner_location: Option<ContentLocation>,
    ) -> Result<PreparedContent>
    where
        S: Clone + 'static,
    {
        let catalog = self.own_catalog(catalog);
        let context = self.mutation_context();
        let location = match owner_location {
            Some(location) => location,
            None => ContentLocation::whole(content_ref),
        };
        let body =
            open_content_import_reader(self.namespace.store.clone(), location, content_ref).await?;
        let _permit = self.namespace.content_writes.acquire().await;
        self.namespace
            .stage_owned_stream(
                catalog,
                subject_id,
                body,
                StreamedPayloadKind::ContentImport,
                &context,
            )
            .await
    }

    /// Aborts an upload session, then deletes the content object it owned
    /// and cleans up its provider state.
    ///
    /// Cleanup starts only after the durable transition. Terminal and
    /// idempotent: repeating it succeeds and returns the original abort
    /// timestamp, and it refuses a session that already completed, whose
    /// content may be published.
    pub async fn abort_upload(&self, upload_id: &UploadId) -> Result<UploadSession> {
        let catalog = self.namespace.load_catalog_entry().await?;
        let context = self.mutation_context();
        let namespace_id = catalog.namespace_id();
        authorize_upload_subject(namespace_id, catalog.access(), self.subject.as_ref())?;
        let loaded = self.namespace.load_upload_session_state(upload_id).await?;
        ensure_session_subject(&loaded.session, self.subject.as_ref())?;
        let aborted_at_ms = match &loaded.outcome {
            Some(outcome) => repeated_abort(outcome, upload_id)?,
            None => {
                let outcome = UploadOutcome::Aborted {
                    aborted_at_ms: context.now_ms,
                };
                match self
                    .namespace
                    .create_upload_outcome(&loaded.session, &outcome)
                    .await?
                {
                    OutcomeCreation::Created => context.now_ms,
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

/// Validates the multipart part size against provider limits.
///
/// The selected part size also bounds the maximum object size because the
/// provider accepts at most [`MAX_MULTIPART_PARTS`] parts.
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

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Validates a group of multipart parts and returns the information needed
    /// to sign their upload URLs.
    ///
    /// Part state remains client-managed. This method writes no durable state.
    pub async fn direct_multipart_part_targets(
        &self,
        upload_id: &UploadId,
        requested: &[UploadPartChecksumClaim],
    ) -> Result<MultipartPartTargets> {
        let now_ms = self.now_ms();
        let catalog = self.namespace.ensure_upload_namespace_available().await?;
        authorize_upload_subject(
            &self.namespace.namespace_id,
            catalog.access(),
            self.subject.as_ref(),
        )?;
        if requested.is_empty() {
            return Err(CoreError::InvalidUploadContent(
                "a part-signing request names at least one part".to_owned(),
            ));
        }
        if requested.len() > MAX_SIGNED_PARTS_PER_REQUEST {
            return Err(CoreError::InvalidUploadContent(format!(
                "a part-signing request names at most {MAX_SIGNED_PARTS_PER_REQUEST} parts"
            )));
        }
        let state = self.namespace.load_upload_session_state(upload_id).await?;
        ensure_session_subject(&state.session, self.subject.as_ref())?;
        ensure_session_open(&state, now_ms)?;
        let session = &state.session;
        let (provider_upload_id, checksum_algorithm) = multipart_session_upload(session)?;

        let mut parts = Vec::with_capacity(requested.len());
        for claim in requested {
            // The only bound is the provider's own part-number range: the
            // session never learned how long the payload would be, so there is
            // no part count to check against.
            if claim.part_number == 0 || claim.part_number > MAX_MULTIPART_PARTS {
                return Err(CoreError::InvalidUploadContent(format!(
                    "part {} is outside the provider's 1..={MAX_MULTIPART_PARTS} part range",
                    claim.part_number
                )));
            }
            parts.push(MultipartPartTarget {
                part_number: claim.part_number,
                checksum: validate_upload_checksum(&claim.checksum, checksum_algorithm)?.clone(),
            });
        }

        Ok(MultipartPartTargets {
            object_key: content_blob(&session.namespace_id, &session.content_id),
            provider_upload_id: provider_upload_id.to_owned(),
            parts,
        })
    }

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

/// Returns the provider upload ID for a direct multipart session.
///
/// Other upload modes return an invalid-upload error.
fn multipart_session_upload(session: &UploadSessionPayload) -> Result<(&str, ChecksumAlgorithm)> {
    match &session.mode {
        UploadSessionMode::DirectMultipart {
            provider_upload_id,
            checksum_algorithm,
            ..
        } => Ok((provider_upload_id, *checksum_algorithm)),
        UploadSessionMode::ServiceProxied {} | UploadSessionMode::DirectPut { .. } => {
            Err(CoreError::InvalidUploadContent(
                "this upload session is not a direct_multipart upload".to_owned(),
            ))
        }
    }
}

/// Builds a content reference from a client's upload claim.
///
fn claimed_content_ref(
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

fn validate_upload_checksum(
    checksum: &Checksum,
    required_algorithm: ChecksumAlgorithm,
) -> Result<&Checksum> {
    if checksum.algorithm() != required_algorithm {
        return Err(CoreError::InvalidUploadContent(format!(
            "checksum algorithm `{}` does not match the session requirement `{required_algorithm}`",
            checksum.algorithm()
        )));
    }
    Ok(checksum)
}

/// Turns a client's part bookkeeping into what the provider assembles from.
fn multipart_parts(
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

/// Values fixed when an upload session is created.
struct NewUploadSession {
    /// The content object this session will write, allocated up front.
    content_id: ContentId,
    /// How the bytes will reach it.
    mode: UploadSessionMode,
}

impl NewUploadSession {
    fn service_proxied() -> Self {
        Self {
            content_id: ContentId::generate(),
            mode: UploadSessionMode::ServiceProxied {},
        }
    }

    fn direct_put(content_id: ContentId, checksum_algorithm: ChecksumAlgorithm) -> Self {
        Self {
            content_id,
            mode: UploadSessionMode::DirectPut { checksum_algorithm },
        }
    }

    /// A multipart session records identity, the provider handle, and the
    /// geometry — and nothing about the payload, which it has not been told.
    fn direct_multipart(
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

async fn create_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    subject_id: Option<SubjectId>,
    session: NewUploadSession,
    context: &MutationContext,
) -> Result<UploadSession> {
    let session =
        create_upload_session_record(store, catalog, subject_id, session, context).await?;
    let state = UploadSessionState {
        session,
        outcome: None,
    };
    Ok(session_response(&state, None, store.checksum_algorithm()))
}

/// Writes the record that starts a new upload session, with put-if-absent.
async fn create_upload_session_record<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    subject_id: Option<SubjectId>,
    session: NewUploadSession,
    context: &MutationContext,
) -> Result<UploadSessionPayload> {
    let upload_id = UploadId::generate();
    let record = UploadSessionPayload {
        namespace_id: catalog.namespace_id().clone(),
        upload_id: upload_id.clone(),
        content_id: session.content_id,
        subject_id,
        mode: session.mode,
        expires_at_ms: context.now_ms.saturating_add(UPLOAD_SESSION_LEASE_MS),
    };
    let object_key = upload_session(catalog.namespace_id(), &upload_id);
    let encoded = encode_control_state(ControlObjectKind::UploadSession, &record)
        .expect("an upload session record should encode");
    create_control_object_under_generated_id(store, &object_key, Bytes::from(encoded)).await?;
    Ok(record)
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

fn authorize_upload_subject(
    namespace_id: &NamespaceId,
    access: &NamespaceAccess,
    subject: Option<&Subject>,
) -> Result<()> {
    Authorizer::for_request(namespace_id, access, CommitAuthority::Subject(subject)).map(|_| ())
}

fn ensure_session_subject(session: &UploadSessionPayload, subject: Option<&Subject>) -> Result<()> {
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
    async fn ensure_upload_namespace_available(&self) -> Result<VerifiedNamespaceCatalogEntry> {
        let manifest = self.load_current_manifest().await?;
        let head =
            crate::namespace::state::NamespaceReadState::from(manifest.state.envelope.payload());
        crate::namespace::control::ensure_namespace_live(&head)?;
        Ok(VerifiedNamespaceCatalogEntry::from_head(&head))
    }

    /// Checks, just before a streamed write sends its final request, that the
    /// session is still open.
    ///
    /// A session aborted after this check keeps its records for a grace period
    /// longer than the final request can take, so a later cleanup removes any
    /// object the request writes.
    async fn ensure_session_still_open(&self, upload_id: &UploadId, now_ms: u64) -> Result<()> {
        let state = self.load_upload_session_state(upload_id).await?;
        ensure_session_open(&state, now_ms)
    }

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
        now_ms: u64,
    ) -> Result<UploadSession> {
        let catalog = self.ensure_upload_namespace_available().await?;
        authorize_upload_subject(&self.namespace_id, catalog.access(), subject)?;
        let loaded = self
            .load_open_proxied_session(upload_id, subject, now_ms)
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
                    self.ensure_session_still_open(upload_id, now_ms),
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
        ensure_session_open(&state, now_ms)?;
        Ok(session_response(
            &state,
            Some(content_ref),
            self.store.checksum_algorithm(),
        ))
    }

    /// Stages a payload this runtime forwards under a session that owns it.
    ///
    /// The streaming twin of [`stage_owned_bytes`]: the bytes are hashed on
    /// their way to the store rather than held, and everything about ownership
    /// is identical.
    pub(crate) async fn stage_owned_stream(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        subject_id: Option<&SubjectId>,
        body: ByteStream,
        payload_kind: StreamedPayloadKind,
        context: &MutationContext,
    ) -> Result<PreparedContent> {
        let session = open_owned_staging_session(&self.store, catalog, subject_id, context).await?;
        let staged = stage_streamed_under_content_id(
            &self.store,
            catalog.namespace_id().clone(),
            session.content_id.clone(),
            body,
            payload_kind,
            self.ensure_session_still_open(&session.upload_id, context.now_ms),
        )
        .await?;
        if staged.already_present {
            // The identity is 128 fresh random bits and this session has made no
            // earlier attempt, so an occupied key is corruption rather than a
            // replay, and it fails loudly.
            return Err(CoreError::NamespaceCorrupt(format!(
                "content object `{}` already holds bytes under a freshly minted identity",
                content_blob(
                    &staged.content_ref.owner_namespace_id,
                    &staged.content_ref.content_id
                )
            )));
        }
        self.complete_owned_staging(&session, staged.content_ref, context)
            .await
    }
}

/// Refuses an operation that needs an open session when the session is
/// completed, aborted, or open at or past its expiry.
fn ensure_session_open(state: &UploadSessionState, now_ms: u64) -> Result<()> {
    let upload_id = state.session.upload_id.clone();
    match &state.outcome {
        None if now_ms < state.session.expires_at_ms => Ok(()),
        Some(UploadOutcome::Completed { .. }) => {
            Err(CoreError::UploadAlreadyCompleted { upload_id })
        }
        None | Some(UploadOutcome::Aborted { .. }) => Err(CoreError::UploadNotFound { upload_id }),
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    async fn load_open_proxied_session(
        &self,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        now_ms: u64,
    ) -> Result<UploadSessionState> {
        let state = self.load_upload_session_state(upload_id).await?;
        ensure_session_subject(&state.session, subject)?;
        ensure_session_open(&state, now_ms)?;
        if !matches!(state.session.mode, UploadSessionMode::ServiceProxied {}) {
            return Err(CoreError::InvalidUploadContent(format!(
                "{} sessions must be completed after using the presigned URLs",
                upload_mode(&state.session.mode).as_str()
            )));
        }
        Ok(state)
    }

    /// Records a verified content reference as the session's completed outcome.
    ///
    /// All upload modes use this transition. Completion makes the content
    /// eligible for publication and later garbage collection.
    async fn record_completion(
        &self,
        session: &UploadSessionPayload,
        verified: ContentRef,
        now_ms: u64,
    ) -> Result<CompletedUpload> {
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

/// Creates the internal upload session that owns an in-process content
/// write.
async fn open_owned_staging_session<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    subject_id: Option<&SubjectId>,
    context: &MutationContext,
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
        context,
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
        session: &UploadSessionPayload,
        written: ContentRef,
        context: &MutationContext,
    ) -> Result<PreparedContent> {
        Ok(self
            .record_completion(session, written, context.now_ms)
            .await?
            .prepared)
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
    owner_namespace_id: NamespaceId,
    content_id: ContentId,
    provider_multipart_upload_id: Option<String>,
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

/// A session with the server-side details needed for fresh capabilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadSessionView {
    pub session: UploadSession,
    /// Fresh evidence for a completed session inside its minting window.
    pub evidence: Option<CompletedUploadEvidence>,
    /// Object key of an open `direct_put` session, for the server to presign.
    pub direct_put_object_key: Option<String>,
}

/// Builds the reference of a proxied session's content object from the
/// object's stored size and checksum, or returns `None` if no upload has
/// written it yet.
///
/// Only this session writes the object, with a create that the store makes
/// conditional, so the stored object contains the bytes that one request
/// uploaded.
async fn find_proxied_content_ref<S: ObjectStore + ?Sized>(
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

/// Builds the wire view of a session. `uploaded` is the content reference
/// an open proxied session reports once its bytes are written.
fn session_response(
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

/// What a completed upload hands back: the wire response, the in-process
/// admission a same-process publication uses, and the evidence a remote one
/// carries back.
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

fn completed_status(content_ref: &ContentRef, completed_at_ms: u64) -> UploadSessionStatus {
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
fn evidence_within_window(
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

fn upload_mode(mode: &UploadSessionMode) -> UploadMode {
    match mode {
        UploadSessionMode::ServiceProxied {} => UploadMode::ServiceProxied,
        UploadSessionMode::DirectPut { .. } => UploadMode::DirectPut,
        UploadSessionMode::DirectMultipart { .. } => UploadMode::DirectMultipart,
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
/// how they answer an upload that is already consumed, so the stored object
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

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::keys::{upload_outcome, upload_session_prefix};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::PutMode;
    use loonfs_test_support::stores::{
        BlockingStore, FailStore, InjectedError, KeyPredicate, OperationSelector,
        RecordedOperation, RecordingStore,
    };
    use std::assert_matches;
    use tempfile::tempdir;

    const BYTES: &[u8] = b"terminal states\n";

    #[tokio::test]
    async fn a_failed_final_session_read_publishes_no_content() {
        use futures::StreamExt;
        use std::sync::Arc;

        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("failed-session-check").expect("namespace id");
        let context = context(1_000);
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
        let session = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .engine()
            .begin_upload()
            .await
            .expect("begin upload");
        let loaded = crate::test_support::namespace_store(&store, &namespace_id)
            .load_upload_session_state(&session.upload_id)
            .await
            .expect("load session");
        let session_key = upload_session(&namespace_id, &session.upload_id);
        let failing = Arc::new(FailStore::new(
            store,
            KeyPredicate::exact(&session_key),
            OperationSelector::Read,
            InjectedError::Transport("session read failed".to_owned()),
        ));
        let recording = Arc::new(RecordingStore::new(
            Arc::clone(&failing),
            KeyPredicate::prefix(upload_session_prefix(&namespace_id)),
        ));
        let body = {
            let failing = Arc::clone(&failing);
            let recording = Arc::clone(&recording);
            futures::stream::once(async move {
                recording.reset();
                failing.fail_next(1);
                Ok(Bytes::from_static(BYTES))
            })
            .boxed()
        };

        let error = crate::test_support::NamespaceFixture::new(
            &recording,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 1_000),
        )
        .engine()
        .upload_content_stream(&session.upload_id, body)
        .await
        .expect_err("the final session read must succeed");

        assert_matches!(error, CoreError::ControlObjectLoad(_));
        assert_eq!(recording.count(OperationSelector::Read), 2);
        assert_eq!(recording.counts().puts, 0);
        assert!(recording
            .get(
                &content_blob(&namespace_id, &loaded.session.content_id),
                None
            )
            .await
            .expect("get content")
            .is_none());
        let loaded = crate::test_support::namespace_store(&recording, &namespace_id)
            .load_upload_session_state(&session.upload_id)
            .await
            .expect("load the open session");
        assert_eq!(loaded.outcome, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retried_upload_of_the_same_bytes_succeeds_and_other_bytes_conflict() {
        let temp_dir = tempdir().expect("tempdir");
        let store = FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::content_blob(),
            OperationSelector::Put,
            InjectedError::Transport("content write timed out".to_owned()),
        )
        .apply_then_fail();
        let namespace_id = NamespaceId::parse("retried-buffered-write").expect("namespace id");
        let context = context(1_000);
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
        let session = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .engine()
            .begin_upload()
            .await
            .expect("begin upload");

        // The write is applied, but its response is lost.
        store.fail_next(1);
        crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 1_000),
        )
        .engine()
        .upload_content(&session.upload_id, BYTES)
        .await
        .expect_err("the content write fails");
        let retry = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 1_000),
        )
        .engine()
        .upload_content(&session.upload_id, BYTES)
        .await
        .expect("a retry of the same bytes finds its own object");
        let other = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 1_000),
        )
        .engine()
        .upload_content(&session.upload_id, b"other bytes\n")
        .await
        .expect_err("other bytes conflict with the stored object");

        assert_eq!(
            retry.content_ref(),
            Some(&ContentRef::blob_v1(
                namespace_id.clone(),
                crate::test_support::namespace_store(&store, &namespace_id)
                    .load_upload_session_state(&session.upload_id)
                    .await
                    .expect("load session")
                    .session
                    .content_id,
                BYTES,
                store.checksum_algorithm(),
            ))
        );
        assert_matches!(other, CoreError::UploadContentConflict { .. });
        let completed = complete(&store, &namespace_id, &session.upload_id, &context)
            .await
            .expect("complete the upload");
        assert_eq!(completed.response.content_ref(), retry.content_ref());
    }

    #[tokio::test]
    async fn a_buffered_upload_aborted_during_its_write_is_refused_and_records_nothing() {
        let temp_dir = tempdir().expect("tempdir");
        let store = BlockingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::content_blob(),
            OperationSelector::Put,
        );
        let namespace_id = NamespaceId::parse("aborted-buffered-write").expect("namespace id");
        let setup = context(1_000);
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .create()
            .await
            .expect("bootstrap");
        let session = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .begin_upload()
            .await
            .expect("begin upload");
        store.block_next();
        let engine = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 1_000),
        )
        .engine();
        let (result, ()) = tokio::join!(engine.upload_content(&session.upload_id, BYTES), async {
            store.wait_until_blocked().await;
            abort(store.inner(), &namespace_id, &session.upload_id, &setup)
                .await
                .expect("abort during the write");
            store.release();
        });

        assert_matches!(result, Err(CoreError::UploadNotFound { .. }));
        let loaded = crate::test_support::namespace_store(&store, &namespace_id)
            .load_upload_session_state(&session.upload_id)
            .await
            .expect("load session");
        assert_matches!(loaded.outcome, Some(UploadOutcome::Aborted { .. }));
    }

    fn context(now_ms: u64) -> MutationContext {
        crate::test_support::mutation_context("upload-test", now_ms)
    }

    /// One store with a namespace and one open, staged session in it.
    async fn staged_session(
        store: &LocalFsStore,
        context: &MutationContext,
    ) -> (NamespaceId, UploadId, ContentRef, String) {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        crate::test_support::NamespaceFixture::new(store, &namespace_id, context)
            .create()
            .await
            .expect("bootstrap");
        let begin = crate::test_support::NamespaceFixture::new(store, &namespace_id, context)
            .engine()
            .begin_upload()
            .await
            .expect("begin upload");
        let staged = crate::test_support::NamespaceFixture::new(
            store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", context.now_ms),
        )
        .engine()
        .upload_content(&begin.upload_id, BYTES)
        .await
        .expect("stage upload");
        let content_key = content_blob(
            &staged
                .content_ref()
                .expect("staged content")
                .owner_namespace_id,
            &staged.content_ref().expect("staged content").content_id,
        );
        (
            namespace_id,
            begin.upload_id.clone(),
            staged.content_ref().expect("staged content").clone(),
            content_key,
        )
    }

    async fn complete<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        context: &MutationContext,
    ) -> Result<CompletedUpload> {
        crate::test_support::NamespaceFixture::new(store, namespace_id, context)
            .engine()
            .complete_upload(
                &crate::test_support::namespace_store(store, namespace_id)
                    .load_catalog_entry()
                    .await
                    .expect("load the catalog"),
                upload_id,
                CompleteUploadBody::ServiceProxied {},
            )
            .await
    }

    async fn abort<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        context: &MutationContext,
    ) -> Result<UploadSession> {
        let catalog = crate::test_support::namespace_store(store, namespace_id)
            .load_catalog_entry()
            .await?;
        crate::test_support::NamespaceFixture::new(store, catalog.namespace_id(), context)
            .engine()
            .abort_upload(upload_id)
            .await
    }

    #[tokio::test]
    async fn an_applied_outcome_put_that_fails_its_precondition_is_this_requests_own() {
        let temp_dir = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        let (namespace_id, upload_id, content_ref, _) =
            staged_session(&inner, &context(1_000)).await;
        // The provider client resends the put that created the outcome
        // record, and the resent put fails its precondition because of the
        // first one.
        let store = FailStore::new(
            inner,
            KeyPredicate::exact(upload_outcome(&namespace_id, &upload_id)),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::PreconditionFailed,
        )
        .apply_then_fail();
        store.fail_next(1);

        let completed = complete(&store, &namespace_id, &upload_id, &context(2_000))
            .await
            .expect("the outcome under the session is this request's own");

        assert_eq!(completed.response.content_ref(), Some(&content_ref));
        let state = crate::test_support::namespace_store(&store, &namespace_id)
            .load_upload_session_state(&upload_id)
            .await
            .expect("session");
        assert_eq!(
            state.outcome,
            Some(UploadOutcome::Completed {
                completed_at_ms: 2_000,
                content_ref,
            })
        );
    }

    #[tokio::test]
    async fn a_completion_whose_outcome_acknowledgement_is_lost_reads_its_record_back() {
        let temp_dir = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(temp_dir.path()).expect("store");
        let (namespace_id, upload_id, content_ref, _) =
            staged_session(&inner, &context(1_000)).await;
        let key = upload_outcome(&namespace_id, &upload_id);
        let failing = FailStore::new(
            inner,
            KeyPredicate::exact(&key),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost completion acknowledgement".to_owned()),
        )
        .apply_then_fail();
        failing.fail_next(1);
        let store = RecordingStore::new(failing, KeyPredicate::exact(&key));

        let first = complete(&store, &namespace_id, &upload_id, &context(2_000))
            .await
            .expect("the read-back finds this completion's own record");
        assert_eq!(store.counts().create_if_absent_puts, 1);
        let stored = store
            .get_with_metadata(&key)
            .await
            .expect("read outcome")
            .expect("outcome");

        store.reset();
        let replay = complete(&store, &namespace_id, &upload_id, &context(3_000))
            .await
            .expect("replay completion");
        assert_eq!(replay.response, first.response);
        assert_eq!(replay.response.content_ref(), Some(&content_ref));
        assert_matches!(
            replay.response.status,
            UploadSessionStatus::Completed {
                completed_at_ms: 2_000,
                ..
            }
        );
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().deletes, 0);
        assert_eq!(
            store
                .get_with_metadata(&key)
                .await
                .expect("read replayed outcome")
                .expect("outcome"),
            stored
        );
    }

    /// Returns how many times each key under the uploads prefix was written.
    fn writes_per_key(store: &RecordingStore<LocalFsStore>) -> Vec<(String, usize)> {
        let mut writes = std::collections::BTreeMap::new();
        for operation in store.snapshot() {
            if matches!(
                operation,
                RecordedOperation::Put { .. } | RecordedOperation::CompareAndSwap { .. }
            ) {
                *writes.entry(operation.key().to_owned()).or_insert(0) += 1;
            }
        }
        writes.into_iter().collect()
    }

    #[tokio::test]
    async fn every_upload_writes_its_session_record_and_its_outcome_record_once() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        crate::test_support::NamespaceFixture::new(
            &LocalFsStore::new(temp_dir.path()).expect("store"),
            &namespace_id,
            &setup,
        )
        .create()
        .await
        .expect("bootstrap");
        let store = RecordingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            KeyPredicate::prefix(upload_session_prefix(&namespace_id)),
        );
        let catalog = crate::test_support::namespace_store(&store, &namespace_id)
            .load_catalog_entry()
            .await
            .expect("catalog");

        let proxied = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .begin_upload()
            .await
            .expect("begin proxied upload");
        crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 1_000),
        )
        .engine()
        .upload_content(&proxied.upload_id, BYTES)
        .await
        .expect("upload proxied bytes");
        complete(&store, &namespace_id, &proxied.upload_id, &setup)
            .await
            .expect("complete proxied upload");

        let direct = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .begin_direct_put_upload_target()
            .await
            .expect("begin direct put");
        store
            .put(
                &direct.object_key,
                Bytes::from_static(BYTES),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("write direct-put bytes");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .complete_upload(
                &crate::test_support::namespace_store(&store, &namespace_id)
                    .load_catalog_entry()
                    .await
                    .expect("load the catalog"),
                &direct.session.upload_id,
                CompleteUploadBody::DirectPut {
                    content: UploadContentClaim {
                        size_bytes: BYTES.len() as u64,
                        checksum: Checksum::crc64nvme(BYTES),
                    },
                },
            )
            .await
            .expect("complete direct put");

        let prepared =
            crate::test_support::NamespaceFixture::new(&store, catalog.namespace_id(), &setup)
                .engine()
                .stage_owned_bytes(&catalog, None, BYTES)
                .await
                .expect("stage owned bytes");
        let owned = prepared.upload_id().expect("owned staging session").clone();

        let mut expected = Vec::new();
        for upload_id in [&proxied.upload_id, &direct.session.upload_id, &owned] {
            expected.push((upload_session(&namespace_id, upload_id), 1));
            expected.push((upload_outcome(&namespace_id, upload_id), 1));
        }
        expected.sort();
        assert_eq!(writes_per_key(&store), expected);
        assert_eq!(store.counts().compare_and_swaps, 0);
    }

    #[test]
    fn multipart_validation_uses_the_algorithm_frozen_in_the_session() {
        let session = UploadSessionPayload {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            upload_id: UploadId::parse("upl_00000000000000000000000001").expect("upload id"),
            content_id: ContentId::parse("con_00000000000000000000000001").expect("content id"),
            subject_id: None,
            mode: UploadSessionMode::DirectMultipart {
                provider_upload_id: "provider-upload".to_owned(),
                part_size_bytes: NonZeroU64::new(8 * 1024 * 1024).expect("part size"),
                // Deliberately not the process default. This models a
                // session reopened after that default changed.
                checksum_algorithm: ChecksumAlgorithm::Crc32c,
            },
            expires_at_ms: 2,
        };
        let (_, required_algorithm) =
            multipart_session_upload(&session).expect("multipart session");
        assert_eq!(required_algorithm, ChecksumAlgorithm::Crc32c);

        let content = UploadContentClaim {
            size_bytes: 7,
            checksum: Checksum::crc32c(b"payload"),
        };
        let content_ref = claimed_content_ref(
            session.namespace_id.clone(),
            session.content_id.clone(),
            &content,
            required_algorithm,
        )
        .expect("the stored algorithm accepts the content claim");
        assert_eq!(content_ref.checksum.algorithm(), ChecksumAlgorithm::Crc32c);

        let part = CompletedUploadPart {
            part_number: 1,
            etag: "etag".to_owned(),
            checksum: Checksum::crc32c(b"payload"),
        };
        assert_eq!(
            multipart_parts(&[part], required_algorithm)
                .expect("the stored algorithm accepts the part")[0]
                .checksum
                .algorithm(),
            ChecksumAlgorithm::Crc32c
        );

        let wrong_part = CompletedUploadPart {
            part_number: 1,
            etag: "etag".to_owned(),
            checksum: Checksum::crc64nvme(b"payload"),
        };
        assert_matches!(
            multipart_parts(&[wrong_part], required_algorithm)
                .expect_err("a part checksum in another algorithm should be rejected"),
            CoreError::InvalidUploadContent(_)
        );
        let wrong_signing_claim = UploadPartChecksumClaim {
            part_number: 1,
            checksum: Checksum::crc64nvme(b"payload"),
        };
        assert_matches!(
            validate_upload_checksum(&wrong_signing_claim.checksum, required_algorithm)
                .expect_err("a signing claim in another algorithm should be rejected"),
            CoreError::InvalidUploadContent(_)
        );
        let wrong_content = UploadContentClaim {
            size_bytes: 7,
            checksum: Checksum::crc64nvme(b"payload"),
        };
        assert_matches!(
            claimed_content_ref(
                session.namespace_id.clone(),
                session.content_id,
                &wrong_content,
                required_algorithm
            )
            .expect_err("a content claim in another algorithm should be rejected"),
            CoreError::InvalidUploadContent(_)
        );
    }

    #[tokio::test]
    async fn a_direct_put_requires_the_session_algorithm_and_replays_completion() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .create()
            .await
            .expect("bootstrap");
        let begin = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .begin_direct_put_upload_target()
            .await
            .expect("begin direct put");

        let wrong_algorithm =
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(2_000))
                .engine()
                .complete_upload(
                    &crate::test_support::namespace_store(&store, &namespace_id)
                        .load_catalog_entry()
                        .await
                        .expect("load the catalog"),
                    &begin.session.upload_id,
                    CompleteUploadBody::DirectPut {
                        content: UploadContentClaim {
                            size_bytes: BYTES.len() as u64,
                            checksum: Checksum::crc32c(BYTES),
                        },
                    },
                )
                .await
                .expect_err("a different checksum algorithm is invalid content");
        assert_matches!(wrong_algorithm, CoreError::InvalidUploadContent(_));
        let open = crate::test_support::namespace_store(&store, &namespace_id)
            .load_upload_session_state(&begin.session.upload_id)
            .await
            .expect("session remains readable");
        assert_eq!(open.outcome, None);

        store
            .put(
                &begin.object_key,
                Bytes::from_static(BYTES),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("write direct-put bytes");
        let completion = CompleteUploadBody::DirectPut {
            content: UploadContentClaim {
                size_bytes: BYTES.len() as u64,
                checksum: Checksum::crc64nvme(BYTES),
            },
        };
        let first =
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(3_000))
                .engine()
                .complete_upload(
                    &crate::test_support::namespace_store(&store, &namespace_id)
                        .load_catalog_entry()
                        .await
                        .expect("load the catalog"),
                    &begin.session.upload_id,
                    completion.clone(),
                )
                .await
                .expect("complete direct put");
        let replay =
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(3_000))
                .engine()
                .complete_upload(
                    &crate::test_support::namespace_store(&store, &namespace_id)
                        .load_catalog_entry()
                        .await
                        .expect("load the catalog"),
                    &begin.session.upload_id,
                    completion,
                )
                .await
                .expect("replay direct-put completion");
        assert_eq!(replay, first);
    }

    #[tokio::test]
    async fn provider_abort_failure_stops_before_object_delete() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(1_000))
            .create()
            .await
            .expect("bootstrap");
        let content_id = ContentId::generate();
        let content_key = content_blob(&namespace_id, &content_id);
        store
            .put(
                &content_key,
                Bytes::from_static(BYTES),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("write unpublished content");
        let abandoned = AbandonedUpload {
            owner_namespace_id: namespace_id.clone(),
            content_id,
            provider_multipart_upload_id: Some("unsupported-provider-upload".to_owned()),
        };

        assert!(
            !abandoned.release(&store,).await,
            "an unsupported provider abort is incomplete cleanup"
        );
        assert!(
            store
                .head(&content_key)
                .await
                .expect("head content")
                .is_some(),
            "object deletion waits until provider abort succeeds"
        );
    }

    #[tokio::test]
    async fn a_completion_after_an_abort_fails_terminally_and_touches_nothing() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, upload_id, _content_ref, content_key) =
            staged_session(&store, &setup).await;

        abort(&store, &namespace_id, &upload_id, &context(2_000))
            .await
            .expect("abort");
        assert!(store.head(&content_key).await.expect("head").is_none());

        let error = complete(&store, &namespace_id, &upload_id, &context(3_000))
            .await
            .expect_err("an aborted session cannot complete");
        assert_matches!(error, CoreError::UploadNotFound { .. });

        let state = crate::test_support::namespace_store(&store, &namespace_id)
            .load_upload_session_state(&upload_id)
            .await
            .expect("session still readable");
        assert_eq!(
            state.outcome,
            Some(UploadOutcome::Aborted {
                aborted_at_ms: 2_000
            })
        );
        assert!(store.head(&content_key).await.expect("head").is_none());
    }

    #[tokio::test]
    async fn a_mismatched_direct_put_claim_is_rejected_and_a_matching_one_completes() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .create()
            .await
            .expect("bootstrap");
        let begin = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .begin_direct_put_upload_target()
            .await
            .expect("begin direct put");
        let written = vec![b'x'; BYTES.len()];
        store
            .put(
                &begin.object_key,
                Bytes::from(written.clone()),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("write direct-put bytes");

        let error =
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(2_000))
                .engine()
                .complete_upload(
                    &crate::test_support::namespace_store(&store, &namespace_id)
                        .load_catalog_entry()
                        .await
                        .expect("load the catalog"),
                    &begin.session.upload_id,
                    CompleteUploadBody::DirectPut {
                        content: UploadContentClaim {
                            size_bytes: BYTES.len() as u64,
                            checksum: Checksum::crc64nvme(BYTES),
                        },
                    },
                )
                .await
                .expect_err("a claim the object does not match cannot complete");
        assert_matches!(error, CoreError::InvalidUploadContent(_));
        let state = crate::test_support::namespace_store(&store, &namespace_id)
            .load_upload_session_state(&begin.session.upload_id)
            .await
            .expect("session remains readable");
        assert_eq!(state.outcome, None);
        assert!(store
            .head(&begin.object_key)
            .await
            .expect("head direct-put content")
            .is_some());

        let completed =
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(3_000))
                .engine()
                .complete_upload(
                    &crate::test_support::namespace_store(&store, &namespace_id)
                        .load_catalog_entry()
                        .await
                        .expect("load the catalog"),
                    &begin.session.upload_id,
                    CompleteUploadBody::DirectPut {
                        content: UploadContentClaim {
                            size_bytes: written.len() as u64,
                            checksum: Checksum::crc64nvme(&written),
                        },
                    },
                )
                .await
                .expect("the matching claim completes the session");
        assert_eq!(
            completed
                .response
                .content_ref()
                .expect("completed content ref")
                .checksum,
            Checksum::crc64nvme(&written)
        );
    }

    #[tokio::test]
    async fn an_abort_after_completion_is_refused_and_keeps_the_content() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, upload_id, _content_ref, content_key) =
            staged_session(&store, &setup).await;
        complete(&store, &namespace_id, &upload_id, &context(2_000))
            .await
            .expect("complete");

        let error = abort(&store, &namespace_id, &upload_id, &context(3_000))
            .await
            .expect_err("a completed session cannot be aborted");
        assert_matches!(error, CoreError::UploadAlreadyCompleted { .. });
        assert!(
            store.head(&content_key).await.expect("head").is_some(),
            "a refused abort must not clean up published-able content"
        );
    }

    #[tokio::test]
    async fn a_repeated_abort_reports_the_first_stamp() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, upload_id, _content_ref, _content_key) =
            staged_session(&store, &setup).await;

        let first = abort(&store, &namespace_id, &upload_id, &context(2_000))
            .await
            .expect("first abort");
        let second = abort(&store, &namespace_id, &upload_id, &context(9_000))
            .await
            .expect("repeated abort");

        assert_eq!(first.mode, UploadMode::ServiceProxied);
        assert_matches!(
            &first.status,
            UploadSessionStatus::Aborted {
                aborted_at_ms: 2_000
            }
        );
        assert_eq!(second, first);
    }

    #[tokio::test]
    async fn staging_into_a_terminal_session_is_refused() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, upload_id, _content_ref, _content_key) =
            staged_session(&store, &setup).await;
        complete(&store, &namespace_id, &upload_id, &context(2_000))
            .await
            .expect("complete");
        let error = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 2_000),
        )
        .engine()
        .upload_content(&upload_id, BYTES)
        .await
        .expect_err("a completed session takes no more bytes");
        assert_matches!(error, CoreError::UploadAlreadyCompleted { .. });

        let aborted = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .begin_upload()
            .await
            .expect("begin a second upload");
        abort(&store, &namespace_id, &aborted.upload_id, &context(3_000))
            .await
            .expect("abort");
        let error = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 3_000),
        )
        .engine()
        .upload_content(&aborted.upload_id, BYTES)
        .await
        .expect_err("an aborted session takes no more bytes");
        assert_matches!(error, CoreError::UploadNotFound { .. });
    }

    #[tokio::test]
    async fn an_open_session_takes_upload_work_only_before_its_expiry() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, staged_id, _, _) = staged_session(&store, &setup).await;
        let catalog = crate::test_support::namespace_store(&store, &namespace_id)
            .load_catalog_entry()
            .await
            .expect("catalog");
        let proxied_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .begin_upload()
            .await
            .expect("begin proxied upload")
            .upload_id;
        let multipart_id = create_upload_session(
            &store,
            &catalog,
            None,
            NewUploadSession::direct_multipart(
                ContentId::generate(),
                "provider-upload",
                NonZeroU64::new(5 * 1024 * 1024).expect("part size"),
                store.checksum_algorithm(),
            ),
            &setup,
        )
        .await
        .expect("begin multipart upload")
        .upload_id;
        let part = [UploadPartChecksumClaim {
            part_number: 1,
            checksum: Checksum::crc64nvme(BYTES),
        }];
        let expires_at_ms = setup.now_ms + UPLOAD_SESSION_LEASE_MS;

        for (now_ms, open) in [(expires_at_ms, false), (expires_at_ms - 1, true)] {
            let results = [
                (
                    "staging",
                    crate::test_support::NamespaceFixture::new(
                        &store,
                        &namespace_id,
                        &crate::test_support::mutation_context("test-writer", now_ms),
                    )
                    .engine()
                    .upload_content(&proxied_id, BYTES)
                    .await
                    .map(drop),
                ),
                (
                    "part signing",
                    crate::test_support::NamespaceFixture::new(
                        &store,
                        &namespace_id,
                        &crate::test_support::mutation_context("test-writer", now_ms),
                    )
                    .engine()
                    .direct_multipart_part_targets(&multipart_id, &part)
                    .await
                    .map(drop),
                ),
                (
                    "completion",
                    complete(&store, &namespace_id, &staged_id, &context(now_ms))
                        .await
                        .map(drop),
                ),
            ];
            for (operation, result) in results {
                assert_matches!(
                    (open, &result),
                    (true, Ok(())) | (false, Err(CoreError::UploadNotFound { .. })),
                    "{operation} at {now_ms} answered {result:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn an_expired_open_session_still_reports_its_status_and_aborts() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, upload_id, _, _) = staged_session(&store, &setup).await;
        let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS);

        let view = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", expired.now_ms),
        )
        .engine()
        .get_upload(&upload_id)
        .await
        .expect("status of an expired session");
        assert_matches!(view.session.status, UploadSessionStatus::Open { .. });
        let aborted = abort(&store, &namespace_id, &upload_id, &expired)
            .await
            .expect("abort an expired session");
        assert_eq!(
            aborted.status,
            UploadSessionStatus::Aborted {
                aborted_at_ms: expired.now_ms
            }
        );
    }

    #[tokio::test]
    async fn only_a_completed_session_mints_evidence() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, upload_id, content_ref, _content_key) =
            staged_session(&store, &setup).await;

        let crate::UploadSessionView {
            session: open,
            evidence,
            ..
        } = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 1_500),
        )
        .engine()
        .get_upload(&upload_id)
        .await
        .expect("status of an open session");
        assert_matches!(open.status, UploadSessionStatus::Open { .. });
        assert!(
            evidence.is_none(),
            "an open session provides no stored checksum evidence"
        );

        complete(&store, &namespace_id, &upload_id, &context(2_000))
            .await
            .expect("complete");
        let crate::UploadSessionView {
            session: completed,
            evidence,
            ..
        } = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 2_500),
        )
        .engine()
        .get_upload(&upload_id)
        .await
        .expect("status of a completed session");
        assert_matches!(completed.status, UploadSessionStatus::Completed { .. });
        assert_eq!(
            evidence.expect("a completed session mints").content_ref(),
            &content_ref
        );

        // A second session, aborted, to check the other terminal state.
        let begin = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
            .engine()
            .begin_upload()
            .await
            .expect("begin second upload");
        abort(&store, &namespace_id, &begin.upload_id, &context(3_000))
            .await
            .expect("abort");
        let crate::UploadSessionView {
            session: aborted,
            evidence,
            ..
        } = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 3_500),
        )
        .engine()
        .get_upload(&begin.upload_id)
        .await
        .expect("status of an aborted session");
        assert_matches!(aborted.status, UploadSessionStatus::Aborted { .. });
        assert!(
            evidence.is_none(),
            "an aborted session provides no stored checksum evidence"
        );
    }

    #[tokio::test]
    async fn a_completed_session_keeps_its_content_ref_after_token_minting_closes() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let setup = context(1_000);
        let (namespace_id, upload_id, content_ref, _content_key) =
            staged_session(&store, &setup).await;
        let completed_at_ms = 2_000;
        complete(&store, &namespace_id, &upload_id, &context(completed_at_ms))
            .await
            .expect("complete");

        // Long after the first evidence would have expired, the durable
        // session still answers with usable evidence.
        let much_later = completed_at_ms + COMPLETED_UPLOAD_RECEIPT_WINDOW_MS - 1;
        let crate::UploadSessionView {
            session: _,
            evidence,
            ..
        } = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", much_later),
        )
        .engine()
        .get_upload(&upload_id)
        .await
        .expect("status inside the receipt window");
        assert_eq!(evidence.expect("still minting").content_ref(), &content_ref);

        let past = completed_at_ms + COMPLETED_UPLOAD_RECEIPT_WINDOW_MS;
        let crate::UploadSessionView {
            session: status,
            evidence,
            ..
        } = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", past),
        )
        .engine()
        .get_upload(&upload_id)
        .await
        .expect("status past the receipt window");
        let completed = match status.status {
            UploadSessionStatus::Completed {
                content_ref,
                content_token,
                ..
            } => Some((content_ref, content_token)),
            _ => None,
        };
        assert_eq!(completed, Some((content_ref.clone(), None)));
        assert!(
            evidence.is_none(),
            "past the window no evidence exists, which is what lets content GC decide"
        );

        // The same rule governs a very late idempotent completion replay.
        let replay = complete(&store, &namespace_id, &upload_id, &context(past))
            .await
            .expect("replay still succeeds");
        assert_eq!(replay.response.content_ref(), Some(&content_ref));
        assert!(replay.response.content_token().is_none());
        assert!(replay.evidence.is_none());
    }
    async fn delete_upload_namespace<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) {
        crate::test_support::NamespaceFixture::new(store, namespace_id, &context(3_000))
            .engine()
            .delete_namespace(Default::default())
            .await
            .expect("delete namespace");
    }

    #[tokio::test]
    async fn deleted_namespace_refuses_multipart_part_targets() {
        let directory = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(directory.path()).expect("store");
        let (namespace_id, _, _, _) = staged_session(&inner, &context(1_000)).await;
        let catalog = crate::test_support::namespace_store(&inner, &namespace_id)
            .load_catalog_entry()
            .await
            .expect("catalog");
        let upload_id = create_upload_session(
            &inner,
            &catalog,
            None,
            NewUploadSession::direct_multipart(
                ContentId::generate(),
                "provider-upload",
                NonZeroU64::new(5 * 1024 * 1024).expect("part size"),
                inner.checksum_algorithm(),
            ),
            &context(1_000),
        )
        .await
        .expect("multipart session")
        .upload_id;
        delete_upload_namespace(&inner, &namespace_id).await;
        let store = loonfs_test_support::stores::RecordingStore::new(inner, KeyPredicate::any());
        let error = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 4_000),
        )
        .engine()
        .direct_multipart_part_targets(
            &upload_id,
            &[UploadPartChecksumClaim {
                part_number: 1,
                checksum: Checksum::crc64nvme(BYTES),
            }],
        )
        .await
        .expect_err("deleted namespace");
        assert_matches!(error, CoreError::NamespaceDeleted { .. });
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().deletes, 0);
    }

    #[tokio::test]
    async fn deleted_namespace_refuses_proxied_content() {
        let directory = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(directory.path()).expect("store");
        let (namespace_id, upload_id, _, _) = staged_session(&inner, &context(1_000)).await;
        delete_upload_namespace(&inner, &namespace_id).await;
        let store = loonfs_test_support::stores::RecordingStore::new(inner, KeyPredicate::any());
        let error = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 4_000),
        )
        .engine()
        .upload_content(&upload_id, BYTES)
        .await
        .expect_err("deleted namespace");
        assert_matches!(error, CoreError::NamespaceDeleted { .. });
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().deletes, 0);
    }

    #[tokio::test]
    async fn deleted_namespace_refuses_upload_completion() {
        let directory = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(directory.path()).expect("store");
        let (namespace_id, upload_id, _, _) = staged_session(&inner, &context(1_000)).await;
        delete_upload_namespace(&inner, &namespace_id).await;
        let store = loonfs_test_support::stores::RecordingStore::new(inner, KeyPredicate::any());
        let error = complete(&store, &namespace_id, &upload_id, &context(4_000))
            .await
            .expect_err("deleted namespace");
        assert_matches!(error, CoreError::NamespaceDeleted { .. });
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().deletes, 0);
    }

    #[tokio::test]
    async fn deleted_namespace_refuses_upload_status_evidence() {
        let directory = tempdir().expect("tempdir");
        let inner = LocalFsStore::new(directory.path()).expect("store");
        let (namespace_id, upload_id, _, _) = staged_session(&inner, &context(1_000)).await;
        complete(&inner, &namespace_id, &upload_id, &context(2_000))
            .await
            .expect("complete");
        delete_upload_namespace(&inner, &namespace_id).await;
        let store = loonfs_test_support::stores::RecordingStore::new(inner, KeyPredicate::any());
        let error = crate::test_support::NamespaceFixture::new(
            &store,
            &namespace_id,
            &crate::test_support::mutation_context("test-writer", 4_000),
        )
        .engine()
        .get_upload(&upload_id)
        .await
        .expect_err("deleted namespace");
        assert_matches!(error, CoreError::NamespaceDeleted { .. });
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().deletes, 0);
    }
}
