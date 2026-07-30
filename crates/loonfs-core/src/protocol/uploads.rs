//! Durable upload sessions: begin, stage, and complete content uploads,
//! including direct-put targets that move bytes past the server.

use crate::context::MutationContext;
use crate::control_update::{
    try_update_upload_session, update_upload_session, UploadSessionCas, UploadSessionUpdate,
};
use crate::engine::{BeginDirectPutUploadTargetResponse, DirectPutUploadTarget};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::namespace::catalog::load_namespace_content_store_id;
use crate::namespace::control::load_namespace_head_control;
use crate::storage::content::{stage_bytes_under_content_id, verify_durable_content_checksum};
use crate::storage::content_admission::{ContentAdmission, PreparedContent};
use bytes::Bytes;
use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, CompleteUploadResponse,
    DirectPutContentClaim, UploadContentResponse, UploadMode,
};
use loonfs_api::wire::control::{
    encode_control_object, CompletedUpload, ControlObjectKind, NamespaceState,
    UploadSessionEnvelope, UploadSessionLifecycle, UploadSessionState,
};
use loonfs_api::{
    ChecksumAlgorithm, ContentId, ContentRef, ContentRefKind, ContentStoreId, NamespaceId,
    StorageChecksum, UploadId,
};
use loonfs_objectstore::keys::{content_blob, upload_session};
use loonfs_objectstore::ObjectStore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UploadSessionSweep {
    Delete,
    Retain,
}

pub(crate) async fn begin_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: BeginUploadRequest,
    context: &MutationContext,
) -> Result<BeginUploadResponse> {
    ensure_upload_namespace_available(store, namespace_id).await?;
    let mode = request.mode.unwrap_or_default();
    if mode == UploadMode::DirectPut {
        return Err(CoreError::InvalidUploadContent(
            "direct_put requires a presigned URL issuer".to_owned(),
        ));
    }
    let upload_id = create_upload_session(
        store,
        namespace_id,
        NewUploadSession::service_proxied(),
        context,
    )
    .await?;
    Ok(BeginUploadResponse {
        namespace_id: namespace_id.clone(),
        upload_id,
        mode: UploadMode::ServiceProxied,
        direct_put: None,
    })
}

/// Mints the content identity a direct upload will write to, and the
/// reference that names it.
///
/// The client declares only what it can know — how many bytes and what they
/// hash to. Identity is the server's, so a caller can never aim a presigned
/// write at an object it chose. The reference returned here is the one the
/// signed write, the completion check, and the later commit all name.
pub(crate) async fn begin_direct_put_upload_target<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    claim: DirectPutContentClaim,
    context: &MutationContext,
) -> Result<BeginDirectPutUploadTargetResponse> {
    ensure_upload_namespace_available(store, namespace_id).await?;
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let content_id = ContentId::generate();
    let content_ref = direct_put_content_ref(content_id.clone(), &claim)?;
    let object_key = content_blob(content_store_id.as_str(), &content_id);
    let upload_id = create_upload_session(
        store,
        namespace_id,
        NewUploadSession::direct_put(content_ref.clone()),
        context,
    )
    .await?;
    Ok(BeginDirectPutUploadTargetResponse {
        namespace_id: namespace_id.clone(),
        upload_id,
        target: DirectPutUploadTarget {
            content_ref,
            object_key,
        },
    })
}

/// Turns a client's claim into the reference the write is bound to.
///
/// The digest is the client's, but it stops being a client claim the moment
/// it is signed into the provider write: the provider refuses any body that
/// does not hash to it, and completion re-checks the stored object against
/// it. That is why the resulting reference may carry `whole_file_sha256`.
fn direct_put_content_ref(
    content_id: ContentId,
    claim: &DirectPutContentClaim,
) -> Result<ContentRef> {
    let storage_checksum = StorageChecksum {
        algorithm: ChecksumAlgorithm::Sha256,
        value: claim.sha256.clone(),
    };
    let content_ref = ContentRef {
        kind: ContentRefKind::BlobV1,
        content_id,
        size_bytes: claim.size_bytes,
        whole_file_sha256: Some(storage_checksum.value.clone()),
        storage_checksum,
    };
    content_ref
        .validate()
        .map_err(|err| CoreError::InvalidUploadContent(err.to_string()))?;
    Ok(content_ref)
}

/// What a session is opened with: everything decided before any byte moves.
struct NewUploadSession {
    mode: UploadMode,
    /// The content object this session will write, allocated up front.
    content_id: ContentId,
    /// What the client promised, for modes that promise anything.
    claimed_checksum: Option<StorageChecksum>,
    /// The reference a `direct_put` write is signed against.
    direct_put_content_ref: Option<ContentRef>,
}

impl NewUploadSession {
    fn service_proxied() -> Self {
        Self {
            mode: UploadMode::ServiceProxied,
            content_id: ContentId::generate(),
            claimed_checksum: None,
            direct_put_content_ref: None,
        }
    }

    fn direct_put(content_ref: ContentRef) -> Self {
        Self {
            mode: UploadMode::DirectPut,
            content_id: content_ref.content_id.clone(),
            claimed_checksum: Some(content_ref.storage_checksum.clone()),
            direct_put_content_ref: Some(content_ref),
        }
    }
}

async fn create_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    session: NewUploadSession,
    context: &MutationContext,
) -> Result<UploadId> {
    let upload_id = UploadId::generate();
    let state = UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        mode: session.mode,
        content_id: session.content_id,
        claimed_checksum: session.claimed_checksum,
        direct_put_content_ref: session.direct_put_content_ref,
        staged_content_ref: None,
        completed: None,
        created_at_ms: context.now_ms,
        state: UploadSessionLifecycle::Active,
    };
    let envelope = UploadSessionEnvelope::from_state(ControlObjectKind::UploadSession, state)
        .map_err(|err| {
            CoreError::Internal(format!("failed to build upload session envelope: {err}"))
        })?;
    let encoded = encode_control_object(&envelope).map_err(|err| {
        CoreError::Internal(format!("failed to encode upload session envelope: {err}"))
    })?;
    let object_key = upload_session(namespace_id.as_str(), upload_id.as_str());
    store
        .put_if_absent(&object_key, Bytes::from(encoded))
        .await
        .map_err(|err| CoreError::store(&object_key, &err))?;
    Ok(upload_id)
}

/// Admits an upload session only for a namespace that exists and still
/// serves writes. The head is the whole existence check: absent means the
/// namespace was never created, and the tombstone refuses.
async fn ensure_upload_namespace_available<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<()> {
    let head = load_namespace_head_control(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?
        .state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::NamespaceDeleted {
            namespace_id: namespace_id.clone(),
        });
    }
    Ok(())
}

/// Stages bytes into a service-proxied session.
///
/// The bytes land under the identity the session allocated when it began,
/// so re-sending the same bytes to the same session writes the same object
/// rather than minting a second one. Two *different* sessions carrying
/// identical bytes still get their own objects; sessions are where retry
/// idempotency lives now, not the key space.
pub(crate) async fn upload_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    bytes: &[u8],
) -> Result<UploadContentResponse> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;

    update_upload_session(
        store,
        namespace_id,
        upload_id,
        CONTENTION_RETRY_LIMIT,
        |mut state| {
            let content_store_id = content_store_id.clone();
            let namespace_id = namespace_id.clone();
            let upload_id = upload_id.to_owned();
            async move {
                if state.state == UploadSessionLifecycle::Condemned {
                    return Err(CoreError::UploadNotFound { upload_id });
                }
                if state.completed.is_some() {
                    return Err(CoreError::UploadAlreadyCompleted { upload_id });
                }
                if state.mode == UploadMode::DirectPut {
                    return Err(CoreError::InvalidUploadContent(
                        "direct_put sessions must be completed after using the presigned URL"
                            .to_owned(),
                    ));
                }

                let content_ref = ContentRef::blob_v1(state.content_id.clone(), bytes);
                if let Some(existing) = &state.staged_content_ref {
                    if existing == &content_ref {
                        return Ok(UploadSessionUpdate::Noop(UploadContentResponse {
                            namespace_id,
                            upload_id,
                            content_ref,
                        }));
                    }
                    return Err(CoreError::UploadContentConflict { upload_id });
                }

                let stored = stage_bytes_under_content_id(
                    store,
                    content_store_id,
                    state.content_id.clone(),
                    bytes,
                )
                .await?;
                state.staged_content_ref = Some(stored.content_ref.clone());

                Ok(UploadSessionUpdate::Replace {
                    next: Box::new(state),
                    outcome: UploadContentResponse {
                        namespace_id,
                        upload_id,
                        content_ref: stored.content_ref,
                    },
                })
            }
        },
    )
    .await
}

pub(crate) async fn complete_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    request: &CompleteUploadRequest,
) -> Result<(CompleteUploadResponse, PreparedContent)> {
    update_upload_session(
        store,
        namespace_id,
        upload_id,
        CONTENTION_RETRY_LIMIT,
        |mut state| {
            let namespace_id = namespace_id.clone();
            let content_store_id = content_store_id.clone();
            let upload_id = upload_id.to_owned();
            let request = request.clone();
            async move {
                if state.state == UploadSessionLifecycle::Condemned {
                    return Err(CoreError::UploadNotFound { upload_id });
                }
                if let Some(completed) = &state.completed {
                    if completed.content_ref == request.content_ref {
                        let prepared_content = prepare_completed_upload_content(
                            content_store_id.clone(),
                            completed.content_ref.clone(),
                        );
                        return Ok(UploadSessionUpdate::Noop((
                            CompleteUploadResponse {
                                namespace_id,
                                upload_id,
                                content_ref: completed.content_ref.clone(),
                                validated_content_token: None,
                            },
                            prepared_content,
                        )));
                    }
                    return Err(CoreError::UploadAlreadyCompleted { upload_id });
                }

                let prepared_content = match state.staged_content_ref.clone() {
                    Some(content_ref) => {
                        prepare_completed_upload_content(content_store_id.clone(), content_ref)
                    }
                    None => {
                        stage_direct_put_content_ref(store, &content_store_id, &state, &request)
                            .await?
                    }
                };
                let staged_content_ref = prepared_content.content_ref().clone();
                if staged_content_ref != request.content_ref {
                    return Err(CoreError::InvalidUploadContent(
                        "completed content ref does not match staged content".to_owned(),
                    ));
                }

                if state.staged_content_ref.is_none() {
                    state.staged_content_ref = Some(staged_content_ref);
                }
                state.completed = Some(CompletedUpload {
                    content_ref: request.content_ref.clone(),
                });

                Ok(UploadSessionUpdate::Replace {
                    next: Box::new(state),
                    outcome: (
                        CompleteUploadResponse {
                            namespace_id,
                            upload_id,
                            content_ref: request.content_ref.clone(),
                            validated_content_token: None,
                        },
                        prepared_content,
                    ),
                })
            }
        },
    )
    .await
}

/// Condemns one abandoned session using exactly the state and etag inspected
/// together. A lost CAS is retained without retry so a racing completion can
/// never be overwritten on a second read. Completed and already-condemned
/// sessions are themselves absorbing and may be deleted once age-qualified.
pub(crate) async fn condemn_upload_session_if_aged<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    reap_window_ms: u64,
    context: &MutationContext,
) -> Result<UploadSessionSweep> {
    let update = try_update_upload_session(
        store,
        namespace_id,
        upload_id,
        |mut state, metadata| async move {
            let Some(last_modified_ms) = metadata.last_modified_ms else {
                return Ok(UploadSessionUpdate::Noop(UploadSessionSweep::Retain));
            };
            if context.now_ms.saturating_sub(last_modified_ms) < reap_window_ms {
                return Ok(UploadSessionUpdate::Noop(UploadSessionSweep::Retain));
            }
            if state.state == UploadSessionLifecycle::Condemned || state.completed.is_some() {
                return Ok(UploadSessionUpdate::Noop(UploadSessionSweep::Delete));
            }
            state.state = UploadSessionLifecycle::Condemned;
            Ok(UploadSessionUpdate::Replace {
                next: Box::new(state),
                outcome: UploadSessionSweep::Delete,
            })
        },
    )
    .await;
    match update {
        Ok(UploadSessionCas::Applied(outcome)) => Ok(outcome),
        Ok(UploadSessionCas::Conflict) => {
            tracing::debug!(
                namespace_id = %namespace_id,
                upload_id = %upload_id,
                "upload-session condemn lost its inspected etag; retaining"
            );
            Ok(UploadSessionSweep::Retain)
        }
        Err(CoreError::UploadNotFound { .. }) => Ok(UploadSessionSweep::Retain),
        Err(error) => Err(error),
    }
}

fn prepare_completed_upload_content(
    content_store_id: ContentStoreId,
    content_ref: ContentRef,
) -> PreparedContent {
    let admission = ContentAdmission::for_durable_content_write(content_store_id, content_ref);
    PreparedContent::from_admission(admission)
}

async fn stage_direct_put_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    state: &UploadSessionState,
    request: &CompleteUploadRequest,
) -> Result<PreparedContent> {
    if state.mode != UploadMode::DirectPut {
        return Err(CoreError::InvalidUploadContent(
            "upload content has not been staged".to_owned(),
        ));
    }

    let Some(expected) = &state.direct_put_content_ref else {
        return Err(CoreError::InvalidUploadContent(
            "direct_put session is missing its target content ref".to_owned(),
        ));
    };
    if expected != &request.content_ref {
        return Err(CoreError::InvalidUploadContent(
            "completed content ref does not match direct_put target".to_owned(),
        ));
    }

    // Bytes bypassed the LoonFS server, so completion is where the server
    // establishes what actually landed. It verifies rather than trusts: the
    // presigned write is checksum-bound, but provider enforcement is not
    // uniform across the family we support, and the object's identity no
    // longer says anything about its bytes. One checksum-bearing HEAD
    // settles both size and content without a download.
    match verify_durable_content_checksum(store, content_store_id, &request.content_ref).await {
        Ok(()) => Ok(prepare_completed_upload_content(
            content_store_id.clone(),
            request.content_ref.clone(),
        )),
        Err(err) => {
            delete_unpublished_content_object(store, content_store_id, &request.content_ref).await;
            Err(CoreError::InvalidUploadContent(err.to_string()))
        }
    }
}

/// Removes the object a failed completion was about.
///
/// The id is random and was never published, so exactly one session can be
/// talking about this object and nothing references it. Deleting is safe and
/// keeping it would leak bytes no one can ever name. Cleanup failure is not
/// worth failing the completion twice over — the session's own reaping
/// covers what this misses — so it is logged and dropped.
async fn delete_unpublished_content_object<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) {
    let object_key = content_blob(content_store_id.as_str(), &content_ref.content_id);
    if let Err(error) = store.delete(&object_key).await {
        tracing::warn!(
            content_id = %content_ref.content_id,
            error = %error,
            "failed to remove the content object of a rejected direct-put completion"
        );
    }
}
