//! Durable upload sessions: begin, stage, and complete content uploads,
//! including direct-put targets that move bytes past the server.

use crate::context::MutationContext;
use crate::control_update::{update_upload_session, UploadSessionUpdate};
use crate::engine::{BeginDirectPutUploadTargetResponse, DirectPutUploadTarget};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, Result};
use crate::namespace::catalog::{
    load_namespace_content_store_id, map_namespace_initialization_error_to_core,
    namespace_initialization_state, NamespaceInitializationState,
};
use crate::namespace::control::{
    load_content_store_descriptor_control, load_namespace_descriptor_control,
    load_namespace_head_control,
};
use crate::storage::content::{probe_durable_content_reference, write_immutable_object};
use bytes::Bytes;
use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, CompleteUploadResponse,
    UploadContentResponse, UploadMode,
};
use loonfs_api::wire::control::{
    encode_control_object, CompletedUpload, ControlObjectKind, UploadSessionEnvelope,
    UploadSessionState,
};
use loonfs_api::{ContentRef, ContentRefKind, NamespaceId, UploadId};
use loonfs_objectstore::keys::{content_blob, namespace_config, upload_session};
use loonfs_objectstore::ObjectStore;

const UPLOAD_SESSION_RETRY_LIMIT: usize = 8;

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
        UploadMode::ServiceProxied,
        None,
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

pub(crate) async fn begin_direct_put_upload_target<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_ref: ContentRef,
    context: &MutationContext,
) -> Result<BeginDirectPutUploadTargetResponse> {
    ensure_upload_namespace_available(store, namespace_id).await?;
    ensure_direct_put_content_ref_supported(&content_ref)?;
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let object_key = content_blob(content_store_id.as_str(), &content_ref.digest)
        .map_err(|err| CoreError::InvalidUploadContent(err.to_string()))?;
    let upload_id = create_upload_session(
        store,
        namespace_id,
        UploadMode::DirectPut,
        Some(content_ref.clone()),
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

fn ensure_direct_put_content_ref_supported(content_ref: &ContentRef) -> Result<()> {
    if content_ref.kind != ContentRefKind::WholeFileV0 {
        return Err(CoreError::InvalidUploadContent(
            "direct_put only supports whole_file_v0 content refs".to_owned(),
        ));
    }
    Ok(())
}

async fn create_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    mode: UploadMode,
    direct_put_content_ref: Option<ContentRef>,
    context: &MutationContext,
) -> Result<UploadId> {
    let upload_id = UploadId::generate();
    let state = UploadSessionState {
        namespace_id: namespace_id.clone(),
        upload_id: upload_id.clone(),
        mode,
        direct_put_content_ref,
        staged_content_ref: None,
        completed: None,
        created_at_ms: context.now_ms,
    };
    let envelope = UploadSessionEnvelope::from_state(
        ControlObjectKind::UploadSession,
        &context.writer_version,
        state,
    )
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

async fn ensure_upload_namespace_available<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<()> {
    match namespace_initialization_state(store, namespace_id).await {
        Ok(NamespaceInitializationState::Complete) => {
            let descriptor = load_namespace_descriptor_control(store, namespace_id)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(
                        MetadataProjectionLoadError::LoadNamespaceDescriptor(error),
                    )
                })?;
            load_content_store_descriptor_control(store, &descriptor.state.content_store_id)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(
                        MetadataProjectionLoadError::LoadContentStoreDescriptor(error),
                    )
                })?;
            load_namespace_head_control(store, namespace_id)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
                })?;
            Ok(())
        }
        Ok(NamespaceInitializationState::Absent) => Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::LoadNamespaceDescriptor(
                crate::namespace::control::ControlObjectLoadError::MissingObject {
                    object_key: namespace_config(namespace_id.as_str()),
                },
            ),
        )),
        Ok(NamespaceInitializationState::Partial | NamespaceInitializationState::PreHeadDebris) => {
            Err(CoreError::NamespacePartiallyInitialized {
                namespace_id: namespace_id.clone(),
            })
        }
        Err(error) => Err(map_namespace_initialization_error_to_core(error)),
    }
}

pub(crate) async fn upload_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    bytes: &[u8],
    context: &MutationContext,
) -> Result<UploadContentResponse> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let content_ref = ContentRef::whole_file_v0(bytes);
    let object_key = content_blob(content_store_id.as_str(), &content_ref.digest)
        .map_err(|err| CoreError::Internal(format!("failed to derive content blob key: {err}")))?;

    update_upload_session(
        store,
        namespace_id,
        upload_id,
        &context.writer_version,
        UPLOAD_SESSION_RETRY_LIMIT,
        |mut state| {
            let content_ref = content_ref.clone();
            let object_key = object_key.clone();
            let namespace_id = namespace_id.clone();
            let upload_id = upload_id.to_owned();
            async move {
                if state.completed.is_some() {
                    return Err(CoreError::UploadAlreadyCompleted { upload_id });
                }
                if state.mode == UploadMode::DirectPut {
                    return Err(CoreError::InvalidUploadContent(
                        "direct_put sessions must be completed after using the presigned URL"
                            .to_owned(),
                    ));
                }

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

                write_immutable_object(store, &object_key, bytes).await?;
                state.staged_content_ref = Some(content_ref.clone());

                Ok(UploadSessionUpdate::Replace {
                    next: Box::new(state),
                    outcome: UploadContentResponse {
                        namespace_id,
                        upload_id,
                        content_ref,
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
    upload_id: &UploadId,
    request: &CompleteUploadRequest,
    context: &MutationContext,
) -> Result<CompleteUploadResponse> {
    update_upload_session(
        store,
        namespace_id,
        upload_id,
        &context.writer_version,
        UPLOAD_SESSION_RETRY_LIMIT,
        |mut state| {
            let namespace_id = namespace_id.clone();
            let upload_id = upload_id.to_owned();
            let request = request.clone();
            async move {
                if let Some(completed) = &state.completed {
                    if completed.content_ref == request.content_ref {
                        return Ok(UploadSessionUpdate::Noop(CompleteUploadResponse {
                            namespace_id,
                            upload_id,
                            content_ref: completed.content_ref.clone(),
                            validated_content_token: None,
                        }));
                    }
                    return Err(CoreError::UploadAlreadyCompleted { upload_id });
                }

                let staged_content_ref = match state.staged_content_ref.clone() {
                    Some(content_ref) => content_ref,
                    None => {
                        stage_direct_put_content_ref(store, &namespace_id, &state, &request).await?
                    }
                };
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
                    outcome: CompleteUploadResponse {
                        namespace_id,
                        upload_id,
                        content_ref: request.content_ref.clone(),
                        validated_content_token: None,
                    },
                })
            }
        },
    )
    .await
}

async fn stage_direct_put_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    state: &UploadSessionState,
    request: &CompleteUploadRequest,
) -> Result<ContentRef> {
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

    // Bytes bypassed the LoonFS server; completion is the authority point
    // where the server proves the upload actually happened. Digest
    // integrity needs no re-proof: the transfer capability signed the
    // digest into the provider write and the key derives from it, so no
    // object can exist at this key with bytes that do not hash to the
    // content ref. One HEAD proves existence and the declared size without
    // pulling the payload back through the server.
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    probe_durable_content_reference(store, &content_store_id, &request.content_ref)
        .await
        .map_err(|err| CoreError::InvalidUploadContent(err.to_string()))?;
    Ok(request.content_ref.clone())
}
