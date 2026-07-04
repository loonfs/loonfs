use crate::checkpoint::{
    head_from_manifest, load_verified_manifest_tables_with_cache, VerifiedMetadataTables,
};
use crate::commit::{
    build_commit_plan_for_publish, commit_request_from_v0, core_commit_fingerprint,
    materialize_commit, prepare_commit_head_publish, publish_commit_head,
    resolve_restore_content_refs_for_publish, wal_payload_from_materialized_commit,
    CommitExecutionContext, CommitHeadPublishError, CommitIdentitySource, CommitOp,
    CommitRequest as CoreCommitRequest, MaterializedCommit, PreparedCommit,
    PublishCommitValidationContext, SemanticMutationIdentity,
};
use crate::content::ContentAdmission;
use crate::context::MutationContext;
use crate::control_update::{update_upload_session, UploadSessionUpdate};
use crate::engine::{BeginDirectPutUploadTargetResponse, DirectPutUploadTarget};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, MetadataViewError};
use crate::metadata::{CommitReceiptRecord, MetadataState, MetadataView};
use crate::namespace::catalog::{
    load_namespace_catalog_entry, load_namespace_content_store_id, namespace_initialization_state,
    NamespaceInitializationError, NamespaceInitializationState,
};
use crate::namespace::control::{
    load_content_store_descriptor_control, load_namespace_descriptor_control,
    load_namespace_head_control,
};
use crate::namespace::control::{read_head_object, ControlObjectLoadError};
use crate::namespace::writer_epoch::acquire_writer_epoch;
use crate::path::write::{path_intent_fingerprint_for_path_intent, PublishPlanningSession};
use crate::publisher::NamespaceMutationCandidate;
use crate::storage::content::{
    validate_durable_content_reference, write_immutable_object, ContentValidationTracker,
};
use crate::timing::MonotonicTimer;
use crate::wal::{
    load_validated_wal_chain, prepare_wal_segment, project_validated_wal_tail, WalChainLoadRequest,
};
use bytes::Bytes;
use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, ChangesResponse, CommitDelta,
    CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse, CommittedChange,
    CompleteUploadRequest, CompleteUploadResponse, UploadContentResponse, UploadMode,
};
use loonfs_api::wire::control::{
    encode_control_object, AcquiredWriter, CompletedUpload, ControlObjectKind, HeadState,
    NamespaceState, UploadSessionEnvelope, UploadSessionState,
};
use loonfs_api::wire::wal::{WalCommitDelta, WalCommitPayload, WalDelta};
use loonfs_api::{
    generate_upload_id, ChangeSeq, CommitId, ContentRef, ContentRefKind, ContentStoreId,
    EffectiveLimit, ManifestId, NameKey, NamespaceId,
};
use loonfs_objectstore::keys::{content_blob, namespace_config, upload_session, wal_head};
use loonfs_objectstore::ObjectStore;
use std::collections::HashMap;
use tracing::Instrument;

const UPLOAD_SESSION_RETRY_LIMIT: usize = 8;

#[derive(Debug, Clone)]
pub(crate) struct PublishBatchAgainstViewResult {
    pub(crate) results: Vec<Result<ApiCommitResponse, CoreError>>,
    pub(crate) published_records: Vec<WalCommitPayload>,
    pub(crate) resulting_head: Option<HeadState>,
    pub(crate) resulting_head_etag: Option<String>,
    pub(crate) can_reuse_loaded_projection: bool,
}

impl PublishBatchAgainstViewResult {
    fn new(results: Vec<Result<ApiCommitResponse, CoreError>>) -> Self {
        Self {
            results,
            published_records: Vec::new(),
            resulting_head: None,
            resulting_head_etag: None,
            can_reuse_loaded_projection: true,
        }
    }

    fn invalidate_projection(results: Vec<Result<ApiCommitResponse, CoreError>>) -> Self {
        Self {
            results,
            published_records: Vec::new(),
            resulting_head: None,
            resulting_head_etag: None,
            can_reuse_loaded_projection: false,
        }
    }

    fn published(
        results: Vec<Result<ApiCommitResponse, CoreError>>,
        published_records: Vec<WalCommitPayload>,
        resulting_head: HeadState,
        resulting_head_etag: Option<String>,
    ) -> Self {
        Self {
            results,
            published_records,
            resulting_head: Some(resulting_head),
            resulting_head_etag,
            can_reuse_loaded_projection: false,
        }
    }
}

pub(crate) struct PublishMetadataView<'a, S: ObjectStore + ?Sized> {
    content_store_id: ContentStoreId,
    head: HeadState,
    head_etag: String,
    acquired_writer: Option<AcquiredWriter>,
    manifest_tables: VerifiedMetadataTables<'a, S>,
    tail_state: MetadataState,
}

impl<S: ObjectStore + ?Sized> PublishMetadataView<'_, S> {
    pub(crate) fn head(&self) -> &HeadState {
        &self.head
    }

    pub(crate) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(&self.head, &self.manifest_tables, &self.tail_state)
    }

    async fn find_commit_receipt(
        &self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitReceiptRecord>, CoreError> {
        self.metadata_view().find_commit_receipt(commit_id).await
    }
}

#[derive(Debug, Clone)]
struct InBatchRequest {
    primary_index: usize,
    semantic_identity: SemanticMutationIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishTailOptions {
    pub(crate) max_tail_rows: usize,
    pub(crate) max_tail_decoded_bytes: Option<usize>,
}

impl Default for PublishTailOptions {
    fn default() -> Self {
        Self {
            max_tail_rows: 1_000_000,
            max_tail_decoded_bytes: Some(256 * 1024 * 1024),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublishTailProjection {
    pub(crate) namespace_id: NamespaceId,
    pub(crate) head_etag: String,
    pub(crate) head_seq: ChangeSeq,
    pub(crate) manifest_id: ManifestId,
    pub(crate) manifest_head_seq: ChangeSeq,
    pub(crate) manifest_payload_checksum: String,
    pub(crate) wal_tail_segments: u64,
    pub(crate) tail_state: MetadataState,
}

impl PublishTailProjection {
    fn matches(
        &self,
        namespace_id: &NamespaceId,
        head: &HeadState,
        head_etag: &str,
        manifest_id: ManifestId,
        manifest_head_seq: ChangeSeq,
        manifest_payload_checksum: &str,
    ) -> bool {
        self.namespace_id == *namespace_id
            && self.head_etag == head_etag
            && self.head_seq == head.seq
            && self.manifest_id == manifest_id
            && self.manifest_head_seq == manifest_head_seq
            && self.manifest_payload_checksum == manifest_payload_checksum
    }

    pub(crate) fn within_limits(&self, options: &PublishTailOptions) -> bool {
        self.tail_state.row_count() <= options.max_tail_rows
            && options
                .max_tail_decoded_bytes
                .map(|max| self.tail_state.decoded_bytes() <= max)
                .unwrap_or(true)
    }
}

struct CandidateCoreRequest {
    request: CoreCommitRequest,
    identity_source: CommitIdentitySource,
}

pub(crate) async fn begin_upload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: BeginUploadRequest,
    context: &MutationContext,
) -> Result<BeginUploadResponse, CoreError> {
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
) -> Result<BeginDirectPutUploadTargetResponse, CoreError> {
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

fn ensure_direct_put_content_ref_supported(content_ref: &ContentRef) -> Result<(), CoreError> {
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
) -> Result<String, CoreError> {
    let upload_id = generate_upload_id();
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
    .map_err(|err| CoreError::Store(err.to_string()))?;
    let encoded =
        encode_control_object(&envelope).map_err(|err| CoreError::Store(err.to_string()))?;
    let object_key = upload_session(namespace_id.as_str(), &upload_id);
    store
        .put_if_absent(&object_key, Bytes::from(encoded))
        .await
        .map_err(|err| CoreError::Store(err.to_string()))?;
    Ok(upload_id)
}

async fn ensure_upload_namespace_available<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(), CoreError> {
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
        Ok(NamespaceInitializationState::Partial) => {
            Err(CoreError::NamespacePartiallyInitialized {
                namespace_id: namespace_id.clone(),
            })
        }
        Err(error) => Err(map_upload_namespace_initialization_error(error)),
    }
}

fn map_upload_namespace_initialization_error(error: NamespaceInitializationError) -> CoreError {
    match error {
        NamespaceInitializationError::InvalidNamespaceId(error) => {
            CoreError::InvalidNamespaceId(error)
        }
        NamespaceInitializationError::LoadNamespaceDescriptor(error) => {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadNamespaceDescriptor(
                error,
            ))
        }
        NamespaceInitializationError::LoadContentStoreDescriptor(error) => {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadContentStoreDescriptor(
                error,
            ))
        }
        NamespaceInitializationError::InspectNamespaceDescriptor(_)
        | NamespaceInitializationError::InspectNamespaceHead(_) => {
            CoreError::Store(error.to_string())
        }
    }
}

pub(crate) async fn upload_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &str,
    bytes: &[u8],
    context: &MutationContext,
) -> Result<UploadContentResponse, CoreError> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let content_ref = ContentRef::whole_file_v0(bytes);
    let object_key = content_blob(content_store_id.as_str(), &content_ref.digest)
        .map_err(|err| CoreError::Store(err.to_string()))?;

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
    upload_id: &str,
    request: &CompleteUploadRequest,
    context: &MutationContext,
) -> Result<CompleteUploadResponse, CoreError> {
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
) -> Result<ContentRef, CoreError> {
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

    // Bytes bypassed the LoonFS server; completion is the authority point where
    // the server proves the object is durable and matches the signed content ref.
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    validate_durable_content_reference(store, &content_store_id, &request.content_ref)
        .await
        .map_err(|err| CoreError::InvalidUploadContent(err.to_string()))?;
    Ok(request.content_ref.clone())
}

pub(crate) async fn commit_operations<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    request: ApiCommitRequest,
    context: &MutationContext,
) -> Result<ApiCommitResponse, CoreError> {
    publish_namespace_mutations_batch(
        store,
        namespace_id,
        vec![NamespaceMutationCandidate::Commit(request)],
        context,
    )
    .await
    .pop()
    .unwrap_or_else(|| Err(CoreError::Store("empty commit batch".to_owned())))
}

pub(crate) async fn commit_operations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    requests: Vec<ApiCommitRequest>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    publish_namespace_mutations_batch(
        store,
        namespace_id,
        requests
            .into_iter()
            .map(NamespaceMutationCandidate::Commit)
            .collect(),
        context,
    )
    .await
}

pub(crate) async fn publish_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    commit_namespace_mutations_batch(store, namespace_id, candidates, context).await
}

async fn commit_namespace_mutations_batch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: Vec<NamespaceMutationCandidate>,
    context: &MutationContext,
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    if candidates.is_empty() {
        return Vec::new();
    }
    let acquired_writer = match acquire_writer_epoch(store, namespace_id, context).await {
        Ok(value) => value,
        Err(error) => {
            return (0..candidates.len())
                .map(|_| Err(CoreError::WriterEpoch(error.clone())))
                .collect();
        }
    };
    let publish_tail_options = PublishTailOptions::default();
    let (publish_view, projection) = match load_publish_metadata_view(
        store,
        namespace_id,
        Some(acquired_writer),
        None,
        &publish_tail_options,
    )
    .instrument(tracing::info_span!(
        "loon.phase",
        phase = "load_publish_view"
    ))
    .await
    {
        Ok(value) => value,
        Err(error) => return (0..candidates.len()).map(|_| Err(error.clone())).collect(),
    };
    if projection.wal_tail_segments > crate::publisher::WAL_TAIL_BACKPRESSURE_SEGMENTS {
        // Same backpressure contract as the caching engine: the commit
        // surface must not outrun maintenance either (format spec,
        // "Maintenance operations").
        let error = MetadataViewError::MaintenanceRequired {
            namespace_id: namespace_id.clone(),
            reason: format!(
                "wal tail has {} segments; publishes resume once maintenance brings it back to {} or fewer",
                projection.wal_tail_segments,
                crate::publisher::WAL_TAIL_BACKPRESSURE_SEGMENTS
            ),
        };
        return (0..candidates.len())
            .map(|_| Err(CoreError::from(error.clone())))
            .collect();
    }
    // One-shot path: each call is its own writer-session decision, so a
    // fresh budget timer per call is correct.
    let timer = crate::timing::StdMonotonicTimer::default();
    publish_namespace_mutations_batch_against_publish_view(
        store,
        namespace_id,
        &candidates,
        context,
        &publish_view,
        &timer,
    )
    .await
    .results
}

pub(crate) async fn load_publish_metadata_view<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    acquired_writer: Option<AcquiredWriter>,
    cached_projection: Option<&PublishTailProjection>,
    options: &PublishTailOptions,
) -> Result<(PublishMetadataView<'a, S>, PublishTailProjection), CoreError> {
    let catalog_entry = load_namespace_catalog_entry(store, namespace_id)
        .await
        .map_err(|error| CoreError::MetadataProjection(MetadataProjectionLoadError::from(error)))?;
    let loaded_head = read_head_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let head_etag = loaded_head.metadata.etag.clone().ok_or_else(|| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::MissingHeadEtag {
            object_key: loaded_head.object_key.clone(),
        })
    })?;
    let head = loaded_head.envelope.state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    if let Some(acquired_writer) = &acquired_writer {
        ensure_publish_head_matches_acquired_writer(&head, acquired_writer)?;
    }
    let manifest_id =
        head.current_manifest_id
            .ok_or_else(|| MetadataViewError::MissingManifest {
                namespace_id: namespace_id.clone(),
            })?;
    let manifest_tables =
        load_verified_manifest_tables_with_cache(store, None, namespace_id, manifest_id)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
    let manifest_head = head_from_manifest(&head, manifest_tables.manifest());
    let manifest_payload_checksum = manifest_tables.manifest().payload_checksum.clone();
    let projection = if let Some(cached) = cached_projection.filter(|cached| {
        cached.matches(
            namespace_id,
            &head,
            &head_etag,
            manifest_id,
            manifest_head.seq,
            &manifest_payload_checksum,
        ) && cached.within_limits(options)
    }) {
        cached.clone()
    } else {
        load_publish_tail_projection(
            store,
            namespace_id,
            &head,
            &head_etag,
            manifest_id,
            &manifest_head,
            manifest_payload_checksum,
        )
        .await?
    };

    let tail_state = projection.tail_state.clone();
    ensure_publish_head_etag_still_current(store, namespace_id, &head_etag).await?;

    Ok((
        PublishMetadataView {
            content_store_id: catalog_entry.content_store_id,
            head,
            head_etag,
            acquired_writer,
            manifest_tables,
            tail_state,
        },
        projection,
    ))
}

fn ensure_publish_head_matches_acquired_writer(
    head: &HeadState,
    acquired_writer: &AcquiredWriter,
) -> Result<(), CoreError> {
    if head.writer_epoch != acquired_writer.writer_epoch {
        let winner = head
            .writer
            .as_ref()
            .map(|writer| writer.writer_id.as_str())
            .unwrap_or("unknown");
        return Err(CoreError::WriterFenced(format!(
            "writer epoch {} was fenced by epoch {} (writer `{winner}`)",
            acquired_writer.writer_epoch.0, head.writer_epoch.0
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn load_publish_tail_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    head: &HeadState,
    head_etag: &str,
    manifest_id: ManifestId,
    manifest_head: &HeadState,
    manifest_payload_checksum: String,
) -> Result<PublishTailProjection, CoreError> {
    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: manifest_head.seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: None,
            recent_segments: &head.recent_segments,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    let replayed = project_validated_wal_tail(
        manifest_head,
        &MetadataState::default(),
        Some(head.writer_epoch),
        &wal_chain,
    )
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalReplay(error))
    })?;
    ensure_publish_reconstructed_head_matches(head, &replayed.resulting_head)?;
    let wal_tail_segments = u64::try_from(wal_chain.segments().len()).unwrap_or(u64::MAX);
    let projection = PublishTailProjection {
        namespace_id: namespace_id.clone(),
        head_etag: head_etag.to_owned(),
        head_seq: head.seq,
        manifest_id,
        manifest_head_seq: manifest_head.seq,
        manifest_payload_checksum,
        wal_tail_segments,
        tail_state: replayed.resulting_metadata_state,
    };
    Ok(projection)
}

fn ensure_publish_reconstructed_head_matches(
    current_head: &HeadState,
    reconstructed: &HeadState,
) -> Result<(), CoreError> {
    if current_head.namespace_id != reconstructed.namespace_id
        || current_head.seq != reconstructed.seq
        || current_head.head_commit_id != reconstructed.head_commit_id
        || current_head.next_inode_id != reconstructed.next_inode_id
        || current_head.name_policy != reconstructed.name_policy
        || current_head.current_manifest_id != reconstructed.current_manifest_id
        || current_head.latest_checkpoint_id != reconstructed.latest_checkpoint_id
        || current_head.retention_floor_seq != reconstructed.retention_floor_seq
        || (reconstructed.visible_wal_tip.is_some()
            && current_head.visible_wal_tip != reconstructed.visible_wal_tip)
    {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::ReplayedHeadMismatch {
                expected: Box::new(current_head.clone()),
                actual: Box::new(reconstructed.clone()),
            },
        ));
    }
    Ok(())
}

async fn ensure_publish_head_etag_still_current<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    loaded_head_etag: &str,
) -> Result<(), CoreError> {
    let object_key = wal_head(namespace_id.as_str());
    let metadata = store
        .head(&object_key)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(
                ControlObjectLoadError::Store(error.to_string()),
            ))
        })?
        .ok_or_else(|| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(
                ControlObjectLoadError::MissingObject {
                    object_key: object_key.clone(),
                },
            ))
        })?;
    let current_head_etag = metadata.etag.ok_or_else(|| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::MissingHeadEtag {
            object_key: object_key.clone(),
        })
    })?;
    if current_head_etag != loaded_head_etag {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::HeadChangedDuringLoad {
                object_key,
                loaded_head_etag: loaded_head_etag.to_owned(),
                current_head_etag,
            },
        ));
    }
    Ok(())
}

/// Self-enforced budget between starting the WAL segment PUT and initiating
/// the head CAS. Sized so budget + request timeout sit well inside the GC
/// grace window; overrunning it abandons the segment instead of publishing a
/// stale-timed one. Local monotonic elapsed time only — never a validity
/// input (format spec, "WAL head").
pub(crate) const PUBLISH_BUDGET_MS: u64 = 60_000;

pub(crate) async fn publish_namespace_mutations_batch_against_publish_view<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: &[NamespaceMutationCandidate],
    context: &MutationContext,
    view: &PublishMetadataView<'_, S>,
    timer: &dyn MonotonicTimer,
) -> PublishBatchAgainstViewResult {
    if candidates.is_empty() {
        return PublishBatchAgainstViewResult::new(Vec::new());
    }
    let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
    if view.head.namespace_id != *namespace_id {
        return PublishBatchAgainstViewResult::new(
            (0..candidates.len())
                .map(|_| {
                    Err(CoreError::Store(
                        "publish view namespace mismatch".to_owned(),
                    ))
                })
                .collect(),
        );
    }
    let mut outcomes: Vec<Option<Result<ApiCommitResponse, CoreError>>> =
        (0..candidates.len()).map(|_| None).collect();
    let mut session = PublishPlanningSession::new(&view.head);
    let mut accepted: Vec<(usize, MaterializedCommit)> = Vec::new();
    let mut in_batch_requests: HashMap<CommitId, InBatchRequest> = HashMap::new();
    let mut aliases: Vec<(usize, usize)> = Vec::new();
    let mut content_validation = ContentValidationTracker::default();

    let prepare_span = tracing::info_span!(
        "publisher.batch_prepare",
        phase = "batch_prepare",
        batch_size,
        accepted_count = tracing::field::Empty
    );
    async {
        for (index, candidate) in candidates.iter().enumerate() {
            let candidate_request = prepare_candidate_request(
                namespace_id,
                view,
                &session,
                candidate,
                index,
                &mut outcomes,
                &mut in_batch_requests,
                &mut aliases,
            )
            .instrument(tracing::info_span!("loon.phase", phase = "prepare_commit"))
            .await;
            let Some(candidate_request) = candidate_request else {
                continue;
            };
            let validation = PublishCommitValidationContext {
                head: session.head(),
                metadata_view: view.metadata_view(),
                accepted_rows: session.accepted_rows(),
            };
            let request = candidate_request.request;
            let resolved_restore_content_refs =
                match resolve_restore_content_refs_for_publish(&request, &validation).await {
                    Ok(value) => value,
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        continue;
                    }
                };
            let admissions = CommitContentAdmissions {
                namespace_id,
                admissions: candidate_content_admissions(candidate),
                now_ms: context.now_ms,
            };
            if let Err(error) = validate_commit_content_references(
                store,
                &view.content_store_id,
                &request,
                &resolved_restore_content_refs,
                admissions,
                &mut content_validation,
            )
            .await
            {
                outcomes[index] = Some(Err(error));
                continue;
            }
            let plan = {
                let span = tracing::info_span!("loon.phase", phase = "build_commit_plan");
                match build_commit_plan_for_publish(&request, &validation)
                    .instrument(span)
                    .await
                {
                    Ok(plan) => plan,
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        continue;
                    }
                }
            };
            let prepared = {
                let _span =
                    tracing::info_span!("loon.phase", phase = "PreparedCommit::prepare").entered();
                match PreparedCommit::prepare(
                    request,
                    plan.clone(),
                    candidate_request.identity_source,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        outcomes[index] = Some(Err(CoreError::Store(format!(
                            "commit preparation failed: {error}"
                        ))));
                        continue;
                    }
                }
            };
            let materialized = {
                let _span =
                    tracing::info_span!("loon.phase", phase = "materialize_commit").entered();
                materialize_commit(prepared)
            };
            let preview = {
                let _span = tracing::info_span!(
                    "loon.phase",
                    phase = "wal_payload_from_materialized_commit"
                )
                .entered();
                match wal_payload_from_materialized_commit(&materialized) {
                    Ok(payload) => payload,
                    Err(error) => {
                        outcomes[index] = Some(Err(error.into()));
                        continue;
                    }
                }
            };
            let applied = {
                let _span = tracing::info_span!("loon.phase", phase = "apply_committed_wal_record")
                    .entered();
                session.apply_accepted_commit(&preview, &plan)
            };
            match applied {
                Ok(()) => accepted.push((index, materialized)),
                Err(error) => outcomes[index] = Some(Err(error.into())),
            }
        }
    }
    .instrument(prepare_span.clone())
    .await;
    prepare_span.record(
        "accepted_count",
        u64::try_from(accepted.len()).unwrap_or(u64::MAX),
    );
    drop(prepare_span);

    if accepted.is_empty() {
        return PublishBatchAgainstViewResult::new(finish_batch_outcomes_with_aliases(
            outcomes, &aliases,
        ));
    }
    let records = accepted
        .iter()
        .map(|(_, record)| record.clone())
        .collect::<Vec<_>>();
    let accepted_count = u64::try_from(records.len()).unwrap_or(u64::MAX);
    let wal_span = tracing::info_span!(
        "publisher.batch_write_wal",
        phase = "batch_write_wal",
        batch_size,
        accepted_count,
        wal_segment_count = 1_u64,
        key_class = "wal_segment",
        result = tracing::field::Empty
    );
    let put_started_ms = timer.monotonic_now_ms();
    let wal_result: Result<_, CoreError> = {
        let _span = wal_span.enter();
        match prepare_wal_segment(
            namespace_id.clone(),
            view.acquired_writer
                .as_ref()
                .expect("publish view should carry acquired writer")
                .writer_epoch,
            view.head.visible_wal_tip.clone(),
            &records,
            &context.writer_version,
        ) {
            Ok(wal) => match store
                .put_if_absent(&wal.object_key, Bytes::copy_from_slice(&wal.encoded_bytes))
                .await
            {
                Ok(_) => Ok(wal),
                Err(error) => Err(CoreError::WalWrite(error.to_string())),
            },
            Err(error) => Err(CoreError::Store(format!("wal build failed: {error:?}"))),
        }
    };
    wal_span.record("result", if wal_result.is_ok() { "ok" } else { "error" });
    drop(wal_span);
    let wal = match wal_result {
        Ok(wal) => wal,
        Err(error) => {
            fail_outcomes_contingent_on_unpublished_batch(&mut outcomes, &accepted, &error);
            return PublishBatchAgainstViewResult::invalidate_projection(
                finish_batch_outcomes_with_aliases(outcomes, &aliases),
            );
        }
    };

    let last_plan = &records
        .last()
        .expect("non-empty accepted records")
        .prepared
        .plan;
    let head_publish =
        prepare_commit_head_publish(&view.head, last_plan, &wal, &context.writer_version);
    let head_publish = match head_publish {
        Ok(value) => value,
        Err(error) => {
            let error = CoreError::Store(format!("head publish preparation failed: {error:?}"));
            fail_outcomes_contingent_on_unpublished_batch(&mut outcomes, &accepted, &error);
            return PublishBatchAgainstViewResult::invalidate_projection(
                finish_batch_outcomes_with_aliases(outcomes, &aliases),
            );
        }
    };
    let elapsed_ms = timer.monotonic_now_ms().saturating_sub(put_started_ms);
    if elapsed_ms > PUBLISH_BUDGET_MS {
        let error = CoreError::HeadPublish(CommitHeadPublishError::PublishBudgetExceeded {
            elapsed_ms,
            budget_ms: PUBLISH_BUDGET_MS,
        });
        fail_outcomes_contingent_on_unpublished_batch(&mut outcomes, &accepted, &error);
        return PublishBatchAgainstViewResult::invalidate_projection(
            finish_batch_outcomes_with_aliases(outcomes, &aliases),
        );
    }
    let head_cas_span = tracing::info_span!(
        "publisher.batch_cas_head",
        phase = "batch_cas_head",
        batch_size,
        accepted_count,
        key_class = "wal_head",
        result = tracing::field::Empty
    );
    let head_metadata_result = {
        let _span = head_cas_span.enter();
        publish_commit_head(store, &view.head_etag, &head_publish).await
    };
    head_cas_span.record(
        "result",
        if head_metadata_result.is_ok() {
            "ok"
        } else {
            "error"
        },
    );
    drop(head_cas_span);
    let resulting_head_etag = match head_metadata_result {
        Ok(metadata) => metadata.etag,
        Err(error) => {
            fail_outcomes_contingent_on_unpublished_batch(&mut outcomes, &accepted, &error.into());
            return PublishBatchAgainstViewResult::invalidate_projection(
                finish_batch_outcomes_with_aliases(outcomes, &aliases),
            );
        }
    };

    let published_records = wal.envelope.payload.records.clone();
    for (accepted_index, (outcome_index, record)) in accepted.into_iter().enumerate() {
        outcomes[outcome_index] = Some(Ok(ApiCommitResponse {
            namespace_id: namespace_id.clone(),
            commit_id: record.prepared.request.commit_id,
            committed_seq: published_records[accepted_index].seq,
        }));
    }
    let results = finish_batch_outcomes_with_aliases(outcomes, &aliases);
    PublishBatchAgainstViewResult::published(
        results,
        published_records,
        head_publish.resulting_head,
        resulting_head_etag,
    )
}

#[allow(clippy::too_many_arguments)]
async fn prepare_candidate_request<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    view: &PublishMetadataView<'_, S>,
    session: &PublishPlanningSession,
    candidate: &NamespaceMutationCandidate,
    index: usize,
    outcomes: &mut [Option<Result<ApiCommitResponse, CoreError>>],
    in_batch_requests: &mut HashMap<CommitId, InBatchRequest>,
    aliases: &mut Vec<(usize, usize)>,
) -> Option<CandidateCoreRequest> {
    let conversion_context = CommitExecutionContext {
        namespace_id: namespace_id.clone(),
        writer_id: view
            .acquired_writer
            .as_ref()
            .expect("publish view should carry acquired writer")
            .writer_id
            .clone(),
        writer_session_id: view
            .acquired_writer
            .as_ref()
            .expect("publish view should carry acquired writer")
            .writer_session_id
            .clone(),
        writer_epoch: view
            .acquired_writer
            .as_ref()
            .expect("publish view should carry acquired writer")
            .writer_epoch,
    };
    match candidate {
        NamespaceMutationCandidate::Commit(request) => {
            if let Err(error) = validate_commit_id(&request.commit_id) {
                outcomes[index] = Some(Err(error));
                return None;
            }
            let request = match commit_request_from_v0(conversion_context, request.clone()) {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(error.into()));
                    return None;
                }
            };
            let semantic_identity = match core_commit_fingerprint(&request) {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(CoreError::Store(error.to_string())));
                    return None;
                }
            };
            let semantic_identity = SemanticMutationIdentity::CoreCommit(semantic_identity);
            let should_prepare = match record_primary_request_or_complete_idempotent(
                namespace_id,
                view,
                outcomes,
                in_batch_requests,
                aliases,
                index,
                &request.commit_id,
                &semantic_identity,
            )
            .await
            {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(error));
                    return None;
                }
            };
            if !should_prepare {
                return None;
            }
            Some(CandidateCoreRequest {
                request,
                identity_source: CommitIdentitySource::CoreCommitRequest,
            })
        }
        NamespaceMutationCandidate::Path(intent)
        | NamespaceMutationCandidate::PathWithContentAdmission { intent, .. } => {
            if let Err(error) = validate_commit_id(intent.commit_id()) {
                outcomes[index] = Some(Err(error));
                return None;
            }
            let path_intent_fingerprint =
                match path_intent_fingerprint_for_path_intent(namespace_id, intent) {
                    Ok(value) => value,
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        return None;
                    }
                };
            let semantic_identity =
                SemanticMutationIdentity::PathIntent(path_intent_fingerprint.clone());
            let commit_id = intent.commit_id().clone();
            let should_prepare = match record_primary_request_or_complete_idempotent(
                namespace_id,
                view,
                outcomes,
                in_batch_requests,
                aliases,
                index,
                &commit_id,
                &semantic_identity,
            )
            .await
            {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(error));
                    return None;
                }
            };
            if !should_prepare {
                return None;
            }
            let planned = match session
                .plan_path_mutation(namespace_id, intent, view.metadata_view())
                .await
            {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(error));
                    return None;
                }
            };
            let request = match commit_request_from_v0(conversion_context, planned.commit_request) {
                Ok(value) => value,
                Err(error) => {
                    outcomes[index] = Some(Err(error.into()));
                    return None;
                }
            };
            Some(CandidateCoreRequest {
                request,
                identity_source: CommitIdentitySource::PathIntent(planned.path_intent_fingerprint),
            })
        }
    }
}

fn validate_commit_id(commit_id: &CommitId) -> Result<(), CoreError> {
    CommitId::parse(commit_id.as_str())
        .map(|_| ())
        .map_err(CoreError::InvalidCommitId)
}

#[allow(clippy::too_many_arguments)]
async fn record_primary_request_or_complete_idempotent<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    view: &PublishMetadataView<'_, S>,
    outcomes: &mut [Option<Result<ApiCommitResponse, CoreError>>],
    in_batch_requests: &mut HashMap<CommitId, InBatchRequest>,
    aliases: &mut Vec<(usize, usize)>,
    index: usize,
    commit_id: &CommitId,
    semantic_identity: &SemanticMutationIdentity,
) -> Result<bool, CoreError> {
    if let Some(existing) = view.find_commit_receipt(commit_id).await? {
        outcomes[index] = Some(
            if existing.semantic_commit_fingerprint != semantic_identity.as_str() {
                Err(CoreError::CommitIdReuseConflict(commit_id.to_string()))
            } else {
                Ok(commit_response_from_commit_receipt(namespace_id, &existing))
            },
        );
        return Ok(false);
    }
    if let Some(existing) = in_batch_requests.get(commit_id) {
        if existing.semantic_identity != *semantic_identity {
            outcomes[index] = Some(Err(CoreError::CommitIdReuseConflict(commit_id.to_string())));
        } else {
            aliases.push((index, existing.primary_index));
        }
        return Ok(false);
    }
    in_batch_requests.insert(
        commit_id.clone(),
        InBatchRequest {
            primary_index: index,
            semantic_identity: semantic_identity.clone(),
        },
    );
    Ok(true)
}

pub(crate) async fn list_changes_after<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    after_seq: ChangeSeq,
    limit: EffectiveLimit,
) -> Result<ChangesResponse, CoreError> {
    load_namespace_catalog_entry(store, namespace_id).await?;
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
    if head.current_manifest_id.is_none() {
        return Err(MetadataViewError::MissingManifest {
            namespace_id: namespace_id.clone(),
        }
        .into());
    }

    if after_seq < head.retention_floor_seq {
        return Err(CoreError::RebootstrapRequired {
            after_seq,
            retention_floor_seq: head.retention_floor_seq,
        });
    }
    if after_seq >= head.seq {
        return Ok(ChangesResponse {
            namespace_id: namespace_id.clone(),
            after_seq,
            through_seq: head.seq,
            next_after_seq: None,
            changes: Vec::new(),
        });
    }

    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: head.retention_floor_seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: Some(after_seq),
            recent_segments: &head.recent_segments,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    let mut changes = Vec::with_capacity(limit.as_usize());
    let mut through_seq = head.seq;
    let mut next_after_seq = None;
    'segments: for segment in wal_chain.segments() {
        for record in segment.records() {
            if record.seq > after_seq {
                let seq = record.seq;
                changes.push(CommittedChange {
                    seq,
                    commit_id: record.commit_id.clone(),
                    message: record.message.clone(),
                    deltas: record
                        .deltas
                        .iter()
                        .map(commit_delta_from_wal)
                        .collect::<Result<Vec<_>, _>>()?,
                });
                if changes.len() == limit.as_usize() {
                    through_seq = seq;
                    if seq < head.seq {
                        next_after_seq = Some(seq);
                    }
                    break 'segments;
                }
            }
        }
    }

    Ok(ChangesResponse {
        namespace_id: namespace_id.clone(),
        after_seq,
        through_seq,
        next_after_seq,
        changes,
    })
}

fn commit_delta_from_wal(delta: &WalCommitDelta) -> Result<CommitDelta, CoreError> {
    let semantic_op_index = delta.semantic_op_index;
    Ok(match &delta.delta {
        WalDelta::CreateInode {
            delta_index,
            inode_id,
            inode_kind,
        } => CommitDelta::CreateInode {
            semantic_op_index,
            delta_index: *delta_index,
            inode_id: *inode_id,
            inode_kind: inode_kind.clone(),
        },
        WalDelta::BindDirentry {
            delta_index,
            parent_inode,
            name_key,
            display_name,
            child_inode,
        } => CommitDelta::BindDirentry {
            semantic_op_index,
            delta_index: *delta_index,
            parent_inode: *parent_inode,
            name_key: NameKey::try_new(name_key.clone()).map_err(|err| {
                CoreError::NamespaceCorrupt(format!("invalid WAL name_key: {err}"))
            })?,
            display_name: display_name.clone(),
            child_inode: *child_inode,
        },
        WalDelta::UnbindDirentry {
            delta_index,
            parent_inode,
            name_key,
            child_inode,
            bind_seq,
            bind_delta_index,
        } => CommitDelta::UnbindDirentry {
            semantic_op_index,
            delta_index: *delta_index,
            parent_inode: *parent_inode,
            name_key: NameKey::try_new(name_key.clone()).map_err(|err| {
                CoreError::NamespaceCorrupt(format!("invalid WAL name_key: {err}"))
            })?,
            child_inode: *child_inode,
            bind_seq: *bind_seq,
            bind_delta_index: *bind_delta_index,
        },
        WalDelta::AppendFileRevision {
            delta_index,
            inode_id,
            revision_no,
            content_ref,
        } => CommitDelta::AppendFileRevision {
            semantic_op_index,
            delta_index: *delta_index,
            inode_id: *inode_id,
            revision_no: *revision_no,
            content_ref: content_ref.clone(),
        },
        WalDelta::TombstoneSubtree {
            delta_index,
            root_inode,
        } => CommitDelta::TombstoneSubtree {
            semantic_op_index,
            delta_index: *delta_index,
            root_inode: *root_inode,
        },
    })
}

struct CommitContentAdmissions<'a> {
    namespace_id: &'a NamespaceId,
    admissions: &'a [ContentAdmission],
    now_ms: u64,
}

impl CommitContentAdmissions<'_> {
    fn admits(&self, content_ref: &ContentRef) -> bool {
        self.admissions
            .iter()
            .any(|admission| admission.admits(self.namespace_id, content_ref, self.now_ms))
    }
}

async fn validate_commit_content_references<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    request: &CoreCommitRequest,
    resolved_restore_content_refs: &[Option<ContentRef>],
    admissions: CommitContentAdmissions<'_>,
    content_validation: &mut ContentValidationTracker,
) -> Result<(), CoreError> {
    let mut content_refs = Vec::new();
    for (index, op) in request.ops.iter().enumerate() {
        match op {
            CommitOp::CreateFile { content_ref, .. }
            | CommitOp::ReplaceFile { content_ref, .. } => {
                content_refs.push(content_ref);
            }
            CommitOp::RestoreRevision { .. } => {
                if let Some(content_ref) = resolved_restore_content_refs
                    .get(index)
                    .and_then(|content_ref| content_ref.as_ref())
                {
                    content_refs.push(content_ref);
                }
            }
            _ => {}
        }
    }

    if content_refs.is_empty() {
        return Ok(());
    }

    for content_ref in content_refs {
        if admissions.admits(content_ref) {
            continue;
        }
        content_validation
            .ensure_validated(store, content_store_id, content_ref)
            .await?;
    }

    Ok(())
}

fn candidate_content_admissions(candidate: &NamespaceMutationCandidate) -> &[ContentAdmission] {
    match candidate {
        NamespaceMutationCandidate::PathWithContentAdmission { admissions, .. } => admissions,
        NamespaceMutationCandidate::Commit(_) | NamespaceMutationCandidate::Path(_) => &[],
    }
}

fn commit_response_from_commit_receipt(
    namespace_id: &NamespaceId,
    record: &CommitReceiptRecord,
) -> ApiCommitResponse {
    ApiCommitResponse {
        namespace_id: namespace_id.clone(),
        commit_id: record.commit_id.clone(),
        committed_seq: record.committed_seq,
    }
}

/// Fails every outcome that was contingent on this batch publishing durably.
///
/// The accepted candidates take the batch error: they never committed. So do
/// rejections recorded after the first acceptance, because their verdicts
/// were decided against session state advanced by tentatively accepted
/// candidates — state that never became durable. Reporting them would hand a
/// client a definitive semantic error (path conflict, missing path, stale
/// revision, ...) it correctly treats as non-retryable, for a precondition
/// that was never durably true (format.md section 3.1.5).
///
/// Rejections recorded before any acceptance were decided against the loaded
/// durable publish view and stand. Idempotent `Ok` completions replay durable
/// commit receipts and stand. Alias slots stay unfilled here and inherit
/// their primary's final outcome.
fn fail_outcomes_contingent_on_unpublished_batch(
    outcomes: &mut [Option<Result<ApiCommitResponse, CoreError>>],
    accepted: &[(usize, MaterializedCommit)],
    error: &CoreError,
) {
    let Some(first_accepted_index) = accepted.first().map(|(index, _)| *index) else {
        return;
    };
    for (index, _) in accepted {
        outcomes[*index] = Some(Err(error.clone()));
    }
    for outcome in outcomes.iter_mut().skip(first_accepted_index + 1) {
        if matches!(outcome, Some(Err(_))) {
            *outcome = Some(Err(error.clone()));
        }
    }
}

fn finish_batch_outcomes_with_aliases(
    mut outcomes: Vec<Option<Result<ApiCommitResponse, CoreError>>>,
    aliases: &[(usize, usize)],
) -> Vec<Result<ApiCommitResponse, CoreError>> {
    for (alias_index, primary_index) in aliases {
        let primary_outcome = outcomes
            .get(*primary_index)
            .and_then(Clone::clone)
            .unwrap_or_else(|| Err(CoreError::Store("missing primary batch outcome".to_owned())));
        outcomes[*alias_index] = Some(primary_outcome);
    }
    outcomes
        .into_iter()
        .map(|outcome| {
            outcome.unwrap_or_else(|| Err(CoreError::Store("missing batch outcome".to_owned())))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use loonfs_api::{ChangeSeq, InodeId};

    #[test]
    fn invalid_wal_delta_name_key_is_namespace_corrupt() {
        let delta = WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::BindDirentry {
                delta_index: 0,
                parent_inode: InodeId(1),
                name_key: "bad/key".to_owned(),
                display_name: "file.txt".to_owned(),
                child_inode: InodeId(2),
            },
        };

        let error = commit_delta_from_wal(&delta).expect_err("invalid durable WAL name key");

        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    }

    #[test]
    fn invalid_wal_unbind_name_key_is_namespace_corrupt() {
        let delta = WalCommitDelta {
            semantic_op_index: 0,
            delta: WalDelta::UnbindDirentry {
                delta_index: 0,
                parent_inode: InodeId(1),
                name_key: "bad/key".to_owned(),
                child_inode: InodeId(2),
                bind_seq: ChangeSeq(1),
                bind_delta_index: 0,
            },
        };

        let error = commit_delta_from_wal(&delta).expect_err("invalid durable WAL name key");

        assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    }
}
