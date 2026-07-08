use crate::cache::{MetadataTableCache, WalTailProjectionCache};
use crate::commit_engine::NamespaceMutationCandidate;
use crate::context::MutationContext;
use crate::error::Result as CoreResult;
use crate::namespace::{bootstrap, delete, fork, BootstrapNamespaceError};
use crate::options::{BootstrapOptions, DeleteNamespaceOptions, WriteOptions};
use crate::path::query::{load_metadata_view, LoadedMetadataView, ReadLoadContext};
use loonfs_api::generated_id;
use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, ChangesResponse, CommitRequest, CommitResponse,
    CompleteUploadRequest, CompleteUploadResponse, UploadContentResponse,
};
use loonfs_api::wire::control::HeadState;
use loonfs_api::EffectiveLimit;
use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq,
    ContentRef, CreateCheckpointResponse, DeleteDirectoryBehavior, DirectoryPageCursor,
    FileRevision, FileRevisionsPageCursor, InodeId, ListFileRevisionsResponse, ManifestId,
    ManifestObjectId, MutationResult, NamespaceId, NamespaceSummary, Page, PageRequest, RevisionNo,
    UploadId,
};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct RuntimeReadContext {
    head: HeadState,
    head_etag: String,
    manifest_id: ManifestId,
    manifest_object_id: ManifestObjectId,
    table_cache: Option<Arc<MetadataTableCache>>,
    tail_cache: Option<Arc<WalTailProjectionCache>>,
}

impl RuntimeReadContext {
    #[doc(hidden)]
    pub fn pinned_head(
        head: HeadState,
        head_etag: String,
        manifest_id: ManifestId,
        manifest_object_id: ManifestObjectId,
        table_cache: Option<Arc<MetadataTableCache>>,
        tail_cache: Option<Arc<WalTailProjectionCache>>,
    ) -> Self {
        Self {
            head,
            head_etag,
            manifest_id,
            manifest_object_id,
            table_cache,
            tail_cache,
        }
    }
}

fn runtime_read_load_context(options: &RuntimeReadContext) -> ReadLoadContext<'_> {
    ReadLoadContext::pinned_head(
        &options.head,
        Some(options.head_etag.as_str()),
        options.manifest_id,
        &options.manifest_object_id,
        options.table_cache.as_deref(),
        options.tail_cache.as_deref(),
    )
}

/// Internal target used by server integrations before they mint a presigned URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectPutUploadTarget {
    pub content_ref: ContentRef,
    pub object_key: String,
}

/// Internal response for preparing a direct_put session before URL signing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginDirectPutUploadTargetResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: UploadId,
    pub target: DirectPutUploadTarget,
}

/// A namespace-scoped core API.
///
/// `NamespaceEngine` owns an object store handle plus the writer identity used
/// for mutations. It is the main entrypoint for direct reads, path writes,
/// explicit commits, uploads, checkpoints, and retention work.
#[derive(Debug)]
pub struct NamespaceEngine<S> {
    store: S,
    namespace_id: NamespaceId,
    writer_id: String,
    writer_session_id: String,
    writer_version: String,
}

impl<S: ObjectStore> NamespaceEngine<S> {
    /// Starts an engine builder for the supplied object store.
    ///
    /// The builder requires a namespace id and writer id before it can build.
    pub fn builder(store: S) -> NamespaceEngineBuilder<S> {
        NamespaceEngineBuilder {
            store,
            namespace_id: None,
            writer_id: None,
            writer_session_id: None,
            writer_version: default_writer_version(),
        }
    }

    /// Returns the namespace this engine is bound to.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Returns the writer id used for epoch acquisition and commit publication.
    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    /// Returns the unique writer session id for this engine instance.
    pub fn writer_session_id(&self) -> &str {
        &self.writer_session_id
    }

    /// Returns the writer version reported in mutation context.
    pub fn writer_version(&self) -> &str {
        &self.writer_version
    }

    /// Consumes the engine and returns the underlying object store.
    pub fn into_store(self) -> S {
        self.store
    }

    /// Creates the namespace if it does not already exist.
    ///
    /// Use this before normal reads and writes for a new namespace.
    pub async fn bootstrap_namespace(
        &self,
        options: BootstrapOptions,
    ) -> Result<NamespaceSummary, BootstrapNamespaceError> {
        bootstrap::bootstrap_namespace(
            &self.store,
            &self.namespace_id,
            &self.mutation_context(),
            options.allow_existing,
        )
        .await
    }

    /// Creates a new namespace at the current head of this namespace.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    pub async fn fork_namespace(&self, target: &NamespaceId) -> CoreResult<NamespaceSummary> {
        fork::fork_namespace(
            &self.store,
            &self.namespace_id,
            target,
            &self.mutation_context(),
        )
        .await
    }

    /// Deletes this namespace: a fenced, terminal head-state transition.
    /// Commits acknowledged before the swap stay committed; everything that
    /// observes the deleted head afterward fails with `namespace_deleted`.
    pub async fn delete_namespace(
        &self,
        options: DeleteNamespaceOptions,
    ) -> CoreResult<loonfs_api::DeleteNamespaceResponse> {
        delete::delete_namespace(
            &self.store,
            &self.namespace_id,
            options,
            &self.mutation_context(),
        )
        .await
    }

    /// Resolves one absolute path to the authoritative entry at the current head.
    pub async fn resolve_path(&self, path: impl AsRef<str>) -> CoreResult<AuthoritativePathEntry> {
        self.resolve_path_with_context(path.as_ref(), ReadLoadContext::latest())
            .await
    }

    #[doc(hidden)]
    pub async fn resolve_path_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        options: &RuntimeReadContext,
    ) -> CoreResult<AuthoritativePathEntry> {
        self.resolve_path_with_context(path.as_ref(), runtime_read_load_context(options))
            .await
    }

    /// Lists the children of a directory path.
    pub async fn list_path(
        &self,
        path: impl AsRef<str>,
    ) -> CoreResult<Vec<AuthoritativePathEntry>> {
        self.list_path_with_context(path.as_ref(), ReadLoadContext::latest())
            .await
    }

    #[doc(hidden)]
    pub async fn list_path_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        options: &RuntimeReadContext,
    ) -> CoreResult<Vec<AuthoritativePathEntry>> {
        self.list_path_with_context(path.as_ref(), runtime_read_load_context(options))
            .await
    }

    /// Lists one page of children for a directory path.
    pub async fn list_path_page(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<DirectoryPageCursor>,
    ) -> CoreResult<Page<AuthoritativePathEntry, DirectoryPageCursor>> {
        self.list_path_page_with_context(path.as_ref(), request, ReadLoadContext::latest())
            .await
    }

    #[doc(hidden)]
    pub async fn list_path_page_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<DirectoryPageCursor>,
        options: &RuntimeReadContext,
    ) -> CoreResult<Page<AuthoritativePathEntry, DirectoryPageCursor>> {
        self.list_path_page_with_context(path.as_ref(), request, runtime_read_load_context(options))
            .await
    }

    /// Reads the current bytes for a file path.
    ///
    /// Content bytes are validated against the file's `content_ref` before they
    /// are returned.
    pub async fn read_file(&self, path: impl AsRef<str>) -> CoreResult<AuthoritativeFileBytes> {
        self.read_file_with_context(path.as_ref(), ReadLoadContext::latest())
            .await
    }

    #[doc(hidden)]
    pub async fn read_file_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        options: &RuntimeReadContext,
    ) -> CoreResult<AuthoritativeFileBytes> {
        self.read_file_with_context(path.as_ref(), runtime_read_load_context(options))
            .await
    }

    /// Lists retained revisions for the file currently visible at `path`.
    pub async fn list_file_revisions(
        &self,
        path: impl AsRef<str>,
    ) -> CoreResult<ListFileRevisionsResponse> {
        self.list_file_revisions_with_context(path.as_ref(), ReadLoadContext::latest())
            .await
    }

    #[doc(hidden)]
    pub async fn list_file_revisions_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        options: &RuntimeReadContext,
    ) -> CoreResult<ListFileRevisionsResponse> {
        self.list_file_revisions_with_context(path.as_ref(), runtime_read_load_context(options))
            .await
    }

    /// Lists one page of retained revisions for the file currently visible at `path`.
    pub async fn list_file_revisions_page(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> CoreResult<Page<FileRevision, FileRevisionsPageCursor>> {
        self.list_file_revisions_page_with_context(
            path.as_ref(),
            request,
            ReadLoadContext::latest(),
        )
        .await
    }

    #[doc(hidden)]
    pub async fn list_file_revisions_page_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<FileRevisionsPageCursor>,
        options: &RuntimeReadContext,
    ) -> CoreResult<Page<FileRevision, FileRevisionsPageCursor>> {
        self.list_file_revisions_page_with_context(
            path.as_ref(),
            request,
            runtime_read_load_context(options),
        )
        .await
    }

    /// Lists retained revisions for a file inode, independent of its current path.
    pub async fn list_file_revisions_for_inode(
        &self,
        inode_id: InodeId,
    ) -> CoreResult<ListFileRevisionsResponse> {
        self.list_file_revisions_for_inode_with_context(inode_id, ReadLoadContext::latest())
            .await
    }

    #[doc(hidden)]
    pub async fn list_file_revisions_for_inode_with_runtime_context(
        &self,
        inode_id: InodeId,
        options: &RuntimeReadContext,
    ) -> CoreResult<ListFileRevisionsResponse> {
        self.list_file_revisions_for_inode_with_context(
            inode_id,
            runtime_read_load_context(options),
        )
        .await
    }

    /// Lists one page of retained revisions for a file inode.
    pub async fn list_file_revisions_for_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
    ) -> CoreResult<Page<FileRevision, FileRevisionsPageCursor>> {
        self.list_file_revisions_for_inode_page_with_context(
            inode_id,
            request,
            ReadLoadContext::latest(),
        )
        .await
    }

    #[doc(hidden)]
    pub async fn list_file_revisions_for_inode_page_with_runtime_context(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        options: &RuntimeReadContext,
    ) -> CoreResult<Page<FileRevision, FileRevisionsPageCursor>> {
        self.list_file_revisions_for_inode_page_with_context(
            inode_id,
            request,
            runtime_read_load_context(options),
        )
        .await
    }

    /// Reads a retained revision for the file currently visible at `path`.
    pub async fn read_file_revision(
        &self,
        path: impl AsRef<str>,
        revision_no: RevisionNo,
    ) -> CoreResult<AuthoritativeFileBytes> {
        self.read_file_revision_with_context(path.as_ref(), revision_no, ReadLoadContext::latest())
            .await
    }

    #[doc(hidden)]
    pub async fn read_file_revision_with_runtime_context(
        &self,
        path: impl AsRef<str>,
        revision_no: RevisionNo,
        options: &RuntimeReadContext,
    ) -> CoreResult<AuthoritativeFileBytes> {
        self.read_file_revision_with_context(
            path.as_ref(),
            revision_no,
            runtime_read_load_context(options),
        )
        .await
    }

    /// Reads a retained revision by stable inode id.
    pub async fn read_file_revision_for_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> CoreResult<Vec<u8>> {
        self.read_file_revision_for_inode_with_context(
            inode_id,
            revision_no,
            ReadLoadContext::latest(),
        )
        .await
    }

    #[doc(hidden)]
    pub async fn read_file_revision_for_inode_with_runtime_context(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: &RuntimeReadContext,
    ) -> CoreResult<Vec<u8>> {
        self.read_file_revision_for_inode_with_context(
            inode_id,
            revision_no,
            runtime_read_load_context(options),
        )
        .await
    }

    async fn load_read_view<'a>(
        &'a self,
        context: ReadLoadContext<'a>,
    ) -> CoreResult<LoadedMetadataView<'a, S>> {
        load_metadata_view(&self.store, &self.namespace_id, context).await
    }

    async fn resolve_path_with_context(
        &self,
        path: &str,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<AuthoritativePathEntry> {
        let view = self.load_read_view(context).await?;
        view.resolve_path(path).await
    }

    async fn list_path_with_context(
        &self,
        path: &str,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<Vec<AuthoritativePathEntry>> {
        let view = self.load_read_view(context).await?;
        view.list_path(path).await
    }

    async fn list_path_page_with_context(
        &self,
        path: &str,
        request: PageRequest<DirectoryPageCursor>,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<Page<AuthoritativePathEntry, DirectoryPageCursor>> {
        let view = self.load_read_view(context).await?;
        view.list_path_page(path, request).await
    }

    async fn read_file_with_context(
        &self,
        path: &str,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<AuthoritativeFileBytes> {
        let view = self.load_read_view(context).await?;
        view.read_file_bytes(&self.store, path).await
    }

    async fn list_file_revisions_with_context(
        &self,
        path: &str,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<ListFileRevisionsResponse> {
        let view = self.load_read_view(context).await?;
        view.list_file_revisions(path).await
    }

    async fn list_file_revisions_page_with_context(
        &self,
        path: &str,
        request: PageRequest<FileRevisionsPageCursor>,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<Page<FileRevision, FileRevisionsPageCursor>> {
        let view = self.load_read_view(context).await?;
        view.list_file_revisions_page(path, request).await
    }

    async fn list_file_revisions_for_inode_with_context(
        &self,
        inode_id: InodeId,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<ListFileRevisionsResponse> {
        let view = self.load_read_view(context).await?;
        view.list_file_revisions_for_inode(inode_id).await
    }

    async fn list_file_revisions_for_inode_page_with_context(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<Page<FileRevision, FileRevisionsPageCursor>> {
        let view = self.load_read_view(context).await?;
        view.list_file_revisions_for_inode_page(inode_id, request)
            .await
    }

    async fn read_file_revision_with_context(
        &self,
        path: &str,
        revision_no: RevisionNo,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<AuthoritativeFileBytes> {
        let view = self.load_read_view(context).await?;
        view.read_file_revision_bytes(&self.store, path, revision_no)
            .await
    }

    async fn read_file_revision_for_inode_with_context(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        context: ReadLoadContext<'_>,
    ) -> CoreResult<Vec<u8>> {
        let view = self.load_read_view(context).await?;
        view.read_file_revision_bytes_for_inode(&self.store, inode_id, revision_no)
            .await
    }

    /// Writes file bytes to a path.
    ///
    /// The bytes become durable content first. Metadata is published only after
    /// that content is safe to reference.
    pub async fn put_file(
        &self,
        path: impl AsRef<str>,
        bytes: impl AsRef<[u8]>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::put_file_bytes(
            &self.store,
            &self.namespace_id,
            path.as_ref(),
            bytes.as_ref(),
            options.put_behavior,
            &self.mutation_context(),
            options.commit_id.as_ref(),
        )
        .await
    }

    /// Publishes a file revision that points at an already-durable content ref.
    ///
    /// Use this when the caller staged content separately.
    pub async fn put_file_content_ref(
        &self,
        path: impl AsRef<str>,
        content_ref: ContentRef,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::put_file_content_ref(
            &self.store,
            &self.namespace_id,
            path.as_ref(),
            content_ref,
            options.put_behavior,
            &self.mutation_context(),
            options.commit_id.as_ref(),
        )
        .await
    }

    /// Creates a directory at an absolute path.
    pub async fn create_directory(
        &self,
        path: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::create_directory_path(
            &self.store,
            &self.namespace_id,
            path.as_ref(),
            &self.mutation_context(),
            options.commit_id.as_ref(),
        )
        .await
    }

    /// Deletes a file or directory path.
    pub async fn delete_path(
        &self,
        path: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        let commit_id = options.commit_id.as_ref();
        if options.delete_behavior == DeleteDirectoryBehavior::Recursive {
            crate::path::write::ops::delete_path(
                &self.store,
                &self.namespace_id,
                path.as_ref(),
                &self.mutation_context(),
                commit_id,
            )
            .await
        } else {
            crate::path::write::ops::delete_path_non_recursive(
                &self.store,
                &self.namespace_id,
                path.as_ref(),
                &self.mutation_context(),
                commit_id,
            )
            .await
        }
    }

    /// Moves a path within the same namespace.
    pub async fn move_path(
        &self,
        source: impl AsRef<str>,
        dest: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::move_path(
            &self.store,
            &self.namespace_id,
            source.as_ref(),
            dest.as_ref(),
            &self.mutation_context(),
            options.commit_id.as_ref(),
        )
        .await
    }

    /// Copies a file path within the same namespace.
    pub async fn copy_path(
        &self,
        source: impl AsRef<str>,
        dest: impl AsRef<str>,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::copy_file_path(
            &self.store,
            &self.namespace_id,
            source.as_ref(),
            dest.as_ref(),
            &self.mutation_context(),
            options.commit_id.as_ref(),
        )
        .await
    }

    /// Restores a prior file revision by appending a new current revision.
    pub async fn restore_file_revision(
        &self,
        path: impl AsRef<str>,
        source_revision_no: RevisionNo,
        options: WriteOptions,
    ) -> CoreResult<MutationResult> {
        crate::path::write::ops::restore_file_revision(
            &self.store,
            &self.namespace_id,
            path.as_ref(),
            source_revision_no,
            &self.mutation_context(),
            options.commit_id.as_ref(),
        )
        .await
    }

    /// Submits one explicit semantic commit request.
    ///
    /// This is the lower-level surface used by clients that need their own
    /// commit ids, preconditions, and operation lists.
    pub async fn commit_operations(&self, request: CommitRequest) -> CoreResult<CommitResponse> {
        crate::protocol::commit_operations(
            &self.store,
            &self.namespace_id,
            request,
            &self.mutation_context(),
        )
        .await
    }

    /// Submits explicit semantic commit requests as one publication attempt.
    pub async fn commit_operations_batch(
        &self,
        requests: Vec<CommitRequest>,
    ) -> Vec<CoreResult<CommitResponse>> {
        crate::protocol::commit_operations_batch(
            &self.store,
            &self.namespace_id,
            requests,
            &self.mutation_context(),
        )
        .await
    }

    /// Publishes already-classified namespace mutation candidates.
    ///
    /// Server code uses this to batch path intents and explicit commits through
    /// one namespace publisher.
    pub async fn publish_namespace_mutations_batch(
        &self,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<CoreResult<CommitResponse>> {
        self.publish_namespace_mutations_batch_gated(candidates, None)
            .await
    }

    /// [`Self::publish_namespace_mutations_batch`] with a content durability
    /// gate awaited between the WAL write and the head CAS; the gate is a
    /// future, so it rides an explicit parameter instead of an options
    /// struct.
    pub async fn publish_namespace_mutations_batch_gated(
        &self,
        candidates: Vec<NamespaceMutationCandidate>,
        content_gate: Option<crate::publish::ContentDurabilityGate>,
    ) -> Vec<CoreResult<CommitResponse>> {
        crate::commit_engine::publish_namespace_mutations_batch_gated(
            &self.store,
            &self.namespace_id,
            candidates,
            &self.mutation_context(),
            content_gate,
        )
        .await
    }

    /// Reads up to `limit` committed changes after `after_seq`.
    pub async fn list_changes_after(
        &self,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
    ) -> CoreResult<ChangesResponse> {
        crate::protocol::list_changes_after(&self.store, &self.namespace_id, after_seq, limit).await
    }

    /// Starts a durable upload session with explicit transport options.
    pub async fn begin_upload(
        &self,
        request: BeginUploadRequest,
    ) -> CoreResult<BeginUploadResponse> {
        crate::protocol::begin_upload(
            &self.store,
            &self.namespace_id,
            request,
            &self.mutation_context(),
        )
        .await
    }

    /// Starts a direct_put upload session and returns the internal object key to sign.
    pub async fn begin_direct_put_upload_target(
        &self,
        content_ref: ContentRef,
    ) -> CoreResult<BeginDirectPutUploadTargetResponse> {
        crate::protocol::begin_direct_put_upload_target(
            &self.store,
            &self.namespace_id,
            content_ref,
            &self.mutation_context(),
        )
        .await
    }

    /// Uploads whole-file content into an upload session.
    pub async fn upload_content(
        &self,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> CoreResult<UploadContentResponse> {
        crate::protocol::upload_content(
            &self.store,
            &self.namespace_id,
            upload_id,
            bytes,
            &self.mutation_context(),
        )
        .await
    }

    /// Completes an upload session when the expected content ref matches.
    pub async fn complete_upload(
        &self,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> CoreResult<CompleteUploadResponse> {
        crate::protocol::complete_upload(
            &self.store,
            &self.namespace_id,
            upload_id,
            request,
            &self.mutation_context(),
        )
        .await
    }

    /// Creates or reuses a checkpoint for the current namespace head.
    ///
    /// A checkpoint pins a manifest version for retention/provenance. If the
    /// current head has no manifest yet, this first publishes one for the
    /// current durable namespace state; it is not a request to compact metadata.
    pub async fn create_checkpoint(&self) -> CoreResult<CreateCheckpointResponse> {
        crate::checkpoint::create_checkpoint(
            &self.store,
            &self.namespace_id,
            &self.mutation_context(),
        )
        .await
    }

    /// Advances the retention floor when a verified checkpoint makes it safe.
    pub async fn advance_retention_floor(&self) -> CoreResult<AdvanceRetentionResponse> {
        crate::checkpoint::advance_retention_floor(
            &self.store,
            &self.namespace_id,
            &self.mutation_context(),
        )
        .await
    }

    fn mutation_context(&self) -> MutationContext {
        MutationContext {
            writer_id: self.writer_id.clone(),
            writer_session_id: self.writer_session_id.clone(),
            writer_version: self.writer_version.clone(),
            now_ms: current_time_ms(),
        }
    }
}

/// Builder for [`NamespaceEngine`].
///
/// The builder keeps construction explicit: choose a namespace, choose the
/// writer identity, then build the engine.
#[derive(Debug)]
pub struct NamespaceEngineBuilder<S> {
    store: S,
    namespace_id: Option<NamespaceId>,
    writer_id: Option<String>,
    writer_session_id: Option<String>,
    writer_version: String,
}

impl<S: ObjectStore> NamespaceEngineBuilder<S> {
    /// Sets the namespace this engine will operate on.
    pub fn namespace_id(mut self, namespace_id: NamespaceId) -> Self {
        self.namespace_id = Some(namespace_id);
        self
    }

    /// Sets the writer identity used for epoch acquisition and commits.
    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    #[doc(hidden)]
    pub fn writer_session_id(mut self, writer_session_id: impl Into<String>) -> Self {
        self.writer_session_id = Some(writer_session_id.into());
        self
    }

    /// Sets a human-readable writer version.
    pub fn writer_version(mut self, writer_version: impl Into<String>) -> Self {
        self.writer_version = writer_version.into();
        self
    }

    /// Builds the engine after required fields are present.
    pub fn build(self) -> Result<NamespaceEngine<S>, NamespaceEngineBuildError> {
        let namespace_id = self
            .namespace_id
            .ok_or(NamespaceEngineBuildError::MissingNamespace)?;
        let writer_id = self
            .writer_id
            .ok_or(NamespaceEngineBuildError::MissingWriter)?;
        if writer_id.trim().is_empty() {
            return Err(NamespaceEngineBuildError::EmptyWriter);
        }
        if self.writer_version.trim().is_empty() {
            return Err(NamespaceEngineBuildError::EmptyWriterVersion);
        }

        Ok(NamespaceEngine {
            store: self.store,
            namespace_id,
            writer_id,
            writer_session_id: self
                .writer_session_id
                .unwrap_or_else(|| generated_id("wrs")),
            writer_version: self.writer_version,
        })
    }
}

/// Error returned when a [`NamespaceEngine`] cannot be built.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum NamespaceEngineBuildError {
    /// A namespace id was not supplied.
    #[error("namespace is required")]
    MissingNamespace,
    /// A writer id was not supplied.
    #[error("writer identity is required")]
    MissingWriter,
    /// The writer id was empty or whitespace.
    #[error("writer identity must not be empty")]
    EmptyWriter,
    /// The writer version was empty or whitespace.
    #[error("writer version must not be empty")]
    EmptyWriterVersion,
}

fn default_writer_version() -> String {
    format!("loonfs-core/{}", env!("CARGO_PKG_VERSION"))
}

#[allow(clippy::disallowed_methods)]
fn current_time_ms() -> u64 {
    // Engine wrappers set request timestamps at this API boundary; core replay remains deterministic.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    #[test]
    fn namespace_engine_builds_with_required_identity() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let engine = NamespaceEngine::builder(store)
            .namespace_id(namespace_id.clone())
            .writer_id("writer-a")
            .build()
            .expect("engine builds");

        assert_eq!(engine.namespace_id(), &namespace_id);
        assert_eq!(engine.writer_id(), "writer-a");
        assert!(!engine.writer_version().is_empty());
    }

    #[test]
    fn namespace_engine_builder_rejects_missing_required_fields() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let err = NamespaceEngine::builder(store)
            .build()
            .expect_err("missing namespace");
        assert_eq!(err, NamespaceEngineBuildError::MissingNamespace);

        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let err = NamespaceEngine::builder(store)
            .namespace_id(NamespaceId::parse("demo").expect("valid namespace id"))
            .build()
            .expect_err("missing writer");
        assert_eq!(err, NamespaceEngineBuildError::MissingWriter);
    }
}
