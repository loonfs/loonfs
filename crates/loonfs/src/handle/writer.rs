//! The write-capable runtime handle.

use super::{owning_runtime, FsReader, HandleBuilderCore};
use crate::background::{BackgroundWork, FsBackgroundWork};
use crate::config::default_writer_version;
use crate::fs::FsCore;
use crate::publish::NamespaceMutationCandidate;
use crate::{
    BeginDirectPutUploadTargetResponse, BeginUploadRequest, BeginUploadResponse,
    CapabilityDocument, CommitRequest, CommitResponse, CompleteUploadRequest,
    CompleteUploadResponse, ContentRef, CopyOptions, CreateDirectoryOptions,
    CreateNamespaceOptions, DeleteNamespaceOptions, DeleteNamespaceResponse, DeleteOptions,
    InodeId, MoveOptions, MutationResult, NamespaceId, NamespaceSummary,
    ObjectStoreMetricsRecorder, PutFileOptions, RestoreRevisionOptions, Result, RevisionNo,
    RuntimeCacheConfig, RuntimeCacheStats, RuntimeError, SharedObjectStore, StoreConfig, TraceMode,
    TraceStoreKind, UploadContentResponse, UploadId,
};
use std::sync::Arc;

/// Write-capable handle for normal application and server use.
///
/// `FsWriter` owns a writer session identity and the full mutation surface:
/// file and directory mutations, commit publication, uploads, and namespace
/// lifecycle. With [`FsBackgroundWork::Enabled`] it may also schedule
/// non-destructive maintenance after writes, spawned on its owning runtime.
/// Retention advancement and garbage collection stay explicit
/// [`FsAdmin`](crate::FsAdmin) work.
///
/// The handle is runtime-bound: open it with `build().await` inside the
/// long-lived Tokio runtime that will drive it, and do not share one writer
/// across unrelated runtimes — open another from [`StoreConfig`] instead.
/// `FsWriter` is cheap to clone; clones share the session, caches, and
/// background-work state.
#[derive(Clone)]
pub struct FsWriter {
    core: FsCore,
}

impl FsWriter {
    /// Starts a writer builder that constructs its object-store client from
    /// configuration inside this handle's runtime ownership domain.
    pub fn builder(store_config: StoreConfig) -> FsWriterBuilder {
        FsWriterBuilder::new(HandleBuilderCore::from_config(store_config))
    }

    /// Starts a writer builder over a caller-supplied store.
    ///
    /// For callers who know the store is safe in this handle's runtime
    /// ownership domain. Do not use it to share one provider client across
    /// unrelated runtimes; open another handle from [`StoreConfig`] instead.
    pub fn builder_with_store(store: SharedObjectStore) -> FsWriterBuilder {
        FsWriterBuilder::new(HandleBuilderCore::from_store(store))
    }

    /// Derives a read-only handle over this writer's store and caches.
    ///
    /// The reader lives in the writer's runtime ownership domain and sees
    /// its cache updates immediately; use it for read paths in the same
    /// process, such as a server's read endpoints. For a reader driven by a
    /// different runtime, open one with [`FsReader::builder`].
    pub fn reader(&self) -> FsReader {
        FsReader::from_core(self.core.clone())
    }

    /// Shared runtime core, for in-crate front-ends like the batching
    /// publisher.
    pub(crate) fn core(&self) -> &FsCore {
        &self.core
    }

    /// Returns the capability document for this embedded build (API spec,
    /// "Capability discovery").
    pub fn capabilities(&self) -> CapabilityDocument {
        self.core.capabilities()
    }

    /// Snapshots the runtime cache counters.
    pub fn runtime_cache_stats(&self) -> RuntimeCacheStats {
        self.core.runtime_cache_stats()
    }

    /// Creates a namespace, bootstrapping its durable state.
    ///
    /// With `options.allow_existing`, an already-existing namespace is
    /// treated as success.
    pub async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> Result<NamespaceSummary> {
        self.core.create_namespace(namespace_id, options).await
    }

    /// Forks `source` into `target` at the source's current head.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    pub async fn fork_namespace(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> Result<NamespaceSummary> {
        self.core.fork_namespace(source, target).await
    }

    /// Deletes a namespace: a fenced, terminal head transition (format
    /// spec, "Tombstones and deletion"). Commits acknowledged before the
    /// swap stay committed; reads, writes, forks, and re-creation of the id
    /// fail with `namespace_deleted` afterward. Deletion does not reclaim
    /// storage; reclamation is explicit garbage collection.
    pub async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        self.core.delete_namespace(namespace_id, options).await
    }

    /// Writes file bytes to a path.
    ///
    /// The bytes become durable content first; metadata referencing them is
    /// published only afterward. `options.behavior` selects create-only or
    /// replace semantics.
    pub async fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        self.core
            .put_file_bytes(namespace_id, absolute_path, bytes, options)
            .await
    }

    /// Publishes a file revision that points at an already-durable content
    /// ref.
    ///
    /// Use this when content was staged separately, for example through the
    /// upload protocol.
    pub async fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> Result<MutationResult> {
        self.core
            .put_file_content_ref(namespace_id, absolute_path, content_ref, options)
            .await
    }

    /// Creates a directory at an absolute path.
    pub async fn create_directory(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> Result<MutationResult> {
        self.core
            .create_directory(namespace_id, absolute_path, options)
            .await
    }

    /// Deletes a file or directory path.
    ///
    /// Deletion is tombstone-first: the commit hides the path without erasing
    /// history. Physical reclamation is explicit garbage collection.
    pub async fn delete_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> Result<MutationResult> {
        self.core
            .delete_path(namespace_id, absolute_path, options)
            .await
    }

    /// Moves a path within the same namespace.
    pub async fn move_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> Result<MutationResult> {
        self.core
            .move_path(namespace_id, from_path, to_path, options)
            .await
    }

    /// Copies a file to a new path in the same namespace. The new file
    /// reuses the source revision's content reference: no bytes are copied.
    pub async fn copy_path(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> Result<MutationResult> {
        self.core
            .copy_path(namespace_id, from_path, to_path, options)
            .await
    }

    /// Restores a prior file revision by appending a new current revision.
    pub async fn restore_file_revision(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        source_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<MutationResult> {
        self.core
            .restore_file_revision(namespace_id, absolute_path, source_revision_no, options)
            .await
    }

    /// Restores a prior revision of an inode, guarded by a base-revision
    /// precondition.
    ///
    /// The commit appends a new current revision from `source_revision_no`
    /// and fails if the inode's current revision is no longer
    /// `base_revision_no`.
    pub async fn restore_file_revision_for_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
        options: RestoreRevisionOptions,
    ) -> Result<CommitResponse> {
        self.core
            .restore_file_revision_for_inode(
                namespace_id,
                inode_id,
                source_revision_no,
                base_revision_no,
                options,
            )
            .await
    }

    /// Starts a durable upload session for a namespace.
    pub async fn begin_upload(
        &self,
        namespace_id: &NamespaceId,
        request: BeginUploadRequest,
    ) -> Result<BeginUploadResponse> {
        self.core.begin_upload(namespace_id, request).await
    }

    /// Starts a direct_put upload session and returns the internal target
    /// for server-side signing.
    pub async fn begin_direct_put_upload_target(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> Result<BeginDirectPutUploadTargetResponse> {
        self.core
            .begin_direct_put_upload_target(namespace_id, content_ref)
            .await
    }

    /// Uploads whole-file content into an upload session.
    pub async fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        self.core
            .upload_content(namespace_id, upload_id, bytes)
            .await
    }

    /// Completes an upload session when the expected content ref matches.
    pub async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        self.core
            .complete_upload(namespace_id, upload_id, request)
            .await
    }

    /// Submits one explicit semantic commit request.
    ///
    /// This is the lower-level surface for clients that need their own commit
    /// ids, preconditions, and operation lists.
    pub async fn commit_operations(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        self.core.commit_operations(namespace_id, request).await
    }

    /// Submits explicit semantic commit requests as one publication attempt,
    /// returning one result per request in order.
    pub async fn commit_operations_batch(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<Result<CommitResponse>> {
        self.core
            .commit_operations_batch(namespace_id, requests)
            .await
    }

    /// Publishes already-classified namespace mutation candidates as one
    /// batch.
    ///
    /// Server code uses this to push path intents and explicit commits
    /// through one namespace publisher; results match candidates in order.
    pub async fn publish_namespace_mutations_batch(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        self.core
            .publish_namespace_mutations_batch(namespace_id, candidates)
            .await
    }

    /// Waits until every writer-scheduled maintenance task has finished,
    /// without closing the handle. Panicked tasks surface as a runtime-task
    /// error.
    pub async fn wait_for_background_work(&self) -> Result<()> {
        self.core.wait_for_background_maintenance().await
    }

    /// Closes the writer: rejects new writer-scheduled background work and
    /// waits for in-flight maintenance tasks to settle, surfacing panics.
    ///
    /// Foreground calls remain usable afterward; `close` only settles
    /// handle-owned background work, and with
    /// [`FsBackgroundWork::ManualOnly`] it is nearly trivial. Dropping the
    /// handle without closing is best-effort cleanup, not the documented
    /// graceful shutdown path.
    pub async fn close(&self) -> Result<()> {
        self.core.shut_down_background();
        self.core.wait_for_background_maintenance().await
    }
}

/// Builder for [`FsWriter`].
pub struct FsWriterBuilder {
    core: HandleBuilderCore,
    writer_id: Option<String>,
    writer_version: String,
    background_work: FsBackgroundWork,
}

impl FsWriterBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self {
            core,
            writer_id: None,
            writer_version: default_writer_version(),
            background_work: FsBackgroundWork::ManualOnly,
        }
    }

    /// Sets the writer id used by namespace mutations. Required.
    pub fn writer_id(mut self, writer_id: impl Into<String>) -> Self {
        self.writer_id = Some(writer_id.into());
        self
    }

    /// Sets the writer version used in mutation context.
    pub fn writer_version(mut self, writer_version: impl Into<String>) -> Self {
        self.writer_version = writer_version.into();
        self
    }

    /// Sets the writer's background maintenance policy.
    ///
    /// Defaults to [`FsBackgroundWork::ManualOnly`]: right for CLIs, tests,
    /// scripts, and short-lived embedders. Long-lived servers should opt
    /// into [`FsBackgroundWork::Enabled`] explicitly.
    pub fn background_work(mut self, background_work: FsBackgroundWork) -> Self {
        self.background_work = background_work;
        self
    }

    /// Sets the commit window for direct publishes, in milliseconds.
    ///
    /// A direct publish holds its namespace's window open this long so
    /// concurrent publishes join the same flush — one WAL segment, one head
    /// CAS — with each caller still awaiting its own durable, visible
    /// result. Defaults to [`crate::DEFAULT_COMMIT_WINDOW_MS`]; zero
    /// disables coalescing and publishes each submission immediately.
    pub fn commit_window_ms(mut self, commit_window_ms: u64) -> Self {
        self.core.commit_window_ms = commit_window_ms;
        self
    }

    /// Sets runtime cache behavior.
    pub fn runtime_cache(mut self, runtime_cache: RuntimeCacheConfig) -> Self {
        self.core.runtime_cache = runtime_cache;
        self
    }

    /// Sets the tracing mode label.
    pub fn trace_mode(mut self, trace_mode: TraceMode) -> Self {
        self.core.trace_mode = trace_mode;
        self
    }

    /// Sets the object-store kind label used by tracing and metrics.
    ///
    /// Config-built stores derive this automatically; setting it overrides
    /// the derived label.
    pub fn trace_store_kind(mut self, trace_store_kind: TraceStoreKind) -> Self {
        self.core.trace_store_kind = Some(trace_store_kind);
        self
    }

    /// Installs object-store metrics collection for this handle.
    ///
    /// The handle wraps its object store before opening; callers do not
    /// need to construct an instrumented store manually.
    pub fn metrics_recorder(mut self, recorder: Arc<dyn ObjectStoreMetricsRecorder>) -> Self {
        self.core.metrics_recorder = Some(recorder);
        self
    }

    /// Opens the writer inside the Tokio runtime that will own it. Any
    /// background work the writer schedules is spawned on that runtime.
    pub async fn build(self) -> Result<FsWriter> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| RuntimeError::Config("writer_id is required".to_owned()))?;
        let background = BackgroundWork::new(self.background_work, Some(owning_runtime()?));
        Ok(FsWriter {
            core: self.core.open(writer_id, self.writer_version, background)?,
        })
    }
}
