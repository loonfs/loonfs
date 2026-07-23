//! The write-capable runtime handle.

use super::{owning_runtime, FsReader, HandleBuilderCore};
use crate::background::{BackgroundWork, FsBackgroundWork};
use crate::config::default_writer_version;
use crate::fs::FsCore;
use crate::metrics::ObjectStoreMetricsRecorder;
use crate::publish::{NamespaceMutationCandidate, PreparedContent};
use crate::uploads::BeginDirectPutUploadTargetResponse;
use crate::{
    BeginUploadRequest, BeginUploadResponse, CapabilityDocument, ChangeSeq, CommitRequest,
    CommitResponse, CompleteUploadRequest, CompleteUploadResponse, ContentRef, CopyOptions,
    CreateDirectoryOptions, CreateNamespaceOptions, DeleteNamespaceOptions,
    DeleteNamespaceResponse, DeleteOptions, InodeId, MoveOptions, NamespaceId, NamespaceSummary,
    PutFileOptions, RestoreRevisionOptions, Result, RevisionNo, RuntimeCacheConfig,
    RuntimeCacheStats, RuntimeError, SharedObjectStore, StoreConfig, TraceMode, TraceStoreKind,
    UndeleteOptions, UploadContentResponse, UploadId,
};
use std::num::NonZeroUsize;
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

    /// Shared runtime core, for in-crate front-ends.
    pub(crate) fn core(&self) -> &FsCore {
        &self.core
    }

    /// This writer's publication service (see [`crate::publisher`]).
    ///
    /// The direct mutation methods on this handle submit through the same
    /// service; hosts that classify their own mutation candidates — the
    /// reference server, for example — submit here directly. Clones share
    /// the writer's per-namespace publishers.
    pub fn publisher(&self) -> crate::publisher::PublisherRegistry {
        self.core.publisher().clone()
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
    ) -> Result<CommitResponse> {
        self.core
            .put_file_bytes(namespace_id, absolute_path, bytes, options)
            .await
    }

    /// Stages file bytes as durable content for later publication.
    ///
    /// This performs one content PUT and no content reads. Preparations may
    /// run concurrently on this writer; pass the result to
    /// [`Self::put_file_prepared`] or [`Self::commit_operations_prepared`].
    pub async fn prepare_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        self.core.prepare_file_bytes(namespace_id, bytes).await
    }

    /// Publishes a file revision from already-prepared content.
    ///
    /// Submission and publication perform no content I/O.
    pub async fn put_file_prepared(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        prepared_content: PreparedContent,
        options: PutFileOptions,
    ) -> Result<CommitResponse> {
        self.core
            .put_file_prepared(namespace_id, absolute_path, prepared_content, options)
            .await
    }

    /// Publishes a file revision that points at an already-durable content ref.
    ///
    /// This explicitly slow helper reads the full object to prove durability
    /// before publication. Callers that already hold proof should prefer
    /// [`Self::put_file_prepared`].
    pub async fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> Result<CommitResponse> {
        self.core
            .put_file_content_ref(namespace_id, absolute_path, content_ref, options)
            .await
    }

    /// Fully validates an existing content ref for later publication.
    ///
    /// This performs one content HEAD followed by one full content GET and
    /// digest check. Later prepared publication performs no content I/O.
    pub async fn prepare_content_ref(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> Result<PreparedContent> {
        self.core
            .prepare_content_ref(namespace_id, content_ref)
            .await
    }

    /// Verifies a namespace-authorized wire token and binds its proof to the
    /// namespace's verified content store.
    ///
    /// The outer result reports runtime failure. The inner result carries
    /// token verification failure as data rather than a runtime error.
    pub async fn prepare_content_token(
        &self,
        namespace_id: &NamespaceId,
        secret: &str,
        token: &loonfs_api::v0::ValidatedContentToken,
        now_ms: u64,
    ) -> Result<std::result::Result<PreparedContent, loonfs_core::content::ContentTokenError>> {
        self.core
            .prepare_content_token(namespace_id, secret, token, now_ms)
            .await
    }

    /// Creates a directory at an absolute path.
    pub async fn create_directory(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> Result<CommitResponse> {
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
    ) -> Result<CommitResponse> {
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
    ) -> Result<CommitResponse> {
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
    ) -> Result<CommitResponse> {
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
    ) -> Result<CommitResponse> {
        self.core
            .restore_file_revision(namespace_id, absolute_path, source_revision_no, options)
            .await
    }

    /// Recovers a deleted file or subtree: clears the tombstone rooted at
    /// `inode_id` (the id the delete reported, also visible in the change
    /// feed) and binds it at `absolute_path`. Fails with `not_deleted`
    /// when the inode is not the root of a live deletion.
    pub async fn undelete(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        absolute_path: &str,
        options: UndeleteOptions,
    ) -> Result<CommitResponse> {
        self.core
            .undelete(
                namespace_id,
                inode_id,
                deleted_at_seq,
                absolute_path,
                options,
            )
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

    /// Starts a direct-put upload session and returns the internal target
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

    /// Completes an upload session and returns proof for later publication.
    ///
    /// Service-proxied completion performs no content-blob I/O. Direct-put
    /// completion performs one content-blob HEAD and no content-blob GET.
    /// Publishing the returned proof performs no content I/O.
    pub async fn complete_upload_prepared(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<(CompleteUploadResponse, PreparedContent)> {
        self.core
            .complete_upload_prepared(namespace_id, upload_id, request)
            .await
    }

    /// Submits one explicit semantic commit request.
    ///
    /// This is the lower-level surface for clients that need their own commit
    /// ids, preconditions, and operation lists. Operations with external
    /// content refs fail with `content_not_prepared`; use
    /// [`Self::commit_operations_prepared`] to attach proofs.
    pub async fn commit_operations(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> Result<CommitResponse> {
        self.core.commit_operations(namespace_id, request).await
    }

    /// Submits one semantic commit request with prepared content proofs.
    ///
    /// Submission and publication perform no content I/O. One prepared value
    /// covers every operation that uses its content ref.
    pub async fn commit_operations_prepared(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
        prepared_content: Vec<PreparedContent>,
    ) -> Result<CommitResponse> {
        self.core
            .commit_operations_prepared(namespace_id, request, prepared_content)
            .await
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

    /// Publishes already-classified candidates as one batch; results match
    /// candidates in order. (The server routes through its own publisher
    /// registry, not this method.)
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

    /// Shuts down writer-scheduled background work: settles admitted
    /// publications whose callers are gone, then rejects new maintenance
    /// scheduling and waits for in-flight maintenance tasks to settle,
    /// surfacing panics. Work that claimed its maintenance slot but has not
    /// started when the shutdown lands is refused, never left running
    /// unobserved.
    ///
    /// Foreground calls remain usable afterward; this settles only
    /// handle-owned background work, and with
    /// [`FsBackgroundWork::ManualOnly`] it is nearly trivial. For a
    /// terminal shutdown that also refuses later submissions with
    /// `shutting_down`, call
    /// [`PublisherRegistry::close_admission`](crate::publisher::PublisherRegistry::close_admission)
    /// via [`Self::publisher`] first, as the reference server does.
    /// Dropping the handle without calling this is best-effort cleanup,
    /// not the documented graceful shutdown path.
    pub async fn shutdown_background(&self) -> Result<()> {
        // Publications first — they can schedule the maintenance the second
        // step settles. Draining without closing admission keeps the
        // handle usable; a caller that keeps submitting concurrently just
        // keeps the drain waiting.
        self.core.publisher().drain().await?;
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
    max_concurrent_maintenance: usize,
}

impl FsWriterBuilder {
    fn new(core: HandleBuilderCore) -> Self {
        Self {
            core,
            writer_id: None,
            writer_version: default_writer_version(),
            background_work: FsBackgroundWork::ManualOnly,
            max_concurrent_maintenance: crate::config::DEFAULT_MAX_CONCURRENT_MAINTENANCE,
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

    /// Caps how many writer-scheduled maintenance ticks may run at once
    /// across all namespaces this writer serves. Each namespace already runs
    /// at most one tick at a time; this bounds the fan-out when many
    /// namespaces cross their thresholds together. Skipped ticks are
    /// rescheduled by the next over-threshold publish. Defaults to
    /// [`crate::DEFAULT_MAX_CONCURRENT_MAINTENANCE`]. The limit must be
    /// greater than zero; [`FsBackgroundWork::ManualOnly`] is the only way
    /// to disable scheduling.
    pub fn max_concurrent_maintenance(mut self, max_concurrent_maintenance: usize) -> Self {
        self.max_concurrent_maintenance = max_concurrent_maintenance;
        self
    }

    /// Caps the file content size the buffered read APIs will materialize
    /// for one call, checked against resolved metadata before any content
    /// fetch; over-limit reads fail with `content_too_large`. Unset by
    /// default: embedded callers read files of any size. Servers set this
    /// so one proxied read cannot buffer arbitrarily large content.
    pub fn max_read_content_bytes(mut self, max_read_content_bytes: u64) -> Self {
        self.core.max_read_content_bytes = Some(max_read_content_bytes);
        self
    }

    /// Sets the minimum interval between publication starts per namespace,
    /// in milliseconds (see [`crate::publisher`]).
    ///
    /// A cold namespace publishes immediately; the interval only paces
    /// follow-up batches, so concurrent publishes amortize into fewer,
    /// larger WAL segments — with each caller still awaiting its own
    /// durable, visible result. Defaults to 15 ms; zero keeps only the
    /// batching that in-flight publications force.
    pub fn min_publish_interval_ms(mut self, min_publish_interval_ms: u64) -> Self {
        self.core.min_publish_interval_ms = min_publish_interval_ms;
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

    /// Registers a synchronous observer called after each successful durable
    /// publication with the namespace and the batch's highest committed
    /// sequence.
    ///
    /// The observer runs on the publication task and must not block. Use a
    /// non-blocking channel send to hand work to another task. Writers that
    /// register no observer retain the default publication behavior.
    pub fn publish_observer(
        mut self,
        observer: impl Fn(&NamespaceId, ChangeSeq) + Send + Sync + 'static,
    ) -> Self {
        self.core.publish_observer = Some(Arc::new(observer));
        self
    }

    /// Opens the writer inside the Tokio runtime that will own it. Any
    /// background work the writer schedules is spawned on that runtime.
    pub async fn build(self) -> Result<FsWriter> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| RuntimeError::Config("writer_id is required".to_owned()))?;
        let max_concurrent_maintenance = NonZeroUsize::new(self.max_concurrent_maintenance)
            .ok_or_else(|| {
                RuntimeError::Config(
                    "`max_concurrent_maintenance` must be greater than zero; \
                     `FsBackgroundWork::ManualOnly` disables scheduling"
                        .to_owned(),
                )
            })?;
        let background = BackgroundWork::new(
            self.background_work,
            Some(owning_runtime()?),
            max_concurrent_maintenance,
        );
        Ok(FsWriter {
            core: self.core.open(writer_id, self.writer_version, background)?,
        })
    }
}
