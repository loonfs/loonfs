//! [`NamespaceEngine`]: the namespace-scoped entry point for reads, writes,
//! uploads, checkpoints, and maintenance.

use crate::cache::{MetadataTableCache, WalTailProjectionCache};
use crate::checkpoint::{CheckpointFilesPage, CheckpointFilesPageCursor};
use crate::commit_engine::CommitCandidate;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::{bootstrap, fork, BootstrapNamespaceError};
use crate::options::{BootstrapOptions, DeleteNamespaceOptions};
use crate::path::read::{
    load_metadata_view, CurrentFileState, DirectDownloadTarget, LoadedMetadataView, ReadLoadContext,
};
use crate::protocol::{
    BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse, CompletedUpload,
    MultipartPartTargets,
};
use crate::storage::content::FileContentStream;
use crate::storage::content_admission::{CompletedUploadReceipt, PreparedContent};
use crate::time::current_time_ms;
use loonfs_api::options::{ListPathEntriesOptions, StatPathOptions};
use loonfs_api::v0::{
    AbortUploadResponse, BeginUploadRequest, BeginUploadResponse, ChangesResponse, CommitResponse,
    CompleteUploadRequest, CompleteUploadResponse, DirectMultipartUploadOptions,
    DirectPutContentClaim, UploadContentResponse, UploadPartChecksumClaim, UploadStatusResponse,
};
use loonfs_api::wire::control::{CheckpointOwner, HeadState, NamespaceState};
use loonfs_api::EffectiveLimit;
use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq,
    CheckpointId, ContentRef, CreateCheckpointResponse, DeleteNamespaceResponse,
    DirectoryPageCursor, FileRevision, FileRevisionsPageCursor, FlushWalResponse, InodeId,
    ListCheckpointsResponse, NamespaceId, NamespaceSummary, Page, PageRequest,
    ReleaseCheckpointResponse, RevisionNo, TrashEntry, TrashPageCursor, UploadId,
};
use loonfs_objectstore::{ByteStream, ObjectStore};
use std::num::NonZeroU64;
use std::sync::Arc;
use thiserror::Error;

/// Read context pinned by the runtime for one request. It contains the head,
/// metadata basis, and shared caches needed to serve every read from the same
/// namespace snapshot.
///
/// This type supports the `loonfs` runtime. Applications should use the
/// higher-level `loonfs` reader handles instead.
#[derive(Debug, Clone)]
pub struct RuntimeReadContext {
    pub head: HeadState,
    pub head_etag: String,
    /// Metadata basis referenced by the pinned head. This is the namespace's own
    /// root after one is published, or its genesis or fork basis before then.
    pub basis: MetadataBasis,
    pub table_cache: Arc<MetadataTableCache>,
    pub tail_cache: Arc<WalTailProjectionCache>,
}

fn runtime_read_load_context(context: &RuntimeReadContext) -> ReadLoadContext<'_> {
    ReadLoadContext::pinned_head(
        &context.head,
        Some(context.head_etag.as_str()),
        &context.basis,
        Some(&context.table_cache),
        Some(&context.tail_cache),
    )
}

/// The actor identity a mutating engine publishes under.
#[derive(Debug)]
struct EngineWriter {
    writer_id: String,
}

/// A namespace-scoped core API.
///
/// `NamespaceEngine` owns an object store handle plus, for a mutating engine,
/// the writer identity used for mutations. It is the main entrypoint for
/// direct reads, path writes, explicit commits, uploads, checkpoints, and
/// retention work.
///
/// An engine built by [`NamespaceEngineBuilder::build_reader`] carries no
/// writer identity: it serves reads, and every mutation refuses.
#[derive(Debug)]
pub struct NamespaceEngine<S> {
    store: S,
    namespace_id: NamespaceId,
    writer: Option<EngineWriter>,
    /// A narrowed per-step row budget, so a test can reach a frozen base
    /// without writing the hundred thousand rows the shipped budget admits.
    /// See [`Self::starve_reorganization_row_budget`].
    #[cfg(any(test, feature = "test-support"))]
    reorganization_row_budget: Option<std::num::NonZeroUsize>,
}

impl<S: ObjectStore> NamespaceEngine<S> {
    /// The reorganization budgets this engine plans and compacts under.
    fn metadata_lsm_policy(&self) -> crate::checkpoint::MetadataLsmPolicy {
        let policy = crate::checkpoint::MetadataLsmPolicy::default();
        #[cfg(any(test, feature = "test-support"))]
        if let Some(max_decoded_input_rows_per_step) = self.reorganization_row_budget {
            return crate::checkpoint::MetadataLsmPolicy {
                max_decoded_input_rows_per_step,
                ..policy
            };
        }
        policy
    }

    /// Narrows the rows one reorganization step may decode, so a namespace a
    /// test can build in seconds has a base run no step can fold.
    ///
    /// That state — a frozen base with delta runs piling up above it — is what
    /// the streaming compaction exists for, and the shipped budget only
    /// reaches it at a scale no test can write. Test-only, and the one budget
    /// that has to move to get there: everything else about planning, running,
    /// and publishing the job is the shipped path.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn starve_reorganization_row_budget(
        mut self,
        max_decoded_input_rows_per_step: std::num::NonZeroUsize,
    ) -> Self {
        self.reorganization_row_budget = Some(max_decoded_input_rows_per_step);
        self
    }
    /// Starts an engine builder for the supplied object store.
    ///
    /// The builder requires a namespace id, and a writer id unless it is
    /// finished with [`NamespaceEngineBuilder::build_reader`].
    pub fn builder(store: S) -> NamespaceEngineBuilder<S> {
        NamespaceEngineBuilder {
            store,
            namespace_id: None,
            writer_id: None,
        }
    }

    /// Returns the namespace this engine is bound to.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Returns the writer id used for epoch acquisition and commit
    /// publication, or `None` for a read-only engine.
    pub fn writer_id(&self) -> Option<&str> {
        self.writer.as_ref().map(|writer| writer.writer_id.as_str())
    }

    /// Creates the namespace if it does not already exist.
    ///
    /// Use this before normal reads and writes for a new namespace.
    pub async fn bootstrap_namespace(
        &self,
        options: BootstrapOptions,
    ) -> std::result::Result<NamespaceSummary, BootstrapNamespaceError> {
        bootstrap::bootstrap_namespace(
            &self.store,
            &self.namespace_id,
            &self.mutation_context()?,
            options.allow_existing,
        )
        .await
    }

    /// Creates a new namespace at the current head of this namespace.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    pub async fn fork_namespace(&self, target: &NamespaceId) -> Result<NamespaceSummary> {
        fork::fork_namespace(
            &self.store,
            &self.namespace_id,
            target,
            &self.mutation_context()?,
        )
        .await
    }

    /// Deletes the namespace by atomically changing its head to the terminal
    /// deleted state. Earlier committed changes remain durable, and later
    /// operations return `namespace_deleted`.
    pub async fn delete_namespace(
        &self,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        crate::commit_engine::delete_namespace(
            &self.store,
            &self.namespace_id,
            options,
            &self.mutation_context()?,
        )
        .await
    }

    /// Stats one path against the pinned runtime read context.
    pub async fn resolve_path(
        &self,
        path: impl AsRef<str>,
        options: StatPathOptions,
        context: &RuntimeReadContext,
    ) -> Result<AuthoritativePathEntry> {
        let view = self.load_read_view(context).await?;
        view.resolve_path(path.as_ref(), options.include_attributes.into())
            .await
    }

    /// Lists one directory page against the pinned runtime read context.
    pub async fn list_path_page(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<DirectoryPageCursor>,
        options: ListPathEntriesOptions,
        context: &RuntimeReadContext,
    ) -> Result<Page<AuthoritativePathEntry, DirectoryPageCursor>> {
        let view = self.load_read_view(context).await?;
        view.list_path_page(path.as_ref(), request, options.include_attributes.into())
            .await
    }

    /// Reads file content against the pinned runtime read context.
    pub async fn read_file(
        &self,
        path: impl AsRef<str>,
        context: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> Result<AuthoritativeFileBytes> {
        let view = self.load_read_view(context).await?;
        view.read_file_bytes(&self.store, path.as_ref(), max_content_bytes)
            .await
    }

    /// Opens a chunked stream for the file resolved from the pinned read context.
    ///
    /// The stream fetches `chunk_bytes` at a time, so memory use is bounded by one
    /// chunk and the buffered-read size limit does not apply. The resolved content
    /// object is immutable, so later commits cannot change the bytes being read.
    ///
    /// When `start_offset` is nonzero, the caller must pass the skipped prefix to
    /// [`FileContentStream::fold_resumed_prefix`] before fetching more data. This
    /// allows the stream to verify the checksum of the complete object.
    pub async fn read_file_stream(
        &self,
        path: impl AsRef<str>,
        context: &RuntimeReadContext,
        chunk_bytes: NonZeroU64,
        start_offset: u64,
    ) -> Result<FileContentStream<S>>
    where
        S: Clone,
    {
        let view = self.load_read_view(context).await?;
        let (entry, content_ref) = view.resolve_file_content(path.as_ref()).await?;
        if start_offset > content_ref.size_bytes {
            return Err(CoreError::ResumeOffsetOutOfRange {
                start_offset,
                size_bytes: content_ref.size_bytes,
            });
        }
        Ok(FileContentStream::open(
            self.store.clone(),
            view.content_store_id(),
            entry,
            content_ref,
            chunk_bytes,
            start_offset,
        )
        .await?)
    }

    /// Resolves a file to the object key needed for a direct download. This
    /// reads metadata only and does not transfer content bytes.
    pub async fn direct_download_target(
        &self,
        path: impl AsRef<str>,
        revision_no: Option<RevisionNo>,
        context: &RuntimeReadContext,
    ) -> Result<DirectDownloadTarget> {
        let view = self.load_read_view(context).await?;
        view.direct_download_target(path.as_ref(), revision_no)
            .await
    }

    /// Lists one revision page for a path against the pinned runtime read context.
    pub async fn list_file_revisions_page(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<FileRevisionsPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>> {
        let view = self.load_read_view(context).await?;
        view.list_file_revisions_page(path.as_ref(), request).await
    }

    /// Lists one trash page against the pinned runtime read context.
    pub async fn list_trash_page(
        &self,
        request: PageRequest<TrashPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<Page<TrashEntry, TrashPageCursor>> {
        let view = self.load_read_view(context).await?;
        view.list_trash_page(request).await
    }

    /// Lists files from the manifest pinned by `checkpoint_id`, ordered by inode
    /// ID.
    ///
    /// The method ignores the current metadata basis and WAL tail, so every page
    /// comes from the same checkpoint snapshot even when new commits arrive. The
    /// read context is used only to validate the namespace identity and confirm
    /// that it has not been deleted.
    pub async fn list_checkpoint_files_page(
        &self,
        checkpoint_id: &CheckpointId,
        request: PageRequest<CheckpointFilesPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<CheckpointFilesPage> {
        // Rejects a mismatched or deleted namespace before any read work.
        self.live_catalog(context)?;
        crate::checkpoint::list_checkpoint_files_page(
            &self.store,
            Some(context.table_cache.as_ref()),
            &self.namespace_id,
            checkpoint_id,
            request,
        )
        .await
    }

    /// Resolves the current visibility, revision, and path for each inode ID.
    ///
    /// All results use the same pinned snapshot and preserve input order. Missing
    /// inode IDs are returned as not visible because callers may hold IDs from an
    /// older listing. Requests above
    /// [`MAX_RESOLVE_CURRENT_FILES`](crate::MAX_RESOLVE_CURRENT_FILES) fail before
    /// reading metadata.
    pub async fn resolve_current_files(
        &self,
        inode_ids: &[InodeId],
        context: &RuntimeReadContext,
    ) -> Result<Vec<CurrentFileState>> {
        crate::path::read::ensure_resolve_batch_within_cap(inode_ids.len())?;
        let view = self.load_read_view(context).await?;
        crate::path::read::resolve_current_files(&view, inode_ids).await
    }

    /// Reads and verifies one immutable content object.
    ///
    /// `max_bytes` is checked against the declared size before the fetch and is
    /// independent of deployment-wide download limits. The method returns an
    /// error if the fetched size or checksum does not match the reference.
    pub async fn read_content_ref(
        &self,
        content_ref: &ContentRef,
        max_bytes: u64,
        context: &RuntimeReadContext,
    ) -> Result<Vec<u8>> {
        let catalog = self.live_catalog(context)?;
        crate::path::read::ensure_within_read_limit(content_ref.size_bytes, Some(max_bytes))?;
        let read = crate::storage::content::read_durable_content_bytes(
            &self.store,
            catalog.content_store_id(),
            content_ref,
        )
        .await?;
        Ok(read.bytes)
    }

    /// Returns the namespace catalog derived from the pinned head after checking
    /// that the head belongs to this namespace and is not deleted.
    ///
    /// Use this for read paths that do not load a full metadata view.
    fn live_catalog(&self, context: &RuntimeReadContext) -> Result<VerifiedNamespaceCatalogEntry> {
        if context.head.namespace_id != self.namespace_id {
            return Err(crate::error::CoreError::NamespaceCorrupt(format!(
                "head namespace `{}` does not match requested namespace `{}`",
                context.head.namespace_id, self.namespace_id
            )));
        }
        if context.head.state == NamespaceState::Deleted {
            return Err(crate::error::CoreError::NamespaceDeleted {
                namespace_id: self.namespace_id.clone(),
            });
        }
        Ok(VerifiedNamespaceCatalogEntry::from_head(&context.head))
    }

    /// Lists one revision page for an inode against the pinned runtime read context.
    pub async fn list_file_revisions_for_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>> {
        let view = self.load_read_view(context).await?;
        view.list_file_revisions_for_inode_page(inode_id, request)
            .await
    }

    /// Reads one revision's content by path against the pinned runtime
    /// read context.
    pub async fn read_file_revision(
        &self,
        path: impl AsRef<str>,
        revision_no: RevisionNo,
        context: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> Result<AuthoritativeFileBytes> {
        let view = self.load_read_view(context).await?;
        view.read_file_revision_bytes(&self.store, path.as_ref(), revision_no, max_content_bytes)
            .await
    }

    /// Reads one revision's content against the pinned runtime read context.
    pub async fn read_file_revision_for_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        context: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> Result<Vec<u8>> {
        let view = self.load_read_view(context).await?;
        view.read_file_revision_bytes_for_inode(
            &self.store,
            inode_id,
            revision_no,
            max_content_bytes,
        )
        .await
    }

    async fn load_read_view<'a>(
        &'a self,
        context: &'a RuntimeReadContext,
    ) -> Result<LoadedMetadataView<'a, S>> {
        load_metadata_view(
            &self.store,
            &self.namespace_id,
            runtime_read_load_context(context),
        )
        .await
    }

    /// Publishes already-classified mutation candidates as one batch: one WAL
    /// segment, one head compare-and-swap, one result per candidate in order.
    pub async fn publish_namespace_commits_batch(
        &self,
        candidates: Vec<CommitCandidate>,
    ) -> Vec<Result<CommitResponse>> {
        let context = match self.mutation_context() {
            Ok(context) => context,
            Err(error) => return candidates.iter().map(|_| Err(error.clone())).collect(),
        };
        crate::commit_engine::publish_namespace_commits_batch(
            &self.store,
            &self.namespace_id,
            candidates,
            &context,
        )
        .await
    }

    /// Reads up to `limit` committed changes after `after_seq`.
    pub async fn list_changes_after(
        &self,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
    ) -> Result<ChangesResponse> {
        crate::protocol::list_changes_after(&self.store, &self.namespace_id, after_seq, limit).await
    }

    /// Starts a durable upload session with explicit transport options.
    pub async fn begin_upload(&self, request: BeginUploadRequest) -> Result<BeginUploadResponse> {
        crate::protocol::begin_upload(
            &self.store,
            &self.namespace_id,
            request,
            &self.mutation_context()?,
        )
        .await
    }

    /// Mints a direct_put upload target: a fresh content identity, the
    /// reference that names it, and the internal object key to sign.
    pub async fn begin_direct_put_upload_target(
        &self,
        claim: DirectPutContentClaim,
    ) -> Result<BeginDirectPutUploadTargetResponse> {
        crate::protocol::begin_direct_put_upload_target(
            &self.store,
            &self.namespace_id,
            claim,
            &self.mutation_context()?,
        )
        .await
    }

    /// Creates a direct multipart upload target with a new object identity,
    /// provider upload ID, and required part size. The final size and checksum are
    /// supplied when the upload is completed.
    pub async fn begin_direct_multipart_upload_target(
        &self,
        options: DirectMultipartUploadOptions,
    ) -> Result<BeginDirectMultipartUploadTargetResponse> {
        crate::protocol::begin_direct_multipart_upload_target(
            &self.store,
            &self.namespace_id,
            options,
            &self.mutation_context()?,
        )
        .await
    }

    /// Validates a group of multipart parts and returns the information needed
    /// to sign their upload URLs. This method does not write durable state.
    pub async fn direct_multipart_part_targets(
        &self,
        upload_id: &UploadId,
        requested: &[UploadPartChecksumClaim],
    ) -> Result<MultipartPartTargets> {
        crate::protocol::direct_multipart_part_targets(
            &self.store,
            &self.namespace_id,
            upload_id,
            requested,
        )
        .await
    }

    /// Uploads whole-file content into an upload session.
    pub async fn upload_content(
        &self,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        crate::protocol::upload_content(
            &self.store,
            &self.namespace_id,
            upload_id,
            bytes,
            &self.mutation_context()?,
        )
        .await
    }

    /// Uploads content that arrives as a stream into an upload session,
    /// hashing it on the way through instead of holding it.
    pub async fn upload_streamed_content(
        &self,
        upload_id: &UploadId,
        body: ByteStream,
    ) -> Result<UploadContentResponse> {
        crate::protocol::upload_streamed_content(
            &self.store,
            &self.namespace_id,
            upload_id,
            body,
            &self.mutation_context()?,
        )
        .await
    }

    /// Completes an upload session when the expected content ref matches.
    pub async fn complete_upload(
        &self,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        Ok(self
            .complete_upload_prepared(upload_id, request)
            .await?
            .response)
    }

    /// Completes an upload session and returns proof for later publication.
    ///
    /// Service-proxied completion performs no content-blob I/O. Direct-put
    /// completion performs one content-blob HEAD and no content-blob GET.
    pub async fn complete_upload_prepared(
        &self,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<CompletedUpload> {
        let catalog = crate::namespace::catalog::load_namespace_catalog_entry(
            &self.store,
            &self.namespace_id,
        )
        .await?;
        self.complete_upload_prepared_with_catalog(&catalog, upload_id, request)
            .await
    }

    /// Completes an upload with a namespace catalog binding already resolved
    /// by the runtime.
    pub async fn complete_upload_prepared_with_catalog(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<CompletedUpload> {
        let catalog = self.own_catalog(catalog)?;
        crate::protocol::complete_upload(
            &self.store,
            &self.namespace_id,
            catalog.content_store_id(),
            upload_id,
            request,
            &self.mutation_context()?,
        )
        .await
    }

    /// Stores in-process bytes as prepared content and completes the associated
    /// upload session.
    ///
    /// This combines session creation, content upload, and completion without a
    /// network round trip or receipt. It still writes the upload-session record
    /// required by garbage collection. The content write is followed by two
    /// small control-object writes.
    pub async fn stage_owned_bytes(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        crate::protocol::stage_owned_bytes(
            &self.store,
            self.own_catalog(catalog)?,
            bytes,
            &self.mutation_context()?,
        )
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
        body: ByteStream,
    ) -> Result<PreparedContent> {
        crate::protocol::stage_owned_stream(
            &self.store,
            self.own_catalog(catalog)?,
            body,
            &self.mutation_context()?,
        )
        .await
    }

    /// Verifies that a runtime-supplied catalog belongs to this engine's
    /// namespace. A mismatch indicates an internal integration error.
    fn own_catalog<'c>(
        &self,
        catalog: &'c VerifiedNamespaceCatalogEntry,
    ) -> Result<&'c VerifiedNamespaceCatalogEntry> {
        if catalog.namespace_id() != &self.namespace_id {
            return Err(CoreError::Internal(format!(
                "an operation on namespace `{}` was given namespace `{}`'s catalog",
                self.namespace_id,
                catalog.namespace_id()
            )));
        }
        Ok(catalog)
    }

    /// Aborts an upload session, then deletes the content object it owned.
    ///
    /// Terminal and idempotent: repeating it succeeds, and it refuses a
    /// session that already completed, whose content may be published.
    pub async fn abort_upload(&self, upload_id: &UploadId) -> Result<AbortUploadResponse> {
        let content_store_id = crate::namespace::catalog::load_namespace_content_store_id(
            &self.store,
            &self.namespace_id,
        )
        .await?;
        crate::protocol::abort_upload(
            &self.store,
            &self.namespace_id,
            &content_store_id,
            upload_id,
            &self.mutation_context()?,
        )
        .await
    }

    /// Reads an upload session. For a completed upload, it also creates a new
    /// receipt so the caller can retry publication without uploading the content
    /// again.
    pub async fn read_upload_status(
        &self,
        upload_id: &UploadId,
    ) -> Result<(UploadStatusResponse, Option<CompletedUploadReceipt>)> {
        let content_store_id = crate::namespace::catalog::load_namespace_content_store_id(
            &self.store,
            &self.namespace_id,
        )
        .await?;
        crate::protocol::read_upload_status(
            &self.store,
            &self.namespace_id,
            &content_store_id,
            upload_id,
            self.now_ms()?,
        )
        .await
    }

    /// Creates or reuses a named checkpoint for the current namespace head.
    ///
    /// The checkpoint pins a manifest for retention and provenance. If the head
    /// has no manifest, the method first publishes one without compacting
    /// metadata. `ttl_ms` sets an expiration time; `None` keeps the checkpoint
    /// until it is released.
    pub async fn create_checkpoint(
        &self,
        name: String,
        ttl_ms: Option<u64>,
    ) -> Result<CreateCheckpointResponse> {
        let context = self.mutation_context()?;
        let expires_at_ms = ttl_ms.map(|ttl_ms| context.now_ms.saturating_add(ttl_ms));
        crate::checkpoint::create_checkpoint(
            &self.store,
            &self.namespace_id,
            CheckpointOwner::User { name },
            expires_at_ms,
            &context,
        )
        .await
    }

    /// Lists active checkpoint records from oldest to newest.
    ///
    /// This method does not release expired records. A record remains active and
    /// appears in the result until garbage collection processes it.
    pub async fn list_checkpoints(&self) -> Result<ListCheckpointsResponse> {
        crate::checkpoint::list_checkpoints(&self.store, &self.namespace_id).await
    }

    /// Releases a user-owned checkpoint by id.
    ///
    /// Idempotent: releasing an already-released or reaped record succeeds.
    /// The record is reaped by a later garbage-collection pass; its basis
    /// becomes collectable only on the pass after that.
    pub async fn release_checkpoint(
        &self,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse> {
        crate::checkpoint::release_checkpoint(
            &self.store,
            &self.namespace_id,
            checkpoint_id,
            &self.mutation_context()?,
        )
        .await
    }

    /// Flushes the visible WAL tail and advances `metadata/root.json` to a
    /// manifest covering the current head.
    ///
    /// This is the latest-state maintenance operation: it absorbs the visible
    /// WAL tail into a published manifest and advances the root, creating no
    /// checkpoint record. Superseded manifests become garbage-collection
    /// candidates once nothing pins them.
    pub async fn flush_wal(&self) -> Result<FlushWalResponse> {
        crate::checkpoint::flush_wal(&self.store, &self.namespace_id, &self.mutation_context()?)
            .await
    }

    /// Performs at most one metadata reorganization step for one row family.
    /// It merges L0 delta rows into new base segments and publishes a manifest
    /// that replaces the old references.
    ///
    /// Run this repeatedly until it returns `NotNeeded`. Each call reloads durable
    /// state, so work can resume safely after interruption.
    ///
    /// A group whose oldest run no longer fits one unit is reported as
    /// [`crate::checkpoint::MetadataReorganizeOutcome::CompactionPlanned`]
    /// instead. The caller runs that plan with
    /// [`Self::run_metadata_compaction`] as a background job and includes its
    /// specification in `compactions` while the job runs. This prevents a
    /// bounded step from merging the same group concurrently. `compactions`
    /// also records how many delta merges have run above an oversized base so
    /// the planner knows when to request the full compaction.
    pub async fn reorganize_metadata(
        &self,
        compactions: crate::checkpoint::MetadataCompactionView<'_>,
    ) -> Result<crate::checkpoint::MetadataReorganizeReport> {
        crate::checkpoint::reorganize_metadata_step(
            &self.store,
            &self.namespace_id,
            &self.mutation_context()?,
            self.metadata_lsm_policy(),
            compactions,
        )
        .await
    }

    /// Rebuilds one family group in a single streaming pass and publishes the
    /// swap, from a plan [`Self::reorganize_metadata`] produced.
    ///
    /// Long-running by design and paced by no budget: it is the caller's
    /// background work, not a bounded step. `cancellation` stops it between
    /// block fetches, which is what a graceful shutdown sets. Every ending
    /// short of a publication costs only the work done — the manifest never
    /// moved, the segments it wrote are staged and referenced by nothing, and
    /// a later step plans the group again.
    pub async fn run_metadata_compaction(
        &self,
        spec: &crate::checkpoint::MetadataCompactionSpec,
        cancellation: &crate::checkpoint::MetadataCompactionCancellation,
    ) -> Result<crate::checkpoint::MetadataCompactionJobOutcome> {
        crate::checkpoint::run_metadata_compaction_job(
            &self.store,
            &self.namespace_id,
            &self.mutation_context()?,
            self.metadata_lsm_policy(),
            spec,
            cancellation,
        )
        .await
    }

    /// Advances the retention floor when a verified checkpoint makes it safe.
    pub async fn advance_retention_floor(&self) -> Result<AdvanceRetentionResponse> {
        crate::checkpoint::advance_retention_floor(
            &self.store,
            &self.namespace_id,
            &self.mutation_context()?,
        )
        .await
    }

    /// Builds the mutation context for this engine. Read-only engines return an
    /// internal error because they have no writer identity.
    fn mutation_context(&self) -> Result<MutationContext> {
        let writer = self.writer.as_ref().ok_or_else(|| {
            crate::error::CoreError::Internal(
                "engine built without writer identity cannot mutate".to_owned(),
            )
        })?;
        Ok(MutationContext {
            writer_id: writer.writer_id.clone(),
            now_ms: self.now_ms()?,
        })
    }

    /// Reads the wall-clock boundary independently of writer identity.
    fn now_ms(&self) -> Result<u64> {
        current_time_ms()
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

    /// Builds a mutating engine after required fields are present.
    pub fn build(self) -> std::result::Result<NamespaceEngine<S>, NamespaceEngineBuildError> {
        let namespace_id = self
            .namespace_id
            .ok_or(NamespaceEngineBuildError::MissingNamespace)?;
        let writer_id = self
            .writer_id
            .ok_or(NamespaceEngineBuildError::MissingWriter)?;
        if writer_id.trim().is_empty() {
            return Err(NamespaceEngineBuildError::EmptyWriter);
        }

        Ok(NamespaceEngine {
            store: self.store,
            namespace_id,
            writer: Some(EngineWriter { writer_id }),
            #[cfg(any(test, feature = "test-support"))]
            reorganization_row_budget: None,
        })
    }

    /// Builds a read-only engine: no writer identity at all.
    ///
    /// Only the namespace is required. Any writer identity set on the
    /// builder is dropped — a read-only engine carries none by definition.
    pub fn build_reader(
        self,
    ) -> std::result::Result<NamespaceEngine<S>, NamespaceEngineBuildError> {
        let namespace_id = self
            .namespace_id
            .ok_or(NamespaceEngineBuildError::MissingNamespace)?;
        Ok(NamespaceEngine {
            store: self.store,
            namespace_id,
            writer: None,
            #[cfg(any(test, feature = "test-support"))]
            reorganization_row_budget: None,
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
        assert_eq!(engine.writer_id(), Some("writer-a"));
    }

    #[test]
    fn reader_engine_builds_without_any_writer_identity() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let engine = NamespaceEngine::builder(store)
            .namespace_id(namespace_id.clone())
            .build_reader()
            .expect("reader engine builds without a writer");

        assert_eq!(engine.namespace_id(), &namespace_id);
        assert_eq!(engine.writer_id(), None);
    }

    #[tokio::test]
    async fn reader_engine_still_serves_reads() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        NamespaceEngine::builder(LocalFsStore::new(temp_dir.path()).expect("store"))
            .namespace_id(namespace_id.clone())
            .writer_id("writer-a")
            .build()
            .expect("engine builds")
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap namespace");

        let reader = NamespaceEngine::builder(LocalFsStore::new(temp_dir.path()).expect("store"))
            .namespace_id(namespace_id.clone())
            .build_reader()
            .expect("reader engine builds");
        let changes = reader
            .list_changes_after(
                ChangeSeq(0),
                loonfs_api::PaginationPolicy::default()
                    .resolve_limit(None)
                    .expect("default limit"),
            )
            .await
            .expect("a reader-built engine serves reads");
        assert_eq!(changes.namespace_id, namespace_id);
    }

    #[tokio::test]
    async fn reader_engine_reads_upload_status_without_writer_identity() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer =
            NamespaceEngine::builder(LocalFsStore::new(temp_dir.path()).expect("writer store"))
                .namespace_id(namespace_id.clone())
                .writer_id("writer-a")
                .build()
                .expect("writer engine builds");
        writer
            .bootstrap_namespace(BootstrapOptions::default())
            .await
            .expect("bootstrap namespace");
        let begun = writer
            .begin_upload(BeginUploadRequest::ServiceProxied {})
            .await
            .expect("begin upload");

        let reader =
            NamespaceEngine::builder(LocalFsStore::new(temp_dir.path()).expect("reader store"))
                .namespace_id(namespace_id)
                .build_reader()
                .expect("reader engine builds");
        let (status, receipt) = reader
            .read_upload_status(begun.upload_id())
            .await
            .expect("reader engine reads upload status");

        assert_eq!(status.upload_id, *begun.upload_id());
        assert!(receipt.is_none());
    }

    #[tokio::test]
    async fn reader_engine_refuses_to_mutate() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let reader = NamespaceEngine::builder(store)
            .namespace_id(NamespaceId::parse("demo").expect("valid namespace id"))
            .build_reader()
            .expect("reader engine builds");

        let error = reader
            .flush_wal()
            .await
            .expect_err("a reader-built engine must refuse mutations");
        assert!(
            error
                .to_string()
                .contains("engine built without writer identity cannot mutate"),
            "unexpected error: {error}"
        );
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
