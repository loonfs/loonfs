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
use crate::protocol::CompletedUpload;
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
    ReleaseCheckpointResponse, RevisionNo, StorageChecksum, TrashEntry, TrashPageCursor, UploadId,
};
use loonfs_objectstore::{ByteStream, ObjectStore};
use std::num::NonZeroU64;
use std::sync::Arc;
use thiserror::Error;

/// The pinned inputs the runtime resolves once per read: the head anchored
/// to its manifest, the shared caches, and (when the runtime has it) the
/// namespace's immutable catalog pair.
///
/// This is the runtime seam: the `loonfs` crate pins one context per
/// request and fans every read of that request through it, so the whole
/// request observes a single snapshot and shares the caches. It is a
/// sanctioned public hook (STYLE, "Harness hooks are sanctioned"), not an
/// application API — embedded applications use the `loonfs` handles, which
/// drive this seam internally.
#[derive(Debug, Clone)]
pub struct RuntimeReadContext {
    pub head: HeadState,
    pub head_etag: String,
    /// The materialized basis the head authorized when the anchor was
    /// taken. It carries the namespace's own root when it has one, and the
    /// genesis or fork basis until then.
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

/// Internal target used by server integrations before they mint part URLs.
///
/// There is no content ref: a multipart session is opened before anything
/// is known about the payload, so identity exists but the reference that
/// describes it does not yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectMultipartUploadTarget {
    pub object_key: String,
    pub part_size_bytes: u64,
}

/// Internal response for preparing a direct_multipart session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginDirectMultipartUploadTargetResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: UploadId,
    pub target: DirectMultipartUploadTarget,
}

/// One part a server integration is about to sign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartTarget {
    pub part_number: u32,
    pub checksum: StorageChecksum,
}

/// Everything a server integration needs to sign one wave of part uploads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPartTargets {
    pub object_key: String,
    pub provider_upload_id: String,
    pub parts: Vec<MultipartPartTarget>,
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
}

impl<S: ObjectStore> NamespaceEngine<S> {
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

    /// Deletes this namespace: a fenced, terminal head-state transition.
    /// Commits acknowledged before the swap stay committed; everything that
    /// observes the deleted head afterward fails with `namespace_deleted`.
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

    /// Opens a bounded streaming read of a file's current content against the
    /// pinned runtime read context.
    ///
    /// The path resolves exactly as it does for [`Self::read_file`]; what
    /// differs is everything after. The content arrives as `chunk_bytes`
    /// ranged reads with the verifying digest folded over them, so what the
    /// read costs in memory is one chunk rather than the file's size, and the
    /// deployment's buffered-read cap deliberately does not apply — that cap
    /// bounds what a caller materializes, and this caller materializes a
    /// chunk.
    ///
    /// The pinned context resolves the path; it does not have to survive the
    /// read. The reference names one immutable object at a random id, so no
    /// commit landing mid-read can change the bytes under a reader.
    ///
    /// `start_offset` skips bytes the caller already holds; the stream still
    /// verifies the whole object, so those bytes reach it through
    /// [`FileContentStream::fold_resumed_prefix`] before it fetches
    /// anything.
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

    /// Resolves a path to the content object a direct read would fetch,
    /// against the pinned runtime read context.
    ///
    /// This reads metadata only. It is the read-side counterpart of
    /// [`Self::begin_direct_put_upload_target`]: both hand a host the one
    /// object key it needs in order to sign a transfer, and neither moves
    /// a byte.
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

    /// Reads one page of the files visible in the state `checkpoint_id`
    /// pins, in ascending inode-id order.
    ///
    /// The checkpoint's pinned manifest is the only state enumerated: the
    /// context's own basis and WAL tail are deliberately not read, so a
    /// commit landing while a consumer pages through changes nothing it
    /// sees. Everything after the pinned sequence is the change feed's job.
    /// The context supplies the namespace's immutable identity and proves
    /// the namespace is still live.
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

    /// Answers, for each inode id, what it looks like in the namespace's
    /// current state: whether it is visible, its current revision, and its
    /// current path.
    ///
    /// One pinned read serves the whole batch, so every answer describes the
    /// same state, and answers come back in input order. Ids that name
    /// nothing are answered as not visible rather than refused — a consumer
    /// holding ids from an earlier enumeration routinely holds stale ones.
    /// At most [`MAX_RESOLVE_CURRENT_FILES`](crate::MAX_RESOLVE_CURRENT_FILES)
    /// ids per call; a larger batch is refused before anything is read.
    pub async fn resolve_current_files(
        &self,
        inode_ids: &[InodeId],
        context: &RuntimeReadContext,
    ) -> Result<Vec<CurrentFileState>> {
        crate::path::read::ensure_resolve_batch_within_cap(inode_ids.len())?;
        let view = self.load_read_view(context).await?;
        crate::path::read::resolve_current_files(&view, inode_ids).await
    }

    /// Reads one immutable content object by reference.
    ///
    /// `max_bytes` is the caller's own budget for this read, checked against
    /// the reference's declared size before any fetch; it is independent of
    /// any deployment-wide download limit, so a consumer sizes its own
    /// buffers. After the fetch the bytes are verified against the
    /// reference's size and digest, and a mismatch fails the read — there is
    /// no partial answer and no second attempt against another key.
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

    /// The namespace's immutable identity, read off the pinned head after
    /// refusing a head that is not this namespace's or that is a tombstone.
    ///
    /// Reads that load a metadata view get both checks from the view load;
    /// this is for the reads that deliberately do not load one.
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

    /// Mints a direct_multipart upload target: a fresh content identity, the
    /// provider upload that assembles it, and the part geometry the client
    /// cuts its payload to. What the payload turns out to be is claimed at
    /// completion.
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

    /// Resolves one wave of parts for signing against the session that owns
    /// them. Nothing durable is written: parts are the client's bookkeeping.
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

    /// Stages bytes this process holds as content a session owns, ready to
    /// publish here.
    ///
    /// This is the whole upload lifecycle for the caller that is also the
    /// uploader: a session opens, the bytes land under the identity it
    /// allocated, and the session completes — with no wire step between them
    /// and no receipt at the end, because the publication happens in this
    /// process and takes the reference directly. What it does not skip is
    /// the session record, which is what content garbage collection reads to
    /// decide an object's fate; without one the bytes would be reachable by
    /// nothing and reclaimable by nothing.
    ///
    /// Two small control writes on top of the content write, in that order.
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

    /// Refuses a catalog resolved for some other namespace.
    ///
    /// A mismatch is the host's wiring mistake rather than anything a request
    /// did, so naming the two namespaces says more than naming what was being
    /// written would — and a multipart completion has no content id to name
    /// anyway.
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

    /// Reads one upload session, minting a fresh receipt when it is
    /// completed so a lost commit response never costs a retransfer.
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
            self.mutation_context()?.now_ms,
        )
        .await
    }

    /// Creates or reuses a named checkpoint pinning the current namespace
    /// head for the calling user.
    ///
    /// A checkpoint pins a manifest version for retention/provenance. If the
    /// current head has no manifest yet, this first publishes one for the
    /// current durable namespace state; it is not a request to compact
    /// metadata. `ttl_ms` computes the record's expiry from the engine's
    /// clock; absent means the pin holds until explicitly released.
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

    /// Lists every active checkpoint record on the namespace, oldest first.
    ///
    /// A read: nothing here releases, expires, or reaps a record. A record
    /// whose expiry has passed but which no collection pass has released is
    /// still active and is still listed, with that expiry in the answer.
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

    /// Runs at most one metadata reorganization unit: folds one family
    /// group's L0 delta rows into new base segments and publishes a manifest
    /// swapping that group's references. Checkpoints only append L0 runs, so
    /// calling this from maintenance is what keeps read fan-out bounded.
    /// Repeat until the report says `NotNeeded`; every call re-reads durable
    /// state, so interrupted reorganizations resume from the live manifest.
    pub async fn reorganize_metadata(&self) -> Result<crate::checkpoint::MetadataReorganizeReport> {
        crate::checkpoint::reorganize_metadata_step(
            &self.store,
            &self.namespace_id,
            &self.mutation_context()?,
            crate::checkpoint::MetadataLsmPolicy::default(),
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

    /// The identity every mutation publishes under.
    ///
    /// A read-only engine has none, so this fails instead of inventing one.
    /// Nothing routes that error: the runtime hands read-only engines only to
    /// read paths, and this exists so a wiring mistake fails honestly rather
    /// than publishing under a fabricated writer.
    fn mutation_context(&self) -> Result<MutationContext> {
        let writer = self.writer.as_ref().ok_or_else(|| {
            crate::error::CoreError::Internal(
                "engine built without writer identity cannot mutate".to_owned(),
            )
        })?;
        Ok(MutationContext {
            writer_id: writer.writer_id.clone(),
            now_ms: current_time_ms()?,
        })
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
