//! [`NamespaceEngine`]: the namespace-scoped entry point for reads, writes,
//! uploads, checkpoints, and maintenance.

use crate::authorize::{Authorizer, CommitAuthority, ReadAccess};
use crate::cache::{MetadataSegmentCache, WalTailProjectionCache};
use crate::checkpoint::{CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointPageCursor};
use crate::commit_engine::CommitCandidate;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::metadata::access::is_administrator;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::{bootstrap, fork};
use crate::options::{BootstrapOptions, DeleteNamespaceOptions};
use crate::path::read::{
    load_metadata_view, load_metadata_view_for_authorization, CurrentFileState,
    DirectDownloadByInodeTarget, DirectDownloadTarget, LoadedMetadataView, ReadLoadContext,
};
use crate::protocol::{
    BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse, CompletedUpload,
    MultipartPartTargets, ResolvedUploadCompletion, UploadSessionView,
};
use crate::storage::content::{
    open_content_import_reader, validate_loaded_content_bytes, ContentLocation, FileContentStream,
    StreamedPayloadKind,
};
use crate::storage::content_admission::PreparedContent;
use crate::time::current_time_ms;
use loonfs_api::options::{
    DirectMultipartUploadOptions, ListInodeChildrenOptions, ListPathEntriesOptions, StatPathOptions,
};
use loonfs_api::v0::{
    Commit, ListChangesResponse, UploadMode, UploadPartChecksumClaim, UploadSession,
};
use loonfs_api::wire::control::PinOwner;
use loonfs_api::EffectiveLimit;
use loonfs_api::{
    AdvanceRetentionResponse, ChangeSeq, Checkpoint, ChecksumAlgorithm, CommitId, ContentRef,
    DeleteCheckpointResponse, DeleteNamespaceResponse, DeleteSnapshotResponse, DirectoryPageCursor,
    FileBytes, FileRevision, FileRevisionsPageCursor, FlushWalResponse, InodeId, Namespace,
    NamespaceAccess, NamespaceId, Page, PageRequest, PathEntry, PinId, RevisionNo, Subject,
    TrashEntry, TrashPageCursor, UploadId, WriterId, ROOT_INODE_ID,
};
use loonfs_objectstore::{ByteStream, ObjectStore};
use std::num::NonZeroU64;
use std::sync::Arc;
use tracing::Instrument;

/// Read context pinned by the runtime for one request. It contains the head,
/// metadata basis, and shared caches needed to serve every read from the same
/// namespace snapshot.
///
/// This type supports the `loonfs` runtime. Applications should use the
/// higher-level `loonfs` reader handles instead.
#[derive(Debug, Clone)]
pub struct RuntimeReadContext {
    pub head: NamespaceReadState,
    /// Verified manifest used to replay the pinned WAL tail.
    pub basis: MetadataBasis,
    pub segment_cache: Arc<MetadataSegmentCache>,
    pub tail_cache: Arc<WalTailProjectionCache>,
}

/// Owned metadata for one buffered read, without retaining its metadata view.
/// The runtime must validate the current path before using speculative bytes.
#[derive(Debug, Clone)]
pub struct ResolvedFileContent {
    /// Entry from the resolved view; old entries cannot authorize current reads.
    pub entry: PathEntry,
    /// Complete immutable identity to compare after current-path validation.
    pub content_ref: ContentRef,
    /// Location resolved in the entry's metadata view, retaining any inline bytes.
    pub location: ContentLocation,
}

impl ResolvedFileContent {
    /// Limits speculation to nonempty files of at most 64 KiB.
    pub fn supports_speculative_read(&self) -> bool {
        (1..=crate::storage::content::MAX_SPECULATIVE_CONTENT_BYTES)
            .contains(&self.content_ref.size_bytes)
    }

    /// Requires equality of every content-reference field.
    pub fn has_same_content(&self, other: &Self) -> bool {
        self.content_ref == other.content_ref
    }
}

fn runtime_read_load_context(context: &RuntimeReadContext) -> ReadLoadContext<'_, '_> {
    ReadLoadContext::pinned_head(
        &context.head,
        &context.basis,
        Some(&context.segment_cache),
        Some(&context.tail_cache),
    )
}

/// Marks an engine that exposes read operations only.
#[derive(Debug)]
pub struct ReadOnly;

/// Marks an engine that exposes mutations under one writer identity.
#[derive(Debug)]
pub struct Writable {
    writer_id: WriterId,
}

/// A namespace engine that exposes read operations only.
pub type NamespaceReaderEngine<S> = NamespaceEngine<S, ReadOnly>;

/// A namespace engine that exposes both read and mutation operations.
pub type NamespaceWriterEngine<S> = NamespaceEngine<S, Writable>;

/// A namespace-scoped core API.
///
/// `NamespaceEngine` owns an object store handle and exposes operations
/// selected by its mode. Use [`NamespaceEngine::reader`] for reads only or
/// [`NamespaceEngine::writer`] for reads and mutations.
#[derive(Debug)]
pub struct NamespaceEngine<S, M> {
    store: S,
    namespace_id: NamespaceId,
    mode: M,
    subject: Option<Subject>,
    authorization_head: Option<RuntimeReadContext>,
    /// A narrowed per-step row budget, so a test can reach a frozen base
    /// without writing the hundred thousand rows the shipped budget admits.
    /// See [`Self::starve_reorganization_row_budget`].
    #[cfg(any(test, feature = "test-support"))]
    reorganization_row_budget: Option<std::num::NonZeroUsize>,
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Searches the supplied WAL and metadata view without acquiring a writer epoch.
    pub async fn find_commit_receipt(
        &self,
        context: &RuntimeReadContext,
        commit_id: &CommitId,
    ) -> Result<Option<crate::metadata::CommitReceiptRecord>> {
        self.load_read_view(context)
            .await?
            .metadata_view()
            .find_commit_receipt(commit_id)
            .await
    }

    /// Sets the subject for this engine's reads.
    pub fn with_subject(mut self, subject: Subject) -> Self {
        self.subject = Some(subject);
        self
    }

    /// Sets the current head used to authorize historical reads.
    pub fn with_authorization_head(mut self, head: RuntimeReadContext) -> Self {
        self.authorization_head = Some(head);
        self
    }

    /// The authorization for one per-subject read against `context`: a live
    /// read evaluates on its own view; a snapshot read evaluates at the head
    /// this engine was given.
    fn read_access<'a>(
        &'a self,
        context: &'a RuntimeReadContext,
        head_view: Option<&'a LoadedMetadataView<'a, S>>,
    ) -> Result<ReadAccess<'a, S>> {
        let authorizer = Authorizer::for_request(
            &self.namespace_id,
            &context.head.access,
            CommitAuthority::Subject(self.subject.as_ref()),
        )?;
        Ok(match head_view {
            Some(head) => ReadAccess::at_head(authorizer, head),
            None => ReadAccess::live(authorizer),
        })
    }

    /// The current head's view, when this engine authorizes historical
    /// reads there.
    async fn authorization_head_view(&self) -> Result<Option<LoadedMetadataView<'_, S>>> {
        match &self.authorization_head {
            Some(head) => Ok(Some(self.load_read_view(head).await?)),
            None => Ok(None),
        }
    }

    /// Refuses a reader with no subject on an ACL namespace, for callers
    /// that could otherwise answer before any per-subject read.
    pub fn require_subject(&self, context: &RuntimeReadContext) -> Result<()> {
        Authorizer::for_request(
            &self.namespace_id,
            &context.head.access,
            CommitAuthority::Subject(self.subject.as_ref()),
        )
        .map(|_| ())
    }

    /// The change feed and bare content reference reads and imports work for
    /// the token holder and for an administrator; any other subject is refused.
    pub async fn require_administrator(&self, context: &RuntimeReadContext) -> Result<()> {
        let Some(subject) = &self.subject else {
            return Ok(());
        };
        let authorizer = Authorizer::for_request(
            &self.namespace_id,
            &context.head.access,
            CommitAuthority::Subject(Some(subject)),
        )?;
        let Authorizer::Subject { principals } = authorizer else {
            return Ok(());
        };
        let view = load_metadata_view_for_authorization(
            &self.store,
            &self.namespace_id,
            runtime_read_load_context(context),
        )
        .await?;
        if is_administrator(&mut view.metadata_view().session(), principals).await? {
            Ok(())
        } else {
            Err(CoreError::Forbidden {
                inode_id: ROOT_INODE_ID,
            })
        }
    }

    /// The root row's current boundary flag and grants. Refused on an
    /// unrestricted namespace, which holds no access rows.
    pub async fn root_access(
        &self,
        context: &RuntimeReadContext,
    ) -> Result<(bool, loonfs_api::AccessGrants, loonfs_api::AccessRevisionNo)> {
        if matches!(context.head.access, NamespaceAccess::Unrestricted {}) {
            return Err(CoreError::NamespaceUnrestricted {
                namespace_id: self.namespace_id.clone(),
            });
        }
        let view = self.load_read_view(context).await?;
        let row = view
            .metadata_view()
            .latest_access_revision(ROOT_INODE_ID)
            .await?;
        Ok(row.map_or(
            (
                false,
                loonfs_api::AccessGrants::default(),
                loonfs_api::AccessRevisionNo(0),
            ),
            |row| (row.boundary, row.grants, row.access_revision_no),
        ))
    }

    /// Returns the namespace this engine is bound to.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Returns the current wall-clock time.
    fn now_ms(&self) -> Result<u64> {
        current_time_ms()
    }

    /// Stats one path against the pinned runtime read context.
    pub async fn resolve_path(
        &self,
        path: impl AsRef<str>,
        options: StatPathOptions,
        context: &RuntimeReadContext,
    ) -> Result<PathEntry> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.resolve_path(path.as_ref(), options.include_attributes, &access)
            .await
    }

    /// Lists one directory page against the pinned runtime read context.
    pub async fn list_path_page(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<DirectoryPageCursor>,
        options: ListPathEntriesOptions,
        context: &RuntimeReadContext,
    ) -> Result<Page<PathEntry, DirectoryPageCursor>> {
        let head_view = self
            .authorization_head_view()
            .instrument(
                tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "load_view"),
            )
            .await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self
            .load_read_view(context)
            .instrument(
                tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "load_view"),
            )
            .await?;
        view.list_path_page(path.as_ref(), request, options.include_attributes, &access)
            .await
    }

    /// Lists one page of a directory's children by inode against the pinned
    /// runtime read context.
    pub async fn list_inode_children_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<DirectoryPageCursor>,
        options: ListInodeChildrenOptions,
        context: &RuntimeReadContext,
    ) -> Result<Page<PathEntry, DirectoryPageCursor>> {
        let head_view = self
            .authorization_head_view()
            .instrument(
                tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "load_view"),
            )
            .await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self
            .load_read_view(context)
            .instrument(
                tracing::debug_span!(target: "loonfs::page", "loonfs.phase", phase = "load_view"),
            )
            .await?;
        view.list_inode_children_page(inode_id, request, options.include_attributes, &access)
            .await
    }
}

impl<S: ObjectStore> NamespaceEngine<S, ReadOnly> {
    /// Creates a read-only engine bound to `namespace_id`.
    pub fn reader(store: S, namespace_id: NamespaceId) -> Self {
        Self {
            store,
            namespace_id,
            mode: ReadOnly,
            subject: None,
            authorization_head: None,
            #[cfg(any(test, feature = "test-support"))]
            reorganization_row_budget: None,
        }
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates a writable engine bound to `namespace_id` and `writer_id`.
    pub fn writer(store: S, namespace_id: NamespaceId, writer_id: WriterId) -> Self {
        Self {
            store,
            namespace_id,
            mode: Writable { writer_id },
            subject: None,
            authorization_head: None,
            #[cfg(any(test, feature = "test-support"))]
            reorganization_row_budget: None,
        }
    }

    /// Returns the writer id used for epoch acquisition and commit publication.
    pub fn writer_id(&self) -> &WriterId {
        &self.mode.writer_id
    }

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

    /// Creates the namespace if it does not already exist.
    ///
    /// Use this before normal reads and writes for a new namespace. Returns
    /// the namespace's status after manifest 1 is installed.
    pub async fn bootstrap_namespace(&self, options: BootstrapOptions) -> Result<Namespace> {
        bootstrap::bootstrap_namespace(
            &self.store,
            &self.namespace_id,
            &self.mutation_context()?,
            &options.actor_id,
            &options.access,
            options.allow_existing,
        )
        .await
    }

    /// Creates a new namespace at this namespace's current head or a live snapshot.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    /// Returns the target's status at the fork point.
    pub async fn fork_namespace(
        &self,
        target: &NamespaceId,
        actor_id: &loonfs_api::ActorId,
        snapshot_id: Option<&PinId>,
    ) -> Result<Namespace> {
        fork::fork_namespace(
            &self.store,
            &self.namespace_id,
            target,
            actor_id,
            snapshot_id,
            &self.mutation_context()?,
        )
        .await
    }

    /// Deletes the current namespace by publishing a tombstone manifest.
    /// Earlier committed changes remain durable. Later operations return `namespace_deleted`.
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
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Reads file content against the pinned runtime read context.
    pub async fn get_file(
        &self,
        path: impl AsRef<str>,
        context: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> Result<FileBytes> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.get_file_bytes(&self.store, path.as_ref(), max_content_bytes, &access)
            .await
    }

    /// Resolves the metadata half of a buffered read for runtime speculation.
    pub async fn resolve_file_content(
        &self,
        path: impl AsRef<str>,
        context: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> Result<ResolvedFileContent> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        let (entry, content_ref) = view
            .resolve_file_content(path.as_ref(), None, &access)
            .await?;
        crate::path::read::ensure_within_read_limit(content_ref.size_bytes, max_content_bytes)?;
        Ok(ResolvedFileContent {
            entry,
            location: view.resolve_content_location(&content_ref)?,
            content_ref,
        })
    }

    /// Verifies at most 64 KiB of speculative content, plus an overflow byte.
    /// These bytes alone do not establish current path visibility.
    pub async fn get_speculative_file_content(
        &self,
        target: &ResolvedFileContent,
    ) -> Result<Vec<u8>> {
        crate::storage::content::get_speculative_content_bytes(
            &self.store,
            &target.location,
            &target.content_ref,
        )
        .await
    }

    /// Fetches and verifies content after `resolve_file_content` authorizes it.
    /// A cached target must first be validated against the current view.
    pub async fn get_resolved_file_content(&self, target: &ResolvedFileContent) -> Result<Vec<u8>> {
        Ok(target
            .location
            .get_bytes(&self.store, &target.content_ref)
            .await?)
    }

    /// Opens a chunked stream for the file resolved from the pinned read context.
    ///
    /// Object reads fetch `chunk_bytes` at a time. Tail reads retain their inline
    /// bytes. The buffered-read size limit does not apply. Later commits cannot
    /// change the bytes being read.
    ///
    /// When `start_offset` is nonzero, the caller must pass the skipped prefix to
    /// [`FileContentStream::fold_resumed_prefix`] before fetching more data. This
    /// allows the stream to verify the checksum of the complete object.
    pub async fn read_file_stream(
        &self,
        path: impl AsRef<str>,
        context: &RuntimeReadContext,
        revision_no: Option<RevisionNo>,
        chunk_bytes: NonZeroU64,
        start_offset: u64,
    ) -> Result<FileContentStream<S>>
    where
        S: Clone,
    {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        let (entry, content_ref) = view
            .resolve_file_content(path.as_ref(), revision_no, &access)
            .await?;
        if start_offset > content_ref.size_bytes {
            return Err(CoreError::ResumeOffsetOutOfRange {
                start_offset,
                size_bytes: content_ref.size_bytes,
            });
        }
        Ok(FileContentStream::open_inner(
            self.store.clone(),
            view.resolve_content_location(&content_ref)?,
            Some(entry),
            content_ref,
            chunk_bytes,
            start_offset,
        )
        .await?)
    }

    /// Streams one retained inode revision without requiring a visible path.
    pub async fn read_file_revision_stream_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        context: &RuntimeReadContext,
    ) -> Result<FileContentStream<S>>
    where
        S: Clone,
    {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        let content_ref = view
            .authorized_revision_for_inode(inode_id, revision_no, &access)
            .await?
            .content_ref;
        Ok(FileContentStream::open_inner(
            self.store.clone(),
            view.resolve_content_location(&content_ref)?,
            None,
            content_ref,
            NonZeroU64::new(crate::CONTENT_READ_CHUNK_BYTES)
                .expect("content read chunk size should be nonzero"),
            0,
        )
        .await?)
    }

    /// Prepares a file's content object for a direct download.
    pub async fn direct_download_target(
        &self,
        path: impl AsRef<str>,
        revision_no: Option<RevisionNo>,
        context: &RuntimeReadContext,
    ) -> Result<DirectDownloadTarget> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.direct_download_target(&self.store, path.as_ref(), revision_no, &access)
            .await
    }

    /// Prepares a retained inode revision's content object for a direct download.
    pub async fn direct_download_target_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        context: &RuntimeReadContext,
    ) -> Result<DirectDownloadByInodeTarget> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.direct_download_target_by_inode(&self.store, inode_id, revision_no, &access)
            .await
    }

    /// Returns the current entry for a visible inode.
    pub async fn stat_inode(
        &self,
        inode_id: InodeId,
        options: StatPathOptions,
        context: &RuntimeReadContext,
    ) -> Result<PathEntry> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.stat_inode(inode_id, options.include_attributes, &access)
            .await
    }

    /// Lists one revision page for a path against the pinned runtime read context.
    pub async fn list_file_revisions_page(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<FileRevisionsPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<(InodeId, Page<FileRevision, FileRevisionsPageCursor>)> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.list_file_revisions_page(path.as_ref(), request, &access)
            .await
    }

    /// Lists one trash page against the pinned runtime read context.
    pub async fn list_trash_page(
        &self,
        request: PageRequest<TrashPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<Page<TrashEntry, TrashPageCursor>> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.list_trash_page(request, &access).await
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
        checkpoint_id: &PinId,
        request: PageRequest<CheckpointFilesPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<CheckpointFilesPage> {
        // Rejects a mismatched or deleted namespace before any read work.
        self.ensure_live_context(context)?;
        crate::checkpoint::list_checkpoint_files_page(
            &self.store,
            Some(context.segment_cache.as_ref()),
            &context.head,
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
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        crate::path::read::resolve_current_files(&view, inode_ids, &access).await
    }

    /// Resolves a reference in the owner's pinned view and verifies resident bytes.
    pub async fn resolve_content_location(
        &self,
        content_ref: &ContentRef,
        context: &RuntimeReadContext,
    ) -> Result<ContentLocation> {
        let location = self
            .load_read_view(context)
            .await?
            .resolve_content_location(content_ref)?;
        if let ContentLocation::Tail { bytes, object_key } = &location {
            validate_loaded_content_bytes(object_key.clone(), content_ref, bytes)?;
        }
        Ok(location)
    }

    /// Reads and verifies the bytes named by a published reference.
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
        self.require_administrator(context).await?;
        self.ensure_live_context(context)?;
        let view = self.load_read_view(context).await?;
        if view
            .metadata_view()
            .find_content_publication(&content_ref.content_id)
            .await?
            .is_none()
        {
            return Err(CoreError::PathNotFound(content_ref.content_id.to_string()));
        }
        crate::path::read::ensure_within_read_limit(content_ref.size_bytes, Some(max_bytes))?;
        let location = view.resolve_content_location(content_ref)?;
        Ok(location.get_bytes(&self.store, content_ref).await?)
    }

    fn ensure_live_context(&self, context: &RuntimeReadContext) -> Result<()> {
        if context.head.namespace_id != self.namespace_id {
            return Err(crate::error::CoreError::NamespaceCorrupt(format!(
                "head namespace `{}` does not match requested namespace `{}`",
                context.head.namespace_id, self.namespace_id
            )));
        }
        crate::namespace::control::ensure_namespace_live(&context.head)?;
        Ok(())
    }

    /// Lists one revision page for an inode against the pinned runtime read context.
    pub async fn list_file_revisions_for_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.list_file_revisions_for_inode_page(inode_id, request, &access)
            .await
    }

    /// Reads one revision's content by path against the pinned runtime
    /// read context.
    pub async fn get_file_revision(
        &self,
        path: impl AsRef<str>,
        revision_no: RevisionNo,
        context: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> Result<FileBytes> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.get_file_revision_bytes(
            &self.store,
            path.as_ref(),
            revision_no,
            max_content_bytes,
            &access,
        )
        .await
    }

    /// Reads one revision's content against the pinned runtime read context.
    pub async fn get_file_revision_for_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        context: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> Result<Vec<u8>> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.get_file_revision_bytes_for_inode(
            &self.store,
            inode_id,
            revision_no,
            max_content_bytes,
            &access,
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
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Publishes already-classified mutation candidates as one batch: one WAL
    /// segment, one numbered WAL put, one result per candidate in order.
    pub async fn publish_namespace_commits_batch(
        &self,
        candidates: Vec<CommitCandidate>,
    ) -> Result<Vec<Result<Commit>>> {
        let context = self.mutation_context()?;
        Ok(crate::commit_engine::publish_namespace_commits_batch(
            &self.store,
            &self.namespace_id,
            candidates,
            &context,
        )
        .await)
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Reads up to `limit` committed changes after `after_seq`.
    pub async fn list_changes_after(
        &self,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
        context: &RuntimeReadContext,
    ) -> Result<ListChangesResponse> {
        let view = self.load_read_view(context).await?;
        crate::protocol::list_changes_after(&view, after_seq, limit).await
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Starts a service-proxied upload session. Direct transports use
    /// [`Self::begin_direct_put_upload_target`] or [`Self::begin_direct_multipart_upload_target`].
    pub async fn begin_upload(&self, subject: Option<&Subject>) -> Result<UploadSession> {
        crate::protocol::begin_service_proxied_upload(
            &self.store,
            &self.namespace_id,
            subject,
            &self.mutation_context()?,
        )
        .await
    }

    /// Starts a direct PUT upload and assigns its content identity.
    pub async fn begin_direct_put_upload_target(
        &self,
        subject: Option<&Subject>,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> Result<BeginDirectPutUploadTargetResponse> {
        crate::protocol::begin_direct_put_upload_target(
            &self.store,
            &self.namespace_id,
            subject,
            checksum_algorithm,
            &self.mutation_context()?,
        )
        .await
    }

    /// Creates a direct multipart upload target with a new object identity,
    /// provider upload ID, and required part size. The final size and checksum are
    /// supplied when the upload is completed.
    pub async fn begin_direct_multipart_upload_target(
        &self,
        subject: Option<&Subject>,
        options: DirectMultipartUploadOptions,
    ) -> Result<BeginDirectMultipartUploadTargetResponse> {
        crate::protocol::begin_direct_multipart_upload_target(
            &self.store,
            &self.namespace_id,
            subject,
            options,
            &self.mutation_context()?,
        )
        .await
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Validates a group of multipart parts and returns the information needed
    /// to sign their upload URLs. This method does not write durable state.
    pub async fn direct_multipart_part_targets(
        &self,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        requested: &[UploadPartChecksumClaim],
    ) -> Result<MultipartPartTargets> {
        crate::protocol::direct_multipart_part_targets(
            &self.store,
            &self.namespace_id,
            upload_id,
            subject,
            requested,
        )
        .await
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Uploads whole-file content into an upload session.
    pub async fn upload_content(
        &self,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        bytes: &[u8],
    ) -> Result<UploadSession> {
        crate::protocol::upload_content(&self.store, &self.namespace_id, upload_id, subject, bytes)
            .await
    }

    /// Uploads content that arrives as a stream into an upload session,
    /// hashing it on the way through instead of holding it.
    pub async fn upload_streamed_content(
        &self,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        body: ByteStream,
    ) -> Result<UploadSession> {
        crate::protocol::upload_streamed_content(
            &self.store,
            &self.namespace_id,
            upload_id,
            subject,
            body,
        )
        .await
    }

    /// Completes an upload session and returns time-bounded proof for later
    /// publication. The caller passes the catalog it already holds, so
    /// completion adds no head read.
    pub async fn complete_upload(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        completion: ResolvedUploadCompletion,
    ) -> Result<CompletedUpload> {
        self.own_catalog(catalog)?;
        crate::protocol::complete_upload(
            &self.store,
            &self.namespace_id,
            upload_id,
            subject,
            completion,
            &self.mutation_context()?,
        )
        .await
    }

    /// Completes an upload after its durable mode selects a request decoder.
    ///
    /// Server integrations use this to decode raw request bytes without a
    /// second upload-session read. A resolver failure is classified as an
    /// invalid upload request.
    pub async fn complete_upload_for_mode<F>(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        resolve: F,
    ) -> Result<CompletedUpload>
    where
        F: FnOnce(UploadMode) -> std::result::Result<ResolvedUploadCompletion, String>,
    {
        self.own_catalog(catalog)?;
        crate::protocol::complete_upload_for_mode(
            &self.store,
            &self.namespace_id,
            upload_id,
            subject,
            resolve,
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

    /// Imports an existing object under a fresh identity owned by this
    /// namespace.
    ///
    /// A content reference locates bytes but does not identify the namespace
    /// whose upload session keeps them alive. This streams the source object
    /// chunk by chunk into a new local upload session, verifying the claimed
    /// size and checksum against what was staged, so collection in the source
    /// namespace cannot invalidate a later publication here.
    pub async fn import_content_ref(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        content_ref: &ContentRef,
    ) -> Result<PreparedContent>
    where
        S: Clone + 'static,
    {
        let catalog = self.own_catalog(catalog)?;
        let context = self.mutation_context()?;
        let body = open_content_import_reader(self.store.clone(), content_ref).await?;
        crate::protocol::stage_owned_stream(
            &self.store,
            catalog,
            body,
            StreamedPayloadKind::ContentImport,
            &context,
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
            StreamedPayloadKind::Request,
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
    pub async fn abort_upload(
        &self,
        upload_id: &UploadId,
        subject: Option<&Subject>,
    ) -> Result<UploadSession> {
        let catalog = crate::namespace::catalog::load_namespace_catalog_entry(
            &self.store,
            &self.namespace_id,
        )
        .await?;
        crate::protocol::abort_upload(
            &self.store,
            &catalog,
            upload_id,
            subject,
            &self.mutation_context()?,
        )
        .await
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Returns an upload session. Completed uploads include a new receipt so
    /// the caller can retry publication without uploading the content again.
    pub async fn get_upload_status(
        &self,
        upload_id: &UploadId,
        subject: Option<&Subject>,
    ) -> Result<UploadSessionView> {
        crate::protocol::get_upload_status(
            &self.store,
            &self.namespace_id,
            upload_id,
            subject,
            self.now_ms()?,
        )
        .await
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates or reuses a named checkpoint for the current namespace head.
    ///
    /// The checkpoint pins a manifest for retention and provenance. If the head
    /// has no manifest, the method first publishes one without compacting
    /// metadata. `ttl_ms` sets an expiration time; `None` keeps the checkpoint
    /// until it is deleted.
    pub async fn create_checkpoint(&self, name: String, ttl_ms: Option<u64>) -> Result<Checkpoint> {
        let context = self.mutation_context()?;
        let expires_at_ms = ttl_ms.map(|ttl_ms| context.now_ms.saturating_add(ttl_ms));
        crate::checkpoint::create_checkpoint(
            &self.store,
            &self.namespace_id,
            PinOwner::User {
                name,
                expires_at_ms,
            },
            &context,
        )
        .await
        .map(crate::checkpoint::checkpoint_summary)
    }

    /// Creates a snapshot of the current namespace state.
    pub async fn create_snapshot(&self, name: String, expires_at_ms: u64) -> Result<Checkpoint> {
        let context = self.mutation_context()?;
        crate::checkpoint::create_checkpoint(
            &self.store,
            &self.namespace_id,
            PinOwner::Snapshot {
                name,
                expires_at_ms,
            },
            &context,
        )
        .await
        .map(crate::checkpoint::checkpoint_summary)
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Lists one page of active checkpoints in ascending id order. Expired
    /// records remain visible until garbage collection deletes them.
    pub async fn list_checkpoints_page(
        &self,
        request: PageRequest<CheckpointPageCursor>,
    ) -> Result<Page<loonfs_api::Checkpoint, CheckpointPageCursor>> {
        crate::checkpoint::list_checkpoints_page(&self.store, &self.namespace_id, request).await
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Deletes a user-owned checkpoint by id.
    ///
    /// A missing pin returns `checkpoint_not_found`.
    /// Deletion makes its unreferenced manifest and runs collectable.
    pub async fn delete_checkpoint(
        &self,
        checkpoint_id: &PinId,
    ) -> Result<DeleteCheckpointResponse> {
        crate::checkpoint::delete_checkpoint(&self.store, &self.namespace_id, checkpoint_id).await
    }

    /// Extends a live snapshot without passing its lifetime ceiling.
    pub async fn extend_snapshot(
        &self,
        checkpoint_id: &PinId,
        requested_expires_at_ms: u64,
        max_lifetime_ms: u64,
    ) -> Result<Checkpoint> {
        crate::checkpoint::extend_snapshot_expiry(
            &self.store,
            &self.namespace_id,
            checkpoint_id,
            requested_expires_at_ms,
            max_lifetime_ms,
            &self.mutation_context()?,
            Arc::new(crate::time::StdMonotonicTimer::default()),
        )
        .await
    }

    /// Deletes a snapshot pin. A missing id returns `snapshot_not_found`.
    pub async fn delete_snapshot(&self, checkpoint_id: &PinId) -> Result<DeleteSnapshotResponse> {
        self.mutation_context()?;
        crate::checkpoint::delete_snapshot(&self.store, &self.namespace_id, checkpoint_id).await
    }

    /// Flushes the visible WAL tail and publishes a
    /// manifest covering the current head.
    ///
    /// This is the latest-state maintenance operation: it absorbs the visible
    /// WAL tail into a new manifest, creating no
    /// pin. Superseded manifests become garbage-collection
    /// candidates once nothing pins them.
    pub async fn flush_wal(&self) -> Result<FlushWalResponse> {
        self.mutation_context()?;
        crate::checkpoint::flush_wal(&self.store, &self.namespace_id).await
    }

    /// Claims the namespace compactor epoch for a maintenance runtime.
    pub async fn claim_compactor(&self) -> Result<u64> {
        crate::checkpoint::claim_compactor(&self.store, &self.namespace_id).await
    }

    /// Merges one bounded run window under the claimed compactor epoch.
    pub async fn reorganize_metadata(
        &self,
        compaction_policy: crate::checkpoint::MetadataCompactionPolicy,
        compactor_epoch: u64,
    ) -> Result<crate::checkpoint::MetadataReorganizeOutcome> {
        crate::checkpoint::reorganize_metadata_step(
            &self.store,
            &self.namespace_id,
            compactor_epoch,
            self.metadata_lsm_policy(),
            compaction_policy,
        )
        .await
    }

    /// Publishes a streaming merge while its epoch and elapsed time remain valid.
    pub async fn run_metadata_compaction(
        &self,
        spec: &crate::checkpoint::MetadataCompactionSpec,
        compactor_epoch: u64,
        cancellation: &crate::checkpoint::MetadataCompactionCancellation,
    ) -> Result<crate::checkpoint::MetadataCompactionJobOutcome> {
        crate::checkpoint::run_metadata_compaction_job(
            &self.store,
            &self.namespace_id,
            compactor_epoch,
            spec,
            self.metadata_lsm_policy(),
            cancellation,
        )
        .await
    }

    /// Advances the retention floor when a verified checkpoint makes it safe.
    pub async fn advance_retention_floor(&self) -> Result<AdvanceRetentionResponse> {
        self.mutation_context()?;
        crate::checkpoint::advance_retention_floor(&self.store, &self.namespace_id).await
    }

    /// Builds the mutation context for this engine's writer identity.
    fn mutation_context(&self) -> Result<MutationContext> {
        Ok(MutationContext {
            writer_id: self.mode.writer_id.clone(),
            now_ms: self.now_ms()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::WalTailProjectionCacheConfig;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    #[test]
    fn namespace_engine_builds_with_required_identity() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let engine = NamespaceEngine::writer(
            store,
            namespace_id.clone(),
            WriterId::parse("writer-a").expect("writer id"),
        );

        assert_eq!(engine.namespace_id(), &namespace_id);
        assert_eq!(engine.writer_id().as_str(), "writer-a");
    }

    #[tokio::test]
    async fn reader_engine_still_serves_reads() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        NamespaceEngine::writer(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            namespace_id.clone(),
            WriterId::parse("writer-a").expect("writer id"),
        )
        .bootstrap_namespace(BootstrapOptions::new(loonfs_test_support::test_actor()))
        .await
        .expect("bootstrap namespace");

        let reader = NamespaceEngine::reader(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            namespace_id.clone(),
        );
        let loaded = crate::namespace::read_anchor::load_read_anchor(&reader.store, &namespace_id)
            .await
            .expect("read basis");
        let context = RuntimeReadContext {
            basis: loaded.basis(),
            head: loaded.read_state,
            segment_cache: Arc::new(MetadataSegmentCache::new(Default::default())),
            tail_cache: Arc::new(WalTailProjectionCache::new(
                WalTailProjectionCacheConfig {
                    max_entries: 1,
                    max_rows: usize::MAX,
                    max_decoded_bytes: usize::MAX,
                },
                None,
            )),
        };
        let changes = reader
            .list_changes_after(
                ChangeSeq(0),
                loonfs_api::PaginationPolicy::default()
                    .resolve_limit(None)
                    .expect("default limit"),
                &context,
            )
            .await
            .expect("a reader-built engine serves reads");
        assert_eq!(changes.namespace_id, namespace_id);
    }

    #[tokio::test]
    async fn reader_engine_reads_upload_status_without_writer_identity() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = NamespaceEngine::writer(
            LocalFsStore::new(temp_dir.path()).expect("writer store"),
            namespace_id.clone(),
            WriterId::parse("writer-a").expect("writer id"),
        );
        writer
            .bootstrap_namespace(BootstrapOptions::new(loonfs_test_support::test_actor()))
            .await
            .expect("bootstrap namespace");
        let begun = writer.begin_upload(None).await.expect("begin upload");

        let reader = NamespaceEngine::reader(
            LocalFsStore::new(temp_dir.path()).expect("reader store"),
            namespace_id,
        );
        let crate::UploadSessionView {
            session: status,
            receipt,
            ..
        } = reader
            .get_upload_status(&begun.upload_id, None)
            .await
            .expect("reader engine reads upload status");

        assert_eq!(status.upload_id, begun.upload_id);
        assert!(receipt.is_none());
    }
}
