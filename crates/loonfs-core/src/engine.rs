//! [`NamespaceEngine`]: the namespace-scoped entry point for reads, writes,
//! uploads, snapshots, and maintenance.

use crate::authorize::{Authorizer, CommitAuthority, ReadAccess};
use crate::cache::{HeadAndWalTailCache, MetadataSegmentCache, ObjectCache};
use crate::commit_engine::CommitCandidate;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::manifest::{FoldWalResponse, PublicationReport};
use crate::metadata::access::is_administrator;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::{bootstrap, fork};
use crate::options::{CreateNamespaceOptions, DeleteNamespaceOptions};
use crate::path::read::{
    load_metadata_view, load_metadata_view_for_authorization, CurrentFileState,
    DirectDownloadByInodeTarget, DirectDownloadTarget, FileBytes, LoadedMetadataView,
    ReadLoadContext,
};
use crate::pin::{
    CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointPageCursor,
    ListCheckpointFilesOptions, SnapshotPageCursor,
};
use crate::protocol::{
    CompletedUpload, DirectMultipartUploadTarget, DirectPutUploadTarget, MultipartPartTargets,
    UploadSessionView,
};
use crate::storage::content::{
    open_content_import_reader, ContentLocation, DurableContentValidationError, FileContentStream,
    StreamRange, StreamedPayloadKind,
};
use crate::storage::content_admission::PreparedContent;
use crate::wal::ProjectedWalTail;
use loonfs_objectstore::{ByteStream, ObjectStore};
use loonfs_types::api::v0::{
    Commit, CompleteUploadBody, ListChangesResponse, UploadPartChecksumClaim, UploadSession,
};
use loonfs_types::format::control::PinOwner;
use loonfs_types::options::{AccessState, DirectMultipartUploadOptions, ListOptions, StatOptions};
use loonfs_types::CompactorEpoch;
use loonfs_types::EffectiveLimit;
use loonfs_types::{
    AdvanceRetentionResponse, ChangeSeq, CommitId, ContentRef, DeleteNamespaceResponse,
    DeleteSnapshotResponse, FileRevision, FileRevisionsPageCursor, InodeId, ListPageCursor,
    NamespaceAccess, NamespaceId, NamespaceMetadata, Page, PageRequest, PathEntry, PinId,
    RevisionNo, SetRetentionWindowResponse, Snapshot, Subject, TrashEntry, TrashPageCursor,
    UploadId, WriterId, ROOT_INODE_ID,
};
use std::num::NonZeroU64;
use std::sync::Arc;
use tracing::Instrument;

/// Read context pinned by the runtime for one request. It contains the head,
/// metadata basis, and shared caches needed to serve every read from the same
/// namespace snapshot.
///
/// This type supports the `loonfs` runtime. Applications should use the
/// higher-level `loonfs` runtime and namespace handles instead.
#[derive(Debug, Clone)]
pub struct RuntimeReadContext {
    pub head: NamespaceReadState,
    /// Verified manifest used to replay the pinned WAL tail.
    pub basis: MetadataBasis,
    pub segment_cache: Arc<MetadataSegmentCache>,
    pub head_and_wal_tail_cache: Arc<HeadAndWalTailCache>,
    pub object_cache: Arc<ObjectCache>,
    /// The projected WAL tail above `basis` at `head`, when the context
    /// starts from a writer session's position. Reads then take this tail
    /// and don't look one up in `head_and_wal_tail_cache`.
    pub tail: Option<Arc<ProjectedWalTail>>,
}

fn runtime_read_load_context(context: &RuntimeReadContext) -> ReadLoadContext<'_, '_> {
    ReadLoadContext::pinned_head(
        &context.head,
        &context.basis,
        context.tail.as_ref(),
        Some(&context.segment_cache),
        Some(&context.head_and_wal_tail_cache),
        Some(&context.object_cache),
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

/// The root's access row: its boundary flag, grants, and revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootAccess {
    /// The root's boundary flag and direct grants, which are empty before the
    /// root has an access row.
    pub access: AccessState,
    /// The row's access revision, which is 0 before the root has an access
    /// row. Commit validation treats a missing row as revision 0 too.
    pub access_revision_no: loonfs_types::AccessRevisionNo,
}

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
    content_writes: Arc<crate::PermitPool>,
    wall_clock: Arc<dyn crate::time::WallClock>,
    timer: Arc<dyn crate::time::MonotonicTimer>,
    subject: Option<Subject>,
    authorization_head: Option<RuntimeReadContext>,
    run_policy: crate::manifest::MetadataRunPolicy,
    segment_cache: Option<Arc<MetadataSegmentCache>>,
    /// A narrowed per-segment row budget, so a test can get many compacted
    /// segments from a few thousand rows. See [`Self::narrow_segment_row_budget`].
    #[cfg(any(test, feature = "test-support"))]
    segment_row_budget: Option<std::num::NonZeroUsize>,
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

    /// Supplies the wall clock behind every timestamp this engine records and
    /// every commit age that its retention advances measure.
    pub fn with_wall_clock(mut self, wall_clock: Arc<dyn crate::time::WallClock>) -> Self {
        self.wall_clock = wall_clock;
        self
    }

    /// Supplies the monotonic timer behind every deadline and observation
    /// this engine starts.
    pub fn with_monotonic_timer(mut self, timer: Arc<dyn loonfs_types::MonotonicTimer>) -> Self {
        self.timer = timer;
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
    pub fn ensure_subject(&self, context: &RuntimeReadContext) -> Result<()> {
        Authorizer::for_request(
            &self.namespace_id,
            &context.head.access,
            CommitAuthority::Subject(self.subject.as_ref()),
        )
        .map(|_| ())
    }

    /// The change feed and bare content reference reads and imports work for
    /// the token holder and for an administrator; any other subject is refused.
    pub async fn ensure_administrator(&self, context: &RuntimeReadContext) -> Result<()> {
        let Some(subject) = &self.subject else {
            return Ok(());
        };
        // Historical data does not restore historical administrator grants.
        let context = self.authorization_head.as_ref().unwrap_or(context);
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

    /// Reads the root's current access row. Refused on an unrestricted
    /// namespace, which holds no access rows.
    pub async fn root_access(&self, context: &RuntimeReadContext) -> Result<RootAccess> {
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
            RootAccess {
                access: AccessState {
                    boundary: false,
                    grants: loonfs_types::AccessGrants::default(),
                },
                access_revision_no: loonfs_types::AccessRevisionNo(0),
            },
            |row| RootAccess {
                access: AccessState {
                    boundary: row.boundary,
                    grants: row.grants,
                },
                access_revision_no: row.access_revision_no,
            },
        ))
    }

    /// Returns the namespace this engine is bound to.
    pub fn namespace_id(&self) -> &NamespaceId {
        &self.namespace_id
    }

    /// Returns the current wall-clock time.
    fn now_ms(&self) -> u64 {
        self.wall_clock.now_ms()
    }

    /// Stats one path against the pinned runtime read context.
    pub async fn resolve_path(
        &self,
        path: impl AsRef<str>,
        options: StatOptions,
        context: &RuntimeReadContext,
    ) -> Result<PathEntry> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.resolve_path(path.as_ref(), options.include_attributes, &access)
            .await
    }

    /// Lists one directory page against the pinned runtime read context, of
    /// the directory's children or, with `options.recursive`, of its subtree.
    pub async fn list_path_page(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
        context: &RuntimeReadContext,
    ) -> Result<Page<PathEntry, ListPageCursor>> {
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
        view.list_path_page(path.as_ref(), request, &options, &access)
            .await
    }

    /// Lists one page of a directory's children by inode against the pinned
    /// runtime read context, or, with `options.recursive`, one page of its
    /// subtree.
    pub async fn list_inode_children_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<ListPageCursor>,
        options: ListOptions,
        context: &RuntimeReadContext,
    ) -> Result<Page<PathEntry, ListPageCursor>> {
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
        view.list_inode_children_page(inode_id, request, &options, &access)
            .await
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    fn new(
        store: S,
        namespace_id: NamespaceId,
        content_writes: Arc<crate::PermitPool>,
        mode: M,
    ) -> Self {
        Self {
            store,
            namespace_id,
            content_writes,
            mode,
            wall_clock: Arc::new(crate::time::SystemWallClock),
            timer: Arc::new(crate::time::StdMonotonicTimer::default()),
            subject: None,
            authorization_head: None,
            run_policy: crate::manifest::MetadataRunPolicy::default(),
            segment_cache: None,
            #[cfg(any(test, feature = "test-support"))]
            #[cfg(any(test, feature = "test-support"))]
            segment_row_budget: None,
        }
    }
}

impl<S: ObjectStore> NamespaceEngine<S, ReadOnly> {
    /// Creates a read-only engine bound to `namespace_id`.
    pub fn reader(
        store: S,
        namespace_id: NamespaceId,
        content_writes: Arc<crate::PermitPool>,
    ) -> Self {
        Self::new(store, namespace_id, content_writes, ReadOnly)
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates a writable engine bound to `namespace_id` and `writer_id`.
    pub fn writer(
        store: S,
        namespace_id: NamespaceId,
        writer_id: WriterId,
        content_writes: Arc<crate::PermitPool>,
    ) -> Self {
        Self::new(store, namespace_id, content_writes, Writable { writer_id })
    }

    /// Returns the writer id used for epoch acquisition and commit publication.
    pub fn writer_id(&self) -> &WriterId {
        &self.mode.writer_id
    }

    /// Sets the budgets this engine's WAL folds and compactions run under.
    pub fn with_metadata_run_policy(mut self, policy: crate::manifest::MetadataRunPolicy) -> Self {
        self.run_policy = policy;
        self
    }

    /// Uses the runtime's metadata segment cache for this engine's folds,
    /// compactions, pins, and retention floor advances.
    pub fn with_metadata_segment_cache(mut self, segment_cache: Arc<MetadataSegmentCache>) -> Self {
        self.segment_cache = Some(segment_cache);
        self
    }

    /// The compaction budgets this engine plans and compacts under.
    fn effective_run_policy(&self) -> crate::manifest::MetadataRunPolicy {
        let policy = self.run_policy;
        #[cfg(any(test, feature = "test-support"))]
        let policy = crate::manifest::MetadataRunPolicy {
            max_rows_per_segment: self
                .segment_row_budget
                .unwrap_or(policy.max_rows_per_segment),
            ..policy
        };
        policy
    }

    /// Narrows the rows one compacted segment may hold, so a namespace a test
    /// can build in seconds compacts into many segments.
    ///
    /// The shipped segment target is 8 MiB of decoded rows, which takes over
    /// a hundred thousand rows to fill several times. Test-only: folds keep
    /// the shipped segment shape, and planning and publishing are unchanged.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn narrow_segment_row_budget(
        mut self,
        max_rows_per_segment: std::num::NonZeroUsize,
    ) -> Self {
        self.segment_row_budget = Some(max_rows_per_segment);
        self
    }

    /// Creates the namespace if it does not already exist.
    ///
    /// Use this before normal reads and writes for a new namespace. Returns
    /// the namespace's status after manifest 1 is installed.
    pub async fn bootstrap_namespace(
        &self,
        actor_id: &loonfs_types::ActorId,
        options: &CreateNamespaceOptions,
    ) -> Result<NamespaceMetadata> {
        bootstrap::bootstrap_namespace(
            &self.store,
            &self.namespace_id,
            &self.mutation_context(),
            actor_id,
            options,
            Arc::clone(&self.timer),
        )
        .await
    }

    /// Creates a new namespace at this namespace's current head or an unexpired snapshot.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    /// Returns the target's status at the fork point.
    pub async fn fork_namespace(
        &self,
        target: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        snapshot_id: Option<&PinId>,
    ) -> Result<NamespaceMetadata> {
        fork::fork_namespace(
            &self.store,
            &self.namespace_id,
            target,
            actor_id,
            snapshot_id,
            &self.mutation_context(),
            Arc::clone(&self.timer),
            self.effective_run_policy(),
            self.segment_cache.as_deref(),
            &self.content_writes,
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
            &self.mutation_context(),
            self.effective_run_policy(),
            Arc::clone(&self.content_writes),
            Arc::clone(&self.timer),
        )
        .await
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Reads file content against the pinned runtime read context.
    pub async fn read_file(
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

    /// Reads the current content of a visible file inode, wherever it is
    /// bound, with its entry.
    pub async fn read_file_by_inode(
        &self,
        inode_id: InodeId,
        context: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> Result<FileBytes> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.get_file_bytes_by_inode(&self.store, inode_id, max_content_bytes, &access)
            .await
    }

    /// Opens a chunked stream for the file resolved from the pinned read context.
    ///
    /// The object prefix is fetched `chunk_bytes` at a time, and pieces the tail
    /// holds are retained. The buffered-read size limit does not apply. Later
    /// commits cannot change the bytes being read, and the read writes no
    /// content object.
    ///
    /// When a whole read's `start_offset` is nonzero, the caller must pass the
    /// skipped prefix to [`FileContentStream::fold_resumed_prefix`] before
    /// fetching more data. This allows the stream to verify the checksum of
    /// the complete object. A ranged read returns its range without
    /// verification.
    pub async fn read_file_stream(
        &self,
        path: impl AsRef<str>,
        context: &RuntimeReadContext,
        revision_no: Option<RevisionNo>,
        chunk_bytes: NonZeroU64,
        range: StreamRange,
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
        range.ensure_within(content_ref.size_bytes)?;
        Ok(FileContentStream::open_inner(
            self.store.clone(),
            view.resolve_content_location(&content_ref).await?,
            Some(entry),
            content_ref,
            chunk_bytes,
            range,
        )
        .await?
        .with_object_cache(Arc::clone(&context.object_cache)))
    }

    /// Streams inode content: one retained revision without requiring a
    /// visible path, or the current revision of a visible file.
    pub async fn read_file_stream_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: Option<RevisionNo>,
        context: &RuntimeReadContext,
        chunk_bytes: NonZeroU64,
        range: StreamRange,
    ) -> Result<FileContentStream<S>>
    where
        S: Clone,
    {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        let content_ref = match revision_no {
            Some(revision_no) => {
                view.authorized_revision_by_inode(inode_id, revision_no, &access)
                    .await?
            }
            None => view.current_revision_by_inode(inode_id, &access).await?,
        }
        .content_ref;
        range.ensure_within(content_ref.size_bytes)?;
        Ok(FileContentStream::open_inner(
            self.store.clone(),
            view.resolve_content_location(&content_ref).await?,
            None,
            content_ref,
            chunk_bytes,
            range,
        )
        .await?
        .with_object_cache(Arc::clone(&context.object_cache)))
    }

    /// Prepares a file's content object for a direct download.
    pub async fn direct_download_target(
        &self,
        path: impl AsRef<str>,
        revision_no: Option<RevisionNo>,
        start_offset: u64,
        context: &RuntimeReadContext,
    ) -> Result<DirectDownloadTarget> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.direct_download_target(
            &self.store,
            path.as_ref(),
            revision_no,
            start_offset,
            &access,
            &self.content_writes,
        )
        .await
    }

    /// Prepares inode content for a direct download: a retained revision, or
    /// the current revision of a visible file when `revision_no` is absent.
    pub async fn direct_download_target_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: Option<RevisionNo>,
        start_offset: u64,
        context: &RuntimeReadContext,
    ) -> Result<DirectDownloadByInodeTarget> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.direct_download_target_by_inode(
            &self.store,
            inode_id,
            revision_no,
            start_offset,
            &access,
            &self.content_writes,
        )
        .await
    }

    /// Returns the current entry for a visible inode.
    pub async fn stat_inode(
        &self,
        inode_id: InodeId,
        options: StatOptions,
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

    /// Lists files from the manifest pinned by the snapshot `checkpoint_id`,
    /// ordered by inode ID.
    ///
    /// The method ignores the current metadata basis and WAL tail, so every page
    /// comes from the same snapshot even when new commits arrive. The read
    /// context is used only to validate the namespace identity and confirm that
    /// it has not been deleted. A snapshot that expired at or before `now_ms`
    /// returns `snapshot_not_found`.
    pub async fn list_checkpoint_files_page(
        &self,
        checkpoint_id: &PinId,
        request: PageRequest<CheckpointFilesPageCursor>,
        options: ListCheckpointFilesOptions,
        context: &RuntimeReadContext,
        now_ms: u64,
    ) -> Result<CheckpointFilesPage> {
        // Rejects a mismatched or deleted namespace before any read work.
        self.ensure_live_context(context)?;
        crate::pin::list_checkpoint_files_page(
            &self.store,
            Some(context.segment_cache.as_ref()),
            &context.head,
            checkpoint_id,
            request,
            options,
            now_ms,
        )
        .await
    }

    /// Resolves the current visibility, revision, and path for each inode ID.
    ///
    /// All results use the same pinned snapshot and preserve input order. Missing
    /// inode IDs are returned as not visible because callers may hold IDs from an
    /// older listing. Requests above
    /// [`MAX_CURRENT_FILES_PER_STAT`](crate::MAX_CURRENT_FILES_PER_STAT) fail before
    /// reading metadata.
    pub async fn stat_current_files_by_inode(
        &self,
        inode_ids: &[InodeId],
        context: &RuntimeReadContext,
    ) -> Result<Vec<CurrentFileState>> {
        crate::path::read::ensure_stat_batch_within_cap(inode_ids.len())?;
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        crate::path::read::stat_current_files_by_inode(&view, inode_ids, &access).await
    }

    /// Resolves an import source, including an unpublished upload.
    pub async fn resolve_content_location(
        &self,
        content_ref: &ContentRef,
        context: &RuntimeReadContext,
    ) -> Result<ContentLocation> {
        let view = load_metadata_view_for_authorization(
            &self.store,
            &self.namespace_id,
            runtime_read_load_context(context),
        )
        .await?;
        if view
            .metadata_view()
            .content_published(&content_ref.content_id)
            .await?
        {
            view.resolve_content_location(content_ref).await
        } else {
            Ok(ContentLocation::whole(content_ref)?)
        }
    }

    /// Reads and verifies the bytes named by a published reference.
    ///
    /// `max_bytes` is checked against the declared size before the fetch and is
    /// independent of deployment-wide download limits. The method returns an
    /// error if the fetched size or checksum does not match the reference. A
    /// published reference whose object collection reclaimed answers like an
    /// unpublished one.
    pub async fn read_content_ref(
        &self,
        content_ref: &ContentRef,
        max_bytes: u64,
        context: &RuntimeReadContext,
    ) -> Result<Vec<u8>> {
        self.ensure_administrator(context).await?;
        self.ensure_live_context(context)?;
        let view = self.load_read_view(context).await?;
        if !view
            .metadata_view()
            .content_published(&content_ref.content_id)
            .await?
        {
            return Err(CoreError::PathNotFound(content_ref.content_id.to_string()));
        }
        crate::path::read::ensure_within_read_limit(content_ref.size_bytes, Some(max_bytes))?;
        let location = view.resolve_content_location(content_ref).await?;
        location
            .read_bytes(&self.store, Some(&context.object_cache), content_ref)
            .await
            .map_err(|error| match error {
                DurableContentValidationError::MissingContentObject { .. } => {
                    CoreError::PathNotFound(content_ref.content_id.to_string())
                }
                error => error.into(),
            })
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
    pub async fn list_file_revisions_by_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.list_file_revisions_by_inode_page(inode_id, request, &access)
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
    ) -> Result<FileBytes> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.read_file_revision_bytes(
            &self.store,
            path.as_ref(),
            revision_no,
            max_content_bytes,
            &access,
        )
        .await
    }

    /// Reads one revision's content against the pinned runtime read context.
    pub async fn read_file_revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        context: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> Result<Vec<u8>> {
        let head_view = self.authorization_head_view().await?;
        let access = self.read_access(context, head_view.as_ref())?;
        let view = self.load_read_view(context).await?;
        view.read_file_revision_bytes_by_inode(
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
    /// object, one numbered WAL put, one result per candidate in order.
    pub async fn publish_namespace_commits_batch(
        &self,
        candidates: Vec<CommitCandidate>,
    ) -> Result<Vec<Result<Commit>>> {
        let context = self.mutation_context();
        Ok(crate::commit_engine::publish_namespace_commits_batch(
            &self.store,
            &self.namespace_id,
            candidates,
            &context,
            Arc::clone(&self.content_writes),
            Arc::clone(&self.timer),
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
            &self.mutation_context(),
        )
        .await
    }

    /// Starts a direct PUT upload and assigns its content identity.
    pub async fn begin_direct_put_upload_target(
        &self,
        subject: Option<&Subject>,
    ) -> Result<DirectPutUploadTarget> {
        crate::protocol::begin_direct_put_upload_target(
            &self.store,
            &self.namespace_id,
            subject,
            &self.mutation_context(),
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
    ) -> Result<DirectMultipartUploadTarget> {
        crate::protocol::begin_direct_multipart_upload_target(
            &self.store,
            &self.namespace_id,
            subject,
            options,
            &self.mutation_context(),
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
            self.now_ms(),
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
        let _permit = self.content_writes.acquire().await;
        crate::protocol::upload_content(
            &self.store,
            &self.namespace_id,
            upload_id,
            subject,
            bytes,
            self.now_ms(),
        )
        .await
    }

    /// Uploads content that arrives as a stream into an upload session,
    /// hashing it on the way through instead of holding it.
    pub async fn upload_content_stream(
        &self,
        upload_id: &UploadId,
        subject: Option<&Subject>,
        body: ByteStream,
    ) -> Result<UploadSession> {
        let _permit = self.content_writes.acquire().await;
        crate::protocol::upload_content_stream(
            &self.store,
            &self.namespace_id,
            upload_id,
            subject,
            body,
            self.now_ms(),
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
        completion: CompleteUploadBody,
    ) -> Result<CompletedUpload> {
        self.own_catalog(catalog);
        crate::protocol::complete_upload(
            &self.store,
            &self.namespace_id,
            upload_id,
            subject,
            completion,
            &self.mutation_context(),
        )
        .await
    }

    /// Stores in-process bytes as prepared content and completes the associated
    /// upload session.
    ///
    /// This combines session creation, content upload, and completion without a
    /// network round trip or receipt. It still writes the upload session's
    /// records that garbage collection needs: the session record before the
    /// content write, and the outcome record after it.
    pub async fn stage_owned_bytes(
        &self,
        catalog: &VerifiedNamespaceCatalogEntry,
        subject_id: Option<&loonfs_types::SubjectId>,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        let _permit = self.content_writes.acquire().await;
        crate::protocol::stage_owned_bytes(
            &self.store,
            self.own_catalog(catalog),
            subject_id,
            bytes,
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
        subject_id: Option<&loonfs_types::SubjectId>,
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
            None => ContentLocation::whole(content_ref)?,
        };
        let body = open_content_import_reader(self.store.clone(), location, content_ref).await?;
        let _permit = self.content_writes.acquire().await;
        crate::protocol::stage_owned_stream(
            &self.store,
            catalog,
            subject_id,
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
        subject_id: Option<&loonfs_types::SubjectId>,
        body: ByteStream,
    ) -> Result<PreparedContent> {
        let _permit = self.content_writes.acquire().await;
        crate::protocol::stage_owned_stream(
            &self.store,
            self.own_catalog(catalog),
            subject_id,
            body,
            StreamedPayloadKind::Request,
            &self.mutation_context(),
        )
        .await
    }

    /// Verifies that a runtime-supplied catalog belongs to this engine's
    /// namespace. A mismatch indicates an internal integration error.
    fn own_catalog<'c>(
        &self,
        catalog: &'c VerifiedNamespaceCatalogEntry,
    ) -> &'c VerifiedNamespaceCatalogEntry {
        assert_eq!(
            catalog.namespace_id(),
            &self.namespace_id,
            "a runtime should give an engine its own namespace's catalog"
        );
        catalog
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
            &self.mutation_context(),
        )
        .await
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Returns an upload session. Completed uploads include new evidence so
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
            self.now_ms(),
        )
        .await
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates a snapshot of the current namespace head.
    ///
    /// Every call generates a fresh `PinId`, and `name` is a label, not a
    /// key. When WAL objects follow the current manifest's `folded_wal_no`,
    /// the method first folds them into a new manifest without compacting
    /// metadata. A snapshot with `expires_at_ms` can't be read after that
    /// time, and it stays listed until collection deletes it. `None` keeps
    /// the snapshot until it's deleted. The report includes the manifest the
    /// fold published.
    pub async fn create_snapshot(
        &self,
        name: String,
        expires_at_ms: Option<u64>,
    ) -> Result<PublicationReport<Snapshot>> {
        let context = self.mutation_context();
        crate::pin::create_pin(
            &self.store,
            &self.namespace_id,
            PinOwner::User {
                name,
                expires_at_ms,
            },
            &context,
            self.effective_run_policy(),
            self.segment_cache.as_deref(),
            &self.content_writes,
            Arc::clone(&self.timer),
        )
        .await
        .map(|report| report.map(crate::pin::snapshot_summary))
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Lists one page of existing checkpoints in ascending id order. Expired
    /// records remain visible until garbage collection deletes them.
    pub async fn list_checkpoints_page(
        &self,
        request: PageRequest<CheckpointPageCursor>,
    ) -> Result<Page<loonfs_types::Checkpoint, CheckpointPageCursor>> {
        crate::pin::list_checkpoints_page(&self.store, &self.namespace_id, request).await
    }

    /// Loads one existing pin of any owner by id, as the checkpoint listing
    /// returns it. An expired record stays readable until garbage collection
    /// deletes it. A missing pin returns `snapshot_not_found`.
    pub async fn get_checkpoint(&self, checkpoint_id: &PinId) -> Result<loonfs_types::Checkpoint> {
        crate::pin::get_checkpoint(&self.store, &self.namespace_id, checkpoint_id).await
    }

    /// Lists one page of snapshots in ascending id order, including expired
    /// snapshots that garbage collection hasn't deleted.
    pub async fn list_snapshots_page(
        &self,
        request: PageRequest<SnapshotPageCursor>,
    ) -> Result<Page<Snapshot, SnapshotPageCursor>> {
        crate::pin::list_snapshots_page(&self.store, &self.namespace_id, request).await
    }

    /// Loads one snapshot by id, as the snapshot listing returns it,
    /// including an expired snapshot that garbage collection hasn't deleted.
    /// A missing id, or the id of a fork pin, returns `snapshot_not_found`.
    pub async fn get_snapshot(&self, snapshot_id: &PinId) -> Result<Snapshot> {
        crate::pin::get_snapshot(&self.store, &self.namespace_id, snapshot_id).await
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Moves an unexpired snapshot's expiry to `requested_expires_at_ms`,
    /// never earlier. A snapshot without an expiry is returned unchanged.
    pub async fn extend_snapshot(
        &self,
        snapshot_id: &PinId,
        requested_expires_at_ms: u64,
    ) -> Result<Snapshot> {
        crate::pin::extend_snapshot_expiry(
            &self.store,
            &self.namespace_id,
            snapshot_id,
            requested_expires_at_ms,
            &self.mutation_context(),
            Arc::clone(&self.timer),
        )
        .await
    }

    /// Deletes a snapshot. Deleting a missing snapshot succeeds, and the id
    /// of a fork pin returns `snapshot_not_found`.
    pub async fn delete_snapshot(&self, snapshot_id: &PinId) -> Result<DeleteSnapshotResponse> {
        crate::pin::delete_snapshot(&self.store, &self.namespace_id, snapshot_id).await
    }

    /// Folds the visible WAL tail and publishes a
    /// manifest covering the current head.
    ///
    /// This is the latest-state maintenance operation: it absorbs the visible
    /// WAL tail into a new manifest, creating no
    /// pin. Superseded manifests become garbage-collection
    /// candidates once nothing pins them. The report includes the manifest
    /// this call published.
    pub async fn fold_wal(&self) -> Result<PublicationReport<FoldWalResponse>> {
        let folded = crate::manifest::fold_wal_tail(
            &self.store,
            self.segment_cache.as_deref(),
            &self.namespace_id,
            None,
            &crate::time::Deadline::start(Arc::clone(&self.timer)),
            self.effective_run_policy(),
            &self.content_writes,
        )
        .await?;
        Ok(PublicationReport {
            published: folded.published(),
            response: folded.response,
        })
    }

    /// Claims the namespace compactor epoch for a maintenance runtime. The
    /// report includes the manifest that records the claim.
    pub async fn claim_compactor(&self) -> Result<PublicationReport<CompactorEpoch>> {
        crate::manifest::claim_compactor(&self.store, &self.namespace_id, Arc::clone(&self.timer))
            .await
    }

    /// Runs one metadata compaction unit under the claimed compactor epoch.
    ///
    /// `admission` decides whether the unit may merge a window over the run
    /// policy's row budget, and `cancellation` stops the merge between blocks.
    pub async fn compact_metadata_unit(
        &self,
        compaction_policy: crate::manifest::MetadataCompactionPolicy,
        admission: crate::manifest::CompactionAdmission,
        compactor_epoch: CompactorEpoch,
        cancellation: &crate::manifest::MetadataCompactionCancellation,
    ) -> Result<crate::manifest::CompactionUnitOutcome> {
        crate::manifest::compact_unit(
            &self.store,
            self.segment_cache.as_deref(),
            &self.namespace_id,
            compactor_epoch,
            self.effective_run_policy(),
            compaction_policy,
            admission,
            cancellation,
            Arc::clone(&self.timer),
        )
        .await
    }

    /// Advances the retention floor toward `target` when a verified checkpoint
    /// makes it safe, measuring commit ages on this engine's wall clock. The
    /// report includes the manifest that records the new floor.
    pub async fn advance_retention_floor(
        &self,
        target: crate::manifest::RetentionTarget,
    ) -> Result<PublicationReport<AdvanceRetentionResponse>> {
        crate::manifest::advance_retention_floor(
            &self.store,
            self.segment_cache.as_deref(),
            &self.namespace_id,
            target,
            self.now_ms(),
            Arc::clone(&self.timer),
        )
        .await
    }

    /// Sets the namespace's retention window, or clears it with `None`. The
    /// report includes the manifest that records the change.
    pub async fn set_retention_window(
        &self,
        retention_window_ms: Option<NonZeroU64>,
    ) -> Result<PublicationReport<SetRetentionWindowResponse>> {
        crate::manifest::set_retention_window(
            &self.store,
            &self.namespace_id,
            retention_window_ms,
            Arc::clone(&self.timer),
        )
        .await
    }

    /// Builds the mutation context for this engine's writer identity.
    fn mutation_context(&self) -> MutationContext {
        MutationContext {
            writer_id: self.mode.writer_id.clone(),
            now_ms: self.now_ms(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    #[tokio::test]
    async fn reader_engine_still_serves_reads() {
        let temp_dir = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = NamespaceEngine::writer(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            namespace_id.clone(),
            WriterId::parse("writer-a").expect("writer id"),
            std::sync::Arc::new(crate::test_support::content_writes()),
        );
        crate::test_support::ops::create(&writer.store, &namespace_id, &writer.mutation_context())
            .await
            .expect("bootstrap namespace");

        let reader = NamespaceEngine::reader(
            LocalFsStore::new(temp_dir.path()).expect("store"),
            namespace_id.clone(),
            std::sync::Arc::new(crate::test_support::content_writes()),
        );
        let loaded = crate::namespace::read_anchor::load_read_anchor(
            &reader.store,
            &namespace_id,
            crate::test_support::observed_now(),
        )
        .await
        .expect("read basis");
        let context = RuntimeReadContext {
            basis: loaded.basis(),
            head: loaded.read_state,
            segment_cache: Arc::new(MetadataSegmentCache::unshared(usize::MAX)),
            head_and_wal_tail_cache: Arc::new(HeadAndWalTailCache::unshared(usize::MAX)),
            object_cache: Arc::new(ObjectCache::unshared(0, 0)),
            tail: None,
        };
        let changes = reader
            .list_changes_after(
                ChangeSeq(0),
                loonfs_types::PaginationPolicy::default()
                    .effective_limit(None)
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
            std::sync::Arc::new(crate::test_support::content_writes()),
        );
        crate::test_support::ops::create(&writer.store, &namespace_id, &writer.mutation_context())
            .await
            .expect("bootstrap namespace");
        let begun = writer.begin_upload(None).await.expect("begin upload");

        let reader = NamespaceEngine::reader(
            LocalFsStore::new(temp_dir.path()).expect("reader store"),
            namespace_id,
            std::sync::Arc::new(crate::test_support::content_writes()),
        );
        let crate::UploadSessionView {
            session: status,
            evidence,
            ..
        } = reader
            .get_upload_status(&begun.upload_id, None)
            .await
            .expect("reader engine reads upload status");

        assert_eq!(status.upload_id, begun.upload_id);
        assert!(evidence.is_none());
    }
}
