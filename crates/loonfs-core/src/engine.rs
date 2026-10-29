//! [`NamespaceEngine`]: the namespace-scoped entry point for reads, writes,
//! uploads, snapshots, and maintenance. This module contains the type, its
//! constructors, its authorization, and its per-subject reads; each other
//! operation is a method in the module that implements it.

use crate::authorize::{Authorizer, CommitAuthority, ReadAccess};
use crate::commit::engine::NamespaceCommitEngine;
use crate::commit::publish_view::WriterPosition;
use crate::error::{CoreError, Result};
use crate::metadata::access::is_administrator;
use crate::namespace::basis::ReadBasis;
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::store::NamespaceStore;
use crate::path::read::{
    CurrentFileState, DirectDownloadByInodeTarget, DirectDownloadTarget, FileBytes,
    LoadedMetadataView,
};
use crate::storage::content::DurableContentValidationError;
use crate::storage::content_location::ContentLocation;
use crate::storage::content_stream::{FileContentStream, StreamRange};
use crate::wal::ProjectedWalTail;
use loonfs_objectstore::ObjectStore;
use loonfs_types::options::{AccessState, ListOptions, StatOptions};
use loonfs_types::EffectiveLimit;
use loonfs_types::ListChangesResponse;
use loonfs_types::{
    ChangeSeq, CommitId, ContentRef, FileRevision, FileRevisionsPageCursor, InodeId,
    ListPageCursor, NamespaceAccess, NamespaceId, Page, PageRequest, PathEntry, RevisionNo,
    Subject, TrashEntry, TrashPageCursor, WriterId, ROOT_INODE_ID,
};
use std::num::NonZeroU64;
use std::sync::Arc;
use tracing::Instrument;

/// The head and metadata basis that the runtime pins for one request, so
/// every read of the request sees the same namespace state.
///
/// This type supports the `loonfs` runtime. Applications should use the
/// higher-level `loonfs` runtime and namespace handles instead.
#[derive(Debug, Clone)]
pub struct RuntimeReadContext {
    pub read_basis: ReadBasis,
    /// The projected WAL tail above the basis at the head, when the context
    /// starts from a writer session's position. Reads then take this tail
    /// and don't look one up in the namespace's WAL tail cache.
    pub tail: Option<Arc<ProjectedWalTail>>,
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
/// `NamespaceEngine` owns one namespace's [`NamespaceStore`] and exposes
/// operations selected by its mode. Use [`NamespaceEngine::reader`] for reads
/// only or [`NamespaceEngine::writer`] for reads and mutations.
#[derive(Debug)]
pub struct NamespaceEngine<S, M> {
    pub(crate) namespace: NamespaceStore<S>,
    mode: M,
    pub(crate) subject: Option<Subject>,
    authorization_head: Option<RuntimeReadContext>,
    /// The writer session's position that [`NamespaceEngine::fold_wal`]
    /// folds, or `None` to load the tail from the store.
    pub(crate) writer_position: Option<WriterPosition>,
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    fn new(namespace: NamespaceStore<S>, mode: M) -> Self {
        Self {
            namespace,
            mode,
            subject: None,
            authorization_head: None,
            writer_position: None,
        }
    }

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

    /// Sets the subject for this engine's reads and uploads.
    pub fn with_subject(mut self, subject: Subject) -> Self {
        self.subject = Some(subject);
        self
    }

    /// Sets the current head used to authorize historical reads.
    pub fn with_authorization_head(mut self, head: RuntimeReadContext) -> Self {
        self.authorization_head = Some(head);
        self
    }

    /// Loads the views and the authorization of one per-subject read at
    /// `context`: the view at `context`, and the current head's view when
    /// this engine authorizes historical reads there.
    async fn authorized_read<'a>(
        &'a self,
        context: &'a RuntimeReadContext,
    ) -> Result<AuthorizedRead<'a, S>> {
        let head_view = match &self.authorization_head {
            Some(head) => Some(
                self.load_read_view(head)
                    .instrument(crate::phase_span!(crate::Phase::LoadView))
                    .await?,
            ),
            None => None,
        };
        let authorizer = Authorizer::for_request(
            &self.namespace.namespace_id,
            &context.read_basis.head.access,
            CommitAuthority::Subject(self.subject.as_ref()),
        )?;
        let view = self
            .load_read_view(context)
            .instrument(crate::phase_span!(crate::Phase::LoadView))
            .await?;
        Ok(AuthorizedRead {
            view,
            head_view,
            authorizer,
        })
    }

    /// Refuses a reader with no subject on an ACL namespace, for callers
    /// that could otherwise return before any per-subject read.
    pub fn ensure_subject(&self, context: &RuntimeReadContext) -> Result<()> {
        Authorizer::for_request(
            &self.namespace.namespace_id,
            &context.read_basis.head.access,
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
            &self.namespace.namespace_id,
            &context.read_basis.head.access,
            CommitAuthority::Subject(Some(subject)),
        )?;
        let Authorizer::Subject { principals } = authorizer else {
            return Ok(());
        };
        let view = self
            .namespace
            .load_metadata_view_for_authorization(context)
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
    /// namespace, which has no access rows.
    pub async fn root_access(&self, context: &RuntimeReadContext) -> Result<RootAccess> {
        if matches!(
            context.read_basis.head.access,
            NamespaceAccess::Unrestricted {}
        ) {
            return Err(CoreError::NamespaceUnrestricted {
                namespace_id: self.namespace.namespace_id.clone(),
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
        &self.namespace.namespace_id
    }

    /// Returns the current wall-clock time.
    pub(crate) fn now_ms(&self) -> u64 {
        self.namespace.wall_clock.now_ms()
    }

    /// Stats one path against the pinned runtime read context.
    pub async fn resolve_path(
        &self,
        path: impl AsRef<str>,
        options: StatOptions,
        context: &RuntimeReadContext,
    ) -> Result<PathEntry> {
        let read = self.authorized_read(context).await?;
        read.view
            .resolve_path(path.as_ref(), options.include_attributes, &read.access())
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
        let read = self.authorized_read(context).await?;
        read.view
            .list_path_page(path.as_ref(), request, &options, &read.access())
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
        let read = self.authorized_read(context).await?;
        read.view
            .list_inode_children_page(inode_id, request, &options, &read.access())
            .await
    }

    /// Reads file content against the pinned runtime read context.
    pub async fn read_file(
        &self,
        path: impl AsRef<str>,
        context: &RuntimeReadContext,
        max_content_bytes: Option<u64>,
    ) -> Result<FileBytes> {
        let read = self.authorized_read(context).await?;
        read.view
            .read_file_bytes(path.as_ref(), max_content_bytes, &read.access())
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
        let read = self.authorized_read(context).await?;
        read.view
            .read_file_bytes_by_inode(inode_id, max_content_bytes, &read.access())
            .await
    }

    /// Opens a chunked stream for the file resolved from the pinned read context.
    ///
    /// The object prefix is fetched `chunk_bytes` at a time, and pieces the tail
    /// contains are retained. The buffered-read size limit does not apply. Later
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
        let read = self.authorized_read(context).await?;
        let entry = read
            .view
            .resolve_file_content(path.as_ref(), revision_no, &read.access())
            .await?;
        let content_ref = crate::path::read::file_content_ref(&entry)?;
        range.ensure_within(content_ref.size_bytes)?;
        Ok(FileContentStream::open_inner(
            self.namespace.store.clone(),
            read.view.resolve_content_location(&content_ref).await?,
            Some(entry),
            content_ref,
            chunk_bytes,
            range,
        )
        .await?
        .with_object_cache(Arc::clone(&self.namespace.object_cache)))
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
        let read = self.authorized_read(context).await?;
        let content_ref = match revision_no {
            Some(revision_no) => {
                read.view
                    .authorized_revision_by_inode(inode_id, revision_no, &read.access())
                    .await?
            }
            None => {
                read.view
                    .current_revision_by_inode(inode_id, &read.access())
                    .await?
            }
        }
        .content_ref;
        range.ensure_within(content_ref.size_bytes)?;
        Ok(FileContentStream::open_inner(
            self.namespace.store.clone(),
            read.view.resolve_content_location(&content_ref).await?,
            None,
            content_ref,
            chunk_bytes,
            range,
        )
        .await?
        .with_object_cache(Arc::clone(&self.namespace.object_cache)))
    }

    /// Prepares a file's content object for a direct download.
    pub async fn direct_download_target(
        &self,
        path: impl AsRef<str>,
        revision_no: Option<RevisionNo>,
        start_offset: u64,
        context: &RuntimeReadContext,
    ) -> Result<DirectDownloadTarget> {
        let read = self.authorized_read(context).await?;
        read.view
            .direct_download_target(
                path.as_ref(),
                revision_no,
                start_offset,
                &read.access(),
                &self.namespace.content_writes,
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
        let read = self.authorized_read(context).await?;
        read.view
            .direct_download_target_by_inode(
                inode_id,
                revision_no,
                start_offset,
                &read.access(),
                &self.namespace.content_writes,
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
        let read = self.authorized_read(context).await?;
        read.view
            .stat_inode(inode_id, options.include_attributes, &read.access())
            .await
    }

    /// Lists one revision page for a path against the pinned runtime read context.
    pub async fn list_file_revisions_page(
        &self,
        path: impl AsRef<str>,
        request: PageRequest<FileRevisionsPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<crate::path::read::FileRevisionsPage> {
        let read = self.authorized_read(context).await?;
        read.view
            .list_file_revisions_page(path.as_ref(), request, &read.access())
            .await
    }

    /// Lists one trash page against the pinned runtime read context.
    pub async fn list_trash_page(
        &self,
        request: PageRequest<TrashPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<Page<TrashEntry, TrashPageCursor>> {
        let read = self.authorized_read(context).await?;
        read.view.list_trash_page(request, &read.access()).await
    }

    /// Resolves the current visibility, revision, and path for each inode ID.
    ///
    /// All results use the same pinned snapshot and preserve input order. Missing
    /// inode IDs are returned as not visible because callers may have IDs from an
    /// older listing. Requests above
    /// [`MAX_CURRENT_FILES_PER_STAT`](crate::MAX_CURRENT_FILES_PER_STAT) fail before
    /// reading metadata.
    pub async fn stat_current_files_by_inode(
        &self,
        inode_ids: &[InodeId],
        context: &RuntimeReadContext,
    ) -> Result<Vec<CurrentFileState>> {
        crate::path::read::ensure_stat_batch_within_cap(inode_ids.len())?;
        let read = self.authorized_read(context).await?;
        crate::path::read::stat_current_files_by_inode(&read.view, inode_ids, &read.access()).await
    }

    /// Resolves an import source, including an unpublished upload.
    pub async fn resolve_content_location(
        &self,
        content_ref: &ContentRef,
        context: &RuntimeReadContext,
    ) -> Result<ContentLocation> {
        let view = self
            .namespace
            .load_metadata_view_for_authorization(context)
            .await?;
        if view
            .metadata_view()
            .content_published(&content_ref.content_id)
            .await?
        {
            view.resolve_content_location(content_ref).await
        } else {
            Ok(ContentLocation::whole(content_ref))
        }
    }

    /// Reads and verifies the bytes named by a published reference.
    ///
    /// `max_bytes` is checked against the declared size before the fetch and is
    /// independent of deployment-wide download limits. The method returns an
    /// error if the fetched size or checksum does not match the reference. A
    /// published reference whose object collection reclaimed returns the same error as an
    /// unpublished one.
    pub async fn read_content_ref(
        &self,
        content_ref: &ContentRef,
        max_bytes: u64,
        context: &RuntimeReadContext,
    ) -> Result<Vec<u8>> {
        self.ensure_administrator(context).await?;
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
            .read_bytes(
                &self.namespace.store,
                Some(&self.namespace.object_cache),
                content_ref,
            )
            .await
            .map_err(|error| match error {
                DurableContentValidationError::MissingContentObject { .. } => {
                    CoreError::PathNotFound(content_ref.content_id.to_string())
                }
                error => error.into(),
            })
    }

    pub(crate) fn ensure_live_context(&self, context: &RuntimeReadContext) -> Result<()> {
        let head = &context.read_basis.head;
        crate::namespace::control::ensure_head_identity(head, &self.namespace.namespace_id)?;
        crate::namespace::control::ensure_namespace_live(&head.namespace_id, head.status)
    }

    /// Lists one revision page for an inode against the pinned runtime read context.
    pub async fn list_file_revisions_by_inode_page(
        &self,
        inode_id: InodeId,
        request: PageRequest<FileRevisionsPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<Page<FileRevision, FileRevisionsPageCursor>> {
        let read = self.authorized_read(context).await?;
        read.view
            .list_file_revisions_by_inode_page(inode_id, request, &read.access())
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
        let read = self.authorized_read(context).await?;
        read.view
            .read_file_revision_bytes(
                path.as_ref(),
                revision_no,
                max_content_bytes,
                &read.access(),
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
        let read = self.authorized_read(context).await?;
        read.view
            .read_file_revision_bytes_by_inode(
                inode_id,
                revision_no,
                max_content_bytes,
                &read.access(),
            )
            .await
    }

    /// Reads up to `limit` committed changes after `after_seq`.
    pub async fn list_changes_after(
        &self,
        after_seq: ChangeSeq,
        limit: EffectiveLimit,
        context: &RuntimeReadContext,
    ) -> Result<ListChangesResponse> {
        let view = self.load_read_view(context).await?;
        crate::changes::list_changes_after(&view, after_seq, limit).await
    }

    async fn load_read_view<'a>(
        &'a self,
        context: &'a RuntimeReadContext,
    ) -> Result<LoadedMetadataView<'a, S>> {
        self.namespace.load_metadata_view(context).await
    }
}

/// The views of one per-subject read, with the authorization that the read
/// runs under.
struct AuthorizedRead<'a, S: ObjectStore> {
    view: LoadedMetadataView<'a, S>,
    /// The current head's view, when the engine authorizes historical reads
    /// there.
    head_view: Option<LoadedMetadataView<'a, S>>,
    authorizer: Authorizer<'a>,
}

impl<S: ObjectStore> AuthorizedRead<'_, S> {
    /// Returns the read's access checks: a live read evaluates them on its
    /// own view, and a historical read evaluates them at the current head.
    fn access(&self) -> ReadAccess<'_, S> {
        match &self.head_view {
            Some(head) => ReadAccess::at_head(self.authorizer, head),
            None => ReadAccess::live(self.authorizer),
        }
    }
}

impl<S: ObjectStore> NamespaceEngine<S, ReadOnly> {
    /// Creates a read-only engine over `namespace`.
    pub fn reader(namespace: NamespaceStore<S>) -> Self {
        Self::new(namespace, ReadOnly)
    }
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates a writable engine over `namespace` that commits as `writer_id`.
    pub fn writer(namespace: NamespaceStore<S>, writer_id: WriterId) -> Self {
        Self::new(namespace, Writable { writer_id })
    }

    /// Returns the writer id used for epoch acquisition and commit publication.
    pub fn writer_id(&self) -> &WriterId {
        &self.mode.writer_id
    }

    /// Sets a writer session's position for [`Self::fold_wal`] to fold, so
    /// the fold takes the tail from the position instead of loading it from
    /// the store, and publishes on the position's manifest.
    pub fn with_writer_position(mut self, position: WriterPosition) -> Self {
        self.writer_position = Some(position);
        self
    }

    /// Narrows the rows one segment that this engine writes may contain, so a
    /// namespace a test can build in seconds compacts into many segments.
    ///
    /// The shipped segment target is 8 MiB of decoded rows, which takes over
    /// a hundred thousand rows to fill several times. Test-only: planning and
    /// publishing are unchanged.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn narrow_segment_row_budget(
        mut self,
        max_rows_per_segment: std::num::NonZeroUsize,
    ) -> Self {
        self.namespace.run_policy.max_rows_per_segment = max_rows_per_segment;
        self
    }

    /// Narrows the rows, by their runs' descriptor counts, that a window of
    /// this engine's compaction units may list when a unit admits only
    /// windows within the row budget, so a test can starve that budget with
    /// a few thousand rows. Test-only: folds and every other budget are
    /// unchanged.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn narrow_compaction_input_rows(
        mut self,
        max_decoded_input_rows_per_step: std::num::NonZeroUsize,
    ) -> Self {
        self.namespace.run_policy.max_decoded_input_rows_per_step = max_decoded_input_rows_per_step;
        self
    }

    /// Returns a commit engine for this engine's namespace: a one-shot
    /// writer session that acquires its own epoch.
    pub fn commit_engine(&self) -> NamespaceCommitEngine<S>
    where
        S: Clone,
    {
        NamespaceCommitEngine::new(self.namespace.clone(), self.mode.writer_id.clone())
    }

    /// Verifies that a runtime-supplied catalog belongs to this engine's
    /// namespace. A mismatch indicates an internal integration error.
    pub(crate) fn own_catalog<'c>(
        &self,
        catalog: &'c VerifiedNamespaceCatalogEntry,
    ) -> &'c VerifiedNamespaceCatalogEntry {
        assert_eq!(
            catalog.namespace_id(),
            &self.namespace.namespace_id,
            "a runtime should give an engine its own namespace's catalog"
        );
        catalog
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{namespace_store, writer_at, NamespaceFixture};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    #[tokio::test]
    async fn reader_engine_still_serves_reads() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        NamespaceFixture::new(&store, &namespace_id, &writer_at("writer-a", 1_000))
            .create()
            .await
            .expect("bootstrap namespace");

        let reader = NamespaceEngine::reader(namespace_store(&store, &namespace_id));
        let loaded = reader
            .namespace
            .load_read_anchor(crate::test_support::observed_now())
            .await
            .expect("read basis");
        let context = RuntimeReadContext {
            read_basis: crate::control::ReadBasis {
                basis: loaded.basis(),
                head: loaded.read_state,
            },
            tail: None,
        };
        let changes = reader
            .list_changes_after(
                ChangeSeq(0),
                loonfs_types::EffectiveLimit::DEFAULT,
                &context,
            )
            .await
            .expect("a reader-built engine serves reads");
        assert_eq!(changes.namespace_id, namespace_id);
    }

    #[tokio::test]
    async fn reader_engine_reads_upload_status_without_writer_identity() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = NamespaceFixture::new(&store, &namespace_id, &writer_at("writer-a", 1_000));
        writer.create().await.expect("bootstrap namespace");
        let begun = writer.engine().begin_upload().await.expect("begin upload");

        let reader = NamespaceEngine::reader(namespace_store(&store, &namespace_id));
        let crate::UploadSessionView {
            session: status,
            evidence,
            ..
        } = reader
            .get_upload(&begun.upload_id)
            .await
            .expect("reader engine reads upload status");

        assert_eq!(status.upload_id, begun.upload_id);
        assert!(evidence.is_none());
    }
}
