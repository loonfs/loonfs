//! [`NamespaceEngine`]: the namespace-scoped entry point for reads, writes,
//! uploads, snapshots, and maintenance. Defines the type, its constructors,
//! authorization, and per-subject reads. Each other operation is implemented
//! as a method in its own module.

use crate::authorize::{Authorizer, ReadAccess};
use crate::commit::engine::NamespaceCommitEngine;
use crate::diff::DiffPage;
use crate::error::{CoreError, Result};
use crate::metadata::access::is_administrator;
use crate::namespace::basis::{ReadBasis, SnapshotReadBasis};
use crate::namespace::boundary::KeptBoundary;
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::namespace::held::Held;
use crate::namespace::store::NamespaceStore;
use crate::path::read::{
    CurrentFileState, DirectDownloadByInodeTarget, DirectDownloadTarget, FileBytes,
    LoadedMetadataView, RetainedRevisionsPage, RetainedRevisionsPageCursor,
};
use crate::segment::KeptBy;
use crate::storage::content::DurableContentValidationError;
use crate::storage::content_stream::{FileContentStream, StreamRange};
use crate::wal::ProjectedWalTail;
use loonfs_objectstore::ObjectStore;
use loonfs_types::options::{CommitSplitting, DiffOptions, ListOptions, StatOptions};
use loonfs_types::ListChangesResponse;
use loonfs_types::{ChangeFeedPosition, EffectiveLimit};
use loonfs_types::{
    CommitId, CommitSeq, ContentRef, FileRevision, FileRevisionsPageCursor, InodeId,
    ListPageCursor, NamespaceId, Page, PageRequest, PathEntry, RevisionNo, Subject, TrashEntry,
    TrashPageCursor, WriterId, ROOT_INODE_ID,
};
use std::num::NonZeroU64;
use std::sync::Arc;
use tracing::Instrument;

/// The head and metadata basis that the runtime pins for one request, so
/// every read of the request sees the same namespace state.
///
/// A context reads at its head or at a sequence below it. Code outside this
/// crate builds a context only through its constructors, and
/// [`Self::at_seq`] refuses a sequence above the head. A context of the
/// namespace's live head is built from a [`Held<ReadBasis>`], which only this
/// crate's loaders return (a cached head, a writer position, a read anchor,
/// or an initializing namespace's basis), so no caller can build a live
/// context from the basis of an earlier state. [`Self::new`] refuses a head
/// that isn't active, so every context reads a live namespace, and the reads
/// that take one don't check the namespace's status again.
///
/// A read of an earlier sequence or of a snapshot checks the subject's
/// rights at the namespace's head when the read started
/// ([API spec, Snapshots](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#snapshots)),
/// so each context also records the head at which its reads are authorized.
/// A context from [`Self::new`] is authorized at its own head, a context
/// from [`Self::at_seq`] at the same head as the context it was made from,
/// and a context from [`Self::at_snapshot`] at the same head as the live
/// context it was built with.
///
/// This type supports the `loonfs` runtime. Applications should use the
/// higher-level `loonfs` runtime and namespace handles instead.
#[derive(Debug, Clone)]
pub struct RuntimeReadContext {
    pub(crate) read_basis: ReadBasis,
    /// The projected WAL tail above the basis at the head, when the context
    /// starts from a writer session's position. Reads then take this tail
    /// and don't look one up in the namespace's WAL tail cache.
    pub(crate) tail: Option<Arc<ProjectedWalTail>>,
    /// The sequence that reads see: a retained sequence below the head, or
    /// `None` to read at the head.
    pub(crate) visible_seq: Option<CommitSeq>,
    /// The context at whose head this context's reads are authorized, or
    /// `None` when they're authorized at this context's own head. A context
    /// from [`Self::new`] has `None` here, and it reads at its head.
    /// Clones of a context, and the contexts made from it, share one copy
    /// through the `Arc`.
    authorization: Option<Arc<RuntimeReadContext>>,
    /// The manifest boundary that a live context's holder (a cached head, a
    /// writer position, or a read anchor) kept when the context was built
    /// from it. A read that checks its manifest against the boundary reads
    /// the boundary object conditionally on this one. A context from
    /// [`Self::at_seq`] or [`Self::at_snapshot`] keeps the boundary of the
    /// live context it was made from, because the boundary belongs to the
    /// namespace and not to one manifest.
    boundary: KeptBoundary,
    /// What keeps the segments of the manifest that the context reads
    /// above: the snapshot for a context from [`Self::at_snapshot`], and
    /// otherwise the namespace's current manifest.
    pub(crate) kept_by: KeptBy,
}

impl RuntimeReadContext {
    /// Returns a context that reads at the head of `basis`, which a loader
    /// returned, with its reads authorized at that same head. `tail` is the
    /// projected WAL tail of a writer session's position at that head, or
    /// `None` for reads that look the tail up in the namespace's WAL tail
    /// cache.
    ///
    /// Returns `namespace_not_found` when the head is initializing and
    /// `namespace_deleted` when it's deleted.
    ///
    /// The basis of another context, such as a snapshot's, isn't a held
    /// basis, so it can't be used to build a live context:
    ///
    /// ```compile_fail
    /// use loonfs_core::RuntimeReadContext;
    ///
    /// fn as_live(snapshot: &RuntimeReadContext) -> RuntimeReadContext {
    ///     RuntimeReadContext::new(snapshot.read_basis(), None).unwrap()
    /// }
    /// ```
    pub fn new(basis: &Held<ReadBasis>, tail: Option<Arc<ProjectedWalTail>>) -> Result<Self> {
        let head = basis.read_basis().head();
        crate::namespace::control::ensure_namespace_live(&head.namespace_id, head.status)?;
        Ok(Self::of_any_status(basis, tail))
    }

    /// Returns a context as [`Self::new`] does, whatever the status of the
    /// head. Only the administrator check of a deletion reads this way.
    fn of_any_status(basis: &Held<ReadBasis>, tail: Option<Arc<ProjectedWalTail>>) -> Self {
        Self {
            read_basis: basis.read_basis().clone(),
            tail,
            visible_seq: None,
            authorization: None,
            boundary: basis.boundary().clone(),
            kept_by: KeptBy::Current,
        }
    }

    /// Returns a context of the same head that reads at `seq`, with its reads
    /// authorized at the same head as this context's reads, or `None` when
    /// `seq` is above the head.
    pub fn at_seq(&self, seq: CommitSeq) -> Option<Self> {
        (seq <= self.read_basis.head.seq).then(|| Self {
            visible_seq: Some(seq),
            authorization: Some(self.authorizing_context()),
            ..self.clone()
        })
    }

    /// Returns a context that reads the state that a snapshot kept, from
    /// `basis`, with its reads authorized at the same head as the reads of
    /// `live`, the context of the namespace's head when the read started.
    pub fn at_snapshot(basis: SnapshotReadBasis, live: RuntimeReadContext) -> Self {
        let boundary = live.boundary.clone();
        let authorization = match live.authorization {
            Some(authorization) => authorization,
            None => Arc::new(live),
        };
        Self {
            read_basis: basis.basis,
            tail: None,
            visible_seq: None,
            authorization: Some(authorization),
            boundary,
            kept_by: KeptBy::Snapshot(basis.snapshot_id),
        }
    }

    /// Returns the context at whose head this context's reads are
    /// authorized, to share with a context made from this one.
    fn authorizing_context(&self) -> Arc<RuntimeReadContext> {
        match &self.authorization {
            Some(authorization) => Arc::clone(authorization),
            None => Arc::new(self.clone()),
        }
    }

    /// Returns the context at whose head this context's reads are
    /// authorized, or `None` when that is this context's own head.
    fn separate_authorization(&self) -> Option<&RuntimeReadContext> {
        self.authorization.as_deref()
    }

    /// Returns the head and metadata basis that this context reads.
    pub fn read_basis(&self) -> &ReadBasis {
        &self.read_basis
    }

    /// Returns the manifest boundary that the holder of this context's live
    /// head (a cached head, a writer position, or a read anchor) kept when
    /// the live context was built from it.
    pub fn boundary(&self) -> &KeptBoundary {
        &self.boundary
    }

    /// Returns the sequence that reads of this context see.
    pub fn visible_seq(&self) -> CommitSeq {
        self.visible_seq.unwrap_or(self.read_basis.head.seq)
    }
}

/// Proof that an engine's subject is the administrator of one namespace, or
/// that the engine has no subject, as in the service context.
///
/// The proof records the namespace it was checked for and not the subject,
/// because a function that receives it, such as the commit engine's
/// deletion, may have no subject to compare with.
///
/// Only [`NamespaceEngine::verify_administrator`] and
/// [`NamespaceEngine::find_administrator_without_read`] return one. A
/// function that is only for the administrator takes the proof and asserts
/// that it was made for the function's own namespace.
///
/// A function that is only for the administrator and takes a read context
/// makes the check itself, at that context. It takes this proof instead
/// when it has no read context (as a snapshot's creation doesn't), or when
/// the check has to run before a step that the caller takes outside the
/// function. For example, the caller of the next merge range checks this
/// namespace before it reads the fork's head.
#[derive(Debug)]
pub struct Administrator {
    namespace_id: NamespaceId,
}

impl Administrator {
    /// Asserts that this proof was made for `namespace_id`. A mismatch is a
    /// bug in the caller, not an error of the request.
    pub(crate) fn assert_for(&self, namespace_id: &NamespaceId) {
        assert_eq!(
            &self.namespace_id, namespace_id,
            "an administrator proof is used for the namespace it was checked for"
        );
    }
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

/// Who reads through an engine, and whose rights the engine's per-subject
/// reads check.
///
/// The subject and its administrator reads are one value, so replacing the
/// subject also removes administrator reads granted to an earlier subject.
/// Administrator reads come only from
/// [`NamespaceEngine::verify_administrator_reads`], which checks the reader
/// they are granted to.
#[derive(Debug)]
enum Reader {
    /// No subject, as in the service context. Reads check no rights.
    Service,
    /// A subject whose rights each read checks, at the head at which the
    /// read's context is authorized.
    Subject(Subject),
    /// A subject that a check showed administers the namespace. Reads check
    /// no rights, and the subject stays the identity of the engine's
    /// commits.
    Administrator(Subject),
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
    reader: Reader,
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    fn new(namespace: NamespaceStore<S>, mode: M) -> Self {
        Self {
            namespace,
            mode,
            reader: Reader::Service,
        }
    }

    /// Searches the supplied WAL and metadata view without acquiring a writer epoch.
    pub async fn find_commit_receipt(
        &self,
        context: &RuntimeReadContext,
        commit_id: &CommitId,
    ) -> Result<Option<crate::metadata::CommitReceiptRecord>> {
        self.namespace
            .load_metadata_view(context)
            .await?
            .metadata_view()
            .find_commit_receipt(commit_id)
            .await
    }

    /// Sets the subject whose rights this engine's reads check. The subject
    /// replaces the engine's earlier reader, so the engine no longer has
    /// administrator reads that a check granted to an earlier subject.
    pub fn with_subject(mut self, subject: Subject) -> Self {
        self.reader = Reader::Subject(subject);
        self
    }

    /// Returns the subject of this engine's reader, the identity of its
    /// reads and commits, or `None` in the service context.
    pub(crate) fn subject(&self) -> Option<&Subject> {
        match &self.reader {
            Reader::Service => None,
            Reader::Subject(subject) | Reader::Administrator(subject) => Some(subject),
        }
    }

    /// Loads the views and the authorization of one per-subject read at
    /// `context`: the view at `context` and, when this engine checks a
    /// subject's rights and `context` reads an earlier sequence or a
    /// snapshot, the view of the head at which its reads are authorized.
    async fn authorized_read<'a>(
        &'a self,
        context: &'a RuntimeReadContext,
    ) -> Result<AuthorizedRead<'a, S>> {
        let Some(subject) = self.subject_to_check() else {
            return Ok(AuthorizedRead {
                view: self
                    .namespace
                    .load_metadata_view(context)
                    .instrument(crate::phase_span!(crate::Phase::LoadView))
                    .await?,
                head_view: None,
                authorizer: Authorizer::Service,
            });
        };
        let head_view = match context.separate_authorization() {
            Some(head) => Some(
                self.namespace
                    .load_metadata_view(head)
                    .instrument(crate::phase_span!(crate::Phase::LoadView))
                    .await?,
            ),
            None => None,
        };
        let authorizer = Authorizer::for_request(Some(subject));
        let view = self
            .namespace
            .load_metadata_view(context)
            .instrument(crate::phase_span!(crate::Phase::LoadView))
            .await?;
        Ok(AuthorizedRead {
            view,
            head_view,
            authorizer,
        })
    }

    /// Checks that the sequence `context` reads at is at or above the
    /// retention floor of its metadata, and returns `below_retention_floor`
    /// otherwise.
    pub async fn ensure_retained(&self, context: &RuntimeReadContext) -> Result<()> {
        self.namespace.load_metadata_view(context).await.map(drop)
    }

    /// Returns the subject whose rights this engine checks, or `None` when
    /// it checks no rights: an engine with no subject, as in the service
    /// context, or one whose reads a check granted administrator rights.
    /// The administrator check and every per-subject read check rights
    /// exactly when this returns a subject.
    fn subject_to_check(&self) -> Option<&Subject> {
        match &self.reader {
            Reader::Subject(subject) => Some(subject),
            Reader::Service | Reader::Administrator(_) => None,
        }
    }

    fn administrator(&self) -> Administrator {
        Administrator {
            namespace_id: self.namespace.namespace_id.clone(),
        }
    }

    /// Returns the administrator proof when this engine needs no read to
    /// make it: when it has no subject, or its reads were granted
    /// administrator rights. Returns `None` when the proof needs a check at
    /// a read context with [`Self::verify_administrator`].
    pub fn find_administrator_without_read(&self) -> Option<Administrator> {
        self.subject_to_check()
            .is_none()
            .then(|| self.administrator())
    }

    /// Checks that this engine's reader administers the namespace, and
    /// returns the proof. The service context and an administrator pass,
    /// and any other subject is refused with `forbidden`.
    ///
    /// The check reads the access row of `/` at the head at which the reads
    /// of `context` are authorized, never at the earlier sequence or the
    /// snapshot that `context` reads, because reading an earlier state doesn't restore the
    /// administrator grants that the namespace had then. It makes no read
    /// when [`Self::find_administrator_without_read`] returns the proof.
    pub async fn verify_administrator(
        &self,
        context: &RuntimeReadContext,
    ) -> Result<Administrator> {
        let Some(subject) = self.subject_to_check() else {
            return Ok(self.administrator());
        };
        let context = context.separate_authorization().unwrap_or(context);
        let view = self.namespace.load_metadata_view(context).await?;
        if is_administrator(&mut view.metadata_view().session(), &subject.principals).await? {
            Ok(self.administrator())
        } else {
            Err(CoreError::Forbidden {
                inode_id: ROOT_INODE_ID,
            })
        }
    }

    /// Checks, as [`Self::verify_administrator`] does, that this engine's
    /// reader administers the namespace at the head of `basis`, whatever the
    /// head's status. A deletion checks an initializing namespace this way,
    /// because a read context can't be built from a head that isn't active.
    pub async fn verify_deletion_administrator(
        &self,
        basis: &Held<ReadBasis>,
    ) -> Result<Administrator> {
        self.verify_administrator(&RuntimeReadContext::of_any_status(basis, None))
            .await
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
            .list_path_page(path.as_ref(), request, &options, &mut read.access())
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
            .list_inode_children_page(inode_id, request, &options, &mut read.access())
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
        Ok(FileContentStream::open(
            self.namespace.store.clone(),
            read.view.resolve_content_location(&content_ref).await?,
            Some(entry),
            content_ref,
            chunk_bytes,
            range,
            Arc::clone(&self.namespace.object_cache),
        )
        .await?)
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
        Ok(FileContentStream::open(
            self.namespace.store.clone(),
            read.view.resolve_content_location(&content_ref).await?,
            None,
            content_ref,
            chunk_bytes,
            range,
            Arc::clone(&self.namespace.object_cache),
        )
        .await?)
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
        read.view.list_trash_page(request, &mut read.access()).await
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
        self.verify_administrator(context).await?;
        let view = self.namespace.load_metadata_view(context).await?;
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

    /// Lists one page of every file revision that the pinned runtime read
    /// context retains, ordered by inode and newest first within each inode.
    /// The listing includes the revisions of deleted files, and it requires
    /// namespace administrator access, as the change feed does.
    pub async fn retained_revisions_page(
        &self,
        request: PageRequest<RetainedRevisionsPageCursor>,
        context: &RuntimeReadContext,
    ) -> Result<RetainedRevisionsPage> {
        self.verify_administrator(context).await?;
        let view = self.namespace.load_metadata_view(context).await?;
        view.retained_revisions_page(request).await
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

    /// Reads up to `limit` committed changes from `position`, and with
    /// [`CommitSplitting::AtPageBudget`], ends the page inside a commit that
    /// alone exceeds the page's budget.
    ///
    /// The change feed is only for the namespace's administrator, and
    /// [`Self::verify_administrator`] checks that at `context` before the
    /// feed is read.
    pub async fn list_changes_after(
        &self,
        position: ChangeFeedPosition,
        limit: EffectiveLimit,
        splitting: CommitSplitting,
        context: &RuntimeReadContext,
    ) -> Result<ListChangesResponse> {
        self.verify_administrator(context).await?;
        let view = self.namespace.load_metadata_view(context).await?;
        crate::changes::list_changes_after(&view, position, limit, splitting).await
    }

    /// Reads one page of the diff from `from_seq` to `options.to_seq` under
    /// `options.path`, starting after `options.after_seq`
    /// ([API section 6.12](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#612-get-diff)).
    ///
    /// `to_seq` defaults to the head of `context`, and `after_seq` to
    /// `from_seq`. The page reads at most `options.limit` commits. The diff
    /// requires namespace administrator access, as the change feed does.
    pub async fn diff_page(
        &self,
        from_seq: CommitSeq,
        options: &DiffOptions,
        context: &RuntimeReadContext,
    ) -> Result<DiffPage> {
        self.verify_administrator(context).await?;
        let view = self.namespace.load_metadata_view(context).await?;
        crate::diff::diff_page(&view, from_seq, options).await
    }

    /// Asserts that a runtime-supplied catalog belongs to this engine's
    /// namespace. A mismatch is a bug in the runtime.
    pub(crate) fn assert_own_catalog(&self, catalog: &VerifiedNamespaceCatalogEntry) {
        assert_eq!(
            catalog.namespace_id(),
            &self.namespace.namespace_id,
            "a runtime should give an engine its own namespace's catalog"
        );
    }
}

/// The views of one per-subject read, with the authorization that the read
/// runs under.
struct AuthorizedRead<'a, S: ObjectStore> {
    view: LoadedMetadataView<'a, S>,
    /// The view of the head at which the read is authorized, when the read's
    /// context reads an earlier sequence or a snapshot and the engine checks
    /// a subject's rights.
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

impl<S: ObjectStore + Clone> NamespaceEngine<S, ReadOnly> {
    /// Checks that this engine's reader administers the namespace at
    /// `context`, as [`Self::verify_administrator`] does, and returns a
    /// reader engine for the same reader whose per-subject reads check no
    /// rights, as reads in the service context do. A subject that isn't the
    /// administrator is refused with `forbidden`.
    ///
    /// The grant applies to the reader that the check checked, and a later
    /// [`Self::with_subject`] removes it.
    pub async fn verify_administrator_reads(&self, context: &RuntimeReadContext) -> Result<Self> {
        self.verify_administrator(context).await?;
        Ok(Self {
            namespace: self.namespace.clone(),
            mode: ReadOnly,
            reader: match &self.reader {
                Reader::Service => Reader::Service,
                Reader::Subject(subject) | Reader::Administrator(subject) => {
                    Reader::Administrator(subject.clone())
                }
            },
        })
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

    /// Returns a commit engine for this engine's namespace: a one-shot
    /// writer session that acquires its own epoch.
    pub fn commit_engine(&self) -> NamespaceCommitEngine<S>
    where
        S: Clone,
    {
        NamespaceCommitEngine::new(self.namespace.clone(), self.mode.writer_id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{namespace_store, writer_at, NamespaceFixture};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use tempfile::tempdir;

    #[tokio::test]
    async fn reader_engine_reads_upload_status_without_writer_identity() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let writer = NamespaceFixture::new(&store, &namespace_id, &writer_at("writer-a", 1_000));
        writer.create().await.expect("bootstrap namespace");
        let catalog = crate::test_support::load_catalog(&store, &namespace_id).await;
        let begun = writer
            .engine()
            .begin_upload(&catalog, &loonfs_test_support::test_actor())
            .await
            .expect("begin upload");

        let reader = NamespaceEngine::reader(namespace_store(&store, &namespace_id));
        let crate::UploadSessionView {
            session: status,
            evidence,
            ..
        } = reader
            .get_upload(
                &catalog,
                &begun.upload_id,
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("reader engine reads upload status");

        assert_eq!(status.upload_id, begun.upload_id);
        assert!(evidence.is_none());
    }
}
