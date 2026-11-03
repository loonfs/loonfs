//! Filesystem reads used by grep indexing and query verification.
//!
//! Grep reads filesystem state and its entry in the namespace manifest
//! through a read-only [`Namespace`] handle, and a fork's source through the
//! read-only runtime, and reads its segments directly from the store.

use crate::entry::{grep_extension_name, GrepEntry};
use crate::{GrepError, Result};
use loonfs::{
    CheckpointFilesPager, Consistency, CoreError, CurrentFileState, ListChangesOptions,
    ListCheckpointFilesOptions, LoonFs, Namespace, NamespaceMetadata, ReadAt, ReadOnly, ReadView,
    RetainedRevisionsPage, RetainedRevisionsPageCursor, StatOptions, MAX_CURRENT_FILES_PER_STAT,
};
use loonfs_types::{
    AbsolutePath, ChangeSeq, ContentRef, EffectiveLimit, InodeId, LimitError, NamespaceId, Page,
    PathEntry, PinId, RevisionNo, Subject, MAX_PAGE_LIMIT,
};
use loonfs_types::{FilesystemChange, ListChangesResponse};

/// Filesystem reads for one namespace.
///
/// Each call reads one consistent state, but consecutive calls may observe
/// different heads. Query execution takes one read view so every metadata
/// phase in one response uses the same head.
pub struct NamespaceReads {
    /// The runtime that a fork's source is read through.
    runtime: LoonFs<ReadOnly>,
    namespace: Namespace<ReadOnly>,
    subject_namespace: Option<Namespace<ReadOnly>>,
}

impl NamespaceReads {
    /// Reads `namespace_id`, and a fork's source, through `runtime`, a
    /// read-only runtime for the service. Performs no I/O.
    pub fn new(runtime: LoonFs<ReadOnly>, namespace_id: &NamespaceId) -> Self {
        Self {
            namespace: runtime.namespace(namespace_id),
            runtime,
            subject_namespace: None,
        }
    }

    /// Verifies candidates and reads their content as `subject`. The index
    /// and the change feed still read as the service.
    pub fn with_subject(mut self, subject: Subject) -> Self {
        self.subject_namespace = Some(self.namespace.with_subject(subject));
        self
    }

    /// Returns the namespace these reads act on.
    pub fn namespace_id(&self) -> &NamespaceId {
        self.namespace.id()
    }

    /// Captures one read view for a query.
    pub(crate) async fn read_view(&self) -> Result<NamespaceReadView<'_>> {
        let namespace = self.subject_namespace.as_ref().unwrap_or(&self.namespace);
        let view = namespace.read_view().await?;
        view.ensure_subject()?;
        Ok(NamespaceReadView { reads: self, view })
    }

    /// Captures a read view of the head as the service, without the
    /// subject, for the reads that the change feed's rule allows the service:
    /// the index's own reads of the namespace's revisions.
    pub(crate) async fn service_view(&self) -> Result<NamespaceReadView<'_>> {
        Ok(NamespaceReadView {
            reads: self,
            view: self.namespace.read_view().await?,
        })
    }

    /// Returns a view of the namespace at `seq` whose reads check no
    /// subject's rights, which a later page decides the files of its cut
    /// through. It reads as the service, so a subject's rights never decide
    /// what the cut contains.
    pub(crate) async fn membership_view(&self, seq: ChangeSeq) -> Result<NamespaceReadView<'_>> {
        let view = self
            .namespace
            .read_view()
            .await?
            .at_seq(seq)
            .await?
            .as_administrator()
            .await?;
        Ok(NamespaceReadView { reads: self, view })
    }

    pub async fn head(&self) -> Result<NamespaceMetadata> {
        Ok(self.namespace.metadata().await?)
    }

    /// Lists the files a snapshot pins, in ascending inode-id order.
    /// Deleted files are included so an undelete needs no new postings;
    /// queries determine visibility.
    ///
    /// A page returns `snapshot_not_found` if the snapshot was deleted,
    /// expired, or collected, and `checkpoint_unavailable` if its manifest is
    /// gone. The caller must then restart the backfill from a new snapshot.
    pub fn list_checkpoint_files(&self, checkpoint_id: &PinId) -> CheckpointFilesPager {
        self.namespace.list_checkpoint_files_with_options(
            checkpoint_id,
            &ListCheckpointFilesOptions {
                include_deleted: true,
            },
        )
    }

    /// Reads committed changes after `after_seq` as semantic events.
    ///
    /// In a fork, when `after_seq` is below the fork point, this reads the
    /// source's changes through the fork pin (see
    /// [`Self::list_fork_gap_after`]). Otherwise, it returns
    /// `rebootstrap_required` when `after_seq` is below the retention floor.
    /// The page is `strong`, because `after_seq` comes
    /// from the index, which another process may have built past this
    /// process's last confirmation of the head.
    pub async fn list_changes_after(
        &self,
        after_seq: ChangeSeq,
        limit: usize,
    ) -> Result<ListChangesResponse> {
        let options = ListChangesOptions {
            at: ReadAt::Live(Some(Consistency::Strong)),
            ..ListChangesOptions::default()
        };
        match self
            .namespace
            .list_changes_with_options(after_seq, &options)
            .page_limit(page_limit(limit))
            .page()
            .await
        {
            Ok(page) => Ok(page),
            Err(error) => self.list_fork_gap_after(after_seq, limit, error).await,
        }
    }

    /// Reads one immutable content object by reference, under grep's own
    /// byte budget rather than any deployment download limit.
    pub async fn read_content(&self, content_ref: &ContentRef, max_bytes: u64) -> Result<Vec<u8>> {
        Ok(self.namespace.read_content(content_ref, max_bytes).await?)
    }

    /// Lists one page of a fork's "gap" after `after_seq`, which is the
    /// source's commits above `after_seq` and at or below the fork point,
    /// read through the fork pin as a rebase reads its source.
    ///
    /// A fork's own feed starts at the fork point, which is its first
    /// retention floor, so grep reads the commits below the fork point that
    /// the fork's index doesn't cover from the source.
    ///
    /// The gap's last page ends at the fork point. When the fork has commits
    /// of its own, that page's `next_after_seq` is the fork point, so a
    /// caller that follows the pages continues in the fork's own feed.
    ///
    /// The source is read through the runtime without a subject, as the
    /// fork's own feed is read, and a query checks the subject's rights on
    /// each matching file in the fork, as it does for any file of the tail.
    ///
    /// `error` is what the fork's own feed returned for `after_seq`. The gap
    /// is read only when that's `rebootstrap_required` and `after_seq` is
    /// below the fork point of a fork, and `error` is returned otherwise.
    /// `error` is also returned when the source can't serve the gap, such as
    /// when the source's retention floor was above `after_seq` when the fork
    /// was made, so the index is rebuilt as it is after any rebootstrap.
    async fn list_fork_gap_after(
        &self,
        after_seq: ChangeSeq,
        limit: usize,
        error: loonfs::Error,
    ) -> Result<ListChangesResponse> {
        if !matches!(
            error,
            loonfs::Error::Core(CoreError::RebootstrapRequired { .. })
        ) {
            return Err(error.into());
        }
        let metadata = self.namespace.metadata().await?;
        let Some(basis) = metadata
            .fork_basis
            .filter(|basis| after_seq < basis.source_head_seq)
        else {
            return Err(error.into());
        };
        let options = ListChangesOptions {
            at: ReadAt::Snapshot(basis.source_pin_id),
            ..ListChangesOptions::default()
        };
        match self
            .runtime
            .namespace(&basis.source_namespace_id)
            .list_changes_with_options(after_seq, &options)
            .page_limit(page_limit(limit))
            .page()
            .await
        {
            Ok(mut page) => {
                if page.next_after_seq.is_none() && metadata.head_seq > basis.source_head_seq {
                    page.next_after_seq = Some(basis.source_head_seq);
                }
                Ok(page)
            }
            Err(loonfs::Error::Core(
                CoreError::RebootstrapRequired { .. }
                | CoreError::SnapshotNotFound { .. }
                | CoreError::CheckpointUnavailable(_)
                | CoreError::NamespaceDeleted { .. }
                | CoreError::NamespaceNotFound { .. },
            )) => Err(error.into()),
            Err(other) => Err(other.into()),
        }
    }
}

/// Filesystem reads pinned to one namespace head for a single grep query.
pub(crate) struct NamespaceReadView<'a> {
    /// The reads that the view came from, whose change feed reads have no
    /// subject.
    reads: &'a NamespaceReads,
    view: ReadView,
}

impl NamespaceReadView<'_> {
    /// Returns the namespace used by this reader.
    pub(crate) fn namespace_id(&self) -> &NamespaceId {
        self.view.namespace_id()
    }

    /// Returns the sequence that every metadata read sees: the head the view
    /// captured, or the earlier sequence of a view from [`Self::at_seq`].
    pub(crate) fn head_seq(&self) -> ChangeSeq {
        self.view.head_seq()
    }

    /// Returns grep's entry in the manifest that this view's reads start
    /// from, or `None` when grep was never enabled. The index is as current as that
    /// manifest.
    pub(crate) async fn grep_entry(&self) -> Result<Option<GrepEntry>> {
        let Some(entry) = self
            .view
            .find_extension_entry(&grep_extension_name())
            .await?
        else {
            return Ok(None);
        };
        GrepEntry::decode(&entry)
            .map(Some)
            .map_err(|error| GrepError::IndexCorrupt {
                message: format!(
                    "the grep entry of namespace `{}` is invalid: {error}",
                    self.namespace_id()
                ),
            })
    }

    /// Returns a view of this view's head at `seq`, which checks the
    /// subject's rights at the head, or `rebootstrap_required` when the
    /// retention floor is above `seq`.
    pub(crate) async fn at_seq(&self, seq: ChangeSeq) -> Result<Self> {
        Ok(Self {
            reads: self.reads,
            view: self.view.at_seq(seq).await?,
        })
    }

    /// Reads committed changes after `after_seq`, capped at the view's head.
    ///
    /// The feed itself may observe a later durable head. Its immutable commit
    /// prefix is truncated here so later commits cannot affect this query. In
    /// a fork, when `after_seq` is below the fork point, this reads the
    /// source's changes through the fork pin (see
    /// [`NamespaceReads::list_fork_gap_after`]), and the page after the gap's
    /// last commit continues in the fork's own feed.
    pub(crate) async fn list_changes_after(
        &self,
        after_seq: ChangeSeq,
        limit: usize,
    ) -> Result<ListChangesResponse> {
        let head_seq = self.head_seq();
        if after_seq > head_seq {
            return Err(CoreError::InvalidCursor(format!(
                "change feed sequence `{after_seq}` is ahead of the read view head `{head_seq}`"
            ))
            .into());
        }
        if after_seq == head_seq {
            return Ok(ListChangesResponse {
                namespace_id: self.namespace_id().clone(),
                after_seq,
                head_seq,
                next_after_seq: None,
                next_event_index: None,
                changes: Vec::new(),
            });
        }
        let mut page = match self
            .reads
            .namespace
            .list_changes(after_seq)
            .page_limit(page_limit(limit))
            .page()
            .await
        {
            Ok(page) => page,
            Err(error) => {
                self.reads
                    .list_fork_gap_after(after_seq, limit, error)
                    .await?
            }
        };
        page.changes
            .retain(|change| change.committed_seq <= head_seq);
        page.head_seq = head_seq;
        page.next_after_seq = page
            .changes
            .last()
            .map(|change| change.committed_seq)
            .filter(|last_seq| *last_seq < head_seq);
        Ok(page)
    }

    /// Lists one page of every file revision that the view's metadata
    /// retains, ordered by inode and newest first within each inode.
    pub(crate) async fn retained_revisions_page(
        &self,
        cursor: Option<RetainedRevisionsPageCursor>,
    ) -> Result<RetainedRevisionsPage> {
        Ok(self
            .view
            .list_retained_revisions()
            .page_limit(page_limit(MAX_PAGE_LIMIT as usize))
            .cursor(cursor)
            .page()
            .await?)
    }

    /// Resolves visibility, revision, and path in input order.
    pub(crate) async fn stat_current_files_by_inode(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        Ok(self.view.stat_current_files_by_inode(inode_ids).await?)
    }

    /// Resolves one path against the read view.
    pub(crate) async fn resolve_path(&self, absolute_path: &AbsolutePath) -> Result<PathEntry> {
        Ok(self
            .view
            .stat_with_options(
                absolute_path.as_str(),
                &StatOptions {
                    include_attributes: loonfs_types::options::AttributeInclusion::Omit,
                    ..Default::default()
                },
            )
            .await?)
    }

    /// Resolves one visible inode, and its current path, against the read view.
    pub(crate) async fn resolve_inode(&self, inode_id: InodeId) -> Result<PathEntry> {
        Ok(self
            .view
            .stat_by_inode_with_options(
                inode_id,
                &StatOptions {
                    include_attributes: loonfs_types::options::AttributeInclusion::Omit,
                    ..Default::default()
                },
            )
            .await?)
    }

    /// Lists one page of a directory inode's children against the read view.
    pub(crate) async fn list_inode_children_page(
        &self,
        inode_id: InodeId,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<Page<PathEntry, String>> {
        let page = self
            .view
            .list_by_inode(inode_id)
            .page_limit(page_limit(limit))
            .cursor(cursor)
            .page()
            .await?;
        Ok(Page {
            items: page.entries,
            next_cursor: page.next_cursor,
        })
    }

    /// Reads an authorized inode revision from the read view.
    pub(crate) async fn read_file_revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        Ok(self
            .view
            .read_file_revision_by_inode(inode_id, revision_no, max_bytes)
            .await?)
    }
}

pub(crate) fn effective_batch_size(wanted: usize) -> usize {
    wanted.clamp(1, MAX_CURRENT_FILES_PER_STAT)
}

/// The revision one change event published.
pub(crate) struct PublishedRevision<'a> {
    pub(crate) inode_id: InodeId,
    pub(crate) revision_no: RevisionNo,
    pub(crate) content_ref: &'a ContentRef,
}

/// Returns the file revision published by a change event.
///
/// Events that do not change file content need no index update. The index is
/// keyed by `(inode_id, revision_no)`, and queries verify each candidate
/// against current state before returning it.
pub(crate) fn published_revision(event: &FilesystemChange) -> Option<PublishedRevision<'_>> {
    match event {
        FilesystemChange::FileCreated {
            inode_id,
            revision_no,
            content_ref,
            ..
        } => Some(PublishedRevision {
            inode_id: *inode_id,
            revision_no: *revision_no,
            content_ref,
        }),
        FilesystemChange::ContentChanged {
            inode_id,
            revision_no,
            content_ref,
            ..
        } => Some(PublishedRevision {
            inode_id: *inode_id,
            revision_no: *revision_no,
            content_ref,
        }),
        // A created directory publishes no content.
        FilesystemChange::DirectoryCreated { .. }
        | FilesystemChange::Moved { .. }
        | FilesystemChange::Deleted { .. }
        | FilesystemChange::Undeleted { .. }
        | FilesystemChange::AttributesChanged { .. }
        | FilesystemChange::AccessChanged { .. }
        | FilesystemChange::Unknown => None,
    }
}

/// Converts an internal page size of `limit` items into a page limit of the
/// public pagination contract.
pub(crate) fn page_limit(limit: usize) -> EffectiveLimit {
    contract_page_limit(limit).expect(
        "an internal page size should be a constant or a step budget validated against the \
         pagination contract",
    )
}

fn contract_page_limit(limit: usize) -> std::result::Result<EffectiveLimit, LimitError> {
    let requested = u32::try_from(limit).unwrap_or(u32::MAX);
    EffectiveLimit::from_requested(Some(requested))
}

#[cfg(test)]
mod tests {
    use super::{contract_page_limit, effective_batch_size};
    use loonfs::MAX_CURRENT_FILES_PER_STAT;
    use loonfs_types::{LimitError, MAX_PAGE_LIMIT};

    #[test]
    fn page_limits_enforce_the_pagination_contract() {
        assert_eq!(contract_page_limit(0), Err(LimitError::Zero));
        assert_eq!(contract_page_limit(7).expect("valid limit").get(), 7);
        assert_eq!(
            contract_page_limit(usize::MAX),
            Err(LimitError::ExceedsMax {
                requested: u32::MAX,
                max_limit: MAX_PAGE_LIMIT,
            })
        );
    }

    #[test]
    fn resolve_batches_stay_within_the_core_batch_cap() {
        assert_eq!(effective_batch_size(0), 1);
        assert_eq!(effective_batch_size(9), 9);
        assert_eq!(
            effective_batch_size(MAX_CURRENT_FILES_PER_STAT * 2),
            MAX_CURRENT_FILES_PER_STAT
        );
    }
}
