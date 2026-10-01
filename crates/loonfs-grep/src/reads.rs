//! Filesystem reads used by grep indexing and query verification.
//!
//! Grep reads filesystem state through a read-only [`Namespace`] handle and
//! reads its own index objects directly from the extension keyspace.

use crate::{GrepError, Result};
use loonfs::{
    CheckpointFilesPage, CheckpointFilesPageCursor, CoreError, CurrentFileState,
    ListChangesOptions, ListCheckpointFilesOptions, Namespace, NamespaceMetadata, ReadOnly,
    ReadView, StatPathOptions, MAX_RESOLVE_CURRENT_FILES,
};
use loonfs_api::v0::{FilesystemChange, ListChangesResponse};
use loonfs_api::{
    decode_cursor, AbsolutePath, ChangeSeq, ContentRef, DirectoryPageCursor, EffectiveLimit,
    InodeId, LimitError, NamespaceId, Page, PageRequest, PaginationPolicy, PathEntry, PinId,
    RevisionNo, Subject,
};

/// Filesystem reads for one namespace.
///
/// Each call reads one consistent state, but consecutive calls may observe
/// different heads. Query execution takes one read view so every metadata
/// phase in one response uses the same head.
pub struct NamespaceReads {
    namespace: Namespace<ReadOnly>,
    subject_namespace: Option<Namespace<ReadOnly>>,
}

impl NamespaceReads {
    /// Reads through a read-only handle on one namespace. Performs no I/O.
    pub fn new(namespace: Namespace<ReadOnly>) -> Self {
        Self {
            namespace,
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
        view.require_subject()?;
        Ok(NamespaceReadView {
            namespace: &self.namespace,
            view,
        })
    }

    pub async fn head(&self) -> Result<NamespaceMetadata> {
        Ok(self.namespace.metadata().await?)
    }

    /// Reads one page of the files a checkpoint pins, in ascending inode-id
    /// order. Deleted files are included so an undelete needs no new
    /// postings; queries decide visibility.
    ///
    /// Returns `checkpoint_not_found` if the checkpoint was deleted or
    /// collected, and `checkpoint_unavailable` if its manifest is gone. The
    /// caller must then restart the backfill from a new checkpoint.
    pub async fn list_checkpoint_files_page(
        &self,
        checkpoint_id: &PinId,
        cursor: Option<CheckpointFilesPageCursor>,
        limit: usize,
    ) -> Result<CheckpointFilesPage> {
        Ok(self
            .namespace
            .list_checkpoint_files_page(
                checkpoint_id,
                PageRequest {
                    limit: page_limit(limit).map_err(invalid_page_limit)?,
                    cursor,
                },
                ListCheckpointFilesOptions {
                    include_deleted: true,
                },
            )
            .await?)
    }

    /// Reads committed changes after `after_seq` as semantic events.
    ///
    /// Returns `rebootstrap_required` when `after_seq` is below the retention
    /// floor.
    pub async fn list_changes_after(
        &self,
        after_seq: ChangeSeq,
        limit: usize,
    ) -> Result<ListChangesResponse> {
        Ok(self
            .namespace
            .list_changes_page(
                after_seq,
                ListChangesOptions {
                    limit: Some(page_limit(limit).map_err(invalid_page_limit)?),
                },
            )
            .await?)
    }

    /// Reads one immutable content object by reference, under grep's own
    /// byte budget rather than any deployment download limit.
    pub async fn read_content_ref(
        &self,
        content_ref: &ContentRef,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        Ok(self
            .namespace
            .read_content_ref(content_ref, max_bytes)
            .await?)
    }
}

/// Filesystem reads held to one namespace head for a single grep query.
pub(crate) struct NamespaceReadView<'a> {
    namespace: &'a Namespace<ReadOnly>,
    view: ReadView,
}

impl NamespaceReadView<'_> {
    /// Returns the namespace used by this reader.
    pub(crate) fn namespace_id(&self) -> &NamespaceId {
        self.view.namespace_id()
    }

    /// Returns the head sequence shared by every metadata read.
    pub(crate) fn head_seq(&self) -> ChangeSeq {
        self.view.head_seq()
    }

    /// Reads committed changes after `after_seq`, capped at the view's head.
    ///
    /// The feed itself may observe a later durable head. Its immutable commit
    /// prefix is truncated here so later commits cannot affect this query.
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
                through_seq: head_seq,
                next_after_seq: None,
                changes: Vec::new(),
            });
        }
        let mut page = self
            .namespace
            .list_changes_page(
                after_seq,
                ListChangesOptions {
                    limit: Some(page_limit(limit).map_err(invalid_page_limit)?),
                },
            )
            .await?;
        page.changes
            .retain(|change| change.committed_seq <= head_seq);
        page.through_seq = head_seq;
        page.next_after_seq = page
            .changes
            .last()
            .map(|change| change.committed_seq)
            .filter(|last_seq| *last_seq < head_seq);
        Ok(page)
    }

    /// Resolves visibility, revision, and path in input order.
    pub(crate) async fn resolve_current_files(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        Ok(self.view.resolve_current_files(inode_ids).await?)
    }

    /// Resolves one path against the read view.
    pub(crate) async fn resolve_path(&self, absolute_path: &AbsolutePath) -> Result<PathEntry> {
        Ok(self
            .view
            .get_path_entry(
                absolute_path.as_str(),
                StatPathOptions {
                    include_attributes: loonfs_api::AttributeInclusion::Omit,
                    snapshot_id: None,
                },
            )
            .await?)
    }

    /// Lists one directory page against the read view.
    pub(crate) async fn list_path_page(
        &self,
        absolute_path: &AbsolutePath,
        cursor: Option<DirectoryPageCursor>,
        limit: usize,
    ) -> Result<Page<PathEntry, DirectoryPageCursor>> {
        let page = self
            .view
            .list_path_entries_page(
                absolute_path.as_str(),
                PageRequest {
                    limit: page_limit(limit).map_err(invalid_page_limit)?,
                    cursor,
                },
                loonfs::ListPathEntriesOptions::default(),
            )
            .await?;
        let next_cursor = page
            .next_cursor
            .as_deref()
            .map(decode_cursor)
            .transpose()
            .map_err(|error| {
                GrepError::from(CoreError::InvalidCursor(format!(
                    "the directory listing cursor did not decode: {error}"
                )))
            })?;
        Ok(Page {
            items: page.entries,
            next_cursor,
        })
    }

    /// Reads an authorized inode revision from the read view.
    pub(crate) async fn get_file_revision_bytes_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        Ok(self
            .view
            .get_file_revision_bytes_by_inode(inode_id, revision_no, max_bytes)
            .await?)
    }
}

/// Ids one [`NamespaceReads::resolve_current_files`] call answers.
pub(crate) fn resolve_batch_size(wanted: usize) -> usize {
    wanted.clamp(1, MAX_RESOLVE_CURRENT_FILES)
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
        | FilesystemChange::AccessChanged { .. } => None,
    }
}

/// Validates an internal page request against the public pagination contract.
fn page_limit(limit: usize) -> std::result::Result<EffectiveLimit, LimitError> {
    let requested = u32::try_from(limit).unwrap_or(u32::MAX);
    PaginationPolicy::default().resolve_limit(Some(requested))
}

fn invalid_page_limit(error: LimitError) -> GrepError {
    CoreError::InvalidQuery(error.to_string()).into()
}

#[cfg(test)]
mod tests {
    use super::{page_limit, resolve_batch_size};
    use loonfs::MAX_RESOLVE_CURRENT_FILES;
    use loonfs_api::{LimitError, DEFAULT_MAX_PAGE_LIMIT};

    #[test]
    fn page_limits_enforce_the_pagination_contract() {
        assert_eq!(page_limit(0), Err(LimitError::Zero));
        assert_eq!(page_limit(7).expect("valid limit").get(), 7);
        assert_eq!(
            page_limit(usize::MAX),
            Err(LimitError::ExceedsMax {
                requested: u32::MAX,
                max_limit: DEFAULT_MAX_PAGE_LIMIT,
            })
        );
    }

    #[test]
    fn resolve_batches_stay_within_the_core_batch_cap() {
        assert_eq!(resolve_batch_size(0), 1);
        assert_eq!(resolve_batch_size(9), 9);
        assert_eq!(
            resolve_batch_size(MAX_RESOLVE_CURRENT_FILES * 2),
            MAX_RESOLVE_CURRENT_FILES
        );
    }
}
