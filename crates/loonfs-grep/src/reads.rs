//! Filesystem reads used by grep indexing and query verification.
//!
//! Grep reads filesystem state through [`FsReader`] and reads its own index
//! objects directly from the extension keyspace.

use crate::{GrepError, Result};
use loonfs::{
    CheckpointFilesPage, CheckpointFilesPageCursor, CoreError, CurrentFileState, FsReadSnapshot,
    FsReader, ListChangesOptions, StatPathOptions, MAX_RESOLVE_CURRENT_FILES,
};
use loonfs_api::v0::{FilesystemChange, ListChangesResponse};
use loonfs_api::{
    decode_cursor, AbsolutePath, ChangeSeq, CheckpointId, ContentRef, DirectoryPageCursor,
    EffectiveLimit, InodeId, LimitError, NamespaceId, Page, PageRequest, PaginationPolicy,
    PathEntry, RevisionNo,
};

/// Filesystem reads for one namespace.
///
/// Each call uses an internally consistent snapshot, but consecutive calls
/// may observe different heads. Query execution pins one view so every
/// metadata phase in one response uses the same head.
pub struct NamespaceReads<'a> {
    reader: &'a FsReader,
    namespace_id: &'a NamespaceId,
}

impl<'a> NamespaceReads<'a> {
    /// Borrows a reader for one namespace. Performs no I/O.
    pub fn new(reader: &'a FsReader, namespace_id: &'a NamespaceId) -> Self {
        Self {
            reader,
            namespace_id,
        }
    }

    /// Returns the namespace used by this reader.
    pub fn namespace_id(&self) -> &NamespaceId {
        self.namespace_id
    }

    /// Pins one metadata view for a query.
    pub(crate) async fn pin(&self) -> Result<PinnedNamespaceReads<'a>> {
        Ok(PinnedNamespaceReads {
            reader: self.reader,
            snapshot: self.reader.pin_namespace(self.namespace_id).await?,
        })
    }

    /// Returns the namespace's current head sequence.
    pub async fn head_seq(&self) -> Result<ChangeSeq> {
        Ok(self.reader.get_namespace(self.namespace_id).await?.head_seq)
    }

    /// Reads one page of the files a checkpoint pins, in ascending inode-id
    /// order.
    ///
    /// Returns `checkpoint_unavailable` if the checkpoint was released,
    /// expired, or removed. The caller must then restart the backfill from a
    /// new checkpoint.
    pub async fn list_checkpoint_files_page(
        &self,
        checkpoint_id: &CheckpointId,
        cursor: Option<CheckpointFilesPageCursor>,
        limit: usize,
    ) -> Result<CheckpointFilesPage> {
        Ok(self
            .reader
            .list_checkpoint_files_page(
                self.namespace_id,
                checkpoint_id,
                PageRequest {
                    limit: page_limit(limit).map_err(invalid_page_limit)?,
                    cursor,
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
            .reader
            .list_changes(
                self.namespace_id,
                after_seq,
                ListChangesOptions {
                    limit: Some(page_limit(limit).map_err(invalid_page_limit)?),
                },
            )
            .await?)
    }

    /// Resolves current visibility, revision, and path in input order.
    ///
    /// At most [`MAX_RESOLVE_CURRENT_FILES`] IDs are accepted per call.
    pub async fn resolve_current_files(
        &self,
        inode_ids: &[InodeId],
    ) -> Result<Vec<CurrentFileState>> {
        Ok(self
            .reader
            .resolve_current_files(self.namespace_id, inode_ids)
            .await?)
    }

    /// Resolves one path to its authoritative entry at the current head.
    ///
    /// Attributes stay out of the projection: this resolution verifies a
    /// candidate's kind and revision, and grep answers with matches rather
    /// than with entries, so paying an attribute lookup per candidate would
    /// buy nothing.
    pub async fn resolve_path(&self, absolute_path: &AbsolutePath) -> Result<PathEntry> {
        Ok(self
            .reader
            .get_path_entry(
                self.namespace_id,
                absolute_path.as_str(),
                StatPathOptions {
                    include_attributes: false,
                },
            )
            .await?)
    }

    /// Lists one page of a directory at the current head.
    pub async fn list_path_page(
        &self,
        absolute_path: &AbsolutePath,
        cursor: Option<DirectoryPageCursor>,
        limit: usize,
    ) -> Result<Page<PathEntry, DirectoryPageCursor>> {
        let page = self
            .reader
            .list_path_entries_page(
                self.namespace_id,
                absolute_path.as_str(),
                PageRequest {
                    limit: page_limit(limit).map_err(invalid_page_limit)?,
                    cursor,
                },
                loonfs::ListPathEntriesOptions::default(),
            )
            .await?;
        // The handle hands back the wire cursor every client resumes with;
        // the walk resumes with the same value, decoded.
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

    /// Reads one immutable content object by reference, under grep's own
    /// byte budget rather than any deployment download limit.
    pub async fn read_content_ref(
        &self,
        content_ref: &ContentRef,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        Ok(self
            .reader
            .read_content_ref(self.namespace_id, content_ref, max_bytes)
            .await?)
    }
}

/// Filesystem reads held to one namespace head for a single grep query.
pub(crate) struct PinnedNamespaceReads<'a> {
    reader: &'a FsReader,
    snapshot: FsReadSnapshot,
}

impl PinnedNamespaceReads<'_> {
    /// Returns the namespace used by this reader.
    pub(crate) fn namespace_id(&self) -> &NamespaceId {
        self.snapshot.namespace_id()
    }

    /// Returns the head sequence shared by every metadata read.
    pub(crate) fn head_seq(&self) -> ChangeSeq {
        self.snapshot.head_seq()
    }

    /// Reads committed changes after `after_seq`, capped at the pinned head.
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
                "change feed sequence `{after_seq}` is ahead of pinned head `{head_seq}`"
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
            .reader
            .list_changes(
                self.namespace_id(),
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
        Ok(self.snapshot.resolve_current_files(inode_ids).await?)
    }

    /// Resolves one path against the pinned metadata view.
    pub(crate) async fn resolve_path(&self, absolute_path: &AbsolutePath) -> Result<PathEntry> {
        Ok(self
            .snapshot
            .get_path_entry(
                absolute_path.as_str(),
                StatPathOptions {
                    include_attributes: false,
                },
            )
            .await?)
    }

    /// Lists one directory page against the pinned metadata view.
    pub(crate) async fn list_path_page(
        &self,
        absolute_path: &AbsolutePath,
        cursor: Option<DirectoryPageCursor>,
        limit: usize,
    ) -> Result<Page<PathEntry, DirectoryPageCursor>> {
        let page = self
            .snapshot
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

    /// Reads one immutable content object selected from the pinned view.
    pub(crate) async fn read_content_ref(
        &self,
        content_ref: &ContentRef,
        max_bytes: u64,
    ) -> Result<Vec<u8>> {
        Ok(self
            .snapshot
            .read_content_ref(content_ref, max_bytes)
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
        | FilesystemChange::AttributesChanged { .. } => None,
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
