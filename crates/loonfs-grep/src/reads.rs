//! Filesystem reads used by grep indexing and query verification.
//!
//! Grep reads filesystem state through [`FsReader`] and reads its own index
//! objects directly from the extension keyspace.

use crate::{GrepError, Result};
use loonfs::{
    CheckpointFilesPage, CheckpointFilesPageCursor, CoreError, CurrentFileState, FsReader,
    ListChangesOptions, StatPathOptions, MAX_RESOLVE_CURRENT_FILES,
};
use loonfs_api::v0::{FilesystemChange, ListChangesResponse};
use loonfs_api::{
    decode_cursor, AbsolutePath, ChangeSeq, CheckpointId, ContentRef, DirectoryPageCursor,
    EffectiveLimit, InodeId, LimitError, NamespaceId, Page, PageRequest, PaginationPolicy,
    PathEntry, RevisionNo, MAX_PUBLIC_INTEGER,
};

/// Filesystem reads for one namespace.
///
/// Each call uses an internally consistent snapshot, but consecutive calls
/// may observe different heads. Query candidates are reverified against
/// current state, so a snapshot does not need to span calls.
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

    /// Returns the namespace's current head sequence.
    ///
    /// Requesting changes after the largest valid sequence reads the current
    /// head without returning any history.
    pub async fn head_seq(&self) -> Result<ChangeSeq> {
        Ok(self
            .list_changes_after(ChangeSeq(MAX_PUBLIC_INTEGER), 1)
            .await?
            .through_seq)
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
