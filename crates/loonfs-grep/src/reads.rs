//! Every filesystem read grep performs, as calls on a runtime read handle.
//!
//! Grep owns an index, not a filesystem: it learns what to index from the
//! checkpointed file enumeration and the semantic change feed, and it
//! verifies query candidates against current state — resolving inodes,
//! paths, directories, and content by reference. Every one of those is a
//! published [`FsReader`] operation any consumer could call, and this module
//! is the one place grep calls them, so the surface it depends on stays
//! countable.
//!
//! Reads of grep's own durable state (segments, manifests, the root
//! pointer, all under `namespaces/{namespace}/extensions/grep/`) do not come
//! through here: that keyspace is grep's, and it reads it directly.

use crate::{GrepError, Result};
use loonfs::{
    CheckpointFilesPage, CheckpointFilesPageCursor, CoreError, CurrentFileState, FsReader,
    ListChangesOptions, MAX_RESOLVE_CURRENT_FILES,
};
use loonfs_api::v0::{ChangesResponse, FilesystemChange};
use loonfs_api::{
    decode_cursor, AbsolutePath, AuthoritativePathEntry, ChangeSeq, CheckpointId, ContentRef,
    DirectoryPageCursor, EffectiveLimit, InodeId, NamespaceId, Page, PageRequest, RevisionNo,
    DEFAULT_MAX_PAGE_LIMIT,
};
use std::num::NonZeroU32;

/// One namespace's filesystem reads, borrowed from a reader handle.
///
/// Each call is its own internally-consistent read: the runtime pins a
/// snapshot per call, so a page of checkpoint files, a batch of resolved
/// inodes, and the content read that follows them may observe different
/// heads. The verification model already tolerates that — a query treats
/// index and feed output as candidates and re-verifies every one against
/// current state before emitting a match, and the worker keys postings by
/// durable `(inode_id, revision_no)` — so nothing here needs a snapshot
/// held across calls.
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

    /// The namespace every read here answers for.
    pub fn namespace_id(&self) -> &NamespaceId {
        self.namespace_id
    }

    /// The namespace's current head sequence.
    ///
    /// The change feed answers this with one head read: asking for changes
    /// after the last possible sequence reports the head it was evaluated
    /// against and no history.
    pub async fn head_seq(&self) -> Result<ChangeSeq> {
        Ok(self
            .list_changes_after(ChangeSeq(u64::MAX), 1)
            .await?
            .through_seq)
    }

    /// Reads one page of the files a checkpoint pins, in ascending inode-id
    /// order.
    ///
    /// A checkpoint that no longer pins its basis — released, expired, or
    /// reaped — answers `checkpoint_unavailable` instead of falling back to
    /// current state, which is what turns a lost backfill basis into an
    /// explicit rebootstrap.
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
                    limit: page_limit(limit),
                    cursor,
                },
            )
            .await?)
    }

    /// Reads committed changes after `after_seq` as semantic events.
    ///
    /// A cursor below the namespace's retention floor answers
    /// `rebootstrap_required`: the history grep would need to catch up is
    /// gone, so it rebuilds from a fresh checkpoint instead.
    pub async fn list_changes_after(
        &self,
        after_seq: ChangeSeq,
        limit: usize,
    ) -> Result<ChangesResponse> {
        Ok(self
            .reader
            .list_changes(
                self.namespace_id,
                after_seq,
                ListChangesOptions {
                    limit: Some(page_limit(limit)),
                },
            )
            .await?)
    }

    /// Answers what each inode looks like now: visible, its current
    /// revision, and its current path, in input order.
    ///
    /// At most [`MAX_RESOLVE_CURRENT_FILES`] ids per call; callers chunk
    /// with [`resolve_batch_size`].
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
    pub async fn resolve_path(
        &self,
        absolute_path: &AbsolutePath,
    ) -> Result<AuthoritativePathEntry> {
        Ok(self
            .reader
            .stat_path(self.namespace_id, absolute_path.as_str())
            .await?)
    }

    /// Lists one page of a directory at the current head.
    pub async fn list_path_page(
        &self,
        absolute_path: &AbsolutePath,
        cursor: Option<DirectoryPageCursor>,
        limit: usize,
    ) -> Result<Page<AuthoritativePathEntry, DirectoryPageCursor>> {
        let page = self
            .reader
            .list_path_entries_page(
                self.namespace_id,
                absolute_path.as_str(),
                PageRequest {
                    limit: page_limit(limit),
                    cursor,
                },
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

/// The revision a change event published, or `None` when the event changed
/// no file content.
///
/// `Moved`, `Deleted`, `Undeleted`, and `AttributesChanged` are deliberately
/// nothing to index: the index is keyed by durable `(inode_id, revision_no)`,
/// and every query verifies its candidates against current state before
/// emitting a match. So a moved file is found at its new path, a deleted one
/// stops verifying, an undeleted one verifies again, and an attribute write
/// leaves the file's bytes alone — none of it touches a posting. Re-indexing
/// them would rewrite postings that are already correct.
pub(crate) fn published_revision(event: &FilesystemChange) -> Option<PublishedRevision<'_>> {
    match event {
        FilesystemChange::Created {
            inode_id,
            revision_no: Some(revision_no),
            content_ref: Some(content_ref),
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
        FilesystemChange::Created { .. }
        | FilesystemChange::Moved { .. }
        | FilesystemChange::Deleted { .. }
        | FilesystemChange::Undeleted { .. }
        | FilesystemChange::AttributesChanged { .. } => None,
    }
}

/// Clamps a caller's page size to what a page request can carry.
fn page_limit(limit: usize) -> EffectiveLimit {
    let limit = u32::try_from(limit)
        .unwrap_or(DEFAULT_MAX_PAGE_LIMIT)
        .clamp(1, DEFAULT_MAX_PAGE_LIMIT);
    EffectiveLimit::new(NonZeroU32::new(limit).expect("a clamped page limit is non-zero"))
}

#[cfg(test)]
mod tests {
    use super::{page_limit, resolve_batch_size};
    use loonfs::MAX_RESOLVE_CURRENT_FILES;
    use loonfs_api::DEFAULT_MAX_PAGE_LIMIT;

    #[test]
    fn page_limits_clamp_into_the_pagination_policy() {
        assert_eq!(page_limit(0).get(), 1);
        assert_eq!(page_limit(7).get(), 7);
        assert_eq!(page_limit(usize::MAX).get(), DEFAULT_MAX_PAGE_LIMIT);
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
