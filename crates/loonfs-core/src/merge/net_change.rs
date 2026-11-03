//! The fork's net change over a range of its commits: the inodes that the
//! range's commits touched, what the fork changed in each of them, and the
//! layouts that the change's content references record.

use super::state::{InodeState, StateReader};
use crate::commit::wal_size::{MAX_MERGED_INODES, MIN_MERGE_OPERATION_BYTES};
use crate::error::{CoreError, MergeBound, Result};
use crate::metadata::MetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::MAX_WAL_OBJECT_BYTES;
use loonfs_types::{ChangeSeq, ContentLayout, ContentRef, InodeId, NamespaceId};
use std::collections::BTreeSet;

/// The most of the fork's commits that one read of a merge's range returns.
/// A page includes each commit's deltas, so it stays at tens of commits.
const MERGE_COMMITS_PAGE: usize = 64;

/// Which components of an inode's state the fork changed in the range.
pub(super) struct Changed {
    pub(super) binding: bool,
    pub(super) content: bool,
    pub(super) attributes: bool,
    pub(super) access: bool,
}

impl Changed {
    /// Compares the inode's state at the range's start, or `None` when the
    /// fork created it in the range, with its state at the end.
    fn between(before: Option<&InodeState>, after: &InodeState) -> Option<Self> {
        let changed = match before {
            None => Self {
                binding: true,
                content: true,
                attributes: true,
                access: true,
            },
            Some(before) => Self {
                binding: !before.same_binding(after),
                content: !before.same_content(after),
                attributes: !before.same_attributes(after),
                access: !before.same_access(after),
            },
        };
        (changed.binding || changed.content || changed.attributes || changed.access)
            .then_some(changed)
    }
}

/// One inode's change over the range.
pub(super) struct InodeChange {
    /// The inode's state at the range's end.
    pub(super) ours: InodeState,
    /// The inode's state at the range's start, or `None` when the fork
    /// created it in the range.
    pub(super) before: Option<InodeState>,
    pub(super) changed: Changed,
}

/// Reads `inode_id`'s state at the range's end from `ours` and at its start
/// from `before`, and returns the inode's change, or `None` when the fork
/// changed none of its components. `fork` is the fork, whose commits in the
/// range touched the inode.
pub(super) async fn inode_change<S: ObjectStore + ?Sized>(
    ours: &mut StateReader<'_, '_, S>,
    before: &mut StateReader<'_, '_, S>,
    fork: &NamespaceId,
    inode_id: InodeId,
) -> Result<Option<InodeChange>> {
    let state = ours.state(inode_id).await?.ok_or_else(|| {
        CoreError::NamespaceCorrupt(format!(
            "a commit of fork `{fork}` touched inode `{inode_id}`, which the fork doesn't have"
        ))
    })?;
    let start = before.state(inode_id).await?;
    Ok(
        Changed::between(start.as_ref(), &state).map(|changed| InodeChange {
            ours: state,
            before: start,
            changed,
        }),
    )
}

/// Lists the inodes that the fork's commits after `source_after_seq` touched,
/// through the sequence that `view` reads at.
///
/// [`MAX_MERGED_INODES`] bounds the set. A range that touched more inodes
/// than one WAL object could contain operations for fails with
/// `merge_too_large`, even if some of those inodes drop out of the net
/// change. The error's `split_at_seq` is the last commit before the one that
/// crossed the bound, so the caller can merge the range through that commit,
/// unless the first commit of the range crossed it.
pub(super) async fn touched_inodes<S: ObjectStore + ?Sized>(
    view: MetadataView<'_, '_, S>,
    source_after_seq: ChangeSeq,
) -> Result<BTreeSet<InodeId>> {
    let mut touched = BTreeSet::new();
    let mut after_seq = source_after_seq;
    // Each pass reads the next page of the range's commits, and the view
    // ends at the range's end, so the loop ends when a page is empty.
    loop {
        let page = view.commits_after(after_seq, MERGE_COMMITS_PAGE).await?;
        if page.is_empty() {
            return Ok(touched);
        }
        for commit in &page {
            touched.extend(crate::diff::touched_inodes(&commit.deltas));
            if touched.len() > MAX_MERGED_INODES {
                return Err(CoreError::MergeTooLarge {
                    bound: MergeBound::TouchedInodes,
                    estimated_bytes: touched.len().saturating_mul(MIN_MERGE_OPERATION_BYTES),
                    max_bytes: MAX_WAL_OBJECT_BYTES,
                    split_at_seq: (after_seq > source_after_seq).then_some(after_seq),
                });
            }
            after_seq = commit.committed_seq;
        }
    }
}

/// Returns the layout of `content_ref` in `view`, a view of `fork`. The
/// fork's fold before a merge wrote a layout for each content reference that
/// the fork's commits made.
pub(super) async fn fork_layout<S: ObjectStore + ?Sized>(
    view: MetadataView<'_, '_, S>,
    fork: &NamespaceId,
    content_ref: &ContentRef,
) -> Result<ContentLayout> {
    Ok(view
        .content_layout(&content_ref.content_id)
        .await?
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "fork `{fork}` has no layout for content `{}` after its fold",
                content_ref.content_id
            ))
        })?
        .layout)
}
