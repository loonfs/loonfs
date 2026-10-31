//! The `advance_subtree` deltas that end a commit, one for each directory
//! below the root that is an ancestor of an inode that the commit changes
//! ([format section 1.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#13-directory-bindings)).

use super::session::CommitValidationSession;
use crate::error::Result;
use crate::metadata::{AncestorWalk, DirentryBindingRecord};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::{WalCommitDelta, WalDelta};
use loonfs_types::{InodeId, ROOT_INODE_ID};
use std::collections::HashMap;

/// The first ancestor of the inode that one delta changes.
enum FirstAncestor {
    /// The directory of the edge that the delta records.
    Directory(InodeId),
    /// The current parent of the inode, after the commit's operations.
    ParentOf(InodeId),
}

impl FirstAncestor {
    fn of(delta: &WalDelta) -> Option<Self> {
        match delta {
            WalDelta::BindDirentry {
                parent_inode_id, ..
            }
            | WalDelta::UnbindDirentry {
                parent_inode_id, ..
            }
            | WalDelta::AppendFileRevision {
                parent_inode_id, ..
            } => Some(Self::Directory(*parent_inode_id)),
            WalDelta::TombstoneSubtree {
                deleted_binding, ..
            } => Some(Self::Directory(deleted_binding.parent_inode_id)),
            WalDelta::RevokeSubtreeTombstone {
                root_inode_id: inode_id,
                ..
            }
            | WalDelta::AppendAttributesRevision { inode_id, .. }
            | WalDelta::AppendAccessRevision { inode_id, .. } => Some(Self::ParentOf(*inode_id)),
            // A new inode changes its parent through the bind that follows
            // it.
            WalDelta::CreateInode { .. } | WalDelta::AdvanceSubtree { .. } => None,
        }
    }
}

/// What one commit's walks up the tree found.
#[derive(Default)]
struct AncestorWalks {
    /// The depth of every directory that a walk reached, counted from the
    /// top of its walk, which is at depth 0: the root, or a directory whose
    /// edge the commit unbound.
    depths: HashMap<InodeId, usize>,
    /// The current edge of each directory to advance, with its depth, in the
    /// order that the walks reached the directories, parents first.
    advanced: Vec<(usize, DirentryBindingRecord)>,
}

impl<S: ObjectStore + ?Sized> CommitValidationSession<'_, S> {
    /// Returns the `advance_subtree` deltas that follow `deltas`, the deltas
    /// of the commit's operations, and adds their rows to the commit's rows.
    ///
    /// Each directory below the root that is an ancestor of an inode that
    /// `deltas` change gets one delta under its current edge, after the
    /// operations. A directory whose current edge this commit bound gets
    /// none, because its bind row already has the commit's sequence. A
    /// directory whose edge this commit unbound gets none either, and its
    /// ancestors are advanced through the unbind's parent. The deltas are
    /// ordered by the directory's depth, parents first.
    pub(crate) async fn advance_subtrees(
        &mut self,
        deltas: &[WalCommitDelta],
    ) -> Result<Vec<WalCommitDelta>> {
        let mut walks = AncestorWalks::default();
        for delta in deltas {
            let first = match FirstAncestor::of(&delta.delta) {
                None => continue,
                Some(FirstAncestor::Directory(directory)) => directory,
                Some(FirstAncestor::ParentOf(inode_id)) => {
                    match self
                        .view()
                        .current_parent_binding_for_child(inode_id)
                        .await?
                    {
                        Some(binding) => binding.parent_inode_id,
                        // The root has no parent.
                        None => continue,
                    }
                }
            };
            self.walk_ancestors(first, &mut walks).await?;
        }
        if walks.advanced.is_empty() {
            return Ok(Vec::new());
        }
        // A stable sort keeps the directories of one depth in the order that
        // the walks reached them.
        walks.advanced.sort_by_key(|(depth, _)| *depth);
        let op_index = self.numbering.reserve_op_index()?;
        let mut advances = Vec::with_capacity(walks.advanced.len());
        for (_, edge) in walks.advanced {
            let display_name = edge
                .display_name()
                .expect("a current binding should be bound")
                .clone();
            let delta = WalDelta::AdvanceSubtree {
                delta_index: self.numbering.reserve_delta_index()?,
                parent_inode_id: edge.parent_inode_id,
                name_key: edge.name_key,
                display_name,
                child_inode_id: edge.child_inode_id,
                child_created_by: edge.child_created_by,
                child_created_at_ms: edge.child_created_at_ms,
                bind_position: edge.bind_position,
            };
            self.apply(&delta);
            advances.push(WalCommitDelta {
                semantic_operation_index: op_index,
                delta,
            });
        }
        Ok(advances)
    }

    /// Climbs the current parent bindings from `first` until the root, a
    /// directory that an earlier walk reached, or a directory with no current
    /// binding, and records the directories it passed.
    ///
    /// Each step reaches a directory that no walk reached before, and the
    /// walk refuses a parent-binding cycle, so the walks of one commit read
    /// each directory's binding at most once. Commit validation reads the
    /// same bindings when it checks that no tombstone covers a target, so
    /// these reads find the bindings cached.
    async fn walk_ancestors(&self, first: InodeId, walks: &mut AncestorWalks) -> Result<()> {
        let mut reads = self.view().reads();
        let mut walk = AncestorWalk::new(first);
        let mut climbed: Vec<DirentryBindingRecord> = Vec::new();
        let top_depth = loop {
            let Some(directory) = walk.next(&mut reads).await? else {
                // The commit unbound the directory that the walk reached
                // last.
                let unbound = climbed.last().map_or(first, |edge| edge.parent_inode_id);
                walks.depths.insert(unbound, 0);
                break 0;
            };
            climbed.extend(walk.take_followed_binding());
            if directory == ROOT_INODE_ID {
                break 0;
            }
            if let Some(depth) = walks.depths.get(&directory) {
                break *depth;
            }
        };
        for (above, edge) in climbed.into_iter().rev().enumerate() {
            let depth = top_depth + above + 1;
            walks.depths.insert(edge.child_inode_id, depth);
            if edge.bind_position.seq != self.committed_seq {
                walks.advanced.push((depth, edge));
            }
        }
        Ok(())
    }
}
