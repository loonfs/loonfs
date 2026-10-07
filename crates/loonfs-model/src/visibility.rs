//! Visibility at one sequence: which inodes are visible, what a path
//! resolves to, what a directory lists, and which moves would create a cycle
//! (`docs/specs/format.md` sections 1.6, 4.3, and 4.4).
//!
//! This module intentionally does not share code with
//! `loonfs-core::metadata::visibility`. Differential tests replay the same
//! commits through both implementations and compare their answers. Merging
//! the implementations would remove the independence those tests require.

use crate::metadata::{
    DirentryBindingRecord, DirentryBindingState, InodeRecord, MetadataState, SubtreeTombstoneAction,
};
use crate::Result;
use loonfs_types::{
    AbsolutePath, ChangeSeq, DisplayName, InodeId, InodeKind, NameKey, ROOT_INODE_ID,
};
use std::collections::BTreeSet;

/// The answer to a path lookup at one sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathLookup<'a> {
    /// Every component named a visible child. `absolute_path` is spelled
    /// with the stored display names, and `binding` is the last binding
    /// followed, absent for the root.
    Found {
        absolute_path: String,
        inode: &'a InodeRecord,
        binding: Option<&'a DirentryBindingRecord>,
    },
    /// The last component of `absolute_path` names no visible child.
    NotFound { absolute_path: String },
    /// The inode at `absolute_path` is a file, so the next component has no
    /// directory to look in.
    NotADirectory {
        absolute_path: String,
        inode: &'a InodeRecord,
    },
}

/// One name in a listing: its visible binding and the child it binds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListedEntry<'a> {
    pub binding: &'a DirentryBindingRecord,
    pub display_name: &'a DisplayName,
    pub child: &'a InodeRecord,
}

impl MetadataState {
    /// The inode, when it was created by `seq` and no active tombstone is
    /// rooted at it or at any ancestor.
    pub fn visible_inode(&self, inode_id: InodeId, seq: ChangeSeq) -> Result<Option<&InodeRecord>> {
        let Some(inode) = self
            .inodes
            .iter()
            .find(|inode| inode.inode_id == inode_id && inode.committed_seq <= seq)
        else {
            return Ok(None);
        };
        for member in self.ancestry(inode_id, seq)? {
            if self.has_active_tombstone(member, seq) {
                return Ok(None);
            }
        }
        Ok(Some(inode))
    }

    /// Resolves `path` from the root, folding each component into its name
    /// key and following only bindings of visible children.
    pub fn resolve_path(&self, path: &AbsolutePath, seq: ChangeSeq) -> Result<PathLookup<'_>> {
        let Some(mut inode) = self.visible_inode(ROOT_INODE_ID, seq)? else {
            return Ok(PathLookup::NotFound {
                absolute_path: absolute_path(&[]),
            });
        };
        let mut binding = None;
        let mut names = Vec::new();
        for component in path.components() {
            if inode.inode_kind != InodeKind::Directory {
                return Ok(PathLookup::NotADirectory {
                    absolute_path: absolute_path(&names),
                    inode,
                });
            }
            let name_key = NameKey::for_display_name(&component.to_display_name());
            let Some(entry) = self.visible_entry(inode.inode_id, &name_key, seq)? else {
                names.push(component.as_str());
                return Ok(PathLookup::NotFound {
                    absolute_path: absolute_path(&names),
                });
            };
            names.push(entry.display_name.as_str());
            inode = entry.child;
            binding = Some(entry.binding);
        }
        Ok(PathLookup::Found {
            absolute_path: absolute_path(&names),
            inode,
            binding,
        })
    }

    /// Lists the directory in name key order: one entry for each name whose
    /// value at `seq` binds a visible child.
    pub fn list_directory(
        &self,
        parent_inode_id: InodeId,
        seq: ChangeSeq,
    ) -> Result<Vec<ListedEntry<'_>>> {
        let name_keys: BTreeSet<&NameKey> = self
            .direntry_binds
            .iter()
            .filter(|row| row.parent_inode_id == parent_inode_id)
            .map(|row| &row.name_key)
            .collect();
        let mut entries = Vec::new();
        for name_key in name_keys {
            entries.extend(self.visible_entry(parent_inode_id, name_key, seq)?);
        }
        Ok(entries)
    }

    /// Whether binding `inode_id` under `new_parent_inode_id` would place it
    /// under itself or under one of its descendants.
    pub fn would_create_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
        seq: ChangeSeq,
    ) -> Result<bool> {
        Ok(self.ancestry(new_parent_inode_id, seq)?.contains(&inode_id))
    }

    fn visible_entry(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
        seq: ChangeSeq,
    ) -> Result<Option<ListedEntry<'_>>> {
        let Some(binding) = self.slot_value(parent_inode_id, name_key, seq) else {
            return Ok(None);
        };
        let DirentryBindingState::Bound { display_name } = &binding.state else {
            return Ok(None);
        };
        Ok(self
            .visible_inode(binding.child_inode_id, seq)?
            .map(|child| ListedEntry {
                binding,
                display_name,
                child,
            }))
    }

    /// Whether the newest tombstone event for `root_inode_id` at or below
    /// `seq` is a set.
    fn has_active_tombstone(&self, root_inode_id: InodeId, seq: ChangeSeq) -> bool {
        self.subtree_tombstones
            .iter()
            .filter(|event| event.root_inode_id == root_inode_id && event.committed_seq <= seq)
            .max_by_key(|event| (event.committed_seq, event.delta_index))
            .is_some_and(|event| matches!(event.action, SubtreeTombstoneAction::Set { .. }))
    }
}

fn absolute_path(names: &[&str]) -> String {
    format!("/{}", names.join("/"))
}
