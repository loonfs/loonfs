//! Slot values, parent lookup, and the rules that keep directory bindings
//! consistent at every applied sequence (`docs/specs/format.md` section 1.3).

use crate::metadata::{DeltaPosition, DirentryBindingRecord, DirentryBindingState, MetadataState};
use crate::{Error, Result};
use loonfs_types::{ChangeSeq, InodeId, NameKey};
use std::collections::{BTreeMap, BTreeSet};

impl DirentryBindingRecord {
    pub(crate) fn position(&self) -> DeltaPosition {
        DeltaPosition {
            seq: self.committed_seq,
            delta_index: self.delta_index,
        }
    }

    pub(crate) fn is_bound(&self) -> bool {
        matches!(self.state, DirentryBindingState::Bound { .. })
    }
}

impl MetadataState {
    /// The value of the slot `(parent_inode_id, name_key)` at `seq`: the
    /// slot's newest row at or below `seq`, bound or unbound.
    pub(crate) fn slot_value(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
        seq: ChangeSeq,
    ) -> Option<&DirentryBindingRecord> {
        self.direntry_binds
            .iter()
            .filter(|row| {
                row.parent_inode_id == parent_inode_id
                    && row.name_key == *name_key
                    && row.committed_seq <= seq
            })
            .max_by_key(|row| row.position())
    }

    /// The binding that places `child_inode_id` in a directory at `seq`: the
    /// child's newest row at or below `seq`, when that row is bound.
    pub fn parent_binding(
        &self,
        child_inode_id: InodeId,
        seq: ChangeSeq,
    ) -> Option<&DirentryBindingRecord> {
        self.direntry_binds
            .iter()
            .filter(|row| row.child_inode_id == child_inode_id && row.committed_seq <= seq)
            .max_by_key(|row| row.position())
            .filter(|row| row.is_bound())
    }

    /// `inode_id`, then each ancestor reached through parent bindings at
    /// `seq`.
    pub(crate) fn ancestry(&self, inode_id: InodeId, seq: ChangeSeq) -> Result<Vec<InodeId>> {
        let mut ancestry = vec![inode_id];
        let mut current = inode_id;
        while let Some(binding) = self.parent_binding(current, seq) {
            current = binding.parent_inode_id;
            if ancestry.contains(&current) {
                return Err(Error::AncestorCycle {
                    seq,
                    inode_id: current,
                });
            }
            ancestry.push(current);
        }
        Ok(ancestry)
    }

    /// Checks the bindings at `seq`, the sequence of the commit just applied:
    /// each child has at most one parent binding, each slot at most one
    /// child, the bound values current in slot order are the ones current in
    /// child order, and no inode is its own ancestor.
    pub(crate) fn check_bindings(&self, seq: ChangeSeq) -> Result<()> {
        let mut by_slot = BTreeMap::new();
        let mut by_child = BTreeMap::new();
        for row in &self.direntry_binds {
            if let Some(value) = self
                .slot_value(row.parent_inode_id, &row.name_key, seq)
                .filter(|value| value.is_bound())
            {
                by_slot.insert((value.parent_inode_id, &value.name_key), value);
            }
            if let Some(value) = self.parent_binding(row.child_inode_id, seq) {
                by_child.insert(value.child_inode_id, value);
            }
        }

        let mut slot_of_child = BTreeMap::new();
        for value in by_slot.values() {
            if let Some(other) = slot_of_child.insert(value.child_inode_id, value.position()) {
                return Err(Error::ChildHasTwoParents {
                    seq,
                    child_inode_id: value.child_inode_id,
                    bindings: [other, value.position()],
                });
            }
        }
        let mut child_of_slot = BTreeMap::new();
        for value in by_child.values() {
            let slot = (value.parent_inode_id, &value.name_key);
            if let Some(other) = child_of_slot.insert(slot, value.position()) {
                return Err(Error::SlotHasTwoChildren {
                    seq,
                    parent_inode_id: value.parent_inode_id,
                    name_key: value.name_key.clone(),
                    bindings: [other, value.position()],
                });
            }
        }

        let slot_order: BTreeSet<_> = by_slot.values().map(|value| value.position()).collect();
        let child_order: BTreeSet<_> = by_child.values().map(|value| value.position()).collect();
        if let Some(binding) = slot_order.symmetric_difference(&child_order).next() {
            return Err(Error::BindingIndexesDisagree {
                seq,
                binding: *binding,
            });
        }

        for child_inode_id in by_child.keys() {
            self.ancestry(*child_inode_id, seq)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::metadata::DeltaPosition;
    use crate::{bootstrap_metadata_state, Error};
    use loonfs_types::format::wal::WalDelta;
    use loonfs_types::{ActorId, ChangeSeq, CommitId, DisplayName, InodeId, InodeKind, NameKey};

    fn position(seq: u64, delta_index: u32) -> DeltaPosition {
        DeltaPosition {
            seq: ChangeSeq(seq),
            delta_index,
        }
    }

    fn bind(delta_index: u32, child: u64, kind: InodeKind, parent: u64, name: &str) -> WalDelta {
        WalDelta::BindDirentry {
            delta_index,
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("valid name key"),
            display_name: DisplayName::parse(name).expect("valid display name"),
            child_inode_id: InodeId(child),
            child_kind: kind,
            child_created_by: ActorId::loonfs(),
            child_created_at_ms: 0,
        }
    }

    fn unbind(
        delta_index: u32,
        child: u64,
        kind: InodeKind,
        parent: u64,
        name: &str,
        bound_at: DeltaPosition,
    ) -> WalDelta {
        WalDelta::UnbindDirentry {
            delta_index,
            parent_inode_id: InodeId(parent),
            name_key: NameKey::parse(name).expect("valid name key"),
            display_name: DisplayName::parse(name).expect("valid display name"),
            child_inode_id: InodeId(child),
            child_kind: kind,
            child_created_by: ActorId::loonfs(),
            child_created_at_ms: 0,
            target: loonfs_types::format::manifest::DeltaPosition {
                seq: bound_at.seq,
                delta_index: bound_at.delta_index,
            },
        }
    }

    fn create(inode_id: u64, kind: InodeKind, parent: u64, name: &str) -> Vec<WalDelta> {
        vec![
            WalDelta::CreateInode {
                delta_index: 0,
                inode_id: InodeId(inode_id),
                inode_kind: kind,
            },
            bind(1, inode_id, kind, parent, name),
        ]
    }

    fn first_violation(commits: &[Vec<WalDelta>]) -> Option<Error> {
        let commit_id = CommitId::parse("c_model_bindings").expect("valid commit id");
        let mut state = bootstrap_metadata_state(0);
        for (seq, deltas) in (1..).zip(commits) {
            match state.apply_committed_wal_deltas(
                ChangeSeq(seq),
                &commit_id,
                &ActorId::loonfs(),
                0,
                deltas,
            ) {
                Ok(next) => state = next,
                Err(error) => return Some(error),
            }
        }
        None
    }

    #[test]
    fn histories_that_break_a_binding_rule_are_model_errors() {
        use InodeKind::{Directory, File};

        // The first child's own value still names the slot the second took.
        assert_eq!(
            first_violation(&[create(2, File, 1, "a"), create(3, File, 1, "a")]),
            Some(Error::SlotHasTwoChildren {
                seq: ChangeSeq(2),
                parent_inode_id: InodeId(1),
                name_key: NameKey::parse("a").expect("valid name key"),
                bindings: [position(1, 1), position(2, 1)],
            })
        );
        assert_eq!(
            first_violation(&[create(2, File, 1, "a"), vec![bind(0, 2, File, 1, "b")]]),
            Some(Error::ChildHasTwoParents {
                seq: ChangeSeq(2),
                child_inode_id: InodeId(2),
                bindings: [position(1, 1), position(2, 0)],
            })
        );
        // An unbind that names another child empties the slot, while the
        // bound child's own value still names it.
        assert_eq!(
            first_violation(&[
                create(2, File, 1, "a"),
                vec![unbind(0, 3, File, 1, "a", position(1, 1))],
            ]),
            Some(Error::BindingIndexesDisagree {
                seq: ChangeSeq(2),
                binding: position(1, 1),
            })
        );
        assert_eq!(
            first_violation(&[
                create(2, Directory, 1, "a"),
                create(3, Directory, 2, "b"),
                vec![
                    unbind(0, 2, Directory, 1, "a", position(1, 1)),
                    bind(1, 2, Directory, 3, "c"),
                ],
            ]),
            Some(Error::AncestorCycle {
                seq: ChangeSeq(3),
                inode_id: InodeId(2),
            })
        );
        assert_eq!(
            first_violation(&[
                create(2, Directory, 1, "a"),
                create(3, Directory, 2, "b"),
                vec![
                    unbind(0, 3, Directory, 2, "b", position(2, 1)),
                    bind(1, 3, Directory, 1, "b"),
                ],
            ]),
            None
        );
    }
}
