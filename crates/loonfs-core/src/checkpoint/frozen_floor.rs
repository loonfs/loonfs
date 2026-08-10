//! The frozen floor's shared rules: what a binding generation is, which ones
//! an unbind retires, and whether a bind row survives.
//!
//! Two things fold rows below the retention floor, and they are peers. A
//! bounded merge holds every row of its window and decides them together
//! ([`super::reorganize`]). A streaming compaction cannot hold them and runs
//! the same rules as streaming operators instead
//! ([`super::compaction_retention`]). Neither owns the policy, so it lives
//! here, and the equivalence oracle in the tests is what says the two reach
//! the same rows.

use loonfs_api::wire::manifest::MetadataRow;
use loonfs_api::{ChangeSeq, InodeId, NameKey};
use std::collections::BTreeSet;

/// Identifies one binding generation, which is what an unbind names and what
/// the bind drop matches on.
///
/// Identity here omits `child_inode_id` (the read path also matches it); the
/// 4-tuple is already unique for writer-produced rows, so the predicates
/// agree on every legal history.
pub(super) type BindingGeneration = (InodeId, NameKey, ChangeSeq, u32);

/// The binding generations an unbind at or below `retention_floor_seq`
/// retires.
///
/// A whole-group fold builds this from every unbind row it merged. A
/// streaming compaction builds it one binding generation at a time, from that
/// generation's own unbinds: a bind's row key is its generation and an
/// unbind's key leads with the generation it retires, so the rows of one
/// generation hold both halves of the pair.
pub(super) fn unbindings_at_or_below_floor(
    unbind_rows: &[MetadataRow],
    retention_floor_seq: ChangeSeq,
) -> BTreeSet<BindingGeneration> {
    let mut unbound_at_floor = BTreeSet::new();
    for row in unbind_rows {
        if let MetadataRow::DirentryUnbind {
            parent_inode_id,
            name_key,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            ..
        } = row
        {
            if *unbind_seq <= retention_floor_seq {
                unbound_at_floor.insert((
                    *parent_inode_id,
                    name_key.clone(),
                    *bind_seq,
                    *bind_delta_index,
                ));
            }
        }
    }
    unbound_at_floor
}

/// Whether one bind row survives the frozen floor.
///
/// A bind above the floor always survives. At or below it the bind survives
/// exactly when nothing retired it, because a bind is only ever superseded by
/// an operation that also unbinds it — the writer invariant
/// `refuse_superseded_bind_without_unbind` in [`super::reorganize`] refuses to
/// compact without.
///
/// Both bind families read this same rule, which is what keeps them dropping
/// in lockstep: the format gives every bind row exactly one reverse row, and a
/// run whose two counts disagree does not load. They reach the rule by
/// different routes — the forward row's unbinds arrive in its own locality
/// group, the reverse row's do not and are point-read instead — but the rule
/// is one function and the answer is one answer.
pub(super) fn bind_survives_frozen_floor(
    row: &MetadataRow,
    retention_floor_seq: ChangeSeq,
    unbound_at_floor: &BTreeSet<BindingGeneration>,
) -> bool {
    let MetadataRow::DirentryBind {
        parent_inode_id,
        name_key,
        bind_seq,
        bind_delta_index,
        ..
    } = row
    else {
        return true;
    };
    *bind_seq > retention_floor_seq
        || !unbound_at_floor.contains(&(
            *parent_inode_id,
            name_key.clone(),
            *bind_seq,
            *bind_delta_index,
        ))
}
