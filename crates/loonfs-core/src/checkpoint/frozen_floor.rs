//! The frozen floor's shared rules: what a binding generation is, which ones
//! an unbind retires, and whether a bind row survives.
//!
//! One engine folds rows below the retention floor
//! ([`super::streaming_compaction`]), and it reaches these rules by two routes.
//! A forward bind row is decided against the unbinds of its own locality group
//! ([`super::compaction_retention`]); a reverse bind row is keyed by child, so
//! its unbinds are point-read out of the same snapshot instead. Neither route
//! owns the policy, so it lives here and both call it.

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
/// The merge builds this one binding generation at a time, from that
/// generation's own unbinds: a bind's row key is its generation and an
/// unbind's key leads with the generation it retires, so the rows of one
/// generation hold both halves of the pair.
pub(super) fn unbindings_at_or_below_floor(
    unbind_rows: &[MetadataRow],
    retention_floor_seq: ChangeSeq,
) -> BTreeSet<BindingGeneration> {
    unbind_rows
        .iter()
        .filter_map(|row| unbinding_at_or_below_floor(row, retention_floor_seq))
        .collect()
}

/// The binding generation this row retires at or below `retention_floor_seq`,
/// or `None` when it retires none.
///
/// The per-row half of [`unbindings_at_or_below_floor`], for a caller that
/// meets unbind rows one at a time in a merged stream rather than holding a
/// slice of them. Both spellings of the set are this one rule.
pub(super) fn unbinding_at_or_below_floor(
    row: &MetadataRow,
    retention_floor_seq: ChangeSeq,
) -> Option<BindingGeneration> {
    let MetadataRow::DirentryUnbind {
        parent_inode_id,
        name_key,
        bind_seq,
        bind_delta_index,
        unbind_seq,
        ..
    } = row
    else {
        return None;
    };
    (*unbind_seq <= retention_floor_seq).then(|| {
        (
            *parent_inode_id,
            name_key.clone(),
            *bind_seq,
            *bind_delta_index,
        )
    })
}

/// Whether one bind row survives the frozen floor.
///
/// A bind above the floor always survives. At or below it the bind survives
/// exactly when nothing retired it, because a bind is only ever superseded by
/// an operation that also unbinds it — the forward binding operator refuses to
/// compact a slot where that does not hold
/// (`super::compaction_retention::BindingRetention::check_the_slot_invariant`).
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
