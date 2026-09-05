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

/// Identifies one durable binding by its parent, name, and originating delta.
/// The writer assigns this identity once; a matching unbind retires it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct BindingIdentity {
    pub(super) parent_inode_id: InodeId,
    pub(super) name_key: NameKey,
    pub(super) bind_seq: ChangeSeq,
    pub(super) bind_delta_index: u32,
}

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
) -> BTreeSet<BindingIdentity> {
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
) -> Option<BindingIdentity> {
    let MetadataRow::DirentryUnbind(crate::metadata::DirentryUnbindRecord {
        parent_inode_id,
        name_key,
        bind_seq,
        bind_delta_index,
        unbind_seq,
        ..
    }) = row
    else {
        return None;
    };
    (*unbind_seq <= retention_floor_seq).then(|| BindingIdentity {
        parent_inode_id: *parent_inode_id,
        name_key: name_key.clone(),
        bind_seq: *bind_seq,
        bind_delta_index: *bind_delta_index,
    })
}

/// Returns whether a bind row remains visible at the frozen retention floor.
///
/// Binds above the floor are always retained. A bind at or below the floor is
/// removed only when a matching unbind is also at or below the floor. The
/// forward and reverse bind families both use this rule so their corresponding
/// rows are retained or removed together.
pub(super) fn bind_survives_frozen_floor(
    row: &MetadataRow,
    retention_floor_seq: ChangeSeq,
    unbound_at_floor: &BTreeSet<BindingIdentity>,
) -> bool {
    let MetadataRow::DirentryBind(crate::metadata::DirentryBindRecord {
        parent_inode_id,
        name_key,
        bind_seq,
        bind_delta_index,
        ..
    }) = row
    else {
        return true;
    };
    *bind_seq > retention_floor_seq
        || !unbound_at_floor.contains(&BindingIdentity {
            parent_inode_id: *parent_inode_id,
            name_key: name_key.clone(),
            bind_seq: *bind_seq,
            bind_delta_index: *bind_delta_index,
        })
}
