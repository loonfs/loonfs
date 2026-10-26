//! The retention floor: advancing it removes the commit receipts at or below it, and expires the
//! deletions at or below it in one commit of the namespace's writer.

use crate::error::Result;
use crate::namespace::{CommitRecord, NamespaceState};
use crate::state::ServerState;
use loonfs_model::metadata::SubtreeTombstoneAction;
use loonfs_types::api::v0::{Commit, FilesystemChange};
use loonfs_types::format::manifest::DeltaPosition;
use loonfs_types::format::wal::WalDelta;
use loonfs_types::{
    expiry_commit_fingerprint, ActorId, ChangeSeq, CommitId, ExpiredDeletion, InodeId, NamespaceId,
};
use std::collections::BTreeSet;

/// Moves the floor of `namespace_id` to `to_seq`, or to the head if `to_seq` is above it, and
/// returns the floor. A target at or below the current floor changes nothing.
pub(crate) fn advance_retention_floor(
    state: &mut ServerState,
    namespace_id: &NamespaceId,
    to_seq: ChangeSeq,
) -> Result<ChangeSeq> {
    let now_ms = state.now_ms();
    let commit_id = state.ids.commit_id();
    let namespace = state.namespace_mut(namespace_id)?;
    let target = to_seq.min(namespace.head_seq());
    if target <= namespace.retention_floor_seq {
        return Ok(namespace.retention_floor_seq);
    }
    namespace.retention_floor_seq = target;
    namespace
        .receipts
        .retain(|_, committed_seq| *committed_seq > target);
    let deletions = deletions_at_or_below(namespace, target);
    if !deletions.is_empty() {
        commit_expiry(namespace, commit_id, now_ms, &deletions);
    }
    Ok(target)
}

/// Returns the recoverable deletions at or below `floor`, in deletion order, with the position of
/// each one's tombstone event.
fn deletions_at_or_below(
    namespace: &NamespaceState,
    floor: ChangeSeq,
) -> Vec<(ExpiredDeletion, DeltaPosition)> {
    let view = namespace.view(namespace.head_seq());
    let roots: BTreeSet<InodeId> = namespace
        .metadata
        .subtree_tombstones
        .iter()
        .map(|event| event.root_inode_id)
        .collect();
    let mut deletions: Vec<_> = roots
        .into_iter()
        .filter_map(|root| view.recoverable_deletion(root))
        .filter(|event| event.committed_seq <= floor)
        .filter(|event| matches!(event.action, SubtreeTombstoneAction::Set { .. }))
        .map(|event| {
            (
                ExpiredDeletion {
                    inode_id: event.root_inode_id,
                    deletion_seq: event.committed_seq,
                },
                DeltaPosition {
                    seq: event.committed_seq,
                    delta_index: event.delta_index,
                },
            )
        })
        .collect();
    deletions.sort_by_key(|(deletion, _)| (deletion.deletion_seq, deletion.inode_id));
    deletions
}

/// Commits one `expire` event for each deletion, attributed to `loonfs`, as the namespace's writer
/// does after a floor advance.
fn commit_expiry(
    namespace: &mut NamespaceState,
    commit_id: CommitId,
    now_ms: u64,
    deletions: &[(ExpiredDeletion, DeltaPosition)],
) {
    let actor = ActorId::loonfs();
    let seq = ChangeSeq(namespace.head_seq().0 + 1);
    let deltas: Vec<WalDelta> = (0_u32..)
        .zip(deletions)
        .map(
            |(delta_index, (deletion, target))| WalDelta::ExpireSubtreeTombstone {
                delta_index,
                root_inode_id: deletion.inode_id,
                target: *target,
            },
        )
        .collect();
    let metadata = namespace
        .metadata
        .apply_committed_wal_deltas(seq, &commit_id, &actor, now_ms, &deltas)
        .expect("expiring recoverable deletions should keep the binding rules");
    let expired: Vec<ExpiredDeletion> = deletions.iter().map(|(deletion, _)| *deletion).collect();
    let commit = Commit {
        namespace_id: namespace.namespace_id.clone(),
        commit_id,
        committed_seq: seq,
        committed_by: actor.clone(),
        committed_at_ms: now_ms,
        message: None,
        events: expired
            .iter()
            .map(|deletion| FilesystemChange::DeletionExpired {
                inode_id: deletion.inode_id,
                deletion_seq: deletion.deletion_seq,
            })
            .collect(),
    };
    let fingerprint = expiry_commit_fingerprint(&namespace.namespace_id, &actor, &expired);
    let next_inode_id = namespace.next_inode_id;
    namespace.publish(
        CommitRecord::new(commit, fingerprint),
        metadata,
        next_inode_id,
    );
}
