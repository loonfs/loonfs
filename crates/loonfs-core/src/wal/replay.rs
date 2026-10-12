//! Replays a validated WAL tail onto metadata state, record by record.

pub(crate) use super::frame::WalObjectError;
use super::ProjectedWalTail;
use super::{ReplayedWalTail, ValidatedWalTail};
use crate::commit::next_inode_after;
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{WalCommitDelta, WalDelta, WalObjectEnvelope};
use loonfs_types::{ChangeSeq, InodeId};

pub(crate) fn project_validated_wal_tail(
    base_head: &NamespaceReadState,
    mut tail: ProjectedWalTail,
    wal_tail: &ValidatedWalTail,
) -> Result<ReplayedWalTail, WalObjectError> {
    let mut head = base_head.clone();
    for object in wal_tail.objects() {
        let payload = object.envelope().payload();
        for record in &payload.records {
            head.seq = record.committed_seq;
            head.next_inode_id =
                replay_next_inode_id_from_commit_deltas(head.next_inode_id, &record.deltas);
        }
        tail.push_object(&payload.namespace_id, &payload.records)?;
        if head.next_inode_id != payload.next_inode_id {
            return Err(WalObjectError::SummaryMismatch);
        }
        head = head.after_wal_object(payload);
    }
    Ok(ReplayedWalTail {
        resulting_head: head,
        projected_tail: tail,
    })
}

pub(crate) fn validate_wal_object_for_replay(
    expected_prior_head_seq: ChangeSeq,
    envelope: &WalObjectEnvelope,
) -> Result<(), WalObjectError> {
    if envelope.payload().records.is_empty() {
        if envelope.payload().head_seq != expected_prior_head_seq {
            return Err(WalObjectError::SummaryMismatch);
        }
        return Ok(());
    }
    let expected_first_seq = expected_prior_head_seq
        .successor()
        .map_err(|_| WalObjectError::SeqOverflow)?;

    if envelope
        .payload()
        .records
        .first()
        .map(|record| record.committed_seq)
        != Some(expected_first_seq)
        || envelope
            .payload()
            .records
            .last()
            .map(|record| record.committed_seq)
            != Some(envelope.payload().head_seq)
    {
        return Err(WalObjectError::SummaryMismatch);
    }
    for (offset, record) in envelope.payload().records.iter().enumerate() {
        let expected = expected_first_seq
            .0
            .checked_add(offset as u64)
            .map(ChangeSeq)
            .ok_or(WalObjectError::SeqOverflow)?;
        if record.committed_seq != expected {
            return Err(WalObjectError::NonContiguousSeq {
                expected,
                actual: record.committed_seq,
            });
        }
    }

    Ok(())
}

fn replay_next_inode_id_from_commit_deltas(
    current_next_inode_id: InodeId,
    deltas: &[WalCommitDelta],
) -> InodeId {
    deltas
        .iter()
        .fold(current_next_inode_id, |next_inode_id, delta| {
            match &delta.delta {
                // At the limit, allocation stops at the current ID.
                WalDelta::CreateInode { inode_id, .. } => {
                    next_inode_id.max(next_inode_after(*inode_id).unwrap_or(*inode_id))
                }
                // Other delta types reference existing inodes and do not
                // allocate IDs.
                WalDelta::BindDirentry { .. }
                | WalDelta::UnbindDirentry { .. }
                | WalDelta::AppendFileRevision { .. }
                | WalDelta::TombstoneSubtree { .. }
                | WalDelta::RevokeSubtreeTombstone { .. }
                | WalDelta::AppendAttributesRevision { .. }
                | WalDelta::AppendAccessRevision { .. } => next_inode_id,
            }
        })
}
