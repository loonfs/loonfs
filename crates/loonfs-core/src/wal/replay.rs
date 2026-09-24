//! Replays a validated WAL tail onto metadata state, record by record.

pub(crate) use super::frame::WalSegmentError;
use super::ProjectedWalTail;
use super::{ReplayedWalTail, ValidatedWalSegment, ValidatedWalTail};
use crate::commit::next_inode_after;
use crate::error::MetadataProjectionLoadError;
use crate::namespace::state::NamespaceReadState;
use bytes::Bytes;
use loonfs_api::wire::wal::{WalCommitDelta, WalDelta, WalSegmentEnvelope};
use loonfs_api::{ChangeSeq, InodeId};

pub(crate) fn project_validated_wal_tail(
    base_head: &NamespaceReadState,
    base_tail: &ProjectedWalTail,
    wal_tail: &ValidatedWalTail,
) -> Result<ReplayedWalTail, WalSegmentError> {
    let mut replayed = ReplayedWalTail {
        resulting_head: base_head.clone(),
        projected_tail: base_tail.clone(),
    };
    for segment in wal_tail.segments() {
        replayed = replay_wal_records(&replayed.resulting_head, &replayed.projected_tail, segment)?;
        let payload = segment.envelope().payload();
        if replayed.resulting_head.next_inode_id != payload.next_inode_id {
            return Err(WalSegmentError::SegmentSummaryMismatch);
        }
        replayed.resulting_head = replayed.resulting_head.after_segment(payload);
    }
    Ok(replayed)
}

/// Verifies that replaying the WAL tail produces the current head.
///
/// An empty tail retains the basis tip, so the tip is compared only when the
/// replay produced one.
pub(crate) fn ensure_replayed_head_matches(
    current_head: &NamespaceReadState,
    reconstructed: &NamespaceReadState,
) -> Result<(), MetadataProjectionLoadError> {
    if current_head.namespace_id != reconstructed.namespace_id
        || current_head.seq != reconstructed.seq
        || current_head.next_inode_id != reconstructed.next_inode_id
        || current_head.wal_no != reconstructed.wal_no
    {
        return Err(MetadataProjectionLoadError::ReplayedHeadMismatch {
            expected: Box::new(current_head.clone()),
            actual: Box::new(reconstructed.clone()),
        });
    }
    Ok(())
}

pub(crate) fn replay_wal_records(
    base_head: &NamespaceReadState,
    base_tail: &ProjectedWalTail,
    segment: &ValidatedWalSegment,
) -> Result<ReplayedWalTail, WalSegmentError> {
    let mut current_head = base_head.clone();
    let mut current_tail = base_tail.clone();
    let payload = segment.envelope().payload();

    for record in &payload.records {
        for value in &record.inline_content {
            let content_ref = record
                .deltas
                .iter()
                .find_map(|delta| match &delta.delta {
                    WalDelta::AppendFileRevision { content_ref, .. }
                        if content_ref.content_id == value.content_id
                            && content_ref.owner_namespace_id == payload.namespace_id =>
                    {
                        Some(content_ref)
                    }
                    _ => None,
                })
                .expect("decoded inline content should have a same-commit reference");
            current_tail
                .insert_inline_content(content_ref.clone(), Bytes::copy_from_slice(&value.bytes));
        }
        current_head.seq = record.seq;
        current_head.next_inode_id =
            replay_next_inode_id_from_commit_deltas(current_head.next_inode_id, &record.deltas);
        current_tail.apply_commit(record)?;
    }

    Ok(ReplayedWalTail {
        resulting_head: current_head,
        projected_tail: current_tail,
    })
}

pub(crate) fn validate_wal_segment_for_replay(
    expected_prior_head_seq: ChangeSeq,
    envelope: &WalSegmentEnvelope,
) -> Result<(), WalSegmentError> {
    if envelope.payload().records.is_empty() {
        if envelope.payload().head_seq != expected_prior_head_seq {
            return Err(WalSegmentError::SegmentSummaryMismatch);
        }
        return Ok(());
    }
    let expected_first_seq = expected_prior_head_seq
        .successor()
        .map_err(|_| WalSegmentError::SeqOverflow)?;

    if envelope.payload().records.first().map(|record| record.seq) != Some(expected_first_seq)
        || envelope.payload().records.last().map(|record| record.seq)
            != Some(envelope.payload().head_seq)
    {
        return Err(WalSegmentError::SegmentSummaryMismatch);
    }
    for (offset, record) in envelope.payload().records.iter().enumerate() {
        let expected = expected_first_seq
            .0
            .checked_add(offset as u64)
            .map(ChangeSeq)
            .ok_or(WalSegmentError::SeqOverflow)?;
        if record.seq != expected {
            return Err(WalSegmentError::NonContiguousSeq {
                expected,
                actual: record.seq,
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
