//! Replays a validated WAL chain onto metadata state, record by record.

pub(crate) use super::frame::WalReplayError;
use super::{DecodedWalRecord, ReplayedWalTail, ValidatedWalChain};
use crate::metadata::{CommitReceiptRecord, MetadataState};
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::wal::{WalCommitDelta, WalDelta, WalSegmentEnvelope};
use loonfs_api::{next_public_ordinal, ChangeSeq, InodeId, NamespaceId, WriterEpoch};

pub(crate) fn project_validated_wal_tail(
    base_head: &HeadState,
    base_metadata_state: &MetadataState,
    expected_writer_epoch: Option<WriterEpoch>,
    wal_tail: &ValidatedWalChain,
) -> Result<ReplayedWalTail, WalReplayError> {
    let mut replayed = replay_wal_records(
        base_head,
        base_metadata_state,
        expected_writer_epoch,
        wal_tail.decoded_records(),
    )?;
    if let Some(last_segment) = wal_tail.segments().last() {
        replayed.resulting_head.visible_wal_tip = Some(last_segment.pointer());
    }
    Ok(replayed)
}

pub(crate) fn replay_wal_records<'a, I>(
    base_head: &HeadState,
    base_metadata_state: &MetadataState,
    expected_writer_epoch: Option<WriterEpoch>,
    records: I,
) -> Result<ReplayedWalTail, WalReplayError>
where
    I: IntoIterator<Item = DecodedWalRecord<'a>>,
{
    let mut current_head = base_head.clone();
    let mut current_metadata_state = base_metadata_state.clone();

    for record in records {
        validate_replay_record(&current_head, expected_writer_epoch, &record)?;
        current_head.seq = record.seq;
        current_head.head_commit_id = record.commit_id.clone();
        current_head.next_inode_id =
            replay_next_inode_id_from_commit_deltas(current_head.next_inode_id, &record.deltas);
        current_metadata_state.apply_committed_wal_record_parts_mut(
            CommitReceiptRecord {
                commit_id: record.commit_id.clone(),
                actor: record.actor.clone(),
                semantic_commit_fingerprint: record.semantic_commit_fingerprint.to_owned(),
                committed_seq: record.seq,
                committed_at_ms: record.committed_at_ms,
                message: record.message.map(str::to_owned),
            },
            &record.deltas,
        );
    }

    Ok(ReplayedWalTail {
        resulting_head: current_head,
        resulting_metadata_state: current_metadata_state,
    })
}

fn validate_replay_record(
    current_head: &HeadState,
    expected_writer_epoch: Option<WriterEpoch>,
    record: &DecodedWalRecord<'_>,
) -> Result<(), WalReplayError> {
    if record.namespace_id != &current_head.namespace_id {
        return Err(WalReplayError::NamespaceMismatch {
            expected: current_head.namespace_id.clone(),
            actual: record.namespace_id.clone(),
        });
    }
    let expected_seq = next_public_ordinal(current_head.seq.0)
        .map(ChangeSeq)
        .ok_or(WalReplayError::SeqOverflow)?;
    if record.seq != expected_seq {
        return Err(WalReplayError::NonContiguousSeq {
            expected: expected_seq,
            actual: record.seq,
        });
    }
    if let Some(expected_max) = expected_writer_epoch {
        // A visible tail may contain older epochs after writer takeover; it
        // must never contain records from an epoch beyond the current head.
        if record.writer_epoch > expected_max {
            return Err(WalReplayError::WriterEpochMismatch {
                expected_max,
                actual: record.writer_epoch,
            });
        }
    }
    Ok(())
}

pub(super) fn validate_wal_segment_for_replay(
    expected_namespace_id: &NamespaceId,
    expected_base_head_seq: ChangeSeq,
    envelope: &WalSegmentEnvelope,
) -> Result<(), WalReplayError> {
    if &envelope.payload.namespace_id != expected_namespace_id {
        return Err(WalReplayError::NamespaceMismatch {
            expected: expected_namespace_id.clone(),
            actual: envelope.payload.namespace_id.clone(),
        });
    }

    if envelope.payload.base_head_seq != expected_base_head_seq {
        return Err(WalReplayError::BaseHeadSeqMismatch {
            expected: expected_base_head_seq,
            actual: envelope.payload.base_head_seq,
        });
    }

    let expected_start = next_public_ordinal(expected_base_head_seq.0)
        .map(ChangeSeq)
        .ok_or(WalReplayError::SeqOverflow)?;

    if envelope.payload.start_seq != expected_start {
        return Err(WalReplayError::NonContiguousSeq {
            expected: expected_start,
            actual: envelope.payload.start_seq,
        });
    }
    if envelope.payload.records.is_empty() {
        return Err(WalReplayError::EmptySegment);
    }
    if envelope.payload.records.first().map(|record| record.seq) != Some(envelope.payload.start_seq)
        || envelope.payload.records.last().map(|record| record.seq)
            != Some(envelope.payload.end_seq)
    {
        return Err(WalReplayError::SegmentSummaryMismatch);
    }
    for (offset, record) in envelope.payload.records.iter().enumerate() {
        let expected = envelope
            .payload
            .start_seq
            .0
            .checked_add(offset as u64)
            .map(ChangeSeq)
            .ok_or(WalReplayError::SeqOverflow)?;
        if record.seq != expected {
            return Err(WalReplayError::NonContiguousSeq {
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
                WalDelta::CreateInode { inode_id, .. } => {
                    InodeId(next_inode_id.0.max(inode_id.0.saturating_add(1)))
                }
                // Every other delta names an inode that already exists, so none
                // of them moves the allocation counter.
                WalDelta::BindDirentry { .. }
                | WalDelta::UnbindDirentry { .. }
                | WalDelta::AppendFileRevision { .. }
                | WalDelta::TombstoneSubtree { .. }
                | WalDelta::RevokeSubtreeTombstone { .. }
                | WalDelta::AppendAttributesRevision { .. } => next_inode_id,
            }
        })
}
