pub(crate) use super::frame::WalReplayError;
use super::{ReplayedWalTail, ValidatedWalSegment};
use crate::invariants::InvariantId;
use crate::metadata::MetadataState;
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::wal::{WalCommitDelta, WalDelta, WalSegmentEnvelope};
use loonfs_api::{validate_wal_segment_id, ChangeSeq, InodeId, NamespaceId};
use loonfs_objectstore::keys::wal_segment;

pub(crate) fn replay_validated_wal_tail_with_metadata(
    base_head: &HeadState,
    base_metadata_state: &MetadataState,
    wal_tail: &[ValidatedWalSegment],
) -> Result<ReplayedWalTail, WalReplayError> {
    let mut current_head = base_head.clone();
    let mut current_metadata_state = base_metadata_state.clone();
    let mut checked_invariants = Vec::new();

    for wal_segment in wal_tail {
        validate_decoded_replayed_wal(
            &current_head.namespace_id,
            current_head.seq,
            wal_segment.object_key(),
            wal_segment.envelope(),
        )?;
        for record in wal_segment.records() {
            current_head.seq = record.seq;
            current_head.head_commit_id = record.commit_id.clone();
            current_head.next_inode_id =
                replay_next_inode_id_from_commit_deltas(current_head.next_inode_id, &record.deltas);
            let apply_invariants = current_metadata_state
                .apply_committed_wal_record_mut(record)
                .map_err(WalReplayError::MetadataApply)?;
            push_invariant(
                &mut checked_invariants,
                InvariantId::WalReplayAppliesMetadataRows,
            );
            extend_invariants(&mut checked_invariants, &apply_invariants);
        }
        current_head.visible_wal_tip = Some(wal_segment.pointer());
        extend_wal_replay_invariants(&mut checked_invariants);
    }

    Ok(ReplayedWalTail {
        resulting_head: current_head,
        resulting_metadata_state: current_metadata_state,
        checked_invariants,
    })
}

pub(super) fn validate_decoded_replayed_wal(
    expected_namespace: &NamespaceId,
    expected_base_head_seq: ChangeSeq,
    object_key: &str,
    envelope: &WalSegmentEnvelope,
) -> Result<(), WalReplayError> {
    validate_wal_segment_id(&envelope.payload.segment_id)
        .map_err(|err| WalReplayError::Codec(err.to_string()))?;
    let expected_object_key = wal_segment(
        envelope.payload.namespace_id.as_str(),
        &envelope.payload.segment_id,
    );

    if object_key != expected_object_key {
        return Err(WalReplayError::ObjectKeyMismatch {
            expected: expected_object_key,
            actual: object_key.to_owned(),
        });
    }

    if &envelope.payload.namespace_id != expected_namespace {
        return Err(WalReplayError::NamespaceMismatch {
            expected: expected_namespace.clone(),
            actual: envelope.payload.namespace_id.clone(),
        });
    }

    if envelope.payload.base_head_seq != expected_base_head_seq {
        return Err(WalReplayError::BaseHeadSeqMismatch {
            expected: expected_base_head_seq,
            actual: envelope.payload.base_head_seq,
        });
    }

    let expected_start = expected_base_head_seq
        .0
        .checked_add(1)
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
        if &record.namespace_id != expected_namespace {
            return Err(WalReplayError::NamespaceMismatch {
                expected: expected_namespace.clone(),
                actual: record.namespace_id.clone(),
            });
        }
    }

    Ok(())
}

pub(super) fn extend_wal_replay_invariants(checked_invariants: &mut Vec<InvariantId>) {
    for invariant in [
        InvariantId::WalPayloadChecksumMatchesPayload,
        InvariantId::WalKeyMatchesSegmentSeqRange,
        InvariantId::WalReplayRequiresMatchingNamespace,
        InvariantId::WalReplayRequiresMatchingBaseHeadSeq,
        InvariantId::WalTailSeqIsContiguous,
    ] {
        push_invariant(checked_invariants, invariant);
    }
}

fn replay_next_inode_id(current_next_inode_id: InodeId, deltas: &[WalDelta]) -> InodeId {
    deltas
        .iter()
        .fold(current_next_inode_id, |next_inode_id, delta| match delta {
            WalDelta::CreateInode { inode_id, .. } => {
                InodeId(next_inode_id.0.max(inode_id.0.saturating_add(1)))
            }
            WalDelta::BindDirentry { .. }
            | WalDelta::UnbindDirentry { .. }
            | WalDelta::AppendFileRevision { .. }
            | WalDelta::TombstoneSubtree { .. } => next_inode_id,
        })
}

fn replay_next_inode_id_from_commit_deltas(
    current_next_inode_id: InodeId,
    deltas: &[WalCommitDelta],
) -> InodeId {
    deltas.iter().fold(current_next_inode_id, |next, delta| {
        replay_next_inode_id(next, std::slice::from_ref(&delta.delta))
    })
}

fn extend_invariants(checked_invariants: &mut Vec<InvariantId>, new_invariants: &[InvariantId]) {
    for invariant in new_invariants {
        push_invariant(checked_invariants, *invariant);
    }
}

fn push_invariant(checked_invariants: &mut Vec<InvariantId>, invariant: InvariantId) {
    if !checked_invariants.contains(&invariant) {
        checked_invariants.push(invariant);
    }
}
