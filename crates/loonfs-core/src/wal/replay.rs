//! Replays a validated WAL chain onto metadata state, record by record.

pub(crate) use super::frame::WalReplayError;
use super::{DecodedWalRecord, ReplayedWalTail, ValidatedWalChain};
use crate::invariants::InvariantId;
use crate::metadata::MetadataState;
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::wal::{WalCommitDelta, WalDelta, WalSegmentEnvelope};
use loonfs_api::{ChangeSeq, InodeId, NameKey, NamespaceId, WriterEpoch};
use loonfs_objectstore::keys::wal_segment;

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
    extend_invariants(
        &mut replayed.checked_invariants,
        wal_tail.checked_invariants(),
    );
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
    let mut checked_invariants = Vec::new();

    for record in records {
        validate_replay_record(&current_head, expected_writer_epoch, &record)?;
        current_head.seq = record.seq;
        current_head.head_commit_id = record.commit_id.clone();
        current_head.next_inode_id =
            replay_next_inode_id_from_commit_deltas(current_head.next_inode_id, &record.deltas);
        let apply_invariants = current_metadata_state
            .apply_committed_wal_record_parts_mut(
                record.seq,
                record.committed_at_ms,
                record.commit_id,
                record.semantic_commit_fingerprint,
                record.message,
                &record.deltas,
            )
            .map_err(WalReplayError::MetadataApply)?;
        push_invariant(
            &mut checked_invariants,
            InvariantId::WalReplayAppliesMetadataRows,
        );
        extend_invariants(&mut checked_invariants, &apply_invariants);
    }

    Ok(ReplayedWalTail {
        resulting_head: current_head,
        resulting_metadata_state: current_metadata_state,
        checked_invariants,
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
    let expected_seq = current_head
        .seq
        .0
        .checked_add(1)
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
    object_key: &str,
    envelope: &WalSegmentEnvelope,
) -> Result<(), WalReplayError> {
    // The id shape is validated on decode: `segment_id` is a typed
    // `WalSegmentId`, so only well-formed ids can reach this point.
    let expected_object_key = wal_segment(
        envelope.payload.namespace_id.as_str(),
        envelope.payload.segment_id.as_str(),
    );

    if object_key != expected_object_key {
        return Err(WalReplayError::ObjectKeyMismatch {
            expected: expected_object_key,
            actual: object_key.to_owned(),
        });
    }

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
        // Replay is the choke point where WAL name keys enter metadata
        // records, so reject malformed keys here: everything downstream
        // (checkpoint row building, view row keys) relies on record name
        // keys converting back to typed `NameKey`s infallibly.
        for delta in &record.deltas {
            if let WalDelta::BindDirentry { name_key, .. }
            | WalDelta::UnbindDirentry { name_key, .. } = &delta.delta
            {
                NameKey::parse(name_key)
                    .map_err(|err| WalReplayError::Codec(format!("invalid WAL name_key: {err}")))?;
            }
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
            | WalDelta::TombstoneSubtree { .. }
            | WalDelta::RevokeSubtreeTombstone { .. } => next_inode_id,
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
