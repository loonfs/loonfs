//! Replays a validated WAL tail onto metadata state, record by record.

pub(crate) use super::frame::WalObjectError;
use super::ProjectedWalTail;
use super::{ReplayedWalTail, ValidatedWalTail};
use crate::error::MetadataProjectionLoadError;
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{WalObjectEnvelope, WalObjectPayload};
use loonfs_types::{ChangeSeq, WalNo};

pub(crate) fn project_validated_wal_tail(
    base_head: &NamespaceReadState,
    mut tail: ProjectedWalTail,
    wal_tail: &ValidatedWalTail,
) -> Result<ReplayedWalTail, MetadataProjectionLoadError> {
    let mut head = base_head.clone();
    for object in wal_tail.objects() {
        let payload = object.envelope().payload();
        let replay_error = |error| MetadataProjectionLoadError::WalReplay {
            object_key: object.object_key().to_owned(),
            error,
        };
        tail.push_object(&payload.namespace_id, &payload.records)
            .map_err(replay_error)?;
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

/// Rejects a WAL object whose basis floor is below `prior_basis_floor_seq`,
/// the floor at WAL number `prior_wal_no` (the previous object's basis
/// floor, or the acknowledged floor of the manifest that folds through
/// `prior_wal_no`).
///
/// Basis floors never decrease along the WAL, and the first object above a
/// manifest records at least the manifest's acknowledged floor, which is why
/// a fold can take the last object's floor as the highest one it folds (see
/// the design note's [Acknowledging the floor](https://github.com/loonfs/loonfs/blob/main/docs/design/retention-window.md#acknowledging-the-floor)).
pub(crate) fn validate_basis_floor(
    prior_wal_no: WalNo,
    prior_basis_floor_seq: ChangeSeq,
    payload: &WalObjectPayload,
) -> Result<(), WalObjectError> {
    if payload.basis_floor_seq < prior_basis_floor_seq {
        return Err(WalObjectError::BasisFloorDecrease {
            wal_no: payload.wal_no,
            basis_floor_seq: payload.basis_floor_seq,
            prior_wal_no,
            prior_basis_floor_seq,
        });
    }
    Ok(())
}
