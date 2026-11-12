//! Replays a validated WAL tail onto metadata state, record by record.

pub(crate) use super::frame::WalObjectError;
use super::ProjectedWalTail;
use super::{ReplayedWalTail, ValidatedWalTail};
use crate::error::MetadataProjectionLoadError;
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::WalObjectEnvelope;
use loonfs_types::CommitSeq;

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
        tail.push_object(&payload.records, object.stored_bytes())
            .map_err(replay_error)?;
        head = head.after_wal_object(payload);
    }
    Ok(ReplayedWalTail {
        resulting_head: head,
        projected_tail: tail,
    })
}

pub(crate) fn validate_wal_object_for_replay(
    expected_prior_head_seq: CommitSeq,
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
            .map(CommitSeq)
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
