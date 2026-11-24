//! Replays a validated WAL tail onto metadata state, record by record.

pub(crate) use super::frame::WalObjectError;
use super::ProjectedWalTail;
use super::{ReplayedWalTail, ValidatedWalTail};
use crate::error::MetadataProjectionLoadError;
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{WalObjectBody, WalObjectEnvelope};
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
        tail.push_object(&payload.namespace_id, &payload.body, object.stored_bytes())
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
    let payload = envelope.payload();
    let commits = match &payload.body {
        WalObjectBody::Commits(commits) => commits,
        WalObjectBody::Fence | WalObjectBody::Floor { .. } => {
            if payload.head_seq != expected_prior_head_seq {
                return Err(WalObjectError::SummaryMismatch);
            }
            return Ok(());
        }
    };
    let expected_first_seq = expected_prior_head_seq
        .successor()
        .map_err(|_| WalObjectError::SeqOverflow)?;

    if commits.oldest().committed_seq != expected_first_seq
        || commits.newest().committed_seq != payload.head_seq
    {
        return Err(WalObjectError::SummaryMismatch);
    }
    for (offset, record) in commits.iter().enumerate() {
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
