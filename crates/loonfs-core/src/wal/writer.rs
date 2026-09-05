//! Assembles materialized commits into one encoded WAL segment ready to
//! publish.

use super::{PreparedWalSegment, WalSegmentError};
use crate::commit::{wal_payload_from_materialized_commit, MaterializedCommit};
use loonfs_api::wire::control::WalSegmentPointer;
use loonfs_api::wire::wal::{
    encode_wal_segment_envelope_zstd, WalCommitPayload, WalSegmentPayload,
};
use loonfs_api::{ChangeSeq, NamespaceId, WalSegmentId, WriterEpoch};

pub(crate) fn prepare_wal_segment(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    prev_visible_segment: Option<WalSegmentPointer>,
    records: &[MaterializedCommit],
) -> Result<PreparedWalSegment, WalSegmentError> {
    if records.is_empty() {
        return Err(WalSegmentError::EmptySegment);
    }

    let mut payload_records: Vec<WalCommitPayload> = Vec::with_capacity(records.len());
    for record in records {
        if record.commit.namespace_id != namespace_id {
            return Err(WalSegmentError::NamespaceMismatch {
                expected: namespace_id.clone(),
                actual: record.commit.namespace_id.clone(),
            });
        }
        let payload_record = wal_payload_from_materialized_commit(record);
        if let Some(previous) = payload_records.last() {
            let expected = previous
                .seq
                .successor()
                .map_err(|_| WalSegmentError::SeqOverflow)?;
            if payload_record.seq != expected {
                return Err(WalSegmentError::NonContiguousSeq {
                    expected,
                    actual: payload_record.seq,
                });
            }
        }
        payload_records.push(payload_record);
    }

    let start_seq = payload_records
        .first()
        .map(|record| record.seq)
        .ok_or(WalSegmentError::EmptySegment)?;
    let end_seq = payload_records
        .last()
        .map(|record| record.seq)
        .ok_or(WalSegmentError::EmptySegment)?;
    let base_head_seq = start_seq
        .0
        .checked_sub(1)
        .map(ChangeSeq)
        .ok_or(WalSegmentError::SeqOverflow)?;
    // WAL segments are proposals: racing writers may both write one for the
    // same position before the head chooses. The id's 20-digit position makes
    // listings and reclamation scans sort by history position; its random
    // suffix keeps competing proposals (and losers' harmless orphans) from
    // ever colliding on a name.
    let segment_id = WalSegmentId::generate(start_seq);
    let payload = WalSegmentPayload {
        namespace_id,
        segment_id: segment_id.clone(),
        writer_epoch,
        prev_visible_segment,
        base_head_seq,
        start_seq,
        end_seq,
        records: payload_records,
    };
    encode_wal_segment_envelope_zstd(payload).map_err(|err| WalSegmentError::Codec(err.to_string()))
}
