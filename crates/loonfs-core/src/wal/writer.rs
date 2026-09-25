//! Assembles and validates data and fence segments before publication.

use super::{PreparedWalSegment, WalSegmentError};
use crate::commit::{wal_payload_from_materialized_commit, MaterializedCommit};
use crate::namespace::state::NamespaceReadState;
use loonfs_api::wire::wal::{encode_wal_segment_envelope_zstd, WalSegmentPayload};
use loonfs_api::{NamespaceId, WriterEpoch};

pub(crate) fn prepare_segment(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    records: &[MaterializedCommit],
) -> Result<PreparedWalSegment, WalSegmentError> {
    for record in records {
        if record.commit.namespace_id != namespace_id {
            return Err(WalSegmentError::NamespaceMismatch {
                expected: namespace_id,
                actual: record.commit.namespace_id.clone(),
            });
        }
    }
    let payload_records: Vec<_> = records
        .iter()
        .map(wal_payload_from_materialized_commit)
        .collect();
    let payload = WalSegmentPayload {
        namespace_id,
        wal_no: head
            .wal_no
            .successor()
            .map_err(|_| WalSegmentError::NumberOverflow)?,
        writer_epoch,
        head_seq: payload_records
            .last()
            .map_or(head.seq, |record| record.committed_seq),
        next_inode_id: records.last().map_or(head.next_inode_id, |record| {
            record.commit.resulting_next_inode_id
        }),
        records: payload_records,
    };
    let segment = encode_wal_segment_envelope_zstd(payload)
        .map_err(|error| WalSegmentError::Codec(error.to_string()))?;
    super::replay::validate_wal_segment_for_replay(head.seq, segment.envelope())?;
    Ok(segment)
}
