//! Assembles data and fence segments and derives the head they publish.

use super::{PreparedWalSegment, WalSegmentError};
use crate::commit::{wal_payload_from_materialized_commit, MaterializedCommit};
use crate::namespace::state::NamespaceReadState;
use loonfs_api::wire::wal::{
    encode_wal_segment_envelope_zstd, WalCommitPayload, WalSegmentPayload,
};
use loonfs_api::{ChangeSeq, CommitId, NamespaceId, WalNo, WriterEpoch};

pub(crate) fn prepare_wal_segment(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    records: &[MaterializedCommit],
) -> Result<PreparedWalSegment, WalSegmentError> {
    let wal_no = next_wal_no(head)?;
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
    let payload = WalSegmentPayload {
        namespace_id,
        wal_no,
        next_inode_id: records
            .last()
            .expect("records should be nonempty")
            .commit
            .resulting_next_inode_id,
        writer_epoch,
        base_head_seq,
        start_seq,
        end_seq,
        records: payload_records,
    };
    encode_segment(payload)
}

pub(crate) fn prepare_fence_segment(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
) -> Result<PreparedWalSegment, WalSegmentError> {
    encode_segment(WalSegmentPayload {
        namespace_id,
        wal_no: next_wal_no(head)?,
        writer_epoch,
        next_inode_id: head.next_inode_id,
        base_head_seq: head.seq,
        start_seq: head.seq,
        end_seq: head.seq,
        records: Vec::new(),
    })
}

pub(crate) fn resulting_head_after(
    segment: &PreparedWalSegment,
    head: &NamespaceReadState,
    head_commit_id: CommitId,
) -> NamespaceReadState {
    let payload = segment.envelope().payload();
    NamespaceReadState {
        seq: payload.end_seq,
        head_commit_id,
        next_inode_id: payload.next_inode_id,
        wal_no: payload.wal_no,
        ..head.clone()
    }
}

fn next_wal_no(head: &NamespaceReadState) -> Result<WalNo, WalSegmentError> {
    head.wal_no
        .successor()
        .map_err(|_| WalSegmentError::NumberOverflow)
}

fn encode_segment(payload: WalSegmentPayload) -> Result<PreparedWalSegment, WalSegmentError> {
    encode_wal_segment_envelope_zstd(payload)
        .map_err(|error| WalSegmentError::Codec(error.to_string()))
}
