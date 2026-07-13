//! Assembles materialized commits into one encoded WAL segment ready to
//! publish.

use super::{PreparedWalSegment, WalBuildError};
use crate::commit::{wal_payload_from_materialized_commit, MaterializedCommit};
use crate::invariants::InvariantId;
use loonfs_api::wire::control::WalSegmentPointer;
use loonfs_api::wire::wal::{
    encode_wal_segment_envelope_zstd, WalCommitPayload, WalSegmentEnvelope, WalSegmentPayload,
};
use loonfs_api::{ChangeSeq, NamespaceId, WalSegmentId, WriterEpoch};
use loonfs_objectstore::keys::wal_segment;

pub(crate) fn prepare_wal_segment(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    prev_visible_segment: Option<WalSegmentPointer>,
    records: &[MaterializedCommit],
    writer_version: &str,
) -> Result<PreparedWalSegment, WalBuildError> {
    if writer_version.trim().is_empty() {
        return Err(WalBuildError::EmptyWriterVersion);
    }
    if records.is_empty() {
        return Err(WalBuildError::EmptySegment);
    }

    let mut payload_records: Vec<WalCommitPayload> = Vec::with_capacity(records.len());
    for record in records {
        if record.prepared.plan.namespace_id != namespace_id {
            return Err(WalBuildError::NamespaceMismatch {
                request: record.prepared.plan.namespace_id.clone(),
                plan: namespace_id.clone(),
            });
        }
        let payload_record = wal_payload_from_materialized_commit(record)?;
        if let Some(previous) = payload_records.last() {
            let expected = previous
                .seq
                .0
                .checked_add(1)
                .map(ChangeSeq)
                .ok_or_else(|| WalBuildError::Codec("seq overflow".to_owned()))?;
            if payload_record.seq != expected {
                return Err(WalBuildError::NonContiguousSeq {
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
        .ok_or(WalBuildError::EmptySegment)?;
    let end_seq = payload_records
        .last()
        .map(|record| record.seq)
        .ok_or(WalBuildError::EmptySegment)?;
    let base_head_seq = start_seq
        .0
        .checked_sub(1)
        .map(ChangeSeq)
        .ok_or_else(|| WalBuildError::Codec("start seq underflow".to_owned()))?;
    // WAL segments are proposals: racing writers may both write one for the
    // same position before the head chooses. The id's ordered prefix makes
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
    let envelope = WalSegmentEnvelope::from_payload(writer_version, payload)
        .map_err(|err| WalBuildError::Codec(err.to_string()))?;
    let encoded_bytes = encode_wal_segment_envelope_zstd(&envelope)
        .map_err(|err| WalBuildError::Codec(err.to_string()))?;
    let object_key = wal_segment(envelope.payload.namespace_id.as_str(), segment_id.as_str());

    Ok(PreparedWalSegment {
        object_key,
        segment_id,
        envelope,
        encoded_bytes,
        checked_invariants: vec![
            InvariantId::WalPayloadChecksumMatchesPayload,
            InvariantId::WalKeyMatchesSegmentSeqRange,
            InvariantId::HeadPublishRequiresDurableWal,
        ],
    })
}
