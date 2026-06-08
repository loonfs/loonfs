use super::{PreparedWalSegment, WalBuildError};
use crate::commit::{wal_payload_from_materialized_commit, MaterializedCommit};
use crate::invariants::InvariantId;
use loon_api::wire::control::WalSegmentPointer;
use loon_api::wire::wal::{
    encode_wal_segment_envelope_zstd, WalCommitPayload, WalSegmentEnvelope, WalSegmentPayload,
};
use loon_api::{generate_wal_segment_id, ChangeSeq, NamespaceId};
use loon_objectstore::keys::wal_segment;

pub(crate) fn prepare_wal_segment(
    namespace_id: NamespaceId,
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
        let payload_record = wal_payload_from_materialized_commit(record)?;
        if payload_record.namespace_id != namespace_id {
            return Err(WalBuildError::NamespaceMismatch {
                request: payload_record.namespace_id.clone(),
                plan: namespace_id.clone(),
            });
        }
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
            if payload_record.apply_after_seq != previous.seq {
                return Err(WalBuildError::BaseHeadSeqMismatch {
                    request: payload_record.apply_after_seq,
                    plan: previous.seq,
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
    // WAL segment IDs are collision-resistant namespace-incarnation IDs. They
    // are intentionally not derived from the seq range, so losing writers can
    // leave harmless orphan segments without creating reusable object names.
    let segment_id = generate_wal_segment_id();
    let payload = WalSegmentPayload {
        namespace_id,
        segment_id: segment_id.clone(),
        prev_visible_segment,
        base_head_seq: payload_records[0].apply_after_seq,
        start_seq,
        end_seq,
        records: payload_records,
    };
    let envelope = WalSegmentEnvelope::from_payload(writer_version, payload)
        .map_err(|err| WalBuildError::Codec(err.to_string()))?;
    let encoded_bytes = encode_wal_segment_envelope_zstd(&envelope)
        .map_err(|err| WalBuildError::Codec(err.to_string()))?;
    let object_key = wal_segment(envelope.payload.namespace_id.as_str(), &segment_id);

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
