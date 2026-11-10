//! Assembles and validates data, fence, and floor WAL objects before
//! publication.

use super::PreparedWalObject;
use crate::commit::{wal_payload_from_commit_plan, CommitPlan};
use crate::error::{CoreError, Result};
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{
    encode_wal_object_envelope_zstd, WalCommitPayload, WalObjectPayload,
};
use loonfs_types::{ChangeSeq, NamespaceId, WriterEpoch};

/// Builds a WAL object for `records` after `head`, or a fence when
/// `records` is empty.
pub(crate) fn prepare_wal_object(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    records: &[CommitPlan],
) -> Result<PreparedWalObject> {
    assert!(
        records
            .iter()
            .all(|record| record.namespace_id == namespace_id),
        "every record in a WAL object should belong to namespace `{namespace_id}`"
    );
    prepare_wal_object_from_payloads(
        namespace_id,
        writer_epoch,
        head,
        records.iter().map(wal_payload_from_commit_plan).collect(),
    )
}

/// Builds a WAL object from commit payloads that the caller has already
/// built.
///
/// A batch publish calls this function instead of [`prepare_wal_object`],
/// because the batch already built each commit's payload to apply the commit
/// to its planning session.
pub(crate) fn prepare_wal_object_from_payloads(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    payload_records: Vec<WalCommitPayload>,
) -> Result<PreparedWalObject> {
    prepare_payload(namespace_id, writer_epoch, head, payload_records, None)
}

/// Builds a floor object after `head`: a WAL object with no records that
/// records `retention_floor_seq`, which is at most the head's sequence
/// (format spec, "Advancing the retention floor").
pub(crate) fn prepare_floor_object(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    retention_floor_seq: ChangeSeq,
) -> Result<PreparedWalObject> {
    assert!(
        retention_floor_seq <= head.seq,
        "a floor object's floor should be at or below the head it follows"
    );
    prepare_payload(
        namespace_id,
        writer_epoch,
        head,
        Vec::new(),
        Some(retention_floor_seq),
    )
}

fn prepare_payload(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    payload_records: Vec<WalCommitPayload>,
    retention_floor_seq: Option<ChangeSeq>,
) -> Result<PreparedWalObject> {
    let payload = WalObjectPayload {
        namespace_id,
        wal_no: head.wal_no.successor().map_err(|_| {
            CoreError::NamespaceCorrupt(format!(
                "the WAL tip is at number {}, the largest WAL number, so no WAL object can \
                 follow it",
                head.wal_no
            ))
        })?,
        writer_epoch,
        head_seq: payload_records
            .last()
            .map_or(head.seq, |record| record.committed_seq),
        records: payload_records,
        retention_floor_seq,
    };
    let object = encode_wal_object_envelope_zstd(payload)
        .expect("a WAL object this process built should encode");
    super::replay::validate_wal_object_for_replay(head.seq, object.envelope())
        .expect("a WAL object this process built should pass replay validation");
    Ok(object)
}
