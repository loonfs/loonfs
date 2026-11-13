//! Assembles and validates data, fence, and floor WAL objects before
//! publication.

use super::PreparedWalObject;
use crate::error::{CoreError, Result};
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{
    encode_wal_object_envelope_zstd, WalCommitPayload, WalObjectPayload,
};
use loonfs_types::{CommitSeq, WriterEpoch};

/// Builds a WAL object that holds `payload_records` after `head`.
pub(crate) fn prepare_commits_object(
    head: &NamespaceReadState,
    writer_epoch: WriterEpoch,
    payload_records: Vec<WalCommitPayload>,
) -> Result<PreparedWalObject> {
    prepare_payload(head, writer_epoch, payload_records, None)
}

/// Builds a fence after `head`: a WAL object with no records.
pub(crate) fn prepare_fence(
    head: &NamespaceReadState,
    writer_epoch: WriterEpoch,
) -> Result<PreparedWalObject> {
    prepare_payload(head, writer_epoch, Vec::new(), None)
}

/// Builds a floor object after `head`: a WAL object with no records that
/// records `retention_floor_seq`, which is at most the head's sequence
/// (format spec, "Advancing the retention floor").
pub(crate) fn prepare_floor_object(
    head: &NamespaceReadState,
    writer_epoch: WriterEpoch,
    retention_floor_seq: CommitSeq,
) -> Result<PreparedWalObject> {
    assert!(
        retention_floor_seq <= head.seq,
        "a floor object's floor should be at or below the head it follows"
    );
    prepare_payload(head, writer_epoch, Vec::new(), Some(retention_floor_seq))
}

fn prepare_payload(
    head: &NamespaceReadState,
    writer_epoch: WriterEpoch,
    payload_records: Vec<WalCommitPayload>,
    retention_floor_seq: Option<CommitSeq>,
) -> Result<PreparedWalObject> {
    let payload = WalObjectPayload {
        namespace_id: head.namespace_id.clone(),
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
