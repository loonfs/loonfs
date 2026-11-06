//! Assembles and validates data and fence WAL objects before publication.

use super::PreparedWalObject;
use crate::commit::{wal_payload_from_commit_plan, CommitPlan};
use crate::error::{CoreError, Result};
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{
    encode_wal_object_envelope_zstd, WalCommitPayload, WalObjectPayload,
};
use loonfs_types::{NamespaceId, WriterEpoch};

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
    };
    let object = encode_wal_object_envelope_zstd(payload)
        .expect("a WAL object this process built should encode");
    super::replay::validate_wal_object_for_replay(head.seq, object.envelope())
        .expect("a WAL object this process built should pass replay validation");
    Ok(object)
}
