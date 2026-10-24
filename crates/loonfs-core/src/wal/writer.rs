//! Assembles and validates data and fence WAL objects before publication.

use super::PreparedWalObject;
use crate::commit::{wal_payload_from_prepared_commit, PreparedCommit};
use crate::error::{CoreError, Result};
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{
    encode_wal_object_envelope_zstd, WalCommitPayload, WalObjectPayload,
};
use loonfs_types::{InodeId, NamespaceId, WriterEpoch};

pub(crate) fn prepare_wal_object(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    records: &[PreparedCommit],
) -> Result<PreparedWalObject> {
    assert!(
        records
            .iter()
            .all(|record| record.commit.namespace_id == namespace_id),
        "every record in a WAL object should belong to namespace `{namespace_id}`"
    );
    let next_inode_id = records.last().map_or(head.next_inode_id, |record| {
        record.commit.resulting_next_inode_id
    });
    prepare_wal_object_from_payloads(
        namespace_id,
        writer_epoch,
        head,
        next_inode_id,
        records
            .iter()
            .map(wal_payload_from_prepared_commit)
            .collect(),
    )
}

/// Builds a WAL object from commit payloads that the caller has already
/// built.
///
/// A batch publish calls this function instead of [`prepare_wal_object`],
/// because the batch already built each commit's payload to apply the commit
/// to its planning session. The caller passes `next_inode_id` (the
/// namespace's next inode ID after the last commit, or the head's when there
/// are no commits) because a commit payload doesn't include that ID.
pub(crate) fn prepare_wal_object_from_payloads(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    next_inode_id: InodeId,
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
        next_inode_id,
        records: payload_records,
    };
    let object = encode_wal_object_envelope_zstd(payload)
        .expect("a WAL object this process built should encode");
    super::replay::validate_wal_object_for_replay(head.seq, object.envelope())
        .expect("a WAL object this process built should pass replay validation");
    Ok(object)
}
