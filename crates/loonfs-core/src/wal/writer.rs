//! Assembles and validates data and fence WAL objects before publication.

use super::PreparedWalObject;
use crate::commit::{wal_payload_from_prepared_commit, PreparedCommit};
use crate::error::{CoreError, Result};
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{
    encode_wal_object_envelope_zstd, WalCommitPayload, WalObjectPayload,
};
use loonfs_types::{ChangeSeq, InodeId, NamespaceId, WriterEpoch};

/// Builds a WAL object for `records` after `head`, or a fence when
/// `records` is empty.
///
/// `basis_floor_seq` is the retention floor of the manifest that the
/// records were validated against, or that the fence was planned against
/// ([Advancing the retention floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor)).
pub(crate) fn prepare_wal_object(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    basis_floor_seq: ChangeSeq,
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
        basis_floor_seq,
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
///
/// A `basis_floor_seq` below the head's is `NamespaceCorrupt`, because the
/// head's floor comes from stored WAL objects or a stored manifest, and
/// replay rejects a WAL object that lowers it.
pub(crate) fn prepare_wal_object_from_payloads(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    basis_floor_seq: ChangeSeq,
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
        basis_floor_seq,
        records: payload_records,
    };
    super::replay::validate_basis_floor(head.wal_no, head.basis_floor_seq, &payload)
        .map_err(|error| CoreError::NamespaceCorrupt(error.to_string()))?;
    let object = encode_wal_object_envelope_zstd(payload)
        .expect("a WAL object this process built should encode");
    super::replay::validate_wal_object_for_replay(head.seq, object.envelope())
        .expect("a WAL object this process built should pass replay validation");
    Ok(object)
}
