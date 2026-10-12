//! Assembles and validates data and fence WAL objects before publication.

use super::{PreparedWalObject, WalObjectError};
use crate::commit::{wal_payload_from_prepared_commit, PreparedCommit};
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{encode_wal_object_envelope_zstd, WalObjectPayload};
use loonfs_types::{NamespaceId, WriterEpoch};

#[allow(
    clippy::disallowed_methods,
    reason = "WAL assembly reports an encode failure against the numbered object key"
)]
pub(crate) fn prepare_wal_object(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    records: &[PreparedCommit],
) -> Result<PreparedWalObject, WalObjectError> {
    for record in records {
        if record.commit.namespace_id != namespace_id {
            return Err(WalObjectError::NamespaceMismatch {
                expected: namespace_id,
                actual: record.commit.namespace_id.clone(),
            });
        }
    }
    let payload_records: Vec<_> = records
        .iter()
        .map(wal_payload_from_prepared_commit)
        .collect();
    let payload = WalObjectPayload {
        namespace_id,
        wal_no: head
            .wal_no
            .successor()
            .map_err(|_| WalObjectError::NumberOverflow)?,
        writer_epoch,
        head_seq: payload_records
            .last()
            .map_or(head.seq, |record| record.committed_seq),
        next_inode_id: records.last().map_or(head.next_inode_id, |record| {
            record.commit.resulting_next_inode_id
        }),
        records: payload_records,
    };
    let object_key = loonfs_objectstore::keys::wal_object(&payload.namespace_id, &payload.wal_no);
    let object =
        encode_wal_object_envelope_zstd(payload).map_err(|error| WalObjectError::Codec {
            object_key,
            message: error.to_string(),
        })?;
    super::replay::validate_wal_object_for_replay(head.seq, object.envelope())?;
    Ok(object)
}
