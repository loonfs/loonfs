//! Assembles and validates data and fence WAL objects before publication.

use super::PreparedWalObject;
use crate::commit::{wal_payload_from_prepared_commit, PreparedCommit};
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{encode_wal_object_envelope_zstd, WalObjectPayload};
use loonfs_types::{NamespaceId, WriterEpoch};

pub(crate) fn prepare_wal_object(
    namespace_id: NamespaceId,
    writer_epoch: WriterEpoch,
    head: &NamespaceReadState,
    records: &[PreparedCommit],
) -> PreparedWalObject {
    assert!(
        records
            .iter()
            .all(|record| record.commit.namespace_id == namespace_id),
        "every record in a WAL object should belong to namespace `{namespace_id}`"
    );
    let payload_records: Vec<_> = records
        .iter()
        .map(wal_payload_from_prepared_commit)
        .collect();
    let payload = WalObjectPayload {
        namespace_id,
        wal_no: head
            .wal_no
            .successor()
            .expect("the WAL number should stay below its limit"),
        writer_epoch,
        head_seq: payload_records
            .last()
            .map_or(head.seq, |record| record.committed_seq),
        next_inode_id: records.last().map_or(head.next_inode_id, |record| {
            record.commit.resulting_next_inode_id
        }),
        records: payload_records,
    };
    let object = encode_wal_object_envelope_zstd(payload)
        .expect("a WAL object this process built should encode");
    super::replay::validate_wal_object_for_replay(head.seq, object.envelope())
        .expect("a WAL object this process built should pass replay validation");
    object
}
