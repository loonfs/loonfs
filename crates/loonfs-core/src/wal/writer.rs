//! Assembles and validates data, fence, and floor WAL objects before
//! publication.

use super::PreparedWalObject;
use crate::error::{CoreError, Result};
use crate::namespace::state::NamespaceReadState;
use loonfs_types::format::wal::{encode_wal_object_envelope_zstd, WalObjectBody, WalObjectPayload};
use loonfs_types::WriterEpoch;

/// Builds the next WAL object after `head`, with `body` as its contents.
///
/// A floor object's retention floor is at most the head's sequence (format
/// spec, "Advancing the retention floor"), and a floor above it fails the
/// encoder's check, so this function panics.
pub(crate) fn prepare_wal_object(
    head: &NamespaceReadState,
    writer_epoch: WriterEpoch,
    body: WalObjectBody,
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
        head_seq: match &body {
            WalObjectBody::Commits(commits) => commits.newest().committed_seq,
            WalObjectBody::Fence | WalObjectBody::Floor { .. } => head.seq,
        },
        body,
    };
    let object = encode_wal_object_envelope_zstd(payload)
        .expect("a WAL object this process built should encode");
    super::replay::validate_wal_object_for_replay(head.seq, object.envelope())
        .expect("a WAL object this process built should pass replay validation");
    Ok(object)
}
