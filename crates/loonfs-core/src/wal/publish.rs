//! Publishes immutable numbered WAL objects.

use super::PreparedWalObject;
use crate::wal::WalPublishError;
use bytes::Bytes;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::WalNo;

pub(crate) async fn publish_wal_object<S: ObjectStore + ?Sized>(
    store: &S,
    wal: &PreparedWalObject,
) -> crate::error::Result<()> {
    // Batch admission rejects a commit whose estimated size is over the limit,
    // so this fires only if the estimate is short for some request shape.
    if wal.document_bytes() > loonfs_types::format::wal::MAX_WAL_OBJECT_BYTES {
        return Err(crate::error::CoreError::CommitTooLarge {
            estimated_bytes: wal.document_bytes(),
            max_bytes: loonfs_types::format::wal::MAX_WAL_OBJECT_BYTES,
        });
    }
    let payload = wal.envelope().payload();
    let object_key = wal_object_key(&payload.namespace_id, &payload.wal_no);
    store.put_if_absent(&object_key, Bytes::copy_from_slice(wal.as_bytes())).await
        .map_err(|error| -> crate::error::CoreError {
            match error {
                // Another batch took this number; the caller re-plans at the tip.
                ObjectStoreError::PreconditionFailed { .. } => WalPublishError::NumberTaken.into(),
                error => {
                    tracing::error!(namespace_id = %payload.namespace_id, %object_key, %error, "WAL publication failed");
                    match error {
                        error @ ObjectStoreError::Transport { .. } => WalPublishError::OutcomeUnknown(error.public_message().into_owned()).into(),
                        error => WalPublishError::Write { object_key, message: error.public_message().into_owned(), kind: error.kind() }.into(),
                    }
                }
            }
        })?;
    Ok(())
}

/// Returns whether a fence, or a floor object that a process without the
/// writer epoch put, is live at `wal_no`, where `newest` is the newest
/// manifest after the put (format spec, "Writer ownership").
///
/// A writer may commit at the number that such a put uses, and a fold and a
/// collection may then empty that number before the put, so the put can
/// succeed where no reader replays it. A collection deletes only WAL objects
/// at or below a published manifest's `folded_wal_no`, so a number above
/// `newest`'s `folded_wal_no` wasn't emptied. When the manifest that the put
/// was planned from is still the newest, the object is live without another
/// load, because the tail was read above that manifest's `folded_wal_no`.
pub(crate) fn wal_object_is_live(wal_no: WalNo, newest: &NamespaceManifestPayload) -> bool {
    wal_no > newest.folded_wal_no
}

/// The key of the WAL object at `wal_no`.
#[expect(
    clippy::disallowed_methods,
    reason = "the wal module is the physical WAL boundary, so only it builds WAL object keys"
)]
pub(super) fn wal_object_key(
    namespace_id: &loonfs_types::NamespaceId,
    wal_no: &loonfs_types::WalNo,
) -> String {
    loonfs_objectstore::keys::wal_object(namespace_id, wal_no)
}
