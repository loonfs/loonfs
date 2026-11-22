//! Publishes immutable numbered WAL objects.

use super::PreparedWalObject;
use crate::wal::WalPublishError;
use bytes::Bytes;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

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
