//! Publishes immutable numbered WAL objects.

use super::PreparedWalObject;
use crate::wal::WalPublishError;
use bytes::Bytes;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

pub(crate) async fn publish_wal_object<S: ObjectStore + ?Sized>(
    store: &S,
    wal: &PreparedWalObject,
    tip: &crate::time::Observation,
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
    let elapsed_ms = tip.age_ms();
    if elapsed_ms > crate::limits::WAL_PUBLISH_BUDGET_MS {
        return Err(WalPublishError::PublishBudgetExceeded {
            elapsed_ms,
            budget_ms: crate::limits::WAL_PUBLISH_BUDGET_MS,
        }
        .into());
    }
    store.put_if_absent(&object_key, Bytes::copy_from_slice(wal.as_bytes())).await
        .map_err(|error| -> crate::error::CoreError {
            match error {
                // Another batch took this number; the caller re-plans at the tip.
                ObjectStoreError::PreconditionFailed { .. } => WalPublishError::NumberTaken.into(),
                error => {
                    tracing::error!(namespace_id = %payload.namespace_id, %object_key, %error, "WAL publication failed");
                    match error {
                        error @ ObjectStoreError::Transport { .. } => WalPublishError::OutcomeUnknown(error.public_message().into_owned()).into(),
                        error => crate::error::CoreError::WalWrite { object_key, message: error.public_message().into_owned(), kind: error.kind() },
                    }
                }
            }
        })?;
    let elapsed_ms = tip.age_ms();
    // A put that lands after the budget may have landed on a reclaimed number.
    if elapsed_ms > crate::limits::WAL_PUBLISH_BUDGET_MS {
        return Err(WalPublishError::OutcomeUnknown(format!(
            "publication budget exceeded after {elapsed_ms}ms (budget {}ms)",
            crate::limits::WAL_PUBLISH_BUDGET_MS,
        ))
        .into());
    }
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
