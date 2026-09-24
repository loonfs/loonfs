//! Publishes immutable numbered WAL segments.

use super::PreparedWalSegment;
use crate::commit::WalPublishError;
use bytes::Bytes;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};

// This function owns physical WAL publication.
#[allow(clippy::disallowed_methods)]
pub(crate) async fn publish_segment<S: ObjectStore + ?Sized>(
    store: &S,
    wal: &PreparedWalSegment,
    tip: &crate::time::Observation,
) -> crate::error::Result<()> {
    if wal.document_len() > loonfs_api::wire::wal::MAX_WAL_SEGMENT_BYTES {
        return Err(crate::error::CoreError::Internal(format!(
            "WAL document is {} bytes, over `MAX_WAL_SEGMENT_BYTES` ({})",
            wal.document_len(),
            loonfs_api::wire::wal::MAX_WAL_SEGMENT_BYTES,
        )));
    }
    let payload = wal.envelope().payload();
    let object_key = loonfs_objectstore::keys::wal_segment(&payload.namespace_id, &payload.wal_no);
    let elapsed_ms = tip.age_ms();
    if elapsed_ms > crate::limits::WAL_PUBLISH_BUDGET_MS {
        return Err(WalPublishError::PublishBudgetExceeded {
            elapsed_ms,
            budget_ms: crate::limits::WAL_PUBLISH_BUDGET_MS,
        }
        .into());
    }
    store.put_if_absent(&object_key, Bytes::copy_from_slice(wal.as_bytes())).await
        .map(|_| ())
        .map_err(|error| {
            match error {
                // Another batch took this number; the caller re-plans at the tip.
                ObjectStoreError::PreconditionFailed { .. } => WalPublishError::StaleHead.into(),
                error => {
                    tracing::error!(namespace_id = %payload.namespace_id, %object_key, %error, "WAL publication failed");
                    match error {
                        error @ ObjectStoreError::Transport { .. } => WalPublishError::OutcomeUnknown(error.public_message().into_owned()).into(),
                        error => crate::error::CoreError::WalWrite { object_key, message: error.public_message().into_owned(), class: crate::error::StoreFailureClass::of(&error) },
                    }
                }
            }
        })
}
