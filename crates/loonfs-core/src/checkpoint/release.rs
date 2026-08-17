//! Explicit checkpoint release: the owner-driven end of a user pin's
//! lifecycle.
//!
//! Release flips the record `active` -> `released` by compare-and-swap,
//! stamping the release instant, and returns. That is terminal: the record
//! never pins anything again, and garbage collection deletes it a grace
//! window after that stamp (format spec, "Garbage collection"). Fork-owned
//! records are not releasable here: their release is decided by garbage
//! collection from the fork target's fate.

use super::record::{read_checkpoint_record, release_checkpoint_record};
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use loonfs_api::wire::control::CheckpointOwner;
use loonfs_api::{CheckpointId, NamespaceId, ReleaseCheckpointResponse};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn release_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    context: &MutationContext,
) -> Result<ReleaseCheckpointResponse> {
    let Some(loaded) = read_checkpoint_record(store, namespace_id, checkpoint_id).await? else {
        // Already reaped (or never created): the end state — no active pin
        // under this id — already holds, so release is idempotent success.
        return Ok(ReleaseCheckpointResponse {
            namespace_id: namespace_id.clone(),
            checkpoint_id: checkpoint_id.clone(),
        });
    };
    if let CheckpointOwner::Fork {
        target_namespace_id,
        ..
    } = &loaded.state.owner
    {
        return Err(CoreError::InvalidCheckpointRequest(format!(
            "checkpoint `{checkpoint_id}` is owned by fork target `{target_namespace_id}`; \
             it is released by deleting that namespace, not by this operation"
        )));
    }
    release_checkpoint_record(store, namespace_id, checkpoint_id, context.now_ms).await?;
    Ok(ReleaseCheckpointResponse {
        namespace_id: namespace_id.clone(),
        checkpoint_id: checkpoint_id.clone(),
    })
}
