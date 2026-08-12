//! Checkpoint inventory: every active record a namespace still carries.
//!
//! A checkpoint record is a garbage-collection root, and the creation
//! response is the only place its id ever appears. An operator who lost that
//! response — or who never saw it, because a duplicate label produced a
//! second record under a second id — has no other way to find the pin and
//! release it. This enumeration is that way.
//!
//! It reads the `checkpoints/` prefix and nothing else. Records that were
//! released are gone from the answer: a release is what stops a record
//! pinning anything, and a released record awaiting its reap pins nothing.

use super::record::load_checkpoint_record_at_key;
use crate::control_object::{core_control_load_error, ControlObjectLoadError};
use crate::error::{CoreError, Result};
use crate::namespace::control::read_head_object;
use futures::StreamExt;
use loonfs_api::wire::control::{CheckpointOwner, CheckpointRecordLifecycle};
use loonfs_api::{CheckpointOwnerSummary, CheckpointSummary, ListCheckpointsResponse, NamespaceId};
use loonfs_objectstore::keys::checkpoint_prefix;
use loonfs_objectstore::ObjectStore;

/// Lists every active checkpoint record under `namespace_id`, oldest first.
///
/// An expired record that no collection pass has released yet is reported,
/// with its expiry in the answer. It is still active, it still roots its
/// basis, and reads still serve from it (`files.rs`) — filtering it out
/// would hide a live root from the one operation whose job is to find live
/// roots. The expiry instant is what says the record is on its way out.
///
/// Fork-owned records are reported beside user pins for the same reason:
/// they root a basis too. The owner in each entry is what says which ones
/// `release_checkpoint` will act on.
///
/// The head is read first so a namespace that does not exist answers
/// `namespace_not_found` rather than "no checkpoints" — an inventory that
/// cannot tell those apart is not an inventory. A terminally deleted
/// namespace still answers: its records outlive the tombstone until garbage
/// collection reaps them, and that is exactly the state an operator is
/// looking into.
pub(crate) async fn list_checkpoints<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<ListCheckpointsResponse> {
    read_head_object(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?;

    let prefix = checkpoint_prefix(namespace_id.as_str());
    let mut keys = store.list_prefix_stream(&prefix);
    let mut checkpoints = Vec::new();
    while let Some(item) = keys.next().await {
        let key = item.map_err(|error| CoreError::store(&prefix, &error))?;
        // Gone between the listing and the read: reaped underneath this
        // call, which is the same answer as never having been there.
        let loaded = match load_checkpoint_record_at_key(store, &key).await {
            Ok(loaded) => loaded,
            Err(ControlObjectLoadError::MissingObject { .. }) => continue,
            Err(error) => return Err(core_control_load_error(error)),
        };
        if loaded.state.state != (CheckpointRecordLifecycle::Active {}) {
            continue;
        }
        let record = loaded.state;
        checkpoints.push(CheckpointSummary {
            checkpoint_id: record.checkpoint_id,
            owner: match record.owner {
                CheckpointOwner::User { name } => CheckpointOwnerSummary::User { name },
                CheckpointOwner::Fork {
                    target_namespace_id,
                } => CheckpointOwnerSummary::Fork {
                    target_namespace_id,
                },
            },
            created_at_ms: record.created_at_ms,
            expires_at_ms: record.expires_at_ms,
            checkpoint_seq: record.manifest_head_seq,
            manifest_id: record.manifest_id,
        });
    }
    // Oldest first, and the id breaks ties, so two records minted in one
    // millisecond still list in one order across calls.
    checkpoints.sort_by(|left, right| {
        left.created_at_ms.cmp(&right.created_at_ms).then_with(|| {
            left.checkpoint_id
                .as_str()
                .cmp(right.checkpoint_id.as_str())
        })
    });

    Ok(ListCheckpointsResponse {
        namespace_id: namespace_id.clone(),
        checkpoints,
    })
}
