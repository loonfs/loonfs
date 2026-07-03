//! Retention floor advancement against the current verified manifest.

use super::load::load_verified_manifest_tables;
use super::publish::HEAD_CAS_RETRY_LIMIT;
use crate::context::MutationContext;
use crate::control_update::{update_head, ControlUpdateError, HeadUpdate};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, MetadataViewError};
use crate::namespace::control::read_head_object;
use loonfs_api::wire::control::HeadState;
use loonfs_api::{AdvanceRetentionResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(crate) async fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AdvanceRetentionResponse, CoreError> {
    // Derive the target floor from the currently published manifest before
    // entering the head update. If a newer manifest lands while the CAS
    // below retries, publishing this floor is still safe: floors only move
    // forward and the newer manifest covers at least this much history.
    let loaded_head = read_head_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let current_manifest_id = loaded_head
        .envelope
        .state
        .current_manifest_id
        .ok_or_else(|| MetadataViewError::MissingManifest {
            namespace_id: namespace_id.clone(),
        })?;
    let manifest_tables = load_verified_manifest_tables(store, namespace_id, current_manifest_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    let target_floor = manifest_tables.manifest().payload.head_seq;

    update_head(
        store,
        namespace_id,
        &context.writer_version,
        HEAD_CAS_RETRY_LIMIT,
        |loaded| {
            let head = &loaded.envelope.state;
            if head.retention_floor_seq >= target_floor {
                return Ok(HeadUpdate::Noop(AdvanceRetentionResponse {
                    namespace_id: namespace_id.clone(),
                    retention_floor_seq: head.retention_floor_seq,
                }));
            }

            // Maintenance head update.
            //
            // Advancing retention_floor_seq is a metadata-retention decision. It
            // preserves writer_epoch and writer_lease and is serialized by head
            // CAS. It must not make new WAL visible and must not delete
            // checkpoint-reachable files by itself.
            let next_head = HeadState {
                namespace_id: head.namespace_id.clone(),
                seq: head.seq,
                head_commit_id: head.head_commit_id.clone(),
                writer_epoch: head.writer_epoch,
                writer_lease: head.writer_lease.clone(),
                next_inode_id: head.next_inode_id,
                name_policy: head.name_policy,
                current_manifest_id: head.current_manifest_id,
                latest_checkpoint_id: head.latest_checkpoint_id.clone(),
                retention_floor_seq: target_floor,
                visible_wal_tip: head.visible_wal_tip.clone(),
                state: head.state,
            };
            Ok(HeadUpdate::Replace {
                next: Box::new(next_head),
                outcome: AdvanceRetentionResponse {
                    namespace_id: namespace_id.clone(),
                    retention_floor_seq: target_floor,
                },
            })
        },
    )
    .await
    .map_err(|error: ControlUpdateError| match error {
        ControlUpdateError::LoadHead(error) => {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        }
        other => CoreError::Store(other.to_string()),
    })
}
