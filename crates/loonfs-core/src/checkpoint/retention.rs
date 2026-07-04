//! Retention floor advancement against the current verified manifest.

use super::load::load_verified_manifest_tables;
use super::publish::HEAD_CAS_RETRY_LIMIT;
use crate::context::MutationContext;
use crate::control_update::{update_head, HeadUpdate};
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, MetadataViewError};
use crate::namespace::control::read_head_object;
use loonfs_api::wire::control::HeadState;
use loonfs_api::{AdvanceRetentionResponse, NamespaceId};
use loonfs_objectstore::ObjectStore;

const MAX_RETENTION_PROBE_IO: usize = 8;

pub(crate) async fn advance_retention_floor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AdvanceRetentionResponse, CoreError> {
    // Derive the target floor from the currently published manifest before
    // entering the head update. The closure refuses to publish if the
    // current manifest changes mid-update: manifest ids do not order
    // coverage, so a newer id is no promise of newer history. The caller
    // retries and re-derives from whatever manifest won.
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
    if loaded_head.envelope.state.retention_floor_seq >= target_floor {
        // Already advanced; skip the existence probes on the idempotent
        // re-invocation path.
        return Ok(AdvanceRetentionResponse {
            namespace_id: namespace_id.clone(),
            retention_floor_seq: loaded_head.envelope.state.retention_floor_seq,
        });
    }

    // Advancing the floor surrenders the WAL replay promise below the
    // target, so every metadata segment the target manifest references must
    // still exist before that promise is given up. This probe is advisory
    // defense-in-depth: the atomic guarantee belongs to the deleter — GC
    // must never remove an object reachable from the current manifest or a
    // retained checkpoint or pin (format spec, "Garbage collection").
    // Corruption discovered later is caught by read-path checksums.
    for metadata_files in manifest_tables
        .manifest()
        .payload
        .metadata_files
        .chunks(MAX_RETENTION_PROBE_IO)
    {
        let probes = metadata_files.iter().map(|metadata_file| async move {
            let present = store
                .head(&metadata_file.object_key)
                .await
                .map_err(|error| CoreError::Store(error.to_string()))?
                .is_some();
            if present {
                Ok(())
            } else {
                Err(CoreError::CheckpointUnavailable(format!(
                    "retention floor cannot advance: missing metadata segment `{}`",
                    metadata_file.object_key
                )))
            }
        });
        futures::future::try_join_all(probes).await?;
    }

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

            // The floor above was derived from `current_manifest_id`. Only
            // publish it while that manifest is still current; any change
            // aborts so the caller re-derives against the winner.
            if head.current_manifest_id != Some(current_manifest_id) {
                return Err(CoreError::CheckpointUnavailable(format!(
                    "current manifest changed during retention advance (floor was derived from {current_manifest_id:?}); retry"
                )));
            }

            // Maintenance head update.
            //
            // Advancing retention_floor_seq is a metadata-retention decision. It
            // preserves writer_epoch and the writer block and is serialized by
            // head CAS. It must not make new WAL visible and must not delete
            // checkpoint-reachable files by itself.
            let next_head = HeadState {
                namespace_id: head.namespace_id.clone(),
                seq: head.seq,
                head_commit_id: head.head_commit_id.clone(),
                writer_epoch: head.writer_epoch,
                writer: head.writer.clone(),
                next_inode_id: head.next_inode_id,
                name_policy: head.name_policy,
                current_manifest_id: head.current_manifest_id,
                latest_checkpoint_id: head.latest_checkpoint_id.clone(),
                retention_floor_seq: target_floor,
                visible_wal_tip: head.visible_wal_tip.clone(),
                recent_segments: head.recent_segments.clone(),
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
}
