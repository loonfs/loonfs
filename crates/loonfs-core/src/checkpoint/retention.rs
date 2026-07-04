//! Retention floor advancement against the current verified manifest.

use super::load::load_verified_manifest_tables;
use super::publish::ROOT_CAS_RETRY_LIMIT;
use crate::context::MutationContext;
use crate::control_update::{update_head, HeadUpdate};
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::namespace::control::{read_head_and_metadata_root, read_metadata_root_object};
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
    let (loaded_head, loaded_root) = read_head_and_metadata_root(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let current_manifest_id = loaded_root.envelope.state.manifest_id;
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

    // The floor was derived from the root's manifest. Re-check that the
    // root still references it just before publishing: a mid-flight root
    // move aborts so the caller re-derives against the winner. (The head
    // CAS cannot guard the root, which is its own object; monotonic root
    // publication plus rebuild-from-current-floor keeps the race benign in
    // the conservative direction.)
    let root_recheck = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    if root_recheck.envelope.state.manifest_id != current_manifest_id {
        return Err(CoreError::CheckpointUnavailable(format!(
            "current manifest changed during retention advance (floor was derived from {current_manifest_id:?}); retry"
        )));
    }

    update_head(
        store,
        namespace_id,
        &context.writer_version,
        ROOT_CAS_RETRY_LIMIT,
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
