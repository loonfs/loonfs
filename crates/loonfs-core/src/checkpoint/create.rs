//! Checkpoint creation: project a manifest from the current verified
//! basis, write its tables, and publish it under one checkpoint record.

use super::build::{
    build_manifest_l0_run_tables, build_manifest_tables,
    debug_assert_manifest_table_segments_do_not_overlap,
};
use super::error::ManifestLoadError;
use super::load::{
    load_manifest_materialization_from_manifest, load_verified_manifest_materialization,
    load_verified_manifest_materialization_if_present,
};
use super::publish::{
    publish_current_manifest_id, write_namespace_manifest, ManifestPublicationOutcome,
};
use super::row::metadata_states_equivalent;
use super::runs::{
    flatten_manifest_tables, l0_run_count, MetadataLsmPolicy, CHECKPOINT_BASE_RUN_LEVEL,
};
use crate::commit::CommitHeadPublishError;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::namespace::basis::{load_verified_namespace_basis, BasisLoadError};
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::manifest::{
    NamespaceCheckpointRecord, NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::{generate_checkpoint_id, CreateCheckpointResponse, ManifestId, NamespaceId};
use loonfs_objectstore::keys::namespace_manifest;
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeMap;
use tracing::Instrument;

// Manifest id allocation can race with other manifest publishers. Exhausting
// this loop means the candidate id range was already occupied.
pub(super) const MANIFEST_ALLOCATION_RETRY_LIMIT: usize = 8;
// Checkpoint publication can race with another manifest update. Retrying here
// preserves one checkpoint id while rebasing its checkpoint record onto the
// newest current manifest.
pub(super) const CHECKPOINT_PUBLICATION_RETRY_LIMIT: usize = 8;

pub(crate) async fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<CreateCheckpointResponse, CoreError> {
    create_checkpoint_with_policy(store, namespace_id, context, MetadataLsmPolicy::default()).await
}

pub(crate) async fn create_checkpoint_with_policy<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> Result<CreateCheckpointResponse, CoreError> {
    // Checkpoint creation pins a manifest version. If the current head does not
    // yet have a manifest, this first publishes a manifest for the current
    // durable namespace file set, then records the checkpoint in manifest state.
    //
    // This is distinct from metadata compaction: creating a checkpoint should
    // not rewrite SSTs unless a manifest must be materialized for an otherwise
    // unmanifested head or the L0 run policy requires a full materialization.
    let checkpoint_id = generate_checkpoint_id();
    let mut saw_head_cas_race = false;
    for _publication_attempt in 0..CHECKPOINT_PUBLICATION_RETRY_LIMIT {
        let basis = load_verified_namespace_basis(store, namespace_id)
            .instrument(tracing::info_span!(
                "loon.phase",
                phase = "scan_namespace_state"
            ))
            .await?;
        let head_seq = basis.head.seq;
        if let Some(current_manifest_id) = basis.head.current_manifest_id {
            let materialized =
                load_verified_manifest_materialization(store, namespace_id, current_manifest_id)
                    .await
                    .map_err(|error| CoreError::Basis(BasisLoadError::ManifestLoad(error)))?;
            if materialized.manifest.payload.head_seq == head_seq {
                if let Some(checkpoint) = checkpoint_record_for_manifest(&materialized.manifest) {
                    return Ok(CreateCheckpointResponse {
                        namespace_id: namespace_id.clone(),
                        checkpoint_id: checkpoint.checkpoint_id.clone(),
                        checkpoint_seq: checkpoint.head_seq,
                        manifest_id: checkpoint.manifest_id,
                        current_manifest_id: basis.head.current_manifest_id,
                        latest_checkpoint_id: basis.head.latest_checkpoint_id.clone(),
                    });
                }
            }
        }

        let mut manifest_id = next_manifest_id(&basis.head)?;
        let mut manifest_ready = false;
        for _allocation_attempt in 0..MANIFEST_ALLOCATION_RETRY_LIMIT {
            let checkpoint_record = NamespaceCheckpointRecord {
                checkpoint_id: checkpoint_id.clone(),
                manifest_id,
                head_seq,
                head_commit_id: basis.head.head_commit_id.clone(),
                created_at_ms: context.now_ms,
                expires_at_ms: None,
                name: None,
            };

            match load_verified_manifest_materialization_if_present(
                store,
                namespace_id,
                manifest_id,
            )
            .await
            {
                Ok(Some(materialized)) => {
                    if checkpoint_record_by_id(&materialized.manifest, &checkpoint_id).is_some() {
                        manifest_ready = true;
                        break;
                    }
                    manifest_id = next_manifest_id_after(manifest_id)?;
                    continue;
                }
                Ok(None) => {
                    let manifest = build_namespace_manifest_for_basis(
                        store,
                        namespace_id,
                        &basis,
                        &context.writer_version,
                        policy,
                        manifest_id,
                        Some(checkpoint_record),
                    )
                    .await?;
                    let materialized = load_manifest_materialization_from_manifest(
                        store,
                        namespace_id,
                        &namespace_manifest(namespace_id.as_str(), manifest_id),
                        &manifest,
                    )
                    .await
                    .map_err(|error| CoreError::Basis(BasisLoadError::ManifestLoad(error)))?;
                    if !metadata_states_equivalent(&basis.metadata_state, &materialized) {
                        return Err(CoreError::Basis(BasisLoadError::ManifestLoad(
                            ManifestLoadError::MetadataMismatch,
                        )));
                    }

                    // `write_namespace_manifest` owns the idempotent "manifest
                    // already exists" path. It accepts a conflict only when the
                    // existing manifest has the same payload checksum. A
                    // same-id/different-payload conflict means another writer
                    // won this allocation slot, so try the next manifest id.
                    let write_result = write_namespace_manifest(store, &manifest).await;
                    match write_result {
                        Ok(()) => {}
                        Err(BasisLoadError::ManifestLoad(
                            ManifestLoadError::ManifestConflict { .. },
                        )) => {
                            manifest_id = next_manifest_id_after(manifest_id)?;
                            continue;
                        }
                        Err(error) => return Err(CoreError::Basis(error)),
                    }
                    manifest_ready = true;
                    break;
                }
                Err(error) => return Err(CoreError::Basis(BasisLoadError::ManifestLoad(error))),
            }
        }
        if !manifest_ready {
            return Err(CoreError::Store(
                "manifest id allocation retry exhausted".to_owned(),
            ));
        }

        let materialized = load_verified_manifest_materialization(store, namespace_id, manifest_id)
            .await
            .map_err(|error| CoreError::Basis(BasisLoadError::ManifestLoad(error)))?;
        let checkpoint = checkpoint_record_by_id(&materialized.manifest, &checkpoint_id)
            .ok_or_else(|| {
                CoreError::CheckpointUnavailable(format!(
                    "namespace `{}` manifest {:?} has no checkpoint record `{checkpoint_id}`",
                    namespace_id.as_str(),
                    manifest_id
                ))
            })?;
        match publish_current_manifest_id(
            store,
            namespace_id,
            manifest_id,
            &checkpoint.checkpoint_id,
            &context.writer_version,
        )
        .await?
        {
            ManifestPublicationOutcome::Published(resulting_head) => {
                let resulting_head = *resulting_head;
                return Ok(CreateCheckpointResponse {
                    namespace_id: namespace_id.clone(),
                    checkpoint_id: checkpoint.checkpoint_id.clone(),
                    checkpoint_seq: checkpoint.head_seq,
                    manifest_id: checkpoint.manifest_id,
                    current_manifest_id: resulting_head.current_manifest_id,
                    latest_checkpoint_id: resulting_head.latest_checkpoint_id,
                });
            }
            ManifestPublicationOutcome::CurrentManifestMissingCheckpoint { .. } => {
                // Another manifest publisher won first. Rebuild the checkpoint
                // record on top of the new current manifest using the same
                // checkpoint id so the operation remains idempotent.
                continue;
            }
            ManifestPublicationOutcome::HeadCasRaceLost => {
                saw_head_cas_race = true;
                continue;
            }
        }
    }

    if saw_head_cas_race {
        Err(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
    } else {
        Err(CoreError::CheckpointUnavailable(
            "checkpoint publication retry exhausted".to_owned(),
        ))
    }
}

pub(super) fn next_manifest_id(head: &HeadState) -> Result<ManifestId, CoreError> {
    head.current_manifest_id
        .map(next_manifest_id_after)
        .unwrap_or_else(|| Ok(ManifestId(head.seq.0)))
}

pub(super) fn next_manifest_id_after(current: ManifestId) -> Result<ManifestId, CoreError> {
    current
        .0
        .checked_add(1)
        .map(ManifestId)
        .ok_or_else(|| CoreError::Store("manifest id overflow".to_owned()))
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "project_manifest")
)]
pub(super) async fn build_namespace_manifest_for_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: &crate::namespace::basis::VerifiedNamespaceBasis,
    writer_version: &str,
    policy: MetadataLsmPolicy,
    manifest_id: ManifestId,
    checkpoint_to_add: Option<NamespaceCheckpointRecord>,
) -> Result<NamespaceManifestEnvelope, CoreError> {
    let head_seq = basis.head.seq;
    let previous_manifest = match basis.head.current_manifest_id {
        Some(previous_id) => Some(
            load_verified_manifest_materialization(store, namespace_id, previous_id)
                .await
                .map_err(|error| CoreError::Basis(BasisLoadError::ManifestLoad(error)))?,
        ),
        _ => None,
    };

    let mut checkpoints = previous_manifest
        .as_ref()
        .map(|previous| previous.manifest.payload.checkpoints.clone())
        .unwrap_or_default();
    if let Some(checkpoint) = checkpoint_to_add {
        checkpoints.push(checkpoint);
    }

    let (base_seq, metadata_files) = match previous_manifest {
        Some(previous) if l0_run_count(&previous.manifest.payload) < policy.max_l0_runs => {
            let mut metadata_files = previous.manifest.payload.metadata_files.clone();
            if previous.manifest.payload.head_seq < head_seq {
                metadata_files.extend(flatten_manifest_tables(
                    build_manifest_l0_run_tables(
                        store,
                        namespace_id,
                        head_seq,
                        previous.manifest.payload.head_seq,
                        &basis.metadata_state,
                        writer_version,
                    )
                    .await?,
                ));
            }
            (previous.manifest.payload.base_seq, metadata_files)
        }
        Some(_) => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                &basis.metadata_state,
                writer_version,
                policy.max_rows_per_segment,
            )
            .await?;
            debug_assert_manifest_table_segments_do_not_overlap(&run_tables);
            (head_seq, flatten_manifest_tables(run_tables))
        }
        _ => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                &basis.metadata_state,
                writer_version,
                policy.max_rows_per_segment,
            )
            .await?;
            (head_seq, flatten_manifest_tables(run_tables))
        }
    };

    NamespaceManifestEnvelope::from_payload(
        writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id,
            head_seq,
            head_commit_id: basis.head.head_commit_id.clone(),
            base_seq,
            active_fence_token: basis.head.active_fence_token,
            next_inode_id: basis.head.next_inode_id,
            name_policy: basis.head.name_policy,
            retention_floor_seq: basis.head.retention_floor_seq,
            initialized: true,
            verified: true,
            fork: None,
            checkpoints,
            features: BTreeMap::new(),
            metadata_files,
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))
}

pub(super) fn checkpoint_record_for_manifest(
    manifest: &NamespaceManifestEnvelope,
) -> Option<&NamespaceCheckpointRecord> {
    manifest.payload.checkpoints.iter().find(|checkpoint| {
        checkpoint.head_seq == manifest.payload.head_seq
            && checkpoint.manifest_id == manifest.payload.manifest_id
    })
}

pub(super) fn checkpoint_record_by_id<'a>(
    manifest: &'a NamespaceManifestEnvelope,
    checkpoint_id: &str,
) -> Option<&'a NamespaceCheckpointRecord> {
    manifest
        .payload
        .checkpoints
        .iter()
        .find(|checkpoint| checkpoint.checkpoint_id == checkpoint_id)
}
