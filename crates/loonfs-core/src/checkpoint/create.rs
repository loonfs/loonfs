//! Checkpoint creation: project a manifest from the current manifest plus WAL
//! tail, write its tables, and publish it under one checkpoint record.

use super::build::{
    build_manifest_l0_run_tables, build_manifest_tables, build_manifest_tables_from_rows,
    debug_assert_manifest_table_segments_do_not_overlap, MetadataTableSegmentation,
};
use super::error::ManifestLoadError;
use super::load::{
    head_from_manifest, load_namespace_manifest_envelope_if_present,
    load_verified_manifest_tables_with_cache, validate_direntry_child_bind_index,
    validate_revision_by_inode_desc_index,
};
use super::publish::{
    publish_current_manifest_id, write_namespace_manifest, ManifestPublicationOutcome,
};
use super::row::manifest_rows_for_family;
use super::runs::{
    flatten_manifest_tables, l0_run_count, MetadataLsmPolicy, CHECKPOINT_BASE_RUN_LEVEL,
    CHECKPOINT_TABLE_FAMILIES,
};
use super::scan::VerifiedMetadataTables;
use crate::commit::CommitHeadPublishError;
use crate::context::MutationContext;
use crate::error::MetadataProjectionLoadError;
use crate::error::{CoreError, MetadataViewError};
use crate::metadata::MetadataState;
use crate::namespace::bootstrap::bootstrap_metadata_state;
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::namespace::control::read_head_object;
use crate::wal::{load_validated_wal_chain, project_validated_wal_tail, WalChainLoadRequest};
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::wire::manifest::{
    MetadataRow, MetadataTableFamily, NamespaceCheckpointRecord, NamespaceManifestEnvelope,
    NamespaceManifestPayload,
};
use loonfs_api::{
    generate_checkpoint_id, ChangeSeq, CreateCheckpointResponse, ManifestId, NamespaceId,
};
use loonfs_objectstore::keys::namespace_manifest;
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeMap;
use tracing::Instrument;

#[cfg(test)]
use super::load::{append_rows_to_metadata, load_manifest_materialization_for_inspection};
#[cfg(test)]
use crate::metadata::MetadataStateBuilder;

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
    // Checkpoint creation pins a manifest version. It records the checkpoint in
    // manifest state, using the current manifest tables plus the visible WAL
    // tail as the row source.
    //
    // This is distinct from rebuilding a whole namespace state from scratch:
    // checkpoint creation projects from the current manifest tables plus the
    // visible WAL tail. It only writes new SSTs when the bootstrap seed, WAL
    // tail, or L0 policy requires new metadata files.
    let checkpoint_id = generate_checkpoint_id();
    let mut saw_head_cas_race = false;
    for _publication_attempt in 0..CHECKPOINT_PUBLICATION_RETRY_LIMIT {
        let projection = load_checkpoint_projection(store, namespace_id)
            .instrument(tracing::info_span!(
                "loon.phase",
                phase = "scan_namespace_state"
            ))
            .await?;
        let head_seq = projection.head.seq;
        let current_manifest = projection.manifest_tables.manifest();
        if current_manifest.payload.head_seq == head_seq {
            if let Some(checkpoint) = checkpoint_record_for_manifest(current_manifest) {
                return Ok(CreateCheckpointResponse {
                    namespace_id: namespace_id.clone(),
                    checkpoint_id: checkpoint.checkpoint_id.clone(),
                    checkpoint_seq: checkpoint.head_seq,
                    manifest_id: checkpoint.manifest_id,
                    current_manifest_id: projection.head.current_manifest_id,
                    latest_checkpoint_id: projection.head.latest_checkpoint_id.clone(),
                });
            }
        }

        let mut manifest_id = next_manifest_id(&projection.head)?;
        let mut manifest_ready = false;
        for _allocation_attempt in 0..MANIFEST_ALLOCATION_RETRY_LIMIT {
            let checkpoint_record = NamespaceCheckpointRecord {
                checkpoint_id: checkpoint_id.clone(),
                manifest_id,
                head_seq,
                head_commit_id: projection.head.head_commit_id.clone(),
                created_at_ms: context.now_ms,
                expires_at_ms: None,
                name: None,
            };

            let manifest_key = namespace_manifest(namespace_id.as_str(), manifest_id);
            match load_namespace_manifest_envelope_if_present(
                store,
                namespace_id,
                manifest_id,
                &manifest_key,
            )
            .await
            {
                Ok(Some(existing_manifest)) => {
                    if checkpoint_record_by_id(&existing_manifest, &checkpoint_id).is_some() {
                        manifest_ready = true;
                        break;
                    }
                    manifest_id = next_manifest_id_after(manifest_id)?;
                    continue;
                }
                Ok(None) => {
                    let manifest = build_namespace_manifest_for_checkpoint_projection(
                        store,
                        namespace_id,
                        &projection,
                        &context.writer_version,
                        policy,
                        manifest_id,
                        Some(checkpoint_record),
                    )
                    .await?;

                    // `write_namespace_manifest` owns the idempotent "manifest
                    // already exists" path. It accepts a conflict only when the
                    // existing manifest has the same payload checksum. A
                    // same-id/different-payload conflict means another writer
                    // won this allocation slot, so try the next manifest id.
                    let write_result = write_namespace_manifest(store, &manifest).await;
                    match write_result {
                        Ok(()) => {}
                        Err(MetadataProjectionLoadError::ManifestLoad(
                            ManifestLoadError::ManifestConflict { .. },
                        )) => {
                            manifest_id = next_manifest_id_after(manifest_id)?;
                            continue;
                        }
                        Err(error) => return Err(CoreError::MetadataProjection(error)),
                    }
                    manifest_ready = true;
                    break;
                }
                Err(error) => {
                    return Err(CoreError::MetadataProjection(
                        MetadataProjectionLoadError::ManifestLoad(error),
                    ))
                }
            }
        }
        if !manifest_ready {
            return Err(CoreError::Store(
                "manifest id allocation retry exhausted".to_owned(),
            ));
        }

        let manifest_key = namespace_manifest(namespace_id.as_str(), manifest_id);
        let manifest = load_namespace_manifest_envelope_if_present(
            store,
            namespace_id,
            manifest_id,
            &manifest_key,
        )
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?
        .ok_or_else(|| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(
                ManifestLoadError::MissingManifest {
                    object_key: manifest_key.clone(),
                },
            ))
        })?;
        let checkpoint = checkpoint_record_by_id(&manifest, &checkpoint_id).ok_or_else(|| {
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

struct CheckpointProjection<'a, S: ObjectStore + ?Sized> {
    head: HeadState,
    manifest_tables: VerifiedMetadataTables<'a, S>,
    tail_state: MetadataState,
}

async fn load_checkpoint_projection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
) -> Result<CheckpointProjection<'a, S>, CoreError> {
    load_namespace_catalog_entry(store, namespace_id)
        .await
        .map_err(|error| CoreError::MetadataProjection(error.into()))?;
    let loaded_head = read_head_object(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let head = loaded_head.envelope.state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    let manifest_id =
        head.current_manifest_id
            .ok_or_else(|| MetadataViewError::MissingManifest {
                namespace_id: namespace_id.clone(),
            })?;
    let manifest_tables =
        load_verified_manifest_tables_with_cache(store, None, namespace_id, manifest_id)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
    let manifest_head = head_from_manifest(&head, manifest_tables.manifest());
    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: manifest_head.seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: None,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    let replayed = {
        let _span = tracing::info_span!("loon.phase", phase = "project_metadata_state").entered();
        project_validated_wal_tail(
            &manifest_head,
            &MetadataState::default(),
            Some(head.writer_epoch),
            &wal_chain,
        )
        .map_err(MetadataProjectionLoadError::WalReplay)
        .map_err(CoreError::MetadataProjection)?
    };
    ensure_checkpoint_reconstructed_head_matches(&head, &replayed.resulting_head)?;
    Ok(CheckpointProjection {
        head,
        manifest_tables,
        tail_state: replayed.resulting_metadata_state,
    })
}

#[cfg(test)]
pub(super) async fn load_checkpoint_projection_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(HeadState, MetadataState), CoreError> {
    let projection = load_checkpoint_projection(store, namespace_id).await?;
    let mut metadata_state = MetadataStateBuilder::default();
    for family in CHECKPOINT_TABLE_FAMILIES {
        let mut rows = projection
            .manifest_tables
            .scan_prefix(family, "")
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
        rows.extend(manifest_rows_for_family(&projection.tail_state, family));
        rows.sort_by_key(|row| row.row_key_for_family(family));
        append_rows_to_metadata(&mut metadata_state, family, "checkpoint projection", &rows)
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
    }
    Ok((projection.head, metadata_state.finish()))
}

fn ensure_checkpoint_reconstructed_head_matches(
    current_head: &HeadState,
    reconstructed: &HeadState,
) -> Result<(), CoreError> {
    if current_head.namespace_id != reconstructed.namespace_id
        || current_head.seq != reconstructed.seq
        || current_head.head_commit_id != reconstructed.head_commit_id
        || current_head.next_inode_id != reconstructed.next_inode_id
        || current_head.name_policy != reconstructed.name_policy
        || current_head.current_manifest_id != reconstructed.current_manifest_id
        || current_head.latest_checkpoint_id != reconstructed.latest_checkpoint_id
        || current_head.retention_floor_seq != reconstructed.retention_floor_seq
        || (reconstructed.visible_wal_tip.is_some()
            && current_head.visible_wal_tip != reconstructed.visible_wal_tip)
    {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::ReplayedHeadMismatch {
                expected: Box::new(current_head.clone()),
                actual: Box::new(reconstructed.clone()),
            },
        ));
    }
    Ok(())
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

pub(crate) async fn build_initial_namespace_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    initial_head: &HeadState,
    writer_version: &str,
) -> Result<NamespaceManifestEnvelope, CoreError> {
    let manifest_id = ManifestId(initial_head.seq.0);
    let metadata_state = bootstrap_metadata_state();
    let run_tables = build_manifest_tables(
        store,
        namespace_id,
        initial_head.seq,
        CHECKPOINT_BASE_RUN_LEVEL,
        &metadata_state,
        writer_version,
        MetadataLsmPolicy::default().max_rows_per_segment,
    )
    .await?;
    debug_assert_manifest_table_segments_do_not_overlap(&run_tables);

    NamespaceManifestEnvelope::from_payload(
        writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id,
            head_seq: initial_head.seq,
            head_commit_id: initial_head.head_commit_id.clone(),
            base_seq: initial_head.seq,
            writer_epoch: initial_head.writer_epoch,
            next_inode_id: initial_head.next_inode_id,
            name_policy: initial_head.name_policy,
            retention_floor_seq: initial_head.retention_floor_seq,
            initialized: true,
            verified: true,
            fork: None,
            checkpoints: Vec::new(),
            features: BTreeMap::new(),
            metadata_files: flatten_manifest_tables(run_tables),
        },
    )
    .map_err(|err| CoreError::Store(err.to_string()))
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "project_manifest")
)]
async fn build_namespace_manifest_for_checkpoint_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &CheckpointProjection<'_, S>,
    writer_version: &str,
    policy: MetadataLsmPolicy,
    manifest_id: ManifestId,
    checkpoint_to_add: Option<NamespaceCheckpointRecord>,
) -> Result<NamespaceManifestEnvelope, CoreError> {
    let head_seq = projection.head.seq;
    let previous_manifest = projection.manifest_tables.manifest();

    let mut checkpoints = previous_manifest.payload.checkpoints.clone();
    if let Some(checkpoint) = checkpoint_to_add {
        checkpoints.push(checkpoint);
    }

    let (base_seq, metadata_files) = if is_bootstrap_seed_manifest(&previous_manifest.payload) {
        let run_tables = build_base_manifest_tables_from_projection(
            store,
            namespace_id,
            head_seq,
            projection,
            writer_version,
            policy.max_rows_per_segment,
        )
        .await?;
        debug_assert_manifest_table_segments_do_not_overlap(&run_tables);
        (head_seq, flatten_manifest_tables(run_tables))
    } else if l0_run_count(&previous_manifest.payload) < policy.max_l0_runs {
        let mut metadata_files = previous_manifest.payload.metadata_files.clone();
        if previous_manifest.payload.head_seq < head_seq {
            metadata_files.extend(flatten_manifest_tables(
                build_manifest_l0_run_tables(
                    store,
                    namespace_id,
                    head_seq,
                    previous_manifest.payload.head_seq,
                    &projection.tail_state,
                    writer_version,
                )
                .await?,
            ));
        }
        (previous_manifest.payload.base_seq, metadata_files)
    } else {
        let run_tables = build_base_manifest_tables_from_projection(
            store,
            namespace_id,
            head_seq,
            projection,
            writer_version,
            policy.max_rows_per_segment,
        )
        .await?;
        debug_assert_manifest_table_segments_do_not_overlap(&run_tables);
        (head_seq, flatten_manifest_tables(run_tables))
    };

    NamespaceManifestEnvelope::from_payload(
        writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id,
            head_seq,
            head_commit_id: projection.head.head_commit_id.clone(),
            base_seq,
            writer_epoch: projection.head.writer_epoch,
            next_inode_id: projection.head.next_inode_id,
            name_policy: projection.head.name_policy,
            retention_floor_seq: projection.head.retention_floor_seq,
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

async fn build_base_manifest_tables_from_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    run_seq: ChangeSeq,
    projection: &CheckpointProjection<'_, S>,
    writer_version: &str,
    max_rows_per_segment: usize,
) -> Result<Vec<super::runs::MetadataTableManifest>, CoreError> {
    let mut rows_by_family = BTreeMap::<MetadataTableFamily, Vec<MetadataRow>>::new();
    for family in CHECKPOINT_TABLE_FAMILIES {
        let mut rows = projection
            .manifest_tables
            .scan_prefix(family, "")
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
        rows.extend(manifest_rows_for_family(&projection.tail_state, family));
        rows.sort_by_key(|row| row.row_key_for_family(family));
        rows_by_family.insert(family, rows);
    }

    // The base rebuild is the one production point that holds every row of
    // every family, so cross-check the index families against their
    // canonical tables before compacting them forward (format spec,
    // "Manifest publication and checkpoint verification").
    let source_manifest_key = namespace_manifest(
        namespace_id.as_str(),
        projection.manifest_tables.manifest().payload.manifest_id,
    );
    let index_error =
        |error| CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error));
    validate_direntry_child_bind_index(
        &source_manifest_key,
        rows_by_family
            .get(&MetadataTableFamily::DirentryBinds)
            .cloned()
            .unwrap_or_default(),
        rows_by_family
            .get(&MetadataTableFamily::DirentryChildBinds)
            .cloned()
            .unwrap_or_default(),
    )
    .map_err(index_error)?;
    validate_revision_by_inode_desc_index(
        &source_manifest_key,
        rows_by_family
            .get(&MetadataTableFamily::Revisions)
            .cloned()
            .unwrap_or_default(),
        rows_by_family
            .get(&MetadataTableFamily::RevisionsByInodeDesc)
            .cloned()
            .unwrap_or_default(),
    )
    .map_err(index_error)?;

    build_manifest_tables_from_rows(
        store,
        namespace_id,
        run_seq,
        CHECKPOINT_BASE_RUN_LEVEL,
        writer_version,
        |family| rows_by_family.remove(&family).unwrap_or_default(),
        MetadataTableSegmentation::Base {
            max_rows_per_segment,
        },
    )
    .await
}

#[cfg(test)]
pub(super) struct ManifestMetadataSource<'a> {
    pub(super) head: &'a HeadState,
    pub(super) metadata_state: &'a MetadataState,
}

#[cfg(test)]
#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "project_manifest")
)]
pub(super) async fn build_namespace_manifest_from_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    source: ManifestMetadataSource<'_>,
    writer_version: &str,
    policy: MetadataLsmPolicy,
    manifest_id: ManifestId,
    checkpoint_to_add: Option<NamespaceCheckpointRecord>,
) -> Result<NamespaceManifestEnvelope, CoreError> {
    let head = source.head;
    let metadata_state = source.metadata_state;
    let head_seq = head.seq;
    let previous_manifest = match head.current_manifest_id {
        Some(previous_id) => Some(
            load_manifest_materialization_for_inspection(store, namespace_id, previous_id)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
                })?,
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
        Some(previous) if is_bootstrap_seed_manifest(&previous.manifest.payload) => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
                writer_version,
                policy.max_rows_per_segment,
            )
            .await?;
            debug_assert_manifest_table_segments_do_not_overlap(&run_tables);
            (head_seq, flatten_manifest_tables(run_tables))
        }
        Some(previous) if l0_run_count(&previous.manifest.payload) < policy.max_l0_runs => {
            let mut metadata_files = previous.manifest.payload.metadata_files.clone();
            if previous.manifest.payload.head_seq < head_seq {
                metadata_files.extend(flatten_manifest_tables(
                    build_manifest_l0_run_tables(
                        store,
                        namespace_id,
                        head_seq,
                        previous.manifest.payload.head_seq,
                        metadata_state,
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
                metadata_state,
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
                metadata_state,
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
            head_commit_id: head.head_commit_id.clone(),
            base_seq,
            writer_epoch: head.writer_epoch,
            next_inode_id: head.next_inode_id,
            name_policy: head.name_policy,
            retention_floor_seq: head.retention_floor_seq,
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

fn is_bootstrap_seed_manifest(payload: &NamespaceManifestPayload) -> bool {
    payload.head_seq == ChangeSeq(0)
        && payload.base_seq == ChangeSeq(0)
        && payload.checkpoints.is_empty()
        && payload.fork.is_none()
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
    checkpoint_id: &loonfs_api::CheckpointId,
) -> Option<&'a NamespaceCheckpointRecord> {
    manifest
        .payload
        .checkpoints
        .iter()
        .find(|checkpoint| &checkpoint.checkpoint_id == checkpoint_id)
}
