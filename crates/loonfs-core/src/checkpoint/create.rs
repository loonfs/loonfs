//! Checkpoint creation: project a manifest from the current manifest plus WAL
//! tail, write its tables, and publish it under one checkpoint record.

use super::build::{
    build_manifest_l0_run_tables, build_manifest_tables,
    debug_assert_manifest_table_segments_do_not_overlap,
};
use super::error::ManifestLoadError;
use super::load::{
    head_from_manifest, load_namespace_manifest_envelope_if_present, load_verified_manifest_tables,
};
use super::publish::{publish_metadata_root, write_namespace_manifest, ManifestPublicationOutcome};
use super::record::{
    deterministic_checkpoint_id, set_checkpoint_record_state, verify_checkpoint_basis,
    write_checkpoint_record, CheckpointRecordWrite,
};
#[cfg(test)]
use super::row::manifest_rows_for_family;
use super::runs::{flatten_manifest_tables, MetadataLsmPolicy, CHECKPOINT_BASE_RUN_LEVEL};
#[cfg(test)]
use super::runs::{l0_run_count, CHECKPOINT_TABLE_FAMILIES};
use super::scan::VerifiedMetadataTables;
use crate::commit::CommitHeadPublishError;
use crate::context::MutationContext;
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::error::Result;
use crate::metadata::MetadataState;
use crate::namespace::bootstrap::bootstrap_metadata_state;
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::namespace::control::{read_head_and_metadata_root, read_wal_floor_seq_or_zero};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use crate::wal::{load_validated_wal_chain, project_validated_wal_tail, WalChainLoadRequest};
use loonfs_api::wire::control::NamespaceState;
use loonfs_api::wire::control::{CheckpointRecordLifecycle, CheckpointRecordState};
use loonfs_api::wire::control::{HeadState, MetadataRootState};
use loonfs_api::wire::manifest::{
    MetadataRow, MetadataTableFamily, NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::{ChangeSeq, CreateCheckpointResponse, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};
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

pub(crate) const CHECKPOINT_VERIFY_BUDGET_MS: u64 = 60_000;

struct CheckpointBasis {
    manifest_id: ManifestId,
    manifest_object_id: ManifestObjectId,
    manifest_head_seq: ChangeSeq,
    manifest_payload_checksum: String,
}

pub(crate) async fn create_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<CreateCheckpointResponse> {
    // Checkpoint creation pins a manifest version as a first-class record
    // under `checkpoints/`, write-then-verify (format spec, "Checkpoints"):
    //
    // 1. Choose a basis manifest that the metadata root references,
    //    materializing and publishing one first when the root lags the head.
    // 2. Write `checkpoints/{id}.json` with state = active.
    // 3. Verify, after the write is durable, that the floor has not passed
    //    the basis and the basis manifest still loads, under the verify
    //    budget.
    // 4. On verification failure, flip the record to dead and retry against
    //    a newer basis.
    let mut saw_root_cas_race = false;
    for _publication_attempt in 0..CHECKPOINT_PUBLICATION_RETRY_LIMIT {
        let projection = load_checkpoint_projection(store, namespace_id)
            .instrument(tracing::info_span!(
                "loon.phase",
                phase = "scan_namespace_state"
            ))
            .await?;
        let head_seq = projection.head.seq;

        let basis = if projection.root.manifest_head_seq == head_seq {
            CheckpointBasis {
                manifest_id: projection.root.manifest_id,
                manifest_object_id: projection.root.manifest_object_id.clone(),
                manifest_head_seq: projection.root.manifest_head_seq,
                manifest_payload_checksum: projection.root.manifest_payload_checksum.clone(),
            }
        } else {
            let manifest_id = next_manifest_id_after(projection.root.manifest_id)?;
            let mut written_manifest = None;
            for _allocation_attempt in 0..MANIFEST_ALLOCATION_RETRY_LIMIT {
                let manifest_object_id = ManifestObjectId::generate(manifest_id);
                let manifest_key =
                    metadata_manifest_object(namespace_id.as_str(), &manifest_object_id);
                match load_namespace_manifest_envelope_if_present(
                    store,
                    namespace_id,
                    &manifest_object_id,
                    &manifest_key,
                )
                .await
                {
                    Ok(Some(_existing)) => {
                        // Physical-id collision or retry; keep the logical
                        // slot and generate another object id.
                        continue;
                    }
                    Ok(None) => {
                        let manifest = build_namespace_manifest_for_checkpoint_projection(
                            store,
                            namespace_id,
                            &projection,
                            &context.writer_version,
                            manifest_id,
                            manifest_object_id,
                        )
                        .await?;
                        // `write_namespace_manifest` owns the idempotent
                        // "manifest already exists" path. A same-id/
                        // different-payload conflict means another writer won
                        // this slot, so try the next manifest id.
                        match write_namespace_manifest(store, &manifest).await {
                            Ok(()) => {}
                            Err(MetadataProjectionLoadError::ManifestLoad(
                                ManifestLoadError::ManifestConflict { .. },
                            )) => {
                                continue;
                            }
                            Err(error) => return Err(CoreError::MetadataProjection(error)),
                        }
                        written_manifest = Some(manifest);
                        break;
                    }
                    Err(error) => {
                        return Err(CoreError::MetadataProjection(
                            MetadataProjectionLoadError::ManifestLoad(error),
                        ))
                    }
                }
            }
            let Some(manifest) = written_manifest else {
                return Err(CoreError::Internal(
                    "manifest id allocation retry exhausted".to_owned(),
                ));
            };
            // Advance the root for readers. A superseded outcome is fine:
            // the manifest we wrote stays durable and valid as a checkpoint
            // basis even when a newer root already won.
            match publish_metadata_root(
                store,
                namespace_id,
                &manifest,
                context.now_ms,
                &context.writer_version,
            )
            .await?
            {
                ManifestPublicationOutcome::Published(_)
                | ManifestPublicationOutcome::Superseded(_) => {}
                ManifestPublicationOutcome::RootCasRaceLost => {
                    saw_root_cas_race = true;
                    continue;
                }
            }
            CheckpointBasis {
                manifest_id: manifest.payload.manifest_id,
                manifest_object_id: manifest.payload.manifest_object_id.clone(),
                manifest_head_seq: manifest.payload.head_seq,
                manifest_payload_checksum: manifest.payload_checksum.clone(),
            }
        };

        let checkpoint_id = deterministic_checkpoint_id(
            namespace_id,
            basis.manifest_id,
            &basis.manifest_object_id,
            &basis.manifest_payload_checksum,
        );
        let record = CheckpointRecordState {
            checkpoint_id: checkpoint_id.clone(),
            namespace_id: namespace_id.clone(),
            manifest_id: basis.manifest_id,
            manifest_object_id: basis.manifest_object_id.clone(),
            manifest_head_seq: basis.manifest_head_seq,
            manifest_payload_checksum: basis.manifest_payload_checksum.clone(),
            head_commit_id: projection.head.head_commit_id.clone(),
            created_at_ms: context.now_ms,
            expires_at_ms: None,
            name: None,
            state: CheckpointRecordLifecycle::Active,
        };
        let timer = StdMonotonicTimer::default();
        let verify_started_ms = timer.monotonic_now_ms();
        let written = write_checkpoint_record(store, &record, &context.writer_version).await?;

        let verified = verify_checkpoint_basis(store, &record).await?;
        let within_budget = timer.monotonic_now_ms().saturating_sub(verify_started_ms)
            <= CHECKPOINT_VERIFY_BUDGET_MS;
        if verified && within_budget {
            if let CheckpointRecordWrite::Existing(existing) = written {
                // Deterministic ids make re-creation idempotent; a dead
                // record for the same verified basis is revived rather than
                // duplicated.
                if existing.state == CheckpointRecordLifecycle::Dead {
                    set_checkpoint_record_state(
                        store,
                        namespace_id,
                        &checkpoint_id,
                        CheckpointRecordLifecycle::Active,
                        &context.writer_version,
                    )
                    .await?;
                }
            }
            return Ok(CreateCheckpointResponse {
                namespace_id: namespace_id.clone(),
                checkpoint_id,
                checkpoint_seq: basis.manifest_head_seq,
                manifest_id: basis.manifest_id,
                current_manifest_id: Some(basis.manifest_id.max(projection.root.manifest_id)),
            });
        }

        // Overrunning the budget counts as verification failure: the record
        // may have raced the grace window, so it must not stand as a root.
        set_checkpoint_record_state(
            store,
            namespace_id,
            &checkpoint_id,
            CheckpointRecordLifecycle::Dead,
            &context.writer_version,
        )
        .await?;
    }

    if saw_root_cas_race {
        Err(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
    } else {
        Err(CoreError::CheckpointUnavailable(
            "checkpoint publication retry exhausted".to_owned(),
        ))
    }
}

struct CheckpointProjection<'a, S: ObjectStore + ?Sized> {
    head: HeadState,
    root: MetadataRootState,
    floor_seq: ChangeSeq,
    manifest_tables: VerifiedMetadataTables<'a, S>,
    tail_state: MetadataState,
}

async fn load_checkpoint_projection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
) -> Result<CheckpointProjection<'a, S>> {
    load_namespace_catalog_entry(store, namespace_id)
        .await
        .map_err(|error| CoreError::MetadataProjection(error.into()))?;
    let (loaded_head, loaded_root) = read_head_and_metadata_root(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let floor_seq = read_wal_floor_seq_or_zero(store, namespace_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
        })?;
    let head = loaded_head.envelope.state;
    let root = loaded_root.envelope.state;
    if head.state == NamespaceState::Deleted {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    let manifest_tables =
        load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
    if manifest_tables.manifest().payload_checksum != root.manifest_payload_checksum {
        return Err(CoreError::NamespaceCorrupt(format!(
            "metadata root for `{}` references manifest `{}` with checksum {} but the manifest carries {}",
            namespace_id.as_str(),
            root.manifest_id,
            root.manifest_payload_checksum,
            manifest_tables.manifest().payload_checksum,
        )));
    }
    let manifest_head = head_from_manifest(&head, manifest_tables.manifest());
    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: manifest_head.seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: None,
            recent_segments: &head.recent_segments,
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
        root,
        floor_seq,
        manifest_tables,
        tail_state: replayed.resulting_metadata_state,
    })
}

#[cfg(test)]
pub(super) async fn load_checkpoint_projection_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<(HeadState, MetadataState)> {
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
) -> Result<()> {
    if current_head.namespace_id != reconstructed.namespace_id
        || current_head.seq != reconstructed.seq
        || current_head.head_commit_id != reconstructed.head_commit_id
        || current_head.next_inode_id != reconstructed.next_inode_id
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

pub(super) fn next_manifest_id_after(current: ManifestId) -> Result<ManifestId> {
    current
        .0
        .checked_add(1)
        .map(ManifestId)
        .ok_or_else(|| CoreError::Internal("manifest id overflow".to_owned()))
}

pub(crate) async fn build_initial_namespace_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    initial_head: &HeadState,
    writer_version: &str,
) -> Result<NamespaceManifestEnvelope> {
    let manifest_id = ManifestId(initial_head.seq.0);
    let manifest_object_id = ManifestObjectId::generate(manifest_id);
    let metadata_state = bootstrap_metadata_state();
    let run_tables = build_manifest_tables(
        store,
        namespace_id,
        initial_head.seq,
        CHECKPOINT_BASE_RUN_LEVEL,
        &metadata_state,
        MetadataLsmPolicy::default().max_rows_per_segment,
    )
    .await?;
    debug_assert_manifest_table_segments_do_not_overlap(&run_tables);

    NamespaceManifestEnvelope::from_payload(
        writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id,
            manifest_object_id,
            head_seq: initial_head.seq,
            head_commit_id: initial_head.head_commit_id.clone(),
            base_seq: initial_head.seq,
            writer_epoch: initial_head.writer_epoch,
            next_inode_id: initial_head.next_inode_id,
            // Bootstrap precedes the floor object; nothing is retained
            // below the genesis seq.
            retention_floor_seq: ChangeSeq(0),
            initialized: true,
            verified: true,
            fork: None,
            features: BTreeMap::new(),
            metadata_files: flatten_manifest_tables(run_tables),
        },
    )
    .map_err(|err| {
        CoreError::Internal(format!(
            "failed to build namespace manifest envelope: {err}"
        ))
    })
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
    manifest_id: ManifestId,
    manifest_object_id: ManifestObjectId,
) -> Result<NamespaceManifestEnvelope> {
    let head_seq = projection.head.seq;
    let previous_manifest = projection.manifest_tables.manifest();

    // Checkpoint publication is manifest-only: every prior run is
    // referenced unchanged and the WAL delta lands as one new L0 run, so
    // the cost follows the delta, never the namespace. Folding L0 runs
    // back into the base is reorganization's job (`reorganize.rs`), off
    // this path.
    let base_seq = previous_manifest.payload.base_seq;
    let mut metadata_files = previous_manifest.payload.metadata_files.clone();
    if previous_manifest.payload.head_seq < head_seq {
        metadata_files.extend(flatten_manifest_tables(
            build_manifest_l0_run_tables(
                store,
                namespace_id,
                head_seq,
                previous_manifest.payload.head_seq,
                &projection.tail_state,
            )
            .await?,
        ));
    }

    NamespaceManifestEnvelope::from_payload(
        writer_version,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_id,
            manifest_object_id,
            head_seq,
            head_commit_id: projection.head.head_commit_id.clone(),
            base_seq,
            writer_epoch: projection.head.writer_epoch,
            next_inode_id: projection.head.next_inode_id,
            retention_floor_seq: projection.floor_seq,
            initialized: true,
            verified: true,
            fork: None,
            features: BTreeMap::new(),
            metadata_files,
        },
    )
    .map_err(|err| {
        CoreError::Internal(format!(
            "failed to build namespace manifest envelope: {err}"
        ))
    })
}

/// Drops rows that no retained sequence can observe (format spec,
/// "Compaction"). Conservative subset: superseded revisions, superseded or
/// unbound bindings, and spent unbind markers at or below the retention
/// floor. Tombstone and inode rows are always retained until
/// reachability-based dropping is designed.
pub(super) fn drop_rows_below_retention_floor(
    rows_by_family: &mut BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
    retention_floor_seq: ChangeSeq,
) -> Result<()> {
    // The latest revision at or below the floor is what the floor itself
    // observes; every older one is superseded at every retained sequence.
    let mut latest_revision_at_floor = BTreeMap::new();
    for row in rows_by_family
        .get(&MetadataTableFamily::Revisions)
        .into_iter()
        .flatten()
    {
        if let MetadataRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            ..
        } = row
        {
            if *committed_seq <= retention_floor_seq {
                let latest = latest_revision_at_floor
                    .entry(*inode_id)
                    .or_insert(*revision_no);
                if *revision_no > *latest {
                    *latest = *revision_no;
                }
            }
        }
    }
    let retain_revision = |row: &MetadataRow| match row {
        MetadataRow::Revision {
            inode_id,
            revision_no,
            committed_seq,
            ..
        } => {
            *committed_seq > retention_floor_seq
                || latest_revision_at_floor.get(inode_id) == Some(revision_no)
        }
        _ => true,
    };
    for family in [
        MetadataTableFamily::Revisions,
        MetadataTableFamily::RevisionsByInodeDesc,
    ] {
        if let Some(rows) = rows_by_family.get_mut(&family) {
            rows.retain(retain_revision);
        }
    }

    // At the floor only the latest non-unbound bind per (parent, name) slot
    // is visible; an unbind marker at or below the floor has finished its
    // work once every bind it covered is gone.
    // Unbind identity here omits child_inode_id (the read path also matches
    // it); the 4-tuple is already unique for writer-produced rows, so the
    // predicates agree on every legal history.
    let mut unbound_at_floor = BTreeSet::new();
    for row in rows_by_family
        .get(&MetadataTableFamily::DirentryUnbinds)
        .into_iter()
        .flatten()
    {
        if let MetadataRow::DirentryUnbind {
            parent_inode_id,
            name_key,
            bind_seq,
            bind_delta_index,
            unbind_seq,
            ..
        } = row
        {
            if *unbind_seq <= retention_floor_seq {
                unbound_at_floor.insert((
                    *parent_inode_id,
                    name_key.clone(),
                    *bind_seq,
                    *bind_delta_index,
                ));
            }
        }
    }
    let mut latest_bind_at_floor = BTreeMap::new();
    for row in rows_by_family
        .get(&MetadataTableFamily::DirentryBinds)
        .into_iter()
        .flatten()
    {
        if let MetadataRow::DirentryBind {
            parent_inode_id,
            name_key,
            bind_seq,
            bind_delta_index,
            ..
        } = row
        {
            if *bind_seq <= retention_floor_seq {
                let candidate = (*bind_seq, *bind_delta_index);
                let latest = latest_bind_at_floor
                    .entry((*parent_inode_id, name_key.clone()))
                    .or_insert(candidate);
                if candidate > *latest {
                    *latest = candidate;
                }
            }
        }
    }
    // Load-bearing writer invariant: a bind is only ever superseded by an
    // operation that also unbinds it, so every non-latest bind at or below
    // the floor must have a matching unbind at or below the floor. The drop
    // is only visibility-preserving under that rule; refuse to compact state
    // that violates it.
    for row in rows_by_family
        .get(&MetadataTableFamily::DirentryBinds)
        .into_iter()
        .flatten()
    {
        if let MetadataRow::DirentryBind {
            parent_inode_id,
            name_key,
            bind_seq,
            bind_delta_index,
            ..
        } = row
        {
            if *bind_seq <= retention_floor_seq
                && latest_bind_at_floor.get(&(*parent_inode_id, name_key.clone()))
                    != Some(&(*bind_seq, *bind_delta_index))
                && !unbound_at_floor.contains(&(
                    *parent_inode_id,
                    name_key.clone(),
                    *bind_seq,
                    *bind_delta_index,
                ))
            {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "bind at seq `{bind_seq}` delta {bind_delta_index} for parent `{parent_inode_id}` is superseded at or below the retention floor without an unbind; refusing to drop rows"
                )));
            }
        }
    }

    let retain_bind = |row: &MetadataRow| match row {
        MetadataRow::DirentryBind {
            parent_inode_id,
            name_key,
            bind_seq,
            bind_delta_index,
            ..
        } => {
            *bind_seq > retention_floor_seq
                || (latest_bind_at_floor.get(&(*parent_inode_id, name_key.clone()))
                    == Some(&(*bind_seq, *bind_delta_index))
                    && !unbound_at_floor.contains(&(
                        *parent_inode_id,
                        name_key.clone(),
                        *bind_seq,
                        *bind_delta_index,
                    )))
        }
        _ => true,
    };
    for family in [
        MetadataTableFamily::DirentryBinds,
        MetadataTableFamily::DirentryChildBinds,
    ] {
        if let Some(rows) = rows_by_family.get_mut(&family) {
            rows.retain(retain_bind);
        }
    }
    if let Some(rows) = rows_by_family.get_mut(&MetadataTableFamily::DirentryUnbinds) {
        rows.retain(|row| match row {
            MetadataRow::DirentryUnbind { unbind_seq, .. } => *unbind_seq > retention_floor_seq,
            _ => true,
        });
    }

    // The idempotency horizon is the retention floor: a commit retried from
    // below it re-bootstraps like any sub-floor cursor, so its receipt no
    // longer needs to be carried forward.
    if let Some(rows) = rows_by_family.get_mut(&MetadataTableFamily::CommitReceipts) {
        rows.retain(|row| match row {
            MetadataRow::CommitReceipt { committed_seq, .. } => {
                *committed_seq >= retention_floor_seq
            }
            _ => true,
        });
    }
    Ok(())
}

#[cfg(test)]
pub(super) struct ManifestMetadataSource<'a> {
    pub(super) head: &'a HeadState,
    pub(super) basis_manifest_id: Option<ManifestId>,
    pub(super) retention_floor_seq: ChangeSeq,
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
#[cfg(test)]
pub(super) async fn build_namespace_manifest_from_metadata_state<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    source: ManifestMetadataSource<'_>,
    writer_version: &str,
    policy: MetadataLsmPolicy,
    manifest_id: ManifestId,
) -> Result<NamespaceManifestEnvelope> {
    let manifest_object_id = ManifestObjectId::generate(manifest_id);
    let head = source.head;
    let metadata_state = source.metadata_state;
    let head_seq = head.seq;
    let previous_manifest = match source.basis_manifest_id {
        Some(previous_id) => Some(
            load_manifest_materialization_for_inspection(store, namespace_id, previous_id)
                .await
                .map_err(|error| {
                    CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
                })?,
        ),
        _ => None,
    };

    let (base_seq, metadata_files) = match previous_manifest {
        Some(previous) if is_bootstrap_seed_manifest(&previous.manifest.payload) => {
            let run_tables = build_manifest_tables(
                store,
                namespace_id,
                head_seq,
                CHECKPOINT_BASE_RUN_LEVEL,
                metadata_state,
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
            manifest_object_id,
            head_seq,
            head_commit_id: head.head_commit_id.clone(),
            base_seq,
            writer_epoch: head.writer_epoch,
            next_inode_id: head.next_inode_id,
            retention_floor_seq: source.retention_floor_seq,
            initialized: true,
            verified: true,
            fork: None,
            features: BTreeMap::new(),
            metadata_files,
        },
    )
    .map_err(|err| {
        CoreError::Internal(format!(
            "failed to build namespace manifest envelope: {err}"
        ))
    })
}

#[cfg(test)]
fn is_bootstrap_seed_manifest(payload: &NamespaceManifestPayload) -> bool {
    payload.head_seq == ChangeSeq(0) && payload.base_seq == ChangeSeq(0) && payload.fork.is_none()
}
