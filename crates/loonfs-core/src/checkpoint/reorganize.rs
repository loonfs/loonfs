//! Bounded metadata reorganization: the background half of the
//! checkpoint/compaction split.
//!
//! Checkpoint publication only ever appends an L0 delta run, so its cost
//! follows the WAL delta, never the namespace. Folding those L0 runs into
//! the base happens here instead, one **family group** at a time: the
//! group's rows are merged from every run it appears in, rows below the
//! retention floor are dropped, new base segments are written, and a
//! manifest publishes that swaps just that group's references. Families
//! whose retention rules read each other compact together — bind, child
//! bind, and unbind rows form one group; revisions and their descending
//! index another — so index parity and the drop invariants hold within
//! every unit.
//!
//! There is no progress record: each unit ends in a durable manifest, so a
//! crashed or interrupted reorganization resumes by reading the live
//! manifest and picking the next group that still has L0 rows. Unit
//! selection is deterministic (most L0 rows first, then group order). A
//! concurrent checkpoint racing a unit wins at the root compare-and-swap;
//! the unit's segments are left unreferenced for garbage collection and the
//! next step retries against the fresh manifest.

use super::build::{
    build_manifest_tables_from_rows, debug_assert_manifest_table_segments_do_not_overlap,
    MetadataTableSegmentation,
};
use super::error::ManifestLoadError;
use super::flush::{ensure_metadata_publication_budget, next_manifest_id_after};
use super::load::{
    load_namespace_manifest_envelope_if_present, load_verified_manifest_tables,
    validate_direntry_child_bind_index, validate_revision_by_inode_desc_index,
};
use super::publish::{publish_metadata_root, write_namespace_manifest, ManifestPublicationOutcome};
use super::runs::{
    flatten_manifest_tables, l0_run_count, MetadataLsmPolicy, CHECKPOINT_BASE_RUN_LEVEL,
    CHECKPOINT_L0_RUN_LEVEL,
};
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::namespace::control::{read_metadata_root_object, read_wal_floor_seq_or_zero};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::manifest::{
    MetadataFileRef, MetadataRow, MetadataTableFamily, NamespaceManifestEnvelope,
    NamespaceManifestPayload,
};
use loonfs_api::{ChangeSeq, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};

/// Families whose rows merge in one reorganization unit. Groupings follow
/// the retention rules in `drop_rows_below_retention_floor`: families that
/// read each other's rows to decide what to drop must compact together, and
/// a secondary index always travels with its canonical family.
const REORGANIZE_FAMILY_GROUPS: [&[MetadataTableFamily]; 5] = [
    &[
        MetadataTableFamily::DirentryBinds,
        MetadataTableFamily::DirentryChildBinds,
        MetadataTableFamily::DirentryUnbinds,
    ],
    &[
        MetadataTableFamily::Revisions,
        MetadataTableFamily::RevisionsByInodeDesc,
    ],
    &[MetadataTableFamily::Inodes],
    &[MetadataTableFamily::Tombstones],
    &[MetadataTableFamily::CommitReceipts],
];

/// What one reorganization step did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataReorganizeOutcome {
    /// The manifest's L0 run count is below the policy trigger; nothing to
    /// fold yet.
    NotNeeded { l0_runs: usize },
    /// One family group folded into new base segments and the manifest
    /// advanced.
    UnitPublished {
        families: Vec<MetadataTableFamily>,
        folded_l0_rows: u64,
        manifest_id: ManifestId,
    },
    /// A concurrent publication moved the root while this unit ran; its
    /// output is unreferenced (garbage collection reclaims it) and the next
    /// step retries against the fresh manifest.
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataReorganizeReport {
    pub namespace_id: NamespaceId,
    pub outcome: MetadataReorganizeOutcome,
}

/// Runs at most one reorganization unit against the namespace's current
/// manifest. Callers (the maintenance step) invoke this repeatedly; each
/// call re-reads durable state, so any two calls compose — including across
/// process restarts.
#[tracing::instrument(
    level = "info",
    name = "loonfs.phase",
    err,
    skip_all,
    fields(phase = "reorganize_metadata", key_class = "manifest")
)]
pub(crate) async fn reorganize_metadata_step<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
) -> Result<MetadataReorganizeReport> {
    let timer = StdMonotonicTimer::default();
    reorganize_metadata_step_with_timer(store, namespace_id, context, policy, &timer).await
}

pub(super) async fn reorganize_metadata_step_with_timer<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
    timer: &dyn MonotonicTimer,
) -> Result<MetadataReorganizeReport> {
    // The publication budget covers the whole unit: measurement starts
    // before any table object is written and gates the root
    // compare-and-swap below.
    let publication_started_ms = timer.monotonic_now_ms();
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .envelope
        .state;
    let tables = load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    let previous = tables.manifest();

    let l0_runs = l0_run_count(&previous.payload);
    if l0_runs < policy.max_l0_runs.get() {
        return Ok(MetadataReorganizeReport {
            namespace_id: namespace_id.clone(),
            outcome: MetadataReorganizeOutcome::NotNeeded { l0_runs },
        });
    }
    let Some(group) = select_family_group(&previous.payload) else {
        // L0 runs exist but hold no rows (empty families); nothing to fold.
        return Ok(MetadataReorganizeReport {
            namespace_id: namespace_id.clone(),
            outcome: MetadataReorganizeOutcome::NotNeeded { l0_runs },
        });
    };
    let folded_l0_rows = group_l0_rows(&previous.payload, group);

    // Merge the group's rows from every run it appears in. The scan reads
    // exactly the manifest's tables — never the WAL tail — so the unit's
    // output represents the same head the manifest already describes.
    let mut rows_by_family = BTreeMap::<MetadataTableFamily, Vec<MetadataRow>>::new();
    for family in group {
        let rows = tables.scan_prefix(*family, "").await.map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
        rows_by_family.insert(*family, rows);
    }
    // The paired groups exist to preserve index parity; verify it on the
    // merged rows before writing anything.
    if group.contains(&MetadataTableFamily::DirentryBinds) {
        validate_direntry_child_bind_index(
            root.manifest_object_id.as_ref(),
            rows_by_family
                .get(&MetadataTableFamily::DirentryBinds)
                .cloned()
                .unwrap_or_default(),
            rows_by_family
                .get(&MetadataTableFamily::DirentryChildBinds)
                .cloned()
                .unwrap_or_default(),
        )
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    }
    if group.contains(&MetadataTableFamily::Revisions) {
        validate_revision_by_inode_desc_index(
            root.manifest_object_id.as_ref(),
            rows_by_family
                .get(&MetadataTableFamily::Revisions)
                .cloned()
                .unwrap_or_default(),
            rows_by_family
                .get(&MetadataTableFamily::RevisionsByInodeDesc)
                .cloned()
                .unwrap_or_default(),
        )
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    }
    let floor_seq = read_wal_floor_seq_or_zero(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?;
    drop_rows_below_retention_floor(&mut rows_by_family, floor_seq)?;

    let run_tables = build_manifest_tables_from_rows(
        store,
        namespace_id,
        previous.payload.head_seq,
        CHECKPOINT_BASE_RUN_LEVEL,
        |family| rows_by_family.remove(&family).unwrap_or_default(),
        MetadataTableSegmentation::Base {
            max_rows_per_segment: policy.max_rows_per_segment,
        },
    )
    .await?;
    debug_assert_manifest_table_segments_do_not_overlap(&run_tables);

    let mut metadata_files: Vec<_> = previous
        .payload
        .metadata_files
        .iter()
        .filter(|descriptor| !group.contains(&descriptor.family))
        .cloned()
        .collect();
    metadata_files.extend(flatten_manifest_tables(run_tables));
    // `base_seq` is the manifest's oldest-run marker: every referenced run
    // must sit at or above it, including L0 runs other groups have not
    // folded yet.
    let base_seq = metadata_files
        .iter()
        .map(|descriptor| descriptor.run_seq)
        .min()
        .unwrap_or(previous.payload.base_seq);

    let manifest = write_reorganized_manifest(
        store,
        namespace_id,
        previous,
        metadata_files,
        base_seq,
        {
            let mut payload_floor = previous.payload.retention_floor_seq;
            if floor_seq > payload_floor {
                payload_floor = floor_seq;
            }
            payload_floor
        },
        &context.writer_version,
    )
    .await?;

    ensure_metadata_publication_budget(timer, publication_started_ms, namespace_id)?;
    match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
        &root.manifest_object_id,
        context.now_ms,
        &context.writer_version,
    )
    .await?
    {
        ManifestPublicationOutcome::Published(_) => Ok(MetadataReorganizeReport {
            namespace_id: namespace_id.clone(),
            outcome: MetadataReorganizeOutcome::UnitPublished {
                families: group.to_vec(),
                folded_l0_rows,
                manifest_id: manifest.payload.manifest_id,
            },
        }),
        ManifestPublicationOutcome::Superseded(_) | ManifestPublicationOutcome::RootCasRaceLost => {
            Ok(MetadataReorganizeReport {
                namespace_id: namespace_id.clone(),
                outcome: MetadataReorganizeOutcome::Superseded,
            })
        }
    }
}

/// The family group with the most L0 rows to fold; ties resolve in group
/// order. `None` when no group has L0 rows.
fn select_family_group(
    payload: &NamespaceManifestPayload,
) -> Option<&'static [MetadataTableFamily]> {
    REORGANIZE_FAMILY_GROUPS
        .into_iter()
        .map(|group| (group_l0_rows(payload, group), group))
        .filter(|(rows, _)| *rows > 0)
        .max_by(|(left_rows, left), (right_rows, right)| {
            left_rows.cmp(right_rows).then_with(|| {
                // On ties the EARLIER group must win; comparing positions
                // reversed makes max_by pick it.
                position_of(right).cmp(&position_of(left))
            })
        })
        .map(|(_, group)| group)
}

fn position_of(group: &[MetadataTableFamily]) -> usize {
    REORGANIZE_FAMILY_GROUPS
        .iter()
        .position(|candidate| candidate.as_ptr() == group.as_ptr())
        .unwrap_or(usize::MAX)
}

fn group_l0_rows(payload: &NamespaceManifestPayload, group: &[MetadataTableFamily]) -> u64 {
    payload
        .metadata_files
        .iter()
        .filter(|descriptor| {
            descriptor.level == CHECKPOINT_L0_RUN_LEVEL && group.contains(&descriptor.family)
        })
        .map(|descriptor| descriptor.row_count)
        .sum()
}

async fn write_reorganized_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    previous: &NamespaceManifestEnvelope,
    metadata_files: Vec<MetadataFileRef>,
    base_seq: ChangeSeq,
    retention_floor_seq: ChangeSeq,
    writer_version: &str,
) -> Result<NamespaceManifestEnvelope> {
    let manifest_id = next_manifest_id_after(previous.payload.manifest_id)?;
    for _allocation_attempt in 0..CONTENTION_RETRY_LIMIT {
        let manifest_object_id = ManifestObjectId::generate(manifest_id);
        let manifest_key = metadata_manifest_object(namespace_id.as_str(), &manifest_object_id);
        match load_namespace_manifest_envelope_if_present(
            store,
            namespace_id,
            &manifest_object_id,
            &manifest_key,
        )
        .await
        {
            Ok(Some(_existing)) => continue,
            Ok(None) => {}
            Err(error) => {
                return Err(CoreError::MetadataProjection(
                    MetadataProjectionLoadError::ManifestLoad(error),
                ))
            }
        }
        let manifest = NamespaceManifestEnvelope::from_payload(
            writer_version,
            NamespaceManifestPayload {
                namespace_id: namespace_id.clone(),
                manifest_id,
                manifest_object_id,
                head_seq: previous.payload.head_seq,
                head_commit_id: previous.payload.head_commit_id.clone(),
                base_seq,
                writer_epoch: previous.payload.writer_epoch,
                next_inode_id: previous.payload.next_inode_id,
                retention_floor_seq,
                fork: previous.payload.fork.clone(),
                metadata_files: metadata_files.clone(),
            },
        )
        .map_err(|err| {
            CoreError::Internal(format!("failed to build reorganized manifest: {err}"))
        })?;
        match write_namespace_manifest(store, &manifest).await {
            Ok(()) => return Ok(manifest),
            Err(MetadataProjectionLoadError::ManifestLoad(
                ManifestLoadError::ManifestConflict { .. },
            )) => continue,
            Err(error) => return Err(CoreError::MetadataProjection(error)),
        }
    }
    Err(CoreError::Internal(
        "reorganized manifest allocation retry exhausted".to_owned(),
    ))
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
