//! Bounded metadata reorganization: the background half of the
//! checkpoint/compaction split.
//!
//! Checkpoint publication only ever appends an L0 delta run, so its cost
//! follows the WAL delta, never the namespace. Folding those L0 runs into
//! the base happens here instead, one **family group** at a time: the
//! group's rows are merged from an oldest-first, budgeted subset of complete
//! runs, rows no retained sequence can observe are dropped, new base
//! segments are written, and a manifest publishes that swaps just those
//! references. Families whose rows must stay mutually consistent compact
//! together — bind, child bind, and unbind rows form one group because their
//! drop rules read each other; revisions travel with their descending index
//! so index parity holds within every unit.
//!
//! There is no progress record: each unit ends in a durable manifest, so a
//! crashed or interrupted reorganization resumes by reading the live
//! manifest and picking the next group that still has L0 rows. Unit
//! selection is deterministic (most L0 rows first, then group order). A
//! concurrent checkpoint racing a unit wins at the root compare-and-swap;
//! the unit's segments are left unreferenced for garbage collection and the
//! next step retries against the fresh manifest.

use super::block_fetch::load_segment_index_for_reorganization;
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
    flatten_manifest_tables, l0_run_count, MetadataLsmPolicy, MetadataRunManifest,
    CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL,
};
use super::scan::VerifiedMetadataTables;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::namespace::basis::resolve_retention_floor_seq;
use crate::namespace::control::{read_head_object, read_metadata_root_object_if_present};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::manifest::{
    MetadataFileRef, MetadataRow, MetadataTableFamily, NamespaceManifestEnvelope,
    NamespaceManifestPayload,
};
use loonfs_api::{ChangeSeq, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};

/// Families whose rows merge in one reorganization unit. Families that read
/// each other's rows to decide what to drop (see
/// `drop_rows_below_retention_floor`) must compact together, and a secondary
/// index always travels with its canonical family.
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
    /// One bounded complete-run subset for a family group folded into new
    /// base segments and the manifest advanced.
    UnitPublished {
        families: Vec<MetadataTableFamily>,
        folded_l0_rows: u64,
        input_runs: usize,
        decoded_input_rows: u64,
        decoded_input_bytes: u64,
        manifest_id: ManifestId,
    },
    /// The trigger fired, but no oldest-first subset that would make
    /// progress fit the hard per-step input budgets.
    BudgetExhausted {
        families: Vec<MetadataTableFamily>,
        l0_runs: usize,
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
    // A namespace that has published no manifest of its own has no runs to
    // fold: reorganization has nothing to do until its first flush.
    let Some(root) = read_metadata_root_object_if_present(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .map(|loaded| loaded.envelope.state)
    else {
        return Ok(MetadataReorganizeReport {
            namespace_id: namespace_id.clone(),
            outcome: MetadataReorganizeOutcome::NotNeeded { l0_runs: 0 },
        });
    };
    let tables = load_verified_manifest_tables(store, namespace_id, &root.manifest_object_id)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    let previous = tables.manifest();

    let l0_runs = l0_run_count(&previous.payload);
    if l0_runs < policy.max_l0_runs.get()
        && !manifest_has_partial_reorganization(tables.scan_runs.as_ref())
    {
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
    let Some(input) = select_reorganization_input(&tables, group, policy)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?
    else {
        return Ok(MetadataReorganizeReport {
            namespace_id: namespace_id.clone(),
            outcome: MetadataReorganizeOutcome::BudgetExhausted {
                families: group.to_vec(),
                l0_runs,
            },
        });
    };

    // Merge only the selected complete runs. The scan reads exactly the
    // manifest's tables — never the WAL tail — and the unselected
    // descriptors remain in the replacement manifest unchanged.
    let mut rows_by_family = BTreeMap::<MetadataTableFamily, Vec<MetadataRow>>::new();
    for family in group {
        let rows = tables
            .scan_prefix_in_runs(&input.runs, *family, "")
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
        rows_by_family.insert(*family, rows);
    }
    // The paired groups exist to preserve index parity; verify it on the
    // selected complete runs before writing anything.
    if group.contains(&MetadataTableFamily::DirentryBinds) {
        validate_direntry_child_bind_index(
            root.manifest_object_id.as_ref(),
            rows_by_family
                .get(&MetadataTableFamily::DirentryBinds)
                .map_or(&[], Vec::as_slice),
            rows_by_family
                .get(&MetadataTableFamily::DirentryChildBinds)
                .map_or(&[], Vec::as_slice),
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
                .map_or(&[], Vec::as_slice),
            rows_by_family
                .get(&MetadataTableFamily::RevisionsByInodeDesc)
                .map_or(&[], Vec::as_slice),
        )
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    }
    let head = read_head_object(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .envelope
        .state;
    let floor_seq = resolve_retention_floor_seq(store, &head)
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
        .filter(|descriptor| {
            !group.contains(&descriptor.family)
                || !input
                    .run_ids
                    .contains(&(descriptor.run_seq, descriptor.level))
        })
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
        Some(root.manifest_object_id.clone()),
        context.now_ms,
        &context.writer_version,
    )
    .await?
    {
        ManifestPublicationOutcome::Published(_) => Ok(MetadataReorganizeReport {
            namespace_id: namespace_id.clone(),
            outcome: MetadataReorganizeOutcome::UnitPublished {
                families: group.to_vec(),
                folded_l0_rows: input.folded_l0_rows,
                input_runs: input.runs.len(),
                decoded_input_rows: input.decoded_rows,
                decoded_input_bytes: input.decoded_bytes,
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

struct ReorganizationInput {
    runs: Vec<MetadataRunManifest>,
    run_ids: BTreeSet<(ChangeSeq, u32)>,
    folded_l0_rows: u64,
    decoded_rows: u64,
    decoded_bytes: u64,
}

/// Selects the existing compacted accumulator followed by L0 runs
/// oldest-first. Index sections are read before row payloads so the
/// decoded-byte budget is known exactly from each data block's durable
/// `decoded_len`; a run that would cross a budget is not decoded or
/// partially included.
async fn select_reorganization_input<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    group: &[MetadataTableFamily],
    policy: MetadataLsmPolicy,
) -> std::result::Result<Option<ReorganizationInput>, ManifestLoadError> {
    let mut candidates = tables
        .scan_runs
        .iter()
        .filter(|run| run_has_group_rows(run, group))
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        let left_is_l0 = left.level == CHECKPOINT_L0_RUN_LEVEL;
        let right_is_l0 = right.level == CHECKPOINT_L0_RUN_LEVEL;
        left_is_l0
            .cmp(&right_is_l0)
            .then(left.run_seq.cmp(&right.run_seq))
            .then(right.level.cmp(&left.level))
    });
    let candidate_count = candidates.len();
    let row_budget =
        u64::try_from(policy.max_decoded_input_rows_per_step.get()).unwrap_or(u64::MAX);
    let byte_budget =
        u64::try_from(policy.max_decoded_input_bytes_per_step.get()).unwrap_or(u64::MAX);

    let mut runs = Vec::new();
    let mut decoded_rows = 0u64;
    let mut decoded_bytes = 0u64;
    let mut folded_l0_rows = 0u64;
    for run in candidates
        .into_iter()
        .take(policy.max_input_runs_per_step.get())
    {
        let run_rows = group_run_descriptors(run, group)
            .map(|descriptor| descriptor.row_count)
            .sum::<u64>();
        if decoded_rows.saturating_add(run_rows) > row_budget {
            break;
        }
        let run_bytes = decoded_group_run_bytes(tables, run, group).await?;
        if decoded_bytes.saturating_add(run_bytes) > byte_budget {
            break;
        }
        if run.level == CHECKPOINT_L0_RUN_LEVEL {
            folded_l0_rows = folded_l0_rows.saturating_add(run_rows);
        }
        decoded_rows = decoded_rows.saturating_add(run_rows);
        decoded_bytes = decoded_bytes.saturating_add(run_bytes);
        runs.push(run.clone());
    }

    let selected_l0 = runs.iter().any(|run| run.level == CHECKPOINT_L0_RUN_LEVEL);
    let makes_progress = selected_l0 && (runs.len() > 1 || candidate_count == 1);
    if !makes_progress {
        return Ok(None);
    }
    let run_ids = runs.iter().map(|run| (run.run_seq, run.level)).collect();
    Ok(Some(ReorganizationInput {
        runs,
        run_ids,
        folded_l0_rows,
        decoded_rows,
        decoded_bytes,
    }))
}

/// A bounded fold stamps its output at the manifest head. If older or
/// same-seq L0 runs remain, that ordering is the durable resume marker. A
/// fresh L0 appended after a completed fold is strictly newer than every
/// base-tier run and therefore does not bypass the normal trigger.
fn manifest_has_partial_reorganization(runs: &[MetadataRunManifest]) -> bool {
    let Some(oldest_l0_seq) = runs
        .iter()
        .filter(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
        .map(|run| run.run_seq)
        .min()
    else {
        return false;
    };
    runs.iter()
        .any(|run| run.level != CHECKPOINT_L0_RUN_LEVEL && run.run_seq >= oldest_l0_seq)
}

fn run_has_group_rows(run: &MetadataRunManifest, group: &[MetadataTableFamily]) -> bool {
    group_run_descriptors(run, group).next().is_some()
}

fn group_run_descriptors<'a>(
    run: &'a MetadataRunManifest,
    group: &'a [MetadataTableFamily],
) -> impl Iterator<Item = &'a MetadataFileRef> {
    run.tables
        .iter()
        .filter(|table| group.contains(&table.family))
        .flat_map(|table| &table.segments)
}

async fn decoded_group_run_bytes<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    run: &MetadataRunManifest,
    group: &[MetadataTableFamily],
) -> std::result::Result<u64, ManifestLoadError> {
    let mut decoded_bytes = 0u64;
    for descriptor in group_run_descriptors(run, group) {
        let index = load_segment_index_for_reorganization(
            tables.store,
            tables.table_cache,
            &tables.block_memo,
            descriptor,
        )
        .await?;
        for entry in index.iter() {
            decoded_bytes = decoded_bytes.saturating_add(u64::from(entry.block.decoded_len));
        }
    }
    Ok(decoded_bytes)
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
/// "Compaction"). Conservative subset: superseded or unbound bindings and
/// spent unbind markers at or below the retention floor. Revision rows are
/// never dropped — file history is durable data retained independently of
/// the replay floor — and tombstone and inode rows are always retained
/// until reachability-based dropping is designed.
pub(super) fn drop_rows_below_retention_floor(
    rows_by_family: &mut BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
    retention_floor_seq: ChangeSeq,
) -> Result<()> {
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
