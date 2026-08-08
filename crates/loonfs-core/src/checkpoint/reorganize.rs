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
//!
//! A group whose oldest run no longer fits one step folds a slice at a time
//! instead ([`super::partial_fold`]). That fold does keep a progress record,
//! in the manifest, because its output arrives in pieces. While it is in
//! flight its group folds no other way: a step that selects the group
//! advances the fold and does nothing else for it. Group selection still
//! runs per step, so the other groups keep folding on the steps they win.

use super::block_fetch::load_segment_index_for_reorganization;
use super::build::{
    build_manifest_tables_from_rows, debug_assert_manifest_table_segments_do_not_overlap,
    MetadataTableSegmentation,
};
use super::error::ManifestLoadError;
use super::flush::{ensure_metadata_publication_budget, next_manifest_id_after};
use super::load::{
    load_verified_manifest_tables, validate_direntry_child_bind_index,
    validate_revision_by_inode_desc_index,
};
use super::partial_fold::{MetadataFoldSliceDrops, MetadataFoldWalk, MetadataFoldWalkOutcome};
use super::publish::{
    manifest_write_failure, publish_metadata_root, write_namespace_manifest,
    ManifestPublicationOutcome,
};
use super::runs::{
    flatten_manifest_tables, l0_run_count, MetadataLsmPolicy, MetadataRunManifest,
    CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL, REORGANIZE_FAMILY_GROUPS,
};
use super::scan::VerifiedMetadataTables;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::basis::resolve_retention_floor_seq;
use crate::namespace::control::{read_head_object, read_metadata_root_object_if_present};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::manifest::{
    ActiveDeletionRowAction, MetadataFileRef, MetadataRow, MetadataRunId, MetadataTableFamily,
    NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::{
    AttributeRevisionNo, ChangeSeq, InodeId, ManifestId, ManifestObjectId, NameKey, NamespaceId,
};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};

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
    /// One slice of a partial fold merged and published. The group's oldest
    /// run does not fit one step, so the group is being folded a partition
    /// at a time and this step moved that fold along.
    PartialFoldAdvanced {
        families: Vec<MetadataTableFamily>,
        /// Partitions of the group's keyspace this slice covered.
        partitions: u64,
        decoded_input_rows: u64,
        decoded_input_bytes: u64,
        /// Rows this slice wrote into the run the fold is building.
        output_rows: u64,
        /// Where the fold now stands: the next partition it has not
        /// processed.
        cursor: String,
        /// Whether this slice dropped what the frozen floor allows.
        drops: MetadataFoldSliceDrops,
        manifest_id: ManifestId,
    },
    /// A partial fold's last publication: the runs it merged left the
    /// manifest, the run it built took their place, and its progress state
    /// cleared.
    PartialFoldCompleted {
        families: Vec<MetadataTableFamily>,
        output_segments: usize,
        output_rows: u64,
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
    // A partial fold in flight keeps stepping whatever the trigger says.
    // Its state is durable and only its own steps clear it, so a step that
    // returned here would leave the fold's outputs unfinished and its group
    // unfoldable for as long as the trigger stayed quiet.
    if l0_runs < policy.max_l0_runs.get()
        && previous.payload.reorganize.is_none()
        && !manifest_has_partial_reorganization(tables.scan_runs.as_ref())
    {
        return Ok(MetadataReorganizeReport {
            namespace_id: namespace_id.clone(),
            outcome: MetadataReorganizeOutcome::NotNeeded { l0_runs },
        });
    }
    let folding_group = manifest_partial_fold_group(&previous.payload);
    // Group selection runs once per step, so a fold in flight takes its
    // turn like anything else rather than holding the namespace. The runs
    // it is already merging do not count as work waiting for its group (see
    // `select_family_group`), so a group with a fold in flight only wins a
    // step when runs have arrived above the fold — and when no group has
    // work waiting at all, the fold is what the step does.
    let Some(group) = select_family_group(&previous.payload).or(folding_group) else {
        // L0 runs exist but hold no rows (empty families); nothing to fold.
        return Ok(MetadataReorganizeReport {
            namespace_id: namespace_id.clone(),
            outcome: MetadataReorganizeOutcome::NotNeeded { l0_runs },
        });
    };
    // A group with a fold in flight folds no other way: no delta merge for
    // it, no second fold, until the fold finishes and swaps its output in.
    if folding_group == Some(group) {
        return advance_partial_fold(store, namespace_id, &tables, policy, context, timer).await;
    }
    let selection = select_reorganization_input(&tables, group, policy)
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
        })?;
    if let Some(bottom) = selection.group_bottom_over_budget {
        // The group's oldest run no longer fits one step. Say so once, then
        // start folding the group a slice at a time; from the next step on
        // the fold's progress is what this group reports.
        report_group_bottom_over_budget(namespace_id, group, &bottom, policy);
        return start_partial_fold(store, namespace_id, &tables, group, policy, context, timer)
            .await;
    }
    let Some(input) = selection.input else {
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
    // Dropping is only visibility-preserving over a merge that starts at
    // the group's oldest run; see
    // [`ReorganizationInput::starts_at_group_bottom`]. A window that had to
    // skip older runs merges them exactly as they are, so its output holds
    // every row its inputs held.
    if input.starts_at_group_bottom {
        drop_rows_below_retention_floor(&mut rows_by_family, floor_seq)?;
    }

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

    let manifest =
        write_reorganized_manifest(store, namespace_id, previous, metadata_files, base_seq, {
            let mut payload_floor = previous.payload.retention_floor_seq;
            if floor_seq > payload_floor {
                payload_floor = floor_seq;
            }
            payload_floor
        })
        .await?;

    ensure_metadata_publication_budget(timer, publication_started_ms, namespace_id)?;
    match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
        Some(root.manifest_object_id.clone()),
        context.now_ms,
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

/// The family group a manifest's partial fold names, or `None` when it
/// carries no fold.
///
/// Manifest load already refused a state naming anything but a
/// reorganization family group, so this only turns the stored list back into
/// the static entry the rest of the module is written against.
fn manifest_partial_fold_group(
    payload: &NamespaceManifestPayload,
) -> Option<&'static [MetadataTableFamily]> {
    let progress = payload.reorganize.as_ref()?;
    REORGANIZE_FAMILY_GROUPS
        .into_iter()
        .find(|candidate| *candidate == progress.families.as_slice())
}

/// Advances the partial fold a manifest carries by one slice, or publishes
/// the swap that finishes it.
async fn advance_partial_fold<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    tables: &VerifiedMetadataTables<'_, S>,
    policy: MetadataLsmPolicy,
    context: &MutationContext,
    timer: &dyn MonotonicTimer,
) -> Result<MetadataReorganizeReport> {
    let Some(mut walk) = MetadataFoldWalk::resume_from_manifest(tables, policy).await? else {
        return Err(CoreError::Internal(
            "a partial fold was selected against a manifest that carries none".to_owned(),
        ));
    };
    let outcome = walk
        .advance(store, namespace_id, tables, policy, context, timer)
        .await?;
    Ok(partial_fold_report(namespace_id, &walk, outcome))
}

/// Starts a partial fold of `group` and runs its first slice.
///
/// Starting is not a step of its own. The executor publishes nothing until a
/// slice is written, so one step both starts the fold and folds its first
/// slice, and the manifest that step publishes carries the state a resume
/// reads back. The budgets still hold across the pair: the start scans the
/// group's unbind rows to freeze the set its drops read, and
/// `MetadataFoldWalk` charges those rows against this step's row budget
/// before it plans the slice.
///
/// The input is every run of the manifest that holds rows of the group.
/// Dropping rows is only visibility-preserving over a merge that starts at
/// the group's oldest run, and taking the whole group is also what leaves it
/// in one run when the fold finishes.
async fn start_partial_fold<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    tables: &VerifiedMetadataTables<'_, S>,
    group: &'static [MetadataTableFamily],
    policy: MetadataLsmPolicy,
    context: &MutationContext,
    timer: &dyn MonotonicTimer,
) -> Result<MetadataReorganizeReport> {
    let head = read_head_object(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .envelope
        .state;
    let frozen_floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::load_head)?;
    let snapshot = tables
        .scan_runs
        .iter()
        .filter(|run| run_has_group_rows(run, group))
        .cloned()
        .collect();
    let mut walk =
        MetadataFoldWalk::start(tables, group, snapshot, frozen_floor_seq, policy).await?;
    let outcome = walk
        .advance(store, namespace_id, tables, policy, context, timer)
        .await?;
    Ok(partial_fold_report(namespace_id, &walk, outcome))
}

/// Reads one partial-fold step as a reorganization report.
fn partial_fold_report(
    namespace_id: &NamespaceId,
    walk: &MetadataFoldWalk,
    outcome: MetadataFoldWalkOutcome,
) -> MetadataReorganizeReport {
    let families = walk.group().to_vec();
    let outcome = match outcome {
        MetadataFoldWalkOutcome::SlicePublished(slice) => {
            MetadataReorganizeOutcome::PartialFoldAdvanced {
                families,
                partitions: slice.partitions,
                decoded_input_rows: slice.decoded_input_rows,
                decoded_input_bytes: slice.decoded_input_bytes,
                output_rows: slice.output_rows,
                // Read back from the fold rather than from the slice: this
                // is the position the manifest now carries, which is where
                // the next step resumes.
                cursor: walk.progress().cursor.clone(),
                drops: slice.drops,
                manifest_id: slice.manifest_id,
            }
        }
        MetadataFoldWalkOutcome::Completed {
            manifest_id,
            output_segments,
            output_rows,
        } => MetadataReorganizeOutcome::PartialFoldCompleted {
            families,
            output_segments,
            output_rows,
            manifest_id,
        },
        MetadataFoldWalkOutcome::Superseded => MetadataReorganizeOutcome::Superseded,
    };
    MetadataReorganizeReport {
        namespace_id: namespace_id.clone(),
        outcome,
    }
}

pub(super) struct ReorganizationInput {
    pub(super) runs: Vec<MetadataRunManifest>,
    run_ids: BTreeSet<(ChangeSeq, u32)>,
    folded_l0_rows: u64,
    decoded_rows: u64,
    decoded_bytes: u64,
    /// Whether the selected window starts at the group's oldest run.
    ///
    /// Retention dropping reads across the merged rows: an unbind cancels
    /// the bind it names, and a removal marker cancels the listed deletion
    /// it repeats. Dropping one half of such a pair is only
    /// visibility-preserving when the other half is in the same merge, and
    /// what guarantees that is starting the merge at the group's oldest
    /// run — the cancelling row is always the newer of the two, so every row
    /// it can cancel is already in the window (format spec, "Compaction").
    pub(super) starts_at_group_bottom: bool,
}

/// What one selection attempt found.
///
/// The saturation record travels beside the input rather than replacing it.
/// A group whose oldest run has outgrown one step's budget is folded a slice
/// at a time instead of whole, which is the caller's decision to make; the
/// delta-only window this search still finds is what the group would have
/// merged before partial folds existed.
pub(super) struct ReorganizationSelection {
    pub(super) input: Option<ReorganizationInput>,
    pub(super) group_bottom_over_budget: Option<OverBudgetRun>,
}

/// A run that does not fit one step's budgets on its own.
#[derive(Debug, Clone, Copy)]
pub(super) struct OverBudgetRun {
    pub(super) run_seq: ChangeSeq,
    pub(super) level: u32,
    pub(super) rows: u64,
    /// The run's decoded byte total, or `None` when the row budget ruled
    /// the run out before its byte total was read.
    pub(super) decoded_bytes: Option<u64>,
}

/// Selects a contiguous window of complete runs to merge, oldest-first.
///
/// **The order.** The comparator below is the group's recency order, oldest
/// first. Base-tier runs hold rows an earlier fold already absorbed, so they
/// sit under every L0 run whatever their `run_seq` says: a bounded fold
/// stamps its output at the manifest head, which can leave a base run
/// carrying a higher `run_seq` than an L0 run it did not fold (see
/// [`manifest_has_partial_reorganization`]). Within one tier the lower
/// `run_seq` is the older run.
///
/// **The invariant.** A merge writes its inputs back as one base-tier run
/// stamped at the manifest head. That output is older than every L0 run in
/// the order above, so it may not carry a row newer than an L0 run it left
/// behind — otherwise a later fold could drop a row while the row it cancels
/// sits outside the window. Two rules keep that true. The window is a
/// *contiguous* slice of the order, and its start only ever moves forward
/// past **base-tier** runs; an L0 run is never stepped over. Every run the
/// window leaves out therefore sits wholly below it or wholly above it, never
/// interleaved, and the output can stand in for the whole window without
/// moving any row past any other. When the window starts at the very bottom
/// nothing is left out below it at all, and that is the stronger property
/// retention dropping needs — see
/// [`ReorganizationInput::starts_at_group_bottom`].
///
/// **The budgets pace the work; they never end it.** When no window starting
/// at the group's oldest run can fit the budgets, the start moves past the
/// base-tier runs that block it and the window merges the L0 runs above them
/// on their own, so the group keeps shedding runs. The caller has a better
/// answer for that case now and takes it — it folds the group a slice at a
/// time instead ([`super::partial_fold`]) — but the window is still what
/// this search reports, and it is still the one the group would merge if the
/// oldest run fit.
///
/// Index sections are read before row payloads so the decoded-byte budget is
/// known exactly from each data block's durable `decoded_len`; a run that
/// would cross a budget is not decoded or partially included.
pub(super) async fn select_reorganization_input<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    group: &[MetadataTableFamily],
    policy: MetadataLsmPolicy,
) -> std::result::Result<ReorganizationSelection, ManifestLoadError> {
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
    let max_runs = policy.max_input_runs_per_step.get();
    // Base-tier runs sort first, so this is also the index of the oldest L0
    // candidate. The window start may move up to it and no further.
    let base_tier_candidates = candidates
        .iter()
        .take_while(|run| run.level != CHECKPOINT_L0_RUN_LEVEL)
        .count();
    let window_starts = (base_tier_candidates + 1)
        .min(candidate_count)
        .min(max_runs);
    // Reading a run's decoded byte total costs its index sections, so each
    // candidate's total is read at most once however many windows weigh it.
    let mut decoded_bytes_by_candidate = vec![None::<u64>; candidate_count];
    let mut group_bottom_over_budget = None;

    for window_start in 0..window_starts {
        let mut runs = Vec::new();
        let mut decoded_rows = 0u64;
        let mut decoded_bytes = 0u64;
        let mut folded_l0_rows = 0u64;
        for index in window_start..candidate_count.min(window_start + max_runs) {
            let run = candidates[index];
            let run_rows = group_run_descriptors(run, group)
                .map(|descriptor| descriptor.row_count)
                .sum::<u64>();
            if decoded_rows.saturating_add(run_rows) > row_budget {
                // Index zero is only reached by the first window, whose
                // accumulator is still empty there, so this is the group's
                // oldest run failing the budget on its own.
                if index == 0 {
                    group_bottom_over_budget = Some(OverBudgetRun {
                        run_seq: run.run_seq,
                        level: run.level,
                        rows: run_rows,
                        decoded_bytes: None,
                    });
                }
                break;
            }
            let run_bytes = match decoded_bytes_by_candidate[index] {
                Some(bytes) => bytes,
                None => {
                    let bytes = decoded_group_run_bytes(tables, run, group).await?;
                    decoded_bytes_by_candidate[index] = Some(bytes);
                    bytes
                }
            };
            if decoded_bytes.saturating_add(run_bytes) > byte_budget {
                if index == 0 {
                    group_bottom_over_budget = Some(OverBudgetRun {
                        run_seq: run.run_seq,
                        level: run.level,
                        rows: run_rows,
                        decoded_bytes: Some(run_bytes),
                    });
                }
                break;
            }
            if run.level == CHECKPOINT_L0_RUN_LEVEL {
                folded_l0_rows = folded_l0_rows.saturating_add(run_rows);
            }
            decoded_rows = decoded_rows.saturating_add(run_rows);
            decoded_bytes = decoded_bytes.saturating_add(run_bytes);
            runs.push(run.clone());
        }

        // Every merge writes one base-tier run, so each L0 run it takes
        // leaves L0 for good and the group's L0 count strictly falls. That
        // is the whole progress condition, and it is what makes the fold
        // terminate: a window holding no L0 run would only rewrite base-tier
        // rows where they stand, forever.
        let makes_progress = runs.iter().any(|run| run.level == CHECKPOINT_L0_RUN_LEVEL);
        if !makes_progress {
            continue;
        }
        let run_ids = runs.iter().map(|run| (run.run_seq, run.level)).collect();
        return Ok(ReorganizationSelection {
            input: Some(ReorganizationInput {
                runs,
                run_ids,
                folded_l0_rows,
                decoded_rows,
                decoded_bytes,
                starts_at_group_bottom: window_start == 0,
            }),
            group_bottom_over_budget,
        });
    }

    Ok(ReorganizationSelection {
        input: None,
        group_bottom_over_budget,
    })
}

/// Says out loud that a family group's oldest run no longer fits one
/// reorganization step.
///
/// Nothing is broken when this fires and nothing stalls. The step that logs
/// this line starts a partial fold of the group, which rebuilds it a
/// partition at a time over the steps that follow; those steps report their
/// progress instead of repeating this line. Raising
/// `max_decoded_input_rows_per_step` and `max_decoded_input_bytes_per_step`
/// past the numbers here is what makes a fold like that finish in fewer
/// steps.
///
/// One line per fold: the step after this one has a fold in flight, and a
/// group with a fold in flight never reaches the selection that logs this.
fn report_group_bottom_over_budget(
    namespace_id: &NamespaceId,
    group: &[MetadataTableFamily],
    bottom: &OverBudgetRun,
    policy: MetadataLsmPolicy,
) {
    tracing::warn!(
        namespace_id = namespace_id.as_str(),
        families = ?group,
        run_seq = bottom.run_seq.0,
        run_level = bottom.level,
        run_rows = bottom.rows,
        run_decoded_bytes = bottom.decoded_bytes,
        max_decoded_input_rows_per_step = policy.max_decoded_input_rows_per_step.get(),
        max_decoded_input_bytes_per_step = policy.max_decoded_input_bytes_per_step.get(),
        "the oldest metadata run in this family group no longer fits one reorganization step; \
         the group folds a slice at a time from here, over as many steps as that takes",
    );
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

/// The family group with the most L0 rows still waiting to be folded; ties
/// resolve in group order. `None` when no group has any.
///
/// The L0 rows a partial fold has already taken into its input do not count
/// for the group that fold belongs to. They are work in progress, not work
/// waiting, and counting them would let a fold in flight win every step for
/// as long as it ran: its input is frozen, so its count would never fall,
/// while every other group's falls to zero the moment it folds. Leaving them
/// out is what makes a fold share the maintenance cadence — it takes the
/// steps no other group has work for, plus one for every run that arrives
/// above it while it runs.
pub(super) fn select_family_group(
    payload: &NamespaceManifestPayload,
) -> Option<&'static [MetadataTableFamily]> {
    REORGANIZE_FAMILY_GROUPS
        .into_iter()
        .map(|group| (group_unfolded_l0_rows(payload, group), group))
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

fn group_unfolded_l0_rows(
    payload: &NamespaceManifestPayload,
    group: &[MetadataTableFamily],
) -> u64 {
    let in_flight: &[MetadataRunId] = match &payload.reorganize {
        Some(progress) if progress.families == group => &progress.input_runs,
        _ => &[],
    };
    payload
        .metadata_files
        .iter()
        .filter(|descriptor| {
            descriptor.level == CHECKPOINT_L0_RUN_LEVEL && group.contains(&descriptor.family)
        })
        .filter(|descriptor| {
            !in_flight
                .iter()
                .any(|run| run.run_seq == descriptor.run_seq && run.level == descriptor.level)
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
) -> Result<NamespaceManifestEnvelope> {
    // One generated object id, one write. The generated id ends in 16 random
    // hex characters, so the key is this unit's alone and a conflict under it
    // is corruption rather than contention.
    let manifest_id = next_manifest_id_after(previous.payload.manifest_id)?;
    let manifest = NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_id,
        manifest_object_id: ManifestObjectId::generate(manifest_id),
        head_seq: previous.payload.head_seq,
        head_commit_id: previous.payload.head_commit_id.clone(),
        base_seq,
        writer_epoch: previous.payload.writer_epoch,
        next_inode_id: previous.payload.next_inode_id,
        retention_floor_seq,
        metadata_files,
        // A whole-group fold only ever runs for a group with no partial
        // fold in flight, so any fold the manifest carries belongs to some
        // other group and travels forward untouched. Dropping it here would
        // strand that group's outputs and leave its base frozen.
        reorganize: previous.payload.reorganize.clone(),
    })
    .map_err(|err| CoreError::Internal(format!("failed to build reorganized manifest: {err}")))?;
    write_namespace_manifest(store, &manifest)
        .await
        .map_err(manifest_write_failure)?;
    Ok(manifest)
}

/// Drops rows that no retained sequence can observe (format spec,
/// "Compaction"). Conservative subset: superseded or unbound bindings and
/// spent unbind markers at or below the retention floor, and cancelled
/// active-deletion pairs. Revision rows are never dropped — file history is
/// durable data retained independently of the replay floor — and tombstone
/// and inode rows are always retained until reachability-based dropping is
/// designed.
pub(super) fn drop_rows_below_retention_floor(
    rows_by_family: &mut BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
    retention_floor_seq: ChangeSeq,
) -> Result<()> {
    let unbound_at_floor = unbindings_at_or_below_floor(
        rows_by_family
            .get(&MetadataTableFamily::DirentryUnbinds)
            .map_or(&[], Vec::as_slice),
        retention_floor_seq,
    );
    drop_rows_below_frozen_floor(rows_by_family, retention_floor_seq, &unbound_at_floor)
}

/// Identifies one binding generation, which is what an unbind names and what
/// the bind drop matches on.
///
/// Identity here omits `child_inode_id` (the read path also matches it); the
/// 4-tuple is already unique for writer-produced rows, so the predicates
/// agree on every legal history.
pub(super) type BindingGeneration = (InodeId, NameKey, ChangeSeq, u32);

/// The binding generations an unbind at or below `retention_floor_seq`
/// retires.
///
/// A whole-group fold builds this from its merged rows. A partial fold
/// builds it once at the start from the whole snapshot, because the slice it
/// is working on holds only part of the unbind family (design doc,
/// "Retention during the walk").
pub(super) fn unbindings_at_or_below_floor(
    unbind_rows: &[MetadataRow],
    retention_floor_seq: ChangeSeq,
) -> BTreeSet<BindingGeneration> {
    let mut unbound_at_floor = BTreeSet::new();
    for row in unbind_rows {
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
    unbound_at_floor
}

/// [`drop_rows_below_retention_floor`] against a floor and an unbind set the
/// caller froze, rather than against rows it can see right now.
///
/// A partial fold calls this per slice: the floor and the unbind set are
/// fixed for the whole walk, so every step and every resumption decides
/// identically. The families the caller leaves out of `rows_by_family` are
/// untouched, which is how a walk keeps the reverse bind index out of this
/// pass — its rows are keyed by child, so the slice holding one does not
/// hold the forward binds the rule below reads.
pub(super) fn drop_rows_below_frozen_floor(
    rows_by_family: &mut BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
    retention_floor_seq: ChangeSeq,
    unbound_at_floor: &BTreeSet<BindingGeneration>,
) -> Result<()> {
    // At the floor only the latest non-unbound bind per (parent, name) slot
    // is visible; an unbind marker at or below the floor has finished its
    // work once every bind it covered is gone.
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

    // Active deletions are current state, not history, so the retention
    // floor has NO say over them: a deletion stays listed and recoverable
    // however far the floor advances — that is the product promise, and
    // dropping a row at the floor would silently retire a recoverable
    // deletion. The only rows that go are the pairs that cancelled each
    // other. A removal marker's listed row is always in the same merged set:
    // the deletion commits before the undelete, runs merge oldest-first, and
    // the selected subset is a prefix of that order, so a marker can never
    // outlive the row it names.
    if let Some(rows) = rows_by_family.get_mut(&MetadataTableFamily::ActiveDeletions) {
        let revoked: BTreeSet<(ChangeSeq, InodeId)> = rows
            .iter()
            .filter_map(|row| match row {
                MetadataRow::ActiveDeletion {
                    root_inode_id,
                    deleted_at_seq,
                    action: ActiveDeletionRowAction::Removed { .. },
                } => Some((*deleted_at_seq, *root_inode_id)),
                _ => None,
            })
            .collect();
        rows.retain(|row| match row {
            MetadataRow::ActiveDeletion {
                root_inode_id,
                deleted_at_seq,
                action,
            } => match action {
                ActiveDeletionRowAction::Removed { .. } => false,
                ActiveDeletionRowAction::Listed { .. } => {
                    !revoked.contains(&(*deleted_at_seq, *root_inode_id))
                }
            },
            _ => true,
        });
    }

    // The idempotency horizon is the retention floor: a receipt dropped
    // here makes its id indistinguishable from one never used, so a commit
    // retried from below the floor commits AGAIN as a new mutation (format
    // spec §3.3; pinned by `a_retry_past_the_receipt_horizon_commits_again`).
    // Replay is guaranteed exactly as long as retained history.
    if let Some(rows) = rows_by_family.get_mut(&MetadataTableFamily::CommitReceipts) {
        rows.retain(|row| match row {
            MetadataRow::CommitReceipt { committed_seq, .. } => {
                *committed_seq >= retention_floor_seq
            }
            _ => true,
        });
    }

    drop_superseded_attribute_revisions(rows_by_family, retention_floor_seq)?;
    Ok(())
}

/// Keeps every attribute revision above the retention floor, plus the newest
/// revision at or below it per inode, and drops the rest.
///
/// An attribute revision is current state, not history: the newest one for an
/// inode is what every read answers with, and no retained sequence can
/// observe an older one once a newer one is at or below the floor. The
/// newest-at-floor row is kept even when its map is empty, because an empty
/// map is the cleared state — dropping it would let an older non-empty map
/// become the newest row and resurrect attributes a caller cleared.
///
/// Attributes are never dropped for being unreachable. A deleted inode keeps
/// its rows, the same posture inode and tombstone rows take, and that is what
/// makes an undelete give back the map the inode had.
fn drop_superseded_attribute_revisions(
    rows_by_family: &mut BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
    retention_floor_seq: ChangeSeq,
) -> Result<()> {
    let Some(rows) = rows_by_family.get_mut(&MetadataTableFamily::Attributes) else {
        return Ok(());
    };
    // Writer invariant: one inode's attribute revisions are
    // numbered without gaps or repeats, so "the newest at or below the floor"
    // names exactly one row. Two rows sharing a number would make the choice
    // arbitrary and the drop unsafe; refuse to compact state that violates
    // it.
    let mut newest_at_floor = BTreeMap::<InodeId, AttributeRevisionNo>::new();
    let mut seen_at_floor = BTreeSet::<(InodeId, AttributeRevisionNo)>::new();
    for row in rows.iter() {
        let MetadataRow::AttributesRevision {
            inode_id,
            attributes_revision_no,
            committed_seq,
            ..
        } = row
        else {
            continue;
        };
        if *committed_seq > retention_floor_seq {
            continue;
        }
        if !seen_at_floor.insert((*inode_id, *attributes_revision_no)) {
            return Err(CoreError::NamespaceCorrupt(format!(
                "inode `{inode_id}` has two attribute rows at revision `{attributes_revision_no}` at or below the retention floor; refusing to drop rows"
            )));
        }
        let newest = newest_at_floor
            .entry(*inode_id)
            .or_insert(*attributes_revision_no);
        if attributes_revision_no > newest {
            *newest = *attributes_revision_no;
        }
    }

    rows.retain(|row| match row {
        MetadataRow::AttributesRevision {
            inode_id,
            attributes_revision_no,
            committed_seq,
            ..
        } => {
            *committed_seq > retention_floor_seq
                || newest_at_floor.get(inode_id) == Some(attributes_revision_no)
        }
        _ => true,
    });
    Ok(())
}
