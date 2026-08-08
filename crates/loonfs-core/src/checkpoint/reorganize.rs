//! Bounded metadata reorganization: the background half of the
//! checkpoint/compaction split.
//!
//! Checkpoint publication only ever appends an L0 delta run, so its cost
//! follows the WAL delta, never the namespace. Folding those L0 runs into
//! the base happens here instead, one **family group** at a time: the
//! group's rows are merged from an oldest-first, budgeted subset of complete
//! runs, rows no retained sequence can observe are dropped, new segments are
//! written, and a manifest publishes that swaps just those references.
//! Families whose rows must stay mutually consistent compact together —
//! bind, child bind, and unbind rows form one group because their drop rules
//! read each other; revisions travel with their descending index so index
//! parity holds within every unit.
//!
//! A merge writes a base-tier run when, and only when, its window starts at
//! the group's oldest run. That is the same condition retention dropping
//! needs, so the level a run carries and the rules that produced it say the
//! same thing: a base run is a run some fold was allowed to drop rows from, a
//! delta run is a run nothing has dropped from yet. A merge that had to start
//! above the group's base therefore writes a bigger delta run rather than a
//! second base run, and a family group holds at most one base run at any
//! time (`super::load::validate_manifest_table_descriptors` refuses a
//! manifest that says otherwise).
//!
//! There is no progress record: each unit ends in a durable manifest, so a
//! crashed or interrupted reorganization resumes by reading the live
//! manifest and picking the next group that still has L0 rows. Unit
//! selection is deterministic (most L0 rows first, then group order). A
//! concurrent checkpoint racing a unit wins at the root compare-and-swap;
//! the unit's segments are left unreferenced for garbage collection and the
//! next step retries against the fresh manifest.
//!
//! A group that can no longer be folded from its oldest run in one step folds
//! a slice at a time instead ([`super::partial_fold`]). That fold does keep a
//! progress record, in the manifest, because its output arrives in pieces.
//! While it is in flight its group folds no other way: a step that selects
//! the group advances the fold and does nothing else for it. Group selection
//! still runs per step, so the other groups keep folding on the steps they
//! win.
//!
//! A manifest carries one such fold at a time. A second group in the same
//! position waits its turn: it merges its delta runs into fewer, bigger delta
//! runs, and its fold starts once the slot is free. A group that has merged
//! them down to one has nothing left to do that way, and the step it wins
//! goes to the fold in flight instead.

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
    /// One bounded complete-run subset for a family group merged into one new
    /// run and the manifest advanced. The new run is the group's base when
    /// the subset started at the group's oldest run, and a bigger delta run
    /// otherwise.
    UnitPublished {
        families: Vec<MetadataTableFamily>,
        folded_l0_rows: u64,
        input_runs: usize,
        decoded_input_rows: u64,
        decoded_input_bytes: u64,
        manifest_id: ManifestId,
    },
    /// One slice of a partial fold merged and published. The group cannot be
    /// folded from its oldest run in one step, so it is being folded a
    /// partition at a time and this step moved that fold along.
    PartialFoldAdvanced {
        families: Vec<MetadataTableFamily>,
        /// Partitions of the group's keyspace this slice covered.
        partitions: u64,
        decoded_input_rows: u64,
        decoded_input_bytes: u64,
        /// Rows this slice wrote into the run the fold is building.
        output_rows: u64,
        /// Point reads this slice made into the snapshot to decide bind rows
        /// whose unbinds sit outside it.
        unbind_probes: u64,
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
    /// The trigger fired, but no oldest-first subset of this group's runs
    /// would make progress, and nothing said the group needs folding a slice
    /// at a time either. A budget that stopped the window says that, so what
    /// is left here is a per-step run limit too small to reach a delta run:
    /// the step has nothing to do for the group and nothing to blame.
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
        && !folding_started_above_unfolded_delta_runs(tables.scan_runs.as_ref())
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
        // The group's oldest run no longer fits one step. Say so, then start
        // folding the group a slice at a time; from the next step on the
        // fold's progress is what this group reports.
        report_group_bottom_over_budget(namespace_id, group, &bottom, policy);
        // Unless another group is already folding. A manifest carries one
        // partial fold at a time, so this group waits its turn: until the
        // fold in flight completes and frees the slot, it merges its delta
        // runs the way it did before partial folds existed. Starting a
        // second fold here would replace the state in flight, and two
        // over-budget groups would take turns discarding each other's work
        // with neither ever finishing.
        if previous.payload.reorganize.is_none() {
            return start_partial_fold(store, namespace_id, &tables, group, policy, context, timer)
                .await;
        }
    }
    let Some(input) = selection.input else {
        // Nothing this group can merge this step. A group waiting for the
        // fold slot reaches this once it has merged its delta runs down to
        // one: its base needs a fold and the slot is taken, and merging one
        // delta run into itself would rewrite every row for nothing. The
        // step spends itself on the fold in flight instead, which is the one
        // piece of work that moves this group's turn closer.
        if folding_group.is_some() {
            return advance_partial_fold(store, namespace_id, &tables, policy, context, timer)
                .await;
        }
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
        input.output.run_seq,
        input.output.level,
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
    let Some(mut walk) = MetadataFoldWalk::resume_from_manifest(tables)? else {
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
/// reads back. Starting reads nothing of its own: everything a slice needs
/// beyond its own rows is a point read the slice pays for.
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
    let mut walk = MetadataFoldWalk::start(tables, group, snapshot, frozen_floor_seq)?;
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
                unbind_probes: slice.unbind_probes,
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
    /// The run identity this merge writes its output at, decided by
    /// [`merge_output_run`].
    pub(super) output: MetadataRunId,
}

/// The run identity a merge writes its output at.
///
/// **A merge's output is base-tier if and only if its window is
/// bottom-anchored.** Base-tier means "rows a fold was allowed to drop from",
/// which is exactly what [`ReorganizationInput::starts_at_group_bottom`]
/// decides, so the level a run carries and the rules that produced it say the
/// same thing.
///
/// A bottom-anchored merge writes the group's base run, stamped at the
/// manifest head. Base-tier runs sort below every delta run whatever sequence
/// they carry, so the output lands at the bottom where its inputs were and
/// the sequence is free to be the head's — which is also what keeps a file at
/// `head_seq` in the manifest when the merge consumed the group's whole top.
/// It replaces the group's previous base run, because a bottom-anchored
/// window always contains it, so the group is left with exactly one.
///
/// A merge that had to start above the group's base rewrites delta runs into
/// one bigger delta run. Two things follow from writing it at the delta level
/// instead of the base level. It cannot mint a second base run, so a group's
/// base never fragments and the trigger that folds a group in slices weighs
/// the whole base rather than one fragment of it. And its rows stay counted
/// as delta pressure, so the L0 run count reports what is really waiting
/// rather than hiding merged runs at the base tier.
///
/// Its sequence is its newest input's, not the manifest head's, so the output
/// stands exactly where that run stood: above every run the window left below
/// it and below every run it left above. The head would put it above runs the
/// window did not reach, and moving merged rows above newer runs is what a
/// later bottom-anchored fold must never see — it could then drop an unbind
/// whose bind had moved out of the window and put a deleted file back.
///
/// Nothing else in the manifest already holds that identity for these
/// families: the newest input run's segments for this group leave
/// `metadata_files` in the same publication the output's enter it. Segments
/// of other families at that identity stay where they are, which is ordinary
/// — a run is a set of families, and each family in it has one producer.
fn merge_output_run(
    runs: &[MetadataRunManifest],
    starts_at_group_bottom: bool,
    head_seq: ChangeSeq,
) -> MetadataRunId {
    if starts_at_group_bottom {
        return MetadataRunId {
            run_seq: head_seq,
            level: CHECKPOINT_BASE_RUN_LEVEL,
        };
    }
    MetadataRunId {
        run_seq: runs.iter().map(|run| run.run_seq).max().unwrap_or(head_seq),
        level: CHECKPOINT_L0_RUN_LEVEL,
    }
}

/// What one selection attempt found.
///
/// The saturation record travels beside the input rather than replacing it.
/// A group that cannot be folded from its oldest run in one step is folded a
/// slice at a time instead, which is the caller's decision to make; the
/// delta-only window this search still finds is what such a group merges
/// while it waits for the fold slot.
pub(super) struct ReorganizationSelection {
    pub(super) input: Option<ReorganizationInput>,
    /// What stopped the window that starts at the group's oldest run, when
    /// the group needs folding a slice at a time. Set when that run does not
    /// fit one step on its own, and also when no window makes progress at
    /// all — see [`select_reorganization_input`].
    pub(super) group_bottom_over_budget: Option<OverBudgetRun>,
}

/// A run that stopped a window: it does not fit one step's budgets beside
/// what the window already held, and at index zero that means on its own.
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
/// sit under every delta run whatever their `run_seq` says: a bottom-anchored
/// fold stamps its output at the manifest head, which can leave a base run
/// carrying a higher `run_seq` than a delta run some other group has not
/// folded yet (see [`folding_started_above_unfolded_delta_runs`]). Within one
/// tier the lower `run_seq` is the older run.
///
/// **The invariant.** A merge writes its inputs back as one run standing
/// where the window stood: at the bottom of the group when the window is
/// bottom-anchored, and at its newest input's identity otherwise
/// ([`merge_output_run`]). Either way the output may not carry a row newer
/// than a run it left above the window — otherwise a later fold could drop a
/// row while the row it cancels sits outside that fold's window. What keeps
/// that true is that the window is a *contiguous* slice of the order: every
/// run it leaves out sits wholly below it or wholly above it, never
/// interleaved, so the output can stand in for the whole window without
/// moving any row past any other. When the window starts at the very bottom
/// nothing is left out below it at all, and that is the stronger property
/// retention dropping needs — see
/// [`ReorganizationInput::starts_at_group_bottom`].
///
/// **There are two windows to try.** A manifest holds at most one base-tier
/// run per family group, so the search is short: the window starting at the
/// group's oldest run, and — when a base run blocks that one — the window
/// starting at the oldest delta run above it. A delta run is never stepped
/// over.
///
/// **The budgets pace the work; they never end it.** When no window starting
/// at the group's oldest run can fit the budgets, the start moves past the
/// base run that blocks it and the window merges the delta runs above it on
/// their own, so the group keeps shedding runs. The caller has a better
/// answer for that case and takes it — it folds the group a slice at a time
/// instead ([`super::partial_fold`]) — but the window is still what this
/// search reports, and it is still the one the group would merge if the
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
    let head_seq = tables.manifest().payload.head_seq;
    let row_budget =
        u64::try_from(policy.max_decoded_input_rows_per_step.get()).unwrap_or(u64::MAX);
    let byte_budget =
        u64::try_from(policy.max_decoded_input_bytes_per_step.get()).unwrap_or(u64::MAX);
    let max_runs = policy.max_input_runs_per_step.get();
    // Base-tier runs sort first, so this is the index of the oldest delta
    // candidate, and starting there is what skips a base run the budgets
    // cannot admit. A group has at most one base run, so this is one place
    // and the only alternative to the bottom.
    let first_delta_candidate = candidates
        .iter()
        .take_while(|run| run.level != CHECKPOINT_L0_RUN_LEVEL)
        .count();
    let delta_only_start = (first_delta_candidate > 0 && first_delta_candidate < candidate_count)
        .then_some(first_delta_candidate);
    // Reading a run's decoded byte total costs its index sections, so each
    // candidate's total is read at most once however many windows weigh it.
    let mut decoded_bytes_by_candidate = vec![None::<u64>; candidate_count];
    // What stopped the window that starts at the group's oldest run, and how
    // far into that window it stood. Position zero means the oldest run does
    // not fit one step on its own, which is reported whatever else the search
    // finds; a later position means the oldest run fits but cannot be read
    // together with what sits above it, which is only reported when no window
    // makes progress at all. A group in that second position still has its
    // delta runs to merge, and doing that first is cheaper than folding the
    // group in slices.
    let mut bottom_window_blocked_by: Option<(usize, OverBudgetRun)> = None;

    for window_start in std::iter::once(0).chain(delta_only_start) {
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
                if window_start == 0 {
                    bottom_window_blocked_by = Some((
                        index,
                        OverBudgetRun {
                            run_seq: run.run_seq,
                            level: run.level,
                            rows: run_rows,
                            decoded_bytes: None,
                        },
                    ));
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
                if window_start == 0 {
                    bottom_window_blocked_by = Some((
                        index,
                        OverBudgetRun {
                            run_seq: run.run_seq,
                            level: run.level,
                            rows: run_rows,
                            decoded_bytes: Some(run_bytes),
                        },
                    ));
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

        // A merge must leave the group with less to fold than it found, or
        // the same window would be chosen again forever. What counts as less
        // follows from the level the output gets.
        //
        // A bottom-anchored merge writes a base run, so every delta run it
        // takes leaves the delta tier for good: one delta run in the window
        // is enough, and a window holding none would only rewrite base rows
        // where they stand.
        //
        // A merge above the base writes another delta run, so it only gains
        // by merging two or more into one. A single-run window there would
        // rewrite that run as itself, at its own identity, having read and
        // written every row for nothing.
        let starts_at_group_bottom = window_start == 0;
        let makes_progress = if starts_at_group_bottom {
            runs.iter().any(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
        } else {
            runs.len() > 1
        };
        if !makes_progress {
            continue;
        }
        let run_ids = runs.iter().map(|run| (run.run_seq, run.level)).collect();
        let output = merge_output_run(&runs, starts_at_group_bottom, head_seq);
        return Ok(ReorganizationSelection {
            input: Some(ReorganizationInput {
                runs,
                run_ids,
                folded_l0_rows,
                decoded_rows,
                decoded_bytes,
                starts_at_group_bottom,
                output,
            }),
            // Position zero is only ever reached by the bottom-anchored
            // window, whose accumulator is still empty there, so it is the
            // group's oldest run failing the budget on its own.
            group_bottom_over_budget: blocker_at_the_group_bottom(bottom_window_blocked_by),
        });
    }

    // No window makes progress. The group's oldest run cannot be merged with
    // anything above it, and there is no pair of delta runs to merge either,
    // so folding the group a slice at a time is the only move left. That is
    // reported here even when the run that stopped the bottom window was not
    // the group's oldest: the group is just as unfoldable either way, and a
    // group left to report a blocked step every step would never drop a row
    // again.
    Ok(ReorganizationSelection {
        input: None,
        group_bottom_over_budget: bottom_window_blocked_by.map(|(_, blocker)| blocker),
    })
}

/// The blocker that stopped the bottom-anchored window at its very first run,
/// which is the reading that means "the group's oldest run does not fit one
/// step on its own".
fn blocker_at_the_group_bottom(
    bottom_window_blocked_by: Option<(usize, OverBudgetRun)>,
) -> Option<OverBudgetRun> {
    bottom_window_blocked_by
        .filter(|(index, _)| *index == 0)
        .map(|(_, blocker)| blocker)
}

/// Says out loud that a family group can no longer be folded from its oldest
/// run in one reorganization step.
///
/// The run named here is what stopped the window: the group's oldest run
/// failing the budget on its own, or — when no window makes progress at all —
/// the run above it that would not fit beside it.
///
/// Nothing is broken when this fires and nothing stalls. The step that logs
/// this line starts a partial fold of the group, which rebuilds it a
/// partition at a time over the steps that follow; those steps report their
/// progress instead of repeating this line. Raising
/// `max_decoded_input_rows_per_step` and `max_decoded_input_bytes_per_step`
/// past the numbers here is what makes a fold like that finish in fewer
/// steps.
///
/// Usually one line per fold: the step after this one has a fold in flight,
/// and a group with a fold in flight never reaches the selection that logs
/// this. A group waiting for another group's fold to finish keeps logging it
/// on the steps it wins, which is what says the group is still over budget
/// and still waiting.
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
        "this family group can no longer be folded from its oldest run in one reorganization \
         step; the run named here is what stopped the window, and the group folds a slice at a \
         time from here, over as many steps as that takes",
    );
}

/// Whether a fold has already run against this manifest head while delta
/// runs are still waiting to be folded.
///
/// A bottom-anchored fold stamps its base run at the manifest head, which is
/// at or above every delta run's sequence. So a base run sitting at or above
/// the oldest delta run says one group folded here and delta runs remain —
/// usually other groups' rows in the very runs it just took its own rows out
/// of. The step keeps going on that evidence rather than stopping at the
/// trigger, which is what makes a run of bounded steps end in the same
/// manifest shape one unbounded step would have produced.
///
/// This used to carry a second job. A merge above the base wrote its output
/// at the base tier too, so its inputs stopped being counted as delta runs
/// and the trigger under-reported what was waiting; this predicate was what
/// kept such a group folding anyway. That job is gone: a merge above the base
/// now writes a delta run ([`merge_output_run`]), so the L0 run count says
/// what is really there.
///
/// A fresh delta run appended after a completed fold is strictly newer than
/// every base run and therefore does not bypass the normal trigger.
fn folding_started_above_unfolded_delta_runs(runs: &[MetadataRunManifest]) -> bool {
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
    drop_rows_below_frozen_floor(
        rows_by_family,
        retention_floor_seq,
        &unbound_at_floor,
        FrozenFloorScope::WholePartitions,
    )
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
/// A whole-group fold builds this from its merged rows, and so does a
/// partial fold for the rows of one slice: binds and the unbinds that retire
/// them share a partition, so a slice holding whole partitions holds both
/// halves of every pair its rows belong to (design doc, "Retention during
/// the walk").
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

/// Whether the rows handed to [`drop_rows_below_frozen_floor`] cover whole
/// partitions of their family group, or one bounded piece of a single
/// partition.
///
/// Some drop rules read a row's neighbours inside its own partition: the
/// bind rule reads the other binds in the slot, the active-deletion rule
/// reads the marker that cancels a listed row, and the attribute rule reads
/// the other revisions of the inode. Those rules only run when the caller
/// holds whole partitions. The rules that decide a row on its own run either
/// way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FrozenFloorScope {
    /// Every row of every partition the rows touch is present.
    WholePartitions,
    /// One bounded piece of one partition, which a partial fold reads when
    /// that partition alone is larger than one step.
    PartitionPiece,
}

/// [`drop_rows_below_retention_floor`] against a floor and an unbind set the
/// caller froze, rather than against rows it can see right now.
///
/// A partial fold calls this per slice: the floor is fixed for the whole
/// walk, so every step and every resumption decides identically. The
/// families the caller leaves out of `rows_by_family` are untouched, which is
/// how a walk keeps the reverse bind index out of this pass — its rows are
/// keyed by child, so the slice holding one does not hold the forward binds
/// the invariant check below reads, and the walk decides those rows with a
/// point read instead.
pub(super) fn drop_rows_below_frozen_floor(
    rows_by_family: &mut BTreeMap<MetadataTableFamily, Vec<MetadataRow>>,
    retention_floor_seq: ChangeSeq,
    unbound_at_floor: &BTreeSet<BindingGeneration>,
    scope: FrozenFloorScope,
) -> Result<()> {
    if scope == FrozenFloorScope::WholePartitions {
        refuse_superseded_bind_without_unbind(
            rows_by_family
                .get(&MetadataTableFamily::DirentryBinds)
                .map_or(&[], Vec::as_slice),
            retention_floor_seq,
            unbound_at_floor,
        )?;
    }
    for family in [
        MetadataTableFamily::DirentryBinds,
        MetadataTableFamily::DirentryChildBinds,
    ] {
        if let Some(rows) = rows_by_family.get_mut(&family) {
            rows.retain(|row| {
                bind_survives_frozen_floor(row, retention_floor_seq, unbound_at_floor)
            });
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
    if let Some(rows) = rows_by_family
        .get_mut(&MetadataTableFamily::ActiveDeletions)
        .filter(|_| scope == FrozenFloorScope::WholePartitions)
    {
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

    if scope == FrozenFloorScope::WholePartitions {
        drop_superseded_attribute_revisions(rows_by_family, retention_floor_seq)?;
    }
    Ok(())
}

/// Whether one bind row survives the frozen floor.
///
/// A bind above the floor always survives. At or below it the bind survives
/// exactly when nothing retired it, because a bind is only ever superseded by
/// an operation that also unbinds it — the writer invariant
/// [`refuse_superseded_bind_without_unbind`] refuses to compact without.
///
/// Both bind families read this same rule, which is what keeps them dropping
/// in lockstep: the format gives every bind row exactly one reverse row, and
/// a run whose two counts disagree does not load. They reach the rule by
/// different routes — the forward row's unbinds share its partition, the
/// reverse row's do not — but the rule is one function and the answer is one
/// answer.
pub(super) fn bind_survives_frozen_floor(
    row: &MetadataRow,
    retention_floor_seq: ChangeSeq,
    unbound_at_floor: &BTreeSet<BindingGeneration>,
) -> bool {
    let MetadataRow::DirentryBind {
        parent_inode_id,
        name_key,
        bind_seq,
        bind_delta_index,
        ..
    } = row
    else {
        return true;
    };
    *bind_seq > retention_floor_seq
        || !unbound_at_floor.contains(&(
            *parent_inode_id,
            name_key.clone(),
            *bind_seq,
            *bind_delta_index,
        ))
}

/// Refuses to compact bind rows that break the writer invariant the drop
/// rests on.
///
/// At the floor only the latest bind per (parent, name) slot is visible, and
/// a bind is only ever superseded by an operation that also unbinds it. So
/// every non-latest bind at or below the floor must have a matching unbind at
/// or below the floor. Where that does not hold, dropping by
/// [`bind_survives_frozen_floor`] would keep a bind no read can reach and
/// call it live, so the compaction stops instead.
fn refuse_superseded_bind_without_unbind(
    bind_rows: &[MetadataRow],
    retention_floor_seq: ChangeSeq,
    unbound_at_floor: &BTreeSet<BindingGeneration>,
) -> Result<()> {
    let mut latest_bind_at_floor = BTreeMap::new();
    for row in bind_rows {
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
    for row in bind_rows {
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
