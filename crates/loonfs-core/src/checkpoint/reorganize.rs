//! Plans and publishes bounded metadata merges.
//!
//! Each step selects one family group and either merges a bounded run window
//! or requests a full background compaction. Base merges may apply retention;
//! delta merges preserve every row. Both paths use the same merge engine.
//!
//! Every successful merge publishes a new manifest. Interrupted or losing
//! publications leave unreferenced output for garbage collection.
//! [`FrozenBasePolicy`] decides when a blocked base requires full compaction.

use super::block_fetch::load_segment_index_for_reorganization;
use super::error::ManifestLoadError;
use super::flush::{ensure_metadata_publication_budget, next_manifest_no_after, next_run_no_after};
use super::load::load_verified_manifest_segments;
use super::publish::{
    manifest_write_failure, publish_metadata_root, write_namespace_manifest,
    ManifestPublicationOutcome,
};
use super::runs::{
    delta_run_count, runs_in_fold_order, MetadataFamilyGroup, MetadataLsmPolicy,
    MetadataRunManifest, REORGANIZE_FAMILY_GROUPS,
};
use super::scan::VerifiedMetadataSegments;
use super::streaming_compaction::{merge_group_in_step, MetadataCompactionSpec};
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::control_snapshot::load_control_snapshot;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::manifest::{
    MetadataRunRef, MetadataSegmentRef, NamespaceManifestEnvelope, NamespaceManifestPayload,
    RunTier,
};
use loonfs_api::{ChangeSeq, ManifestNo, ManifestObjectId, NamespaceId, RunNo};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};

/// Delta merges allowed over a frozen base before full compaction.
pub(super) const DELTA_MERGES_OVER_A_FROZEN_BASE: u32 = 2;

/// How planning treats a family group whose base run is frozen.
///
/// Selects immediate or amortized compaction for a frozen base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrozenBasePolicy {
    /// Merge newer delta runs before requesting a full compaction.
    /// Counts are per family group and may be rebuilt after restart.
    Amortized {
        published_delta_merges_over_frozen_base: BTreeMap<MetadataFamilyGroup, u32>,
    },
    /// Request full compaction as soon as the base is blocked.
    CompactImmediately,
}

impl Default for FrozenBasePolicy {
    fn default() -> Self {
        Self::Amortized {
            published_delta_merges_over_frozen_base: BTreeMap::new(),
        }
    }
}

impl FrozenBasePolicy {
    /// Whether a blocked bottom-anchored window goes straight to the job
    /// rather than taking the delta merge above it.
    fn compact_a_frozen_base(&self, group: MetadataFamilyGroup) -> bool {
        match self {
            Self::Amortized {
                published_delta_merges_over_frozen_base,
            } => {
                published_delta_merges_over_frozen_base
                    .get(&group)
                    .copied()
                    .unwrap_or(0)
                    >= DELTA_MERGES_OVER_A_FROZEN_BASE
            }
            Self::CompactImmediately => true,
        }
    }
}

/// What the process running maintenance knows about one namespace's
/// streaming compactions, which one step's read of durable state cannot see.
#[derive(Debug, Clone, Default)]
pub struct MetadataCompactionView<'a> {
    /// The plan of the job running for this namespace, or `None` when it is
    /// running none. The step leaves that job's group alone and folds
    /// another, because every run of that group is in the job's snapshot and
    /// merging one would waste the whole job at finalization.
    ///
    /// In-memory process state, and safe to lose: a restart forgets the
    /// active job and a later step plans it again.
    pub active: Option<&'a MetadataCompactionSpec>,
    /// What this caller wants done about a group whose base is frozen.
    pub frozen_base: FrozenBasePolicy,
}

/// What one reorganization step did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataReorganizeOutcome {
    /// The manifest's delta run count is below the policy trigger; nothing to
    /// fold yet.
    NotNeeded { delta_runs: usize },
    /// One bounded complete-run subset for a family group merged into one
    /// new run and the manifest advanced. The new run is the group's base
    /// when the subset started at the group's oldest run, and a bigger delta
    /// run otherwise.
    UnitPublished {
        /// The group this unit merged, which is what a runtime keys its
        /// per-group bookkeeping by. Its families are
        /// [`MetadataFamilyGroup::families`].
        group: MetadataFamilyGroup,
        merged_delta_rows: u64,
        input_runs: usize,
        decoded_input_rows: u64,
        decoded_input_bytes: u64,
        manifest_no: ManifestNo,
        /// True when the window starting at the group's oldest run could not
        /// reach a delta run, so this merge ran above a frozen base and the
        /// group's retention is still stopped. A runtime that amortizes counts
        /// these per group and hands the count back in
        /// [`FrozenBasePolicy::Amortized`], which is what decides when the
        /// planner stops merging deltas over that base and plans the job that
        /// unfreezes it.
        bottom_anchored_merge_blocked: bool,
    },
    /// No oldest-first subset that would make progress fits the hard
    /// per-step input budgets, so the group is rebuilt by a streaming
    /// compaction over every run it holds. The step publishes nothing and
    /// hands the plan back; starting the job is the runtime's.
    CompactionPlanned {
        group: MetadataFamilyGroup,
        spec: MetadataCompactionSpec,
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

fn report(
    namespace_id: &NamespaceId,
    outcome: MetadataReorganizeOutcome,
) -> MetadataReorganizeReport {
    MetadataReorganizeReport {
        namespace_id: namespace_id.clone(),
        outcome,
    }
}

/// Runs at most one reorganization step against the current manifest. Each
/// call reloads durable state, so callers may repeat it safely across process
/// restarts.
///
/// `compactions` is what this process knows about the namespace's streaming
/// compactions: which group a job is rebuilding right now, and how long each
/// group has been merging deltas over a frozen base.
#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "reorganize_metadata", key_class = "namespace_manifest")
)]
pub(crate) async fn reorganize_metadata_step<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
    compactions: MetadataCompactionView<'_>,
) -> Result<MetadataReorganizeReport> {
    let timer = StdMonotonicTimer::default();
    reorganize_metadata_step_with_timer(store, namespace_id, context, policy, compactions, &timer)
        .await
}

pub(super) async fn reorganize_metadata_step_with_timer<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
    compactions: MetadataCompactionView<'_>,
    timer: &dyn MonotonicTimer,
) -> Result<MetadataReorganizeReport> {
    // The publication budget covers the whole unit: measurement starts
    // before any segment object is written and gates the root
    // compare-and-swap below.
    let publication_started_ms = timer.monotonic_now_ms();
    // A streaming compaction keeps the floor read with this root.
    let snapshot = load_control_snapshot(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let floor_seq = snapshot.retention_floor_seq;
    // A namespace that has published no manifest of its own has no runs to
    // fold: reorganization has nothing to do until its first flush.
    let Some(root) = snapshot.root.map(|loaded| loaded.state) else {
        return Ok(report(
            namespace_id,
            MetadataReorganizeOutcome::NotNeeded { delta_runs: 0 },
        ));
    };
    let segments = load_verified_manifest_segments(
        store,
        None,
        namespace_id,
        &root.manifest.manifest_object_id,
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
    })?;
    let previous = segments.manifest();

    let delta_runs = delta_run_count(&previous.payload);
    if delta_runs < policy.max_delta_runs.get()
        && !manifest_has_partial_reorganization(segments.scan_runs.as_ref())
    {
        return Ok(report(
            namespace_id,
            MetadataReorganizeOutcome::NotNeeded { delta_runs },
        ));
    }
    let Some(group) = select_family_group(
        &previous.payload,
        compactions.active.map(MetadataCompactionSpec::group),
    ) else {
        // Delta runs exist but hold no rows (empty families), or the only group
        // with rows is the one a job is rebuilding; nothing to fold here.
        return Ok(report(
            namespace_id,
            MetadataReorganizeOutcome::NotNeeded { delta_runs },
        ));
    };
    let selection = select_reorganization_input(
        &segments,
        group,
        policy,
        floor_seq,
        &compactions.frozen_base,
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
    })?;
    if let Some(bottom) = selection.group_bottom_over_budget {
        report_group_bottom_over_budget(namespace_id, group, &bottom, policy);
    }
    let bottom_anchored_merge_blocked = selection.bottom_anchored_merge_blocked;
    let input = match selection.plan {
        ReorganizationPlan::BoundedMerge(input) => input,
        ReorganizationPlan::FullCompaction(spec) => {
            return Ok(report(
                namespace_id,
                MetadataReorganizeOutcome::CompactionPlanned { group, spec },
            ))
        }
        ReorganizationPlan::Nothing => {
            return Ok(report(
                namespace_id,
                MetadataReorganizeOutcome::NotNeeded { delta_runs },
            ))
        }
    };

    // Merge only the selected complete runs, through the one engine both
    // reorganization paths run ([`super::streaming_compaction`]). It reads
    // exactly the manifest's segments — never the WAL tail — and the unselected
    // descriptors remain in the replacement manifest unchanged. Whether it
    // drops rows follows from the placement, and its segments go to ordinary
    // segment keys because this step publishes them below.
    // The manifest that first names a run allocates its number, and this
    // step publishes the manifest that names this merge's output.
    let merged = merge_group_in_step(
        store,
        namespace_id,
        group,
        &input.runs,
        input.placement,
        floor_seq,
        policy,
    )
    .await?;

    let surviving = previous
        .payload
        .runs
        .iter()
        .filter_map(|run| {
            let mut run = run.clone();
            if input.run_nos.contains(&run.run_no) {
                run.segments
                    .retain(|descriptor| !group.contains(descriptor.family));
            }
            (!run.segments.is_empty()).then_some(run)
        })
        .collect();
    let manifest = write_replacement_manifest(
        store,
        namespace_id,
        previous,
        surviving,
        merged.output_segments,
        input.placement,
        floor_seq,
    )
    .await?;

    ensure_metadata_publication_budget(timer, publication_started_ms, namespace_id)?;
    match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
        Some(root.manifest.manifest_object_id.clone()),
        context.now_ms,
    )
    .await?
    {
        ManifestPublicationOutcome::Published(_) => Ok(report(
            namespace_id,
            MetadataReorganizeOutcome::UnitPublished {
                group,
                merged_delta_rows: input.merged_delta_rows,
                input_runs: input.runs.len(),
                decoded_input_rows: input.decoded_rows,
                decoded_input_bytes: input.decoded_bytes,
                manifest_no: manifest.payload.manifest_no,
                bottom_anchored_merge_blocked,
            },
        )),
        ManifestPublicationOutcome::CoveredByCurrent(_)
        | ManifestPublicationOutcome::PredecessorChanged(_)
        | ManifestPublicationOutcome::Installable => {
            Ok(report(namespace_id, MetadataReorganizeOutcome::Superseded))
        }
    }
}

/// What one reorganization step should do for the group it selected.
///
/// The two arms are the two shapes of work, not two levels of urgency: a
/// bounded merge is one step's worth of merging that ends in a publication,
/// and a full compaction is a background job over the whole group that no
/// budget paces. The planner decides between them once, from the same run
/// list, so no caller has to rediscover which case it is in.
pub(super) enum ReorganizationPlan {
    /// The group has no runs.
    Nothing,
    /// A window of complete runs fits one step's budgets and makes progress.
    BoundedMerge(ReorganizationInput),
    /// No window that makes progress fits the budgets, so the group is
    /// rebuilt by a streaming compaction over every run it holds. The step
    /// reports the plan and reads no further; the runtime starts the job.
    FullCompaction(MetadataCompactionSpec),
}

pub(super) struct ReorganizationInput {
    pub(super) runs: Vec<MetadataRunManifest>,
    run_nos: BTreeSet<RunNo>,
    merged_delta_rows: u64,
    decoded_rows: u64,
    decoded_bytes: u64,
    /// Where this merge's output stands in the group, which decides both the
    /// tier it is written at and whether it may drop rows.
    pub(super) placement: MergePlacement,
}

/// Determines a merge's output level, sequence, and retention behavior.
///
/// A merge that includes the group's oldest run writes a base-tier run at the
/// manifest head. Its input includes the previous base run and all older rows
/// needed by retention rules, so it may remove rows below the retention floor.
/// Replacing the previous base leaves the group with at most one base run.
///
/// A merge that starts above the base writes a delta-tier run and preserves
/// every input row. The output uses its newest input sequence so it remains in
/// the same position relative to runs outside the selected window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MergePlacement {
    /// The window includes the group's oldest run. The output is the base run,
    /// and retention may remove related row pairs because both the older row
    /// and its newer cancellation row are included (format spec,
    /// "Compaction").
    Base { output_seq: ChangeSeq },
    /// The window started above the group's oldest run. The output is a delta
    /// run at the newest input's sequence, and nothing is dropped.
    Delta { output_seq: ChangeSeq },
}

impl MergePlacement {
    pub(super) fn output_seq(self) -> ChangeSeq {
        match self {
            Self::Base { output_seq } | Self::Delta { output_seq } => output_seq,
        }
    }

    pub(super) fn output_tier(self) -> RunTier {
        match self {
            Self::Base { .. } => RunTier::Base,
            Self::Delta { .. } => RunTier::Delta,
        }
    }

    pub(super) fn may_drop_rows_below_the_retention_floor(self) -> bool {
        matches!(self, Self::Base { .. })
    }
}

/// Where the output of the selected window stands.
fn merge_placement(
    bottom_anchored: bool,
    runs: &[MetadataRunManifest],
    head_seq: ChangeSeq,
) -> MergePlacement {
    if bottom_anchored {
        return MergePlacement::Base {
            output_seq: head_seq,
        };
    }
    MergePlacement::Delta {
        output_seq: runs
            .iter()
            .map(|run| run.run_seq)
            .max()
            .expect("a delta merge window should hold at least one run"),
    }
}

/// Result of selecting input runs for one reorganization step.
///
/// `group_bottom_over_budget` is reported for status and logging even when the
/// plan can merge newer delta runs or start a full compaction.
pub(super) struct ReorganizationSelection {
    /// What the step should do.
    pub(super) plan: ReorganizationPlan,
    pub(super) group_bottom_over_budget: Option<OverBudgetRun>,
    /// True when the window starting at the group's oldest run could not
    /// reach a delta run inside the budgets. The group's base is frozen while
    /// that holds, and only a streaming compaction unfreezes it, so this is
    /// what the runtime counts to decide when to stop merging deltas over it.
    pub(super) bottom_anchored_merge_blocked: bool,
}

/// A run that does not fit one step's budgets on its own.
#[derive(Debug, Clone, Copy)]
pub(super) struct OverBudgetRun {
    pub(super) run_no: RunNo,
    pub(super) run_seq: ChangeSeq,
    pub(super) tier: RunTier,
    pub(super) rows: u64,
    /// Decoded bytes, or `None` if the row limit rejected the run first.
    pub(super) decoded_bytes: Option<u64>,
}

fn over_budget_run(
    run: &MetadataRunManifest,
    rows: u64,
    decoded_bytes: Option<u64>,
) -> OverBudgetRun {
    OverBudgetRun {
        run_no: run.run_no,
        run_seq: run.run_seq,
        tier: run.tier,
        rows,
        decoded_bytes,
    }
}

/// Selects bounded merge input or plans a full compaction for one family
/// group.
///
/// Base runs sort before delta runs, with older runs first in each tier. The
/// selector tries a window from the oldest run, then a delta-only window. It
/// never skips a delta run. If neither bounded window can make progress, it
/// plans a full compaction.
///
/// Indexes are read before row data so each run either fits the row and byte
/// budgets completely or is excluded.
pub(super) async fn select_reorganization_input<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    group: MetadataFamilyGroup,
    policy: MetadataLsmPolicy,
    frozen_floor_seq: ChangeSeq,
    frozen_base: &FrozenBasePolicy,
) -> std::result::Result<ReorganizationSelection, ManifestLoadError> {
    let candidates = runs_in_fold_order(
        segments
            .scan_runs
            .iter()
            .filter(|run| run_has_group_rows(run, group))
            .collect(),
    );
    let candidate_count = candidates.len();
    let head_seq = segments.manifest().payload.head_seq;
    let budgets = ReorganizationBudgets {
        rows: u64::try_from(policy.max_decoded_input_rows_per_step.get())
            .expect("a usize input row budget should fit in u64"),
        bytes: u64::try_from(policy.max_decoded_input_bytes_per_step.get())
            .expect("a usize input byte budget should fit in u64"),
        runs: policy.max_input_runs_per_step.get(),
    };
    // Base-tier runs sort first, so this is the index of the oldest delta
    // candidate, and starting there is what skips a base run the budgets
    // cannot admit. A group has at most one base run, so this is one place
    // and the only alternative to the bottom.
    let first_delta_candidate = candidates
        .iter()
        .take_while(|run| run.tier == RunTier::Base)
        .count();
    let delta_only_start = (first_delta_candidate > 0 && first_delta_candidate < candidate_count)
        .then_some(first_delta_candidate);
    // Reading a run's decoded byte total costs its index sections, so each
    // candidate's total is read at most once however many windows weigh it.
    let mut decoded_bytes_by_candidate = vec![None::<u64>; candidate_count];
    let mut group_bottom_over_budget = None;
    let mut bottom_anchored_merge_blocked = false;

    for window_start in std::iter::once(0).chain(delta_only_start) {
        // Reaching a window above the bottom means the bottom-anchored one
        // made no progress, so the group's base is frozen and its retention
        // has stopped. A delta merge still helps — it keeps the run count
        // down while the base waits — but only the job unfreezes the base, and
        // under sustained writes there is always another pair of delta runs to
        // merge. So the caller's policy decides whether to take that merge
        // once more or to go to the job now.
        if window_start > 0 && frozen_base.compact_a_frozen_base(group) {
            break;
        }
        let Some(window) = weigh_window(
            segments,
            &candidates,
            group,
            window_start,
            budgets,
            &mut decoded_bytes_by_candidate,
        )
        .await?
        else {
            continue;
        };
        if window_start == 0 {
            group_bottom_over_budget = window.over_budget;
        }

        // A bottom-anchored merge makes progress when it moves at least one
        // delta run into the base tier. A delta-only merge must combine at least
        // two runs; rewriting one delta run would not reduce pending work.
        let bottom_anchored = window_start == 0;
        let makes_progress = if bottom_anchored {
            window.runs.iter().any(|run| run.tier == RunTier::Delta)
        } else {
            window.runs.len() > 1
        };
        if !makes_progress {
            bottom_anchored_merge_blocked |= bottom_anchored;
            continue;
        }
        let run_nos = window.runs.iter().map(|run| run.run_no).collect();
        let placement = merge_placement(bottom_anchored, &window.runs, head_seq);
        return Ok(ReorganizationSelection {
            plan: ReorganizationPlan::BoundedMerge(ReorganizationInput {
                runs: window.runs,
                run_nos,
                merged_delta_rows: window.merged_delta_rows,
                decoded_rows: window.decoded_rows,
                decoded_bytes: window.decoded_bytes,
                placement,
            }),
            group_bottom_over_budget,
            bottom_anchored_merge_blocked,
        });
    }

    // Nothing above got taken. Either no window makes progress inside the
    // budgets — the bottom-anchored window cannot reach a delta run, so
    // retention for the group has stopped, and the delta runs above the bottom
    // are down to one or blocked as well — or the delta merge was available
    // and the runtime said this group has taken enough of them. Both end the
    // same way: a streaming compaction takes the whole group. Its input is
    // every run the group holds, which is bottom-anchored by construction, so
    // its output is the group's base run and it may drop what the floor
    // allows.
    let plan = if candidates.is_empty() {
        ReorganizationPlan::Nothing
    } else {
        ReorganizationPlan::FullCompaction(MetadataCompactionSpec::new(
            group,
            candidates.iter().map(|run| run.run_no).collect(),
            candidates
                .iter()
                .flat_map(|run| group_run_descriptors(run, group))
                .map(|descriptor| descriptor.row_count)
                .sum(),
            MergePlacement::Base {
                output_seq: head_seq,
            },
            frozen_floor_seq,
        ))
    };
    Ok(ReorganizationSelection {
        plan,
        group_bottom_over_budget,
        bottom_anchored_merge_blocked,
    })
}

#[derive(Clone, Copy)]
struct ReorganizationBudgets {
    rows: u64,
    bytes: u64,
    runs: usize,
}

struct WeighedWindow {
    runs: Vec<MetadataRunManifest>,
    merged_delta_rows: u64,
    decoded_rows: u64,
    decoded_bytes: u64,
    over_budget: Option<OverBudgetRun>,
}

async fn weigh_window<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    candidates: &[&MetadataRunManifest],
    group: MetadataFamilyGroup,
    start: usize,
    budgets: ReorganizationBudgets,
    decoded_bytes_by_candidate: &mut [Option<u64>],
) -> std::result::Result<Option<WeighedWindow>, ManifestLoadError> {
    if start >= candidates.len() {
        return Ok(None);
    }
    let mut window = WeighedWindow {
        runs: Vec::new(),
        merged_delta_rows: 0,
        decoded_rows: 0,
        decoded_bytes: 0,
        over_budget: None,
    };
    for index in start..candidates.len().min(start + budgets.runs) {
        let run = candidates[index];
        let run_rows = group_run_descriptors(run, group)
            .map(|descriptor| descriptor.row_count)
            .sum::<u64>();
        if window.decoded_rows.saturating_add(run_rows) > budgets.rows {
            if index == 0 {
                window.over_budget = Some(over_budget_run(run, run_rows, None));
            }
            break;
        }
        let run_bytes = match decoded_bytes_by_candidate[index] {
            Some(bytes) => bytes,
            None => {
                let bytes = decoded_group_run_bytes(segments, run, group).await?;
                decoded_bytes_by_candidate[index] = Some(bytes);
                bytes
            }
        };
        if window.decoded_bytes.saturating_add(run_bytes) > budgets.bytes {
            if index == 0 {
                window.over_budget = Some(over_budget_run(run, run_rows, Some(run_bytes)));
            }
            break;
        }
        if run.tier == RunTier::Delta {
            window.merged_delta_rows = window.merged_delta_rows.saturating_add(run_rows);
        }
        window.decoded_rows = window.decoded_rows.saturating_add(run_rows);
        window.decoded_bytes = window.decoded_bytes.saturating_add(run_bytes);
        window.runs.push(run.clone());
    }
    Ok(Some(window))
}

/// Reports that the oldest run in a family group exceeds one step's input
/// budget.
///
/// Bounded steps may continue merging newer delta runs. When no bounded merge
/// can make the required progress, maintenance plans a streaming compaction
/// of the complete group. This is normal adaptive behavior, so it reports at
/// debug; the case that never resolves — no compaction runner — has its own
/// warning where the job would have started.
fn report_group_bottom_over_budget(
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
    bottom: &OverBudgetRun,
    policy: MetadataLsmPolicy,
) {
    tracing::debug!(
        namespace_id = namespace_id.as_str(),
        families = ?group.families(),
        run_no = bottom.run_no.0,
        run_seq = bottom.run_seq.0,
        run_tier = ?bottom.tier,
        run_rows = bottom.rows,
        run_decoded_bytes = bottom.decoded_bytes,
        max_decoded_input_rows_per_step = policy.max_decoded_input_rows_per_step.get(),
        max_decoded_input_bytes_per_step = policy.max_decoded_input_bytes_per_step.get(),
        "the oldest metadata run in this family group no longer fits one reorganization step; \
         steps merge the newer runs above it into one delta run, and once that merge has \
         nothing left to take, a background streaming compaction rebuilds the whole group and \
         reclaims its rows",
    );
}

/// A bottom-anchored fold stamps its base run at the manifest head, which is
/// at or above every delta run's sequence. So a base run sitting at or above the
/// oldest delta run says one group folded here and delta runs remain — usually
/// other groups' rows in the very runs it just took its own rows out of. The
/// step keeps going on that evidence rather than stopping at the trigger,
/// which is what makes a run of bounded steps end in the same manifest shape
/// one unbounded step would have produced.
///
/// A merge above the base writes a delta run at its newest input's sequence
/// ([`MergePlacement`]), so it never puts a base run here and its inputs stay
/// counted as delta pressure. A fresh delta run appended after a completed fold is
/// strictly newer than every base-tier run and therefore does not bypass the
/// normal trigger.
fn manifest_has_partial_reorganization(runs: &[MetadataRunManifest]) -> bool {
    let Some(oldest_delta_seq) = runs
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .map(|run| run.run_seq)
        .min()
    else {
        return false;
    };
    runs.iter()
        .any(|run| run.tier == RunTier::Base && run.run_seq >= oldest_delta_seq)
}

fn run_has_group_rows(run: &MetadataRunManifest, group: MetadataFamilyGroup) -> bool {
    group_run_descriptors(run, group).next().is_some()
}

pub(super) fn group_run_descriptors(
    run: &MetadataRunManifest,
    group: MetadataFamilyGroup,
) -> impl Iterator<Item = &MetadataSegmentRef> {
    run.segments
        .iter()
        .filter(move |family_segments| group.contains(family_segments.family))
        .flat_map(|family_segments| &family_segments.segments)
}

async fn decoded_group_run_bytes<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    run: &MetadataRunManifest,
    group: MetadataFamilyGroup,
) -> std::result::Result<u64, ManifestLoadError> {
    let mut decoded_bytes = 0u64;
    for descriptor in group_run_descriptors(run, group) {
        let index = load_segment_index_for_reorganization(
            segments.store,
            segments.segment_cache,
            &segments.block_memo,
            descriptor,
        )
        .await?;
        for entry in index.iter() {
            decoded_bytes = decoded_bytes.saturating_add(u64::from(entry.block.decoded_len));
        }
    }
    Ok(decoded_bytes)
}

/// The family group with the most delta rows to fold; ties resolve in group
/// order. `None` when no group has delta rows.
///
/// `rebuilding` is the group a streaming compaction is rebuilding right now,
/// and it is skipped. That one exclusion is the whole of the input-exclusion
/// rule the design asks for, and it holds for two reasons. The job's snapshot
/// is every run the group held, so any window over that group would touch it.
/// And a run is a set of families: a merge of another group rewrites only its
/// own families' descriptors, so the segments the job is reading stay
/// referenced and unchanged whatever else folds meanwhile.
///
/// Without it the excluded group would win every step for as long as the job
/// ran — its delta rows are frozen in the job's snapshot, so its count never
/// falls, while every other group's falls the moment it folds — and the step
/// would spend itself re-planning a job that is already running.
pub(super) fn select_family_group(
    payload: &NamespaceManifestPayload,
    rebuilding: Option<MetadataFamilyGroup>,
) -> Option<MetadataFamilyGroup> {
    REORGANIZE_FAMILY_GROUPS
        .into_iter()
        .filter(|group| Some(*group) != rebuilding)
        .map(|group| (group_delta_rows(payload, group), group))
        .filter(|(rows, _)| *rows > 0)
        .max_by(|(left_rows, left), (right_rows, right)| {
            // On ties the EARLIER group must win, and group order is the
            // enum's declaration order; comparing the groups reversed makes
            // max_by pick it.
            left_rows.cmp(right_rows).then_with(|| right.cmp(left))
        })
        .map(|(_, group)| group)
}

fn group_delta_rows(payload: &NamespaceManifestPayload, group: MetadataFamilyGroup) -> u64 {
    payload
        .runs
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .flat_map(|run| &run.segments)
        .filter(|descriptor| group.contains(descriptor.family))
        .map(|descriptor| descriptor.row_count)
        .sum()
}

/// Writes a replacement manifest from surviving and newly produced runs.
///
/// The resulting runs determine the base sequence and next run number.
pub(super) async fn write_replacement_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    previous: &NamespaceManifestEnvelope,
    surviving: Vec<MetadataRunRef>,
    outputs: Vec<MetadataSegmentRef>,
    placement: MergePlacement,
    floor_seq: ChangeSeq,
) -> Result<NamespaceManifestEnvelope> {
    let next_run_no = if outputs.is_empty() {
        previous.payload.next_run_no
    } else {
        next_run_no_after(previous.payload.next_run_no)?
    };
    let mut runs = surviving;
    if !outputs.is_empty() {
        runs.push(MetadataRunRef {
            run_no: previous.payload.next_run_no,
            run_seq: placement.output_seq(),
            tier: placement.output_tier(),
            segments: outputs,
        });
    }
    let base_seq = runs
        .iter()
        .map(|run| run.run_seq)
        .min()
        .expect("a replacement manifest should hold at least one run");
    let retention_floor_seq = previous.payload.retention_floor_seq.max(floor_seq);
    // One generated object id, one write. The generated id ends in 16 random
    // hex characters, so the key is this unit's alone and a conflict under it
    // is corruption rather than contention.
    let manifest_no = next_manifest_no_after(previous.payload.manifest_no)?;
    let manifest_object_id = ManifestObjectId::generate(manifest_no);
    let object_key = metadata_manifest_object(namespace_id, &manifest_object_id);
    let manifest = NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_no,
        manifest_object_id,
        head_seq: previous.payload.head_seq,
        head_commit_id: previous.payload.head_commit_id.clone(),
        base_seq,
        writer_epoch: previous.payload.writer_epoch,
        next_inode_id: previous.payload.next_inode_id,
        next_run_no,
        retention_floor_seq,
        runs,
    })
    .map_err(|error| CoreError::Codec {
        object_key,
        message: error.to_string(),
    })?;
    write_namespace_manifest(store, &manifest)
        .await
        .map_err(manifest_write_failure)?;
    Ok(manifest)
}
