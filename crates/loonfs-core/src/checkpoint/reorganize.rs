//! Plans and publishes bounded metadata merges.
//!
//! Each step selects one family group and either merges a bounded run window
//! or requests a full background compaction. Base merges may apply retention;
//! delta merges preserve every row. Both paths use the same merge engine.
//!
//! Every successful merge publishes a new manifest. Interrupted or losing
//! publications leave unreferenced output for garbage collection.
//! [`MetadataCompactionPolicy`] decides when input sizes warrant a merge.

use super::block_fetch::{load_segment_index_for_reorganization, segment_object_len};
use super::compaction_lease::{group_lease_state, GroupLeaseState};
use super::error::ManifestLoadError;
use super::flush::{ensure_metadata_publication_budget, next_manifest_no_after, next_run_no_after};
use super::load::load_manifest_segments;
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
use crate::wal::{count_visible_wal_tail_segments, WalChainLoadRequest};
use loonfs_api::wire::manifest::{
    MetadataRunRef, MetadataSegmentRef, NamespaceManifestEnvelope, NamespaceManifestPayload,
    RunTier,
};
use loonfs_api::{ChangeSeq, ManifestNo, ManifestObjectId, NamespaceId, RunNo};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;

/// Maximum run fan-in for either execution path. Larger groups are merged in
/// several publications, keeping decoded input blocks independent of history size.
pub(super) const MAX_COMPACTION_INPUT_RUNS: usize = 8;

/// A large run is rewritten only after newer input reaches one quarter its size.
const COMPACTION_SIZE_RATIO: u64 = 4;

/// How much input must accumulate before a run is worth rewriting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MetadataCompactionPolicy {
    /// Consolidate small runs and merge large runs as newer data accumulates.
    #[default]
    SizeTiered,
    /// Ignore size ratios for an explicit compaction request. Input fan-in stays bounded.
    CompactImmediately,
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
        /// The group this unit merged. Its families are
        /// [`MetadataFamilyGroup::families`].
        group: MetadataFamilyGroup,
        merged_delta_rows: u64,
        input_runs: usize,
        decoded_input_rows: u64,
        decoded_input_bytes: u64,
        manifest_no: ManifestNo,
        /// True when this merge ran above a frozen base.
        bottom_anchored_merge_blocked: bool,
    },
    /// The selected window exceeds the step's row or byte budget. A background
    /// job merges it with the same bounded fan-in; this step publishes nothing.
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
    compaction_policy: MetadataCompactionPolicy,
) -> Result<MetadataReorganizeReport> {
    let timer = StdMonotonicTimer::default();
    reorganize_metadata_step_with_timer(
        store,
        namespace_id,
        context,
        policy,
        compaction_policy,
        &timer,
    )
    .await
}

pub(super) async fn reorganize_metadata_step_with_timer<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    policy: MetadataLsmPolicy,
    compaction_policy: MetadataCompactionPolicy,
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
    let segments = load_manifest_segments(store, None, &root.manifest).await?;
    let previous = segments.manifest();

    let delta_runs = delta_run_count(previous.payload());
    if !manifest_has_reorganization_work(
        previous.payload(),
        segments.scan_runs.as_ref(),
        policy,
        compaction_policy,
    ) {
        return Ok(report(
            namespace_id,
            MetadataReorganizeOutcome::NotNeeded { delta_runs },
        ));
    }
    let Some(group) = select_family_group(
        store,
        namespace_id,
        previous.payload(),
        context.now_ms,
        compaction_policy,
        policy,
    )
    .await?
    else {
        // Delta runs exist but hold no rows (empty families), or the only group
        // with rows is the one a job is rebuilding; nothing to fold here.
        return Ok(report(
            namespace_id,
            MetadataReorganizeOutcome::NotNeeded { delta_runs },
        ));
    };
    let selection =
        select_reorganization_input(&segments, group, policy, floor_seq, &compaction_policy)
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
        ReorganizationPlan::StreamingCompaction(spec) => {
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
        .payload()
        .runs
        .iter()
        .filter_map(|run| {
            let mut run = run.clone();
            if input.run_nos.contains(&run.run_no) {
                run.segments
                    .retain(|descriptor| !group.families().contains(&descriptor.family));
            }
            (!run.segments.is_empty()).then_some(run)
        })
        .collect();
    let manifest = write_replacement_manifest(
        store,
        namespace_id,
        previous,
        surviving,
        ReplacementOutput {
            segments: merged.output_segments,
            placement: input.placement,
        },
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
                manifest_no: manifest.payload().manifest_no,
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

/// Checks WAL and manifest descriptors without decoding segment rows.
pub async fn metadata_maintenance_due<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&super::cache::MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    max_wal_tail_segments: u64,
    compaction_policy: MetadataCompactionPolicy,
) -> Result<bool> {
    let snapshot = load_control_snapshot(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    crate::namespace::control::ensure_namespace_live(&snapshot.head.state)?;
    let basis = snapshot.basis();
    let basis_head_seq = basis
        .manifest()
        .map_or(ChangeSeq(0), |manifest| manifest.manifest_head_seq);
    let head = &snapshot.head.state;
    let wal_tail_segments = count_visible_wal_tail_segments(&WalChainLoadRequest {
        namespace_id,
        chain_base_seq: basis_head_seq,
        head_seq: head.seq,
        visible_tip: head.visible_wal_tip.clone(),
        stop_after_seq: None,
        max_segment_fetches: None,
        recent_segments: &head.recent_segments,
    })
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    if wal_tail_segments >= max_wal_tail_segments {
        return Ok(true);
    }
    let Some(root) = snapshot.root else {
        return Ok(false);
    };
    let segments = load_manifest_segments(store, segment_cache, &root.state.manifest).await?;
    Ok(manifest_has_reorganization_work(
        segments.manifest().payload(),
        segments.scan_runs.as_ref(),
        MetadataLsmPolicy::default(),
        compaction_policy,
    ))
}

/// One selected window, executed inside the step or by a leased background job.
pub(super) enum ReorganizationPlan {
    /// The group has no runs.
    Nothing,
    /// A window of complete runs fits one step's budgets and makes progress.
    BoundedMerge(ReorganizationInput),
    /// The selected window exceeds the step's row or byte budget.
    StreamingCompaction(MetadataCompactionSpec),
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
    /// True when this selection leaves the base alone, or it exceeds the step budget.
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

fn group_candidates(
    runs: &[MetadataRunManifest],
    group: MetadataFamilyGroup,
) -> Vec<&MetadataRunManifest> {
    runs_in_fold_order(
        runs.iter()
            .filter(|run| run_has_group_rows(run, group))
            .collect(),
    )
}

/// Select the oldest eligible contiguous window. Never mix rows across an
/// unselected gap: a base merge may drop history only when it includes a prefix.
/// Sizes come from the manifest's block handles, so planning needs no data reads.
fn select_merge_window(
    candidates: &[&MetadataRunManifest],
    group: MetadataFamilyGroup,
    policy: MetadataCompactionPolicy,
    small_run_bytes: u64,
) -> Option<std::ops::Range<usize>> {
    for start in 0..candidates.len() {
        let end = candidates.len().min(start + MAX_COMPACTION_INPUT_RUNS);
        if window_is_eligible(
            &candidates[start..end],
            group,
            start == 0,
            policy,
            small_run_bytes,
        ) {
            return Some(start..end);
        }
    }
    None
}

fn window_is_eligible(
    runs: &[&MetadataRunManifest],
    group: MetadataFamilyGroup,
    bottom_anchored: bool,
    policy: MetadataCompactionPolicy,
    small_run_bytes: u64,
) -> bool {
    let Some((oldest, newer)) = runs.split_first() else {
        return false;
    };
    if newer.is_empty() {
        // Promoting the only delta establishes a base without rewriting it later
        // just to change its tier. A lone base has nothing to merge.
        return bottom_anchored
            && (oldest.tier == RunTier::Delta
                || policy == MetadataCompactionPolicy::CompactImmediately);
    }
    let stored_bytes = |run: &&MetadataRunManifest| {
        group_run_descriptors(run, group)
            .map(segment_object_len)
            .sum::<u64>()
    };
    let oldest_bytes = stored_bytes(oldest);
    policy == MetadataCompactionPolicy::CompactImmediately
        || oldest_bytes <= small_run_bytes
        || newer
            .iter()
            .map(stored_bytes)
            .sum::<u64>()
            .saturating_mul(COMPACTION_SIZE_RATIO)
            >= oldest_bytes
}

/// Choose input by size first, then choose how to execute it. A background job
/// has the same run limit as a bounded merge; it only relaxes total rows and bytes.
pub(super) async fn select_reorganization_input<S: ObjectStore + ?Sized>(
    segments: &VerifiedMetadataSegments<'_, S>,
    group: MetadataFamilyGroup,
    policy: MetadataLsmPolicy,
    frozen_floor_seq: ChangeSeq,
    compaction_policy: &MetadataCompactionPolicy,
) -> std::result::Result<ReorganizationSelection, ManifestLoadError> {
    let candidates = group_candidates(segments.scan_runs.as_ref(), group);
    let Some(range) = select_merge_window(
        &candidates,
        group,
        *compaction_policy,
        policy.small_run_bytes.get() as u64,
    ) else {
        return Ok(ReorganizationSelection {
            plan: ReorganizationPlan::Nothing,
            group_bottom_over_budget: None,
            bottom_anchored_merge_blocked: false,
        });
    };
    let selected = &candidates[range.clone()];
    let budgets = ReorganizationBudgets {
        rows: policy.max_decoded_input_rows_per_step.get() as u64,
        bytes: policy.max_decoded_input_bytes_per_step.get() as u64,
        runs: policy
            .max_input_runs_per_step
            .get()
            .min(MAX_COMPACTION_INPUT_RUNS),
    };
    let window = weigh_window(segments, selected, group, budgets).await?;
    let bottom_anchored = range.start == 0;
    let group_bottom_over_budget = if bottom_anchored {
        window.over_budget
    } else {
        None
    };
    let bottom_anchored_merge_blocked = !bottom_anchored || group_bottom_over_budget.is_some();
    let bounded_runs = window.runs.iter().collect::<Vec<_>>();
    let plan = if window_is_eligible(
        &bounded_runs,
        group,
        bottom_anchored,
        *compaction_policy,
        policy.small_run_bytes.get() as u64,
    ) && (window.runs.len() > 1
        || selected.len() == 1
        || window.runs[0].tier == RunTier::Delta)
    {
        let placement = merge_placement(
            bottom_anchored,
            &window.runs,
            segments.manifest().payload().head_seq,
        );
        ReorganizationPlan::BoundedMerge(ReorganizationInput {
            run_nos: window.runs.iter().map(|run| run.run_no).collect(),
            runs: window.runs,
            merged_delta_rows: window.merged_delta_rows,
            decoded_rows: window.decoded_rows,
            decoded_bytes: window.decoded_bytes,
            placement,
        })
    } else {
        let runs = selected
            .iter()
            .map(|run| (*run).clone())
            .collect::<Vec<_>>();
        ReorganizationPlan::StreamingCompaction(MetadataCompactionSpec::new(
            group,
            runs.iter().map(|run| run.run_no).collect(),
            runs.iter()
                .flat_map(|run| group_run_descriptors(run, group))
                .map(|d| d.row_count)
                .sum(),
            merge_placement(
                bottom_anchored,
                &runs,
                segments.manifest().payload().head_seq,
            ),
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
    budgets: ReorganizationBudgets,
) -> std::result::Result<WeighedWindow, ManifestLoadError> {
    let mut window = WeighedWindow {
        runs: Vec::new(),
        merged_delta_rows: 0,
        decoded_rows: 0,
        decoded_bytes: 0,
        over_budget: None,
    };
    for (index, run) in candidates.iter().take(budgets.runs).enumerate() {
        let run_rows = group_run_descriptors(run, group)
            .map(|descriptor| descriptor.row_count)
            .sum::<u64>();
        if window.decoded_rows.saturating_add(run_rows) > budgets.rows {
            if index == 0 {
                window.over_budget = Some(over_budget_run(run, run_rows, None));
            }
            break;
        }
        let run_bytes = decoded_group_run_bytes(segments, run, group).await?;
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
        window.runs.push((*run).clone());
    }
    Ok(window)
}

/// Reports the row or byte budget that requires a streaming job.
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
         eligible windows run as streaming jobs with bounded input fan-in",
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

fn manifest_has_reorganization_work(
    payload: &NamespaceManifestPayload,
    runs: &[MetadataRunManifest],
    policy: MetadataLsmPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> bool {
    let groups = ranked_family_groups(payload);
    let triggered = compaction_policy == MetadataCompactionPolicy::CompactImmediately
        || (delta_run_count(payload) >= policy.max_delta_runs.get() && !groups.is_empty())
        || manifest_has_partial_reorganization(runs);
    triggered
        && groups.into_iter().any(|group| {
            select_merge_window(
                &group_candidates(runs, group),
                group,
                compaction_policy,
                policy.small_run_bytes.get() as u64,
            )
            .is_some()
        })
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
        .filter(move |family_segments| group.families().contains(&family_segments.family))
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

/// The available family group with the most delta rows to fold; ties resolve
/// in group order. `None` when no available group has delta rows.
///
/// Groups under unexpired active or reaping leases are skipped. A job's
/// snapshot contains its selected runs, while merges of other groups leave
/// those descriptors unchanged.
///
/// Without it the excluded group would win every step for as long as the job
/// ran — its delta rows are frozen in the job's snapshot, so its count never
/// falls, while every other group's falls the moment it folds — and the step
/// would spend itself re-planning a job that is already running.
pub(super) async fn select_family_group<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    payload: &NamespaceManifestPayload,
    now_ms: u64,
    compaction_policy: MetadataCompactionPolicy,
    policy: MetadataLsmPolicy,
) -> Result<Option<MetadataFamilyGroup>> {
    let runs = super::runs::runs_in_reorganization_order(payload);
    for group in ranked_family_groups(payload) {
        let candidates = group_candidates(&runs, group);
        if select_merge_window(
            &candidates,
            group,
            compaction_policy,
            policy.small_run_bytes.get() as u64,
        )
        .is_none()
        {
            continue;
        }
        if group_lease_state(store, namespace_id, group, now_ms).await? != GroupLeaseState::Held {
            return Ok(Some(group));
        }
    }
    Ok(None)
}

pub(super) fn ranked_family_groups(payload: &NamespaceManifestPayload) -> Vec<MetadataFamilyGroup> {
    let mut ranked = REORGANIZE_FAMILY_GROUPS
        .into_iter()
        .map(|group| (group_delta_rows(payload, group), group))
        .filter(|(rows, _)| *rows > 0)
        .collect::<Vec<_>>();
    ranked.sort_by(|(left_rows, left), (right_rows, right)| {
        right_rows.cmp(left_rows).then_with(|| left.cmp(right))
    });
    ranked.into_iter().map(|(_, group)| group).collect()
}

fn group_delta_rows(payload: &NamespaceManifestPayload, group: MetadataFamilyGroup) -> u64 {
    payload
        .runs
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .flat_map(|run| &run.segments)
        .filter(|descriptor| group.families().contains(&descriptor.family))
        .map(|descriptor| descriptor.row_count)
        .sum()
}

pub(super) struct ReplacementOutput {
    pub(super) segments: Vec<MetadataSegmentRef>,
    pub(super) placement: MergePlacement,
}

/// Writes a replacement manifest from surviving and newly produced runs.
///
/// The resulting runs determine the base sequence and next run number.
pub(super) async fn write_replacement_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    previous: &NamespaceManifestEnvelope,
    surviving: Vec<MetadataRunRef>,
    output: ReplacementOutput,
    floor_seq: ChangeSeq,
) -> Result<NamespaceManifestEnvelope> {
    let next_run_no = if output.segments.is_empty() {
        previous.payload().next_run_no
    } else {
        next_run_no_after(previous.payload().next_run_no)?
    };
    let mut runs = surviving;
    if !output.segments.is_empty() {
        runs.push(MetadataRunRef {
            run_no: previous.payload().next_run_no,
            run_seq: output.placement.output_seq(),
            tier: output.placement.output_tier(),
            segments: output.segments,
        });
    }
    let base_seq = runs
        .iter()
        .map(|run| run.run_seq)
        .min()
        .expect("a replacement manifest should hold at least one run");
    let retention_floor_seq = previous.payload().retention_floor_seq.max(floor_seq);
    // One generated object id, one write. The generated id ends in 16 random
    // hex characters, so the key is this unit's alone and a conflict under it
    // is corruption rather than contention.
    let manifest_no = next_manifest_no_after(previous.payload().manifest_no)?;
    let manifest_object_id = ManifestObjectId::generate(manifest_no);
    write_namespace_manifest(
        store,
        NamespaceManifestPayload {
            namespace_id: namespace_id.clone(),
            manifest_no,
            manifest_object_id,
            head_seq: previous.payload().head_seq,
            head_commit_id: previous.payload().head_commit_id.clone(),
            base_seq,
            writer_epoch: previous.payload().writer_epoch,
            next_inode_id: previous.payload().next_inode_id,
            next_run_no,
            retention_floor_seq,
            runs,
        },
    )
    .await
    .map_err(manifest_write_failure)
}

#[cfg(test)]
mod planning_tests {
    use super::super::runs::{runs_in_reorganization_order, MetadataFamilySegments};
    use super::*;
    use loonfs_api::wire::manifest::{decode_namespace_manifest_json, MetadataRowFamily};

    // The planner only reads descriptors. Give valid descriptor shapes arbitrary
    // stored lengths to exercise GiB-scale layouts without allocating their data.
    fn runs(sizes: &[u64]) -> Vec<MetadataRunManifest> {
        let manifest = decode_namespace_manifest_json(include_bytes!(
            "../../../loonfs-api/tests/golden/namespace_manifest.v4.json"
        ))
        .expect("manifest fixture");
        let mut template = runs_in_reorganization_order(manifest.payload()).remove(0);
        template
            .segments
            .retain(|family| family.family == MetadataRowFamily::Inodes);
        sizes
            .iter()
            .enumerate()
            .map(|(index, size)| {
                let mut run = template.clone();
                run.run_no = RunNo(index as u64);
                run.run_seq = ChangeSeq(index as u64);
                run.tier = if index == 0 {
                    RunTier::Base
                } else {
                    RunTier::Delta
                };
                let MetadataFamilySegments { segments, .. } = &mut run.segments[0];
                segments[0].index_block.offset =
                    *size - u64::from(segments[0].index_block.stored_len);
                run
            })
            .collect()
    }

    fn window(
        sizes_mib: &[u64],
        policy: MetadataCompactionPolicy,
    ) -> Option<std::ops::Range<usize>> {
        let runs = runs(
            &sizes_mib
                .iter()
                .map(|size| size * 1024 * 1024)
                .collect::<Vec<_>>(),
        );
        select_merge_window(
            &runs.iter().collect::<Vec<_>>(),
            MetadataFamilyGroup::Inodes,
            policy,
            8 * 1024 * 1024,
        )
    }

    #[test]
    fn a_large_base_waits_for_proportionate_new_data() {
        let policy = MetadataCompactionPolicy::SizeTiered;
        assert_eq!(window(&[1024, 1, 1], policy), Some(1..3));
        assert_eq!(window(&[1024, 2], policy), None);
        assert_eq!(window(&[1024, 255], policy), None);
        assert_eq!(window(&[1024, 256], policy), Some(0..2));
        assert_eq!(
            window(&[1024, 2], MetadataCompactionPolicy::CompactImmediately),
            Some(0..2)
        );
    }

    #[test]
    fn a_large_delta_obeys_the_same_ratio_as_a_large_base() {
        assert_eq!(
            window(&[4096, 256, 1, 1], MetadataCompactionPolicy::SizeTiered),
            Some(2..4)
        );
        assert_eq!(
            window(&[4096, 256, 64], MetadataCompactionPolicy::SizeTiered),
            Some(1..3)
        );
    }

    #[test]
    fn a_backlog_is_merged_in_contiguous_windows_with_bounded_fan_in() {
        let policy = MetadataCompactionPolicy::SizeTiered;
        assert_eq!(
            window(&[1; 100], policy),
            Some(0..MAX_COMPACTION_INPUT_RUNS)
        );
        let mut sizes = vec![4096];
        sizes.extend([1; 100]);
        assert_eq!(
            window(&sizes, policy),
            Some(1..1 + MAX_COMPACTION_INPUT_RUNS)
        );
    }
}
