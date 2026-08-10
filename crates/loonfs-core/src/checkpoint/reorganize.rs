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
//! The merging itself is not here. A step drives the same engine a background
//! compaction drives ([`super::streaming_compaction`]), synchronously and with
//! nothing around it: no lease, no job, no registry, no admission, and output
//! written at ordinary table keys because the publication that names it is the
//! step's own. What the step owns is choosing the window, publishing the
//! result, and reporting what it did.
//!
//! A merge writes a base-tier run when, and only when, its window starts at
//! the group's oldest run. That is the same condition retention dropping
//! needs, so the level a run carries and the rules that produced it say the
//! same thing: a base run is a run some fold was allowed to drop rows from,
//! a delta run is a run nothing has dropped from yet ([`MergePlacement`]). A
//! merge that had to start above the group's base therefore writes a bigger
//! delta run rather than a second base run, and a family group holds at most
//! one base run at any time (manifest load refuses a manifest that says
//! otherwise).
//!
//! There is no progress record: each unit ends in a durable manifest, so a
//! crashed or interrupted reorganization resumes by reading the live
//! manifest and picking the next group that still has L0 rows. Unit
//! selection is deterministic (most L0 rows first, then group order). A
//! concurrent checkpoint racing a unit wins at the root compare-and-swap;
//! the unit's segments are left unreferenced for garbage collection and the
//! next step retries against the fresh manifest.
//!
//! The planner answers with one of two plans ([`ReorganizationPlan`]). A
//! group with a window that fits the budgets and makes progress gets that
//! window. A group with no such window gets a streaming compaction over
//! every run it holds ([`super::streaming_compaction`]), which is what takes
//! a frozen base off the step budgets entirely. The step does not run that
//! job: it reports the plan, and the runtime starts the job in the
//! background and hands its spec back to every step that follows, which is
//! how a step knows to leave that group alone while it runs.
//!
//! One step cannot see whether a group is stuck. A group whose
//! bottom-anchored window is blocked still has delta runs to merge, and under
//! sustained writes there is always another pair of them, so a planner
//! deciding from one step's view alone would take the delta merge every time
//! and never start the job — the base would stay frozen and the group's
//! retention would stay stopped. The caller therefore states a
//! [`FrozenBasePolicy`]. A writer's own maintenance amortizes: it counts, per
//! group, the delta merges it has published over that frozen base, and at
//! [`DELTA_MERGES_OVER_A_FROZEN_BASE`] the planner stops taking the merge and
//! plans the job. Everyone else asks for the job outright, because they are
//! either reporting that the namespace needs one or running it.

use super::block_fetch::load_segment_index_for_reorganization;
use super::error::ManifestLoadError;
use super::flush::{ensure_metadata_publication_budget, next_manifest_id_after};
use super::load::load_verified_manifest_tables;
use super::publish::{
    manifest_write_failure, publish_metadata_root, write_namespace_manifest,
    ManifestPublicationOutcome,
};
use super::runs::{
    l0_run_count, MetadataFamilyGroup, MetadataLsmPolicy, MetadataRunManifest,
    CHECKPOINT_BASE_RUN_LEVEL, CHECKPOINT_L0_RUN_LEVEL, REORGANIZE_FAMILY_GROUPS,
};
use super::scan::VerifiedMetadataTables;
use super::streaming_compaction::{merge_group_in_step, MetadataCompactionSpec};
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::basis::resolve_retention_floor_seq;
use crate::namespace::control::{read_head_object, read_metadata_root_object_if_present};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::manifest::{
    MetadataFileRef, NamespaceManifestEnvelope, NamespaceManifestPayload,
};
use loonfs_api::{ChangeSeq, ManifestId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};

/// Delta merges that may publish over a frozen base before the planner
/// insists on the job that unfreezes it.
///
/// Delta merges between jobs are how a stuck group amortizes: a job rereads
/// the whole group, so running one for every eight delta runs would reread
/// megabytes to fold in a sliver. Two merges is the smallest count that still
/// lets the ordinary merge do that work, and it bounds how long retention for
/// the group can stay stopped — the job starts within two published delta
/// merges of the block, whatever the write rate is.
pub(super) const DELTA_MERGES_OVER_A_FROZEN_BASE: u32 = 2;

/// How planning treats a family group whose base run is frozen.
///
/// A group whose bottom-anchored window is blocked still has delta runs above
/// that base to merge, and under sustained writes there is always another pair
/// of them. So the planner has to be told what to do about that, and the two
/// answers are genuinely different work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrozenBasePolicy {
    /// Amortize the rebuild: keep taking the delta merge above the frozen
    /// base until [`DELTA_MERGES_OVER_A_FROZEN_BASE`] of them have published
    /// for the group, then plan the job. This is what a writer's own
    /// maintenance runs under, because upkeep that rebuilt the whole group
    /// for every eight delta runs would reread megabytes to fold in a sliver.
    ///
    /// The counts are per family group, keyed by the group itself: a group's
    /// block is its own, and one step reorganizes one group. A group absent
    /// from the map has published none.
    ///
    /// They are in-memory process state and safe to lose: a restart forgets
    /// them and the next two merges rebuild them, which delays one cycle.
    Amortized {
        published_delta_merges_over_frozen_base: BTreeMap<MetadataFamilyGroup, u32>,
    },
    /// Plan the job as soon as the bottom-anchored window is blocked. This is
    /// what an explicit compaction request runs under, and what a bounded step
    /// with nowhere to run background work runs under: the first reports the
    /// operation its caller asked for, and the second reports that the
    /// namespace needs one promptly instead of publishing delta merges that
    /// never reach it.
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
    /// The manifest's L0 run count is below the policy trigger; nothing to
    /// fold yet.
    NotNeeded { l0_runs: usize },
    /// One bounded complete-run subset for a family group merged into one
    /// new run and the manifest advanced. The new run is the group's base
    /// when the subset started at the group's oldest run, and a bigger delta
    /// run otherwise.
    UnitPublished {
        /// The group this unit merged, which is what a runtime keys its
        /// per-group bookkeeping by. Its families are
        /// [`MetadataFamilyGroup::families`].
        group: MetadataFamilyGroup,
        folded_l0_rows: u64,
        input_runs: usize,
        decoded_input_rows: u64,
        decoded_input_bytes: u64,
        manifest_id: ManifestId,
        /// True when the window starting at the group's oldest run could not
        /// reach an L0 run, so this merge ran above a frozen base and the
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

/// Runs at most one reorganization unit against the namespace's current
/// manifest. Callers (the maintenance step) invoke this repeatedly; each
/// call re-reads durable state, so any two calls compose — including across
/// process restarts.
///
/// `compactions` is what this process knows about the namespace's streaming
/// compactions: which group a job is rebuilding right now, and how long each
/// group has been merging deltas over a frozen base.
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
    let Some(group) = select_family_group(
        &previous.payload,
        compactions.active.map(MetadataCompactionSpec::group),
    ) else {
        // L0 runs exist but hold no rows (empty families), or the only group
        // with rows is the one a job is rebuilding; nothing to fold here.
        return Ok(MetadataReorganizeReport {
            namespace_id: namespace_id.clone(),
            outcome: MetadataReorganizeOutcome::NotNeeded { l0_runs },
        });
    };
    // The floor is read before the plan rather than after it, because a plan
    // that hands the group to a streaming compaction carries the floor that
    // job will judge every row against, and the spec it carries is immutable
    // for the job's life.
    let head = read_head_object(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .envelope
        .state;
    let floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::load_head)?;
    let selection =
        select_reorganization_input(&tables, group, policy, floor_seq, &compactions.frozen_base)
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
    if let Some(bottom) = selection.group_bottom_over_budget {
        report_group_bottom_over_budget(namespace_id, group, &bottom, policy);
    }
    let bottom_anchored_merge_blocked = selection.bottom_anchored_merge_blocked;
    let input = match selection.plan {
        Some(ReorganizationPlan::BoundedMerge(input)) => input,
        Some(ReorganizationPlan::FullCompaction(spec)) => {
            return Ok(MetadataReorganizeReport {
                namespace_id: namespace_id.clone(),
                outcome: MetadataReorganizeOutcome::CompactionPlanned { group, spec },
            })
        }
        // A selected group holds L0 rows, so it holds runs, so the planner
        // always answers with one of the two plans above.
        None => {
            return Ok(MetadataReorganizeReport {
                namespace_id: namespace_id.clone(),
                outcome: MetadataReorganizeOutcome::NotNeeded { l0_runs },
            })
        }
    };

    // Merge only the selected complete runs, through the one engine both
    // reorganization paths run ([`super::streaming_compaction`]). It reads
    // exactly the manifest's tables — never the WAL tail — and the unselected
    // descriptors remain in the replacement manifest unchanged. Whether it
    // drops rows follows from the placement, and its segments go to ordinary
    // table keys because this step publishes them below.
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

    let mut metadata_files: Vec<_> = previous
        .payload
        .metadata_files
        .iter()
        .filter(|descriptor| {
            !group.contains(descriptor.family)
                || !input
                    .run_ids
                    .contains(&(descriptor.run_seq, descriptor.level))
        })
        .cloned()
        .collect();
    metadata_files.extend(merged.output_segments);
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
                group,
                folded_l0_rows: input.folded_l0_rows,
                input_runs: input.runs.len(),
                decoded_input_rows: input.decoded_rows,
                decoded_input_bytes: input.decoded_bytes,
                manifest_id: manifest.payload.manifest_id,
                bottom_anchored_merge_blocked,
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

/// What one reorganization step should do for the group it selected.
///
/// The two arms are the two shapes of work, not two levels of urgency: a
/// bounded merge is one step's worth of merging that ends in a publication,
/// and a full compaction is a background job over the whole group that no
/// budget paces. The planner decides between them once, from the same run
/// list, so no caller has to rediscover which case it is in.
pub(super) enum ReorganizationPlan {
    /// A window of complete runs fits one step's budgets and makes progress.
    BoundedMerge(ReorganizationInput),
    /// No window that makes progress fits the budgets, so the group is
    /// rebuilt by a streaming compaction over every run it holds. The step
    /// reports the plan and reads no further; the runtime starts the job.
    FullCompaction(MetadataCompactionSpec),
}

pub(super) struct ReorganizationInput {
    pub(super) runs: Vec<MetadataRunManifest>,
    run_ids: BTreeSet<(ChangeSeq, u32)>,
    folded_l0_rows: u64,
    decoded_rows: u64,
    decoded_bytes: u64,
    /// Where this merge's output stands in the group, which decides both the
    /// level it is written at and whether it may drop rows.
    pub(super) placement: MergePlacement,
}

/// Where a merge's output stands in its family group.
///
/// **A merge's output is base-tier if and only if its window starts at the
/// group's oldest run.** Base-tier means "rows a fold was allowed to drop
/// from", so the level a run carries and the rules that produced it say the
/// same thing. Both the output level and the right to drop rows are read off
/// this one value; there is no separate flag either can disagree with.
///
/// A bottom-anchored merge writes the group's base run, stamped at the
/// manifest head. Base-tier runs sort below every delta run whatever sequence
/// they carry, so the output lands at the bottom where its inputs were, and
/// the sequence is free to be the head's — which is also what keeps a file at
/// `head_seq` in the manifest when the merge consumed the group's whole top.
/// It replaces the group's previous base run, because a bottom-anchored
/// window always contains it, so the group is left with exactly one.
///
/// A merge that had to start above the group's base rewrites delta runs into
/// one bigger delta run. Two things follow from writing it at the delta level
/// rather than the base level. It cannot mint a second base run, so a group's
/// base never fragments and nothing ever hides an over-budget bottom behind
/// fragments that each fit. And its rows stay counted as delta pressure, so
/// the L0 run count reports what is really waiting.
///
/// Its sequence is its newest input's, not the manifest head's, so the output
/// stands exactly where that run stood: above every run the window left below
/// it and below every run it left above. The head would put it above runs the
/// window never reached, and moving merged rows above newer runs is what a
/// later bottom-anchored fold must never see — it could then drop an unbind
/// whose bind had moved out of the window and put a deleted file back. The
/// head is also shared by consecutive maintenance steps on a quiet namespace,
/// so head-stamped outputs of different merges collide at one identity.
///
/// Nothing else in the manifest already holds that identity for these
/// families: the newest input run's segments for this group leave
/// `metadata_files` in the same publication the output's enter it. Segments
/// of other families at that identity stay where they are, which is ordinary
/// — a run is a set of families, and each family in it has one producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MergePlacement {
    /// The window started at the group's oldest run. The output is the
    /// group's base run at the manifest head, and retention may drop rows.
    ///
    /// Retention dropping reads across the merged rows: an unbind cancels the
    /// bind it names, and a removal marker cancels the listed deletion it
    /// repeats. Dropping one half of such a pair is only visibility-preserving
    /// when the other half is in the same merge, and what guarantees that is
    /// starting the merge at the group's oldest run — the cancelling row is
    /// always the newer of the two, so every row it can cancel is already in
    /// the window (format spec, "Compaction").
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

    pub(super) fn output_level(self) -> u32 {
        match self {
            Self::Base { .. } => CHECKPOINT_BASE_RUN_LEVEL,
            Self::Delta { .. } => CHECKPOINT_L0_RUN_LEVEL,
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
    // The newest input's sequence. A merge above the base needs two runs to
    // make progress, so the window is never empty here and the fallback only
    // keeps this total.
    MergePlacement::Delta {
        output_seq: runs.iter().map(|run| run.run_seq).max().unwrap_or(head_seq),
    }
}

/// What one selection attempt found.
///
/// The saturation record travels beside the input because the caller
/// reports it on both paths: a group whose oldest run has outgrown one
/// step's budget still folds its newer runs, and that is exactly when an
/// operator needs to hear that the run underneath them is frozen.
pub(super) struct ReorganizationSelection {
    /// What the step should do, or `None` when the group holds no runs at
    /// all and there is nothing to do either way.
    pub(super) plan: Option<ReorganizationPlan>,
    pub(super) group_bottom_over_budget: Option<OverBudgetRun>,
    /// True when the window starting at the group's oldest run could not
    /// reach an L0 run inside the budgets. The group's base is frozen while
    /// that holds, and only a streaming compaction unfreezes it, so this is
    /// what the runtime counts to decide when to stop merging deltas over it.
    pub(super) bottom_anchored_merge_blocked: bool,
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
/// sit under every L0 run whatever their `run_seq` says: a bottom-anchored
/// fold stamps its output at the manifest head, which can leave a base run
/// carrying a higher `run_seq` than an L0 run some other group has not folded
/// yet (see [`manifest_has_partial_reorganization`]). Within one tier the
/// lower `run_seq` is the older run.
///
/// **The invariant.** A merge writes its inputs back as one run standing
/// where the window stood: at the bottom of the group when the window is
/// bottom-anchored, and at its newest input's identity otherwise
/// ([`MergePlacement`]). Either way the output may not carry a row newer than
/// a run it left above the window — otherwise a later fold could drop a row
/// while the row it cancels sits outside that fold's window. What keeps that
/// true is that the window is a *contiguous* slice of the order: every run it
/// leaves out sits wholly below it or wholly above it, never interleaved, so
/// the output can stand in for the whole window without moving any row past
/// any other. When the window starts at the very bottom nothing is left out
/// below it at all, and that is the stronger property retention dropping
/// needs — see [`MergePlacement::Base`].
///
/// **There are two windows to try.** A manifest holds at most one base-tier
/// run per family group, so the search is short: the window starting at the
/// group's oldest run, and — when a base run blocks that one — the window
/// starting at the oldest delta run above it. A delta run is never stepped
/// over.
///
/// **The budgets pace the work; they never end it.** When no window starting
/// at the group's oldest run can fit the budgets, the start moves past the
/// base run that blocks it and the step merges the L0 runs above it on their
/// own. The group keeps shedding runs even while the run at its bottom stays
/// too large to fold. When not even that is left — no window at all makes
/// progress inside the budgets — the group is handed to a streaming
/// compaction, which merges every run it holds and is paced by nothing.
///
/// Index sections are read before row payloads so the decoded-byte budget is
/// known exactly from each data block's durable `decoded_len`; a run that
/// would cross a budget is not decoded or partially included.
///
/// `frozen_floor_seq` is the live retention floor. A bounded merge reads it
/// again at drop time; a compaction spec carries it, because the floor a job
/// judges every row against is fixed when the job is planned.
///
/// `frozen_base` is the caller's answer to the one thing this function cannot
/// see: whether the delta merge above a base no window can reach is still the
/// work this group needs. When the policy says it is not, a blocked
/// bottom-anchored window goes straight to the job rather than taking that
/// merge — the merge would make progress, but not the progress the group
/// needs.
pub(super) async fn select_reorganization_input<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    group: MetadataFamilyGroup,
    policy: MetadataLsmPolicy,
    frozen_floor_seq: ChangeSeq,
    frozen_base: &FrozenBasePolicy,
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
    // Base-tier runs sort first, so this is the index of the oldest L0
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

        // A merge must leave the group with less to fold than it found, or
        // the same window would be chosen again forever. What counts as less
        // follows from where the output lands.
        //
        // A bottom-anchored merge writes a base run, so every L0 run it takes
        // leaves L0 for good: one L0 run in the window is enough, and a
        // window holding none would only rewrite base rows where they stand.
        //
        // A merge above the base writes another delta run, so it only gains
        // by merging two or more into one. A single-run window there would
        // rewrite that run as itself, at its own identity, having read and
        // written every row for nothing.
        let bottom_anchored = window_start == 0;
        let makes_progress = if bottom_anchored {
            runs.iter().any(|run| run.level == CHECKPOINT_L0_RUN_LEVEL)
        } else {
            runs.len() > 1
        };
        if !makes_progress {
            bottom_anchored_merge_blocked |= bottom_anchored;
            continue;
        }
        let run_ids = runs.iter().map(|run| (run.run_seq, run.level)).collect();
        let placement = merge_placement(bottom_anchored, &runs, head_seq);
        return Ok(ReorganizationSelection {
            plan: Some(ReorganizationPlan::BoundedMerge(ReorganizationInput {
                runs,
                run_ids,
                folded_l0_rows,
                decoded_rows,
                decoded_bytes,
                placement,
            })),
            group_bottom_over_budget,
            bottom_anchored_merge_blocked,
        });
    }

    // Nothing above got taken. Either no window makes progress inside the
    // budgets — the bottom-anchored window cannot reach an L0 run, so
    // retention for the group has stopped, and the delta runs above the bottom
    // are down to one or blocked as well — or the delta merge was available
    // and the runtime said this group has taken enough of them. Both end the
    // same way: a streaming compaction takes the whole group. Its input is
    // every run the group holds, which is bottom-anchored by construction, so
    // its output is the group's base run and it may drop what the floor
    // allows.
    let spec = (!candidates.is_empty()).then(|| {
        MetadataCompactionSpec::new(
            group,
            candidates
                .iter()
                .map(|run| (run.run_seq, run.level))
                .collect(),
            candidates
                .iter()
                .flat_map(|run| group_run_descriptors(run, group))
                .map(|descriptor| descriptor.row_count)
                .sum(),
            MergePlacement::Base {
                output_seq: head_seq,
            },
            frozen_floor_seq,
        )
    });
    Ok(ReorganizationSelection {
        plan: spec.map(ReorganizationPlan::FullCompaction),
        group_bottom_over_budget,
        bottom_anchored_merge_blocked,
    })
}

/// Says out loud that a family group's oldest run no longer fits one
/// reorganization step.
///
/// Nothing is corrupt when this fires and no work stops. Steps keep merging
/// the newer runs above the run named here, into one bigger delta run each
/// time. Once that merge has nothing left to take, the group has no window
/// that makes progress, and the step hands the whole group to a background
/// streaming compaction that rebuilds it in one pass under no budget at all.
/// The group's rows are reclaimed when that job publishes. So this line says
/// what a namespace has grown into, and that the rebuild of the group has
/// moved off the step; there is nothing for an operator to do about it.
///
/// One line per step until the job starts: a step is already the unit
/// maintenance schedules, so the cadence of the warning is the cadence of
/// maintenance.
fn report_group_bottom_over_budget(
    namespace_id: &NamespaceId,
    group: MetadataFamilyGroup,
    bottom: &OverBudgetRun,
    policy: MetadataLsmPolicy,
) {
    tracing::warn!(
        namespace_id = namespace_id.as_str(),
        families = ?group.families(),
        run_seq = bottom.run_seq.0,
        run_level = bottom.level,
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
/// at or above every L0 run's sequence. So a base run sitting at or above the
/// oldest L0 run says one group folded here and L0 runs remain — usually
/// other groups' rows in the very runs it just took its own rows out of. The
/// step keeps going on that evidence rather than stopping at the trigger,
/// which is what makes a run of bounded steps end in the same manifest shape
/// one unbounded step would have produced.
///
/// A merge above the base writes a delta run at its newest input's sequence
/// ([`MergePlacement`]), so it never puts a base run here and its inputs stay
/// counted as L0 pressure. A fresh L0 appended after a completed fold is
/// strictly newer than every base-tier run and therefore does not bypass the
/// normal trigger.
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

fn run_has_group_rows(run: &MetadataRunManifest, group: MetadataFamilyGroup) -> bool {
    group_run_descriptors(run, group).next().is_some()
}

pub(super) fn group_run_descriptors(
    run: &MetadataRunManifest,
    group: MetadataFamilyGroup,
) -> impl Iterator<Item = &MetadataFileRef> {
    run.tables
        .iter()
        .filter(move |table| group.contains(table.family))
        .flat_map(|table| &table.segments)
}

async fn decoded_group_run_bytes<S: ObjectStore + ?Sized>(
    tables: &VerifiedMetadataTables<'_, S>,
    run: &MetadataRunManifest,
    group: MetadataFamilyGroup,
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
/// ran — its L0 rows are frozen in the job's snapshot, so its count never
/// falls, while every other group's falls the moment it folds — and the step
/// would spend itself re-planning a job that is already running.
pub(super) fn select_family_group(
    payload: &NamespaceManifestPayload,
    rebuilding: Option<MetadataFamilyGroup>,
) -> Option<MetadataFamilyGroup> {
    REORGANIZE_FAMILY_GROUPS
        .into_iter()
        .filter(|group| Some(*group) != rebuilding)
        .map(|group| (group_l0_rows(payload, group), group))
        .filter(|(rows, _)| *rows > 0)
        .max_by(|(left_rows, left), (right_rows, right)| {
            // On ties the EARLIER group must win, and group order is the
            // enum's declaration order; comparing the groups reversed makes
            // max_by pick it.
            left_rows.cmp(right_rows).then_with(|| right.cmp(left))
        })
        .map(|(_, group)| group)
}

fn group_l0_rows(payload: &NamespaceManifestPayload, group: MetadataFamilyGroup) -> u64 {
    payload
        .metadata_files
        .iter()
        .filter(|descriptor| {
            descriptor.level == CHECKPOINT_L0_RUN_LEVEL && group.contains(descriptor.family)
        })
        .map(|descriptor| descriptor.row_count)
        .sum()
}

pub(super) async fn write_reorganized_manifest<S: ObjectStore + ?Sized>(
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
    })
    .map_err(|err| CoreError::Internal(format!("failed to build reorganized manifest: {err}")))?;
    write_namespace_manifest(store, &manifest)
        .await
        .map_err(manifest_write_failure)?;
    Ok(manifest)
}
