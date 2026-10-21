//! Plans and runs one metadata compaction unit.
//!
//! A unit selects one family group and a window of at most eight complete
//! runs from the current manifest's descriptors, merges the window with the
//! engine in [`super::streaming_compaction`], and publishes the replacement
//! manifest through that module's finalization. A base merge may apply
//! retention, and a delta merge drops only superseded layouts.
//! [`MetadataCompactionPolicy`] decides when input sizes warrant a merge.

use super::cache::MetadataSegmentCache;
use super::fold::{next_manifest_no_after, next_run_no_after};
use super::index_block_load::segment_object_bytes;
use super::load::metadata_basis_from_manifest;
use super::published::PublishedManifest;
use super::runs::{
    delta_run_count, runs_in_merge_order, runs_newest_first, MetadataFamilyGroup,
    MetadataRunManifest, MetadataRunPolicy,
};
use super::streaming_compaction::{
    finalize_metadata_compaction, input_segment_keys, run_metadata_compaction,
    CompactionPublication, MetadataCompactionCancellation, MetadataCompactionSpec,
};
use crate::error::{CoreError, Result};
use crate::limits::MAX_COMPACTION_INPUT_RUNS;
use crate::namespace::control::{ensure_namespace_live, load_current_manifest};
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::namespace::state::NamespaceReadState;
use crate::time::{Deadline, MonotonicTimer};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{
    MetadataRunRef, MetadataSegmentRef, NamespaceManifestPayload, RunTier,
};
use loonfs_types::CompactorEpoch;
use loonfs_types::{ChangeSeq, NamespaceId};
use std::sync::Arc;

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

/// Which windows one compaction call may merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionAdmission {
    /// Any window, whatever the number of rows its runs list.
    AnySize,
    /// A window whose runs list at most `max_decoded_input_rows_per_step`
    /// rows, so the call never runs a long compaction. A larger window
    /// returns [`CompactionUnitOutcome::TooLarge`].
    WithinRowBudget,
}

/// How one compaction unit ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionUnitOutcome {
    /// No family group has eligible input.
    NotNeeded,
    /// The selected window lists more rows than the call admits, so the unit
    /// merged nothing.
    TooLarge {
        /// The group the unit selected.
        group: MetadataFamilyGroup,
    },
    /// The unit merged one family group's window and published the next
    /// manifest. The new run is the group's base when the window started at
    /// the group's oldest run, and a bigger delta run otherwise.
    Published {
        /// The group this unit merged. Its families are
        /// [`MetadataFamilyGroup::families`].
        group: MetadataFamilyGroup,
        /// The published manifest.
        manifest: PublishedManifest,
        input_runs: usize,
        rows_read: u64,
        rows_written: u64,
        input_bytes: u64,
        output_bytes: u64,
        output_segments: usize,
    },
    /// The caller cancelled the unit before it published.
    Cancelled,
    /// The unit's inputs changed, its time bound passed, or every publication
    /// attempt lost.
    Abandoned,
    /// Another process claimed the namespace compactor role.
    Fenced,
}

/// Runs one compaction unit against the current manifest.
///
/// The unit loads the current manifest once, plans from its descriptors, and
/// reads its input runs without the cache. The merge checks `cancellation`
/// between blocks. The unit publishes through [`finalize_metadata_compaction`],
/// which retries a lost put over the manifest that won while the unit's input
/// segments are unchanged. The unit's time bound starts before the load (see
/// `METADATA_COMPACTION_BUDGET_MS`). Each call reloads durable state, so
/// callers may repeat it safely across process restarts.
#[allow(
    clippy::too_many_arguments,
    reason = "one unit's inputs, listed instead of grouped into a second struct"
)]
#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "metadata_compaction_step", key_class = "manifest")
)]
pub(crate) async fn compact_unit<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    compactor_epoch: CompactorEpoch,
    policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
    admission: CompactionAdmission,
    cancellation: &MetadataCompactionCancellation,
    timer: Arc<dyn MonotonicTimer>,
) -> Result<CompactionUnitOutcome> {
    let compaction = Deadline::start(Arc::clone(&timer));
    let current = load_current_manifest(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let payload = current.state.envelope.payload();
    if payload.compactor_epoch != compactor_epoch {
        return Ok(CompactionUnitOutcome::Fenced);
    }
    ensure_namespace_live(&NamespaceReadState::from(payload))?;
    // The unit reads its input runs without the cache, and the merge inserts
    // its output into the cache.
    let segments = metadata_basis_from_manifest(store, None, &current).segments;
    let Some(spec) = plan_compaction(payload, &segments.scan_runs, policy, compaction_policy)
    else {
        return Ok(CompactionUnitOutcome::NotNeeded);
    };
    if admission == CompactionAdmission::WithinRowBudget
        && spec.input_rows() > policy.max_decoded_input_rows_per_step.get() as u64
    {
        return Ok(CompactionUnitOutcome::TooLarge {
            group: spec.group(),
        });
    }
    // What the unit is about to read, recorded before it reads anything.
    // Finalization compares the manifest against this, so the run it
    // publishes stands in for exactly the segments it merged.
    let input_keys = input_segment_keys(&segments.scan_runs, &spec)
        .expect("a plan should list runs of the manifest it was made from");
    tracing::debug!(
        namespace_id = namespace_id.as_str(),
        job_id = spec.job_id().as_str(),
        families = ?spec.families(),
        input_runs = spec.input_runs(),
        input_rows = spec.input_rows(),
        frozen_floor_seq = spec.frozen_floor_seq().0,
        "metadata compaction unit started"
    );
    let outcome = match run_metadata_compaction(
        &segments,
        segment_cache,
        namespace_id,
        &spec,
        policy,
        cancellation,
    )
    .await?
    {
        Ok(merged) => {
            drop(segments);
            finalize_metadata_compaction(
                store,
                namespace_id,
                &spec,
                &input_keys,
                merged,
                cancellation,
                &CompactionPublication {
                    compactor_epoch,
                    compaction,
                    publication: Deadline::start(timer),
                },
            )
            .await?
        }
        Err(stopped) => stopped,
    };
    log_unit_outcome(namespace_id, &spec, &outcome);
    Ok(outcome)
}

fn log_unit_outcome(
    namespace_id: &NamespaceId,
    spec: &MetadataCompactionSpec,
    outcome: &CompactionUnitOutcome,
) {
    match outcome {
        CompactionUnitOutcome::Published {
            manifest,
            input_runs,
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments,
            ..
        } => tracing::info!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            input_runs,
            rows_read,
            rows_written,
            input_bytes,
            output_bytes,
            output_segments,
            manifest_no = manifest.manifest.manifest_no.0,
            "metadata compaction unit published"
        ),
        CompactionUnitOutcome::Cancelled => tracing::info!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            "metadata compaction unit cancelled"
        ),
        CompactionUnitOutcome::Abandoned => tracing::info!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            "metadata compaction unit abandoned"
        ),
        CompactionUnitOutcome::Fenced => tracing::warn!(
            namespace_id = namespace_id.as_str(),
            job_id = spec.job_id().as_str(),
            families = ?spec.families(),
            "metadata compaction unit fenced"
        ),
        CompactionUnitOutcome::NotNeeded | CompactionUnitOutcome::TooLarge { .. } => {}
    }
}

/// Plans the compaction unit that a manifest warrants, from its descriptors.
///
/// The plan merges the oldest eligible window of the family group with the
/// most delta rows. A window has at most eight complete runs, and its sizes
/// come from the manifest's block handles, so planning reads no data.
pub(super) fn plan_compaction(
    payload: &NamespaceManifestPayload,
    runs: &[MetadataRunManifest],
    policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> Option<MetadataCompactionSpec> {
    if !compaction_triggered(payload, runs, policy, compaction_policy) {
        return None;
    }
    let group = select_family_group(runs, payload, compaction_policy, policy)?;
    plan_group(payload, runs, group, policy, compaction_policy)
}

/// Plans a unit over `group`'s oldest eligible window, whether or not the
/// manifest has reached its compaction trigger.
pub(super) fn plan_group(
    payload: &NamespaceManifestPayload,
    runs: &[MetadataRunManifest],
    group: MetadataFamilyGroup,
    policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> Option<MetadataCompactionSpec> {
    let candidates = group_candidates(runs, group);
    let range = select_merge_window(
        &candidates,
        group,
        compaction_policy,
        policy.small_run_bytes.get() as u64,
        effective_max_input_runs(policy),
    )?;
    let bottom_anchored = range.start == 0;
    let selected = candidates[range]
        .iter()
        .map(|run| (*run).clone())
        .collect::<Vec<_>>();
    Some(MetadataCompactionSpec::new(
        group,
        selected.iter().map(|run| run.run_no).collect(),
        selected
            .iter()
            .flat_map(|run| group_run_descriptors(run, group))
            .map(|descriptor| descriptor.row_count)
            .sum(),
        merge_placement(bottom_anchored, &selected, payload.head_seq),
        payload.retention_floor_seq,
    ))
}

/// Checks an anchor's manifest descriptors for compaction work without
/// reading the store.
pub fn metadata_compaction_due(
    anchor: &NamespaceReadAnchor,
    compaction_policy: MetadataCompactionPolicy,
) -> bool {
    let manifest = anchor.manifest.state.envelope.payload();
    manifest_has_compaction_work(
        manifest,
        &runs_newest_first(manifest),
        MetadataRunPolicy::default(),
        compaction_policy,
    )
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
    /// "Retention and compaction").
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

fn group_candidates(
    runs: &[MetadataRunManifest],
    group: MetadataFamilyGroup,
) -> Vec<&MetadataRunManifest> {
    runs_in_merge_order(
        runs.iter()
            .filter(|run| run_has_group_rows(run, group))
            .collect(),
    )
}

/// Returns the run fan-in one unit may merge: the policy's knob, capped by
/// the ceiling.
fn effective_max_input_runs(policy: MetadataRunPolicy) -> usize {
    policy
        .max_input_runs_per_step
        .get()
        .min(MAX_COMPACTION_INPUT_RUNS)
}

/// Select the oldest eligible contiguous window. Never mix rows across an
/// unselected gap: a base merge may drop history only when it includes a prefix.
/// Sizes come from the manifest's block handles, so planning needs no data reads.
fn select_merge_window(
    candidates: &[&MetadataRunManifest],
    group: MetadataFamilyGroup,
    policy: MetadataCompactionPolicy,
    small_run_bytes: u64,
    max_input_runs: usize,
) -> Option<std::ops::Range<usize>> {
    for start in 0..candidates.len() {
        if start == 0
            && policy == MetadataCompactionPolicy::CompactImmediately
            && candidates.len() > max_input_runs
            && group_run_descriptors(candidates[0], group)
                .map(segment_object_bytes)
                .sum::<u64>()
                > small_run_bytes
        {
            continue;
        }
        let end = candidates.len().min(start + max_input_runs);
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
        // just to change its tier.
        return bottom_anchored && oldest.tier == RunTier::Delta;
    }
    let stored_bytes = |run: &&MetadataRunManifest| {
        group_run_descriptors(run, group)
            .map(segment_object_bytes)
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

/// A bottom-anchored compaction stamps its base run at the manifest head, which is
/// at or above every delta run's sequence. So a base run sitting at or above the
/// oldest delta run says one group compacted here and delta runs remain — usually
/// other groups' rows in the very runs it just took its own rows out of. The
/// planner keeps going on that evidence rather than stopping at the trigger,
/// which is what makes a series of units end in the same manifest shape one
/// unbounded merge would have produced.
///
/// A merge above the base writes a delta run at its newest input's sequence
/// ([`MergePlacement`]), so it never puts a base run here and its inputs stay
/// counted as delta pressure. A fresh delta run appended after a completed compaction is
/// strictly newer than every base-tier run and therefore does not bypass the
/// normal trigger.
fn manifest_has_partial_compaction(runs: &[MetadataRunManifest]) -> bool {
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

fn manifest_has_compaction_work(
    payload: &NamespaceManifestPayload,
    runs: &[MetadataRunManifest],
    policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> bool {
    compaction_triggered(payload, runs, policy, compaction_policy)
        && select_family_group(runs, payload, compaction_policy, policy).is_some()
}

fn compaction_triggered(
    payload: &NamespaceManifestPayload,
    runs: &[MetadataRunManifest],
    policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> bool {
    compaction_policy == MetadataCompactionPolicy::CompactImmediately
        || delta_run_count(payload) >= policy.max_delta_runs.get()
        || manifest_has_partial_compaction(runs)
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

pub(super) fn select_family_group(
    runs: &[MetadataRunManifest],
    payload: &NamespaceManifestPayload,
    compaction_policy: MetadataCompactionPolicy,
    policy: MetadataRunPolicy,
) -> Option<MetadataFamilyGroup> {
    for group in ranked_family_groups(payload) {
        let candidates = group_candidates(runs, group);
        if select_merge_window(
            &candidates,
            group,
            compaction_policy,
            policy.small_run_bytes.get() as u64,
            effective_max_input_runs(policy),
        )
        .is_none()
        {
            continue;
        }
        return Some(group);
    }
    None
}

pub(super) fn ranked_family_groups(payload: &NamespaceManifestPayload) -> Vec<MetadataFamilyGroup> {
    let mut ranked = MetadataFamilyGroup::ALL
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

/// The surviving and new runs determine the base sequence and next run number.
pub(super) fn build_replacement_manifest(
    namespace_id: &NamespaceId,
    previous: &NamespaceManifestPayload,
    surviving: Vec<MetadataRunRef>,
    output: ReplacementOutput,
    floor_seq: ChangeSeq,
) -> Result<NamespaceManifestPayload> {
    let next_run_no = if output.segments.is_empty() {
        previous.next_run_no
    } else {
        next_run_no_after(previous.next_run_no)?
    };
    let mut runs = surviving;
    if !output.segments.is_empty() {
        runs.push(MetadataRunRef {
            run_no: previous.next_run_no,
            run_seq: output.placement.output_seq(),
            tier: output.placement.output_tier(),
            segments: output.segments,
        });
    }
    let retention_floor_seq = previous.retention_floor_seq.max(floor_seq);
    let manifest_no = next_manifest_no_after(previous.manifest_no)?;
    Ok(NamespaceManifestPayload {
        compactor_epoch: previous.compactor_epoch,
        namespace_id: namespace_id.clone(),
        manifest_no,
        head_seq: previous.head_seq,
        writer_epoch: previous.writer_epoch,
        next_inode_id: previous.next_inode_id,
        next_run_no,
        retention_floor_seq,
        runs,
        ..previous.clone()
    })
}

#[cfg(test)]
mod planning_tests {
    use super::super::runs::{runs_newest_first, MetadataFamilySegments};
    use super::*;
    use loonfs_types::format::manifest::{decode_namespace_manifest_json, MetadataRowFamily};
    use loonfs_types::RunNo;

    // The planner only reads descriptors. Give valid descriptor shapes arbitrary
    // stored lengths to exercise GiB-scale layouts without allocating their data.
    fn runs(sizes: &[u64]) -> Vec<MetadataRunManifest> {
        let manifest = decode_namespace_manifest_json(include_bytes!(
            "../../../loonfs-types/tests/golden/manifest.v1.json"
        ))
        .expect("manifest fixture");
        let mut template = runs_newest_first(manifest.payload()).remove(0);
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
                    *size - u64::from(segments[0].index_block.stored_bytes);
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
            MAX_COMPACTION_INPUT_RUNS,
        )
    }

    #[test]
    fn a_lone_delta_is_promoted_but_a_lone_base_is_not_selected() {
        let mut runs = runs(&[1024]);
        for policy in [
            MetadataCompactionPolicy::SizeTiered,
            MetadataCompactionPolicy::CompactImmediately,
        ] {
            for (tier, expected) in [(RunTier::Base, None), (RunTier::Delta, Some(0..1))] {
                runs[0].tier = tier;
                assert_eq!(
                    select_merge_window(
                        &runs.iter().collect::<Vec<_>>(),
                        MetadataFamilyGroup::Inodes,
                        policy,
                        8 * 1024 * 1024,
                        MAX_COMPACTION_INPUT_RUNS,
                    ),
                    expected
                );
            }
        }
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

    #[test]
    fn immediate_compaction_merges_deltas_before_rewriting_a_large_base() {
        let policy = MetadataCompactionPolicy::CompactImmediately;
        for delta_runs in [12, 20] {
            let mut sizes = vec![4096];
            sizes.extend(vec![1; delta_runs]);
            while sizes.len() > MAX_COMPACTION_INPUT_RUNS {
                let selected = window(&sizes, policy).expect("delta merge");
                assert_eq!(selected, 1..1 + MAX_COMPACTION_INPUT_RUNS);
                let merged_size = sizes[selected.clone()].iter().sum();
                sizes.splice(selected, [merged_size]);
            }
            assert_eq!(window(&sizes, policy), Some(0..sizes.len()));
        }
        assert_eq!(window(&[8, 1, 1], policy), Some(0..3));
        assert_eq!(window(&[8; 12], policy), Some(0..MAX_COMPACTION_INPUT_RUNS));
        assert_eq!(window(&[4096, 1, 1, 1, 1, 1, 1, 1], policy), Some(0..8));
    }
}
