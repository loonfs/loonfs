//! Plans and runs one metadata compaction unit.
//!
//! A unit selects one family group and a window of at most eight complete
//! runs from the current manifest's descriptors, merges the window with the
//! engine in [`super::streaming_compaction`], and publishes the replacement
//! manifest through that module's finalization. A base merge may apply
//! retention, and a delta merge drops only superseded layouts.
//! [`MetadataCompactionPolicy`] decides which runs a group merges, and when.

use super::cache::MetadataSegmentCache;
use super::fold::{next_manifest_no_after, next_run_no_after};
use super::index_block_load::segment_object_bytes;
use super::load::metadata_basis_from_manifest;
use super::published::PublishedManifest;
use super::runs::{
    runs_in_merge_order, runs_newest_first, MetadataFamilyGroup, MetadataRunManifest,
    MetadataRunPolicy,
};
use super::streaming_compaction::{
    finalize_metadata_compaction, input_segment_keys, run_metadata_compaction,
    CompactionPublication, MetadataCompactionCancellation, MetadataCompactionSpec,
};
use crate::error::{CoreError, Result};
use crate::limits::MAX_COMPACTION_INPUT_RUNS;
use crate::namespace::control::{ensure_namespace_live, load_current_manifest, CurrentManifest};
use crate::namespace::state::NamespaceReadState;
use crate::time::{Deadline, MonotonicTimer};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{
    MetadataRunRef, MetadataSegmentRef, NamespaceManifestPayload, RunTier,
};
use loonfs_types::CompactorEpoch;
use loonfs_types::{ChangeSeq, NamespaceId};
use std::sync::Arc;

/// A run joins a size tier when its stored bytes are at most this many times
/// the smallest run already in the tier.
const COMPACTION_SIZE_RATIO: u64 = 4;

/// Which runs of a family group a unit merges, and when.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MetadataCompactionPolicy {
    /// Merge a tier of similar-size runs once it has
    /// [`MetadataRunPolicy::min_compaction_runs`] runs, and a group's newest
    /// runs once it has [`MetadataRunPolicy::max_delta_runs`] delta runs.
    #[default]
    SizeTiered,
    /// Merge a group's runs whatever their sizes, for an explicit compaction
    /// request. Input fan-in stays bounded.
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
/// The plan merges the window of the family group with the most delta rows,
/// among the groups that have a window to merge. A window has at most eight
/// complete runs, and its sizes come from the manifest's block handles, so
/// planning reads no data.
pub(super) fn plan_compaction(
    payload: &NamespaceManifestPayload,
    runs: &[MetadataRunManifest],
    policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> Option<MetadataCompactionSpec> {
    let group = select_family_group(runs, payload, compaction_policy, policy)?;
    plan_group(payload, runs, group, policy, compaction_policy)
}

/// Plans a unit over `group`'s window, if the group has one to merge.
pub(super) fn plan_group(
    payload: &NamespaceManifestPayload,
    runs: &[MetadataRunManifest],
    group: MetadataFamilyGroup,
    policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> Option<MetadataCompactionSpec> {
    let candidates = group_candidates(runs, group);
    let range = select_merge_window(&candidates, group, policy, compaction_policy)?;
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

/// Checks a manifest's descriptors for compaction work without reading the
/// store.
pub fn metadata_compaction_due(
    manifest: &CurrentManifest,
    compaction_policy: MetadataCompactionPolicy,
) -> bool {
    let manifest = manifest.envelope.payload();
    select_family_group(
        &runs_newest_first(manifest),
        manifest,
        compaction_policy,
        MetadataRunPolicy::default(),
    )
    .is_some()
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

/// Returns the runs a size tier needs before it merges. A merge above the
/// group's oldest run needs at least two runs (format spec, "Compaction
/// windows"), so a minimum of one acts as two.
fn effective_min_compaction_runs(policy: MetadataRunPolicy) -> usize {
    policy.min_compaction_runs.get().max(2)
}

/// Selects the contiguous window of `candidates`, which are in merge order,
/// that a unit merges for `group`, or `None` when the group has nothing due.
/// Sizes come from the manifest's block handles, so planning needs no data
/// reads.
fn select_merge_window(
    candidates: &[&MetadataRunManifest],
    group: MetadataFamilyGroup,
    policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> Option<std::ops::Range<usize>> {
    let max_runs = effective_max_input_runs(policy);
    let stored_bytes = candidates
        .iter()
        .map(|run| {
            group_run_descriptors(run, group)
                .map(segment_object_bytes)
                .sum::<u64>()
        })
        .collect::<Vec<_>>();
    let window = match compaction_policy {
        MetadataCompactionPolicy::CompactImmediately => {
            Some(immediate_window(&stored_bytes, max_runs))
        }
        MetadataCompactionPolicy::SizeTiered => {
            let min_runs = effective_min_compaction_runs(policy);
            tiered_window(&stored_bytes, min_runs, max_runs).or_else(|| {
                capped_window(candidates, policy.max_delta_runs.get(), min_runs, max_runs)
            })
        }
    }?;
    window_is_valid(candidates, &window).then_some(window)
}

/// Returns the window that merges a group's runs whatever their sizes.
///
/// A group that fits in one unit merges whole. A larger group merges its
/// oldest `max_runs` runs, unless the oldest run is too large to join a tier
/// with the runs above it. Then the unit merges the runs above it first, so
/// the large run is rewritten once, by the unit that finishes the group.
/// `stored_bytes` lists the runs in merge order, oldest first.
fn immediate_window(stored_bytes: &[u64], max_runs: usize) -> std::ops::Range<usize> {
    let runs = stored_bytes.len();
    if runs <= max_runs {
        return 0..runs;
    }
    let oldest_joins = stored_bytes[1..max_runs]
        .iter()
        .min()
        .is_none_or(|smallest| stored_bytes[0] <= smallest.saturating_mul(COMPACTION_SIZE_RATIO));
    if oldest_joins {
        0..max_runs
    } else {
        1..1 + max_runs
    }
}

/// Returns the newest size tier that has at least `min_runs` runs.
///
/// A tier starts at one run and grows toward older runs while the next older
/// run's stored bytes are at most [`COMPACTION_SIZE_RATIO`] times the smallest
/// run already in it, so the largest tier the ratio allows is merged. A tier
/// longer than `max_runs` merges its oldest runs, and the newer ones wait for
/// the next unit. `stored_bytes` lists the runs in merge order, oldest first.
fn tiered_window(
    stored_bytes: &[u64],
    min_runs: usize,
    max_runs: usize,
) -> Option<std::ops::Range<usize>> {
    (1..=stored_bytes.len()).rev().find_map(|end| {
        let mut start = end - 1;
        let mut smallest = stored_bytes[start];
        while start > 0 && stored_bytes[start - 1] <= smallest.saturating_mul(COMPACTION_SIZE_RATIO)
        {
            start -= 1;
            smallest = smallest.min(stored_bytes[start]);
        }
        (end - start >= min_runs).then(|| start..end.min(start + max_runs))
    })
}

/// Returns the group's newest `min_runs` runs once it has `max_delta_runs`
/// delta runs, so the runs a read consults stay bounded when no tier is ready.
fn capped_window(
    candidates: &[&MetadataRunManifest],
    max_delta_runs: usize,
    min_runs: usize,
    max_runs: usize,
) -> Option<std::ops::Range<usize>> {
    let delta_runs = candidates
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .count();
    (delta_runs >= max_delta_runs)
        .then(|| candidates.len().saturating_sub(min_runs.min(max_runs))..candidates.len())
}

/// Checks that a window merges at least two runs, or promotes the group's
/// oldest run from a delta to the base. A one-run merge of anything else
/// would rewrite the run without changing the group.
fn window_is_valid(candidates: &[&MetadataRunManifest], window: &std::ops::Range<usize>) -> bool {
    match window.len() {
        0 => false,
        1 => window.start == 0 && candidates[0].tier == RunTier::Delta,
        _ => true,
    }
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
    ranked_family_groups(payload).into_iter().find(|group| {
        select_merge_window(
            &group_candidates(runs, *group),
            *group,
            policy,
            compaction_policy,
        )
        .is_some()
    })
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
    use super::super::runs::runs_newest_first;
    use super::*;
    use loonfs_types::format::manifest::{decode_namespace_manifest_json, MetadataRowFamily};
    use loonfs_types::RunNo;
    use std::num::NonZeroUsize;
    use std::ops::Range;

    const MIB: u64 = 1024 * 1024;

    /// A manifest whose only group, the inodes, has one run of each size in
    /// MiB, in merge order: a base, then delta runs from oldest to newest.
    ///
    /// The planner only reads descriptors, so the runs share the fixture's
    /// descriptor and differ only in the stored length its index block
    /// implies. That exercises GiB-scale layouts without allocating their
    /// data.
    fn manifest(sizes_mib: &[u64]) -> NamespaceManifestPayload {
        let document = include_bytes!("../../../loonfs-types/tests/golden/manifest.v1.json");
        let stored = zstd::stream::encode_all(&document[..], 0).expect("compress fixture");
        let fixture = decode_namespace_manifest_json(&stored).expect("manifest fixture");
        let mut payload = fixture.payload().clone();
        let mut template = payload.runs[0].clone();
        template
            .segments
            .retain(|segment| segment.family == MetadataRowFamily::Inodes);
        payload.runs = sizes_mib
            .iter()
            .enumerate()
            .map(|(index, size_mib)| {
                let mut run = template.clone();
                run.run_no = RunNo(index as u64);
                run.run_seq = ChangeSeq(index as u64);
                run.tier = if index == 0 {
                    RunTier::Base
                } else {
                    RunTier::Delta
                };
                let index_block = &mut run.segments[0].index_block;
                index_block.offset = size_mib * MIB - u64::from(index_block.stored_bytes);
                run
            })
            .collect();
        payload.head_seq = ChangeSeq(sizes_mib.len() as u64);
        payload.next_run_no = RunNo(sizes_mib.len() as u64);
        payload
    }

    /// The positions, in merge order, of the runs the planner merges.
    fn planned(
        payload: &NamespaceManifestPayload,
        policy: MetadataRunPolicy,
        compaction_policy: MetadataCompactionPolicy,
    ) -> Option<Range<u64>> {
        let spec = plan_compaction(
            payload,
            &runs_newest_first(payload),
            policy,
            compaction_policy,
        )?;
        let inputs = spec.inputs();
        let window = inputs[0].0..inputs[inputs.len() - 1].0 + 1;
        assert_eq!(
            inputs,
            window.clone().map(RunNo).collect::<Vec<_>>(),
            "a window is contiguous"
        );
        Some(window)
    }

    fn tiered(sizes_mib: &[u64]) -> Option<Range<u64>> {
        planned(
            &manifest(sizes_mib),
            MetadataRunPolicy::default(),
            MetadataCompactionPolicy::SizeTiered,
        )
    }

    fn immediate(sizes_mib: &[u64]) -> Option<Range<u64>> {
        planned(
            &manifest(sizes_mib),
            MetadataRunPolicy::default(),
            MetadataCompactionPolicy::CompactImmediately,
        )
    }

    fn min_compaction_runs() -> usize {
        MetadataRunPolicy::default().min_compaction_runs.get()
    }

    #[test]
    fn a_lone_delta_is_promoted_at_the_delta_run_limit_or_on_request_but_a_lone_base_is_not() {
        let at_the_limit = MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::MIN,
            ..MetadataRunPolicy::default()
        };
        for (tier, policy, compaction_policy, expected) in [
            (
                RunTier::Delta,
                MetadataRunPolicy::default(),
                MetadataCompactionPolicy::SizeTiered,
                None,
            ),
            (
                RunTier::Delta,
                at_the_limit,
                MetadataCompactionPolicy::SizeTiered,
                Some(0..1),
            ),
            (
                RunTier::Delta,
                MetadataRunPolicy::default(),
                MetadataCompactionPolicy::CompactImmediately,
                Some(0..1),
            ),
            (
                RunTier::Base,
                at_the_limit,
                MetadataCompactionPolicy::SizeTiered,
                None,
            ),
            (
                RunTier::Base,
                MetadataRunPolicy::default(),
                MetadataCompactionPolicy::CompactImmediately,
                None,
            ),
        ] {
            let mut payload = manifest(&[1024]);
            payload.runs[0].tier = tier;
            assert_eq!(
                planned(&payload, policy, compaction_policy),
                expected,
                "{tier:?} {compaction_policy:?}"
            );
        }
    }

    #[test]
    fn a_tier_is_due_at_min_compaction_runs_and_not_one_run_earlier() {
        let tier = min_compaction_runs();
        // Similar runs, the base among them.
        assert_eq!(tiered(&vec![1; tier - 1]), None);
        assert_eq!(tiered(&vec![1; tier]), Some(0..tier as u64));
        // Similar delta runs above a base too large to join them.
        let mut sizes = vec![1024];
        sizes.extend(vec![1; tier - 1]);
        assert_eq!(tiered(&sizes), None);
        sizes.push(1);
        assert_eq!(tiered(&sizes), Some(1..1 + tier as u64));
    }

    #[test]
    fn the_planner_merges_the_largest_tier_the_ratio_allows() {
        let tier = min_compaction_runs() as u64;
        // Every delta run, not just the first `tier` of them.
        assert_eq!(tiered(&[1024, 1, 1, 1, 1, 1, 1]), Some(1..7));
        // The base joins when it is at most four times the smallest run.
        assert_eq!(tiered(&[4, 1, 1, 1, 1, 1]), Some(0..6));
        assert_eq!(tiered(&[5, 1, 1, 1, 1, 1]), Some(1..6));
        // A tier stops at a run more than four times its smallest run, so a
        // large delta waits for runs of its own size, like a large base.
        assert_eq!(tiered(&[4096, 256, 1, 1, 1, 1]), Some(2..6));
        assert_eq!(tiered(&[4096, 256, 64, 64, 64]), Some(1..5));
        assert_eq!(tiered(&[1024, 255, 255, 255, 255]), Some(1..5));
        assert_eq!(tiered(&[1024, 256, 256, 256, 256]), Some(0..5));
        // A small newest run doesn't keep an older tier from merging.
        assert_eq!(tiered(&[1024, 16, 16, 16, 16, 1]), Some(1..1 + tier));
    }

    #[test]
    fn a_group_at_the_delta_run_limit_merges_its_newest_runs_when_no_tier_is_ready() {
        let policy = MetadataRunPolicy::default();
        let limit = policy.max_delta_runs.get();
        // Each run is five times the next newer one, so no two runs form a tier.
        let sizes = (0..=u32::try_from(limit).expect("the test limit fits a u32"))
            .rev()
            .map(|power| 5_u64.pow(power))
            .collect::<Vec<_>>();
        assert_eq!(tiered(&sizes[1..]), None, "one delta run below the limit");
        let runs = sizes.len() as u64;
        assert_eq!(
            tiered(&sizes),
            Some(runs - min_compaction_runs() as u64..runs)
        );
    }

    #[test]
    fn a_backlog_is_merged_in_contiguous_windows_with_bounded_fan_in() {
        let fan_in = MAX_COMPACTION_INPUT_RUNS as u64;
        assert_eq!(tiered(&[1; 100]), Some(0..fan_in));
        let mut sizes = vec![4096];
        sizes.extend([1; 100]);
        assert_eq!(tiered(&sizes), Some(1..1 + fan_in));
    }

    #[test]
    fn immediate_compaction_merges_deltas_before_rewriting_a_large_base() {
        let fan_in = MAX_COMPACTION_INPUT_RUNS;
        for delta_runs in [12, 20] {
            let mut sizes = vec![4096];
            sizes.extend(vec![1; delta_runs]);
            while sizes.len() > fan_in {
                let selected = immediate(&sizes).expect("delta merge");
                assert_eq!(selected, 1..1 + fan_in as u64);
                let selected = 1..1 + fan_in;
                let merged_size = sizes[selected.clone()].iter().sum();
                sizes.splice(selected, [merged_size]);
            }
            assert_eq!(immediate(&sizes), Some(0..sizes.len() as u64));
        }
        assert_eq!(immediate(&[8, 1, 1]), Some(0..3));
        assert_eq!(immediate(&[1024, 2]), Some(0..2));
        assert_eq!(immediate(&[8; 12]), Some(0..fan_in as u64));
        // A base joins the runs above it when a tier would take it.
        let mut sizes = vec![4];
        sizes.extend([1; MAX_COMPACTION_INPUT_RUNS]);
        assert_eq!(immediate(&sizes), Some(0..fan_in as u64));
        sizes[0] = 5;
        assert_eq!(immediate(&sizes), Some(1..1 + fan_in as u64));
        assert_eq!(
            immediate(&[4096, 1, 1, 1, 1, 1, 1, 1]),
            Some(0..fan_in as u64)
        );
    }
}
