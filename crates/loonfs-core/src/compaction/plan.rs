//! Plans one metadata compaction unit: the family group it merges, the
//! window of runs, and the tier and sequence of the output run.

use super::step::MetadataCompactionPolicy;
use crate::limits::MAX_COMPACTION_INPUT_RUNS;
use crate::namespace::control::LoadedManifest;
use crate::segment::index_block_load::segment_object_bytes;
use crate::segment::runs::{
    runs_in_merge_order, runs_newest_first, MetadataFamilyGroup, MetadataRunManifest,
    MetadataRunPolicy,
};
use loonfs_types::format::manifest::{
    MetadataRowFamily, MetadataSegmentRef, NamespaceManifestPayload, RunTier,
};
use loonfs_types::{ChangeSeq, MetadataCompactionId, RunNo};
use std::num::NonZeroUsize;

/// The size ratio within which a run joins a merge window.
///
/// A run joins a size tier when its stored bytes are at most this many times
/// the smallest run already in the tier. At the delta run limit, a run older
/// than the window's two newest runs joins the window when its stored bytes
/// are at most this many times the total of the runs already in the window.
const COMPACTION_SIZE_RATIO: u64 = 4;

/// The immutable plan of one compaction unit.
///
/// Everything about the unit is set here before it starts: which group it
/// rebuilds, which runs it reads, where its output stands, and the retention
/// floor every row is judged against. A unit run again from the same spec
/// against the same durable state produces the same rows, which is what makes
/// a cancelled attempt free to throw away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataCompactionSpec {
    job_id: MetadataCompactionId,
    group: MetadataFamilyGroup,
    /// A contiguous window of at most eight runs, in oldest-first order.
    pub(crate) inputs: Vec<RunNo>,
    /// Rows those runs list for the group, from their descriptors. What the
    /// unit reports it is about to read. The rows it writes are fewer by
    /// whatever the floor lets go.
    input_rows: u64,
    pub(crate) placement: MergePlacement,
    frozen_floor_seq: ChangeSeq,
}

impl MetadataCompactionSpec {
    pub(crate) fn new(
        group: MetadataFamilyGroup,
        inputs: Vec<RunNo>,
        input_rows: u64,
        placement: MergePlacement,
        frozen_floor_seq: ChangeSeq,
    ) -> Self {
        assert!(
            !inputs.is_empty() && inputs.len() <= crate::limits::MAX_COMPACTION_INPUT_RUNS,
            "a compaction plan must have bounded, nonempty input"
        );
        Self {
            job_id: MetadataCompactionId::generate(),
            group,
            inputs,
            input_rows,
            placement,
            frozen_floor_seq,
        }
    }

    /// The family group this unit rebuilds.
    pub(crate) fn group(&self) -> MetadataFamilyGroup {
        self.group
    }

    pub(crate) fn job_id(&self) -> &MetadataCompactionId {
        &self.job_id
    }

    /// The families this unit rebuilds, for a caller reporting what it started.
    pub(crate) fn families(&self) -> &'static [MetadataRowFamily] {
        self.group.families()
    }

    /// How many runs the unit reads.
    pub(crate) fn input_runs(&self) -> usize {
        self.inputs.len()
    }

    /// How many rows those runs contain.
    pub(crate) fn input_rows(&self) -> u64 {
        self.input_rows
    }

    pub(crate) fn inputs(&self) -> &[RunNo] {
        &self.inputs
    }

    pub(crate) fn frozen_floor_seq(&self) -> ChangeSeq {
        self.frozen_floor_seq
    }

    pub(crate) fn placement(&self) -> MergePlacement {
        self.placement
    }

    #[cfg(test)]
    pub(crate) fn with_frozen_floor_seq(&self, frozen_floor_seq: ChangeSeq) -> Self {
        Self {
            frozen_floor_seq,
            ..self.clone()
        }
    }
}

/// Plans the compaction unit that a manifest warrants, from its descriptors.
///
/// The plan merges the window of the family group with the most delta rows,
/// among the groups that have a window to merge. A window has at most eight
/// complete runs, and its sizes come from the manifest's block handles, so
/// planning reads no data.
pub(crate) fn plan_compaction(
    payload: &NamespaceManifestPayload,
    runs: &[MetadataRunManifest],
    run_policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> Option<MetadataCompactionSpec> {
    let group = select_family_group(runs, payload, compaction_policy, run_policy)?;
    plan_group(payload, runs, group, run_policy, compaction_policy)
}

/// Plans a unit over `group`'s window, if the group has one to merge.
pub(crate) fn plan_group(
    payload: &NamespaceManifestPayload,
    runs: &[MetadataRunManifest],
    group: MetadataFamilyGroup,
    run_policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> Option<MetadataCompactionSpec> {
    let candidates = group_candidates(runs, group);
    let range = select_merge_window(&candidates, group, run_policy, compaction_policy)?;
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
    manifest: &LoadedManifest,
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
pub(crate) enum MergePlacement {
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
fn effective_max_input_runs(run_policy: MetadataRunPolicy) -> usize {
    run_policy
        .max_input_runs_per_step
        .get()
        .min(MAX_COMPACTION_INPUT_RUNS)
}

/// Returns the runs a size tier needs before it merges. A merge above the
/// group's oldest run needs at least two runs (format spec, "Compaction
/// windows"), so a minimum of one acts as two.
fn effective_min_compaction_runs(run_policy: MetadataRunPolicy) -> usize {
    run_policy.min_compaction_runs.get().max(2)
}

/// Selects the contiguous window of `candidates`, which are in merge order,
/// that a unit merges for `group`, or `None` when the group has nothing due.
/// Sizes come from the manifest's block handles, so planning needs no data
/// reads.
fn select_merge_window(
    candidates: &[&MetadataRunManifest],
    group: MetadataFamilyGroup,
    run_policy: MetadataRunPolicy,
    compaction_policy: MetadataCompactionPolicy,
) -> Option<std::ops::Range<usize>> {
    let max_runs = effective_max_input_runs(run_policy);
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
            let min_runs = effective_min_compaction_runs(run_policy);
            tiered_window(&stored_bytes, min_runs, max_runs).or_else(|| {
                capped_window(
                    candidates,
                    &stored_bytes,
                    run_policy.max_delta_runs,
                    max_runs,
                )
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

/// Returns the window a group merges once the group has `max_delta_runs`
/// delta runs and no size tier is ready, so the number of runs a read checks
/// stays bounded.
///
/// The window starts with the two newest runs, because merging one run
/// wouldn't reduce the group's run count. It grows toward older runs while
/// the next older run's stored bytes are at most [`COMPACTION_SIZE_RATIO`]
/// times the total stored bytes of the runs already in it, up to `max_runs`.
/// `stored_bytes` lists the runs in merge order, oldest first.
fn capped_window(
    candidates: &[&MetadataRunManifest],
    stored_bytes: &[u64],
    max_delta_runs: NonZeroUsize,
    max_runs: usize,
) -> Option<std::ops::Range<usize>> {
    let delta_runs = candidates
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .count();
    if delta_runs < max_delta_runs.get() {
        return None;
    }
    let end = stored_bytes.len();
    let mut start = end - 1;
    let mut window_bytes = stored_bytes[start];
    while start > 0
        && end - start < max_runs
        && (end - start == 1
            || stored_bytes[start - 1] <= window_bytes.saturating_mul(COMPACTION_SIZE_RATIO))
    {
        start -= 1;
        window_bytes = window_bytes.saturating_add(stored_bytes[start]);
    }
    Some(start..end)
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

pub(crate) fn group_run_descriptors(
    run: &MetadataRunManifest,
    group: MetadataFamilyGroup,
) -> impl Iterator<Item = &MetadataSegmentRef> {
    run.segments
        .iter()
        .filter(move |family_segments| group.families().contains(&family_segments.family))
        .flat_map(|family_segments| &family_segments.segments)
}

pub(crate) fn select_family_group(
    runs: &[MetadataRunManifest],
    payload: &NamespaceManifestPayload,
    compaction_policy: MetadataCompactionPolicy,
    run_policy: MetadataRunPolicy,
) -> Option<MetadataFamilyGroup> {
    ranked_family_groups(payload).into_iter().find(|group| {
        select_merge_window(
            &group_candidates(runs, *group),
            *group,
            run_policy,
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

#[cfg(test)]
mod planning_tests {
    use super::*;
    use crate::segment::runs::runs_newest_first;
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
        // The window starts with the two newest runs, 1 and 5 MiB, and the
        // next older run is more than four times their total.
        let runs = sizes.len() as u64;
        assert_eq!(tiered(&sizes), Some(runs - 2..runs));
    }

    /// The window the planner merges for a group at the delta run limit, with
    /// a tier minimum above the group's run count so that no size tier is due.
    fn capped(sizes_mib: &[u64]) -> Option<Range<u64>> {
        let policy = MetadataRunPolicy {
            max_delta_runs: NonZeroUsize::new(sizes_mib.len() - 1)
                .expect("the group should have a delta run"),
            min_compaction_runs: NonZeroUsize::new(sizes_mib.len() + 1)
                .expect("a run count plus one is nonzero"),
            ..MetadataRunPolicy::default()
        };
        planned(
            &manifest(sizes_mib),
            policy,
            MetadataCompactionPolicy::SizeTiered,
        )
    }

    #[test]
    fn a_capped_window_takes_the_two_newest_runs_and_each_older_run_within_the_ratio() {
        for (sizes, expected) in [
            // An earlier output more than four times the small runs above it
            // stays out of their merge.
            (vec![50_000, 12_000, 8, 8, 8], 2..5),
            // The two newest runs merge whatever their sizes, so the output
            // joins one small run.
            (vec![50_000, 12_000, 8], 1..3),
            // After the two newest runs, the window keeps growing while the
            // next older run is within four times their total: 40 is at most
            // four times 31.
            (vec![50_000, 40, 30, 1], 1..4),
            // Each delta run below the two newest is at most four times the
            // total of the runs above it (16, 24, 48, and 144), and the base
            // is more than four times 444.
            (vec![50_000, 300, 96, 24, 8, 8, 8], 1..7),
            // 600 is more than four times 144, so the window stops above it.
            (vec![50_000, 600, 96, 24, 8, 8, 8], 2..7),
        ] {
            assert_eq!(capped(&sizes), Some(expected), "{sizes:?}");
        }
        // The window keeps its newest runs when more are within the ratio
        // than one unit merges.
        let fan_in = MAX_COMPACTION_INPUT_RUNS as u64;
        let mut sizes = vec![50_000];
        sizes.extend([1; 10]);
        assert_eq!(capped(&sizes), Some(11 - fan_in..11));
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
