//! The LSM run model: ordered manifest runs, row families, and layout policy.

use crate::heap_bytes::{arc_bytes, manifest_heap_bytes, HeapBytes};
use loonfs_types::format::manifest::{
    MetadataRowFamily, MetadataRunRef, MetadataSegmentRef, NamespaceManifestEnvelope,
    NamespaceManifestPayload, RunTier,
};
use loonfs_types::{ChangeSeq, RunNo};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

pub(crate) use loonfs_types::format::segment_blocks::{
    DEFAULT_MAX_COMPACTION_INPUT_BYTES, DEFAULT_MAX_COMPACTION_INPUT_ROWS,
    DEFAULT_MAX_COMPACTION_INPUT_RUNS, DEFAULT_MAX_DELTA_RUNS, DEFAULT_MAX_ROWS_PER_SEGMENT,
    DEFAULT_MIN_COMPACTION_RUNS,
};
pub use loonfs_types::MetadataFamilyGroup;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataFamilySegments {
    pub(crate) family: MetadataRowFamily,
    pub(crate) segments: Vec<MetadataSegmentRef>,
}

/// One run of a cached manifest, in the order that reads scan the runs.
///
/// The manifest cache keeps these with each decoded manifest, so the type
/// is public, but its fields aren't.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRunManifest {
    pub(crate) run_no: RunNo,
    pub(crate) run_seq: ChangeSeq,
    pub(crate) tier: RunTier,
    pub(crate) segments: Vec<MetadataFamilySegments>,
}

/// How folds and compactions shape runs, and the memory one of them may
/// keep while it works.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRunPolicy {
    /// Delta runs at which a family group merges its newest runs, even when
    /// no size tier is ready.
    pub max_delta_runs: NonZeroUsize,
    /// Runs of similar size that a family group needs before they merge.
    /// A minimum of one acts as two.
    pub min_compaction_runs: NonZeroUsize,
    pub max_rows_per_segment: NonZeroUsize,
    /// Decoded data-byte target; the last row may cross it.
    pub target_segment_bytes: NonZeroUsize,
    /// Complete logical runs one compaction unit may merge, up to eight.
    pub max_input_runs_per_step: NonZeroUsize,
    /// Rows, by its runs' descriptor counts, that a window may list when the
    /// call admits only windows within this budget (a `metadata` pass). A
    /// larger window waits for a unit that admits any size.
    pub max_decoded_input_rows_per_step: NonZeroUsize,
    /// Decoded data-block bytes one merge keeps in memory across its open
    /// inputs, though each input always keeps one whole block.
    pub max_decoded_input_bytes_per_step: NonZeroUsize,
}

impl Default for MetadataRunPolicy {
    fn default() -> Self {
        Self {
            max_delta_runs: DEFAULT_MAX_DELTA_RUNS,
            min_compaction_runs: DEFAULT_MIN_COMPACTION_RUNS,
            max_rows_per_segment: DEFAULT_MAX_ROWS_PER_SEGMENT,
            target_segment_bytes: const {
                NonZeroUsize::new(
                    loonfs_types::format::segment_blocks::DEFAULT_TARGET_SEGMENT_BYTES,
                )
                .unwrap()
            },
            max_input_runs_per_step: DEFAULT_MAX_COMPACTION_INPUT_RUNS,
            max_decoded_input_rows_per_step: DEFAULT_MAX_COMPACTION_INPUT_ROWS,
            max_decoded_input_bytes_per_step: DEFAULT_MAX_COMPACTION_INPUT_BYTES,
        }
    }
}

/// Orders runs for compaction planning: delta runs before base runs,
/// later sequences before earlier ones, and later run numbers first.
///
/// Two runs may have the same sequence and tier, because a compaction
/// writes its output beside the runs it did not consume. They contain different
/// families in that case, and the later-allocated run number keeps the order
/// total.
impl HeapBytes for MetadataFamilySegments {
    fn heap_bytes(&self) -> usize {
        self.segments.heap_bytes()
    }
}

impl HeapBytes for MetadataRunManifest {
    fn heap_bytes(&self) -> usize {
        self.segments.heap_bytes()
    }
}

/// A cached manifest: the decoded envelope and its scan-ordered runs, each
/// behind its own `Arc`. The runs copy every segment descriptor.
pub(crate) fn manifest_cache_heap_bytes(
    envelope: &NamespaceManifestEnvelope,
    scan_runs: &Vec<MetadataRunManifest>,
) -> usize {
    manifest_heap_bytes(envelope) + arc_bytes::<Vec<MetadataRunManifest>>() + scan_runs.heap_bytes()
}

pub(crate) fn runs_newest_first(payload: &NamespaceManifestPayload) -> Vec<MetadataRunManifest> {
    let mut runs = payload
        .runs
        .iter()
        .map(metadata_run_manifest)
        .collect::<Vec<_>>();
    runs.sort_by(|left, right| {
        left.tier
            .cmp(&right.tier)
            .then(right.run_seq.cmp(&left.run_seq))
            .then(right.run_no.cmp(&left.run_no))
    });
    runs
}

/// Oldest run first, which is the order a materialization applies rows in.
/// It is the reverse of [`runs_newest_first`], down to the same tiebreak.
pub(crate) fn runs_in_materialization_order(
    payload: &NamespaceManifestPayload,
) -> Vec<MetadataRunManifest> {
    let mut runs = payload
        .runs
        .iter()
        .map(metadata_run_manifest)
        .collect::<Vec<_>>();
    runs.sort_by(|left, right| {
        left.run_seq
            .cmp(&right.run_seq)
            .then(right.tier.cmp(&left.tier))
            .then(left.run_no.cmp(&right.run_no))
    });
    runs
}

/// Orders candidate runs for merging: base before delta, then oldest first.
pub(crate) fn runs_in_merge_order(
    mut runs: Vec<&MetadataRunManifest>,
) -> Vec<&MetadataRunManifest> {
    runs.sort_by(|left, right| {
        right
            .tier
            .cmp(&left.tier)
            .then(left.run_seq.cmp(&right.run_seq))
            .then(left.run_no.cmp(&right.run_no))
    });
    runs
}

fn metadata_run_manifest(run: &MetadataRunRef) -> MetadataRunManifest {
    MetadataRunManifest {
        run_no: run.run_no,
        run_seq: run.run_seq,
        tier: run.tier,
        segments: MetadataRowFamily::ALL
            .into_iter()
            .map(|family| {
                let segments = run
                    .segments
                    .iter()
                    .filter(|descriptor| descriptor.family == family)
                    .cloned()
                    .collect::<Vec<_>>();
                MetadataFamilySegments { family, segments }
            })
            .collect(),
    }
}

pub(crate) fn flatten_manifest_segments(
    segments_by_family: Vec<MetadataFamilySegments>,
) -> Vec<MetadataSegmentRef> {
    segments_by_family
        .into_iter()
        .flat_map(|family_segments| family_segments.segments)
        .collect()
}
