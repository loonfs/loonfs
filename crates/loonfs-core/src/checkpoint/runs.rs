//! The LSM run model: ordered manifest runs, row families, and layout policy.

use loonfs_api::wire::manifest::{
    MetadataRowFamily, MetadataRunRef, MetadataSegmentRef, NamespaceManifestPayload, RunTier,
};
use loonfs_api::{ChangeSeq, RunNo};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

pub(super) use loonfs_api::wire::sst_blocks::{
    DEFAULT_MAX_DELTA_RUNS as DEFAULT_MAX_CHECKPOINT_DELTA_RUNS,
    DEFAULT_MAX_REORGANIZATION_INPUT_BYTES, DEFAULT_MAX_REORGANIZATION_INPUT_ROWS,
    DEFAULT_MAX_REORGANIZATION_INPUT_RUNS,
    DEFAULT_MAX_ROWS_PER_SEGMENT as DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT,
};
pub use loonfs_api::MetadataFamilyGroup;

pub(super) const MAX_MAINTENANCE_SEGMENT_IO: usize = 8;

pub(super) const CHECKPOINT_ROW_FAMILIES: [MetadataRowFamily; 10] = [
    MetadataRowFamily::Inodes,
    MetadataRowFamily::DirentryBinds,
    MetadataRowFamily::DirentryChildBinds,
    MetadataRowFamily::DirentryUnbinds,
    MetadataRowFamily::Revisions,
    MetadataRowFamily::RevisionsByInodeDesc,
    MetadataRowFamily::Tombstones,
    MetadataRowFamily::ActiveDeletions,
    MetadataRowFamily::CommitReceipts,
    MetadataRowFamily::Attributes,
];

pub(super) const REORGANIZE_FAMILY_GROUPS: [MetadataFamilyGroup; 7] = MetadataFamilyGroup::ALL;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataFamilySegments {
    pub(super) family: MetadataRowFamily,
    pub(super) segments: Vec<MetadataSegmentRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MetadataRunManifest {
    pub(super) run_no: RunNo,
    pub(super) run_seq: ChangeSeq,
    pub(super) tier: RunTier,
    pub(super) segments: Vec<MetadataFamilySegments>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MetadataLsmPolicy {
    pub max_delta_runs: NonZeroUsize,
    pub max_rows_per_segment: NonZeroUsize,
    /// Complete logical runs one reorganization step may inspect and merge.
    pub max_input_runs_per_step: NonZeroUsize,
    /// Row payloads one reorganization step may decode across its selected runs.
    pub max_decoded_input_rows_per_step: NonZeroUsize,
    /// Decoded SST data-block bytes one reorganization step may materialize.
    pub max_decoded_input_bytes_per_step: NonZeroUsize,
}

impl Default for MetadataLsmPolicy {
    fn default() -> Self {
        Self {
            max_delta_runs: const { NonZeroUsize::new(DEFAULT_MAX_CHECKPOINT_DELTA_RUNS).unwrap() },
            max_rows_per_segment: const {
                NonZeroUsize::new(DEFAULT_MAX_CHECKPOINT_ROWS_PER_SEGMENT).unwrap()
            },
            max_input_runs_per_step: const {
                NonZeroUsize::new(DEFAULT_MAX_REORGANIZATION_INPUT_RUNS).unwrap()
            },
            max_decoded_input_rows_per_step: const {
                NonZeroUsize::new(DEFAULT_MAX_REORGANIZATION_INPUT_ROWS).unwrap()
            },
            max_decoded_input_bytes_per_step: const {
                NonZeroUsize::new(DEFAULT_MAX_REORGANIZATION_INPUT_BYTES).unwrap()
            },
        }
    }
}

pub(super) fn delta_run_count(payload: &NamespaceManifestPayload) -> usize {
    payload
        .runs
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .count()
}

/// Orders runs for reorganization planning: delta runs before base runs,
/// later sequences before earlier ones, and later run numbers first.
///
/// Two runs may carry the same sequence and tier, because a reorganization
/// writes its output beside the runs it did not consume. They hold different
/// families in that case, and the later-allocated run number keeps the order
/// total.
pub(super) fn runs_in_reorganization_order(
    payload: &NamespaceManifestPayload,
) -> Vec<MetadataRunManifest> {
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
/// It is the reverse of the reorganization order, down to the same tiebreak.
pub(super) fn runs_in_materialization_order(
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

/// Orders candidate runs for folding: base before delta, then oldest first.
pub(super) fn runs_in_fold_order(mut runs: Vec<&MetadataRunManifest>) -> Vec<&MetadataRunManifest> {
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
        segments: CHECKPOINT_ROW_FAMILIES
            .into_iter()
            .map(|family| {
                let mut segments = run
                    .segments
                    .iter()
                    .filter(|descriptor| descriptor.family == family)
                    .cloned()
                    .collect::<Vec<_>>();
                segments.sort_by_key(|descriptor| descriptor.segment_index);
                MetadataFamilySegments { family, segments }
            })
            .collect(),
    }
}

pub(super) fn flatten_manifest_segments(
    segments_by_family: Vec<MetadataFamilySegments>,
) -> Vec<MetadataSegmentRef> {
    segments_by_family
        .into_iter()
        .flat_map(|family_segments| family_segments.segments)
        .collect()
}
