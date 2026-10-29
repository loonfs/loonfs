//! Publishes the run that one metadata compaction unit merged in place of
//! the runs it replaces, fenced by the compactor epoch.

use super::group_merge::MetadataMergeReport;
use super::plan::{group_run_descriptors, MergePlacement, MetadataCompactionSpec};
use super::step::{CompactionUnitOutcome, MetadataCompactionCancellation};
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::limits::METADATA_COMPACTION_BUDGET_MS;
use crate::manifest::fold::{next_run_no_after, no_manifest_after};
use crate::manifest::publish::ManifestChange;
use crate::manifest::published::PublishedManifest;
use crate::namespace::control::LoadedManifest;
use crate::namespace::store::NamespaceStore;
use crate::namespace::PublicationBasis;
use crate::segment::runs::{runs_newest_first, MetadataRunManifest};
use crate::time::OperationClock;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{
    MetadataRunRef, MetadataSegmentRef, NamespaceManifestPayload,
};
use loonfs_types::{ChangeSeq, CompactorEpoch, NamespaceId};
use std::collections::BTreeSet;

/// Publication attempts one finalization makes before giving up.
///
/// Only an unrelated publication between the load and the manifest
/// put-if-absent costs an attempt, and the next attempt loads the manifest
/// that won. A namespace publishing fast enough to win four in a row is one
/// where running the unit again later is better than spinning
/// here, and running it again is always safe.
const MAX_FINALIZATION_ATTEMPTS: usize = 4;

pub(crate) struct CompactionPublication {
    pub(crate) compactor_epoch: CompactorEpoch,
    pub(crate) compaction: OperationClock,
    pub(crate) publication: OperationClock,
}

impl CompactionPublication {
    fn expired(&self) -> bool {
        self.compaction.elapsed_ms() > METADATA_COMPACTION_BUDGET_MS
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Swaps the rebuilt run in for the input it replaces.
    ///
    /// The publication starts from `basis`, the manifest that the unit planned
    /// from, or loads the current manifest when `basis` is `None`. Each attempt
    /// checks the current manifest that `update_manifest` found.
    /// That manifest must still contain the segments the unit merged, and the
    /// replacement keeps every run published meanwhile. A lost manifest put is
    /// retried over the manifest that won, while changes to the unit's input
    /// abandon the output.
    ///
    /// The publication budget starts during finalization rather than at the start
    /// of the potentially long rebuild.
    pub(crate) async fn finalize_metadata_compaction(
        &self,
        basis: Option<PublicationBasis>,
        spec: &MetadataCompactionSpec,
        input_keys: &BTreeSet<String>,
        result: MetadataMergeReport,
        cancellation: &MetadataCompactionCancellation,
        publication: &CompactionPublication,
    ) -> Result<CompactionUnitOutcome> {
        let output_segments = &result.output_segments;
        // `None` stands for the publication, and `Some` for every other outcome.
        let finalized = self
            .update_manifest(
                basis,
                &publication.publication,
                MAX_FINALIZATION_ATTEMPTS,
                |current: LoadedManifest| async move {
                    let settled = |outcome| Ok(ManifestChange::Finished(Some(outcome)));
                    if publication.expired() {
                        return settled(CompactionUnitOutcome::Abandoned);
                    }
                    if cancellation.is_cancelled() {
                        return settled(CompactionUnitOutcome::Cancelled);
                    }
                    let previous = current.payload();
                    if previous.status.is_deleted() {
                        return settled(CompactionUnitOutcome::Abandoned);
                    }
                    if previous.compactor_epoch != publication.compactor_epoch {
                        return settled(CompactionUnitOutcome::Fenced);
                    }
                    if input_segment_keys(&runs_newest_first(previous), spec).as_ref()
                        != Some(input_keys)
                    {
                        tracing::info!(
                            namespace_id = self.namespace_id.as_str(),
                            families = ?spec.families(),
                            "metadata compaction unit abandoned: its input runs moved while it ran"
                        );
                        return settled(CompactionUnitOutcome::Abandoned);
                    }
                    let surviving = previous
                        .runs
                        .iter()
                        .filter_map(|run| {
                            let mut run = run.clone();
                            run.segments.retain(|descriptor| {
                                !input_keys.contains(&metadata_segment_object_key(descriptor))
                            });
                            (!run.segments.is_empty()).then_some(run)
                        })
                        .collect();
                    let manifest = build_replacement_manifest(
                        &self.namespace_id,
                        previous,
                        surviving,
                        ReplacementOutput {
                            segments: output_segments.clone(),
                            placement: spec.placement,
                        },
                        spec.frozen_floor_seq(),
                    )?;
                    // The last check before the swap that makes this output reader
                    // truth. Everything above it is reads and objects nothing references.
                    if cancellation.is_cancelled() {
                        return settled(CompactionUnitOutcome::Cancelled);
                    }
                    if publication.expired() {
                        return settled(CompactionUnitOutcome::Abandoned);
                    }
                    Ok(ManifestChange::Next(Box::new(manifest), None))
                },
            )
            .await;
        match finalized {
            Ok((Some(outcome), _, _)) => Ok(outcome),
            Ok((None, published, _)) => Ok(CompactionUnitOutcome::Published {
                group: spec.group(),
                manifest: PublishedManifest::new(
                    &published.envelope,
                    Some(publication.publication.origin()),
                ),
                input_runs: spec.input_runs(),
                rows_read: result.rows_read,
                rows_written: result.rows_written,
                input_bytes: result.input_bytes,
                output_bytes: result.output_bytes,
                output_segments: result.output_segments.len(),
            }),
            // Every attempt lost its manifest number to another publication.
            Err(CoreError::StoreContention { .. }) => {
                tracing::info!(
                    namespace_id = self.namespace_id.as_str(),
                    families = ?spec.families(),
                    attempts = MAX_FINALIZATION_ATTEMPTS,
                    "metadata compaction unit lost every publication attempt; a later unit plans it again"
                );
                Ok(CompactionUnitOutcome::Abandoned)
            }
            // The namespace's objects were removed while the unit ran.
            Err(CoreError::ControlObjectLoad(ControlObjectLoadError::MissingObject { .. })) => {
                Ok(CompactionUnitOutcome::Abandoned)
            }
            Err(error) => Err(error),
        }
    }
}

/// The object keys of the spec's runs for its group, or `None` when the
/// manifest no longer references one of those runs at all.
///
/// Segments are immutable and their keys are generated, so two manifests
/// agreeing on this set agree on every row the unit read. Runs the manifest
/// gained meanwhile are not in it (the unit never read them), and they
/// survive the publication untouched.
pub(crate) fn input_segment_keys(
    runs: &[MetadataRunManifest],
    spec: &MetadataCompactionSpec,
) -> Option<BTreeSet<String>> {
    let mut keys = BTreeSet::new();
    for run_no in spec.inputs() {
        let run = runs.iter().find(|run| run.run_no == *run_no)?;
        keys.extend(group_run_descriptors(run, spec.group()).map(metadata_segment_object_key));
    }
    Some(keys)
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
    Ok(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        next_run_no,
        retention_floor_seq: previous.retention_floor_seq.max(floor_seq),
        ..previous
            .successor(runs)
            .map_err(|_| no_manifest_after(previous.manifest_no))?
    })
}
