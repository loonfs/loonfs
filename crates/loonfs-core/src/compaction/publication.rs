//! Publishes the run that one metadata compaction unit wrote in place of
//! the runs it replaces.

use super::plan::{group_run_descriptors, CompactionPlacement, MetadataCompactionSpec};
use super::step::{
    log_unit_outcome, CompactionUnitOutcome, CompactionWrites, MaintenanceCancellation,
};
use crate::commit::publication_lane::LaneTurn;
use crate::error::Result;
use crate::limits::METADATA_COMPACTION_BUDGET_MS;
use crate::manifest::fold::{next_run_no_after, no_manifest_after};
use crate::namespace::store::NamespaceStore;
use crate::segment::runs::{runs_newest_first, MetadataRunManifest};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{
    MetadataRunRef, MetadataSegmentRef, NamespaceManifestPayload,
};
use loonfs_types::NamespaceId;
use std::collections::BTreeSet;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Publishes the run that `writes`, a compaction unit's output, lists in
    /// place of the unit's input runs, as one item of a writer session's
    /// publication lane, whose turn is `turn` (see
    /// [`Self::publish_manifest_in_turn`]).
    ///
    /// The turn's basis, the session's newest published manifest, must still
    /// list the input runs with the segments that the unit compacted, and
    /// the replacement keeps every run published meanwhile. A basis that
    /// doesn't, and a unit past its time bound
    /// (`METADATA_COMPACTION_BUDGET_MS` from the start of the unit) publish
    /// nothing and return `Abandoned`, and a cancelled unit returns
    /// `Cancelled`. Nothing references the output of a unit that publishes
    /// nothing, so a collection deletes it by age.
    pub async fn publish_compaction_in_turn(
        &self,
        turn: LaneTurn<'_>,
        writes: CompactionWrites,
        cancellation: &MaintenanceCancellation,
    ) -> Result<CompactionUnitOutcome> {
        let publication = self.start_clock();
        let CompactionWrites {
            spec,
            input_keys,
            report,
            compaction,
        } = writes;
        let previous = turn.basis().manifest().payload();
        let outcome = if compaction.elapsed_ms() > METADATA_COMPACTION_BUDGET_MS {
            CompactionUnitOutcome::Abandoned
        } else if cancellation.is_cancelled() {
            CompactionUnitOutcome::Cancelled
        } else if input_segment_keys(&runs_newest_first(previous), &spec).as_ref()
            != Some(&input_keys)
        {
            tracing::info!(
                namespace_id = self.namespace_id.as_str(),
                families = ?spec.families(),
                "metadata compaction unit abandoned: its input runs moved while it ran"
            );
            CompactionUnitOutcome::Abandoned
        } else {
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
                    segments: report.output_segments.clone(),
                    placement: spec.placement(),
                },
            )?;
            let manifest = self
                .publish_manifest_in_turn(turn, manifest, &publication)
                .await?;
            CompactionUnitOutcome::Published {
                group: spec.group(),
                manifest,
                input_runs: spec.input_runs(),
                rows_read: report.rows_read,
                rows_written: report.rows_written,
                input_bytes: report.input_bytes,
                output_bytes: report.output_bytes,
                output_segments: report.output_segments.len(),
                skipped: report.skipped,
            }
        };
        log_unit_outcome(&self.namespace_id, &spec, &outcome);
        Ok(outcome)
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
    pub(super) placement: CompactionPlacement,
}

/// The surviving and new runs determine the base sequence and next run number.
///
/// The manifest copies its predecessor's retention floor, because only a fold
/// or a fork's active manifest changes the floor (format spec, "Advancing the
/// retention floor").
pub(super) fn build_replacement_manifest(
    namespace_id: &NamespaceId,
    previous: &NamespaceManifestPayload,
    surviving: Vec<MetadataRunRef>,
    output: ReplacementOutput,
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
        ..previous
            .successor(runs)
            .map_err(|_| no_manifest_after(previous.manifest_no))?
    })
}
