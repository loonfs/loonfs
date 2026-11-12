//! Grep's entry in a namespace manifest: decoding it from the core entry,
//! its validation, and the rule between two of grep's entries in one
//! namespace's manifests.

use super::error::GrepEntryError;
use super::status::{GrepIndexStatus, GrepReorganizeState};
use loonfs_types::format::manifest::{ExtensionEntry, ExtensionRunRef, ExtensionSegmentRef};
use loonfs_types::{ExtensionName, IndexSegmentId, NamespaceId, RunNo};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// The version of grep's format that this build reads and writes, which
/// governs grep's entry and the rows of its segments.
pub const GREP_ENTRY_VERSION: u32 = 1;

/// Returns the name of grep's entry in a namespace manifest, which is also
/// part of the key of each grep segment.
pub fn grep_extension_name() -> ExtensionName {
    ExtensionName::parse("grep").expect("`grep` should be a valid extension name")
}

/// The members of grep's entry that grep defines.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrepEntryState {
    status: GrepIndexStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reorganize: Option<GrepReorganizeState>,
}

/// Grep's entry in a namespace manifest: the index's lifecycle, its
/// in-progress reorganization, and its runs
/// ([Grep extension format](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#appendix-d-grep-extension-format)).
///
/// The fields stay private, so every entry that grep builds or decodes
/// passes the checks of [`Self::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrepEntry {
    status: GrepIndexStatus,
    reorganize: Option<GrepReorganizeState>,
    runs: Vec<ExtensionRunRef>,
}

impl GrepEntry {
    /// Builds an entry after checking that a disabled index lists no runs
    /// and no reorganization, that no two runs have the same number, and
    /// that a reorganization's inputs and outputs are distinct segments that
    /// `runs` lists, with each output in the reorganization's run.
    pub fn new(
        status: GrepIndexStatus,
        reorganize: Option<GrepReorganizeState>,
        runs: Vec<ExtensionRunRef>,
    ) -> Result<Self, GrepEntryError> {
        let entry = Self {
            status,
            reorganize,
            runs,
        };
        entry.validate()?;
        Ok(entry)
    }

    /// Returns the entry of a disabled index, which lists no segments.
    pub(crate) fn disabled() -> Self {
        Self {
            status: GrepIndexStatus::Disabled {},
            reorganize: None,
            runs: Vec::new(),
        }
    }

    /// Returns the index's lifecycle state.
    pub fn status(&self) -> &GrepIndexStatus {
        &self.status
    }

    /// Returns the in-progress reorganization, if any.
    pub fn reorganize(&self) -> Option<&GrepReorganizeState> {
        self.reorganize.as_ref()
    }

    /// Returns the index's runs, in list order.
    pub fn runs(&self) -> &[ExtensionRunRef] {
        &self.runs
    }

    /// Returns the segments of the index's runs, in list order.
    pub fn segments(&self) -> impl Iterator<Item = &ExtensionSegmentRef> {
        self.runs.iter().flat_map(|run| &run.segments)
    }

    /// Decodes grep's entry from its core entry, and rejects any other
    /// version, members that grep doesn't define, and an entry that breaks
    /// the rules of [`Self::new`].
    pub fn decode(entry: &ExtensionEntry) -> Result<Self, GrepEntryError> {
        if entry.version != GREP_ENTRY_VERSION {
            return Err(GrepEntryError::UnsupportedVersion {
                version: entry.version,
            });
        }
        let state: GrepEntryState = serde_json::from_value(serde_json::Value::Object(
            entry.state().clone(),
        ))
        .map_err(|error| GrepEntryError::UndecodableState {
            message: error.to_string(),
        })?;
        Self::new(state.status, state.reorganize, entry.runs.clone())
    }

    /// Encodes the entry as the core entry that a manifest lists.
    pub fn encode(&self) -> ExtensionEntry {
        let state = GrepEntryState {
            status: self.status.clone(),
            reorganize: self.reorganize.clone(),
        };
        let state = match serde_json::to_value(state) {
            Ok(serde_json::Value::Object(state)) => Some(state),
            _ => None,
        }
        .expect("grep's entry members should encode as a JSON object");
        ExtensionEntry::new(GREP_ENTRY_VERSION, self.runs.clone(), state)
    }

    /// Returns this entry without its reorganization when another namespace
    /// owns an output of the reorganization.
    ///
    /// Such a reorganization is the fork source's, and the fork inherited it
    /// with the source's entry. The entry keeps every segment, including
    /// those outputs, and `namespace_id`'s own reorganizations start from
    /// them. A reorganization takes the union of its inputs, so reorganizing
    /// the inputs of the source's outputs again is harmless.
    pub(crate) fn without_inherited_reorganize(self, namespace_id: &NamespaceId) -> Self {
        let inherited = self.reorganize.as_ref().is_some_and(|reorganize| {
            self.segments().any(|segment| {
                reorganize.output_segment_ids.contains(&segment.segment_id)
                    && segment.owner_namespace_id != *namespace_id
            })
        });
        if !inherited {
            return self;
        }
        Self {
            reorganize: None,
            ..self
        }
    }

    /// Returns the number of the next run that a step writes: one more than
    /// the largest number among the runs and the in-progress reorganization,
    /// whose output run can have no segment yet, or 0 when there are none.
    pub(crate) fn next_run_no(&self) -> crate::Result<RunNo> {
        let largest = self
            .runs
            .iter()
            .map(|run| run.run_no)
            .chain(self.reorganize.as_ref().map(|reorganize| reorganize.run_no))
            .max();
        let Some(largest) = largest else {
            return Ok(RunNo(0));
        };
        largest
            .successor()
            .map_err(|_| crate::GrepError::IndexCorrupt {
                message: format!(
                    "grep run {largest} has the largest run number, so no run can follow it"
                ),
            })
    }

    /// Returns whether `later`, the entry that a step makes from this one,
    /// keeps this entry's build and doesn't move its indexing position back,
    /// or starts a later build from a new snapshot.
    ///
    /// A backfill moves its cursor back only when it restarts from a new
    /// snapshot, and an active index keeps the `captured_seq` of the backfill
    /// that built it. Disabling and enabling the index starts over.
    pub(crate) fn is_followed_by(&self, later: &Self) -> bool {
        match (&self.status, &later.status) {
            (
                GrepIndexStatus::Active {
                    captured_seq: before_build,
                    built_through_seq: before,
                    next_event_index: before_event,
                },
                GrepIndexStatus::Active {
                    captured_seq: after_build,
                    built_through_seq: after,
                    next_event_index: after_event,
                },
            ) => {
                after_build == before_build
                    && after >= before
                    && !(after == before && *before_event == 0 && *after_event != 0)
                    && !(after == before && *after_event != 0 && after_event < before_event)
            }
            (
                GrepIndexStatus::Backfilling {
                    captured_seq,
                    snapshot_id,
                    cursor_inode_id,
                },
                GrepIndexStatus::Backfilling {
                    captured_seq: next_target,
                    snapshot_id: next_snapshot_id,
                    cursor_inode_id: next_cursor,
                },
            ) => {
                next_target >= captured_seq
                    && (next_snapshot_id != snapshot_id
                        || (next_target == captured_seq && next_cursor >= cursor_inode_id))
            }
            (
                GrepIndexStatus::Backfilling { captured_seq, .. },
                GrepIndexStatus::Active {
                    captured_seq: build,
                    built_through_seq,
                    ..
                },
            ) => build == captured_seq && built_through_seq >= captured_seq,
            (
                GrepIndexStatus::Active {
                    built_through_seq, ..
                },
                GrepIndexStatus::Backfilling { captured_seq, .. },
            ) => captured_seq >= built_through_seq,
            _ => true,
        }
    }

    fn validate(&self) -> Result<(), GrepEntryError> {
        if matches!(self.status, GrepIndexStatus::Disabled {}) {
            if !self.runs.is_empty() {
                return Err(GrepEntryError::DisabledHasRuns);
            }
            if self.reorganize.is_some() {
                return Err(GrepEntryError::DisabledHasReorganize);
            }
        }
        let mut run_nos = BTreeSet::new();
        if let Some(run) = self.runs.iter().find(|run| !run_nos.insert(run.run_no)) {
            return Err(GrepEntryError::DuplicateRunNo { run_no: run.run_no });
        }
        if let Some(reorganize) = &self.reorganize {
            let runs_by_segment: BTreeMap<&IndexSegmentId, &ExtensionRunRef> = self
                .runs
                .iter()
                .flat_map(|run| {
                    run.segments
                        .iter()
                        .map(move |segment| (&segment.segment_id, run))
                })
                .collect();
            validate_reorganize(reorganize, &runs_by_segment)?;
        }
        Ok(())
    }
}

/// Checks a reorganization's segments against `segments`, the run of each
/// segment that the entry lists.
fn validate_reorganize(
    reorganize: &GrepReorganizeState,
    segments: &BTreeMap<&IndexSegmentId, &ExtensionRunRef>,
) -> Result<(), GrepEntryError> {
    let mut input_ids = BTreeSet::new();
    for segment_id in &reorganize.input_segment_ids {
        if !input_ids.insert(segment_id) {
            return Err(GrepEntryError::DuplicateReorganizeSegmentId {
                segment_id: segment_id.clone(),
            });
        }
        if !segments.contains_key(segment_id) {
            return Err(GrepEntryError::MissingReorganizeInputSegment {
                segment_id: segment_id.clone(),
            });
        }
    }
    let mut output_ids = BTreeSet::new();
    for segment_id in &reorganize.output_segment_ids {
        if input_ids.contains(segment_id) || !output_ids.insert(segment_id) {
            return Err(GrepEntryError::DuplicateReorganizeSegmentId {
                segment_id: segment_id.clone(),
            });
        }
        let Some(run) = segments.get(segment_id) else {
            return Err(GrepEntryError::MissingReorganizeOutputSegment {
                segment_id: segment_id.clone(),
            });
        };
        if run.level != reorganize.output_level || run.run_no != reorganize.run_no {
            return Err(GrepEntryError::ReorganizeOutputDescriptorMismatch {
                segment_id: segment_id.clone(),
            });
        }
    }
    Ok(())
}
