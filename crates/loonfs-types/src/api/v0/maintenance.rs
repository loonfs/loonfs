//! Maintenance job requests and responses for the v0 HTTP API.

use super::{GcRequest, GcResponse};
use crate::{AccessRevisionNo, ChangeSeq, CommitId, ManifestNo, NamespaceId, PrincipalId};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// A request to advance the retention floor to the folded manifest head, or to the one target that
/// the request sets.
///
/// Whatever the target, the floor doesn't pass a commit that's less than one hour old (the
/// default garbage collection grace window).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct AdvanceRetentionRequest {
    /// The sequence to advance the floor to. If the folded manifest head is lower, the floor
    /// advances to the head instead. This field can't be combined with `cutoff_at_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub to_seq: Option<ChangeSeq>,
    /// A Unix time in milliseconds. The floor advances to the last commit made at or before this
    /// time. LoonFS reads commits upward from the floor, and the first commit made after this time
    /// ends the advance. This field can't be combined with `to_seq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub cutoff_at_ms: Option<u64>,
}

/// The result of advancing the retention floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AdvanceRetentionResponse {
    /// The namespace whose retention floor was advanced.
    pub namespace_id: NamespaceId,
    /// The retention floor that the advance reached, which is the minimum sequence for incremental
    /// replay. It's below the target when LoonFS reads a commit that's less than one hour old
    /// before it reaches the target.
    pub retention_floor_seq: ChangeSeq,
}

/// A request to set the namespace's retention window, or to clear it when the request doesn't
/// include `retention_window_ms`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct SetRetentionWindowRequest {
    /// The namespace's new retention window, in milliseconds, which must be positive. Without it,
    /// the job clears the namespace's window, and the reference server's `retention_window_ms`
    /// setting applies to the namespace again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "openapi",
        schema(value_type = Option<u64>, nullable = false, minimum = 1)
    )]
    pub retention_window_ms: Option<NonZeroU64>,
}

/// The namespace's retention window after a `retention_window` job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SetRetentionWindowResponse {
    /// The namespace whose retention window the job set or cleared.
    pub namespace_id: NamespaceId,
    /// The namespace's retention window, in milliseconds. It's absent when the namespace has no
    /// window of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub retention_window_ms: Option<u64>,
}

/// One maintenance job for one namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunMaintenanceRequest {
    /// A job that folds the WAL tail and compacts metadata until nothing is due, or until the job
    /// reaches its work limit.
    Metadata(MetadataMaintenanceRequest),
    /// A garbage collection job for aged, unreferenced objects, which also collects the grep
    /// index's objects when the deployment maintains a grep index.
    Gc(GcRequest),
    /// A job that advances the retention floor toward the folded manifest head, or toward the
    /// target that the request sets.
    ///
    /// The floor doesn't pass a commit that's less than one hour old.
    Retention(AdvanceRetentionRequest),
    /// A job that sets or clears the namespace's retention window.
    RetentionWindow(SetRetentionWindowRequest),
    /// A job that restores a root administrator.
    RecoverAdministrator(RecoverAdministratorRequest),
}

/// A request to grant `admin` on the root access row to one principal, through a commit that
/// doesn't check any subject's rights.
///
/// Every other root grant stays in place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct RecoverAdministratorRequest {
    /// The principal that receives administrator rights.
    pub principal_id: PrincipalId,
}

/// The committed administrator recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RecoverAdministratorResponse {
    /// The namespace whose root grants changed.
    pub namespace_id: NamespaceId,
    /// The ID of the recovery commit.
    pub commit_id: CommitId,
    /// The sequence of the recovery commit.
    pub committed_seq: ChangeSeq,
    /// The root's access revision after the recovery.
    pub access_revision_no: AccessRevisionNo,
}

/// Overrides for the `metadata` maintenance job.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct MetadataMaintenanceRequest {
    /// The WAL tail length, in WAL objects, that triggers a fold. Omit it to use the server
    /// default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(
        feature = "openapi",
        schema(value_type = Option<u64>, nullable = false, minimum = 1)
    )]
    pub max_wal_tail_objects: Option<NonZeroU64>,
}

/// What the WAL fold step of a maintenance pass did.
///
/// The embedded runtime reports it. It isn't part of the HTTP API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum WalFoldStepOutcome {
    /// The tail was below the threshold, so there was nothing to fold.
    NotNeeded,
    /// The step folded the WAL tail and published the next current manifest.
    Folded {
        /// The last sequence that the published manifest includes.
        manifest_head_seq: ChangeSeq,
    },
    /// The current manifest already included the captured WAL tail, so this step didn't publish a
    /// manifest.
    AlreadyPublished {
        /// The sequence that this step tried to fold through.
        attempted_seq: ChangeSeq,
        /// The number of the namespace's current manifest.
        current_manifest_no: ManifestNo,
    },
    /// Concurrent updates prevented every publication attempt.
    RetriesExhausted {
        /// The head sequence observed before the step ran.
        observed_head_seq: ChangeSeq,
    },
    /// An outcome from a newer server. This variant doesn't include the outcome's own fields.
    #[serde(other)]
    Unknown,
}

/// The result of one maintenance job.
///
/// The `kind` matches the request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunMaintenanceResponse {
    /// Whether metadata maintenance is caught up, and the compaction units that the job published.
    Metadata(MetadataMaintenanceResponse),
    /// The counts and deadlines from one garbage collection call.
    Gc(GcResponse),
    /// The result of advancing the retention floor.
    Retention(AdvanceRetentionResponse),
    /// The namespace's retention window after the job.
    RetentionWindow(SetRetentionWindowResponse),
    /// The committed administrator recovery.
    RecoverAdministrator(RecoverAdministratorResponse),
    /// A job kind from a newer server. This variant doesn't include the job's own fields.
    #[serde(other)]
    Unknown,
}

/// The result of one `metadata` maintenance job.
///
/// The job folds the WAL tail and compacts metadata until nothing is due, or until it reaches a
/// work limit such as 16 compaction units. `Maintenance::maintain_metadata_while_due` in the
/// embedded runtime returns the same result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MetadataMaintenanceResponse {
    /// The namespace that this run maintained.
    pub namespace_id: NamespaceId,
    /// Whether work is still due when the run returns.
    pub outcome: MetadataMaintenanceOutcome,
    /// The compaction units that this run published. A unit merges one family group's window.
    pub compaction_units: u64,
}

/// Whether metadata maintenance left work due for a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum MetadataMaintenanceOutcome {
    /// Nothing is due now, and nothing can become due without a new commit.
    CaughtUp,
    /// Work is still due, or the run couldn't tell. Another run continues the work.
    Due,
    /// An outcome from a newer server.
    #[serde(other)]
    Unknown,
}

/// What the WAL fold and the compaction unit of one metadata maintenance pass did.
///
/// The embedded runtime reports it. It isn't part of the HTTP API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPassReport {
    /// The namespace that this pass maintained.
    pub namespace_id: NamespaceId,
    /// What the WAL fold did.
    pub wal_fold: WalFoldStepOutcome,
    /// What the compaction unit did.
    pub compaction: MetadataCompactionOutcome,
}

/// What one metadata compaction unit did.
///
/// The embedded runtime reports it. It isn't part of the HTTP API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataCompactionResponse {
    /// The namespace that this run compacted.
    pub namespace_id: NamespaceId,
    /// The outcome of the compaction.
    pub compaction: MetadataCompactionOutcome,
}

/// The outcome of one metadata compaction unit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum MetadataCompactionOutcome {
    /// No eligible family group was available, so nothing was published.
    NotNeeded,
    /// The unit merged the selected window and published the next manifest.
    Published {
        /// The manifest that the compaction published.
        manifest_no: ManifestNo,
        /// The rows that the compaction read.
        rows_read: u64,
        /// The rows that the compaction wrote.
        rows_written: u64,
        /// The input bytes that the compaction read.
        input_bytes: u64,
        /// The output bytes that the compaction wrote.
        output_bytes: u64,
        /// The output segments that the compaction wrote.
        output_segments: u64,
    },
    /// The selected window lists more rows than a metadata pass merges, so nothing was published.
    ///
    /// `Maintenance::compact_metadata` merges a window of any size, and
    /// `Maintenance::maintain_metadata_while_due` runs it whenever a pass reports this outcome.
    MetadataCompactionRequired,
    /// The run was cancelled, so the current manifest didn't change.
    Cancelled,
    /// The unit stopped because its inputs changed, time ran out, or every publication attempt
    /// lost.
    Abandoned,
    /// Another process claimed the namespace's compactor epoch, so nothing was published.
    Fenced,
    /// An outcome from a newer server. This variant doesn't include the outcome's own fields.
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that a `metadata` run reports its outcome and compaction units as flat fields next
    /// to the job kind.
    #[test]
    fn a_metadata_run_reports_its_outcome_and_compaction_units() {
        assert_eq!(
            serde_json::to_value(RunMaintenanceResponse::Metadata(
                MetadataMaintenanceResponse {
                    namespace_id: NamespaceId::parse("demo").expect("namespace id"),
                    outcome: MetadataMaintenanceOutcome::Due,
                    compaction_units: 16,
                },
            ))
            .expect("serialize metadata maintenance response"),
            serde_json::json!({
                "kind": "metadata",
                "namespace_id": "demo",
                "outcome": "due",
                "compaction_units": 16
            })
        );
        assert_eq!(
            serde_json::to_value(MetadataMaintenanceOutcome::CaughtUp)
                .expect("serialize metadata maintenance outcome"),
            serde_json::json!("caught_up")
        );
    }

    /// Checks that a client decodes a metadata maintenance outcome from a newer server as
    /// `Unknown`.
    #[test]
    fn a_metadata_outcome_from_a_newer_server_decodes_as_unknown() {
        let response: RunMaintenanceResponse = serde_json::from_value(serde_json::json!({
            "kind": "metadata",
            "namespace_id": "demo",
            "outcome": "throttled",
            "compaction_units": 2
        }))
        .expect("a newer outcome decodes");
        let RunMaintenanceResponse::Metadata(metadata) = response else {
            panic!("a metadata response decodes as metadata");
        };
        assert_eq!(metadata.outcome, MetadataMaintenanceOutcome::Unknown);
        assert_eq!(metadata.compaction_units, 2);
    }

    #[test]
    fn run_maintenance_requests_are_strict_and_round_trip() {
        let cases = [
            (
                serde_json::json!({"kind": "metadata"}),
                Some(RunMaintenanceRequest::Metadata(
                    MetadataMaintenanceRequest::default(),
                )),
            ),
            (
                serde_json::json!({"kind": "metadata", "max_wal_tail_objects": 4}),
                Some(RunMaintenanceRequest::Metadata(
                    MetadataMaintenanceRequest {
                        max_wal_tail_objects: NonZeroU64::new(4),
                    },
                )),
            ),
            (serde_json::json!({"kind": "metadata_compaction"}), None),
            (
                serde_json::json!({"kind": "gc"}),
                Some(RunMaintenanceRequest::Gc(GcRequest::default())),
            ),
            (
                serde_json::json!({
                    "kind": "gc",
                    "grace_window_ms": 600_000
                }),
                Some(RunMaintenanceRequest::Gc(GcRequest {
                    grace_window_ms: Some(600_000),
                })),
            ),
            (
                serde_json::json!({"kind": "retention"}),
                Some(RunMaintenanceRequest::Retention(
                    AdvanceRetentionRequest::default(),
                )),
            ),
            (
                serde_json::json!({"kind": "retention", "to_seq": 12}),
                Some(RunMaintenanceRequest::Retention(AdvanceRetentionRequest {
                    to_seq: Some(ChangeSeq(12)),
                    cutoff_at_ms: None,
                })),
            ),
            (
                serde_json::json!({"kind": "retention", "cutoff_at_ms": 1_752_624_000_000u64}),
                Some(RunMaintenanceRequest::Retention(AdvanceRetentionRequest {
                    to_seq: None,
                    cutoff_at_ms: Some(1_752_624_000_000),
                })),
            ),
            (
                serde_json::json!({"kind": "retention_window"}),
                Some(RunMaintenanceRequest::RetentionWindow(
                    SetRetentionWindowRequest::default(),
                )),
            ),
            (
                serde_json::json!({"kind": "retention_window", "retention_window_ms": 86_400_000}),
                Some(RunMaintenanceRequest::RetentionWindow(
                    SetRetentionWindowRequest {
                        retention_window_ms: NonZeroU64::new(86_400_000),
                    },
                )),
            ),
            (
                serde_json::json!({"kind": "retention_window", "bogus": 1}),
                None,
            ),
            (serde_json::json!({"kind": "grep_gc"}), None),
            (serde_json::json!({}), None),
            (serde_json::json!({"kind": "nope"}), None),
            (serde_json::json!({"kind": "gc", "bogus": 1}), None),
            (serde_json::json!({"kind": "gc", "max_objects": 1}), None),
            (serde_json::json!({"kind": "gc", "max_steps": 1}), None),
            (serde_json::json!({"kind": "retention", "bogus": 1}), None),
            (serde_json::json!({"kind": "metadata", "bogus": 1}), None),
        ];

        for (body, expected) in cases {
            let decoded = serde_json::from_value::<RunMaintenanceRequest>(body.clone());
            match expected {
                Some(expected) => {
                    let decoded = decoded.expect("valid maintenance request should decode");
                    assert_eq!(decoded, expected);
                    assert_eq!(
                        serde_json::to_value(decoded)
                            .expect("maintenance request should serialize"),
                        body
                    );
                }
                None => {
                    decoded.expect_err(&format!("invalid maintenance request decoded: {body}"));
                }
            }
        }
    }
}
