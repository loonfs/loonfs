//! Human-readable text for namespace-level and maintenance command results.

use super::summaries::*;
use crate::commands::MaintenanceRan;
use loonfs_merge::{
    MergeFailure, MergeSessionOutcome, MergeSessionState, MergeSessionStatus, MAX_NEW_FORKS,
};
use loonfs_types::{
    CommitSeq, DeleteNamespaceResponse, DeleteSnapshotResponse, GrepIndex, ListChangesResponse,
    ListSnapshotRecordsResponse, ListSnapshotsResponse, MetadataMaintenanceOutcome,
    MetadataSkippedWork, NamespaceMetadata, RunMaintenanceJobResponse,
    RunPoolMaintenanceJobResponse, Snapshot, StoreProbeResponse,
};
use loonfs_types::{MergeConflict, MergeConflictKind};

pub(super) fn human_default_namespace(profile: &str, namespace: &str) -> String {
    format!("default namespace for `{profile}` set to `{namespace}`")
}

pub(super) fn human_current(profile: &str, namespace: Option<&str>) -> String {
    format!(
        "profile: {profile}\nnamespace: {}",
        namespace.unwrap_or("-")
    )
}

pub(super) fn human_namespace_status(namespace: &NamespaceMetadata) -> String {
    let access = match &namespace.access {
        loonfs_types::NamespaceAccessMode::Unrestricted {} => "unrestricted".to_owned(),
        loonfs_types::NamespaceAccessMode::Acl { principal_scope } => {
            format!("acl (scope {principal_scope})")
        }
        loonfs_types::NamespaceAccessMode::Unknown => "access mode unknown to this CLI".to_owned(),
    };
    format!(
        "{} @ seq {} (retention floor {})\ncreated_by: {}\naccess: {access}\n\
         retention_window_ms: {}\npool: {}",
        namespace.namespace_id,
        namespace.head_seq.0,
        namespace.retention_floor_seq.0,
        namespace.created_by,
        retention_window(namespace.retention_window_ms),
        namespace.pool_id,
    )
}

/// Formats a namespace's retention window, or `-` when the namespace has no
/// window.
fn retention_window(retention_window_ms: Option<u64>) -> String {
    retention_window_ms.map_or_else(|| "-".to_owned(), |window_ms| window_ms.to_string())
}

pub(super) fn human_namespace_deleted(response: &DeleteNamespaceResponse) -> String {
    format!(
        "deleted {} (head_seq {})",
        response.namespace_id, response.head_seq.0
    )
}

/// Formats a snapshot's expiry, or `never` for a snapshot without one.
fn snapshot_expiry(snapshot: &Snapshot) -> String {
    snapshot
        .expires_at_ms
        .map_or_else(|| "never".to_owned(), format_utc_ms)
}

pub(super) fn human_snapshot_created(snapshot: &Snapshot) -> String {
    format!(
        "snapshot {} created for {} at sequence {} (name: {}, expires: {})",
        snapshot.snapshot_id,
        snapshot.namespace_id,
        snapshot.captured_seq.0,
        snapshot.name,
        snapshot_expiry(snapshot)
    )
}

pub(super) fn human_snapshots_listed(response: &ListSnapshotsResponse) -> String {
    let mut lines = vec![
        format!("snapshots for {}", response.namespace_id),
        "SNAPSHOT\tNAME\tSEQ\tCREATED\tEXPIRES".to_owned(),
    ];
    for snapshot in &response.snapshots {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}",
            snapshot.snapshot_id,
            snapshot.name,
            snapshot.captured_seq.0,
            format_utc_ms(snapshot.created_at_ms),
            snapshot_expiry(snapshot),
        ));
    }
    if response.snapshots.is_empty() {
        lines.push("(none)".to_owned());
    }
    if let Some(cursor) = &response.next_cursor {
        lines.push(format!("next_cursor: {cursor}"));
    }
    lines.join("\n")
}

pub(super) fn human_snapshot_extended(snapshot: &Snapshot) -> String {
    format!(
        "snapshot {} in {} extended to {}",
        snapshot.snapshot_id,
        snapshot.namespace_id,
        snapshot_expiry(snapshot)
    )
}

pub(super) fn human_snapshot_deleted(response: &DeleteSnapshotResponse) -> String {
    format!(
        "snapshot {} in {} deleted",
        response.snapshot_id, response.namespace_id
    )
}

pub(super) fn human_snapshot_records_listed(response: &ListSnapshotRecordsResponse) -> String {
    let mut lines = vec![
        format!("snapshots for {}", response.namespace_id),
        "CREATED\tEXPIRES\tSEQ\tOWNER\tSNAPSHOT".to_owned(),
    ];
    for snapshot in &response.snapshots {
        let owner = snapshot_record_owner_label(&snapshot.owner);
        let expiry = snapshot
            .expires_at_ms
            .map_or_else(|| "-".to_owned(), format_utc_ms);
        lines.push(format!(
            "{}\t{expiry}\t{}\t{owner}\t{}",
            format_utc_ms(snapshot.created_at_ms),
            snapshot.captured_seq.0,
            snapshot.snapshot_id,
        ));
    }
    if response.snapshots.is_empty() {
        lines.push("(none)".to_owned());
    }
    if let Some(cursor) = &response.next_cursor {
        lines.push(format!("next_cursor: {cursor}"));
    }
    lines.join("\n")
}

pub(super) fn human_maintenance_ran(ran: &MaintenanceRan) -> String {
    match &ran.response {
        RunMaintenanceJobResponse::RecoverAdministrator(recovery) => format!(
            "administrator recovery for {}: access revision {} at seq {}",
            recovery.namespace_id, recovery.access_revision_no, recovery.committed_seq,
        ),
        RunMaintenanceJobResponse::Metadata(metadata) => {
            let mut lines = vec![format!(
                "metadata maintenance for {}: {} ({} compaction units published)",
                metadata.namespace_id,
                match metadata.outcome {
                    MetadataMaintenanceOutcome::CaughtUp => "caught up",
                    MetadataMaintenanceOutcome::Due => "work still due, run again later",
                    MetadataMaintenanceOutcome::Unknown => "outcome unknown to this CLI",
                },
                metadata.compaction_units,
            )];
            lines.extend(metadata.skipped.iter().map(|reason| {
                match reason {
                    MetadataSkippedWork::ExpiredRowRemovalWalkLimit => {
                        "skipped the removal of rows that expired deletions hide: the kept inode walk reached its limit of 4,096 open directories"
                    }
                    MetadataSkippedWork::Unknown => "skipped work for a reason unknown to this CLI",
                }
                .to_owned()
            }));
            lines.join("\n")
        }
        RunMaintenanceJobResponse::Gc(gc) => {
            format!("gc for {}: {}", gc.namespace_id, gc_summary(gc))
        }
        RunMaintenanceJobResponse::Retention(retention) => {
            let state = if ran
                .retention_floor_before
                .is_some_and(|before| retention.retention_floor_seq > before)
            {
                "advanced to"
            } else {
                "unchanged at"
            };
            format!(
                "retention for {}: floor {state} seq {}",
                retention.namespace_id, retention.retention_floor_seq.0
            )
        }
        RunMaintenanceJobResponse::RetentionWindow(window) => match window.retention_window_ms {
            Some(window_ms) => format!(
                "retention window for {}: {window_ms} ms",
                window.namespace_id
            ),
            None => format!(
                "retention window for {}: none, so the sweep doesn't advance the floor",
                window.namespace_id
            ),
        },
        RunMaintenanceJobResponse::Unknown => "maintenance job unknown to this CLI".to_owned(),
    }
}

pub(super) fn human_pool_maintenance_ran(response: &RunPoolMaintenanceJobResponse) -> String {
    match response {
        RunPoolMaintenanceJobResponse::Gc(gc) => {
            format!("gc for {}: {}", gc.pool_id, pool_gc_summary(gc))
        }
        RunPoolMaintenanceJobResponse::Unknown => "maintenance job unknown to this CLI".to_owned(),
    }
}

pub(super) fn human_changes(response: &ListChangesResponse) -> String {
    let mut lines = vec![
        format!(
            "changes for {} after seq {} (head seq {})",
            response.namespace_id, response.after_seq.0, response.head_seq.0
        ),
        "SEQ\tDATE\tEVENTS\tMESSAGE".to_owned(),
    ];
    for change in &response.changes {
        lines.push(format!(
            "{}\t{}\t{}\t{}",
            change.committed_seq.0,
            format_utc_ms(change.committed_at_ms),
            event_summary(&change.events),
            change.message.as_deref().unwrap_or("-")
        ));
    }
    if let Some(next_after_seq) = response.next_after_seq {
        lines.push(format!("next_after_seq: {}", next_after_seq.0));
    }
    lines.join("\n")
}

pub(super) fn human_grep_index_enabled(
    response: &GrepIndex,
    waited_for_seq: Option<CommitSeq>,
    steps: u64,
    budget_exhausted: bool,
) -> String {
    let opening = format!("grep index enabled on {}", response.namespace_id);
    if budget_exhausted {
        let target =
            waited_for_seq.map_or_else(|| "its target".to_owned(), |seq| format!("seq {}", seq.0));
        format!(
            "{opening}; gave up waiting for {target} after {steps} steps — {}",
            grep_index_state_summary(&response.status)
        )
    } else {
        format!("{opening}; {}", grep_index_state_summary(&response.status))
    }
}

pub(super) fn human_store_probed(response: &StoreProbeResponse) -> String {
    store_probe_report_lines(response).join("\n")
}

pub(super) fn human_grep_index_disabled(response: &GrepIndex) -> String {
    format!("grep index disabled on {}", response.namespace_id)
}

pub(super) fn human_grep_index_status(response: &GrepIndex) -> String {
    let mut summary = format!(
        "grep index on {}: {}",
        response.namespace_id,
        grep_index_state_summary(&response.status)
    );
    if response.reorganize_pending {
        summary.push_str("; a reorganization is in progress");
    }
    summary
}

/// Returns the line that `loonfs merge` prints for one step of its session.
pub(crate) fn human_merge_step(outcome: &MergeSessionOutcome) -> String {
    match outcome {
        MergeSessionOutcome::MergeCommitted {
            fork_id,
            committed_seq,
            through_seq,
            ..
        } => format!(
            "merged `{fork_id}` through seq {} at seq {}",
            through_seq.0, committed_seq.0
        ),
        MergeSessionOutcome::Split {
            error,
            split_at_seq,
        } => format!(
            "{}; merging through seq {} first",
            error.message, split_at_seq.0
        ),
        MergeSessionOutcome::Conflicted { error, new_fork_id } => {
            format!("{}; rebasing into `{new_fork_id}`", error.message)
        }
        MergeSessionOutcome::ReplayCommitted {
            new_fork_id,
            through_seq,
            conflict_count,
        } => {
            let noun = if *conflict_count == 1 {
                "change"
            } else {
                "changes"
            };
            format!(
                "replayed through seq {} into `{new_fork_id}`, leaving out {conflict_count} \
                 conflicting {noun}",
                through_seq.0
            )
        }
        MergeSessionOutcome::ReportLost {
            replaced_fork_id,
            new_fork_id,
        } => format!(
            "the conflict report of `{replaced_fork_id}` was lost; rebasing again into \
             `{new_fork_id}`"
        ),
        MergeSessionOutcome::Resolved {
            fork_id,
            through_seq,
        } => format!(
            "resolved `{fork_id}`; merging it through seq {}",
            through_seq.0
        ),
        MergeSessionOutcome::Failed => "failed".to_owned(),
        MergeSessionOutcome::Deleted { namespace_id } => {
            format!("deleted new fork `{namespace_id}`")
        }
    }
}

pub(super) fn human_merge_session(
    record_path: &str,
    session: &MergeSessionState,
    steps: &[MergeSessionOutcome],
) -> String {
    let fork_id = &session.original_fork_id;
    let namespace_id = &session.namespace_id;
    match &session.status {
        MergeSessionStatus::Merged => {
            let merges = steps
                .iter()
                .filter(|step| matches!(step, MergeSessionOutcome::MergeCommitted { .. }))
                .count();
            match merges {
                0 => format!("`{fork_id}` has nothing left to merge into `{namespace_id}`"),
                1 => format!("merged `{fork_id}` into `{namespace_id}` in 1 merge"),
                _ => format!("merged `{fork_id}` into `{namespace_id}` in {merges} merges"),
            }
        }
        MergeSessionStatus::Abandoned => format!(
            "abandoned the merge of `{fork_id}` into `{namespace_id}`, and deleted its new forks"
        ),
        MergeSessionStatus::Failed {
            fork_id: inspect_id,
            failure,
            reports,
        } => {
            let mut lines = vec![
                format!(
                    "the merge of `{fork_id}` into `{namespace_id}` failed: {}",
                    merge_failure(failure)
                ),
                format!("fork to inspect: {inspect_id}"),
            ];
            lines.extend(
                reports
                    .iter()
                    .flat_map(|report| &report.conflicts)
                    .map(merge_conflict_line),
            );
            lines.push(format!(
                "the session is saved in `{record_path}`; rerun with `--abandon` to delete its \
                 new forks"
            ));
            lines.join("\n")
        }
        // A run ends only in one of the statuses above, so this is a record
        // read in the middle of a session.
        status => format!(
            "the merge of `{fork_id}` into `{namespace_id}` is {}",
            status.as_str()
        ),
    }
}

fn merge_failure(failure: &MergeFailure) -> String {
    match failure {
        MergeFailure::Refused { error } => format!("{} (`{}`)", error.message, error.code),
        MergeFailure::Unresolved { message } => message.clone(),
        MergeFailure::NewForkLimit => format!(
            "the session's rebases created {MAX_NEW_FORKS} new forks, the most that one \
             session creates"
        ),
        MergeFailure::RangeStartBelowFloor {
            retention_floor_seq,
        } => format!(
            "the fork's next merge starts below the fork's retention floor, seq {}, so the fork \
             can't be merged (`range_start_below_floor`)",
            retention_floor_seq.0
        ),
        MergeFailure::ForkPointBelowFloor {
            retention_floor_seq,
        } => format!(
            "the fork point is below the namespace's retention floor, seq {}, so the fork can't \
             be merged (`fork_point_below_floor`)",
            retention_floor_seq.0
        ),
        MergeFailure::UnknownRangeOutcome => {
            "the server returned a next-range outcome unknown to this CLI".to_owned()
        }
    }
}

fn merge_conflict_line(conflict: &MergeConflict) -> String {
    let kind = match conflict.kind {
        MergeConflictKind::Content => "content",
        MergeConflictKind::Binding => "binding",
        MergeConflictKind::Attributes => "attributes",
        MergeConflictKind::Access => "access",
        MergeConflictKind::Visibility => "visibility",
        MergeConflictKind::Name => "name",
        MergeConflictKind::Subtree => "subtree",
        MergeConflictKind::Dependency => "dependency",
        MergeConflictKind::Unknown => "unknown",
    };
    let version = conflict
        .ours
        .as_ref()
        .or(conflict.theirs.as_ref())
        .or(conflict.base.as_ref());
    match version {
        Some(version) => format!("conflict: {kind} {}", version.path),
        None => format!("conflict: {kind} inode {}", conflict.inode_id),
    }
}
