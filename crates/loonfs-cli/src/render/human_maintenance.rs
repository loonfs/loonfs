use super::summaries::*;
use crate::commands::MaintenanceKeyReport;
use loonfs_api::v0::{
    GrepGcResponse, GrepIndex, ListChangesResponse, ListSnapshotsResponse, ReleaseSnapshotResponse,
    SnapshotSummary, StoreProbeResponse,
};
use loonfs_api::{
    ChangeSeq, Checkpoint, DeleteNamespaceResponse, GcResponse, ListCheckpointsResponse,
    MaintenanceRunResponse, MetadataCompactionOutcome, Namespace, NamespaceId,
    ReleaseCheckpointResponse, ReorganizeStepOutcome,
};

pub(super) fn human_default_namespace(profile: &str, namespace: &str) -> String {
    format!("default namespace for `{profile}` set to `{namespace}`")
}

pub(super) fn human_current(profile: &str, namespace: Option<&str>) -> String {
    format!(
        "profile: {profile}\nnamespace: {}",
        namespace.unwrap_or("-")
    )
}

pub(super) fn human_namespace_status(namespace: &Namespace) -> String {
    format!(
        "{} @ seq {} (retention floor {})",
        namespace.namespace_id, namespace.head_seq.0, namespace.retention_floor_seq.0
    )
}

pub(super) fn human_namespace_deleted(response: &DeleteNamespaceResponse) -> String {
    format!(
        "deleted {} (head_seq {})",
        response.namespace_id, response.head_seq.0
    )
}

pub(super) fn human_snapshot_created(snapshot: &SnapshotSummary) -> String {
    format!(
        "snapshot {} created for {} at sequence {} (name: {}, expires: {})",
        snapshot.snapshot_id,
        snapshot.namespace_id,
        snapshot.head_seq.0,
        snapshot.name,
        format_utc_ms(snapshot.expires_at_ms)
    )
}

pub(super) fn human_snapshots_listed(response: &ListSnapshotsResponse) -> String {
    let mut lines = vec![
        format!("live snapshots for {}", response.namespace_id),
        "SNAPSHOT\tNAME\tSEQ\tCREATED\tEXPIRES".to_owned(),
    ];
    for snapshot in &response.snapshots {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}",
            snapshot.snapshot_id,
            snapshot.name,
            snapshot.head_seq.0,
            format_utc_ms(snapshot.created_at_ms),
            format_utc_ms(snapshot.expires_at_ms),
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

pub(super) fn human_snapshot_extended(snapshot: &SnapshotSummary) -> String {
    format!(
        "snapshot {} in {} extended to {}",
        snapshot.snapshot_id,
        snapshot.namespace_id,
        format_utc_ms(snapshot.expires_at_ms)
    )
}

pub(super) fn human_snapshot_released(response: &ReleaseSnapshotResponse) -> String {
    format!(
        "snapshot {} in {} released",
        response.snapshot_id, response.namespace_id
    )
}

pub(super) fn human_checkpoint_created(checkpoint: &Checkpoint) -> String {
    let expiry = match checkpoint.expires_at_ms {
        Some(expires_at_ms) => format!(", expires at {}", format_utc_ms(expires_at_ms)),
        None => String::new(),
    };
    format!(
        "checkpointed {} @ seq {} (checkpoint {}, manifest {}{expiry})",
        checkpoint.namespace_id,
        checkpoint.checkpoint_seq.0,
        checkpoint.checkpoint_id,
        checkpoint.manifest_no
    )
}

pub(super) fn human_checkpoints_listed(response: &ListCheckpointsResponse) -> String {
    let mut lines = vec![
        format!("active checkpoints for {}", response.namespace_id),
        "CREATED\tEXPIRES\tSEQ\tOWNER\tCHECKPOINT".to_owned(),
    ];
    for checkpoint in &response.checkpoints {
        let owner = checkpoint_owner_label(&checkpoint.owner);
        let expiry = checkpoint
            .expires_at_ms
            .map_or_else(|| "-".to_owned(), format_utc_ms);
        lines.push(format!(
            "{}\t{expiry}\t{}\t{owner}\t{}",
            format_utc_ms(checkpoint.created_at_ms),
            checkpoint.checkpoint_seq.0,
            checkpoint.checkpoint_id,
        ));
    }
    if response.checkpoints.is_empty() {
        lines.push("(none)".to_owned());
    }
    if let Some(cursor) = &response.next_cursor {
        lines.push(format!("next_cursor: {cursor}"));
    }
    lines.join("\n")
}

pub(super) fn human_checkpoint_released(response: &ReleaseCheckpointResponse) -> String {
    format!(
        "checkpoint {} in {} released or already gone",
        response.checkpoint_id, response.namespace_id
    )
}

pub(super) fn human_maintenance_ran(response: &MaintenanceRunResponse) -> String {
    match response {
        MaintenanceRunResponse::Metadata(metadata) => format!(
            "metadata maintenance: {}; {}",
            wal_flush_summary(&metadata.wal_flush),
            match metadata.reorganize {
                ReorganizeStepOutcome::NotNeeded => "reorganize not needed",
                ReorganizeStepOutcome::UnitPublished => "reorganized one family group",
                ReorganizeStepOutcome::CompactionStarted => {
                    "started a background compaction of one family group"
                }
                ReorganizeStepOutcome::CompactionAtCapacity => {
                    "queued a background compaction of one family group behind the server's compaction limit"
                }
                ReorganizeStepOutcome::CompactionRunning => {
                    "one family group is waiting on a background compaction"
                }
                ReorganizeStepOutcome::CompactionRequired => {
                    "one family group needs a compaction this server will not run on its own"
                }
                ReorganizeStepOutcome::RootAdvanced => {
                    "another publisher moved the metadata root, so the reorganize published nothing"
                }
            }
        ),
        MaintenanceRunResponse::MetadataCompaction(response) => match &response.outcome {
            MetadataCompactionOutcome::NotNeeded => "metadata compaction not needed".to_owned(),
            MetadataCompactionOutcome::BoundedMergePublished => {
                "metadata compaction published one bounded merge".to_owned()
            }
            MetadataCompactionOutcome::AlreadyRunning => {
                "metadata compaction already running".to_owned()
            }
            MetadataCompactionOutcome::Published {
                manifest_no,
                rows_read,
                rows_written,
                output_segments,
                ..
            } => format!(
                "metadata compaction published manifest {manifest_no}: {rows_read} rows read, \
                 {rows_written} rows written, {output_segments} output segments"
            ),
            MetadataCompactionOutcome::Cancelled => "metadata compaction cancelled".to_owned(),
            MetadataCompactionOutcome::Abandoned => "metadata compaction abandoned".to_owned(),
            MetadataCompactionOutcome::Fenced => "metadata compaction fenced".to_owned(),
            MetadataCompactionOutcome::Superseded => "metadata compaction superseded".to_owned(),
        },
        MaintenanceRunResponse::Gc(gc) => {
            format!("gc for {}: {}", gc.namespace_id, gc_summary(gc))
        }
        MaintenanceRunResponse::Retention(retention) => {
            format!("retention floor at seq {}", retention.retention_floor_seq.0)
        }
    }
}

pub(super) fn human_garbage_collected(response: &GcResponse) -> String {
    format!("gc for {}: {}", response.namespace_id, gc_summary(response))
}

pub(super) fn human_changes(response: &ListChangesResponse) -> String {
    let mut lines = vec![
        format!(
            "changes for {} after seq {} (through seq {})",
            response.namespace_id, response.after_seq.0, response.through_seq.0
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
    waited_for_seq: Option<ChangeSeq>,
    steps: u64,
    budget_exhausted: bool,
) -> String {
    let opening = format!("grep index enabled on {}", response.namespace_id);
    if budget_exhausted {
        let target =
            waited_for_seq.map_or_else(|| "its target".to_owned(), |seq| format!("seq {}", seq.0));
        format!(
            "{opening}; gave up waiting for {target} after {steps} steps — {}",
            grep_index_state_summary(&response.lifecycle)
        )
    } else {
        format!(
            "{opening}; {}",
            grep_index_state_summary(&response.lifecycle)
        )
    }
}

pub(super) fn human_maintenance_hosted(namespaces: &[NamespaceId], jobs: &[String]) -> String {
    format!(
        "hosted {}; stopped on signal",
        maintenance_assignment(namespaces, jobs)
    )
}

pub(super) fn human_maintenance_drained(
    namespaces: &[NamespaceId],
    jobs: &[String],
    keys: &[MaintenanceKeyReport],
    steps: u64,
    budget_exhausted: bool,
) -> String {
    let assignment = maintenance_assignment(namespaces, jobs);
    let settled = keys.iter().filter(|key| key.settled).count();
    let mut lines: Vec<String> = keys.iter().map(maintenance_key_line).collect();
    lines.push(if budget_exhausted {
        format!(
            "gave up on {assignment}: {settled} of {} keys settled after {}",
            keys.len(),
            steps_phrase(steps)
        )
    } else {
        format!(
            "drained {assignment}: {settled} keys settled after {}",
            steps_phrase(steps)
        )
    });
    lines.join("\n")
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
        grep_index_state_summary(&response.lifecycle)
    );
    if response.reorganize_pending {
        summary.push_str("; a reorganization is in progress");
    }
    summary
}

pub(super) fn human_grep_index_collected(response: &GrepGcResponse) -> String {
    let mut summary = format!(
        "index gc for {}: {} segments, {} other objects deleted, {} retained",
        response.namespace_id,
        response.deleted_segments,
        response.deleted_other_objects,
        response.retained_candidates
    );
    if response.namespace_reaped {
        summary.push_str("; the namespace's grep state was reaped");
    }
    if response.namespace_degraded {
        summary.push_str("; unreadable state forced conservative retention");
    }
    if let Some(cursor) = &response.next_cursor {
        summary.push_str(&format!("; more to examine (next_cursor: {cursor})"));
    }
    summary
}
