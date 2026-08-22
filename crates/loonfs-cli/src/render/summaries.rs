//! Short, reusable phrases for human-readable command summaries.

use super::*;

pub(super) fn public_inode_id(inode_id: loonfs_api::InodeId) -> String {
    loonfs_api::public_inode_id::encode(inode_id)
}

/// Formats one object-store contract check.
pub(super) fn store_probe_check_line(check: &StoreProbeCheckResult) -> String {
    match check.outcome {
        StoreProbeCheckOutcome::Passed => format!("{}: passed", check.name),
        StoreProbeCheckOutcome::Unsupported => format!("{}: unsupported", check.name),
        StoreProbeCheckOutcome::Failed => match &check.message {
            Some(message) => format!("{}: failed: {message}", check.name),
            None => format!("{}: failed", check.name),
        },
    }
}

/// Formats the object-store probe for both `admin store probe` and
/// `doctor --write-check`.
pub(super) fn store_probe_report_lines(
    response: &loonfs_api::v0::StoreProbeResponse,
) -> Vec<String> {
    let failed = response
        .checks
        .iter()
        .filter(|check| check.outcome == StoreProbeCheckOutcome::Failed)
        .count();
    let mut lines: Vec<String> = response.checks.iter().map(store_probe_check_line).collect();
    lines.push(if failed == 0 {
        format!(
            "store probe {}: {} checks passed",
            response.run_id,
            response.checks.len()
        )
    } else {
        format!(
            "store probe {}: {failed} of {} checks failed",
            response.run_id,
            response.checks.len()
        )
    });
    lines
}

/// Groups failed probe checks by the one-line message shown by `doctor`.
pub(super) fn store_probe_failure_group_lines(
    response: &loonfs_api::v0::StoreProbeResponse,
) -> Vec<String> {
    let mut groups: Vec<(String, Vec<&str>)> = Vec::new();
    for check in &response.checks {
        if check.outcome != StoreProbeCheckOutcome::Failed {
            continue;
        }
        let message = check
            .message
            .as_deref()
            .map(normalize_probe_message)
            .filter(|message| !message.is_empty())
            .unwrap_or_else(|| "object-store check failed".to_owned());
        match groups.iter_mut().find(|(existing, _)| *existing == message) {
            Some((_, affected)) => affected.push(check.name.as_str()),
            None => groups.push((message, vec![check.name.as_str()])),
        }
    }

    groups
        .into_iter()
        .flat_map(|(message, affected)| {
            let count = affected.len();
            let noun = if count == 1 { "check" } else { "checks" };
            [
                format!("{count} {noun} failed: {message}"),
                format!("affected: {}", affected.join(", ")),
            ]
        })
        .collect()
}

fn normalize_probe_message(message: &str) -> String {
    let normalized = message.split_whitespace().collect::<Vec<_>>().join(" ");
    match normalized.find("object-store ") {
        Some(public_start) => normalized[public_start..].to_owned(),
        None => normalized,
    }
}

/// Formats the result of one WAL flush step.
pub(super) fn wal_flush_summary(outcome: &WalFlushStepOutcome, tail_segments: u64) -> String {
    match outcome {
        WalFlushStepOutcome::NotNeeded => {
            format!("wal flush not needed (tail {tail_segments} segments)")
        }
        WalFlushStepOutcome::Flushed { manifest_head_seq } => {
            format!("wal flushed @ seq {}", manifest_head_seq.0)
        }
        WalFlushStepOutcome::Superseded {
            attempted_seq,
            current_manifest_no,
        } => format!(
            "wal flush @ seq {} superseded (current manifest {current_manifest_no})",
            attempted_seq.0
        ),
        WalFlushStepOutcome::RaceLost { observed_head_seq } => format!(
            "wal flush race lost (head moved past seq {})",
            observed_head_seq.0
        ),
    }
}

/// Formats the current grep-index state.
pub(super) fn grep_index_state_summary(state: &GrepIndexLifecycle) -> String {
    match state {
        GrepIndexLifecycle::Disabled => "disabled".to_owned(),
        GrepIndexLifecycle::Backfilling {
            target_seq,
            cursor_inode_id,
            ..
        } => match cursor_inode_id {
            Some(inode_id) => format!(
                "backfilling toward seq {}, walked through inode {}",
                target_seq.0,
                public_inode_id(*inode_id)
            ),
            None => format!("backfilling toward seq {}, not yet started", target_seq.0),
        },
        GrepIndexLifecycle::Active {
            built_through_seq,
            next_event_index,
        } => {
            if *next_event_index == 0 {
                format!("active, built through seq {}", built_through_seq.0)
            } else {
                format!(
                    "active, built through seq {} up to event {}",
                    built_through_seq.0, next_event_index
                )
            }
        }
    }
}

/// Formats the final state of one maintenance assignment.
pub(super) fn maintenance_key_line(key: &MaintenanceKeyReport) -> String {
    let Some(conclusion) = &key.conclusion else {
        return format!(
            "{}/{}: not started; the budget ran out first",
            key.namespace_id, key.job
        );
    };
    let spent = steps_phrase(key.steps);
    if key.settled {
        format!(
            "{}/{}: {conclusion} after {spent}",
            key.namespace_id, key.job
        )
    } else {
        format!(
            "{}/{}: {conclusion} after {spent}, still not settled",
            key.namespace_id, key.job
        )
    }
}

pub(super) fn steps_phrase(steps: u64) -> String {
    if steps == 1 {
        "1 step".to_owned()
    } else {
        format!("{steps} steps")
    }
}

pub(super) fn maintenance_assignment(namespaces: &[NamespaceId], jobs: &[String]) -> String {
    format!(
        "{} for {}",
        jobs.join(", "),
        namespaces
            .iter()
            .map(NamespaceId::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// Formats a checkpoint owner for the table view.
///
/// User checkpoints show their label. Fork checkpoints show their target
/// namespace because users cannot release them.
pub(super) fn checkpoint_owner_label(owner: &CheckpointOwnerSummary) -> String {
    match owner {
        CheckpointOwnerSummary::User { name } => name.clone(),
        CheckpointOwnerSummary::Fork {
            target_namespace_id,
        } => format!("fork -> {target_namespace_id}"),
    }
}

pub(super) fn gc_summary(report: &GcResponse) -> String {
    let mut summary = format!(
        "gc deleted {} wal segments, {} tables, {} manifests, {} checkpoint records, {} content objects ({} retained)",
        report.deleted_wal_segments,
        report.deleted_metadata_tables,
        report.deleted_manifests,
        report.deleted_checkpoint_records,
        report.deleted_content_objects,
        report.retained_candidates
    );
    // Keep the text summary short by showing only the most common retention
    // reason. JSON output includes the complete breakdown.
    if let Some((reason, count)) = report.retained.top_reason() {
        summary.push_str(&format!("; mostly {reason}: {count}"));
    }
    if report.released_fork_checkpoints > 0 {
        summary.push_str(&format!(
            "; released {} fork checkpoints",
            report.released_fork_checkpoints
        ));
    }
    if report.degraded_retention {
        summary.push_str("; retention degraded: ambiguous roots suppressed deletion");
    }
    if report.content_reclamation_deferred {
        summary.push_str(
            "; content reclamation deferred: the reference scan did not fit in --max-objects",
        );
    }
    if report.budget_exhausted {
        summary.push_str("; stopped on --max-objects before the pass finished");
    }
    if let Some(cursor) = &report.next_cursor {
        summary.push_str(&format!("; next_cursor: {cursor}"));
    }
    summary
}

/// Renders Unix milliseconds as `YYYY-MM-DD HH:MM:SSZ` without adding a date
/// library dependency. The conversion uses Howard Hinnant's civil-date
/// algorithm.
pub(crate) fn format_utc_ms(unix_ms: u64) -> String {
    let seconds = unix_ms / 1_000;
    let days = i64::try_from(seconds / 86_400).unwrap_or(0);
    let second_of_day = seconds % 86_400;
    let (hh, mm, ss) = (
        second_of_day / 3_600,
        (second_of_day % 3_600) / 60,
        second_of_day % 60,
    );
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02} {hh:02}:{mm:02}:{ss:02}Z")
}

/// Formats one change-feed event for human-readable output.
pub(super) fn event_descriptor(event: &loonfs_api::v0::FilesystemChange) -> String {
    use loonfs_api::v0::FilesystemChange;
    match event {
        FilesystemChange::DirectoryCreated { display_name, .. }
        | FilesystemChange::FileCreated { display_name, .. } => {
            format!("create '{display_name}'")
        }
        FilesystemChange::ContentChanged {
            inode_id,
            revision_no,
            ..
        } => format!(
            "write inode {} rev #{}",
            public_inode_id(*inode_id),
            revision_no.0
        ),
        FilesystemChange::Moved {
            from_display_name,
            to_display_name,
            ..
        } => format!("move '{from_display_name}' -> '{to_display_name}'"),
        FilesystemChange::Deleted {
            inode_id,
            deleted_direntry,
        } => match deleted_direntry {
            Some(direntry) => format!("delete '{}'", direntry.display_name),
            None => format!("delete inode {}", public_inode_id(*inode_id)),
        },
        FilesystemChange::Undeleted { display_name, .. } => {
            format!("undelete '{display_name}'")
        }
        FilesystemChange::AttributesChanged {
            inode_id,
            attributes_revision_no,
            ..
        } => format!(
            "attributes inode {} rev #{}",
            public_inode_id(*inode_id),
            attributes_revision_no.0
        ),
    }
}

pub(super) fn event_summary(events: &[loonfs_api::v0::FilesystemChange]) -> String {
    const SHOWN: usize = 3;
    if events.is_empty() {
        return "-".to_owned();
    }
    let mut shown: Vec<String> = events.iter().take(SHOWN).map(event_descriptor).collect();
    if events.len() > SHOWN {
        shown.push(format!("+{} more", events.len() - SHOWN));
    }
    shown.join("; ")
}
