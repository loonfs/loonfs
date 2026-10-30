//! Short, reusable phrases for human-readable command summaries.

use super::*;

/// Overall result of an object-store probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreProbeRunOutcome {
    Failed { checks: usize },
    Unsupported { checks: usize },
    Passed,
}

pub(crate) fn store_probe_run_outcome(
    response: &loonfs_types::StoreProbeResponse,
) -> StoreProbeRunOutcome {
    let unsupported = response
        .checks
        .iter()
        .filter(|check| check.outcome == StoreProbeCheckOutcome::Unsupported)
        .count();
    let failed = response
        .checks
        .iter()
        .filter(|check| {
            !matches!(
                check.outcome,
                StoreProbeCheckOutcome::Passed | StoreProbeCheckOutcome::Unsupported
            )
        })
        .count();
    match (failed, unsupported) {
        (0, 0) => StoreProbeRunOutcome::Passed,
        (0, checks) => StoreProbeRunOutcome::Unsupported { checks },
        (checks, _) => StoreProbeRunOutcome::Failed { checks },
    }
}

pub(crate) fn store_probe_summary_line(response: &loonfs_types::StoreProbeResponse) -> String {
    let run_id = &response.run_id;
    let total = response.checks.len();
    match store_probe_run_outcome(response) {
        StoreProbeRunOutcome::Failed { checks } => {
            format!("store probe {run_id}: {checks} of {total} checks failed")
        }
        StoreProbeRunOutcome::Unsupported { checks } => {
            format!("store probe {run_id}: {checks} of {total} checks are unsupported")
        }
        StoreProbeRunOutcome::Passed => format!("store probe {run_id}: {total} checks passed"),
    }
}

pub(super) fn store_probe_report_lines(response: &loonfs_types::StoreProbeResponse) -> Vec<String> {
    let mut lines: Vec<String> = response
        .checks
        .iter()
        .map(StoreProbeCheck::check_line)
        .collect();
    lines.push(store_probe_summary_line(response));
    lines
}

/// Groups failed probe checks by the one-line message shown by `doctor`.
pub(super) fn store_probe_failure_group_lines(
    response: &loonfs_types::StoreProbeResponse,
) -> Vec<String> {
    let mut groups: Vec<(String, Vec<&str>)> = Vec::new();
    for check in &response.checks {
        let StoreProbeCheckOutcome::Failed { message } = &check.outcome else {
            continue;
        };
        let message = Some(normalize_probe_message(message))
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

/// Formats the current grep index state.
pub(super) fn grep_index_state_summary(state: &GrepIndexStatus) -> String {
    match state {
        GrepIndexStatus::Disabled => "disabled".to_owned(),
        GrepIndexStatus::Unknown => "status unknown to this CLI".to_owned(),
        GrepIndexStatus::Backfilling {
            captured_seq,
            cursor_inode_id,
            ..
        } => match cursor_inode_id {
            Some(inode_id) => format!(
                "backfilling toward seq {}, walked through inode {inode_id}",
                captured_seq.0
            ),
            None => format!("backfilling toward seq {}, not yet started", captured_seq.0),
        },
        GrepIndexStatus::Active {
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

/// Formats a checkpoint owner for the table view.
///
/// Snapshots show their label. Fork checkpoints show their target namespace
/// because users can't delete them.
pub(super) fn checkpoint_owner_label(owner: &CheckpointOwner) -> String {
    match owner {
        CheckpointOwner::User { name } => name.clone(),
        CheckpointOwner::Fork {
            target_namespace_id,
        } => format!("fork -> {target_namespace_id}"),
        CheckpointOwner::Unknown => "owner unknown to this CLI".to_owned(),
    }
}

fn gc_deleted_counts(report: &GcResponse) -> [(&'static str, u64); 9] {
    let deleted = &report.deleted;
    let checkpoints = &report.deleted_checkpoints_by_owner;
    [
        ("wal objects", deleted.wal_objects),
        ("metadata segments", deleted.metadata_segments),
        ("manifests", deleted.manifests),
        ("fork checkpoints", checkpoints.fork),
        ("snapshots", checkpoints.user),
        ("upload sessions", deleted.upload_sessions),
        ("content objects", deleted.content_objects),
        ("retired content objects", deleted.retired_content_objects),
        ("temporary objects", deleted.temporary_objects),
    ]
}

fn push_top_retention_reason(summary: &mut String, response: &GcResponse) {
    if let Some((reason, count)) = response.retained.top_reason() {
        summary.push_str(&format!("; mostly {reason}: {count}"));
    }
}

pub(super) fn gc_summary(report: &GcResponse) -> String {
    let deleted = gc_deleted_counts(report)
        .into_iter()
        .map(|(family, count)| format!("{count} {family}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut summary = format!(
        "gc deleted {deleted} ({} retained)",
        report.retained.total()
    );
    push_top_retention_reason(&mut summary, report);
    if let Some(grep) = report.extensions.grep {
        summary.push_str(&format!(
            "\ngrep index gc deleted {} segments, {} other objects ({} retained)",
            grep.deleted_segments, grep.deleted_other_objects, grep.retained_candidates
        ));
        if grep.namespace_reaped {
            summary.push_str("; the namespace's grep state was reaped");
        }
    }
    if let Some(deadline) = report.reclaimable_at_ms {
        summary.push_str(&format!(
            "\nnamespace can be reclaimed at or after {} after pin cleanup; only fork pins can hold it beyond this deadline",
            format_utc_ms(deadline)
        ));
    }
    summary
}

/// Renders Unix milliseconds as `YYYY-MM-DD HH:MM:SSZ` without adding a date
/// library dependency. The conversion uses Howard Hinnant's civil-date
/// algorithm.
pub(super) fn format_utc_ms(unix_ms: u64) -> String {
    let seconds = unix_ms / 1_000;
    let days = i64::try_from(seconds / 86_400).unwrap_or(0);
    let second_of_day = seconds % 86_400;
    let (hh, mm, ss) = (
        second_of_day / 3_600,
        (second_of_day % 3_600) / 60,
        second_of_day % 60,
    );
    let loonfs_types::CivilDate { year, month, day } = loonfs_types::civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02} {hh:02}:{mm:02}:{ss:02}Z")
}

/// Formats one change-feed event for human-readable output.
pub(super) fn event_descriptor(event: &loonfs_types::FilesystemChange) -> String {
    use loonfs_types::FilesystemChange;
    match event {
        FilesystemChange::DirectoryCreated { display_name, .. }
        | FilesystemChange::FileCreated { display_name, .. } => {
            format!("create '{display_name}'")
        }
        FilesystemChange::ContentChanged {
            inode_id,
            revision_no,
            ..
        } => format!("write inode {inode_id} rev #{}", revision_no.0),
        FilesystemChange::Moved {
            source_display_name,
            destination_display_name,
            ..
        } => format!("move '{source_display_name}' -> '{destination_display_name}'"),
        FilesystemChange::Deleted {
            deleted_binding, ..
        } => format!("delete '{}'", deleted_binding.display_name),
        FilesystemChange::Undeleted { display_name, .. } => {
            format!("undelete '{display_name}'")
        }
        FilesystemChange::AttributesChanged {
            inode_id,
            attributes_revision_no,
            ..
        } => format!(
            "attributes inode {inode_id} rev #{}",
            attributes_revision_no.0
        ),
        FilesystemChange::AccessChanged {
            inode_id,
            access_revision_no,
            ..
        } => format!("access inode {inode_id} rev #{}", access_revision_no.0),
        FilesystemChange::Unknown => "unknown event".to_owned(),
    }
}

pub(super) fn event_summary(events: &[loonfs_types::FilesystemChange]) -> String {
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
