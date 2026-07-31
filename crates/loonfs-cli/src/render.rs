//! Renders command outcomes as human-readable text or `--json`.

use crate::args::CommandKind;
use crate::commands::{CommandData, CommandFailure, CommandOutput};
use crate::error::CliError;
use loonfs_api::v0::GrepIndexLifecycle;
use loonfs_api::{GcResponse, ReorganizeStepOutcome, WalFlushStepOutcome};
use serde::Serialize;
use std::io::{self, Write};

/// One phrase for where the index is, in the terms that phase actually has.
fn grep_index_state_summary(state: &GrepIndexLifecycle) -> String {
    match state {
        GrepIndexLifecycle::Disabled => "disabled".to_owned(),
        GrepIndexLifecycle::Backfilling {
            target_seq,
            cursor_inode_id,
            ..
        } => match cursor_inode_id {
            Some(inode_id) => format!(
                "backfilling toward seq {}, walked through inode {}",
                target_seq.0, inode_id.0
            ),
            None => format!("backfilling toward seq {}, not yet started", target_seq.0),
        },
        GrepIndexLifecycle::Steady {
            built_through_seq,
            next_event_index,
        } => {
            if *next_event_index == 0 {
                format!("steady, built through seq {}", built_through_seq.0)
            } else {
                format!(
                    "steady, built through seq {} up to event {}",
                    built_through_seq.0, next_event_index
                )
            }
        }
    }
}

fn gc_summary(report: &GcResponse) -> String {
    let mut summary = format!(
        "gc deleted {} wal segments, {} tables, {} manifests, {} checkpoint records, {} content objects ({} retained)",
        report.deleted_wal_segments,
        report.deleted_metadata_tables,
        report.deleted_manifests,
        report.deleted_checkpoint_records,
        report.deleted_content_objects,
        report.retained_candidates
    );
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
    if let Some(cursor) = &report.next_cursor {
        summary.push_str(&format!("; next_cursor: {cursor}"));
    }
    summary
}

/// Renders Unix milliseconds as `YYYY-MM-DD HH:MM:SSZ`. Hand-rolled
/// civil-from-days arithmetic (Howard Hinnant's algorithm), matching the
/// presign signer's approach, so the CLI takes no date dependency.
fn format_utc_ms(unix_ms: u64) -> String {
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

/// Compact human descriptor for one semantic feed event.
fn event_descriptor(event: &loonfs_api::v0::FilesystemChange) -> String {
    use loonfs_api::v0::FilesystemChange;
    match event {
        FilesystemChange::Created { name, .. } => format!("create '{name}'"),
        FilesystemChange::ContentChanged {
            inode_id,
            revision_no,
            ..
        } => format!("write inode {inode_id} rev #{}", revision_no.0),
        FilesystemChange::Moved {
            from_name, to_name, ..
        } => format!("move '{from_name}' -> '{to_name}'"),
        FilesystemChange::Deleted { inode_id, name, .. } => match name {
            Some(name) => format!("delete '{name}'"),
            None => format!("delete inode {inode_id}"),
        },
        FilesystemChange::Undeleted { name, .. } => format!("undelete '{name}'"),
    }
}

fn event_summary(events: &[loonfs_api::v0::FilesystemChange]) -> String {
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

const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
struct JsonEnvelope<'a, T>
where
    T: Serialize,
{
    kind: &'a str,
    format_version: u32,
    profile: Option<&'a str>,
    mode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a CliError>,
}

pub(crate) fn render_success(output: &CommandOutput, json_mode: bool) -> io::Result<()> {
    if json_mode {
        let body = json_success(output)?;
        let mut stdout = io::stdout().lock();
        stdout.write_all(body.as_bytes())?;
        stdout.write_all(b"\n")?;
        return Ok(());
    }

    match &output.data {
        CommandData::StreamBytes(bytes) => {
            let mut stdout = io::stdout().lock();
            stdout.write_all(bytes)?;
        }
        _ => {
            let rendered = human_success(output);
            let mut stdout = io::stdout().lock();
            stdout.write_all(rendered.as_bytes())?;
            if !rendered.ends_with('\n') {
                stdout.write_all(b"\n")?;
            }
        }
    }
    Ok(())
}

pub(crate) fn render_error(failure: &CommandFailure, json_mode: bool) -> io::Result<()> {
    let mut stderr = io::stderr().lock();
    if json_mode {
        let body = json_error(failure)?;
        stderr.write_all(body.as_bytes())?;
        stderr.write_all(b"\n")?;
    } else {
        stderr.write_all(failure.error.message.as_bytes())?;
        if let Some(request_id) = &failure.error.request_id {
            stderr.write_all(format!(" (request id: {request_id})").as_bytes())?;
        }
        stderr.write_all(b"\n")?;
    }
    Ok(())
}

pub(crate) fn write_stderr_warning(message: impl std::fmt::Display) {
    let _ = writeln!(io::stderr().lock(), "warning: {message}");
}

/// Per-item progress for long-running recursive transfers, on stderr so
/// stdout stays the machine-readable summary.
pub(crate) fn write_stderr_progress(message: impl std::fmt::Display) {
    let _ = writeln!(io::stderr().lock(), "{message}");
}

pub(crate) fn json_success(output: &CommandOutput) -> io::Result<String> {
    match &output.data {
        CommandData::StreamBytes(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "streaming output does not support json rendering",
        )),
        data => serde_json::to_string_pretty(&JsonEnvelope {
            kind: output.kind.as_str(),
            format_version: FORMAT_VERSION,
            profile: output.profile.as_deref(),
            mode: output.mode.as_deref(),
            data: Some(data),
            error: None,
        })
        .map_err(io::Error::other),
    }
}

pub(crate) fn json_error(failure: &CommandFailure) -> io::Result<String> {
    serde_json::to_string_pretty(&JsonEnvelope::<serde_json::Value> {
        kind: failure.kind.as_str(),
        format_version: FORMAT_VERSION,
        profile: failure.profile.as_deref(),
        mode: failure.mode.as_deref(),
        data: None,
        error: Some(&failure.error),
    })
    .map_err(io::Error::other)
}

pub(crate) fn human_success(output: &CommandOutput) -> String {
    match &output.data {
        CommandData::Profile(profile) => {
            let rendered = toml::to_string_pretty(profile)
                .unwrap_or_else(|_| format!("mode = \"{}\"", profile.mode_str()));
            if output.kind == CommandKind::ProfileShow {
                let name = output.profile.as_deref().unwrap_or("<unknown>");
                format!("name = \"{name}\"\n{rendered}")
            } else {
                rendered
            }
        }
        CommandData::ProfileSummary(profile) => match output.kind {
            CommandKind::ProfileDelete => format!("deleted profile {}", profile.name),
            _ => {
                let store = profile
                    .store_kind
                    .as_deref()
                    .map(|s| format!(" ({s})"))
                    .unwrap_or_default();
                format!("{} {}{store}", profile.mode, profile.name)
            }
        },
        CommandData::ProfileList {
            default_profile,
            profiles,
        } => {
            let mut lines = vec!["NAME\tMODE\tSTORE\tDEFAULT".to_owned()];
            for profile in profiles {
                let store = profile.store_kind.as_deref().unwrap_or("-");
                let default = if default_profile.as_deref() == Some(profile.name.as_str()) {
                    "*"
                } else {
                    ""
                };
                lines.push(format!(
                    "{}\t{}\t{store}\t{default}",
                    profile.name, profile.mode
                ));
            }
            lines.join("\n")
        }
        CommandData::DefaultProfile { name } => format!("default profile set to `{name}`"),
        CommandData::DefaultNamespace { profile, namespace } => {
            format!("default namespace for `{profile}` set to `{namespace}`")
        }
        CommandData::Current { profile, namespace } => {
            let namespace = namespace.as_deref().unwrap_or("-");
            format!("profile: {profile}\nnamespace: {namespace}")
        }
        CommandData::NamespaceSummary(namespace) => namespace.namespace_id.to_string(),
        CommandData::NamespaceDeleted(response) => format!(
            "deleted {} (head_seq {})",
            response.namespace_id, response.head_seq.0
        ),
        CommandData::CheckpointCreated(response) => {
            let expiry = match response.expires_at_ms {
                Some(expires_at_ms) => format!(", expires at {expires_at_ms}"),
                None => String::new(),
            };
            format!(
                "checkpointed {} @ seq {} (checkpoint {}, manifest {}{expiry})",
                response.namespace_id,
                response.checkpoint_seq.0,
                response.checkpoint_id,
                response.manifest_id
            )
        }
        CommandData::CheckpointReleased(response) => {
            let state = if response.was_active {
                "released"
            } else {
                "already released or gone"
            };
            format!(
                "checkpoint {} in {}: {state}",
                response.checkpoint_id, response.namespace_id
            )
        }
        CommandData::MaintenanceStepped(response) => {
            let wal_flush = match &response.wal_flush {
                WalFlushStepOutcome::NotNeeded => format!(
                    "wal flush not needed (tail {} segments)",
                    response.status_before.wal_tail_segments
                ),
                WalFlushStepOutcome::Flushed { manifest_head_seq } => {
                    format!("wal flushed @ seq {}", manifest_head_seq.0)
                }
                WalFlushStepOutcome::Superseded {
                    attempted_seq,
                    current_manifest_id,
                } => format!(
                    "wal flush @ seq {} superseded (current manifest {})",
                    attempted_seq.0, current_manifest_id
                ),
                WalFlushStepOutcome::RaceLost { observed_head_seq } => format!(
                    "wal flush race lost (head moved past seq {})",
                    observed_head_seq.0
                ),
            };
            let reorganize = match response.reorganize {
                ReorganizeStepOutcome::NotNeeded => "reorganize not needed",
                ReorganizeStepOutcome::UnitPublished => "reorganized one family group",
                ReorganizeStepOutcome::BudgetExhausted => "reorganize over the per-step budget",
                ReorganizeStepOutcome::Superseded => "reorganize superseded",
            };
            let retention =
                if response.retention_floor_seq > response.status_before.retention_floor_seq {
                    format!(
                        "retention floor advanced to seq {}",
                        response.retention_floor_seq.0
                    )
                } else {
                    format!(
                        "retention floor unchanged at seq {}",
                        response.retention_floor_seq.0
                    )
                };
            let mut line = format!(
                "maintenance step for {}: {wal_flush}; {reorganize}; {retention}",
                response.namespace_id
            );
            if let Some(gc) = &response.gc {
                line.push_str(&format!("; {}", gc_summary(gc)));
            }
            line
        }
        CommandData::GarbageCollected(response) => {
            format!("gc for {}: {}", response.namespace_id, gc_summary(response))
        }
        CommandData::Changes(response) => {
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
                    change.seq.0,
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
        CommandData::Trash(response) => {
            let mut lines = vec![
                format!(
                    "trash for {} (head seq {})",
                    response.namespace_id, response.head_seq.0
                ),
                "DELETED\tNAME\tINODE\tSEQ\tRECOVER".to_owned(),
            ];
            for entry in &response.entries {
                let name = entry
                    .display_name
                    .as_ref()
                    .map(|name| name.as_str().to_owned())
                    .unwrap_or_else(|| "-".to_owned());
                let destination = entry
                    .display_name
                    .as_ref()
                    .map(|name| format!("/{name}"))
                    .unwrap_or_else(|| "<path>".to_owned());
                lines.push(format!(
                    "{}\t{}\t{}\t{}\tloonfs undelete {} --inode {} --deleted-at {}",
                    format_utc_ms(entry.deleted_at_ms),
                    name,
                    entry.root_inode_id,
                    entry.deleted_at_seq.0,
                    destination,
                    entry.root_inode_id,
                    entry.deleted_at_seq.0,
                ));
            }
            if response.entries.is_empty() {
                lines.push("(empty)".to_owned());
            }
            if let Some(cursor) = &response.next_cursor {
                lines.push(format!("next_cursor: {cursor}"));
            }
            lines.join("\n")
        }
        CommandData::PathEntries { entries } => entries
            .iter()
            .map(|entry| {
                let size = entry
                    .size_bytes
                    .map(|value: u64| value.to_string())
                    .unwrap_or_else(|| "-".to_owned());
                format!("{}\t{}\t{}", entry.inode_kind, size, entry.absolute_path)
            })
            .collect::<Vec<_>>()
            .join("\n"),
        CommandData::GrepIndexEnabled {
            namespace_id,
            already_enabled,
            state,
            waited_for_seq,
            steps,
            budget_exhausted,
        } => {
            let opening = if *already_enabled {
                format!("grep index already enabled on {namespace_id}")
            } else {
                format!("grep index enabled on {namespace_id}")
            };
            if *budget_exhausted {
                let target = waited_for_seq
                    .map_or_else(|| "its target".to_owned(), |seq| format!("seq {}", seq.0));
                format!(
                    "{opening}; gave up waiting for {target} after {steps} steps — {}",
                    grep_index_state_summary(state)
                )
            } else {
                format!("{opening}; {}", grep_index_state_summary(state))
            }
        }
        CommandData::GrepIndexDisabled(response) => {
            if response.was_enabled {
                format!("grep index disabled on {}", response.namespace_id)
            } else {
                format!("grep index was not enabled on {}", response.namespace_id)
            }
        }
        CommandData::GrepIndexStatus(response) => {
            let mut summary = format!(
                "grep index on {}: {}",
                response.namespace_id,
                grep_index_state_summary(&response.state)
            );
            if response.reorganize_pending {
                summary.push_str("; a reorganization is in progress");
            }
            summary
        }
        CommandData::GrepIndexCollected(response) => {
            let mut summary = format!(
                "index-gc for {}: {} segments, {} other objects deleted, {} retained",
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
        CommandData::GrepMatches {
            pattern,
            matches,
            tail_scanned,
            ..
        } => {
            let mut lines: Vec<String> = matches
                .iter()
                .map(|found| {
                    format!(
                        "{}:{}:{}",
                        found.absolute_path, found.line_number, found.line
                    )
                })
                .collect();
            lines.push(format!("{} matches for `{pattern}`", matches.len()));
            if !tail_scanned {
                lines.push(
                    "warning: recent commits were not scanned (allow_stale); results may be stale"
                        .to_owned(),
                );
            }
            lines.join("\n")
        }
        CommandData::PathEntry(entry) => {
            let mut lines = vec![format!("path: {}", entry.absolute_path)];
            if let Some(display_name) = &entry.display_name {
                lines.push(format!("name: {display_name}"));
            }
            lines.extend([
                format!("inode: {}", entry.inode_id),
                format!("kind: {}", entry.inode_kind),
                format!("seq: {}", entry.head_seq.0),
            ]);
            if let Some(size) = entry.size_bytes {
                lines.push(format!("size: {size}"));
            }
            if let Some(revision) = entry.revision_no {
                lines.push(format!("revision: {}", revision.0));
            }
            if let Some(committed_at_ms) = entry.committed_at_ms {
                lines.push(format!("modified: {}", format_utc_ms(committed_at_ms)));
            }
            if let Some(content_ref) = &entry.content_ref {
                lines.push(format!("content_id: {}", content_ref.content_id));
                lines.push(format!("content_kind: {}", content_ref.kind));
            }
            lines.join("\n")
        }
        CommandData::FileRevisions {
            target,
            revisions,
            next_cursor,
        } => {
            let mut lines = vec![
                format!("revisions for {target}"),
                "REVISION\tDATE\tSEQ\tSIZE\tDIGEST".to_owned(),
            ];
            for revision in revisions {
                lines.push(format!(
                    "{}\t{}\t{}\t{}\t{}",
                    revision.revision_no.0,
                    format_utc_ms(revision.committed_at_ms),
                    revision.committed_seq.0,
                    revision.content_ref.size_bytes,
                    revision.content_ref.content_id
                ));
            }
            if let Some(cursor) = next_cursor {
                lines.push(format!("next_cursor: {cursor}"));
            }
            lines.join("\n")
        }
        CommandData::TreeTransfer {
            source,
            destination,
            files,
            directories,
            failures,
        } => {
            let verb = match output.kind {
                CommandKind::FilesystemGet => "downloaded",
                CommandKind::FilesystemCp => "copied",
                _ => "stored",
            };
            let mut lines = Vec::new();
            for failure in failures {
                lines.push(format!(
                    "failed {}: {}: {}",
                    failure.path, failure.error.code, failure.error.message
                ));
            }
            let mut summary = format!(
                "{verb} {files} files and {directories} directories ({source} -> {destination})"
            );
            if !failures.is_empty() {
                summary.push_str(&format!("; {} failed", failures.len()));
            }
            lines.push(summary);
            lines.join("\n")
        }
        CommandData::FileTransfer {
            destination,
            bytes_written,
            ..
        } => format!("wrote {bytes_written} bytes to {destination}"),
        CommandData::FileMutation {
            target,
            committed_seq,
            commit_id,
            inode_id,
        } => match output.kind {
            CommandKind::FilesystemPut => {
                format!("stored {target} @ seq {committed_seq} (commit {commit_id})")
            }
            CommandKind::FilesystemRm => match inode_id {
                Some(inode_id) => format!(
                    "removed {target} @ seq {committed_seq} (commit {commit_id}); \
                     recover with `loonfs undelete <path> --inode {inode_id} \
                     --deleted-at {committed_seq}`"
                ),
                None => format!("removed {target} @ seq {committed_seq} (commit {commit_id})"),
            },
            CommandKind::FilesystemUndelete => {
                format!("recovered {target} @ seq {committed_seq} (commit {commit_id})")
            }
            _ => format!("{target} @ seq {committed_seq} (commit {commit_id})"),
        },
        CommandData::PathMove {
            from,
            to,
            committed_seq,
            commit_id,
        } => match output.kind {
            CommandKind::FilesystemMv => {
                format!("moved {from} -> {to} @ seq {committed_seq} (commit {commit_id})")
            }
            CommandKind::FilesystemCp => {
                format!("copied {from} -> {to} @ seq {committed_seq} (commit {commit_id})")
            }
            _ => format!("{from} -> {to} @ seq {committed_seq} (commit {commit_id})"),
        },
        CommandData::ConfigPath { path } => path.clone(),
        CommandData::ConfigShow { config } => {
            toml::to_string_pretty(config).unwrap_or_else(|_| "failed to render config".to_owned())
        }
        CommandData::ConfigShowDegraded { error, config_toml } => {
            format!(
                "warning: {error}\nshowing the file as parsed, secrets masked:\n\n{config_toml}"
            )
        }
        CommandData::Version {
            version,
            commit,
            commit_date,
        } => format!("{version} ({commit} {commit_date})"),
        CommandData::StreamBytes(_) => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{human_success, json_error, json_success};
    use crate::args::CommandKind;
    use crate::commands::{CommandData, CommandFailure, CommandOutput};
    use crate::config::{ProfileConfig, StoreConfig};
    use crate::error::CliError;
    use crate::profiles::ProfileSummary;
    use insta::{assert_json_snapshot, assert_snapshot};
    use loonfs_api::{
        AbsolutePath, AuthoritativePathEntry, ChangeSeq, DisplayName, InodeId, InodeKind,
        NamespaceId,
    };

    fn path_entry(path: &str, display_name: Option<&str>) -> AuthoritativePathEntry {
        AuthoritativePathEntry {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            absolute_path: AbsolutePath::parse(path).expect("absolute path"),
            inode_id: InodeId(if display_name.is_some() { 2 } else { 1 }),
            inode_kind: InodeKind::Directory,
            head_seq: ChangeSeq(3),
            parent_inode_id: display_name.map(|_| InodeId(1)),
            display_name: display_name.map(|name| DisplayName::parse(name).expect("display name")),
            revision_no: None,
            size_bytes: None,
            content_ref: None,
            committed_at_ms: None,
        }
    }

    fn stat_output(entry: AuthoritativePathEntry) -> CommandOutput {
        CommandOutput {
            kind: CommandKind::FilesystemStat,
            profile: Some("default".to_owned()),
            mode: Some("embedded".to_owned()),
            data: CommandData::PathEntry(entry),
        }
    }

    #[test]
    fn human_stat_omits_the_absent_root_name() {
        let root = stat_output(path_entry("/", None));
        assert_eq!(human_success(&root), "path: /\ninode: 1\nkind: dir\nseq: 3");

        let named = stat_output(path_entry("/docs", Some("docs")));
        assert_eq!(
            human_success(&named),
            "path: /docs\nname: docs\ninode: 2\nkind: dir\nseq: 3"
        );
    }

    #[test]
    fn json_stat_preserves_the_absent_root_name() {
        let root = stat_output(path_entry("/", None));
        let json: serde_json::Value =
            serde_json::from_str(&json_success(&root).expect("JSON stat should render"))
                .expect("rendered stat is JSON");

        assert!(json["data"].get("display_name").is_none());
    }

    #[test]
    fn human_profile_list_renders_default_marker() {
        let output = CommandOutput {
            kind: CommandKind::ProfileList,
            profile: None,
            mode: None,
            data: CommandData::ProfileList {
                default_profile: Some("default".to_owned()),
                profiles: vec![
                    ProfileSummary {
                        name: "default".to_owned(),
                        mode: "embedded".to_owned(),
                        store_kind: Some("local-fs".to_owned()),
                    },
                    ProfileSummary {
                        name: "prod".to_owned(),
                        mode: "remote".to_owned(),
                        store_kind: None,
                    },
                ],
            },
        };
        assert_snapshot!(human_success(&output));
    }

    #[test]
    fn human_profile_show_includes_name() {
        let output = CommandOutput {
            kind: CommandKind::ProfileShow,
            profile: Some("default".to_owned()),
            mode: Some("embedded".to_owned()),
            data: CommandData::Profile(ProfileConfig::Embedded {
                store: StoreConfig::LocalFs {
                    root: "/tmp/store".to_owned(),
                    key_prefix: None,
                },
                default_namespace: Some("demo".to_owned()),
                writer_id: None,
            }),
        };
        assert_snapshot!(human_success(&output));
    }

    #[test]
    fn json_error_carries_profile_and_code() {
        let failure = CommandFailure {
            kind: CommandKind::ConfigShow,
            profile: Some("default".to_owned()),
            mode: Some("remote".to_owned()),
            error: Box::new(CliError::from(
                crate::backend_error::BackendError::client_error("connection refused"),
            )),
        };
        assert_json_snapshot!(serde_json::from_str::<serde_json::Value>(
            &json_error(&failure).expect("json error renders")
        )
        .expect("rendered error is valid json"));
    }
}
