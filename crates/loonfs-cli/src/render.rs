//! Renders command outcomes as human-readable text or `--json`.

use crate::args::CommandKind;
use crate::commands::{CommandData, CommandFailure, CommandOutput};
use crate::error::CliError;
use loonfs_api::{GcResponse, ReorganizeStepOutcome, RepairNamespaceOutcome, WalFlushStepOutcome};
use serde::Serialize;
use std::io::{self, Write};

fn gc_summary(report: &GcResponse) -> String {
    if report.incomplete_namespace_ignored {
        return "gc ignored incomplete namespace".to_owned();
    }
    let mut summary = format!(
        "gc deleted {} wal segments, {} tables, {} manifests, {} checkpoint records ({} retained)",
        report.deleted_wal_segments,
        report.deleted_metadata_tables,
        report.deleted_manifests,
        report.deleted_checkpoint_records,
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
    if let Some(cursor) = &report.next_cursor {
        summary.push_str(&format!("; next_cursor: {cursor}"));
    }
    summary
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
        CommandData::NamespaceRepaired(response) => {
            let outcome = match response.outcome {
                RepairNamespaceOutcome::AlreadyComplete => "already complete",
                RepairNamespaceOutcome::Completed => "completed",
                RepairNamespaceOutcome::Reaped => "reaped",
                RepairNamespaceOutcome::InFlight => "in flight",
            };
            format!("repair for {}: {outcome}", response.namespace_id)
        }
        CommandData::Changes(response) => {
            let mut lines = vec![
                format!(
                    "changes for {} after seq {} (through seq {})",
                    response.namespace_id, response.after_seq.0, response.through_seq.0
                ),
                "SEQ\tCOMMIT_ID\tDELTAS\tMESSAGE".to_owned(),
            ];
            for change in &response.changes {
                lines.push(format!(
                    "{}\t{}\t{}\t{}",
                    change.seq.0,
                    change.commit_id,
                    change.deltas.len(),
                    change.message.as_deref().unwrap_or("-")
                ));
            }
            if let Some(next_after_seq) = response.next_after_seq {
                lines.push(format!("next_after_seq: {}", next_after_seq.0));
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
        CommandData::GrepIndexEnabled(response) => {
            if response.already_enabled {
                format!(
                    "grep index already enabled on {} (built through seq {}); grep worker \
                     maintenance continues independently",
                    response.namespace_id, response.built_through_seq.0
                )
            } else {
                format!(
                    "grep index enabled on {}; grep worker backfill targets seq <= {}",
                    response.namespace_id, response.built_through_seq.0
                )
            }
        }
        CommandData::GrepIndexDisabled(response) => {
            if response.was_enabled {
                format!("grep index disabled on {}", response.namespace_id)
            } else {
                format!("grep index was not enabled on {}", response.namespace_id)
            }
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
            if let Some(content_ref) = &entry.content_ref {
                lines.push(format!("content_ref: {}", content_ref.digest));
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
                "REVISION\tSEQ\tSIZE\tDIGEST".to_owned(),
            ];
            for revision in revisions {
                lines.push(format!(
                    "{}\t{}\t{}\t{}",
                    revision.revision_no.0,
                    revision.committed_seq.0,
                    revision.content_ref.size_bytes,
                    revision.content_ref.digest
                ));
            }
            if let Some(cursor) = next_cursor {
                lines.push(format!("next_cursor: {cursor}"));
            }
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
                     recover with `loon undelete <path> --inode {inode_id} \
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
                writer_version: None,
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
            error: Box::new(CliError::from(crate::backend::BackendError::client_error(
                "connection refused",
            ))),
        };
        assert_json_snapshot!(serde_json::from_str::<serde_json::Value>(
            &json_error(&failure).expect("json error renders")
        )
        .expect("rendered error is valid json"));
    }
}
