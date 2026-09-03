//! Human-readable rendering, including the intentionally exhaustive output match.

use super::summaries::*;
use super::*;
use crate::commands::{TrashListing, TreeTransferFailure};
use crate::config::{CliConfig, ProfileConfig};
use crate::profiles::ProfileSummary;
use loonfs_api::{ChangeSeq, CommitId, FileRevision, GrepMatch, PathEntry};

use super::human_maintenance::*;

pub(crate) fn human_success(output: &CommandOutput) -> String {
    match &output.data {
        CommandData::Capabilities(document) => human_capabilities(document),
        CommandData::Doctor { checks } => human_doctor(checks),
        CommandData::Profile(profile) => human_profile(output, profile),
        CommandData::ProfileSummary(profile) => human_profile_summary(output.kind, profile),
        CommandData::ProfileList {
            default_profile,
            profiles,
        } => human_profile_list(default_profile.as_deref(), profiles),
        CommandData::DefaultProfile { name } => format!("default profile set to `{name}`"),
        CommandData::DefaultNamespace { profile, namespace } => {
            human_default_namespace(profile, namespace)
        }
        CommandData::Current { profile, namespace } => human_current(profile, namespace.as_deref()),
        CommandData::NamespaceStatus(namespace) => human_namespace_status(namespace),
        CommandData::NamespaceDeleted(response) => human_namespace_deleted(response),
        CommandData::SnapshotCreated(snapshot) => human_snapshot_created(snapshot),
        CommandData::SnapshotsListed(response) => human_snapshots_listed(response),
        CommandData::SnapshotExtended(snapshot) => human_snapshot_extended(snapshot),
        CommandData::SnapshotReleased(response) => human_snapshot_released(response),
        CommandData::CheckpointCreated(checkpoint) => human_checkpoint_created(checkpoint),
        CommandData::CheckpointsListed(response) => human_checkpoints_listed(response),
        CommandData::CheckpointReleased(response) => human_checkpoint_released(response),
        CommandData::MaintenanceStepped(response) => human_maintenance_stepped(response),
        CommandData::GarbageCollected(response) => human_garbage_collected(response),
        CommandData::Changes(response) => human_changes(response),
        CommandData::Trash(listing) => human_trash(listing),
        CommandData::PathEntries {
            entries,
            next_cursor,
            ..
        } => human_path_entries(entries, next_cursor.as_deref()),
        CommandData::GrepIndexEnabled {
            response,
            waited_for_seq,
            steps,
            budget_exhausted,
        } => human_grep_index_enabled(response, *waited_for_seq, *steps, *budget_exhausted),
        CommandData::MaintenanceHosted { namespaces, jobs } => {
            human_maintenance_hosted(namespaces, jobs)
        }
        CommandData::MaintenanceDrained {
            namespaces,
            jobs,
            keys,
            steps,
            budget_exhausted,
        } => human_maintenance_drained(namespaces, jobs, keys, *steps, *budget_exhausted),
        CommandData::StoreProbed(response) => human_store_probed(response),
        CommandData::GrepIndexDisabled(response) => human_grep_index_disabled(response),
        CommandData::GrepIndexStatus(response) => human_grep_index_status(response),
        CommandData::GrepIndexCollected(response) => human_grep_index_collected(response),
        CommandData::GrepMatches {
            pattern,
            matches,
            tail_scanned,
            truncated,
            next_cursor,
            ..
        } => human_grep_matches(
            pattern,
            matches,
            *tail_scanned,
            *truncated,
            next_cursor.as_deref(),
        ),
        CommandData::PathEntry(entry) => human_path_entry_details(entry),
        CommandData::FileRevisions {
            target,
            revisions,
            next_cursor,
        } => human_file_revisions(target, revisions, next_cursor.as_deref()),
        CommandData::TreeTransfer {
            source,
            destination,
            files,
            directories,
            failures,
            ..
        } => human_tree_transfer(
            output.kind,
            source,
            destination,
            *files,
            *directories,
            failures,
        ),
        CommandData::FileTransfer {
            destination,
            bytes_written,
            ..
        } => format!("wrote {bytes_written} bytes to {destination}"),
        CommandData::FileMutation {
            target,
            committed_seq,
            commit_id,
            recovery_command,
            ..
        } => human_file_mutation(
            output.kind,
            target,
            *committed_seq,
            commit_id,
            recovery_command.as_deref(),
        ),
        CommandData::DirectoryAlreadyExists { target, .. } => {
            format!("{target} is already a directory")
        }
        CommandData::PathMove {
            from,
            to,
            committed_seq,
            commit_id,
        } => human_path_move(output.kind, from, to, *committed_seq, commit_id),
        CommandData::ConfigPath {
            path,
            source,
            preferred_path,
        } => human_config_path(path, *source, preferred_path.as_deref()),
        CommandData::ConfigShow { config } => human_config_show(config),
        CommandData::ConfigShowDegraded { error, config_toml } => {
            human_config_show_degraded(error, config_toml)
        }
        CommandData::Version { version } => version.clone(),
        CommandData::CompletionScript(_)
        | CommandData::StreamBytes(_)
        | CommandData::StreamedToStdout => String::new(),
    }
}

fn human_profile(output: &CommandOutput, profile: &ProfileConfig) -> String {
    let rendered = toml::to_string_pretty(profile)
        .unwrap_or_else(|_| format!("mode = \"{}\"", profile.mode_str()));
    if output.kind == CommandKind::ProfileShow {
        let name = output.profile.as_deref().unwrap_or("<unknown>");
        format!("name = \"{name}\"\n{rendered}")
    } else {
        rendered
    }
}

fn human_profile_summary(kind: CommandKind, profile: &ProfileSummary) -> String {
    match kind {
        CommandKind::ProfileDelete => format!("deleted profile {}", profile.name),
        _ => {
            let store = profile
                .store_kind
                .as_deref()
                .map(|store| format!(" ({store})"))
                .unwrap_or_default();
            format!("{} {}{store}", profile.mode, profile.name)
        }
    }
}

fn human_profile_list(default_profile: Option<&str>, profiles: &[ProfileSummary]) -> String {
    let mut lines = vec!["NAME\tMODE\tSTORE\tDEFAULT".to_owned()];
    for profile in profiles {
        let store = profile.store_kind.as_deref().unwrap_or("-");
        let default = if default_profile == Some(profile.name.as_str()) {
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

fn human_trash(listing: &TrashListing) -> String {
    let response = &listing.response;
    let mut lines = vec![
        format!(
            "trash for {} (head seq {})",
            response.namespace_id, response.head_seq.0
        ),
        "DELETED\tDELETED_BY\tNAME\tINODE\tSEQ\tRECOVER".to_owned(),
    ];
    for (entry, recovery_command) in response.entries.iter().zip(&listing.recovery_commands) {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{recovery_command}",
            format_utc_ms(entry.deleted_at_ms),
            render_actor(&entry.deleted_by),
            entry.deleted_binding.display_name,
            public_inode_id(entry.inode_id),
            entry.deletion_seq.0,
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

fn human_path_entries(entries: &[PathEntry], next_cursor: Option<&str>) -> String {
    let mut lines: Vec<String> = entries.iter().map(human_path_entry).collect();
    if let Some(cursor) = next_cursor {
        lines.push(more_entries_hint(cursor));
    }
    lines.join("\n")
}

fn human_grep_matches(
    pattern: &str,
    matches: &[GrepMatch],
    tail_scanned: bool,
    truncated: bool,
    next_cursor: Option<&str>,
) -> String {
    let mut lines: Vec<String> = matches
        .iter()
        .map(|found| format!("{}:{}:{}", found.path, found.line_number, found.line))
        .collect();
    if truncated {
        lines.push(format!(
            "{} matches for `{pattern}` (stopped at --limit; there are more)",
            matches.len()
        ));
    } else {
        lines.push(format!("{} matches for `{pattern}`", matches.len()));
    }
    if !tail_scanned {
        lines.push(
            "warning: recent commits were not scanned (allow_stale); results may be stale"
                .to_owned(),
        );
    }
    if let Some(cursor) = next_cursor {
        lines.push(format!("next_cursor: {cursor}"));
    }
    lines.join("\n")
}

fn human_path_entry_details(entry: &PathEntry) -> String {
    let mut lines = vec![format!("path: {}", entry.path)];
    if let Some(display_name) = &entry.display_name {
        lines.push(format!("name: {display_name}"));
    }
    lines.extend([
        format!("inode: {}", public_inode_id(entry.inode_id)),
        format!("kind: {}", entry.inode_kind()),
        format!("seq: {}", entry.head_seq.0),
        format!("created_by: {}", render_actor(&entry.created_by)),
        format!("created: {}", format_utc_ms(entry.created_at_ms)),
    ]);
    if let loonfs_api::PathEntryKind::File {
        revision_no,
        size_bytes,
        content_ref,
        revision_committed_by,
        revision_committed_at_ms,
    } = &entry.kind
    {
        lines.push(format!("size: {size_bytes}"));
        lines.push(format!("revision: {}", revision_no.0));
        lines.push(format!(
            "revision_committed_by: {}",
            render_actor(revision_committed_by)
        ));
        lines.push(format!(
            "modified: {}",
            format_utc_ms(*revision_committed_at_ms)
        ));
        lines.push(format!("content_id: {}", content_ref.content_id));
        lines.push(format!("content_kind: {}", content_ref.kind));
    }
    if let Some(attributes) = &entry.attributes {
        if let Some(updated_by) = &attributes.attributes_updated_by {
            lines.push(format!(
                "attributes_updated_by: {}",
                render_actor(updated_by)
            ));
        }
        if let Some(updated_at_ms) = attributes.attributes_updated_at_ms {
            lines.push(format!(
                "attributes_updated: {}",
                format_utc_ms(updated_at_ms)
            ));
        }
        for (key, value) in attributes.attributes.iter() {
            lines.push(format!("attr.{key}: {}", render_attribute_value(value)));
        }
    }
    lines.join("\n")
}

fn human_file_revisions(
    target: &str,
    revisions: &[FileRevision],
    next_cursor: Option<&str>,
) -> String {
    let mut lines = vec![
        format!("revisions for {target}"),
        "REVISION\tDATE\tCOMMITTED_BY\tSEQ\tSIZE\tDIGEST".to_owned(),
    ];
    for revision in revisions {
        lines.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            revision.revision_no.0,
            format_utc_ms(revision.committed_at_ms),
            render_actor(&revision.committed_by),
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

fn human_tree_transfer(
    kind: CommandKind,
    source: &str,
    destination: &str,
    files: u64,
    directories: u64,
    failures: &[TreeTransferFailure],
) -> String {
    let verb = match kind {
        CommandKind::FilesystemGet => "downloaded",
        CommandKind::FilesystemCp => "copied",
        _ => "stored",
    };
    let mut lines = Vec::new();
    for failure in failures {
        let rendered = human_error(&failure.error);
        lines.push(format!("failed {}: {rendered}", failure.path));
    }
    let directory_noun = if directories == 1 {
        "directory"
    } else {
        "directories"
    };
    let mut summary = format!(
        "{verb} {files} files and {directories} {directory_noun} ({source} -> {destination})"
    );
    if !failures.is_empty() {
        summary.push_str(&format!("; {} failed", failures.len()));
    }
    lines.push(summary);
    lines.join("\n")
}

fn human_file_mutation(
    kind: CommandKind,
    target: &str,
    committed_seq: ChangeSeq,
    commit_id: &CommitId,
    recovery_command: Option<&str>,
) -> String {
    match kind {
        CommandKind::FilesystemPut => {
            format!("stored {target} @ seq {committed_seq} (commit {commit_id})")
        }
        CommandKind::FilesystemRm => match recovery_command {
            Some(recovery_command) => format!(
                "removed {target} @ seq {committed_seq} (commit {commit_id}); recover with `{recovery_command}`"
            ),
            None => format!("removed {target} @ seq {committed_seq} (commit {commit_id})"),
        },
        CommandKind::FilesystemUndelete => {
            format!("recovered {target} @ seq {committed_seq} (commit {commit_id})")
        }
        CommandKind::FilesystemAnnotate => {
            format!("annotated {target} @ seq {committed_seq} (commit {commit_id})")
        }
        _ => format!("{target} @ seq {committed_seq} (commit {commit_id})"),
    }
}

fn human_path_move(
    kind: CommandKind,
    from: &str,
    to: &str,
    committed_seq: ChangeSeq,
    commit_id: &CommitId,
) -> String {
    match kind {
        CommandKind::FilesystemMv => {
            format!("moved {from} -> {to} @ seq {committed_seq} (commit {commit_id})")
        }
        CommandKind::FilesystemCp => {
            format!("copied {from} -> {to} @ seq {committed_seq} (commit {commit_id})")
        }
        _ => format!("{from} -> {to} @ seq {committed_seq} (commit {commit_id})"),
    }
}

fn human_config_path(path: &str, source: ConfigSource, preferred_path: Option<&str>) -> String {
    let chosen = match (source, preferred_path) {
        (ConfigSource::Flag, _) => "from --config".to_owned(),
        (ConfigSource::Env, _) => "from LOONFS_CONFIG".to_owned(),
        (ConfigSource::Xdg, _) => "from XDG_CONFIG_HOME".to_owned(),
        (ConfigSource::Legacy, Some(preferred)) => {
            format!("legacy location; move it to {preferred} once convenient")
        }
        (ConfigSource::Legacy, None) => "default location".to_owned(),
    };
    format!("{path} ({chosen})")
}

fn human_config_show(config: &CliConfig) -> String {
    toml::to_string_pretty(config).unwrap_or_else(|_| "failed to render config".to_owned())
}

fn human_config_show_degraded(error: &str, config_toml: &str) -> String {
    format!("warning: {error}\nshowing the file as parsed, secrets masked:\n\n{config_toml}")
}

fn human_capabilities(document: &loonfs_api::CapabilityDocument) -> String {
    let mut planes = document.planes.clone();
    planes.sort();

    let enabled = document
        .features
        .iter()
        .filter(|(_, enabled)| **enabled)
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let disabled = document
        .features
        .iter()
        .filter(|(_, enabled)| !**enabled)
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let limits = document
        .limits
        .iter()
        .map(|(name, value)| format!("{name}\t{value}"))
        .collect::<Vec<_>>();

    [
        capability_group(
            "protocol version",
            std::slice::from_ref(&document.protocol_version),
        ),
        capability_group("planes", &planes),
        capability_group("enabled features", &enabled),
        capability_group("disabled features", &disabled),
        capability_group("limits", &limits),
    ]
    .join("\n\n")
}

fn capability_group(heading: &str, rows: &[String]) -> String {
    let mut lines = vec![heading.to_owned()];
    if rows.is_empty() {
        lines.push("(none)".to_owned());
    } else {
        lines.extend_from_slice(rows);
    }
    lines.join("\n")
}

fn human_doctor(checks: &[DoctorCheck]) -> String {
    let mut lines = Vec::new();
    for check in checks {
        if check.status == DoctorStatus::Failed {
            if let Some(response) = &check.store_probe {
                let groups = store_probe_failure_group_lines(response);
                if !groups.is_empty() {
                    lines.push(format!("FAILED  {}", check.name));
                    lines.extend(groups.into_iter().map(|line| format!("        {line}")));
                    continue;
                }
            }
        }
        if check.status == DoctorStatus::Failed {
            lines.push(format!("{}: failed", check.name));
            let detail = single_line(&check.message);
            let request_id = request_id_suffix(check.request_id.as_deref());
            lines.push(format!("  detail: {detail}{request_id}"));
        } else {
            lines.push(format!(
                "{}: {}: {}",
                check.name,
                check.status.as_str(),
                single_line(&check.message)
            ));
        }
        if let Some(response) = &check.store_probe {
            lines.extend(
                response
                    .checks
                    .iter()
                    .map(|check| format!("  {}", store_probe_check_line(check))),
            );
        }
    }
    lines.join("\n")
}

fn single_line(message: &str) -> String {
    message.lines().collect::<Vec<_>>().join(" | ")
}

pub(crate) fn human_path_entry(entry: &loonfs_api::PathEntry) -> String {
    let size = entry
        .size_bytes()
        .map(|value: u64| value.to_string())
        .unwrap_or_else(|| "-".to_owned());
    format!(
        "{}\t{}\t{}\t{}\t{}",
        entry.inode_kind(),
        size,
        format_utc_ms(entry.created_at_ms),
        render_actor(&entry.created_by),
        entry.path
    )
}

fn render_actor(actor: &loonfs_api::ActorRef) -> String {
    format!("{}:{}", actor.kind.as_str(), actor.id)
}

/// One attribute value on one line.
fn render_attribute_value(value: &AttributeValue) -> String {
    escape_control_characters(value.as_str())
}

/// Escapes control characters and leaves every other character alone.
///
/// Attribute values are user-provided UTF-8. Escaping control characters
/// prevents newlines from creating extra output rows and prevents terminal
/// escape sequences from being interpreted. JSON output relies on serde's
/// normal string escaping instead.
fn escape_control_characters(value: &str) -> String {
    if !value.chars().any(char::is_control) {
        return value.to_owned();
    }
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_control() {
            escaped.extend(character.escape_default());
        } else {
            escaped.push(character);
        }
    }
    escaped
}
