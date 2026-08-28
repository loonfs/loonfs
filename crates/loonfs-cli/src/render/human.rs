//! Human-readable rendering, including the intentionally exhaustive output match.

use super::summaries::*;
use super::*;

pub(crate) fn human_success(output: &CommandOutput) -> String {
    match &output.data {
        CommandData::Capabilities(document) => human_capabilities(document),
        CommandData::Doctor { checks } => human_doctor(checks),
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
        CommandData::NamespaceStatus(namespace) => format!(
            "{} @ seq {} (retention floor {})",
            namespace.namespace_id, namespace.head_seq.0, namespace.retention_floor_seq.0
        ),
        CommandData::NamespaceDeleted(response) => format!(
            "deleted {} (head_seq {})",
            response.namespace_id, response.head_seq.0
        ),
        CommandData::SnapshotCreated(snapshot) => format!(
            "snapshot {} created for {} @ seq {} (name {}, expires at {})",
            snapshot.snapshot_id,
            snapshot.namespace_id,
            snapshot.head_seq.0,
            snapshot.name,
            format_utc_ms(snapshot.expires_at_ms)
        ),
        CommandData::SnapshotsListed(response) => {
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
        CommandData::SnapshotExtended(snapshot) => format!(
            "snapshot {} in {} extended to {}",
            snapshot.snapshot_id,
            snapshot.namespace_id,
            format_utc_ms(snapshot.expires_at_ms)
        ),
        CommandData::SnapshotReleased(response) => format!(
            "snapshot {} in {} released or already gone",
            response.snapshot_id, response.namespace_id
        ),
        CommandData::CheckpointCreated(checkpoint) => {
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
        CommandData::CheckpointsListed(response) => {
            let mut lines = vec![
                format!("active checkpoints for {}", response.namespace_id),
                "CREATED\tEXPIRES\tSEQ\tOWNER\tCHECKPOINT".to_owned(),
            ];
            for checkpoint in &response.checkpoints {
                let (owner, expiry) = (
                    checkpoint_owner_label(&checkpoint.owner),
                    checkpoint
                        .expires_at_ms
                        .map_or_else(|| "-".to_owned(), format_utc_ms),
                );
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
        CommandData::CheckpointReleased(response) => format!(
            "checkpoint {} in {} released or already gone",
            response.checkpoint_id, response.namespace_id
        ),
        // One clause per action the step selected, and none for an action it
        // did not: the line says what ran, never what was skipped.
        CommandData::MaintenanceStepped(response) => {
            let mut clauses = Vec::new();
            if let Some(metadata) = &response.metadata_maintenance {
                clauses.push(wal_flush_summary(
                    &metadata.wal_flush,
                    response.status_before.wal_tail_segments,
                ));
                clauses.push(
                    match metadata.reorganize {
                        ReorganizeStepOutcome::NotNeeded => "reorganize not needed",
                        ReorganizeStepOutcome::UnitPublished => "reorganized one family group",
                        ReorganizeStepOutcome::CompactionStarted => {
                            "started a background compaction of one family group"
                        }
                        ReorganizeStepOutcome::CompactionAtCapacity => {
                            "queued a background compaction of one family group behind the \
                             server's compaction limit"
                        }
                        ReorganizeStepOutcome::CompactionRunning => {
                            "one family group is waiting on a background compaction"
                        }
                        ReorganizeStepOutcome::CompactionRequired => {
                            "one family group needs a compaction this server will not run on its \
                             own"
                        }
                        ReorganizeStepOutcome::RootAdvanced => {
                            "another publisher moved the metadata root, so the reorganize \
                             published nothing"
                        }
                    }
                    .to_owned(),
                );
            }
            if let Some(retention) = &response.retention {
                clauses.push(
                    if retention.retention_floor_seq > response.status_before.retention_floor_seq {
                        format!(
                            "retention floor advanced to seq {}",
                            retention.retention_floor_seq.0
                        )
                    } else {
                        format!(
                            "retention floor unchanged at seq {}",
                            retention.retention_floor_seq.0
                        )
                    },
                );
            }
            if let Some(gc) = &response.gc {
                clauses.push(gc_summary(gc));
            }
            format!(
                "maintenance step for {}: {}",
                response.namespace_id,
                clauses.join("; ")
            )
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
        CommandData::Trash(listing) => {
            let response = &listing.response;
            let mut lines = vec![
                format!(
                    "trash for {} (head seq {})",
                    response.namespace_id, response.head_seq.0
                ),
                "DELETED\tDELETED_BY\tNAME\tINODE\tSEQ\tRECOVER".to_owned(),
            ];
            // The commands were built one per entry, in this order.
            for (entry, recovery_command) in response.entries.iter().zip(&listing.recovery_commands)
            {
                let name = entry
                    .deleted_binding
                    .as_ref()
                    .map(|binding| binding.display_name.as_str().to_owned())
                    .unwrap_or_else(|| "-".to_owned());
                lines.push(format!(
                    "{}\t{}\t{}\t{}\t{}\t{recovery_command}",
                    format_utc_ms(entry.deleted_at_ms),
                    render_actor(&entry.deleted_by),
                    name,
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
        CommandData::PathEntries {
            entries,
            next_cursor,
            ..
        } => {
            let mut lines: Vec<String> = entries.iter().map(human_path_entry).collect();
            if let Some(cursor) = next_cursor {
                lines.push(more_entries_hint(cursor));
            }
            lines.join("\n")
        }
        CommandData::GrepIndexEnabled {
            response,
            waited_for_seq,
            steps,
            budget_exhausted,
        } => {
            let opening = format!("grep index enabled on {}", response.namespace_id);
            if *budget_exhausted {
                let target = waited_for_seq
                    .map_or_else(|| "its target".to_owned(), |seq| format!("seq {}", seq.0));
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
        CommandData::MaintenanceHosted { namespaces, jobs } => format!(
            "hosted {}; stopped on signal",
            maintenance_assignment(namespaces, jobs)
        ),
        CommandData::MaintenanceDrained {
            namespaces,
            jobs,
            keys,
            steps,
            budget_exhausted,
        } => {
            let assignment = maintenance_assignment(namespaces, jobs);
            let settled = keys.iter().filter(|key| key.settled).count();
            let mut lines: Vec<String> = keys.iter().map(maintenance_key_line).collect();
            lines.push(if *budget_exhausted {
                format!(
                    "gave up on {assignment}: {settled} of {} keys settled after {}",
                    keys.len(),
                    steps_phrase(*steps)
                )
            } else {
                format!(
                    "drained {assignment}: {settled} keys settled after {}",
                    steps_phrase(*steps)
                )
            });
            lines.join("\n")
        }
        CommandData::StoreProbed(response) => store_probe_report_lines(response).join("\n"),
        CommandData::GrepIndexDisabled(response) => {
            format!("grep index disabled on {}", response.namespace_id)
        }
        CommandData::GrepIndexStatus(response) => {
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
        CommandData::GrepIndexCollected(response) => {
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
        CommandData::GrepMatches {
            pattern,
            matches,
            tail_scanned,
            truncated,
            next_cursor,
            ..
        } => {
            let mut lines: Vec<String> = matches
                .iter()
                .map(|found| format!("{}:{}:{}", found.path, found.line_number, found.line))
                .collect();
            if *truncated {
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
        CommandData::PathEntry(entry) => {
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
            // One line per attribute, after the fields that say what the item
            // is. An inode holding no attributes prints nothing extra: the
            // header already answered the question that was asked.
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
        CommandData::FileRevisions {
            target,
            revisions,
            next_cursor,
        } => {
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
        CommandData::TreeTransfer {
            source,
            destination,
            files,
            directories,
            failures,
            ..
        } => {
            let verb = match output.kind {
                CommandKind::FilesystemGet => "downloaded",
                CommandKind::FilesystemCp => "copied",
                _ => "stored",
            };
            let mut lines = Vec::new();
            for failure in failures {
                let rendered = human_error(&failure.error);
                lines.push(format!("failed {}: {rendered}", failure.path));
            }
            let directory_noun = if *directories == 1 {
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
        } => match output.kind {
            CommandKind::FilesystemPut => {
                format!("stored {target} @ seq {committed_seq} (commit {commit_id})")
            }
            CommandKind::FilesystemRm => match recovery_command {
                Some(recovery_command) => format!(
                    "removed {target} @ seq {committed_seq} (commit {commit_id}); \
                     recover with `{recovery_command}`"
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
        },
        CommandData::DirectoryAlreadyExists { target, .. } => {
            format!("{target} is already a directory")
        }
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
        CommandData::ConfigPath {
            path,
            source,
            preferred_path,
        } => {
            let chosen = match (source, preferred_path) {
                (ConfigSource::Flag, _) => "from --config".to_owned(),
                (ConfigSource::Env, _) => "from LOONFS_CONFIG".to_owned(),
                (ConfigSource::Xdg, _) => "from XDG_CONFIG_HOME".to_owned(),
                // The migration case: say where the file belongs, since
                // moving it there is what makes this note go away.
                (ConfigSource::Legacy, Some(preferred)) => {
                    format!("legacy location; move it to {preferred} once convenient")
                }
                (ConfigSource::Legacy, None) => "default location".to_owned(),
            };
            format!("{path} ({chosen})")
        }
        CommandData::ConfigShow { config } => {
            toml::to_string_pretty(config).unwrap_or_else(|_| "failed to render config".to_owned())
        }
        CommandData::ConfigShowDegraded { error, config_toml } => {
            format!(
                "warning: {error}\nshowing the file as parsed, secrets masked:\n\n{config_toml}"
            )
        }
        CommandData::Version { version } => version.clone(),
        CommandData::CompletionScript(_)
        | CommandData::StreamBytes(_)
        | CommandData::StreamedToStdout => String::new(),
    }
}

fn human_capabilities(document: &loonfs_api::CapabilityDocument) -> String {
    let mut profiles = document.profiles.clone();
    profiles.sort();

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
        capability_group("profiles", &profiles),
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
