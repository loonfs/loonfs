use crate::args::CommandKind;
use crate::commands::{CommandData, CommandFailure, CommandOutput};
use crate::config::ProfileMode;
use crate::error::CliError;
use serde::Serialize;
use std::io::{self, Write};

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

pub fn render_success(output: &CommandOutput, json_mode: bool) -> io::Result<()> {
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

pub fn render_error(failure: &CommandFailure, json_mode: bool) -> io::Result<()> {
    let mut stderr = io::stderr().lock();
    if json_mode {
        let body = json_error(failure)?;
        stderr.write_all(body.as_bytes())?;
        stderr.write_all(b"\n")?;
    } else {
        stderr.write_all(failure.error.message.as_bytes())?;
        stderr.write_all(b"\n")?;
    }
    Ok(())
}

pub fn json_success(output: &CommandOutput) -> io::Result<String> {
    match &output.data {
        CommandData::StreamBytes(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "streaming output does not support json rendering",
        )),
        data => serde_json::to_string_pretty(&JsonEnvelope {
            kind: output.kind.as_str(),
            format_version: FORMAT_VERSION,
            profile: output.profile.as_deref(),
            mode: output.mode.map(profile_mode_str),
            data: Some(data),
            error: None,
        })
        .map_err(io::Error::other),
    }
}

pub fn json_error(failure: &CommandFailure) -> io::Result<String> {
    serde_json::to_string_pretty(&JsonEnvelope::<serde_json::Value> {
        kind: failure.kind.as_str(),
        format_version: FORMAT_VERSION,
        profile: failure.profile.as_deref(),
        mode: failure.mode.map(profile_mode_str),
        data: None,
        error: Some(&failure.error),
    })
    .map_err(io::Error::other)
}

pub fn human_success(output: &CommandOutput) -> String {
    match &output.data {
        CommandData::Profile(profile) => toml::to_string_pretty(profile).unwrap_or_else(|_| {
            format!(
                "name = \"{}\"\nmode = \"{}\"",
                profile.name,
                profile_mode_str(profile.mode)
            )
        }),
        CommandData::ProfileSummary(profile) => match output.kind {
            CommandKind::ProfileRemove => format!("removed profile {}", profile.name),
            _ => format!("{} ({})", profile.name, profile_mode_str(profile.mode)),
        },
        CommandData::ProfileList { profiles } => profiles
            .iter()
            .map(|profile| {
                format!(
                    "{} {}\t{}",
                    if profile.active { "*" } else { "-" },
                    profile.name,
                    profile_mode_str(profile.mode)
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
        CommandData::LocalStatus(status) => {
            let mut lines = vec![
                format!("profile: {}", status.profile_name),
                format!("status: {}", local_status_str(status.status)),
                format!("server_url: {}", status.server_url),
                format!("server_config_path: {}", status.server_config_path),
            ];
            if let Some(pid) = status.pid {
                lines.push(format!("pid: {pid}"));
            }
            lines.join("\n")
        }
        CommandData::NamespaceSummary(namespace) => namespace.name.to_string(),
        CommandData::NamespaceList { namespaces } => namespaces
            .iter()
            .map(|namespace| namespace.name.to_string())
            .collect::<Vec<_>>()
            .join("\n"),
        CommandData::PathEntries { entries } => entries
            .iter()
            .map(|entry| {
                let size = entry
                    .size_bytes
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_owned());
                format!("{:?}\t{}\t{}", entry.inode_kind, size, entry.absolute_path)
            })
            .collect::<Vec<_>>()
            .join("\n"),
        CommandData::PathEntry(entry) => {
            let mut lines = vec![
                format!("path: {}", entry.absolute_path),
                format!("inode: {}", entry.inode_id),
                format!("kind: {:?}", entry.inode_kind),
                format!("seq: {}", entry.authoritative_head_seq.0),
            ];
            if let Some(size) = entry.size_bytes {
                lines.push(format!("size: {size}"));
            }
            if let Some(revision) = entry.revision_no {
                lines.push(format!("revision: {}", revision.0));
            }
            if let Some(manifest) = &entry.content_manifest_digest {
                lines.push(format!("content_manifest: {manifest}"));
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
        } => match output.kind {
            CommandKind::FilesystemPut => format!("stored {target} @ seq {committed_seq}"),
            CommandKind::FilesystemRm => format!("removed {target} @ seq {committed_seq}"),
            _ => format!("{target} @ seq {committed_seq}"),
        },
        CommandData::PathMove {
            from,
            to,
            committed_seq,
        } => match output.kind {
            CommandKind::FilesystemMv => format!("moved {from} -> {to} @ seq {committed_seq}"),
            CommandKind::FilesystemCp => format!("copied {from} -> {to} @ seq {committed_seq}"),
            _ => format!("{from} -> {to} @ seq {committed_seq}"),
        },
        CommandData::ConfigPath { path } => path.clone(),
        CommandData::ConfigShow { config } => {
            toml::to_string_pretty(config).unwrap_or_else(|_| "failed to render config".to_owned())
        }
        CommandData::Version { version } => version.clone(),
        CommandData::StreamBytes(_) => String::new(),
    }
}

fn profile_mode_str(mode: ProfileMode) -> &'static str {
    match mode {
        ProfileMode::Local => "local",
        ProfileMode::Remote => "remote",
    }
}

fn local_status_str(status: crate::local_runtime::LocalServerStatusKind) -> &'static str {
    match status {
        crate::local_runtime::LocalServerStatusKind::Running => "running",
        crate::local_runtime::LocalServerStatusKind::Stale => "stale",
        crate::local_runtime::LocalServerStatusKind::Stopped => "stopped",
    }
}

#[cfg(test)]
mod tests {
    use super::{human_success, json_error, json_success};
    use crate::args::CommandKind;
    use crate::commands::{CommandData, CommandFailure, CommandOutput};
    use crate::config::{ProfileMode, RedactedCliConfig};
    use crate::error::CliError;
    use crate::local_runtime::{LocalServerStatus, LocalServerStatusKind};
    use crate::profiles::{ProfileSummary, ProfileView};
    use insta::{assert_json_snapshot, assert_snapshot};
    use std::collections::BTreeMap;

    #[test]
    fn human_profile_list_snapshot() {
        let output = CommandOutput {
            kind: CommandKind::ProfileList,
            profile: None,
            mode: None,
            data: CommandData::ProfileList {
                profiles: vec![
                    ProfileSummary {
                        name: "alpha".to_owned(),
                        mode: ProfileMode::Local,
                        active: true,
                    },
                    ProfileSummary {
                        name: "beta".to_owned(),
                        mode: ProfileMode::Remote,
                        active: false,
                    },
                ],
            },
        };
        assert_snapshot!(human_success(&output));
    }

    #[test]
    fn json_local_status_snapshot() {
        let output = CommandOutput {
            kind: CommandKind::LocalStatus,
            profile: Some("alpha".to_owned()),
            mode: Some(ProfileMode::Local),
            data: CommandData::LocalStatus(LocalServerStatus {
                profile_name: "alpha".to_owned(),
                status: LocalServerStatusKind::Running,
                server_url: "http://127.0.0.1:9400".to_owned(),
                server_config_path: "/tmp/loond.toml".to_owned(),
                pid: Some(42),
            }),
        };
        assert_json_snapshot!(serde_json::from_str::<serde_json::Value>(
            &json_success(&output).unwrap()
        )
        .unwrap());
    }

    #[test]
    fn json_error_snapshot() {
        let failure = CommandFailure {
            kind: CommandKind::ConfigShow,
            profile: Some("alpha".to_owned()),
            mode: Some(ProfileMode::Remote),
            error: CliError::new("client_error", "connection refused"),
        };
        assert_json_snapshot!(serde_json::from_str::<serde_json::Value>(
            &json_error(&failure).unwrap()
        )
        .unwrap());
    }

    #[test]
    fn human_profile_show_snapshot() {
        let output = CommandOutput {
            kind: CommandKind::ProfileShow,
            profile: Some("alpha".to_owned()),
            mode: Some(ProfileMode::Remote),
            data: CommandData::Profile(ProfileView {
                name: "alpha".to_owned(),
                mode: ProfileMode::Remote,
                active: true,
                local: None,
                remote: Some(crate::config::RedactedRemoteProfileConfig {
                    server_url: "https://example.com".to_owned(),
                    auth_token: Some("REDACTED".to_owned()),
                }),
            }),
        };
        assert_snapshot!(human_success(&output));
    }

    #[test]
    fn human_config_show_snapshot() {
        let output = CommandOutput {
            kind: CommandKind::ConfigShow,
            profile: None,
            mode: None,
            data: CommandData::ConfigShow {
                config: RedactedCliConfig {
                    config_version: 1,
                    active_profile: Some("alpha".to_owned()),
                    profiles: BTreeMap::new(),
                },
            },
        };
        assert_snapshot!(human_success(&output));
    }
}
