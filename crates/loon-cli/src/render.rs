use crate::args::CommandKind;
use crate::commands::{CommandData, CommandFailure, CommandOutput};
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
            mode: output.mode.as_deref(),
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
        mode: failure.mode.as_deref(),
        data: None,
        error: Some(&failure.error),
    })
    .map_err(io::Error::other)
}

pub fn human_success(output: &CommandOutput) -> String {
    match &output.data {
        CommandData::Profile(profile) => toml::to_string_pretty(profile)
            .unwrap_or_else(|_| format!("mode = \"{}\"", profile.mode_str())),
        CommandData::ProfileSummary(profile) => match output.kind {
            CommandKind::ProfileRemove => format!("removed profile {}", profile.name),
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
                    .map(|value: u64| value.to_string())
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

#[cfg(test)]
mod tests {
    use super::{human_success, json_error};
    use crate::args::CommandKind;
    use crate::commands::{CommandData, CommandFailure, CommandOutput};
    use crate::error::CliError;
    use crate::profiles::ProfileSummary;
    use insta::{assert_json_snapshot, assert_snapshot};

    #[test]
    fn human_profile_list_snapshot() {
        let output = CommandOutput {
            kind: CommandKind::ProfileList,
            profile: None,
            mode: None,
            data: CommandData::ProfileList {
                default_profile: Some("default".to_owned()),
                profiles: vec![
                    ProfileSummary {
                        name: "default".to_owned(),
                        mode: "local".to_owned(),
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
    fn json_error_snapshot() {
        let failure = CommandFailure {
            kind: CommandKind::ConfigShow,
            profile: Some("default".to_owned()),
            mode: Some("remote".to_owned()),
            error: CliError::new("client_error", "connection refused"),
        };
        assert_json_snapshot!(serde_json::from_str::<serde_json::Value>(
            &json_error(&failure).unwrap()
        )
        .unwrap());
    }
}
