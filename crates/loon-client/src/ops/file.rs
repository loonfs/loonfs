use super::OpsConfig;
use anyhow::{bail, Context, Result};
use loon_types::server::{AuthoritativeFileBytes, AuthoritativePathEntry, ServerTransport};
use loon_types::NamespaceId;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileCommand {
    Ls {
        config_path: PathBuf,
        selector: String,
    },
    Stat {
        config_path: PathBuf,
        selector: String,
    },
    Get {
        config_path: PathBuf,
        selector: String,
        local_path: PathBuf,
    },
    Cat {
        config_path: PathBuf,
        selector: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileCommandOutput {
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoritativePathSelector {
    pub namespace_id: NamespaceId,
    pub absolute_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileGetReport {
    source: String,
    written_to: String,
    size_bytes: u64,
}

#[derive(Debug, Error)]
enum FileGetError {
    #[error("download target already exists: `{path}`")]
    TargetAlreadyExists { path: String },
    #[error("download target parent directory is missing: `{path}`")]
    TargetParentMissing { path: String },
    #[error("download target has no parent directory: `{path}`")]
    TargetParentUnavailable { path: String },
    #[error("failed local download write during `{operation}` for `{path}`: {source}")]
    LocalWrite {
        operation: &'static str,
        path: String,
        source: std::io::Error,
    },
    #[error("download source is not a file selector: `{selector}`")]
    SourceNotFile { selector: String },
}

pub fn parse_authoritative_path_selector(selector: &str) -> Result<AuthoritativePathSelector> {
    let Some((namespace, path)) = selector.split_once(':') else {
        bail!("invalid authoritative selector `{selector}`: expected <namespace>:/absolute/path");
    };
    if namespace.trim().is_empty() || !path.starts_with('/') {
        bail!("invalid authoritative selector `{selector}`: expected <namespace>:/absolute/path");
    }

    let mut components = Vec::new();
    for component in path.split('/') {
        if component.is_empty() {
            continue;
        }
        if component == "." || component == ".." {
            bail!("invalid authoritative selector `{selector}`: `.` and `..` are not allowed");
        }
        components.push(component);
    }

    let absolute_path = if components.is_empty() {
        "/".to_owned()
    } else {
        format!("/{}", components.join("/"))
    };

    Ok(AuthoritativePathSelector {
        namespace_id: NamespaceId::from(namespace.to_owned()),
        absolute_path,
    })
}

pub fn run_file_command<T: ServerTransport>(
    command: FileCommand,
    transport_factory: &impl Fn(&OpsConfig) -> Result<T>,
) -> Result<FileCommandOutput> {
    match command {
        FileCommand::Ls {
            config_path,
            selector,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let transport = transport_factory(&config)?;
            let selector = parse_authoritative_path_selector(&selector)?;
            let entries = transport
                .list_path(&selector.namespace_id, &selector.absolute_path)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let rendered = entries
                .into_iter()
                .map(render_ls_entry)
                .collect::<Vec<_>>()
                .join("\n");
            Ok(FileCommandOutput::Text(if rendered.is_empty() {
                String::new()
            } else {
                format!("{rendered}\n")
            }))
        }
        FileCommand::Stat {
            config_path,
            selector,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let transport = transport_factory(&config)?;
            let selector = parse_authoritative_path_selector(&selector)?;
            let entry = transport
                .resolve_path(&selector.namespace_id, &selector.absolute_path)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok(FileCommandOutput::Text(
                serde_yaml::to_string(&entry).context("render authoritative file stat")?,
            ))
        }
        FileCommand::Get {
            config_path,
            selector,
            local_path,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let transport = transport_factory(&config)?;
            let selector_str = selector.clone();
            let selector = parse_authoritative_path_selector(&selector)?;
            let read = transport
                .read_file_bytes(&selector.namespace_id, &selector.absolute_path)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            if read.entry.inode_kind != loon_types::InodeKind::File {
                return Err(FileGetError::SourceNotFile {
                    selector: selector_str,
                }
                .into());
            }
            let target_path = resolve_download_target(&read, &local_path)?;
            write_downloaded_file(&target_path, &read.bytes)?;
            let written_to = fs::canonicalize(&target_path)
                .unwrap_or(target_path.clone())
                .display()
                .to_string();
            let report = FileGetReport {
                source: selector_str,
                written_to,
                size_bytes: read.bytes.len() as u64,
            };
            Ok(FileCommandOutput::Text(
                serde_yaml::to_string(&report).context("render file get report")?,
            ))
        }
        FileCommand::Cat {
            config_path,
            selector,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let transport = transport_factory(&config)?;
            let selector = parse_authoritative_path_selector(&selector)?;
            let read = transport
                .read_file_bytes(&selector.namespace_id, &selector.absolute_path)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok(FileCommandOutput::Bytes(read.bytes))
        }
    }
}

fn render_ls_entry(entry: AuthoritativePathEntry) -> String {
    match entry.inode_kind {
        loon_types::InodeKind::Dir => format!("{}/", entry.display_name),
        loon_types::InodeKind::File
        | loon_types::InodeKind::Symlink
        | loon_types::InodeKind::Mount => entry.display_name,
    }
}

fn resolve_download_target(
    read: &AuthoritativeFileBytes,
    local_path: &Path,
) -> Result<PathBuf, FileGetError> {
    if local_path.is_dir() {
        return Ok(local_path.join(&read.entry.display_name));
    }
    Ok(local_path.to_path_buf())
}

fn write_downloaded_file(target_path: &Path, bytes: &[u8]) -> Result<(), FileGetError> {
    if target_path.exists() {
        return Err(FileGetError::TargetAlreadyExists {
            path: target_path.display().to_string(),
        });
    }
    let parent = target_path
        .parent()
        .ok_or_else(|| FileGetError::TargetParentUnavailable {
            path: target_path.display().to_string(),
        })?;
    if !parent.is_dir() {
        return Err(FileGetError::TargetParentMissing {
            path: parent.display().to_string(),
        });
    }

    let stage_path = parent.join(format!(
        ".loon-get-stage-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    ));
    let mut stage_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stage_path)
        .map_err(|source| FileGetError::LocalWrite {
            operation: "create_stage_file",
            path: stage_path.display().to_string(),
            source,
        })?;
    stage_file
        .write_all(bytes)
        .map_err(|source| FileGetError::LocalWrite {
            operation: "write_stage_file",
            path: stage_path.display().to_string(),
            source,
        })?;
    stage_file
        .sync_all()
        .map_err(|source| FileGetError::LocalWrite {
            operation: "sync_stage_file",
            path: stage_path.display().to_string(),
            source,
        })?;
    drop(stage_file);
    fs::rename(&stage_path, target_path).map_err(|source| FileGetError::LocalWrite {
        operation: "rename_stage_file",
        path: target_path.display().to_string(),
        source,
    })?;
    Ok(())
}
