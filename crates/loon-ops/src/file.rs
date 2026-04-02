use crate::OpsConfig;
use anyhow::{bail, Context, Result};
use loon_core::content::read_durable_content_bytes;
use loon_server::mutation::ClientMutationExecutionParams;
use loon_server::ops::{
    collect_authoritative_subtree_download, cp_authoritative_file_within_namespace,
    cp_replace_authoritative_file_within_namespace, list_authoritative_path,
    mkdir_authoritative_path, mv_authoritative_path, put_authoritative_file_from_path,
    put_authoritative_subtree_from_path, read_authoritative_file_bytes,
    replace_authoritative_file_from_path, resolve_authoritative_path, rm_authoritative_path,
    AuthoritativeFileBytes, AuthoritativePathEntry, AuthoritativeRecursivePutResult,
    AuthoritativeSubtreeDownload,
};
use loon_types::NamespaceId;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
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
        recursive: bool,
    },
    Cat {
        config_path: PathBuf,
        selector: String,
    },
    Put {
        config_path: PathBuf,
        local_path: PathBuf,
        selector: String,
        replace: bool,
        recursive: bool,
    },
    Mkdir {
        config_path: PathBuf,
        selector: String,
    },
    Rm {
        config_path: PathBuf,
        selector: String,
        recursive: bool,
    },
    Mv {
        config_path: PathBuf,
        from_selector: String,
        to_selector: String,
    },
    Cp {
        config_path: PathBuf,
        from_selector: String,
        to_selector: String,
        replace: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileCommandOutput {
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AuthoritativePathSelector {
    pub namespace_id: NamespaceId,
    pub absolute_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileGetReport {
    source: String,
    written_to: String,
    size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileGetRecursiveReport {
    source: String,
    written_to: String,
    recursive: bool,
    file_count: u64,
    directory_count: u64,
    total_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FilePutReport {
    source_local_path: String,
    destination: String,
    replace: bool,
    entry: AuthoritativePathEntry,
    committed_seq: ChangeSeqReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FilePutRecursiveReport {
    source_local_path: String,
    destination: String,
    recursive: bool,
    directory_count: u64,
    file_count: u64,
    total_size_bytes: u64,
    root_entry: AuthoritativePathEntry,
    committed_seq: ChangeSeqReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileMkdirReport {
    selector: String,
    entry: AuthoritativePathEntry,
    committed_seq: ChangeSeqReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileRmReport {
    selector: String,
    recursive: bool,
    deleted_path: String,
    inode_id: String,
    inode_kind: loon_types::InodeKind,
    committed_seq: ChangeSeqReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileMvReport {
    from: String,
    to: String,
    inode_id: String,
    inode_kind: loon_types::InodeKind,
    committed_seq: ChangeSeqReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FileCpReport {
    from: String,
    to: String,
    replace: bool,
    entry: AuthoritativePathEntry,
    committed_seq: ChangeSeqReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ChangeSeqReport {
    seq: u64,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecursiveGetWriteResult {
    written_to: String,
    file_count: u64,
    directory_count: u64,
    total_size_bytes: u64,
}

#[derive(Debug, Error)]
enum FilePutLocalSourceError {
    #[error("upload source path does not exist: `{path}`")]
    Missing { path: String },
    #[error("upload source must be a regular file: `{path}`")]
    NotRegularFile { path: String },
    #[error("recursive upload source must be a directory: `{path}`")]
    NotDirectory { path: String },
    #[error("failed to inspect upload source `{path}`: {source}")]
    ReadMetadata {
        path: String,
        source: std::io::Error,
    },
}

#[derive(Debug, Error)]
enum FilePutCommandError {
    #[error("file put flags `--recursive` and `--replace` are mutually exclusive")]
    RecursiveReplaceConflict,
}

#[derive(Debug, Error)]
enum FilePathPairSelectorError {
    #[error(
        "authoritative path command requires both selectors in one namespace: from `{from_selector}` to `{to_selector}`"
    )]
    CrossNamespace {
        from_selector: String,
        to_selector: String,
    },
}

pub(crate) fn parse_authoritative_path_selector(
    selector: &str,
) -> Result<AuthoritativePathSelector> {
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

pub fn run_file_command(command: FileCommand) -> Result<FileCommandOutput> {
    match command {
        FileCommand::Ls {
            config_path,
            selector,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let selector = parse_authoritative_path_selector(&selector)?;
            let entries =
                list_authoritative_path(&store, &selector.namespace_id, &selector.absolute_path)?;
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
            let store = config.open_store()?;
            let selector = parse_authoritative_path_selector(&selector)?;
            let entry = resolve_authoritative_path(
                &store,
                &selector.namespace_id,
                &selector.absolute_path,
            )?;
            Ok(FileCommandOutput::Text(
                serde_yaml::to_string(&entry).context("render authoritative file stat")?,
            ))
        }
        FileCommand::Get {
            config_path,
            selector,
            local_path,
            recursive,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let selector_str = selector.clone();
            let selector = parse_authoritative_path_selector(&selector)?;
            if recursive {
                let subtree = collect_authoritative_subtree_download(
                    &store,
                    &selector.namespace_id,
                    &selector.absolute_path,
                )?;
                let result = write_downloaded_subtree(
                    &store,
                    &selector.namespace_id,
                    &subtree,
                    &local_path,
                )?;
                let report = FileGetRecursiveReport {
                    source: selector_str,
                    written_to: result.written_to,
                    recursive: true,
                    file_count: result.file_count,
                    directory_count: result.directory_count,
                    total_size_bytes: result.total_size_bytes,
                };
                Ok(FileCommandOutput::Text(
                    serde_yaml::to_string(&report).context("render recursive file get report")?,
                ))
            } else {
                let read = read_authoritative_file_bytes(
                    &store,
                    &selector.namespace_id,
                    &selector.absolute_path,
                )?;
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
        }
        FileCommand::Cat {
            config_path,
            selector,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let selector = parse_authoritative_path_selector(&selector)?;
            let read = read_authoritative_file_bytes(
                &store,
                &selector.namespace_id,
                &selector.absolute_path,
            )?;
            Ok(FileCommandOutput::Bytes(read.bytes))
        }
        FileCommand::Put {
            config_path,
            local_path,
            selector,
            replace,
            recursive,
        } => {
            if recursive && replace {
                return Err(FilePutCommandError::RecursiveReplaceConflict.into());
            }
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let selector = parse_authoritative_path_selector(&selector)?;
            if recursive {
                ensure_put_source_is_directory(&local_path)?;
                let result = put_authoritative_subtree_from_path(
                    &store,
                    &selector.namespace_id,
                    &local_path,
                    &selector.absolute_path,
                    &mutation_params(&config),
                )?;
                Ok(FileCommandOutput::Text(render_recursive_put_report(
                    &local_path,
                    &selector,
                    result,
                )?))
            } else {
                ensure_put_source_is_regular_file(&local_path)?;
                let result = if replace {
                    replace_authoritative_file_from_path(
                        &store,
                        &selector.namespace_id,
                        &local_path,
                        &selector.absolute_path,
                        &mutation_params(&config),
                    )?
                } else {
                    put_authoritative_file_from_path(
                        &store,
                        &selector.namespace_id,
                        &local_path,
                        &selector.absolute_path,
                        &mutation_params(&config),
                    )?
                };
                let report = FilePutReport {
                    source_local_path: local_path.display().to_string(),
                    destination: format_selector(&selector),
                    replace,
                    entry: result.entry,
                    committed_seq: ChangeSeqReport {
                        seq: result.committed_seq.0,
                    },
                };
                Ok(FileCommandOutput::Text(
                    serde_yaml::to_string(&report).context("render file put report")?,
                ))
            }
        }
        FileCommand::Mkdir {
            config_path,
            selector,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let selector = parse_authoritative_path_selector(&selector)?;
            let result = mkdir_authoritative_path(
                &store,
                &selector.namespace_id,
                &selector.absolute_path,
                &mutation_params(&config),
            )?;
            let report = FileMkdirReport {
                selector: format_selector(&selector),
                entry: result.entry,
                committed_seq: ChangeSeqReport {
                    seq: result.committed_seq.0,
                },
            };
            Ok(FileCommandOutput::Text(
                serde_yaml::to_string(&report).context("render file mkdir report")?,
            ))
        }
        FileCommand::Rm {
            config_path,
            selector,
            recursive,
        } => {
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let selector = parse_authoritative_path_selector(&selector)?;
            let result = rm_authoritative_path(
                &store,
                &selector.namespace_id,
                &selector.absolute_path,
                recursive,
                &mutation_params(&config),
            )?;
            let report = FileRmReport {
                selector: format_selector(&selector),
                recursive,
                deleted_path: result.absolute_path,
                inode_id: result.inode_id.0.to_string(),
                inode_kind: result.inode_kind,
                committed_seq: ChangeSeqReport {
                    seq: result.committed_seq.0,
                },
            };
            Ok(FileCommandOutput::Text(
                serde_yaml::to_string(&report).context("render file rm report")?,
            ))
        }
        FileCommand::Mv {
            config_path,
            from_selector,
            to_selector,
        } => {
            let from = parse_authoritative_path_selector(&from_selector)?;
            let to = parse_authoritative_path_selector(&to_selector)?;
            ensure_same_namespace(&from, &to, &from_selector, &to_selector)?;
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let result = mv_authoritative_path(
                &store,
                &from.namespace_id,
                &from.absolute_path,
                &to.absolute_path,
                &mutation_params(&config),
            )?;
            let report = FileMvReport {
                from: from_selector,
                to: to_selector,
                inode_id: result.inode_id.0.to_string(),
                inode_kind: result.inode_kind,
                committed_seq: ChangeSeqReport {
                    seq: result.committed_seq.0,
                },
            };
            Ok(FileCommandOutput::Text(
                serde_yaml::to_string(&report).context("render file mv report")?,
            ))
        }
        FileCommand::Cp {
            config_path,
            from_selector,
            to_selector,
            replace,
        } => {
            let from = parse_authoritative_path_selector(&from_selector)?;
            let to = parse_authoritative_path_selector(&to_selector)?;
            ensure_same_namespace(&from, &to, &from_selector, &to_selector)?;
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let result = if replace {
                cp_replace_authoritative_file_within_namespace(
                    &store,
                    &from.namespace_id,
                    &from.absolute_path,
                    &to.absolute_path,
                    &mutation_params(&config),
                )?
            } else {
                cp_authoritative_file_within_namespace(
                    &store,
                    &from.namespace_id,
                    &from.absolute_path,
                    &to.absolute_path,
                    &mutation_params(&config),
                )?
            };
            let report = FileCpReport {
                from: from_selector,
                to: to_selector,
                replace,
                entry: result.entry,
                committed_seq: ChangeSeqReport {
                    seq: result.committed_seq.0,
                },
            };
            Ok(FileCommandOutput::Text(
                serde_yaml::to_string(&report).context("render file cp report")?,
            ))
        }
    }
}

fn mutation_params(config: &OpsConfig) -> ClientMutationExecutionParams {
    ClientMutationExecutionParams {
        writer_id: config.server.writer_id.clone(),
        writer_version: config.server.writer_version.clone(),
        now_ms: config.now_ms(),
        lease_duration_ms: config.server.lease_duration_ms,
    }
}

fn ensure_put_source_is_regular_file(local_path: &Path) -> Result<(), FilePutLocalSourceError> {
    let metadata = fs::metadata(local_path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            FilePutLocalSourceError::Missing {
                path: local_path.display().to_string(),
            }
        } else {
            FilePutLocalSourceError::ReadMetadata {
                path: local_path.display().to_string(),
                source,
            }
        }
    })?;
    if !metadata.is_file() {
        return Err(FilePutLocalSourceError::NotRegularFile {
            path: local_path.display().to_string(),
        });
    }
    Ok(())
}

fn ensure_put_source_is_directory(local_path: &Path) -> Result<(), FilePutLocalSourceError> {
    let metadata = fs::metadata(local_path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            FilePutLocalSourceError::Missing {
                path: local_path.display().to_string(),
            }
        } else {
            FilePutLocalSourceError::ReadMetadata {
                path: local_path.display().to_string(),
                source,
            }
        }
    })?;
    if !metadata.is_dir() {
        return Err(FilePutLocalSourceError::NotDirectory {
            path: local_path.display().to_string(),
        });
    }
    Ok(())
}

fn ensure_same_namespace(
    from: &AuthoritativePathSelector,
    to: &AuthoritativePathSelector,
    from_selector: &str,
    to_selector: &str,
) -> Result<(), FilePathPairSelectorError> {
    if from.namespace_id != to.namespace_id {
        return Err(FilePathPairSelectorError::CrossNamespace {
            from_selector: from_selector.to_owned(),
            to_selector: to_selector.to_owned(),
        });
    }
    Ok(())
}

fn format_selector(selector: &AuthoritativePathSelector) -> String {
    format!(
        "{}:{}",
        selector.namespace_id.as_str(),
        selector.absolute_path
    )
}

fn render_recursive_put_report(
    local_path: &Path,
    selector: &AuthoritativePathSelector,
    result: AuthoritativeRecursivePutResult,
) -> Result<String> {
    let report = FilePutRecursiveReport {
        source_local_path: local_path.display().to_string(),
        destination: format_selector(selector),
        recursive: true,
        directory_count: result.directory_count,
        file_count: result.file_count,
        total_size_bytes: result.total_size_bytes,
        root_entry: result.root_entry,
        committed_seq: ChangeSeqReport {
            seq: result.committed_seq.0,
        },
    };
    serde_yaml::to_string(&report).context("render recursive file put report")
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

fn write_downloaded_subtree<S: loon_objectstore::ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    subtree: &AuthoritativeSubtreeDownload,
    destination_root: &Path,
) -> Result<RecursiveGetWriteResult, FileGetError> {
    if destination_root.exists() {
        return Err(FileGetError::TargetAlreadyExists {
            path: destination_root.display().to_string(),
        });
    }
    let parent =
        destination_root
            .parent()
            .ok_or_else(|| FileGetError::TargetParentUnavailable {
                path: destination_root.display().to_string(),
            })?;
    if !parent.is_dir() {
        return Err(FileGetError::TargetParentMissing {
            path: parent.display().to_string(),
        });
    }

    let stage_root = parent.join(format!(
        ".loon-get-tree-stage-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos()
    ));

    let result = (|| -> Result<RecursiveGetWriteResult, FileGetError> {
        fs::create_dir(&stage_root).map_err(|source| FileGetError::LocalWrite {
            operation: "create_stage_root",
            path: stage_root.display().to_string(),
            source,
        })?;

        for directory in &subtree.directories {
            let directory_path = stage_root.join(&directory.relative_path);
            fs::create_dir(&directory_path).map_err(|source| FileGetError::LocalWrite {
                operation: "create_stage_directory",
                path: directory_path.display().to_string(),
                source,
            })?;
        }

        let mut total_size_bytes = 0u64;
        for file in &subtree.files {
            let read =
                read_durable_content_bytes(store, namespace_id, &file.content_manifest_digest)
                    .map_err(anyhow::Error::from)
                    .context(format!(
                        "read immutable content for recursive get `{}`",
                        file.absolute_path
                    ))
                    .map_err(|source| FileGetError::LocalWrite {
                        operation: "read_recursive_source",
                        path: file.absolute_path.clone(),
                        source: std::io::Error::other(source.to_string()),
                    })?;
            let target_path = stage_root.join(&file.relative_path);
            let parent = target_path
                .parent()
                .expect("recursive staged file should have a parent");
            let stage_file_path = parent.join(format!(
                ".loon-get-file-stage-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock after epoch")
                    .as_nanos()
            ));
            let mut stage_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&stage_file_path)
                .map_err(|source| FileGetError::LocalWrite {
                    operation: "create_stage_file",
                    path: stage_file_path.display().to_string(),
                    source,
                })?;
            stage_file
                .write_all(&read.bytes)
                .map_err(|source| FileGetError::LocalWrite {
                    operation: "write_stage_file",
                    path: stage_file_path.display().to_string(),
                    source,
                })?;
            stage_file
                .sync_all()
                .map_err(|source| FileGetError::LocalWrite {
                    operation: "sync_stage_file",
                    path: stage_file_path.display().to_string(),
                    source,
                })?;
            drop(stage_file);
            fs::rename(&stage_file_path, &target_path).map_err(|source| {
                FileGetError::LocalWrite {
                    operation: "rename_stage_file",
                    path: target_path.display().to_string(),
                    source,
                }
            })?;
            total_size_bytes = total_size_bytes.saturating_add(read.bytes.len() as u64);
        }

        for directory in subtree.directories.iter().rev() {
            sync_directory(&stage_root.join(&directory.relative_path))?;
        }
        sync_directory(&stage_root)?;
        fs::rename(&stage_root, destination_root).map_err(|source| FileGetError::LocalWrite {
            operation: "rename_stage_root",
            path: destination_root.display().to_string(),
            source,
        })?;
        sync_directory(parent)?;
        let written_to = fs::canonicalize(destination_root)
            .unwrap_or(destination_root.to_path_buf())
            .display()
            .to_string();
        Ok(RecursiveGetWriteResult {
            written_to,
            file_count: subtree.files.len() as u64,
            directory_count: subtree.directories.len() as u64 + 1,
            total_size_bytes,
        })
    })();

    if result.is_err() && stage_root.exists() {
        let _ = fs::remove_dir_all(&stage_root);
    }
    result
}

fn sync_directory(path: &Path) -> Result<(), FileGetError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| FileGetError::LocalWrite {
            operation: "sync_directory",
            path: path.display().to_string(),
            source,
        })
}

#[cfg(test)]
mod tests {
    use super::{
        parse_authoritative_path_selector, run_file_command, FileCommand, FileCommandOutput,
    };
    use crate::OpsConfig;
    use loon_objectstore::keys::{blob, content_manifest};
    use loon_objectstore::ObjectStore;
    use loon_server::mutation::{execute_client_mutation, ClientMutationExecutionParams};
    use loon_server::ops::{bootstrap_namespace, NamespaceBootstrapParams};
    use loon_testkit::tempdir::TestDir;
    use loon_types::{
        sha256_digest, ClientMutationOp, ClientMutationRequest, ContentBlockDescriptor,
        ContentManifestEnvelope, ContentManifestPayload, NamespaceId,
    };
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn parse_selector_normalizes_root_and_nested_paths() {
        let root = parse_authoritative_path_selector("demo:/").expect("parse root");
        assert_eq!(root.namespace_id.as_str(), "demo");
        assert_eq!(root.absolute_path, "/");

        let nested =
            parse_authoritative_path_selector("demo:/docs//report.txt").expect("parse nested");
        assert_eq!(nested.absolute_path, "/docs/report.txt");
    }

    #[test]
    fn parse_selector_rejects_invalid_syntax() {
        assert!(parse_authoritative_path_selector("demo").is_err());
        assert!(parse_authoritative_path_selector("demo:docs").is_err());
        assert!(parse_authoritative_path_selector("demo:/docs/../secret").is_err());
    }

    #[test]
    fn ls_stat_get_and_cat_use_authoritative_store_directly() {
        let temp_dir = TestDir::new("loon-ops-file-read");
        let config_path = write_local_fs_config(temp_dir.path());
        let namespace_id = NamespaceId::from("demo");
        seed_namespace_with_hello_file(&config_path, &namespace_id, b"hello from loon\n");

        let ls = run_file_command(FileCommand::Ls {
            config_path: config_path.clone(),
            selector: "demo:/".to_owned(),
        })
        .expect("run ls");
        assert_eq!(ls, FileCommandOutput::Text("hello.txt\n".to_owned()));

        let stat = run_file_command(FileCommand::Stat {
            config_path: config_path.clone(),
            selector: "demo:/hello.txt".to_owned(),
        })
        .expect("run stat");
        let stat_text = match stat {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text stat output, got {other:?}"),
        };
        assert!(stat_text.contains("absolute_path: /hello.txt"));
        assert!(stat_text.contains("inode_kind: file"));

        let download_dir = temp_dir.path().join("downloads");
        fs::create_dir_all(&download_dir).expect("create download dir");
        let get = run_file_command(FileCommand::Get {
            config_path: config_path.clone(),
            selector: "demo:/hello.txt".to_owned(),
            local_path: download_dir.clone(),
            recursive: false,
        })
        .expect("run get");
        let get_text = match get {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text get output, got {other:?}"),
        };
        assert!(get_text.contains("size_bytes: 16"));
        assert_eq!(
            fs::read(download_dir.join("hello.txt")).expect("read downloaded file"),
            b"hello from loon\n"
        );

        let cat = run_file_command(FileCommand::Cat {
            config_path,
            selector: "demo:/hello.txt".to_owned(),
        })
        .expect("run cat");
        assert_eq!(cat, FileCommandOutput::Bytes(b"hello from loon\n".to_vec()));
    }

    #[test]
    fn put_mkdir_rm_and_mv_mutate_authoritative_store_directly() {
        let temp_dir = TestDir::new("loon-ops-file-write");
        let config_path = write_local_fs_config(temp_dir.path());
        let namespace_id = NamespaceId::from("demo");
        bootstrap_empty_namespace(&config_path, &namespace_id);

        let mkdir = run_file_command(FileCommand::Mkdir {
            config_path: config_path.clone(),
            selector: "demo:/docs".to_owned(),
        })
        .expect("run mkdir");
        let mkdir_text = match mkdir {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text mkdir output, got {other:?}"),
        };
        assert!(mkdir_text.contains("selector: demo:/docs"));
        assert!(mkdir_text.contains("absolute_path: /docs"));

        let local_file = temp_dir.path().join("hello.txt");
        fs::write(&local_file, b"hello write path\n").expect("write local source");
        let put = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: local_file,
            selector: "demo:/docs/hello.txt".to_owned(),
            replace: false,
            recursive: false,
        })
        .expect("run put");
        let put_text = match put {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text put output, got {other:?}"),
        };
        assert!(put_text.contains("destination: demo:/docs/hello.txt"));
        assert!(put_text.contains("replace: false"));
        assert!(put_text.contains("absolute_path: /docs/hello.txt"));

        let replacement_local_file = temp_dir.path().join("hello-v2.txt");
        fs::write(&replacement_local_file, b"hello replace path\n")
            .expect("write replacement local source");
        let replace = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: replacement_local_file,
            selector: "demo:/docs/hello.txt".to_owned(),
            replace: true,
            recursive: false,
        })
        .expect("run put --replace");
        let replace_text = match replace {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text replace output, got {other:?}"),
        };
        assert!(replace_text.contains("replace: true"));
        assert!(replace_text.contains("destination: demo:/docs/hello.txt"));
        assert!(replace_text.contains("revision_no: 2"));

        let cp = run_file_command(FileCommand::Cp {
            config_path: config_path.clone(),
            from_selector: "demo:/docs/hello.txt".to_owned(),
            to_selector: "demo:/docs/hello-copy.txt".to_owned(),
            replace: false,
        })
        .expect("run cp");
        let cp_text = match cp {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text cp output, got {other:?}"),
        };
        assert!(cp_text.contains("from: demo:/docs/hello.txt"));
        assert!(cp_text.contains("to: demo:/docs/hello-copy.txt"));
        assert!(cp_text.contains("replace: false"));
        assert!(cp_text.contains("absolute_path: /docs/hello-copy.txt"));

        let mv = run_file_command(FileCommand::Mv {
            config_path: config_path.clone(),
            from_selector: "demo:/docs/hello.txt".to_owned(),
            to_selector: "demo:/docs/archive.txt".to_owned(),
        })
        .expect("run mv");
        let mv_text = match mv {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text mv output, got {other:?}"),
        };
        assert!(mv_text.contains("from: demo:/docs/hello.txt"));
        assert!(mv_text.contains("to: demo:/docs/archive.txt"));

        let ls = run_file_command(FileCommand::Ls {
            config_path: config_path.clone(),
            selector: "demo:/docs".to_owned(),
        })
        .expect("list docs after mv");
        assert_eq!(
            ls,
            FileCommandOutput::Text("archive.txt\nhello-copy.txt\n".to_owned())
        );

        let rm = run_file_command(FileCommand::Rm {
            config_path,
            selector: "demo:/docs/archive.txt".to_owned(),
            recursive: false,
        })
        .expect("run rm");
        let rm_text = match rm {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text rm output, got {other:?}"),
        };
        assert!(rm_text.contains("deleted_path: /docs/archive.txt"));
        assert!(rm_text.contains("recursive: false"));
    }

    #[test]
    fn write_commands_fail_closed_for_invalid_targets() {
        let temp_dir = TestDir::new("loon-ops-file-write-errors");
        let config_path = write_local_fs_config(temp_dir.path());
        let namespace_id = NamespaceId::from("demo");
        seed_namespace_with_hello_file(&config_path, &namespace_id, b"hello from loon\n");

        let local_dir = temp_dir.path().join("local-dir");
        fs::create_dir_all(&local_dir).expect("create local dir");
        let put_dir_error = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: local_dir,
            selector: "demo:/copied.txt".to_owned(),
            replace: false,
            recursive: false,
        })
        .expect_err("local directory source should fail");
        assert!(put_dir_error.to_string().contains("regular file"));

        let put_missing_parent = temp_dir.path().join("upload.txt");
        fs::write(&put_missing_parent, b"hello").expect("write local upload");
        let put_parent_error = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: put_missing_parent,
            selector: "demo:/missing/copied.txt".to_owned(),
            replace: false,
            recursive: false,
        })
        .expect_err("missing remote parent should fail");
        assert!(put_parent_error
            .to_string()
            .contains("visible path not found"));

        let replace_missing = temp_dir.path().join("replace-missing.txt");
        fs::write(&replace_missing, b"replace").expect("write replacement file");
        let replace_missing_error = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: replace_missing,
            selector: "demo:/missing.txt".to_owned(),
            replace: true,
            recursive: false,
        })
        .expect_err("missing replace target should fail");
        assert!(replace_missing_error
            .to_string()
            .contains("visible path not found"));

        let replace_directory = temp_dir.path().join("replace-directory.txt");
        fs::write(&replace_directory, b"replace").expect("write replacement file");
        run_file_command(FileCommand::Mkdir {
            config_path: config_path.clone(),
            selector: "demo:/docs".to_owned(),
        })
        .expect("mkdir docs for replace-dir rejection");
        let replace_directory_error = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: replace_directory,
            selector: "demo:/docs".to_owned(),
            replace: true,
            recursive: false,
        })
        .expect_err("directory replace target should fail");
        assert!(replace_directory_error
            .to_string()
            .contains("must resolve to visible file"));

        let rm_dir_error = run_file_command(FileCommand::Rm {
            config_path: config_path.clone(),
            selector: "demo:/".to_owned(),
            recursive: true,
        })
        .expect_err("root remove should fail");
        assert!(rm_dir_error.to_string().contains("must not be root"));

        let mv_cross_namespace = run_file_command(FileCommand::Mv {
            config_path,
            from_selector: "demo:/hello.txt".to_owned(),
            to_selector: "other:/hello.txt".to_owned(),
        })
        .expect_err("cross namespace move should fail");
        assert!(mv_cross_namespace
            .to_string()
            .contains("requires both selectors in one namespace"));
    }

    #[test]
    fn recursive_put_uploads_nested_directory_tree_atomically() {
        let temp_dir = TestDir::new("loon-ops-file-put-recursive");
        let config_path = write_local_fs_config(temp_dir.path());
        let namespace_id = NamespaceId::from("demo");
        bootstrap_empty_namespace(&config_path, &namespace_id);

        run_file_command(FileCommand::Mkdir {
            config_path: config_path.clone(),
            selector: "demo:/imports".to_owned(),
        })
        .expect("mkdir imports");

        let docs_root = temp_dir.path().join("docs");
        let drafts_root = docs_root.join("drafts");
        fs::create_dir_all(&drafts_root).expect("create local drafts dir");
        fs::write(docs_root.join("report.txt"), b"report bytes\n").expect("write report");
        fs::write(drafts_root.join("note.txt"), b"note bytes\n").expect("write note");

        let put = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: docs_root,
            selector: "demo:/imports/docs".to_owned(),
            replace: false,
            recursive: true,
        })
        .expect("run recursive put");
        let put_text = match put {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text recursive put output, got {other:?}"),
        };
        assert!(put_text.contains("recursive: true"));
        assert!(put_text.contains("directory_count: 2"));
        assert!(put_text.contains("file_count: 2"));
        assert!(put_text.contains("destination: demo:/imports/docs"));

        let listing = run_file_command(FileCommand::Ls {
            config_path: config_path.clone(),
            selector: "demo:/imports/docs".to_owned(),
        })
        .expect("list uploaded docs");
        assert_eq!(
            listing,
            FileCommandOutput::Text("drafts/\nreport.txt\n".to_owned())
        );

        let report_bytes = run_file_command(FileCommand::Cat {
            config_path: config_path.clone(),
            selector: "demo:/imports/docs/report.txt".to_owned(),
        })
        .expect("cat uploaded report");
        assert_eq!(
            report_bytes,
            FileCommandOutput::Bytes(b"report bytes\n".to_vec())
        );
        let note_bytes = run_file_command(FileCommand::Cat {
            config_path,
            selector: "demo:/imports/docs/drafts/note.txt".to_owned(),
        })
        .expect("cat uploaded note");
        assert_eq!(
            note_bytes,
            FileCommandOutput::Bytes(b"note bytes\n".to_vec())
        );
    }

    #[test]
    fn recursive_put_fails_closed_for_invalid_sources_and_targets() {
        let temp_dir = TestDir::new("loon-ops-file-put-recursive-errors");
        let config_path = write_local_fs_config(temp_dir.path());
        let namespace_id = NamespaceId::from("demo");
        bootstrap_empty_namespace(&config_path, &namespace_id);

        run_file_command(FileCommand::Mkdir {
            config_path: config_path.clone(),
            selector: "demo:/imports".to_owned(),
        })
        .expect("mkdir imports");

        let local_file = temp_dir.path().join("not-a-dir.txt");
        fs::write(&local_file, b"hello").expect("write local file");
        let not_directory = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: local_file,
            selector: "demo:/imports/docs".to_owned(),
            replace: false,
            recursive: true,
        })
        .expect_err("recursive put file source should fail");
        assert!(not_directory.to_string().contains("must be a directory"));

        let docs_root = temp_dir.path().join("docs");
        fs::create_dir_all(&docs_root).expect("create docs root");
        let conflict = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: docs_root.clone(),
            selector: "demo:/imports/docs".to_owned(),
            replace: true,
            recursive: true,
        })
        .expect_err("recursive put replace conflict should fail");
        assert!(conflict.to_string().contains("mutually exclusive"));

        run_file_command(FileCommand::Mkdir {
            config_path: config_path.clone(),
            selector: "demo:/imports/docs".to_owned(),
        })
        .expect("mkdir occupied destination");
        let occupied = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: docs_root.clone(),
            selector: "demo:/imports/docs".to_owned(),
            replace: false,
            recursive: true,
        })
        .expect_err("occupied recursive destination should fail");
        assert!(occupied.to_string().contains("already occupied"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let symlink_root = temp_dir.path().join("symlink-root");
            fs::create_dir_all(&symlink_root).expect("create symlink root");
            symlink("missing-target", symlink_root.join("link"))
                .expect("create unsupported symlink child");
            let symlink_error = run_file_command(FileCommand::Put {
                config_path,
                local_path: symlink_root,
                selector: "demo:/imports/with-link".to_owned(),
                replace: false,
                recursive: true,
            })
            .expect_err("symlink child should fail");
            assert!(symlink_error.to_string().contains("symlink"));
        }
    }

    #[test]
    fn copy_rejects_occupied_directory_and_cross_namespace_targets() {
        let temp_dir = TestDir::new("loon-ops-file-copy-errors");
        let config_path = write_local_fs_config(temp_dir.path());
        let namespace_id = NamespaceId::from("demo");
        bootstrap_empty_namespace(&config_path, &namespace_id);

        run_file_command(FileCommand::Mkdir {
            config_path: config_path.clone(),
            selector: "demo:/docs".to_owned(),
        })
        .expect("mkdir docs");
        let local_file = temp_dir.path().join("hello.txt");
        fs::write(&local_file, b"hello").expect("write local file");
        run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: local_file,
            selector: "demo:/docs/hello.txt".to_owned(),
            replace: false,
            recursive: false,
        })
        .expect("seed source file");
        run_file_command(FileCommand::Mkdir {
            config_path: config_path.clone(),
            selector: "demo:/docs/archive".to_owned(),
        })
        .expect("mkdir occupied destination");

        let occupied_error = run_file_command(FileCommand::Cp {
            config_path: config_path.clone(),
            from_selector: "demo:/docs/hello.txt".to_owned(),
            to_selector: "demo:/docs/archive".to_owned(),
            replace: false,
        })
        .expect_err("occupied destination should fail");
        assert!(occupied_error.to_string().contains("already occupied"));

        let identical_error = run_file_command(FileCommand::Cp {
            config_path: config_path.clone(),
            from_selector: "demo:/docs/hello.txt".to_owned(),
            to_selector: "demo:/docs//hello.txt".to_owned(),
            replace: false,
        })
        .expect_err("identical path copy should fail");
        assert!(identical_error.to_string().contains("identical path"));

        let cross_namespace_error = run_file_command(FileCommand::Cp {
            config_path,
            from_selector: "demo:/docs/hello.txt".to_owned(),
            to_selector: "other:/docs/hello.txt".to_owned(),
            replace: false,
        })
        .expect_err("cross namespace copy should fail");
        assert!(cross_namespace_error
            .to_string()
            .contains("requires both selectors in one namespace"));
    }

    #[test]
    fn get_fails_closed_for_existing_target_and_missing_parent() {
        let temp_dir = TestDir::new("loon-ops-file-get-errors");
        let config_path = write_local_fs_config(temp_dir.path());
        let namespace_id = NamespaceId::from("demo");
        seed_namespace_with_hello_file(&config_path, &namespace_id, b"hello from loon\n");

        let existing_target = temp_dir.path().join("existing.txt");
        fs::write(&existing_target, b"present").expect("seed existing target");
        let existing_error = run_file_command(FileCommand::Get {
            config_path: config_path.clone(),
            selector: "demo:/hello.txt".to_owned(),
            local_path: existing_target,
            recursive: false,
        })
        .expect_err("existing target should fail");
        assert!(existing_error.to_string().contains("already exists"));

        let missing_parent = temp_dir.path().join("missing/download.txt");
        let missing_parent_error = run_file_command(FileCommand::Get {
            config_path,
            selector: "demo:/hello.txt".to_owned(),
            local_path: missing_parent,
            recursive: false,
        })
        .expect_err("missing parent should fail");
        assert!(missing_parent_error
            .to_string()
            .contains("parent directory is missing"));
    }

    #[test]
    fn cp_replace_and_recursive_get_follow_authoritative_semantics() {
        let temp_dir = TestDir::new("loon-ops-file-cp-replace-recursive-get");
        let config_path = write_local_fs_config(temp_dir.path());
        let namespace_id = NamespaceId::from("demo");
        bootstrap_empty_namespace(&config_path, &namespace_id);

        run_file_command(FileCommand::Mkdir {
            config_path: config_path.clone(),
            selector: "demo:/docs".to_owned(),
        })
        .expect("mkdir docs");
        run_file_command(FileCommand::Mkdir {
            config_path: config_path.clone(),
            selector: "demo:/docs/drafts".to_owned(),
        })
        .expect("mkdir drafts");

        let source_file = temp_dir.path().join("source.txt");
        fs::write(&source_file, b"source bytes\n").expect("write source file");
        run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: source_file,
            selector: "demo:/docs/source.txt".to_owned(),
            replace: false,
            recursive: false,
        })
        .expect("put source");

        let target_file = temp_dir.path().join("target.txt");
        fs::write(&target_file, b"target bytes\n").expect("write target file");
        run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: target_file,
            selector: "demo:/docs/target.txt".to_owned(),
            replace: false,
            recursive: false,
        })
        .expect("put target");

        let note_file = temp_dir.path().join("note.txt");
        fs::write(&note_file, b"note bytes\n").expect("write note file");
        run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: note_file,
            selector: "demo:/docs/drafts/note.txt".to_owned(),
            replace: false,
            recursive: false,
        })
        .expect("put note");

        let cp_replace = run_file_command(FileCommand::Cp {
            config_path: config_path.clone(),
            from_selector: "demo:/docs/source.txt".to_owned(),
            to_selector: "demo:/docs/target.txt".to_owned(),
            replace: true,
        })
        .expect("run cp --replace");
        let cp_replace_text = match cp_replace {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text cp --replace output, got {other:?}"),
        };
        assert!(cp_replace_text.contains("replace: true"));
        assert!(cp_replace_text.contains("to: demo:/docs/target.txt"));
        assert!(cp_replace_text.contains("revision_no: 2"));

        let recursive_root = temp_dir.path().join("recursive-download");
        let recursive_get = run_file_command(FileCommand::Get {
            config_path: config_path.clone(),
            selector: "demo:/docs".to_owned(),
            local_path: recursive_root.clone(),
            recursive: true,
        })
        .expect("run recursive get");
        let recursive_get_text = match recursive_get {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text recursive get output, got {other:?}"),
        };
        assert!(recursive_get_text.contains("recursive: true"));
        assert!(recursive_get_text.contains("file_count: 3"));
        assert!(recursive_get_text.contains("directory_count: 2"));
        assert_eq!(
            fs::read(recursive_root.join("source.txt")).expect("read downloaded source"),
            b"source bytes\n"
        );
        assert_eq!(
            fs::read(recursive_root.join("target.txt")).expect("read downloaded target"),
            b"source bytes\n"
        );
        assert_eq!(
            fs::read(recursive_root.join("drafts/note.txt")).expect("read downloaded note"),
            b"note bytes\n"
        );
    }

    #[test]
    fn cp_replace_and_recursive_get_fail_closed_for_invalid_targets() {
        let temp_dir = TestDir::new("loon-ops-file-cp-replace-recursive-get-errors");
        let config_path = write_local_fs_config(temp_dir.path());
        let namespace_id = NamespaceId::from("demo");
        bootstrap_empty_namespace(&config_path, &namespace_id);

        run_file_command(FileCommand::Mkdir {
            config_path: config_path.clone(),
            selector: "demo:/docs".to_owned(),
        })
        .expect("mkdir docs");
        let source_file = temp_dir.path().join("source.txt");
        fs::write(&source_file, b"source bytes\n").expect("write source file");
        run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: source_file,
            selector: "demo:/docs/source.txt".to_owned(),
            replace: false,
            recursive: false,
        })
        .expect("put source");

        let absent_cp_replace = run_file_command(FileCommand::Cp {
            config_path: config_path.clone(),
            from_selector: "demo:/docs/source.txt".to_owned(),
            to_selector: "demo:/docs/missing.txt".to_owned(),
            replace: true,
        })
        .expect_err("absent cp replace target should fail");
        assert!(absent_cp_replace
            .to_string()
            .contains("visible path not found"));

        let directory_cp_replace = run_file_command(FileCommand::Cp {
            config_path: config_path.clone(),
            from_selector: "demo:/docs/source.txt".to_owned(),
            to_selector: "demo:/docs".to_owned(),
            replace: true,
        })
        .expect_err("directory cp replace target should fail");
        assert!(directory_cp_replace
            .to_string()
            .contains("must resolve to visible file"));

        let identical_cp_replace = run_file_command(FileCommand::Cp {
            config_path: config_path.clone(),
            from_selector: "demo:/docs/source.txt".to_owned(),
            to_selector: "demo:/docs//source.txt".to_owned(),
            replace: true,
        })
        .expect_err("identical cp replace target should fail");
        assert!(identical_cp_replace.to_string().contains("identical path"));

        let recursive_file_error = run_file_command(FileCommand::Get {
            config_path: config_path.clone(),
            selector: "demo:/docs/source.txt".to_owned(),
            local_path: temp_dir.path().join("download-file-root"),
            recursive: true,
        })
        .expect_err("recursive get file selector should fail");
        assert!(recursive_file_error
            .to_string()
            .contains("directory command requires directory"));

        let existing_root = temp_dir.path().join("existing-root");
        fs::create_dir_all(&existing_root).expect("seed existing root");
        let existing_root_error = run_file_command(FileCommand::Get {
            config_path: config_path.clone(),
            selector: "demo:/docs".to_owned(),
            local_path: existing_root,
            recursive: true,
        })
        .expect_err("existing recursive target root should fail");
        assert!(existing_root_error.to_string().contains("already exists"));

        let missing_parent_root = temp_dir.path().join("missing-parent/output-root");
        let missing_parent_error = run_file_command(FileCommand::Get {
            config_path,
            selector: "demo:/docs".to_owned(),
            local_path: missing_parent_root.clone(),
            recursive: true,
        })
        .expect_err("missing recursive target parent should fail");
        assert!(missing_parent_error
            .to_string()
            .contains("parent directory is missing"));
        assert!(
            !missing_parent_root.exists(),
            "failed recursive get should leave final destination absent"
        );
    }

    fn write_local_fs_config(root: &Path) -> PathBuf {
        let object_store_root = root.join("object-store");
        let state_db_path = root.join("client.sqlite3");
        let mirror_root = root.join("mirror");
        fs::create_dir_all(&object_store_root).expect("create object store root");
        fs::create_dir_all(&mirror_root).expect("create mirror root");
        let config = OpsConfig {
            object_store: crate::OpsObjectStoreSpec::LocalFs {
                root: object_store_root,
                key_prefix: None,
            },
            client: crate::OpsClientConfig {
                state_db_path,
                mirror_root,
            },
            server: crate::OpsServerConfig {
                writer_id: "writer-a".to_owned(),
                writer_version: "loon-ops-test".to_owned(),
                lease_duration_ms: 60_000,
            },
            ops: crate::OpsSection {
                now_ms: Some(1_000),
                max_steps: None,
            },
        };
        let config_path = root.join("loondb-demo.local.toml");
        fs::write(
            &config_path,
            toml::to_string_pretty(&config).expect("serialize config"),
        )
        .expect("write config");
        config_path
    }

    fn bootstrap_empty_namespace(config_path: &Path, namespace_id: &NamespaceId) {
        let config = OpsConfig::load(config_path).expect("load config");
        let store = config.open_store().expect("open store");
        bootstrap_namespace(
            &store,
            namespace_id,
            &NamespaceBootstrapParams {
                holder_id: config.server.writer_id.clone(),
                writer_version: config.server.writer_version.clone(),
                now_ms: config.ops.now_ms.expect("configured now_ms"),
                lease_duration_ms: config.server.lease_duration_ms,
                allow_existing: false,
            },
        )
        .expect("bootstrap namespace");
    }

    fn seed_namespace_with_hello_file(
        config_path: &Path,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) {
        let config = OpsConfig::load(config_path).expect("load config");
        let store = config.open_store().expect("open store");
        bootstrap_empty_namespace(config_path, namespace_id);

        let file_digest_sha256 = sha256_digest(bytes);
        let block_digest = sha256_digest(bytes);
        store
            .put_if_absent(&blob(namespace_id.as_str(), &block_digest), bytes)
            .expect("write content block");
        let manifest = ContentManifestEnvelope::from_payload(ContentManifestPayload {
            namespace_id: namespace_id.clone(),
            file_size_bytes: bytes.len() as u64,
            file_digest_sha256,
            block_size_bytes: bytes.len() as u64,
            blocks: vec![ContentBlockDescriptor {
                content_digest_sha256: block_digest,
                plaintext_size_bytes: bytes.len() as u64,
            }],
        })
        .expect("build manifest");
        let manifest_bytes = serde_json::to_vec(&manifest).expect("serialize manifest");
        let manifest_digest = sha256_digest(&manifest_bytes);
        store
            .put_if_absent(
                &content_manifest(namespace_id.as_str(), &manifest_digest),
                &manifest_bytes,
            )
            .expect("write manifest");

        execute_client_mutation(
            &store,
            &ClientMutationRequest {
                namespace_id: namespace_id.clone(),
                client_request_id: "create-file".to_owned(),
                op: ClientMutationOp::CreateFile {
                    parent_inode_id: loon_types::InodeId(1),
                    display_name: "hello.txt".to_owned(),
                    content_manifest_digest: manifest_digest,
                },
            },
            &ClientMutationExecutionParams {
                writer_id: config.server.writer_id,
                writer_version: config.server.writer_version,
                now_ms: 2_000,
                lease_duration_ms: config.server.lease_duration_ms,
            },
        )
        .expect("create authoritative file");
    }
}
