use crate::OpsConfig;
use anyhow::{bail, Context, Result};
use loon_server::mutation::ClientMutationExecutionParams;
use loon_server::ops::{
    list_authoritative_path, mkdir_authoritative_path, mv_authoritative_path,
    put_authoritative_file_from_path, read_authoritative_file_bytes, resolve_authoritative_path,
    rm_authoritative_path, AuthoritativeFileBytes, AuthoritativePathEntry,
};
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
    Put {
        config_path: PathBuf,
        local_path: PathBuf,
        selector: String,
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
struct FilePutReport {
    source_local_path: String,
    destination: String,
    entry: AuthoritativePathEntry,
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

#[derive(Debug, Error)]
enum FilePutLocalSourceError {
    #[error("upload source path does not exist: `{path}`")]
    Missing { path: String },
    #[error("upload source must be a regular file: `{path}`")]
    NotRegularFile { path: String },
    #[error("failed to inspect upload source `{path}`: {source}")]
    ReadMetadata {
        path: String,
        source: std::io::Error,
    },
}

#[derive(Debug, Error)]
enum FileMvSelectorError {
    #[error(
        "authoritative move requires both selectors in one namespace: from `{from_selector}` to `{to_selector}`"
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
        } => {
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let selector_str = selector.clone();
            let selector = parse_authoritative_path_selector(&selector)?;
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
        } => {
            ensure_put_source_is_regular_file(&local_path)?;
            let config = OpsConfig::load(&config_path)?;
            let store = config.open_store()?;
            let selector = parse_authoritative_path_selector(&selector)?;
            let result = put_authoritative_file_from_path(
                &store,
                &selector.namespace_id,
                &local_path,
                &selector.absolute_path,
                &mutation_params(&config),
            )?;
            let report = FilePutReport {
                source_local_path: local_path.display().to_string(),
                destination: format_selector(&selector),
                entry: result.entry,
                committed_seq: ChangeSeqReport {
                    seq: result.committed_seq.0,
                },
            };
            Ok(FileCommandOutput::Text(
                serde_yaml::to_string(&report).context("render file put report")?,
            ))
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

fn ensure_same_namespace(
    from: &AuthoritativePathSelector,
    to: &AuthoritativePathSelector,
    from_selector: &str,
    to_selector: &str,
) -> Result<(), FileMvSelectorError> {
    if from.namespace_id != to.namespace_id {
        return Err(FileMvSelectorError::CrossNamespace {
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
        })
        .expect("run put");
        let put_text = match put {
            FileCommandOutput::Text(text) => text,
            other => panic!("expected text put output, got {other:?}"),
        };
        assert!(put_text.contains("destination: demo:/docs/hello.txt"));
        assert!(put_text.contains("absolute_path: /docs/hello.txt"));

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
        assert_eq!(ls, FileCommandOutput::Text("archive.txt\n".to_owned()));

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
        })
        .expect_err("local directory source should fail");
        assert!(put_dir_error.to_string().contains("regular file"));

        let put_missing_parent = temp_dir.path().join("upload.txt");
        fs::write(&put_missing_parent, b"hello").expect("write local upload");
        let put_parent_error = run_file_command(FileCommand::Put {
            config_path: config_path.clone(),
            local_path: put_missing_parent,
            selector: "demo:/missing/copied.txt".to_owned(),
        })
        .expect_err("missing remote parent should fail");
        assert!(put_parent_error
            .to_string()
            .contains("visible path not found"));

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
        })
        .expect_err("existing target should fail");
        assert!(existing_error.to_string().contains("already exists"));

        let missing_parent = temp_dir.path().join("missing/download.txt");
        let missing_parent_error = run_file_command(FileCommand::Get {
            config_path,
            selector: "demo:/hello.txt".to_owned(),
            local_path: missing_parent,
        })
        .expect_err("missing parent should fail");
        assert!(missing_parent_error
            .to_string()
            .contains("parent directory is missing"));
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
