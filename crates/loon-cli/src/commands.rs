use crate::args::{
    Cli, Command, CommandKind, ConfigCommand, FilesystemCommand, NamespaceCommand,
    ProfileAddCommand, ProfileCommand, RuntimeBehavior,
};
use crate::config::{
    load_config, load_config_if_exists, load_or_default_config, save_config, ProfileConfig,
    StoreConfig,
};
use crate::error::CliError;
use crate::profiles::{add_profile, list_profiles, remove_profile, show_profile, ProfileSummary};
use crate::resolve::{resolve_target_profile, resolved_config_path};
use loon_api::{AuthoritativePathEntry, NamespaceSummary};
use loon_client::NamespacePath;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

pub struct CommandOutput {
    pub kind: CommandKind,
    pub profile: Option<String>,
    pub mode: Option<String>,
    pub data: CommandData,
}

pub struct CommandFailure {
    pub kind: CommandKind,
    pub profile: Option<String>,
    pub mode: Option<String>,
    pub error: CliError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandData {
    Profile(ProfileConfig),
    ProfileSummary(ProfileSummary),
    ProfileList {
        profiles: Vec<ProfileSummary>,
    },
    NamespaceSummary(NamespaceSummary),
    NamespaceList {
        namespaces: Vec<NamespaceSummary>,
    },
    PathEntries {
        entries: Vec<AuthoritativePathEntry>,
    },
    PathEntry(AuthoritativePathEntry),
    FileTransfer {
        target: String,
        destination: String,
        bytes_written: u64,
    },
    FileMutation {
        target: String,
        committed_seq: u64,
    },
    PathMove {
        from: String,
        to: String,
        committed_seq: u64,
    },
    ConfigPath {
        path: String,
    },
    ConfigShow {
        config: crate::config::CliConfig,
    },
    Version {
        version: String,
    },
    StreamBytes(Vec<u8>),
}

pub fn run(cli: Cli, runtime: RuntimeBehavior) -> Result<CommandOutput, CommandFailure> {
    let kind = cli.kind();
    if runtime.json && !kind.supports_json() {
        return Err(CommandFailure {
            kind,
            profile: cli.profile.clone(),
            mode: None,
            error: CliError::json_not_supported_for_streaming(),
        });
    }

    run_inner(cli, runtime)
}

fn run_inner(cli: Cli, runtime: RuntimeBehavior) -> Result<CommandOutput, CommandFailure> {
    let kind = cli.kind();
    match cli.command {
        Command::Version => Ok(CommandOutput {
            kind,
            profile: None,
            mode: None,
            data: CommandData::Version {
                version: env!("CARGO_PKG_VERSION").to_owned(),
            },
        }),
        Command::Config { command } => run_config_command(kind, cli.config.as_deref(), command),
        Command::Profile { command } => {
            run_profile_command(kind, cli.config.as_deref(), command, runtime)
        }
        Command::Namespace { command } => {
            run_namespace_command(kind, cli.config.as_deref(), cli.profile.as_deref(), command)
        }
        Command::Filesystem { command } => run_filesystem_command(
            kind,
            cli.config.as_deref(),
            cli.profile.as_deref(),
            command,
            runtime,
        ),
    }
}

fn run_config_command(
    kind: CommandKind,
    explicit_config: Option<&Path>,
    command: ConfigCommand,
) -> Result<CommandOutput, CommandFailure> {
    let config_path =
        resolved_config_path(explicit_config).map_err(|error| fail(kind, None, None, error))?;
    match command {
        ConfigCommand::Path => Ok(CommandOutput {
            kind,
            profile: None,
            mode: None,
            data: CommandData::ConfigPath {
                path: config_path.display().to_string(),
            },
        }),
        ConfigCommand::Show => {
            let config =
                load_config(&config_path).map_err(|error| fail(kind, None, None, error))?;
            Ok(CommandOutput {
                kind,
                profile: None,
                mode: None,
                data: CommandData::ConfigShow {
                    config: config.redacted(),
                },
            })
        }
    }
}

fn run_profile_command(
    kind: CommandKind,
    explicit_config: Option<&Path>,
    command: ProfileCommand,
    _runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let config_path =
        resolved_config_path(explicit_config).map_err(|error| fail(kind, None, None, error))?;
    match command {
        ProfileCommand::Add { command } => run_profile_add(kind, &config_path, command),
        ProfileCommand::List => {
            let config = load_config_if_exists(&config_path)
                .map_err(|error| fail(kind, None, None, error))?;
            Ok(CommandOutput {
                kind,
                profile: None,
                mode: None,
                data: CommandData::ProfileList {
                    profiles: list_profiles(config.as_ref()),
                },
            })
        }
        ProfileCommand::Show { name } => {
            let config =
                load_config(&config_path).map_err(|error| fail(kind, name.clone(), None, error))?;
            let (profile_name, redacted) = show_profile(&config, name.as_deref())
                .map_err(|error| fail(kind, name.clone(), None, error))?;
            let mode = redacted.mode_str().to_owned();
            Ok(CommandOutput {
                kind,
                profile: Some(profile_name),
                mode: Some(mode),
                data: CommandData::Profile(redacted),
            })
        }
        ProfileCommand::Remove { name } => {
            let mut config = load_config(&config_path)
                .map_err(|error| fail(kind, Some(name.clone()), None, error))?;
            let removed = remove_profile(&mut config, &name)
                .map_err(|error| fail(kind, Some(name.clone()), None, error))?;
            let mode = removed.mode.clone();
            save_config(&config_path, &config)
                .map_err(|error| fail(kind, Some(name.clone()), Some(mode.clone()), error))?;
            Ok(CommandOutput {
                kind,
                profile: Some(name),
                mode: Some(mode),
                data: CommandData::ProfileSummary(removed),
            })
        }
    }
}

fn run_profile_add(
    kind: CommandKind,
    config_path: &Path,
    command: ProfileAddCommand,
) -> Result<CommandOutput, CommandFailure> {
    let (name, profile) = match command {
        ProfileAddCommand::LocalFs(args) => {
            let profile = ProfileConfig::Local {
                store: StoreConfig::LocalFs {
                    root: args.root,
                    key_prefix: args.key_prefix,
                },
                writer_id: None,
                writer_version: None,
                lease_duration_ms: None,
            };
            (args.name, profile)
        }
        ProfileAddCommand::AwsS3(args) => {
            let profile = ProfileConfig::Local {
                store: StoreConfig::AwsS3 {
                    bucket: args.bucket,
                    region: args.region,
                    endpoint_url: args.endpoint_url,
                    access_key_id: args.access_key_id,
                    secret_access_key: args.secret_access_key,
                    session_token: args.session_token,
                    key_prefix: args.key_prefix,
                    force_path_style: if args.force_path_style { Some(true) } else { None },
                },
                writer_id: None,
                writer_version: None,
                lease_duration_ms: None,
            };
            (args.name, profile)
        }
        ProfileAddCommand::CloudflareR2(args) => {
            let profile = ProfileConfig::Local {
                store: StoreConfig::CloudflareR2 {
                    bucket: args.bucket,
                    account_id: args.account_id,
                    endpoint_url: args.endpoint_url,
                    access_key_id: args.access_key_id,
                    secret_access_key: args.secret_access_key,
                    key_prefix: args.key_prefix,
                },
                writer_id: None,
                writer_version: None,
                lease_duration_ms: None,
            };
            (args.name, profile)
        }
        ProfileAddCommand::Remote(args) => {
            let profile = ProfileConfig::Remote {
                server_url: args.server_url,
                auth_token: args.auth_token,
            };
            (args.name, profile)
        }
    };

    let mode = profile.mode_str().to_owned();
    let mut config = load_or_default_config(config_path)
        .map_err(|error| fail(kind, Some(name.clone()), Some(mode.clone()), error))?;
    let (profile_name, redacted) = add_profile(&mut config, &name, profile)
        .map_err(|error| fail(kind, Some(name.clone()), Some(mode.clone()), error))?;
    save_config(config_path, &config)
        .map_err(|error| fail(kind, Some(name.clone()), Some(mode.clone()), error))?;
    Ok(CommandOutput {
        kind,
        profile: Some(profile_name),
        mode: Some(mode),
        data: CommandData::Profile(redacted),
    })
}

fn run_namespace_command(
    kind: CommandKind,
    explicit_config: Option<&Path>,
    global_profile: Option<&str>,
    command: NamespaceCommand,
) -> Result<CommandOutput, CommandFailure> {
    let resolved = resolve_target_profile(explicit_config, global_profile)
        .map_err(|error| fail(kind, global_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let backend = resolved.target.backend();
    let output = match command {
        NamespaceCommand::Create { name } => {
            validate_namespace_name(&name).map_err(|error| {
                fail(
                    kind,
                    Some(resolved.profile_name.clone()),
                    Some(mode.clone()),
                    error,
                )
            })?;
            let namespace = backend.create_namespace(&name).map_err(|error| {
                fail(
                    kind,
                    Some(resolved.profile_name.clone()),
                    Some(mode.clone()),
                    error,
                )
            })?;
            CommandData::NamespaceSummary(namespace)
        }
        NamespaceCommand::List => {
            let namespaces = backend.list_namespaces().map_err(|error| {
                fail(
                    kind,
                    Some(resolved.profile_name.clone()),
                    Some(mode.clone()),
                    error,
                )
            })?;
            CommandData::NamespaceList { namespaces }
        }
    };

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: output,
    })
}

fn run_filesystem_command(
    kind: CommandKind,
    explicit_config: Option<&Path>,
    global_profile: Option<&str>,
    command: FilesystemCommand,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let resolved = resolve_target_profile(explicit_config, global_profile)
        .map_err(|error| fail(kind, global_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let backend = resolved.target.backend();
    let profile_name = resolved.profile_name.clone();

    let output = (|| -> Result<CommandData, CliError> {
        match command {
            FilesystemCommand::Ls { namespace, path } => {
                let spec = namespace_path(&namespace, path.as_deref().unwrap_or("/"), true)?;
                let entries = backend.list_path(&spec)?;
                Ok(CommandData::PathEntries { entries })
            }
            FilesystemCommand::Stat { namespace, path } => {
                let spec = namespace_path(&namespace, &path, true)?;
                let entry = backend.stat_path(&spec)?;
                Ok(CommandData::PathEntry(entry))
            }
            FilesystemCommand::Cat { namespace, path } => {
                let spec = namespace_path(&namespace, &path, false)?;
                let bytes = backend.read_file_bytes(&spec)?;
                Ok(CommandData::StreamBytes(bytes))
            }
            FilesystemCommand::Get {
                namespace,
                remote_path,
                local_destination,
            } => {
                if runtime.json && local_destination.as_deref() == Some("-") {
                    return Err(CliError::json_not_supported_for_streaming());
                }
                let spec = namespace_path(&namespace, &remote_path, false)?;
                let entry = backend.stat_path(&spec)?;
                if entry.inode_kind == loon_api::InodeKind::Dir {
                    return Err(CliError::invalid_input(format!(
                        "directory operations are not available for `{}`",
                        spec.absolute_path
                    )));
                }
                let bytes = backend.read_file_bytes(&spec)?;
                match local_destination.as_deref() {
                    Some("-") => Ok(CommandData::StreamBytes(bytes)),
                    other => {
                        let destination = destination_path_for_get(&spec.absolute_path, other)?;
                        fs::write(&destination, &bytes).map_err(CliError::io)?;
                        Ok(CommandData::FileTransfer {
                            target: render_target(&namespace, &spec.absolute_path),
                            destination: destination.display().to_string(),
                            bytes_written: bytes.len() as u64,
                        })
                    }
                }
            }
            FilesystemCommand::Put {
                namespace,
                local_path,
                remote_path,
                force,
            } => {
                let local_path = PathBuf::from(&local_path);
                if local_path == Path::new("-") {
                    return Err(CliError::invalid_input(
                        "`-` is not supported for `filesystem put`",
                    ));
                }
                let metadata = fs::metadata(&local_path).map_err(CliError::io)?;
                if metadata.is_dir() {
                    return Err(CliError::invalid_input(format!(
                        "directory operations are not available for `{}`",
                        local_path.display()
                    )));
                }
                let remote_path = match remote_path {
                    Some(path) => normalize_absolute_path(&path, false)?,
                    None => default_remote_put_path(&local_path)?,
                };
                let spec = namespace_path(&namespace, &remote_path, false)?;
                let bytes = fs::read(&local_path).map_err(CliError::io)?;
                let result = backend.put_file_bytes(&spec, &bytes, force)?;
                Ok(CommandData::FileMutation {
                    target: render_target(&namespace, &spec.absolute_path),
                    committed_seq: result.committed_seq.0,
                })
            }
            FilesystemCommand::Rm {
                namespace,
                remote_path,
            } => {
                let spec = namespace_path(&namespace, &remote_path, false)?;
                let result = backend.delete_path(&spec)?;
                Ok(CommandData::FileMutation {
                    target: render_target(&namespace, &spec.absolute_path),
                    committed_seq: result.committed_seq.0,
                })
            }
            FilesystemCommand::Mv {
                namespace,
                source_path,
                dest_path,
            } => {
                let from = namespace_path(&namespace, &source_path, false)?;
                let to = namespace_path(&namespace, &dest_path, false)?;
                let result = backend.move_path(&from, &to)?;
                Ok(CommandData::PathMove {
                    from: render_target(&namespace, &from.absolute_path),
                    to: render_target(&namespace, &to.absolute_path),
                    committed_seq: result.committed_seq.0,
                })
            }
            FilesystemCommand::Cp {
                namespace,
                source_path,
                dest_path,
            } => {
                let from = namespace_path(&namespace, &source_path, false)?;
                let entry = backend.stat_path(&from)?;
                if entry.inode_kind == loon_api::InodeKind::Dir {
                    return Err(CliError::invalid_input(format!(
                        "directory operations are not available for `{}`",
                        from.absolute_path
                    )));
                }
                let to = namespace_path(&namespace, &dest_path, false)?;
                let result = backend.copy_path(&from, &to)?;
                Ok(CommandData::PathMove {
                    from: render_target(&namespace, &from.absolute_path),
                    to: render_target(&namespace, &to.absolute_path),
                    committed_seq: result.committed_seq.0,
                })
            }
        }
    })()
    .map_err(|error| fail(kind, Some(profile_name.clone()), Some(mode.clone()), error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(profile_name),
        mode: Some(mode),
        data: output,
    })
}

fn validate_namespace_name(namespace: &str) -> Result<(), CliError> {
    if namespace.trim().is_empty() {
        return Err(CliError::invalid_input("namespace name must not be empty"));
    }
    Ok(())
}

fn namespace_path(
    namespace: &str,
    path: &str,
    allow_root: bool,
) -> Result<NamespacePath, CliError> {
    validate_namespace_name(namespace)?;
    Ok(NamespacePath {
        namespace: namespace.to_owned(),
        absolute_path: normalize_absolute_path(path, allow_root)?,
    })
}

fn normalize_absolute_path(path: &str, allow_root: bool) -> Result<String, CliError> {
    if !path.starts_with('/') {
        return Err(CliError::invalid_input(format!(
            "filesystem paths must be absolute: `{path}`"
        )));
    }
    let mut components = Vec::new();
    for component in path.split('/') {
        if component.is_empty() {
            continue;
        }
        if component == "." || component == ".." {
            return Err(CliError::invalid_input(format!(
                "invalid filesystem path `{path}`"
            )));
        }
        components.push(component);
    }
    if components.is_empty() {
        if allow_root {
            return Ok("/".to_owned());
        }
        return Err(CliError::invalid_input(
            "root path is not allowed for this command",
        ));
    }
    Ok(format!("/{}", components.join("/")))
}

fn default_remote_put_path(local_path: &Path) -> Result<String, CliError> {
    let file_name = local_path.file_name().ok_or_else(|| {
        CliError::invalid_input(format!(
            "unable to derive remote target from `{}`",
            local_path.display()
        ))
    })?;
    Ok(format!("/{}", file_name.to_string_lossy()))
}

fn destination_path_for_get(
    remote_path: &str,
    explicit_destination: Option<&str>,
) -> Result<PathBuf, CliError> {
    match explicit_destination {
        Some(path) => Ok(PathBuf::from(path)),
        None => {
            let file_name = Path::new(remote_path).file_name().ok_or_else(|| {
                CliError::invalid_input(format!(
                    "unable to derive local destination from `{remote_path}`"
                ))
            })?;
            Ok(PathBuf::from(file_name))
        }
    }
}

fn render_target(namespace: &str, path: &str) -> String {
    format!("{namespace}:{path}")
}

fn fail(
    kind: CommandKind,
    profile: Option<String>,
    mode: Option<String>,
    error: CliError,
) -> CommandFailure {
    CommandFailure {
        kind,
        profile,
        mode,
        error,
    }
}
