use crate::args::{
    Cli, Command, CommandKind, ConfigCommand, FilesystemCommand, LocalCommand, NamespaceCommand,
    ProfileAddCommand, ProfileAddLocalArgs, ProfileAddRemoteArgs, ProfileCommand, RuntimeBehavior,
};
use crate::config::{
    load_config, load_config_if_exists, load_or_default_config, resolve_profile_path, save_config,
    LocalProfileConfig, ProfileMode, RemoteProfileConfig,
};
use crate::error::CliError;
use crate::local_runtime::{
    inspect_local_server, start_local_server, stop_local_server, LocalServerStatus,
};
use crate::profiles::{
    add_local_profile, add_remote_profile, list_profiles, remove_profile, show_profile,
    use_profile, ProfileSummary, ProfileView,
};
use crate::resolve::{resolve_target_profile, resolved_config_path};
use loon_api::{AuthoritativePathEntry, NamespaceSummary};
use loon_client::{ClientError, NamespacePath};
use serde::Serialize;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub struct CommandOutput {
    pub kind: CommandKind,
    pub profile: Option<String>,
    pub mode: Option<ProfileMode>,
    pub data: CommandData,
}

pub struct CommandFailure {
    pub kind: CommandKind,
    pub profile: Option<String>,
    pub mode: Option<ProfileMode>,
    pub error: CliError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandData {
    Profile(ProfileView),
    ProfileSummary(ProfileSummary),
    ProfileList {
        profiles: Vec<ProfileSummary>,
    },
    LocalStatus(LocalServerStatus),
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
        config: crate::config::RedactedCliConfig,
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
        Command::Local { command } => {
            run_local_command(kind, cli.config.as_deref(), cli.profile.as_deref(), command)
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
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let config_path =
        resolved_config_path(explicit_config).map_err(|error| fail(kind, None, None, error))?;
    match command {
        ProfileCommand::Add { command } => match command {
            ProfileAddCommand::Local(args) => add_local(kind, &config_path, args, runtime),
            ProfileAddCommand::Remote(args) => add_remote(kind, &config_path, args, runtime),
        },
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
        ProfileCommand::Use { name } => {
            let mut config = load_config(&config_path)
                .map_err(|error| fail(kind, Some(name.clone()), None, error))?;
            let view = use_profile(&mut config, &name)
                .map_err(|error| fail(kind, Some(name.clone()), None, error))?;
            save_config(&config_path, &config)
                .map_err(|error| fail(kind, Some(name.clone()), Some(view.mode), error))?;
            Ok(CommandOutput {
                kind,
                profile: Some(name),
                mode: Some(view.mode),
                data: CommandData::Profile(view),
            })
        }
        ProfileCommand::Show { name } => {
            let config =
                load_config(&config_path).map_err(|error| fail(kind, name.clone(), None, error))?;
            let view = show_profile(&config, name.as_deref())
                .map_err(|error| fail(kind, name.clone(), None, error))?;
            Ok(CommandOutput {
                kind,
                profile: Some(view.name.clone()),
                mode: Some(view.mode),
                data: CommandData::Profile(view),
            })
        }
        ProfileCommand::Remove { name } => {
            let mut config = load_config(&config_path)
                .map_err(|error| fail(kind, Some(name.clone()), None, error))?;
            let removed = remove_profile(&mut config, &name)
                .map_err(|error| fail(kind, Some(name.clone()), None, error))?;
            save_config(&config_path, &config)
                .map_err(|error| fail(kind, Some(name.clone()), Some(removed.mode), error))?;
            Ok(CommandOutput {
                kind,
                profile: Some(name),
                mode: Some(removed.mode),
                data: CommandData::ProfileSummary(removed),
            })
        }
    }
}

fn add_local(
    kind: CommandKind,
    config_path: &Path,
    args: ProfileAddLocalArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let profile_name = args.name.clone();
    let mut config = load_or_default_config(config_path).map_err(|error| {
        fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Local),
            error,
        )
    })?;
    if config.profiles.contains_key(&profile_name) {
        return Err(fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Local),
            CliError::profile_already_exists(&profile_name),
        ));
    }

    let server_config = required_path(
        args.server_config,
        "server-config",
        "local server config path",
        runtime,
    )
    .map_err(|error| {
        fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Local),
            error,
        )
    })?;
    let resolved_server_config = resolve_cli_input_path(&server_config)
        .unwrap_or_else(|_| resolve_profile_path(config_path, &server_config));
    loon_server::load_server_config(&resolved_server_config).map_err(|error| {
        fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Local),
            CliError::invalid_config(format!(
                "failed to load local server config `{}`: {error}",
                resolved_server_config.display()
            )),
        )
    })?;
    let view = add_local_profile(
        &mut config,
        &profile_name,
        LocalProfileConfig {
            server_config_path: resolved_server_config.display().to_string(),
        },
    )
    .map_err(|error| {
        fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Local),
            error,
        )
    })?;
    save_config(config_path, &config).map_err(|error| {
        fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Local),
            error,
        )
    })?;
    Ok(CommandOutput {
        kind,
        profile: Some(view.name.clone()),
        mode: Some(ProfileMode::Local),
        data: CommandData::Profile(view),
    })
}

fn resolve_cli_input_path(path: &Path) -> Result<PathBuf, CliError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|err| {
                CliError::invalid_config(format!(
                    "failed to resolve current working directory: {err}"
                ))
            })?
            .join(path)
    };

    absolute.canonicalize().map_err(|err| {
        CliError::invalid_config(format!(
            "failed to resolve server config path `{}`: {err}",
            path.display()
        ))
    })
}

fn add_remote(
    kind: CommandKind,
    config_path: &Path,
    args: ProfileAddRemoteArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let profile_name = args.name.clone();
    let mut config = load_or_default_config(config_path).map_err(|error| {
        fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Remote),
            error,
        )
    })?;
    if config.profiles.contains_key(&profile_name) {
        return Err(fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Remote),
            CliError::profile_already_exists(&profile_name),
        ));
    }

    let remote = RemoteProfileConfig {
        server_url: required_value(args.server_url, "server-url", "remote server URL", runtime)
            .map_err(|error| {
                fail(
                    kind,
                    Some(profile_name.clone()),
                    Some(ProfileMode::Remote),
                    error,
                )
            })?,
        auth_token: args.auth_token,
    };
    remote.validate(&profile_name).map_err(|error| {
        fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Remote),
            error,
        )
    })?;
    let view = add_remote_profile(&mut config, &profile_name, remote).map_err(|error| {
        fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Remote),
            error,
        )
    })?;
    save_config(config_path, &config).map_err(|error| {
        fail(
            kind,
            Some(profile_name.clone()),
            Some(ProfileMode::Remote),
            error,
        )
    })?;
    Ok(CommandOutput {
        kind,
        profile: Some(view.name.clone()),
        mode: Some(ProfileMode::Remote),
        data: CommandData::Profile(view),
    })
}

fn run_local_command(
    kind: CommandKind,
    explicit_config: Option<&Path>,
    global_profile: Option<&str>,
    command: LocalCommand,
) -> Result<CommandOutput, CommandFailure> {
    let resolved = resolve_target_profile(explicit_config, global_profile)
        .map_err(|error| fail(kind, global_profile.map(ToOwned::to_owned), None, error))?;
    let Some(local) = resolved.target.as_local() else {
        return Err(fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(resolved.target.mode()),
            CliError::invalid_profile_mode(
                &resolved.profile_name,
                "local",
                profile_mode_str(resolved.target.mode()),
            ),
        ));
    };

    let status = match command {
        LocalCommand::Up => start_local_server(
            &resolved.config_path,
            &resolved.profile_name,
            &local.server_url,
            &local.server_config_path,
        ),
        LocalCommand::Status => inspect_local_server(
            &resolved.config_path,
            &resolved.profile_name,
            &local.server_url,
            &local.server_config_path,
        ),
        LocalCommand::Down => stop_local_server(
            &resolved.config_path,
            &resolved.profile_name,
            &local.server_url,
            &local.server_config_path,
        ),
    }
    .map_err(|error| {
        fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(ProfileMode::Local),
            error,
        )
    })?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(ProfileMode::Local),
        data: CommandData::LocalStatus(status),
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
    let mode = resolved.target.mode();
    let client = resolved.target.client();
    let output = match command {
        NamespaceCommand::Create { name } => {
            validate_namespace_name(&name).map_err(|error| {
                fail(kind, Some(resolved.profile_name.clone()), Some(mode), error)
            })?;
            let namespace = client.create_namespace(&name).map_err(|error| {
                fail(
                    kind,
                    Some(resolved.profile_name.clone()),
                    Some(mode),
                    map_client_error(error),
                )
            })?;
            CommandData::NamespaceSummary(namespace)
        }
        NamespaceCommand::List => {
            let namespaces = client.list_namespaces().map_err(|error| {
                fail(
                    kind,
                    Some(resolved.profile_name.clone()),
                    Some(mode),
                    map_client_error(error),
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
    let mode = resolved.target.mode();
    let client = resolved.target.client();
    let profile_name = resolved.profile_name.clone();

    let output = (|| -> Result<CommandData, CliError> {
        match command {
            FilesystemCommand::Ls { namespace, path } => {
                let spec = namespace_path(&namespace, path.as_deref().unwrap_or("/"), true)?;
                let entries = client.list_path(&spec).map_err(map_client_error)?;
                Ok(CommandData::PathEntries { entries })
            }
            FilesystemCommand::Stat { namespace, path } => {
                let spec = namespace_path(&namespace, &path, true)?;
                let entry = client.stat_path(&spec).map_err(map_client_error)?;
                Ok(CommandData::PathEntry(entry))
            }
            FilesystemCommand::Cat { namespace, path } => {
                let spec = namespace_path(&namespace, &path, false)?;
                let bytes = client.read_file_bytes(&spec).map_err(map_client_error)?;
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
                let entry = client.stat_path(&spec).map_err(map_client_error)?;
                if entry.inode_kind == loon_api::InodeKind::Dir {
                    return Err(CliError::invalid_input(format!(
                        "directory operations are not available for `{}`",
                        spec.absolute_path
                    )));
                }
                let bytes = client.read_file_bytes(&spec).map_err(map_client_error)?;
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
                let result = client
                    .put_file_bytes(&spec, &bytes, force)
                    .map_err(map_client_error)?;
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
                let result = client.delete_path(&spec).map_err(map_client_error)?;
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
                let result = client.move_path(&from, &to).map_err(map_client_error)?;
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
                let entry = client.stat_path(&from).map_err(map_client_error)?;
                if entry.inode_kind == loon_api::InodeKind::Dir {
                    return Err(CliError::invalid_input(format!(
                        "directory operations are not available for `{}`",
                        from.absolute_path
                    )));
                }
                let to = namespace_path(&namespace, &dest_path, false)?;
                let result = client.copy_path(&from, &to).map_err(map_client_error)?;
                Ok(CommandData::PathMove {
                    from: render_target(&namespace, &from.absolute_path),
                    to: render_target(&namespace, &to.absolute_path),
                    committed_seq: result.committed_seq.0,
                })
            }
        }
    })()
    .map_err(|error| fail(kind, Some(profile_name.clone()), Some(mode), error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(profile_name),
        mode: Some(mode),
        data: output,
    })
}

fn required_value(
    existing: Option<String>,
    field: &str,
    prompt_label: &str,
    runtime: RuntimeBehavior,
) -> Result<String, CliError> {
    if let Some(value) = existing {
        if !value.trim().is_empty() {
            return Ok(value);
        }
    }
    if !runtime.interactive {
        return Err(CliError::non_interactive_input_required(field));
    }
    eprint!("{prompt_label}: ");
    io::stderr().flush().map_err(CliError::io)?;
    let mut line = String::new();
    io::stdin().read_line(&mut line).map_err(CliError::io)?;
    let value = line.trim().to_owned();
    if value.is_empty() {
        return Err(CliError::invalid_input(format!("missing `{field}`")));
    }
    Ok(value)
}

fn required_path(
    existing: Option<PathBuf>,
    field: &str,
    prompt_label: &str,
    runtime: RuntimeBehavior,
) -> Result<PathBuf, CliError> {
    if let Some(value) = existing {
        return Ok(value);
    }
    if !runtime.interactive {
        return Err(CliError::non_interactive_input_required(field));
    }
    eprint!("{prompt_label}: ");
    io::stderr().flush().map_err(CliError::io)?;
    let mut line = String::new();
    io::stdin().read_line(&mut line).map_err(CliError::io)?;
    let value = line.trim();
    if value.is_empty() {
        return Err(CliError::invalid_input(format!("missing `{field}`")));
    }
    Ok(PathBuf::from(value))
}

fn map_client_error(error: ClientError) -> CliError {
    match error {
        ClientError::ConfigIo(message) | ClientError::ConfigDecode(message) => {
            CliError::invalid_config(message)
        }
        ClientError::MissingConfigField { field } => {
            CliError::invalid_config(format!("missing `{field}`"))
        }
        ClientError::ConfigValidation { field, reason } => {
            CliError::invalid_config(format!("invalid `{field}`: {reason}"))
        }
        ClientError::InvalidNamespacePath(message) => CliError::invalid_input(message),
        ClientError::Http(message) | ClientError::Json(message) => CliError::client_error(message),
        ClientError::Api { code, message, .. } => CliError::new(code, message),
        ClientError::Io(message) => CliError::new("io_error", format!("i/o error: {message}")),
    }
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

fn profile_mode_str(mode: ProfileMode) -> &'static str {
    match mode {
        ProfileMode::Local => "local",
        ProfileMode::Remote => "remote",
    }
}

fn render_target(namespace: &str, path: &str) -> String {
    format!("{namespace}:{path}")
}

fn fail(
    kind: CommandKind,
    profile: Option<String>,
    mode: Option<ProfileMode>,
    error: CliError,
) -> CommandFailure {
    CommandFailure {
        kind,
        profile,
        mode,
        error,
    }
}
