use crate::args::{
    Cli, Command, CommandKind, ConfigCommand, CurrentArgs, FilesystemGetArgs, FilesystemLsArgs,
    FilesystemMoveArgs, FilesystemPathArgs, FilesystemPutArgs, InitArgs, NamespaceCommand,
    NamespaceCreateArgs, NamespaceForkArgs, NamespaceListArgs, NamespaceUseArgs, ProfileCommand,
    ProfileCreateArgs, ProfileUpdateArgs, RuntimeBehavior, TargetSelectorArgs,
};
use crate::config::{
    default_config_path, load_config, load_config_if_exists, load_or_default_config, save_config,
    CliConfig, ProfileConfig, StoreConfig,
};
use crate::error::CliError;
use crate::profiles::{
    add_profile, default_namespace, list_profiles, make_default_profile, remove_profile,
    set_default_namespace, show_profile, update_profile, ProfileSummary,
};
use crate::prompt;
use crate::resolve::{
    load_cli_config, resolve_namespace, resolve_target_profile, resolve_target_profile_from_config,
};
use loon_api::{AuthoritativePathEntry, InodeKind, NamespaceId, NamespaceSummary};
use loon_client::NamespacePath;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

const AWS_REGIONS: &[&str] = &[
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-central-1",
    "eu-central-2",
    "eu-north-1",
    "eu-south-1",
    "eu-south-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-south-2",
    "ap-east-1",
    "ca-central-1",
    "ca-west-1",
    "sa-east-1",
    "me-south-1",
    "me-central-1",
    "af-south-1",
    "il-central-1",
];

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
        default_profile: Option<String>,
        profiles: Vec<ProfileSummary>,
    },
    DefaultProfile {
        name: String,
    },
    DefaultNamespace {
        profile: String,
        namespace: String,
    },
    Current {
        profile: String,
        namespace: Option<String>,
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
        config: CliConfig,
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
            profile: None,
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
        Command::Init(args) => run_init(kind, args, runtime),
        Command::Config { command } => run_config_command(kind, command),
        Command::Profile { command } => run_profile_command(kind, command, runtime),
        Command::Namespace { command } => run_namespace_command(kind, command),
        Command::Use(args) => run_namespace_use(kind, args),
        Command::Current(args) => run_current(kind, args),
        Command::Ls(args) => run_filesystem_ls(kind, args),
        Command::Stat(args) => run_filesystem_stat(kind, args),
        Command::Cat(args) => run_filesystem_cat(kind, args),
        Command::Get(args) => run_filesystem_get(kind, args, runtime),
        Command::Put(args) => run_filesystem_put(kind, args),
        Command::Rm(args) => run_filesystem_rm(kind, args),
        Command::Mv(args) => run_filesystem_mv(kind, args),
        Command::Cp(args) => run_filesystem_cp(kind, args),
    }
}

// --- init ---

fn run_init(
    kind: CommandKind,
    args: InitArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let config_path = default_config_path().map_err(|error| fail(kind, None, None, error))?;

    let result = (|| -> Result<(String, ProfileConfig), CliError> {
        if config_path.exists() {
            return Err(CliError::config_already_exists(
                &config_path.display().to_string(),
            ));
        }

        let name = match &args.name {
            Some(name) => name.clone(),
            None if runtime.interactive => prompt::prompt_line_default("profile name", "default")?,
            None => "default".to_owned(),
        };
        let profile = build_profile_from_create_spec(create_profile_spec_from_init(args), runtime)?;

        let mut config = load_or_default_config(&config_path)?;
        let (profile_name, redacted) = add_profile(&mut config, &name, profile)?;
        config.default_profile = Some(profile_name.clone());
        save_config(&config_path, &config)?;
        Ok((profile_name, redacted))
    })()
    .map_err(|error| fail(kind, None, None, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(result.0),
        mode: Some(result.1.mode_str().to_owned()),
        data: CommandData::Profile(result.1),
    })
}

// --- config ---

fn run_config_command(
    kind: CommandKind,
    command: ConfigCommand,
) -> Result<CommandOutput, CommandFailure> {
    let config_path = default_config_path().map_err(|error| fail(kind, None, None, error))?;
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

// --- profile ---

fn run_profile_command(
    kind: CommandKind,
    command: ProfileCommand,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let config_path = default_config_path().map_err(|error| fail(kind, None, None, error))?;
    match command {
        ProfileCommand::Create(args) => run_profile_create(kind, &config_path, args, runtime),
        ProfileCommand::List => {
            let config = load_config_if_exists(&config_path)
                .map_err(|error| fail(kind, None, None, error))?;
            Ok(CommandOutput {
                kind,
                profile: None,
                mode: None,
                data: CommandData::ProfileList {
                    default_profile: config.as_ref().and_then(|c| c.default_profile.clone()),
                    profiles: list_profiles(config.as_ref()),
                },
            })
        }
        ProfileCommand::Show { name } => {
            let config =
                load_config(&config_path).map_err(|error| fail(kind, name.clone(), None, error))?;
            let (profile_name, redacted) = show_profile(&config, name.as_deref())
                .map_err(|error| fail(kind, name.clone(), None, error))?;
            Ok(CommandOutput {
                kind,
                profile: Some(profile_name),
                mode: Some(redacted.mode_str().to_owned()),
                data: CommandData::Profile(redacted),
            })
        }
        ProfileCommand::Update(args) => run_profile_update(kind, &config_path, args, runtime),
        ProfileCommand::Remove { name } => run_profile_remove(kind, &config_path, &name, runtime),
        ProfileCommand::Use { name } => run_profile_use(kind, &config_path, &name),
    }
}

fn run_profile_create(
    kind: CommandKind,
    config_path: &Path,
    args: ProfileCreateArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let name = args.name.clone();
    let result = (|| -> Result<(String, ProfileConfig), CliError> {
        let profile =
            build_profile_from_create_spec(create_profile_spec_from_create(args), runtime)?;
        let mut config = load_or_default_config(config_path)?;
        let (profile_name, redacted) = add_profile(&mut config, &name, profile)?;
        save_config(config_path, &config)?;
        Ok((profile_name, redacted))
    })()
    .map_err(|error| fail(kind, Some(name.clone()), None, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(result.0),
        mode: Some(result.1.mode_str().to_owned()),
        data: CommandData::Profile(result.1),
    })
}

fn run_profile_update(
    kind: CommandKind,
    config_path: &Path,
    args: ProfileUpdateArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let name = args.name.clone();
    let result = (|| -> Result<(String, ProfileConfig), CliError> {
        let mut config = load_config(config_path)?;
        let existing = config
            .profiles
            .get(&name)
            .ok_or_else(|| CliError::profile_not_found(&name))?
            .clone();

        let has_flags = args.root.is_some()
            || args.bucket.is_some()
            || args.region.is_some()
            || args.access_key_id.is_some()
            || args.secret_access_key.is_some()
            || args.endpoint_url.is_some()
            || args.session_token.is_some()
            || args.account_id.is_some()
            || args.key_prefix.is_some()
            || args.server_url.is_some()
            || args.auth_token.is_some();

        let updated = if has_flags {
            apply_update_flags(existing, &args)?
        } else if runtime.interactive {
            apply_update_interactive(existing)?
        } else {
            return Err(CliError::non_interactive_input_required(
                "update flags (e.g. --root, --bucket)",
            ));
        };

        let (profile_name, redacted) = update_profile(&mut config, &name, updated)?;
        save_config(config_path, &config)?;
        Ok((profile_name, redacted))
    })()
    .map_err(|error| fail(kind, Some(name.clone()), None, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(result.0),
        mode: Some(result.1.mode_str().to_owned()),
        data: CommandData::Profile(result.1),
    })
}

fn run_profile_remove(
    kind: CommandKind,
    config_path: &Path,
    name: &str,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    if runtime.interactive {
        let confirmed = prompt::prompt_confirm(&format!("remove profile `{name}`?"))
            .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
        if !confirmed {
            return Err(fail(
                kind,
                Some(name.to_owned()),
                None,
                CliError::new("cancelled", "operation cancelled"),
            ));
        }
    }

    let mut config =
        load_config(config_path).map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
    let removed = remove_profile(&mut config, name)
        .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
    let mode = removed.mode.clone();
    save_config(config_path, &config)
        .map_err(|error| fail(kind, Some(name.to_owned()), Some(mode.clone()), error))?;
    Ok(CommandOutput {
        kind,
        profile: Some(name.to_owned()),
        mode: Some(mode),
        data: CommandData::ProfileSummary(removed),
    })
}

fn run_profile_use(
    kind: CommandKind,
    config_path: &Path,
    name: &str,
) -> Result<CommandOutput, CommandFailure> {
    let mut config =
        load_config(config_path).map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
    make_default_profile(&mut config, name)
        .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
    save_config(config_path, &config)
        .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
    Ok(CommandOutput {
        kind,
        profile: Some(name.to_owned()),
        mode: None,
        data: CommandData::DefaultProfile {
            name: name.to_owned(),
        },
    })
}

// --- namespace ---

fn run_namespace_command(
    kind: CommandKind,
    command: NamespaceCommand,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        NamespaceCommand::Create(args) => run_namespace_create(kind, args),
        NamespaceCommand::Fork(args) => run_namespace_fork(kind, args),
        NamespaceCommand::List(args) => run_namespace_list(kind, args),
    }
}

fn run_namespace_create(
    kind: CommandKind,
    args: NamespaceCreateArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(explicit_profile)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    validate_namespace_id(&args.namespace_id).map_err(|error| {
        fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(mode.clone()),
            error,
        )
    })?;
    let namespace = resolved
        .target
        .backend()
        .create_namespace(&args.namespace_id)
        .map_err(|error| {
            fail(
                kind,
                Some(resolved.profile_name.clone()),
                Some(mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::NamespaceSummary(namespace),
    })
}

fn run_namespace_fork(
    kind: CommandKind,
    args: NamespaceForkArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(explicit_profile)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    validate_namespace_id(&args.source).map_err(|error| {
        fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(mode.clone()),
            error,
        )
    })?;
    validate_namespace_id(&args.new_namespace_id).map_err(|error| {
        fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(mode.clone()),
            error,
        )
    })?;
    let namespace = resolved
        .target
        .backend()
        .fork_namespace(&args.source, &args.new_namespace_id)
        .map_err(|error| {
            fail(
                kind,
                Some(resolved.profile_name.clone()),
                Some(mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::NamespaceSummary(namespace),
    })
}

fn run_namespace_list(
    kind: CommandKind,
    args: NamespaceListArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(explicit_profile)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let namespaces = resolved
        .target
        .backend()
        .list_namespaces()
        .map_err(|error| {
            fail(
                kind,
                Some(resolved.profile_name.clone()),
                Some(mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::NamespaceList { namespaces },
    })
}

fn run_namespace_use(
    kind: CommandKind,
    args: NamespaceUseArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let mut loaded = load_cli_config()
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let resolved = resolve_target_profile_from_config(&loaded.config, explicit_profile)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    validate_namespace_id(&args.namespace).map_err(|error| {
        fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(mode.clone()),
            error,
        )
    })?;

    let namespaces = resolved
        .target
        .backend()
        .list_namespaces()
        .map_err(|error| {
            fail(
                kind,
                Some(resolved.profile_name.clone()),
                Some(mode.clone()),
                error,
            )
        })?;
    if !namespaces
        .iter()
        .any(|candidate| candidate.namespace_id.as_str() == args.namespace)
    {
        return Err(fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(mode.clone()),
            CliError::new(
                "namespace_not_found",
                format!("namespace `{}` does not exist", args.namespace),
            ),
        ));
    }

    set_default_namespace(&mut loaded.config, &resolved.profile_name, &args.namespace).map_err(
        |error| {
            fail(
                kind,
                Some(resolved.profile_name.clone()),
                Some(mode.clone()),
                error,
            )
        },
    )?;
    save_config(&loaded.path, &loaded.config).map_err(|error| {
        fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(mode.clone()),
            error,
        )
    })?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name.clone()),
        mode: Some(mode),
        data: CommandData::DefaultNamespace {
            profile: resolved.profile_name,
            namespace: args.namespace,
        },
    })
}

fn run_current(kind: CommandKind, args: CurrentArgs) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let loaded = load_cli_config()
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let (profile_name, profile) =
        crate::profiles::resolve_profile(&loaded.config, explicit_profile)
            .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = profile.mode_str().to_owned();

    Ok(CommandOutput {
        kind,
        profile: Some(profile_name.to_owned()),
        mode: Some(mode),
        data: CommandData::Current {
            profile: profile_name.to_owned(),
            namespace: default_namespace(profile).map(ToOwned::to_owned),
        },
    })
}

// --- filesystem ---

fn run_filesystem_ls(
    kind: CommandKind,
    args: FilesystemLsArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    let spec = namespace_path(
        &context.namespace,
        args.path.as_deref().unwrap_or("/"),
        true,
    )
    .map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let entries = context.target.backend().list_path(&spec).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::PathEntries { entries },
    })
}

fn run_filesystem_stat(
    kind: CommandKind,
    args: FilesystemPathArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_path_lookup(kind, args, |backend, spec| {
        backend.stat_path(spec).map(CommandData::PathEntry)
    })
}

fn run_filesystem_cat(
    kind: CommandKind,
    args: FilesystemPathArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_path_lookup(kind, args, |backend, spec| {
        backend.read_file_bytes(spec).map(CommandData::StreamBytes)
    })
}

fn run_filesystem_get(
    kind: CommandKind,
    args: FilesystemGetArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    if runtime.json && args.local_destination.as_deref() == Some("-") {
        return Err(fail(
            kind,
            Some(context.profile_name),
            Some(context.mode),
            CliError::json_not_supported_for_streaming(),
        ));
    }

    let spec = namespace_path(&context.namespace, &args.remote_path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let entry = context.target.backend().stat_path(&spec).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    if entry.inode_kind == InodeKind::Dir {
        return Err(fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            CliError::invalid_input(format!(
                "directory operations are not available for `{}`",
                spec.absolute_path
            )),
        ));
    }

    let bytes = context
        .target
        .backend()
        .read_file_bytes(&spec)
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;
    let data = match args.local_destination.as_deref() {
        Some("-") => CommandData::StreamBytes(bytes),
        other => {
            let destination =
                destination_path_for_get(&spec.absolute_path, other).map_err(|error| {
                    fail(
                        kind,
                        Some(context.profile_name.clone()),
                        Some(context.mode.clone()),
                        error,
                    )
                })?;
            fs::write(&destination, &bytes).map_err(|error| {
                fail(
                    kind,
                    Some(context.profile_name.clone()),
                    Some(context.mode.clone()),
                    CliError::io(error),
                )
            })?;
            CommandData::FileTransfer {
                target: render_target(&context.namespace, &spec.absolute_path),
                destination: destination.display().to_string(),
                bytes_written: bytes.len() as u64,
            }
        }
    };

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data,
    })
}

fn run_filesystem_put(
    kind: CommandKind,
    args: FilesystemPutArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    let local_path = PathBuf::from(&args.local_path);
    if local_path == Path::new("-") {
        return Err(fail(
            kind,
            Some(context.profile_name),
            Some(context.mode),
            CliError::invalid_input("`-` is not supported for `put`"),
        ));
    }

    let metadata = fs::metadata(&local_path).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            CliError::io(error),
        )
    })?;
    if metadata.is_dir() {
        return Err(fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            CliError::invalid_input(format!(
                "directory operations are not available for `{}`",
                local_path.display()
            )),
        ));
    }

    let remote_path = match args.remote_path {
        Some(path) => normalize_absolute_path(&path, false),
        None => default_remote_put_path(&local_path),
    }
    .map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let spec = namespace_path(&context.namespace, &remote_path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let bytes = fs::read(&local_path).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            CliError::io(error),
        )
    })?;
    let result = context
        .target
        .backend()
        .put_file_bytes(&spec, &bytes, args.force)
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, &spec.absolute_path),
            committed_seq: result.committed_seq.0,
        },
    })
}

fn run_filesystem_rm(
    kind: CommandKind,
    args: FilesystemPathArgs,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let result = context
        .target
        .backend()
        .delete_path(&spec)
        .map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::FileMutation {
            target: render_target(&context.namespace, &spec.absolute_path),
            committed_seq: result.committed_seq.0,
        },
    })
}

fn run_filesystem_mv(
    kind: CommandKind,
    args: FilesystemMoveArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_move(kind, args, false)
}

fn run_filesystem_cp(
    kind: CommandKind,
    args: FilesystemMoveArgs,
) -> Result<CommandOutput, CommandFailure> {
    run_filesystem_move(kind, args, true)
}

// --- create/update helpers ---

#[derive(Debug, Clone)]
struct CreateProfileSpec {
    mode: Option<String>,
    store_kind: Option<String>,
    root: Option<String>,
    key_prefix: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint_url: Option<String>,
    session_token: Option<String>,
    force_path_style: bool,
    account_id: Option<String>,
    server_url: Option<String>,
    auth_token: Option<String>,
}

fn create_profile_spec_from_init(args: InitArgs) -> CreateProfileSpec {
    CreateProfileSpec {
        mode: args.mode,
        store_kind: args.store_kind,
        root: args.root,
        key_prefix: args.key_prefix,
        bucket: args.bucket,
        region: args.region,
        access_key_id: args.access_key_id,
        secret_access_key: args.secret_access_key,
        endpoint_url: args.endpoint_url,
        session_token: args.session_token,
        force_path_style: args.force_path_style,
        account_id: args.account_id,
        server_url: args.server_url,
        auth_token: args.auth_token,
    }
}

fn create_profile_spec_from_create(args: ProfileCreateArgs) -> CreateProfileSpec {
    CreateProfileSpec {
        mode: args.mode,
        store_kind: args.store_kind,
        root: args.root,
        key_prefix: args.key_prefix,
        bucket: args.bucket,
        region: args.region,
        access_key_id: args.access_key_id,
        secret_access_key: args.secret_access_key,
        endpoint_url: args.endpoint_url,
        session_token: args.session_token,
        force_path_style: args.force_path_style,
        account_id: args.account_id,
        server_url: args.server_url,
        auth_token: args.auth_token,
    }
}

fn build_profile_from_create_spec(
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    let mode = match spec.mode.as_deref() {
        Some("local") => "local".to_owned(),
        Some("remote") => "remote".to_owned(),
        Some(other) => {
            return Err(CliError::invalid_input(format!(
                "unknown mode: `{other}` (expected local or remote)"
            )))
        }
        None if runtime.interactive => prompt::prompt_choice("mode", &["local", "remote"])?,
        None => {
            return Err(CliError::non_interactive_input_required("mode"));
        }
    };

    match mode.as_str() {
        "local" => build_local_profile(spec, runtime),
        "remote" => build_remote_profile(spec, runtime),
        _ => unreachable!(),
    }
}

fn build_local_profile(
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    reject_create_flag("server-url", spec.server_url.is_some(), "local")?;
    reject_create_flag("auth-token", spec.auth_token.is_some(), "local")?;

    let store_kind = match spec.store_kind.as_deref() {
        Some("local-fs") => "local-fs",
        Some("aws-s3") => "aws-s3",
        Some("cloudflare-r2") => "cloudflare-r2",
        Some(other) => {
            return Err(CliError::invalid_input(format!(
                "unknown store kind: `{other}` (expected local-fs, aws-s3, or cloudflare-r2)"
            )))
        }
        None if runtime.interactive => {
            return prompt::prompt_choice("store kind", &["aws-s3", "cloudflare-r2", "local-fs"])
                .and_then(|choice| {
                    build_local_profile(
                        CreateProfileSpec {
                            store_kind: Some(choice),
                            ..spec
                        },
                        runtime,
                    )
                });
        }
        None => return Err(CliError::non_interactive_input_required("store-kind")),
    };

    let store = match store_kind {
        "local-fs" => {
            reject_create_flag("bucket", spec.bucket.is_some(), "local-fs")?;
            reject_create_flag("region", spec.region.is_some(), "local-fs")?;
            reject_create_flag("access-key-id", spec.access_key_id.is_some(), "local-fs")?;
            reject_create_flag(
                "secret-access-key",
                spec.secret_access_key.is_some(),
                "local-fs",
            )?;
            reject_create_flag("endpoint-url", spec.endpoint_url.is_some(), "local-fs")?;
            reject_create_flag("session-token", spec.session_token.is_some(), "local-fs")?;
            reject_create_flag("account-id", spec.account_id.is_some(), "local-fs")?;
            reject_create_flag("force-path-style", spec.force_path_style, "local-fs")?;
            StoreConfig::LocalFs {
                root: require_or_prompt(spec.root.as_ref(), "root", runtime)?,
                key_prefix: spec.key_prefix,
            }
        }
        "aws-s3" => {
            reject_create_flag("root", spec.root.is_some(), "aws-s3")?;
            reject_create_flag("account-id", spec.account_id.is_some(), "aws-s3")?;
            StoreConfig::AwsS3 {
                bucket: require_or_prompt(spec.bucket.as_ref(), "bucket name", runtime)?,
                region: require_or_prompt_region(spec.region.as_ref(), runtime)?,
                endpoint_url: spec.endpoint_url,
                access_key_id: require_or_prompt(
                    spec.access_key_id.as_ref(),
                    "access-key-id",
                    runtime,
                )?,
                secret_access_key: require_or_prompt(
                    spec.secret_access_key.as_ref(),
                    "secret-access-key",
                    runtime,
                )?,
                session_token: spec.session_token,
                key_prefix: spec.key_prefix,
                force_path_style: if spec.force_path_style {
                    Some(true)
                } else {
                    None
                },
            }
        }
        "cloudflare-r2" => {
            reject_create_flag("root", spec.root.is_some(), "cloudflare-r2")?;
            reject_create_flag("region", spec.region.is_some(), "cloudflare-r2")?;
            reject_create_flag(
                "session-token",
                spec.session_token.is_some(),
                "cloudflare-r2",
            )?;
            reject_create_flag("force-path-style", spec.force_path_style, "cloudflare-r2")?;
            StoreConfig::CloudflareR2 {
                bucket: require_or_prompt(spec.bucket.as_ref(), "bucket name", runtime)?,
                account_id: require_or_prompt(spec.account_id.as_ref(), "account-id", runtime)?,
                endpoint_url: require_or_prompt(
                    spec.endpoint_url.as_ref(),
                    "endpoint-url",
                    runtime,
                )?,
                access_key_id: require_or_prompt(
                    spec.access_key_id.as_ref(),
                    "access-key-id",
                    runtime,
                )?,
                secret_access_key: require_or_prompt(
                    spec.secret_access_key.as_ref(),
                    "secret-access-key",
                    runtime,
                )?,
                key_prefix: spec.key_prefix,
            }
        }
        _ => unreachable!(),
    };

    Ok(ProfileConfig::Local {
        store,
        default_namespace: None,
        writer_id: None,
        writer_version: None,
        lease_duration_ms: None,
    })
}

fn build_remote_profile(
    spec: CreateProfileSpec,
    runtime: RuntimeBehavior,
) -> Result<ProfileConfig, CliError> {
    reject_create_flag("store-kind", spec.store_kind.is_some(), "remote")?;
    reject_create_flag("root", spec.root.is_some(), "remote")?;
    reject_create_flag("key-prefix", spec.key_prefix.is_some(), "remote")?;
    reject_create_flag("bucket", spec.bucket.is_some(), "remote")?;
    reject_create_flag("region", spec.region.is_some(), "remote")?;
    reject_create_flag("access-key-id", spec.access_key_id.is_some(), "remote")?;
    reject_create_flag(
        "secret-access-key",
        spec.secret_access_key.is_some(),
        "remote",
    )?;
    reject_create_flag("endpoint-url", spec.endpoint_url.is_some(), "remote")?;
    reject_create_flag("session-token", spec.session_token.is_some(), "remote")?;
    reject_create_flag("force-path-style", spec.force_path_style, "remote")?;
    reject_create_flag("account-id", spec.account_id.is_some(), "remote")?;

    Ok(ProfileConfig::Remote {
        server_url: require_or_prompt(spec.server_url.as_ref(), "server url", runtime)?,
        default_namespace: None,
        auth_token: match spec.auth_token {
            Some(token) if token.trim().is_empty() => None,
            other => other,
        },
    })
}

fn require_or_prompt(
    value: Option<&String>,
    field: &str,
    runtime: RuntimeBehavior,
) -> Result<String, CliError> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v.clone()),
        _ if runtime.interactive => prompt::prompt_line(field),
        _ => Err(CliError::non_interactive_input_required(field)),
    }
}

fn require_or_prompt_region(
    value: Option<&String>,
    runtime: RuntimeBehavior,
) -> Result<String, CliError> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v.clone()),
        _ if runtime.interactive => prompt::prompt_fuzzy_choice("region", AWS_REGIONS, 0),
        _ => Err(CliError::non_interactive_input_required("region")),
    }
}

fn reject_create_flag(flag: &str, present: bool, profile_kind: &str) -> Result<(), CliError> {
    if present {
        return Err(CliError::invalid_input(format!(
            "`--{flag}` does not apply to {profile_kind} profiles"
        )));
    }
    Ok(())
}

fn apply_update_flags(
    existing: ProfileConfig,
    args: &ProfileUpdateArgs,
) -> Result<ProfileConfig, CliError> {
    match &existing {
        ProfileConfig::Local { store, .. } => {
            reject_flag("server-url", &args.server_url, "local")?;
            reject_flag("auth-token", &args.auth_token, "local")?;
            match store {
                StoreConfig::LocalFs { .. } => {
                    reject_flag("bucket", &args.bucket, "local-fs")?;
                    reject_flag("region", &args.region, "local-fs")?;
                    reject_flag("access-key-id", &args.access_key_id, "local-fs")?;
                    reject_flag("secret-access-key", &args.secret_access_key, "local-fs")?;
                    reject_flag("endpoint-url", &args.endpoint_url, "local-fs")?;
                    reject_flag("session-token", &args.session_token, "local-fs")?;
                    reject_flag("account-id", &args.account_id, "local-fs")?;
                }
                StoreConfig::AwsS3 { .. } => {
                    reject_flag("root", &args.root, "aws-s3")?;
                    reject_flag("account-id", &args.account_id, "aws-s3")?;
                }
                StoreConfig::CloudflareR2 { .. } => {
                    reject_flag("root", &args.root, "cloudflare-r2")?;
                    reject_flag("region", &args.region, "cloudflare-r2")?;
                    reject_flag("session-token", &args.session_token, "cloudflare-r2")?;
                }
            }
        }
        ProfileConfig::Remote { .. } => {
            reject_flag("root", &args.root, "remote")?;
            reject_flag("bucket", &args.bucket, "remote")?;
            reject_flag("region", &args.region, "remote")?;
            reject_flag("access-key-id", &args.access_key_id, "remote")?;
            reject_flag("secret-access-key", &args.secret_access_key, "remote")?;
            reject_flag("endpoint-url", &args.endpoint_url, "remote")?;
            reject_flag("session-token", &args.session_token, "remote")?;
            reject_flag("account-id", &args.account_id, "remote")?;
            reject_flag("key-prefix", &args.key_prefix, "remote")?;
        }
    }

    match existing {
        ProfileConfig::Local {
            store,
            default_namespace,
            writer_id,
            writer_version,
            lease_duration_ms,
        } => {
            let store = match store {
                StoreConfig::LocalFs { root, key_prefix } => StoreConfig::LocalFs {
                    root: args.root.clone().unwrap_or(root),
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                },
                StoreConfig::AwsS3 {
                    bucket,
                    region,
                    endpoint_url,
                    access_key_id,
                    secret_access_key,
                    session_token,
                    key_prefix,
                    force_path_style,
                } => StoreConfig::AwsS3 {
                    bucket: args.bucket.clone().unwrap_or(bucket),
                    region: args.region.clone().unwrap_or(region),
                    endpoint_url: args.endpoint_url.clone().or(endpoint_url),
                    access_key_id: args.access_key_id.clone().unwrap_or(access_key_id),
                    secret_access_key: args.secret_access_key.clone().unwrap_or(secret_access_key),
                    session_token: args.session_token.clone().or(session_token),
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                    force_path_style,
                },
                StoreConfig::CloudflareR2 {
                    bucket,
                    account_id,
                    endpoint_url,
                    access_key_id,
                    secret_access_key,
                    key_prefix,
                } => StoreConfig::CloudflareR2 {
                    bucket: args.bucket.clone().unwrap_or(bucket),
                    account_id: args.account_id.clone().unwrap_or(account_id),
                    endpoint_url: args.endpoint_url.clone().unwrap_or(endpoint_url),
                    access_key_id: args.access_key_id.clone().unwrap_or(access_key_id),
                    secret_access_key: args.secret_access_key.clone().unwrap_or(secret_access_key),
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                },
            };
            Ok(ProfileConfig::Local {
                store,
                default_namespace,
                writer_id,
                writer_version,
                lease_duration_ms,
            })
        }
        ProfileConfig::Remote {
            server_url,
            default_namespace,
            auth_token,
        } => Ok(ProfileConfig::Remote {
            server_url: args.server_url.clone().unwrap_or(server_url),
            default_namespace,
            auth_token: args.auth_token.clone().or(auth_token),
        }),
    }
}

fn reject_flag(flag: &str, value: &Option<String>, profile_kind: &str) -> Result<(), CliError> {
    if value.is_some() {
        return Err(CliError::invalid_input(format!(
            "`--{flag}` does not apply to {profile_kind} profiles"
        )));
    }
    Ok(())
}

fn apply_update_interactive(existing: ProfileConfig) -> Result<ProfileConfig, CliError> {
    match existing {
        ProfileConfig::Local {
            store,
            default_namespace,
            writer_id,
            writer_version,
            lease_duration_ms,
        } => {
            let store = match store {
                StoreConfig::LocalFs { root, key_prefix } => StoreConfig::LocalFs {
                    root: prompt::prompt_line_default("root", &root)?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
                StoreConfig::AwsS3 {
                    bucket,
                    region,
                    endpoint_url,
                    access_key_id,
                    secret_access_key,
                    session_token,
                    key_prefix,
                    force_path_style,
                } => StoreConfig::AwsS3 {
                    bucket: prompt::prompt_line_default("bucket name", &bucket)?,
                    region: {
                        let default_idx =
                            AWS_REGIONS.iter().position(|r| *r == region).unwrap_or(0);
                        prompt::prompt_fuzzy_choice("region", AWS_REGIONS, default_idx)?
                    },
                    access_key_id: prompt::prompt_line_default("access key id", &access_key_id)?,
                    secret_access_key: prompt::prompt_line_default(
                        "secret access key",
                        &secret_access_key,
                    )?,
                    endpoint_url: prompt::prompt_optional("endpoint url", endpoint_url.as_deref())?,
                    session_token: prompt::prompt_optional(
                        "session token",
                        session_token.as_deref(),
                    )?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                    force_path_style,
                },
                StoreConfig::CloudflareR2 {
                    bucket,
                    account_id,
                    endpoint_url,
                    access_key_id,
                    secret_access_key,
                    key_prefix,
                } => StoreConfig::CloudflareR2 {
                    bucket: prompt::prompt_line_default("bucket name", &bucket)?,
                    account_id: prompt::prompt_line_default("account id", &account_id)?,
                    endpoint_url: prompt::prompt_line_default("endpoint url", &endpoint_url)?,
                    access_key_id: prompt::prompt_line_default("access key id", &access_key_id)?,
                    secret_access_key: prompt::prompt_line_default(
                        "secret access key",
                        &secret_access_key,
                    )?,
                    key_prefix: prompt::prompt_optional("key prefix", key_prefix.as_deref())?,
                },
            };
            Ok(ProfileConfig::Local {
                store,
                default_namespace,
                writer_id,
                writer_version,
                lease_duration_ms,
            })
        }
        ProfileConfig::Remote {
            server_url,
            default_namespace,
            auth_token,
        } => Ok(ProfileConfig::Remote {
            server_url: prompt::prompt_line_default("server url", &server_url)?,
            default_namespace,
            auth_token: prompt::prompt_optional("auth token", auth_token.as_deref())?,
        }),
    }
}

// --- filesystem helpers ---

struct CommandContext {
    profile_name: String,
    mode: String,
    namespace: String,
    target: crate::backend::ResolvedTarget,
}

fn resolve_command_context(
    kind: CommandKind,
    target: &TargetSelectorArgs,
) -> Result<CommandContext, CommandFailure> {
    let explicit_profile = target.profile.profile.as_deref();
    let loaded = load_cli_config()
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let resolved = resolve_target_profile_from_config(&loaded.config, explicit_profile)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let namespace = resolve_namespace(
        &loaded.config,
        explicit_profile,
        target.namespace.as_deref(),
    )
    .map_err(|error| {
        fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(mode.clone()),
            error,
        )
    })?
    .namespace;

    // Keep the loaded config alive long enough for the backend borrow to remain valid within the caller.
    Ok(CommandContext {
        profile_name: resolved.profile_name,
        mode,
        namespace,
        target: resolved.target,
    })
}

fn run_filesystem_path_lookup<F>(
    kind: CommandKind,
    args: FilesystemPathArgs,
    op: F,
) -> Result<CommandOutput, CommandFailure>
where
    F: FnOnce(&dyn crate::backend::Backend, &NamespacePath) -> Result<CommandData, CliError>,
{
    let context = resolve_command_context(kind, &args.target)?;
    let spec = namespace_path(&context.namespace, &args.path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let data = op(context.target.backend(), &spec).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data,
    })
}

fn run_filesystem_move(
    kind: CommandKind,
    args: FilesystemMoveArgs,
    copy: bool,
) -> Result<CommandOutput, CommandFailure> {
    let context = resolve_command_context(kind, &args.target)?;
    let from = namespace_path(&context.namespace, &args.source_path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;
    let to = namespace_path(&context.namespace, &args.dest_path, false).map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;

    let result = if copy {
        let entry = context.target.backend().stat_path(&from).map_err(|error| {
            fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                error,
            )
        })?;
        if entry.inode_kind == InodeKind::Dir {
            return Err(fail(
                kind,
                Some(context.profile_name.clone()),
                Some(context.mode.clone()),
                CliError::invalid_input(format!(
                    "directory operations are not available for `{}`",
                    from.absolute_path
                )),
            ));
        }
        context.target.backend().copy_path(&from, &to)
    } else {
        context.target.backend().move_path(&from, &to)
    }
    .map_err(|error| {
        fail(
            kind,
            Some(context.profile_name.clone()),
            Some(context.mode.clone()),
            error,
        )
    })?;

    Ok(CommandOutput {
        kind,
        profile: Some(context.profile_name),
        mode: Some(context.mode),
        data: CommandData::PathMove {
            from: render_target(&context.namespace, &from.absolute_path),
            to: render_target(&context.namespace, &to.absolute_path),
            committed_seq: result.committed_seq.0,
        },
    })
}

// --- general helpers ---

fn validate_namespace_id(namespace: &str) -> Result<(), CliError> {
    NamespaceId::parse(namespace)
        .map(|_| ())
        .map_err(|error| CliError::invalid_input(error.to_string()))
}

fn namespace_path(
    namespace: &str,
    path: &str,
    allow_root: bool,
) -> Result<NamespacePath, CliError> {
    validate_namespace_id(namespace)?;
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
