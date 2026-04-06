use crate::args::{
    Cli, Command, CommandKind, ConfigCommand, FilesystemCommand, InitArgs, NamespaceCommand,
    ProfileAddCommand, ProfileCommand, ProfileUpdateArgs, RuntimeBehavior,
};
use crate::config::{
    load_config, load_config_if_exists, load_or_default_config, save_config, ProfileConfig,
    StoreConfig,
};
use crate::error::CliError;
use crate::profiles::{
    add_profile, list_profiles, make_default_profile, remove_profile, show_profile,
    update_profile, ProfileSummary,
};
use crate::prompt;
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
    DefaultProfile {
        name: String,
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
        Command::Init(args) => run_init(kind, cli.config.as_deref(), args, runtime),
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

// --- init ---

fn run_init(
    kind: CommandKind,
    explicit_config: Option<&Path>,
    args: InitArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let config_path =
        resolved_config_path(explicit_config).map_err(|error| fail(kind, None, None, error))?;

    let result = (|| -> Result<(String, ProfileConfig), CliError> {
        let name = match &args.name {
            Some(n) => n.clone(),
            None if runtime.interactive => prompt::prompt_line_default("profile name", "default")?,
            None => "default".to_owned(),
        };

        let store = build_local_store_interactive(&args, runtime)?;
        let profile = ProfileConfig::Local {
            store,
            writer_id: None,
            writer_version: None,
            lease_duration_ms: None,
        };

        let mut config = load_or_default_config(&config_path)?;
        let (profile_name, redacted) = add_profile(&mut config, &name, profile)?;
        config.default_profile = profile_name.clone();
        save_config(&config_path, &config)?;
        Ok((profile_name, redacted))
    })()
    .map_err(|error| fail(kind, None, Some("local".to_owned()), error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(result.0),
        mode: Some("local".to_owned()),
        data: CommandData::Profile(result.1),
    })
}

fn build_local_store_interactive(
    args: &InitArgs,
    runtime: RuntimeBehavior,
) -> Result<StoreConfig, CliError> {
    let store_kind = match &args.store_kind {
        Some(k) => k.clone(),
        None if runtime.interactive => {
            prompt::prompt_choice("store kind", &["local-fs", "aws-s3", "cloudflare-r2"])?
        }
        None => {
            return Err(CliError::non_interactive_input_required("store-kind"));
        }
    };

    match store_kind.as_str() {
        "local-fs" => {
            let root = require_or_prompt(&args.root, "root", runtime)?;
            Ok(StoreConfig::LocalFs {
                root,
                key_prefix: args.key_prefix.clone(),
            })
        }
        "aws-s3" => {
            let bucket = require_or_prompt(&args.bucket, "bucket", runtime)?;
            let region = require_or_prompt(&args.region, "region", runtime)?;
            let access_key_id =
                require_or_prompt(&args.access_key_id, "access-key-id", runtime)?;
            let secret_access_key =
                require_or_prompt(&args.secret_access_key, "secret-access-key", runtime)?;
            Ok(StoreConfig::AwsS3 {
                bucket,
                region,
                endpoint_url: args.endpoint_url.clone(),
                access_key_id,
                secret_access_key,
                session_token: None,
                key_prefix: args.key_prefix.clone(),
                force_path_style: None,
            })
        }
        "cloudflare-r2" => {
            let bucket = require_or_prompt(&args.bucket, "bucket", runtime)?;
            let account_id = require_or_prompt(&args.account_id, "account-id", runtime)?;
            let endpoint_url =
                require_or_prompt(&args.endpoint_url, "endpoint-url", runtime)?;
            let access_key_id =
                require_or_prompt(&args.access_key_id, "access-key-id", runtime)?;
            let secret_access_key =
                require_or_prompt(&args.secret_access_key, "secret-access-key", runtime)?;
            Ok(StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                access_key_id,
                secret_access_key,
                key_prefix: args.key_prefix.clone(),
            })
        }
        other => Err(CliError::invalid_input(format!(
            "unknown store kind: `{other}` (expected local-fs, aws-s3, or cloudflare-r2)"
        ))),
    }
}

fn require_or_prompt(
    value: &Option<String>,
    field: &str,
    runtime: RuntimeBehavior,
) -> Result<String, CliError> {
    match value {
        Some(v) if !v.trim().is_empty() => Ok(v.clone()),
        _ if runtime.interactive => prompt::prompt_line(field),
        _ => Err(CliError::non_interactive_input_required(field)),
    }
}

// --- config ---

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

// --- profile ---

fn run_profile_command(
    kind: CommandKind,
    explicit_config: Option<&Path>,
    command: ProfileCommand,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let config_path =
        resolved_config_path(explicit_config).map_err(|error| fail(kind, None, None, error))?;
    match command {
        ProfileCommand::Add { command } => {
            run_profile_add(kind, &config_path, command, runtime)
        }
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
        ProfileCommand::Update(args) => {
            run_profile_update(kind, &config_path, args, runtime)
        }
        ProfileCommand::Remove { name } => {
            run_profile_remove(kind, &config_path, &name, runtime)
        }
        ProfileCommand::MakeDefault { name } => {
            run_profile_make_default(kind, &config_path, &name)
        }
    }
}

fn run_profile_add(
    kind: CommandKind,
    config_path: &Path,
    command: Option<ProfileAddCommand>,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let (name, profile) = match command {
        Some(cmd) => build_profile_from_add_command(cmd),
        None => build_profile_interactive(runtime)
            .map_err(|error| fail(kind, None, None, error))?,
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

fn build_profile_from_add_command(command: ProfileAddCommand) -> (String, ProfileConfig) {
    match command {
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
    }
}

fn build_profile_interactive(
    runtime: RuntimeBehavior,
) -> Result<(String, ProfileConfig), CliError> {
    if !runtime.interactive {
        return Err(CliError::non_interactive_input_required(
            "subcommand (local-fs, aws-s3, cloudflare-r2, or remote)",
        ));
    }

    let name = prompt::prompt_line("profile name")?;
    let mode = prompt::prompt_choice("mode", &["local", "remote"])?;

    match mode.as_str() {
        "local" => {
            let store_kind =
                prompt::prompt_choice("store kind", &["local-fs", "aws-s3", "cloudflare-r2"])?;
            let store = match store_kind.as_str() {
                "local-fs" => StoreConfig::LocalFs {
                    root: prompt::prompt_line("root")?,
                    key_prefix: prompt::prompt_optional("key prefix", None)?,
                },
                "aws-s3" => StoreConfig::AwsS3 {
                    bucket: prompt::prompt_line("bucket")?,
                    region: prompt::prompt_line("region")?,
                    access_key_id: prompt::prompt_line("access key id")?,
                    secret_access_key: prompt::prompt_line("secret access key")?,
                    endpoint_url: prompt::prompt_optional("endpoint url", None)?,
                    session_token: None,
                    key_prefix: prompt::prompt_optional("key prefix", None)?,
                    force_path_style: None,
                },
                "cloudflare-r2" => StoreConfig::CloudflareR2 {
                    bucket: prompt::prompt_line("bucket")?,
                    account_id: prompt::prompt_line("account id")?,
                    endpoint_url: prompt::prompt_line("endpoint url")?,
                    access_key_id: prompt::prompt_line("access key id")?,
                    secret_access_key: prompt::prompt_line("secret access key")?,
                    key_prefix: prompt::prompt_optional("key prefix", None)?,
                },
                _ => unreachable!(),
            };
            Ok((
                name,
                ProfileConfig::Local {
                    store,
                    writer_id: None,
                    writer_version: None,
                    lease_duration_ms: None,
                },
            ))
        }
        "remote" => {
            let server_url = prompt::prompt_line("server url")?;
            let auth_token = prompt::prompt_optional("auth token", None)?;
            Ok((
                name,
                ProfileConfig::Remote {
                    server_url,
                    auth_token,
                },
            ))
        }
        _ => unreachable!(),
    }
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

fn apply_update_flags(
    existing: ProfileConfig,
    args: &ProfileUpdateArgs,
) -> Result<ProfileConfig, CliError> {
    match existing {
        ProfileConfig::Local {
            store,
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
                    secret_access_key: args
                        .secret_access_key
                        .clone()
                        .unwrap_or(secret_access_key),
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
                    secret_access_key: args
                        .secret_access_key
                        .clone()
                        .unwrap_or(secret_access_key),
                    key_prefix: args.key_prefix.clone().or(key_prefix),
                },
            };
            Ok(ProfileConfig::Local {
                store,
                writer_id,
                writer_version,
                lease_duration_ms,
            })
        }
        ProfileConfig::Remote {
            server_url,
            auth_token,
        } => Ok(ProfileConfig::Remote {
            server_url: args.server_url.clone().unwrap_or(server_url),
            auth_token: args.auth_token.clone().or(auth_token),
        }),
    }
}

fn apply_update_interactive(existing: ProfileConfig) -> Result<ProfileConfig, CliError> {
    match existing {
        ProfileConfig::Local {
            store,
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
                    bucket: prompt::prompt_line_default("bucket", &bucket)?,
                    region: prompt::prompt_line_default("region", &region)?,
                    access_key_id: prompt::prompt_line_default("access key id", &access_key_id)?,
                    secret_access_key: prompt::prompt_line_default(
                        "secret access key",
                        &secret_access_key,
                    )?,
                    endpoint_url: prompt::prompt_optional(
                        "endpoint url",
                        endpoint_url.as_deref(),
                    )?,
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
                    bucket: prompt::prompt_line_default("bucket", &bucket)?,
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
                writer_id,
                writer_version,
                lease_duration_ms,
            })
        }
        ProfileConfig::Remote {
            server_url,
            auth_token,
        } => Ok(ProfileConfig::Remote {
            server_url: prompt::prompt_line_default("server url", &server_url)?,
            auth_token: prompt::prompt_optional("auth token", auth_token.as_deref())?,
        }),
    }
}

fn run_profile_remove(
    kind: CommandKind,
    config_path: &Path,
    name: &str,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    if runtime.interactive {
        let confirmed =
            prompt::prompt_confirm(&format!("remove profile `{name}`?"))
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

    let mut config = load_config(config_path)
        .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
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

fn run_profile_make_default(
    kind: CommandKind,
    config_path: &Path,
    name: &str,
) -> Result<CommandOutput, CommandFailure> {
    let mut config = load_config(config_path)
        .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
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

// --- filesystem ---

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

// --- helpers ---

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
