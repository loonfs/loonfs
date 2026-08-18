//! `loonfs namespace` commands: create, fork, delete, use, and current.

use super::context::{fail, fail_for, parse_public_ordinal_arg};
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{
    CommandKind, CurrentArgs, NamespaceCommand, NamespaceCreateArgs, NamespaceDeleteArgs,
    NamespaceForkArgs, NamespaceShowArgs, NamespaceUseArgs, RuntimeBehavior,
};
use crate::config::mutate_config;
use crate::error::CliError;
use crate::profiles::set_default_namespace;
use crate::prompt::prompt_line;
use crate::resolve::{
    load_cli_config, parse_namespace_id, resolve_namespace, resolve_target_profile,
    resolve_target_profile_from_config,
};
use std::path::Path;

// --- namespace ---

pub(crate) async fn run_namespace_command(
    kind: CommandKind,
    config_path: &Path,
    command: NamespaceCommand,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        NamespaceCommand::Create(args) => run_namespace_create(kind, config_path, args).await,
        NamespaceCommand::Show(args) => run_namespace_show(kind, config_path, args).await,
        NamespaceCommand::Delete(args) => {
            run_namespace_delete(kind, config_path, args, runtime).await
        }
        NamespaceCommand::Fork(args) => run_namespace_fork(kind, config_path, args).await,
    }
}

async fn run_namespace_show(
    kind: CommandKind,
    config_path: &Path,
    args: NamespaceShowArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.target.profile.profile.as_deref();
    let loaded = load_cli_config(config_path)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let resolved = resolve_target_profile_from_config(
        &loaded.config,
        explicit_profile,
        args.target.request.no_retry,
    )
    .await
    .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let explicit_namespace = args
        .namespace_id
        .as_deref()
        .or(args.target.namespace.as_deref());
    let namespace_id = resolve_namespace(&loaded.config, explicit_profile, explicit_namespace)
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?
        .namespace;
    let namespace = resolved
        .target
        .namespace_status(&namespace_id)
        .await
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::NamespaceStatus(namespace),
    })
}

async fn run_namespace_create(
    kind: CommandKind,
    config_path: &Path,
    args: NamespaceCreateArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(config_path, explicit_profile, args.request.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let namespace_id = parse_namespace_id(&args.namespace_id)
        .map_err(|error| error.with_param("namespace_id"))
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    let namespace = resolved
        .target
        .create_namespace(&namespace_id)
        .await
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::NamespaceStatus(namespace),
    })
}

async fn run_namespace_delete(
    kind: CommandKind,
    config_path: &Path,
    args: NamespaceDeleteArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(config_path, explicit_profile, args.request.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let namespace_id = parse_namespace_id(&args.namespace_id)
        .map_err(|error| error.with_param("namespace_id"))
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    let expected_head_seq = args
        .expected_head_seq
        .map(|value| {
            parse_public_ordinal_arg("--expected-head-seq", value, loonfs_api::ChangeSeq::parse)
        })
        .transpose()
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    if !args.yes {
        // Without a terminal (or under --no-input / --json) there is no
        // prompt to answer; say what is required instead of surfacing the
        // prompt machinery's i/o error.
        if !runtime.interactive {
            return Err(fail(
                kind,
                Some(resolved.profile_name),
                Some(mode),
                CliError::non_interactive_input_required(
                    "deleting a namespace requires confirmation: pass --yes, or run \
                     interactively to confirm at the prompt",
                ),
            ));
        }
        // Deletion is terminal and retires the id; require the operator to
        // type the namespace id back (or pass --yes).
        let typed = prompt_line(&format!(
            "deleting `{}` is permanent and retires the id; type the namespace id to confirm",
            args.namespace_id
        ))
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
        if typed.trim() != args.namespace_id {
            return Err(fail(
                kind,
                Some(resolved.profile_name),
                Some(mode),
                CliError::invalid_input(format!(
                    "confirmation `{typed}` does not match namespace id `{}`",
                    args.namespace_id
                )),
            ));
        }
    }

    let response = resolved
        .target
        .delete_namespace(&namespace_id, expected_head_seq)
        .await
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::NamespaceDeleted(response),
    })
}

async fn run_namespace_fork(
    kind: CommandKind,
    config_path: &Path,
    args: NamespaceForkArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(config_path, explicit_profile, args.request.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let source_namespace_id = parse_namespace_id(&args.source)
        .map_err(|error| error.with_param("source"))
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    let new_namespace_id = parse_namespace_id(&args.new_namespace_id)
        .map_err(|error| error.with_param("new_namespace_id"))
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    let namespace = resolved
        .target
        .fork_namespace(&source_namespace_id, &new_namespace_id)
        .await
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::NamespaceStatus(namespace),
    })
}

pub(crate) async fn run_namespace_use(
    kind: CommandKind,
    config_path: &Path,
    args: NamespaceUseArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let loaded = load_cli_config(config_path)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let resolved =
        resolve_target_profile_from_config(&loaded.config, explicit_profile, args.request.no_retry)
            .await
            .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let namespace_id = parse_namespace_id(&args.namespace)
        .map_err(|error| error.with_param("namespace"))
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    resolved
        .target
        .namespace_status(&namespace_id)
        .await
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    mutate_config(&loaded.path, |config| {
        set_default_namespace(config, &resolved.profile_name, &args.namespace)
    })
    .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

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

pub(crate) async fn run_namespace_current(
    kind: CommandKind,
    config_path: &Path,
    args: CurrentArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let loaded = load_cli_config(config_path)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let (profile_name, profile) =
        crate::profiles::resolve_profile(&loaded.config, explicit_profile)
            .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = profile.mode_str().to_owned();
    // `current` is a status command, so an unassigned namespace remains a
    // successful `null`; configured values still use normal flag/env/profile
    // precedence and validation.
    let namespace = match resolve_namespace(&loaded.config, explicit_profile, None) {
        Ok(resolved) => Some(resolved.namespace.to_string()),
        Err(error) if error.is_no_default_namespace() => None,
        Err(error) => return Err(fail_for(kind, profile_name, &mode, error)),
    };

    Ok(CommandOutput {
        kind,
        profile: Some(profile_name.to_owned()),
        mode: Some(mode),
        data: CommandData::Current {
            profile: profile_name.to_owned(),
            namespace,
        },
    })
}
