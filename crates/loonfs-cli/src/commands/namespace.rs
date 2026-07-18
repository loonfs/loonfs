//! `loon namespace` commands: create, fork, delete, use, and current.

use super::context::{fail, fail_for, validate_namespace_id};
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{
    CommandKind, CurrentArgs, NamespaceCommand, NamespaceCreateArgs, NamespaceDeleteArgs,
    NamespaceForkArgs, NamespaceUseArgs, RuntimeBehavior,
};
use crate::config::save_config;
use crate::error::CliError;
use crate::profiles::{default_namespace, set_default_namespace};
use crate::prompt::prompt_line;
use crate::resolve::{load_cli_config, resolve_target_profile, resolve_target_profile_from_config};

// --- namespace ---

pub(crate) async fn run_namespace_command(
    kind: CommandKind,
    command: NamespaceCommand,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        NamespaceCommand::Create(args) => run_namespace_create(kind, args).await,
        NamespaceCommand::Delete(args) => run_namespace_delete(kind, args, runtime).await,
        NamespaceCommand::Fork(args) => run_namespace_fork(kind, args).await,
    }
}

async fn run_namespace_create(
    kind: CommandKind,
    args: NamespaceCreateArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(explicit_profile, args.profile.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    validate_namespace_id(&args.namespace_id)
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    let namespace = resolved
        .target
        .backend()
        .create_namespace(&args.namespace_id)
        .await
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::NamespaceSummary(namespace),
    })
}

async fn run_namespace_delete(
    kind: CommandKind,
    args: NamespaceDeleteArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(explicit_profile, args.profile.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    validate_namespace_id(&args.namespace_id)
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
        .backend()
        .delete_namespace(&args.namespace_id, args.expected_head_seq)
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
    args: NamespaceForkArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let resolved = resolve_target_profile(explicit_profile, args.profile.no_retry)
        .await
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    validate_namespace_id(&args.source)
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    validate_namespace_id(&args.new_namespace_id)
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    let namespace = resolved
        .target
        .backend()
        .fork_namespace(&args.source, &args.new_namespace_id)
        .await
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(resolved.profile_name),
        mode: Some(mode),
        data: CommandData::NamespaceSummary(namespace),
    })
}

pub(crate) async fn run_namespace_use(
    kind: CommandKind,
    args: NamespaceUseArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let mut loaded = load_cli_config()
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let resolved =
        resolve_target_profile_from_config(&loaded.config, explicit_profile, args.profile.no_retry)
            .await
            .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    validate_namespace_id(&args.namespace)
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    resolved
        .target
        .backend()
        .namespace_status(&args.namespace)
        .await
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;

    set_default_namespace(&mut loaded.config, &resolved.profile_name, &args.namespace)
        .map_err(|error| fail_for(kind, &resolved.profile_name, &mode, error))?;
    save_config(&loaded.path, &loaded.config)
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
    args: CurrentArgs,
) -> Result<CommandOutput, CommandFailure> {
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
