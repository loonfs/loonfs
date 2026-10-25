//! `loonfs namespace` commands: create, show, fork, and delete.

use super::context::{
    fail, fail_for, resolve_profile_context, resolve_profile_context_from_config,
};
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{
    CommandKind, CurrentArgs, NamespaceAccessArg, NamespaceCommand, NamespaceCreateArgs,
    NamespaceDeleteArgs, NamespaceForkArgs, NamespaceNamingArg, NamespaceShowArgs,
    NamespaceUseArgs, RuntimeBehavior,
};
use crate::config::mutate_config;
use crate::error::CliError;
use crate::profiles::set_default_namespace;
use crate::prompt::prompt_line;
use crate::resolve::{load_cli_config, resolve_actor, resolve_namespace};
use loonfs_client::CreateNamespaceOptions;
use loonfs_types::{AccessGrants, AccessRights, NamespaceAccess, NamespaceNaming, PrincipalScope};
use std::collections::BTreeMap;
use std::path::Path;

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
    let (mut context, profile) = resolve_profile_context_from_config(
        kind,
        &loaded.config,
        explicit_profile,
        args.target.request.no_retry,
        Some(&args.target.subject),
    )
    .await?;
    let explicit_namespace = args
        .namespace_id
        .as_ref()
        .or(args.target.namespace.as_ref());
    let namespace_id = resolve_namespace(&context.profile_name, profile, explicit_namespace)
        .map_err(|error| context.fail(kind, error))?
        .namespace;
    context.namespace = Some(namespace_id);
    let namespace = context
        .target
        .client
        .get_namespace(context.namespace())
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(kind, CommandData::NamespaceStatus(namespace)))
}

async fn run_namespace_create(
    kind: CommandKind,
    config_path: &Path,
    args: NamespaceCreateArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let loaded = load_cli_config(config_path)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let (context, profile) = resolve_profile_context_from_config(
        kind,
        &loaded.config,
        explicit_profile,
        args.request.no_retry,
        None,
    )
    .await?;
    let actor_id = resolve_actor(profile, args.actor.actor_id.as_ref())
        .map_err(|error| context.fail(kind, error))?;
    let namespace_id = args.namespace_id.clone();
    let access = namespace_access(&args).map_err(|error| context.fail(kind, error))?;
    let naming = match args.naming {
        NamespaceNamingArg::CaseInsensitive => NamespaceNaming::CaseInsensitive,
        NamespaceNamingArg::CaseSensitive => NamespaceNaming::CaseSensitive,
    };
    let namespace = context
        .target
        .client
        .create_namespace_with_options(
            &namespace_id,
            &actor_id,
            &CreateNamespaceOptions {
                access,
                naming,
                retention_window_ms: args.retention_window_ms,
            },
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(kind, CommandData::NamespaceStatus(namespace)))
}

fn namespace_access(args: &NamespaceCreateArgs) -> Result<NamespaceAccess, CliError> {
    match args.access {
        NamespaceAccessArg::Unrestricted => Ok(NamespaceAccess::unrestricted()),
        NamespaceAccessArg::Acl => {
            let principal_scope =
                PrincipalScope::parse(args.principal_scope.as_deref().ok_or_else(|| {
                    CliError::invalid_request("`--principal-scope` is required with `--access acl`")
                        .with_param("--principal-scope")
                })?)
                .map_err(|error| {
                    CliError::invalid_request(error.to_string()).with_param("--principal-scope")
                })?;
            let root_grants = args
                .administrators
                .iter()
                .map(|principal| (principal.clone(), AccessRights::ADMIN))
                .collect::<BTreeMap<_, _>>();
            let root_grants = AccessGrants::new(root_grants).map_err(|error| {
                CliError::invalid_request(error.to_string()).with_param("--administrator")
            })?;
            Ok(NamespaceAccess::Acl {
                principal_scope,
                root_grants,
            })
        }
    }
}

async fn run_namespace_delete(
    kind: CommandKind,
    config_path: &Path,
    args: NamespaceDeleteArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let context = resolve_profile_context(
        kind,
        config_path,
        explicit_profile,
        args.request.no_retry,
        Some(&args.subject),
    )
    .await?;
    let namespace_id = args.namespace_id.clone();
    let expected_head_seq = args.expected_head_seq;

    if !args.yes {
        // Without a terminal (or under --no-input / --json) there is no
        // prompt to answer; say what is required instead of surfacing the
        // prompt machinery's i/o error.
        if !runtime.interactive {
            return Err(context.fail(
                kind,
                CliError::non_interactive_input_required(
                    "deleting a namespace requires confirmation: pass `--yes`, or run \
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
        .map_err(|error| context.fail(kind, error))?;
        if typed.trim() != args.namespace_id.as_str() {
            return Err(context.fail(
                kind,
                CliError::invalid_request(format!(
                    "confirmation `{typed}` does not match namespace id `{}`",
                    args.namespace_id
                )),
            ));
        }
    }

    let response = context
        .target
        .client
        .delete_namespace(&namespace_id, expected_head_seq)
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(kind, CommandData::NamespaceDeleted(response)))
}

async fn run_namespace_fork(
    kind: CommandKind,
    config_path: &Path,
    args: NamespaceForkArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let loaded = load_cli_config(config_path)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let (context, profile) = resolve_profile_context_from_config(
        kind,
        &loaded.config,
        explicit_profile,
        args.request.no_retry,
        Some(&args.subject),
    )
    .await?;
    let actor_id = resolve_actor(profile, args.actor.actor_id.as_ref())
        .map_err(|error| context.fail(kind, error))?;
    let source_namespace_id = args.source_namespace_id.clone();
    let new_namespace_id = args.new_namespace_id.clone();
    let snapshot_id = args.snapshot_id.clone();
    let namespace = context
        .target
        .client
        .fork_namespace_with_options(
            &source_namespace_id,
            &new_namespace_id,
            &actor_id,
            &loonfs::ForkNamespaceOptions { snapshot_id },
        )
        .await
        .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(kind, CommandData::NamespaceStatus(namespace)))
}

pub(crate) async fn run_namespace_use(
    kind: CommandKind,
    config_path: &Path,
    args: NamespaceUseArgs,
) -> Result<CommandOutput, CommandFailure> {
    let explicit_profile = args.profile.profile.as_deref();
    let loaded = load_cli_config(config_path)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let (context, _) = resolve_profile_context_from_config(
        kind,
        &loaded.config,
        explicit_profile,
        args.request.no_retry,
        None,
    )
    .await?;
    let namespace_id = args.namespace_id.clone();

    context
        .target
        .client
        .get_namespace(&namespace_id)
        .await
        .map_err(|error| context.fail(kind, error))?;

    mutate_config(&loaded.path, |config| {
        set_default_namespace(config, &context.profile_name, args.namespace_id.as_str())
    })
    .map_err(|error| context.fail(kind, error))?;

    Ok(context.output(
        kind,
        CommandData::DefaultNamespace {
            profile: context.profile_name.clone(),
            namespace: args.namespace_id.to_string(),
        },
    ))
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
    // `current` is a status command, so an unset namespace is returned as
    // `null`. Environment selection still takes precedence over the profile
    // default.
    let namespace = match resolve_namespace(profile_name, profile, None) {
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
