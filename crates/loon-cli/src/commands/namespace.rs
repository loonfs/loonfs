use super::context::{fail, validate_namespace_id};
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{
    CommandKind, CurrentArgs, NamespaceCommand, NamespaceCreateArgs, NamespaceForkArgs,
    NamespaceListArgs, NamespaceUseArgs,
};
use crate::config::save_config;
use crate::error::CliError;
use crate::profiles::{default_namespace, set_default_namespace};
use crate::resolve::{load_cli_config, resolve_target_profile, resolve_target_profile_from_config};

// --- namespace ---

pub(crate) fn run_namespace_command(
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

pub(crate) fn run_namespace_use(
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

pub(crate) fn run_current(
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
