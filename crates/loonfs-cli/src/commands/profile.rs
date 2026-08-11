//! `loonfs profile` commands: list, show, create, update, and delete.

use super::context::fail;
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::profile_config::{
    apply_update_flags, apply_update_interactive, build_profile_from_create_spec,
    create_profile_spec_from_create, has_update_flags, AmbientCredentials,
};
use crate::args::{
    CommandKind, ProfileCommand, ProfileCreateArgs, ProfileUpdateArgs, RuntimeBehavior,
};
use crate::config::{
    load_config, load_config_for_repair, load_config_if_exists, mutate_config, save_config_table,
    ConfigLoad, ProfileConfig,
};
use crate::error::CliError;
use crate::profiles::{
    add_profile, delete_profile, delete_profile_in_table, list_profiles, make_default_profile,
    make_default_profile_in_table, show_profile, update_profile,
};
use crate::prompt;
use std::path::Path;

// --- profile ---

pub(crate) fn run_profile_command(
    kind: CommandKind,
    config_path: &Path,
    command: ProfileCommand,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    match command {
        ProfileCommand::Create(args) => run_profile_create(kind, config_path, args, runtime),
        ProfileCommand::List => {
            let config = load_config_if_exists(config_path)
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
                load_config(config_path).map_err(|error| fail(kind, name.clone(), None, error))?;
            let (profile_name, redacted) = show_profile(&config, name.as_deref())
                .map_err(|error| fail(kind, name.clone(), None, error))?;
            Ok(CommandOutput {
                kind,
                profile: Some(profile_name),
                mode: Some(redacted.mode_str().to_owned()),
                data: CommandData::Profile(redacted),
            })
        }
        ProfileCommand::Update(args) => run_profile_update(kind, config_path, args, runtime),
        ProfileCommand::Delete { name } => run_profile_delete(kind, config_path, &name, runtime),
        ProfileCommand::Use { name } => run_profile_use(kind, config_path, &name),
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
        let profile = build_profile_from_create_spec(
            create_profile_spec_from_create(args),
            &AmbientCredentials::from_env(),
            runtime,
        )?;
        mutate_config(config_path, |config| add_profile(config, &name, profile))
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
        // The update is gathered before the lock: the interactive editor can
        // sit at a prompt for minutes, and holding the config lock across it
        // would block every other writer. `update_profile` re-checks the
        // profile against the fresh config under the lock, so a profile
        // deleted meanwhile still fails cleanly.
        let existing = load_config(config_path)?
            .profiles
            .get(&name)
            .ok_or_else(|| CliError::profile_not_found(&name))?
            .clone();

        let updated = if has_update_flags(&args) {
            apply_update_flags(existing, &args)?
        } else if runtime.interactive {
            apply_update_interactive(existing)?
        } else {
            return Err(CliError::non_interactive_input_required(
                "update flags (e.g. --root, --bucket)",
            ));
        };

        mutate_config(config_path, |config| update_profile(config, &name, updated))
    })()
    .map_err(|error| fail(kind, Some(name.clone()), None, error))?;

    Ok(CommandOutput {
        kind,
        profile: Some(result.0),
        mode: Some(result.1.mode_str().to_owned()),
        data: CommandData::Profile(result.1),
    })
}

fn run_profile_delete(
    kind: CommandKind,
    config_path: &Path,
    name: &str,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    if runtime.interactive {
        let confirmed = prompt::prompt_confirm(&format!("delete profile `{name}`?"))
            .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
        if !confirmed {
            return Err(fail(
                kind,
                Some(name.to_owned()),
                None,
                CliError::cancelled(),
            ));
        }
    }

    // Delete is a repair command: it operates on the loose table when the
    // strict decode fails, so the profile that broke the config can still be
    // removed with the tool that manages it.
    let loaded = load_config_for_repair(config_path)
        .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
    let removed = match loaded {
        ConfigLoad::Valid(_) => {
            let mut removed_mode = None;
            mutate_config(config_path, |config| {
                let removed = delete_profile(config, name)?;
                removed_mode = Some(removed.mode.clone());
                Ok(removed)
            })
            .map_err(|error| fail(kind, Some(name.to_owned()), removed_mode, error))?
        }
        ConfigLoad::Degraded { mut table, .. } => {
            let removed = delete_profile_in_table(&mut table, name)
                .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
            save_config_table(config_path, &table).map_err(|error| {
                fail(
                    kind,
                    Some(name.to_owned()),
                    Some(removed.mode.clone()),
                    error,
                )
            })?;
            removed
        }
    };
    Ok(CommandOutput {
        kind,
        profile: Some(name.to_owned()),
        mode: Some(removed.mode.clone()),
        data: CommandData::ProfileSummary(removed),
    })
}

fn run_profile_use(
    kind: CommandKind,
    config_path: &Path,
    name: &str,
) -> Result<CommandOutput, CommandFailure> {
    // Use is a repair command like delete: switching the default away from a
    // profile that broke the config must not require hand-editing the file.
    let loaded = load_config_for_repair(config_path)
        .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
    match loaded {
        ConfigLoad::Valid(_) => {
            mutate_config(config_path, |config| make_default_profile(config, name))
                .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?
        }
        ConfigLoad::Degraded { mut table, .. } => {
            make_default_profile_in_table(&mut table, name)
                .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
            save_config_table(config_path, &table)
                .map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
        }
    }
    Ok(CommandOutput {
        kind,
        profile: Some(name.to_owned()),
        mode: None,
        data: CommandData::DefaultProfile {
            name: name.to_owned(),
        },
    })
}
