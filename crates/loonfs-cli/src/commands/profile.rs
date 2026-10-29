//! `loonfs profile` commands: list, show, create, update, and delete.

use super::context::Invocation;
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::profile_config::{
    apply_update_flags, apply_update_interactive, build_profile_from_command,
    validate_update_provider,
};
use crate::args::{ProfileCommand, ProfileProviderCommand};
use crate::config::{
    load_config, load_config_for_repair, load_config_if_exists, mutate_config, save_config_table,
    ConfigLoad, ProfileConfig, ProfileMode,
};
use crate::error::{CliError, Result};
use crate::profiles::{
    add_profile, delete_profile, delete_profile_in_table, list_profiles, set_default_profile,
    set_default_profile_in_table, show_profile, update_profile, NamedProfile,
};
use crate::prompt;

pub(crate) fn run_profile_command(
    invocation: &Invocation,
    command: ProfileCommand,
) -> Result<CommandOutput, CommandFailure> {
    let config_path = invocation.config.path.as_path();
    match command {
        ProfileCommand::Create { provider } => run_profile_create(invocation, *provider),
        ProfileCommand::List => {
            let config =
                load_config_if_exists(config_path).map_err(|error| invocation.fail(error))?;
            Ok(invocation.output(CommandData::ProfileList {
                default_profile: config.as_ref().and_then(|c| c.default_profile.clone()),
                profiles: list_profiles(config.as_ref()),
            }))
        }
        ProfileCommand::Show { name } => {
            let shown = load_config(config_path)
                .and_then(|config| show_profile(&config, name.as_deref()))
                .map_err(|error| invocation.fail_for_profile(name.as_deref(), error))?;
            Ok(profile_output(invocation, shown, None))
        }
        ProfileCommand::Update { provider } => run_profile_update(invocation, *provider),
        ProfileCommand::Delete { name } => run_profile_delete(invocation, &name),
        ProfileCommand::Use { name } => run_profile_use(invocation, &name),
    }
}

fn profile_output(
    invocation: &Invocation,
    named: NamedProfile<ProfileConfig>,
    default_profile: Option<String>,
) -> CommandOutput {
    CommandOutput {
        kind: invocation.kind,
        profile: Some(named.name),
        mode: Some(named.profile.mode()),
        data: CommandData::Profile {
            profile: named.profile,
            default_profile,
        },
    }
}

fn run_profile_create(
    invocation: &Invocation,
    command: ProfileProviderCommand,
) -> Result<CommandOutput, CommandFailure> {
    let name = command.name().to_owned();
    let (result, default_profile) =
        build_profile_from_command(&command, &invocation.selectors.subject, invocation.behavior)
            .and_then(|profile| {
                mutate_config(&invocation.config.path, |config| {
                    let result = add_profile(config, &name, profile)?;
                    Ok((result, config.default_profile.clone()))
                })
            })
            .map_err(|error| invocation.fail_for_profile(Some(&name), error))?;

    Ok(profile_output(invocation, result, default_profile))
}

fn run_profile_update(
    invocation: &Invocation,
    command: ProfileProviderCommand,
) -> Result<CommandOutput, CommandFailure> {
    let name = command.name().to_owned();
    let config_path = invocation.config.path.as_path();
    let result = (|| -> Result<NamedProfile<ProfileConfig>> {
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

        let subject = &invocation.selectors.subject;
        let updated = if command.has_update_flags(subject) {
            apply_update_flags(existing, &command, subject)?
        } else if invocation.behavior.interactive {
            validate_update_provider(&existing, &command)?;
            apply_update_interactive(&name, existing)?
        } else {
            return Err(CliError::non_interactive_input_required(
                "update flags (e.g. `--root`, `--bucket`)",
            ));
        };

        mutate_config(config_path, |config| update_profile(config, &name, updated))
    })()
    .map_err(|error| invocation.fail_for_profile(Some(&name), error))?;

    Ok(profile_output(invocation, result, None))
}

fn run_profile_delete(
    invocation: &Invocation,
    name: &str,
) -> Result<CommandOutput, CommandFailure> {
    let fail = |mode: Option<ProfileMode>, error: CliError| {
        super::context::fail(invocation.kind, Some(name.to_owned()), mode, error)
    };
    if invocation.behavior.interactive {
        let confirmed = prompt::prompt_confirm(&format!("delete profile `{name}`?"))
            .map_err(|error| fail(None, error))?;
        if !confirmed {
            return Err(fail(None, CliError::cancelled()));
        }
    }

    // Delete is a repair command: it operates on the loose table when the
    // strict decode fails, so the profile that broke the config can still be
    // removed with the tool that manages it.
    let config_path = invocation.config.path.as_path();
    let loaded = load_config_for_repair(config_path).map_err(|error| fail(None, error))?;
    let removed = match loaded {
        ConfigLoad::Valid(_) => {
            let mut removed_mode = None;
            mutate_config(config_path, |config| {
                let removed = delete_profile(config, name)?;
                removed_mode = ProfileMode::parse(&removed.mode);
                Ok(removed)
            })
            .map_err(|error| fail(removed_mode, error))?
        }
        ConfigLoad::Degraded { mut table, .. } => {
            let removed =
                delete_profile_in_table(&mut table, name).map_err(|error| fail(None, error))?;
            save_config_table(config_path, &table)
                .map_err(|error| fail(ProfileMode::parse(&removed.mode), error))?;
            removed
        }
    };
    Ok(CommandOutput {
        kind: invocation.kind,
        profile: Some(name.to_owned()),
        mode: ProfileMode::parse(&removed.mode),
        data: CommandData::ProfileSummary(removed),
    })
}

fn run_profile_use(invocation: &Invocation, name: &str) -> Result<CommandOutput, CommandFailure> {
    // Use is a repair command like delete: switching the default away from a
    // profile that broke the config must not require hand-editing the file.
    let config_path = invocation.config.path.as_path();
    let fail = |error| invocation.fail_for_profile(Some(name), error);
    let loaded = load_config_for_repair(config_path).map_err(fail)?;
    match loaded {
        ConfigLoad::Valid(_) => {
            mutate_config(config_path, |config| set_default_profile(config, name)).map_err(fail)?
        }
        ConfigLoad::Degraded { mut table, .. } => {
            set_default_profile_in_table(&mut table, name).map_err(fail)?;
            save_config_table(config_path, &table).map_err(fail)?;
        }
    }
    Ok(CommandOutput {
        kind: invocation.kind,
        profile: Some(name.to_owned()),
        mode: None,
        data: CommandData::DefaultProfile {
            name: name.to_owned(),
        },
    })
}
