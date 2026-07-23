//! `loon profile` commands: list, show, create, update, and delete.

use super::context::fail;
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::profile_config::{
    apply_update_flags, apply_update_interactive, build_profile_from_create_spec,
    create_profile_spec_from_create, has_update_flags,
};
use crate::args::{
    CommandKind, ProfileCommand, ProfileCreateArgs, ProfileUpdateArgs, RuntimeBehavior,
};
use crate::config::{
    default_config_path, load_config, load_config_if_exists, load_or_default_config, save_config,
    ProfileConfig,
};
use crate::error::CliError;
use crate::profiles::{
    add_profile, delete_profile, list_profiles, make_default_profile, show_profile, update_profile,
};
use crate::prompt;
use std::path::Path;

// --- profile ---

pub(crate) fn run_profile_command(
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
        ProfileCommand::Delete { name } => run_profile_delete(kind, &config_path, &name, runtime),
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

        let updated = if has_update_flags(&args) {
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

    let mut config =
        load_config(config_path).map_err(|error| fail(kind, Some(name.to_owned()), None, error))?;
    let removed = delete_profile(&mut config, name)
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
