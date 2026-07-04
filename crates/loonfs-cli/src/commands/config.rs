use super::context::fail;
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::profile_config::{build_profile_from_create_spec, create_profile_spec_from_init};
use crate::args::{CommandKind, ConfigCommand, InitArgs, RuntimeBehavior};
use crate::config::{
    default_config_path, load_config, load_or_default_config, save_config, ProfileConfig,
};
use crate::error::CliError;
use crate::profiles::add_profile;
use crate::prompt;

// --- init ---

pub(crate) fn run_config_init(
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

pub(crate) fn run_config_command(
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
