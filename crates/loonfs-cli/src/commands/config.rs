//! `loonfs config` commands: init and config-file inspection.

use super::context::Invocation;
use super::output::{CommandData, CommandFailure, CommandOutput};
use super::profile_config::build_profile_interactive;
use crate::args::ConfigCommand;
use crate::config::{redacted_config_table, ProfileConfig};
use crate::config_file::{load_config_for_repair, mutate_config, ConfigLoad};
use crate::error::{CliError, Result};
use crate::profiles::{add_profile, NamedProfile};
use crate::prompt;

/// Writes the resolved config file, whichever way it was named, creating
/// the directories it needs. Naming a path that has no file yet is the
/// way back from a default config this build refuses to read: init only
/// ever touches the file resolution named.
pub(crate) fn run_config_init(invocation: &Invocation) -> Result<CommandOutput, CommandFailure> {
    let config_path = invocation.config.path.as_path();
    let runtime = invocation.behavior;
    let result = (|| -> Result<NamedProfile<ProfileConfig>> {
        if config_path.exists() {
            return Err(CliError::config_already_exists(
                &config_path.display().to_string(),
            ));
        }

        if !runtime.interactive {
            return Err(CliError::non_interactive_input_required(
                "`loonfs init` is interactive; use `loonfs profile create <provider>` for scripted setup",
            ));
        }
        let name = prompt::prompt_line_default("profile name", "default")?;
        let profile = build_profile_interactive(&name, runtime)?;

        mutate_config(config_path, |config| {
            let added = add_profile(config, &name, profile)?;
            config.default_profile = Some(added.name.clone());
            Ok(added)
        })
    })()
    .map_err(|error| invocation.fail(error))?;

    Ok(CommandOutput {
        kind: invocation.kind,
        profile: Some(result.name),
        mode: Some(result.profile.mode()),
        data: CommandData::Profile {
            profile: result.profile,
            default_profile: None,
        },
    })
}

pub(crate) fn run_config_command(
    invocation: &Invocation,
    command: ConfigCommand,
) -> Result<CommandOutput, CommandFailure> {
    let location = &invocation.config;
    let config_path = location.path.as_path();
    match command {
        // Path never reads the file: the one command that reports "which
        // config is this build even looking at" has to work while that file
        // is one the build refuses to read.
        ConfigCommand::Path => Ok(invocation.output(CommandData::ConfigPath {
            path: config_path.display().to_string(),
            source: location.source,
        })),
        ConfigCommand::Show => {
            let loaded =
                load_config_for_repair(config_path).map_err(|error| invocation.fail(error))?;
            let data = match loaded {
                ConfigLoad::Valid(config) => CommandData::ConfigShow {
                    config: config.redacted(),
                },
                // Show is a repair command: a config the strict decoder
                // rejects still renders (secrets masked) with the failure on
                // top, so the file can be inspected with the tool that reads
                // it.
                ConfigLoad::Degraded { table, error } => CommandData::ConfigShowDegraded {
                    error: error.message.clone(),
                    config_toml: toml::to_string_pretty(&redacted_config_table(&table))
                        .unwrap_or_else(|_| "failed to render config".to_owned()),
                },
            };
            Ok(invocation.output(data))
        }
    }
}
