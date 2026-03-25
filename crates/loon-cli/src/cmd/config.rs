use anyhow::{anyhow, Context, Result};
use clap::{Args, Subcommand};
use loon_ops::OpsConfig;
use std::env;
use std::path::PathBuf;

pub(crate) const EXAMPLE_CONFIG_TEMPLATE: &str = "configs/loondb-demo.local-fs.example.toml";
const LOCAL_CONFIG_NAME: &str = "loondb-demo.local.toml";
const DEMO_CONFIG_NAME: &str = "loondb-demo.toml";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigCommand {
    Path { config_path: Option<PathBuf> },
    Show { config_path: Option<PathBuf> },
    Validate { config_path: Option<PathBuf> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolvedConfigSource {
    Explicit,
    Env,
    LocalDefault,
    DemoDefault,
}

impl ResolvedConfigSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Env => "env",
            Self::LocalDefault => LOCAL_CONFIG_NAME,
            Self::DemoDefault => DEMO_CONFIG_NAME,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedConfigPath {
    pub path: PathBuf,
    pub source: ResolvedConfigSource,
}

#[derive(Debug, Clone, Args)]
#[command(
    after_help = "Examples:\n  loon config path\n  loon config show --config ./loondb-demo.local.toml\n  loon config validate"
)]
pub struct ConfigArgs {
    #[command(subcommand)]
    command: ConfigSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
enum ConfigSubcommand {
    /// Print the resolved ops config path.
    Path(OptionalConfigArgs),
    /// Parse the ops config and print a normalized TOML rendering.
    Show(OptionalConfigArgs),
    /// Validate that the ops config can be read and parsed.
    Validate(OptionalConfigArgs),
}

#[derive(Debug, Clone, Args, PartialEq, Eq)]
struct OptionalConfigArgs {
    #[arg(
        long,
        value_name = "PATH",
        help = "Optional path to the ops TOML config file. If omitted, LOON_CONFIG, ./loondb-demo.local.toml, and ./loondb-demo.toml are checked in that order."
    )]
    config: Option<PathBuf>,
}

impl ConfigArgs {
    pub(crate) fn into_command(self) -> ConfigCommand {
        match self.command {
            ConfigSubcommand::Path(args) => ConfigCommand::Path {
                config_path: args.config,
            },
            ConfigSubcommand::Show(args) => ConfigCommand::Show {
                config_path: args.config,
            },
            ConfigSubcommand::Validate(args) => ConfigCommand::Validate {
                config_path: args.config,
            },
        }
    }
}

pub(crate) fn resolve_config_path(config_path: Option<PathBuf>) -> Result<ResolvedConfigPath> {
    if let Some(path) = config_path {
        return Ok(ResolvedConfigPath {
            path,
            source: ResolvedConfigSource::Explicit,
        });
    }

    if let Some(path) = env::var_os("LOON_CONFIG").filter(|value| !value.is_empty()) {
        return Ok(ResolvedConfigPath {
            path: PathBuf::from(path),
            source: ResolvedConfigSource::Env,
        });
    }

    let local_default = PathBuf::from(LOCAL_CONFIG_NAME);
    if local_default.is_file() {
        return Ok(ResolvedConfigPath {
            path: local_default,
            source: ResolvedConfigSource::LocalDefault,
        });
    }

    let demo_default = PathBuf::from(DEMO_CONFIG_NAME);
    if demo_default.is_file() {
        return Ok(ResolvedConfigPath {
            path: demo_default,
            source: ResolvedConfigSource::DemoDefault,
        });
    }

    Err(anyhow!(
        "no config path resolved; pass --config, set LOON_CONFIG, or copy {}",
        EXAMPLE_CONFIG_TEMPLATE
    ))
}

pub(crate) fn render_config(command: ConfigCommand) -> Result<String> {
    match command {
        ConfigCommand::Path { config_path } => {
            let resolved = resolve_config_path(config_path)?;
            Ok(resolved.path.display().to_string())
        }
        ConfigCommand::Show { config_path } => {
            let resolved = resolve_config_path(config_path)?;
            let config = OpsConfig::load(&resolved.path)?;
            toml::to_string_pretty(&config).context("render normalized ops config")
        }
        ConfigCommand::Validate { config_path } => {
            let resolved = resolve_config_path(config_path)?;
            OpsConfig::load(&resolved.path)?;
            Ok(format!(
                "command=config/validate\nconfig_path={}\nconfig_source={}\nstatus=valid\n",
                resolved.path.display(),
                resolved.source.as_str()
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_config_path, ResolvedConfigSource};
    use std::path::PathBuf;

    #[test]
    fn resolve_config_prefers_explicit_path() {
        let resolved = resolve_config_path(Some(PathBuf::from("custom.toml"))).expect("resolve");
        assert_eq!(resolved.path, PathBuf::from("custom.toml"));
        assert_eq!(resolved.source, ResolvedConfigSource::Explicit);
    }
}
