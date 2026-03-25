use crate::cmd::completion::{render_completion, CompletionCommand};
use crate::cmd::config::{render_config, ConfigCommand};
use crate::cmd::doctor::{render_doctor, DoctorCommand};
use crate::cmd::manpages::{render_manpages, ManpagesCommand};
use crate::cmd::ops::OpsArgs;
use crate::cmd::version::render_version;
use crate::error::{CliError, CliResult};
use clap::{CommandFactory, Parser, Subcommand};

const ROOT_AFTER_HELP: &str = "Examples:\n  loon help ops bootstrap-namespace\n  loon ops bootstrap-namespace --config ./loondb-demo.local.toml --namespace demo\n  loon config validate\n  loon doctor\n  loon manpages ./target/man\n\n`bootstrap-namespace` is the supported namespace-init path today.";

#[derive(Debug, Parser)]
#[command(
    name = "loon",
    bin_name = "loon",
    version,
    about = "LoonDB command-line interface",
    long_about = "Thin CLI frontend over the shared loon-ops contract.",
    after_help = ROOT_AFTER_HELP
)]
struct Cli {
    #[command(subcommand)]
    command: RootCommand,
}

#[derive(Debug, Subcommand)]
enum RootCommand {
    /// Operability commands backed by the shared loon-ops contract.
    Ops(OpsArgs),
    /// Resolve, show, or validate the existing ops TOML config.
    Config(crate::cmd::config::ConfigArgs),
    /// Run local CLI diagnostics without mutating namespace state.
    Doctor(crate::cmd::doctor::DoctorArgs),
    /// Generate shell completion scripts for the active CLI grammar.
    Completion(crate::cmd::completion::CompletionArgs),
    /// Generate man pages for the active CLI grammar.
    Manpages(crate::cmd::manpages::ManpagesArgs),
    /// Print the active CLI version.
    Version,
}

#[derive(Debug, PartialEq, Eq)]
enum ParsedCommand {
    Ops(loon_ops::OpsCommand),
    Config(ConfigCommand),
    Doctor(DoctorCommand),
    Completion(CompletionCommand),
    Manpages(ManpagesCommand),
    Version,
}

pub fn run(
    args: impl IntoIterator<Item = impl Into<std::ffi::OsString> + Clone>,
) -> CliResult<String> {
    match parse_args(args)? {
        ParsedCommand::Ops(command) => loon_ops::run_command(command).map_err(CliError::from),
        ParsedCommand::Config(command) => render_config(command).map_err(CliError::from),
        ParsedCommand::Doctor(command) => render_doctor(command).map_err(CliError::from),
        ParsedCommand::Completion(command) => {
            render_completion(build_command(), command).map_err(CliError::from)
        }
        ParsedCommand::Manpages(command) => {
            render_manpages(build_command(), command).map_err(CliError::from)
        }
        ParsedCommand::Version => Ok(render_version()),
    }
}

pub(crate) fn build_command() -> clap::Command {
    Cli::command()
}

fn parse_args(
    args: impl IntoIterator<Item = impl Into<std::ffi::OsString> + Clone>,
) -> CliResult<ParsedCommand> {
    let cli = Cli::try_parse_from(args).map_err(CliError::from)?;
    match cli.command {
        RootCommand::Ops(args) => Ok(ParsedCommand::Ops(args.into_command())),
        RootCommand::Config(args) => Ok(ParsedCommand::Config(args.into_command())),
        RootCommand::Doctor(args) => Ok(ParsedCommand::Doctor(args.into_command())),
        RootCommand::Completion(args) => Ok(ParsedCommand::Completion(args.into_command())),
        RootCommand::Manpages(args) => Ok(ParsedCommand::Manpages(args.into_command())),
        RootCommand::Version => Ok(ParsedCommand::Version),
    }
}

#[cfg(test)]
mod tests {
    use super::ParsedCommand;
    use crate::app::parse_args;
    use crate::cmd::completion::{CompletionCommand, CompletionShell};
    use crate::cmd::config::ConfigCommand;
    use crate::cmd::doctor::DoctorCommand;
    use crate::cmd::manpages::ManpagesCommand;
    use loon_ops::OpsCommand;
    use loon_types::NamespaceId;
    use std::path::PathBuf;

    #[test]
    fn parses_ops_bootstrap_with_allow_existing() {
        let parsed = parse_args([
            "loon",
            "ops",
            "bootstrap-namespace",
            "--config",
            "demo.toml",
            "--namespace",
            "demo",
            "--allow-existing",
        ])
        .expect("parse bootstrap command");
        assert_eq!(
            parsed,
            ParsedCommand::Ops(OpsCommand::BootstrapNamespace {
                config_path: PathBuf::from("demo.toml"),
                namespace_id: NamespaceId::from("demo"),
                allow_existing: true,
            })
        );
    }

    #[test]
    fn parses_ops_show_client_state_common_args() {
        let parsed = parse_args([
            "loon",
            "ops",
            "show-client-state",
            "--config",
            "demo.toml",
            "--namespace",
            "demo",
        ])
        .expect("parse show-client-state command");
        assert_eq!(
            parsed,
            ParsedCommand::Ops(OpsCommand::ShowClientState {
                config_path: PathBuf::from("demo.toml"),
                namespace_id: NamespaceId::from("demo"),
            })
        );
    }

    #[test]
    fn parses_ops_observe_local_path_args() {
        let parsed = parse_args([
            "loon",
            "ops",
            "observe-local",
            "--config",
            "demo.toml",
            "--namespace",
            "demo",
            "--path",
            "mirror/hello.txt",
        ])
        .expect("parse observe-local command");
        assert_eq!(
            parsed,
            ParsedCommand::Ops(OpsCommand::ObserveLocal {
                config_path: PathBuf::from("demo.toml"),
                namespace_id: NamespaceId::from("demo"),
                path: PathBuf::from("mirror/hello.txt"),
            })
        );
    }

    #[test]
    fn parses_ops_observe_move_from_to_args() {
        let parsed = parse_args([
            "loon",
            "ops",
            "observe-move",
            "--config",
            "demo.toml",
            "--namespace",
            "demo",
            "--from",
            "mirror/hello.txt",
            "--to",
            "mirror/archive/hello.txt",
        ])
        .expect("parse observe-move command");
        assert_eq!(
            parsed,
            ParsedCommand::Ops(OpsCommand::ObserveMove {
                config_path: PathBuf::from("demo.toml"),
                namespace_id: NamespaceId::from("demo"),
                from: PathBuf::from("mirror/hello.txt"),
                to: PathBuf::from("mirror/archive/hello.txt"),
            })
        );
    }

    #[test]
    fn parses_ops_sync_until_idle_with_max_steps() {
        let parsed = parse_args([
            "loon",
            "ops",
            "sync-until-idle",
            "--config",
            "demo.toml",
            "--namespace",
            "demo",
            "--max-steps",
            "7",
        ])
        .expect("parse sync-until-idle command");
        assert_eq!(
            parsed,
            ParsedCommand::Ops(OpsCommand::SyncUntilIdle {
                config_path: PathBuf::from("demo.toml"),
                namespace_id: NamespaceId::from("demo"),
                max_steps: Some(7),
            })
        );
    }

    #[test]
    fn parses_completion_command() {
        let parsed = parse_args(["loon", "completion", "bash"]).expect("parse completion");
        assert_eq!(
            parsed,
            ParsedCommand::Completion(CompletionCommand {
                shell: CompletionShell::Bash,
            })
        );
    }

    #[test]
    fn parses_manpages_command() {
        let parsed =
            parse_args(["loon", "manpages", "target/man"]).expect("parse manpages command");
        assert_eq!(
            parsed,
            ParsedCommand::Manpages(ManpagesCommand {
                output_dir: PathBuf::from("target/man"),
            })
        );
    }

    #[test]
    fn parses_version_command() {
        let parsed = parse_args(["loon", "version"]).expect("parse version command");
        assert_eq!(parsed, ParsedCommand::Version);
    }

    #[test]
    fn parses_config_validate_command() {
        let parsed = parse_args(["loon", "config", "validate"]).expect("parse config validate");
        assert_eq!(
            parsed,
            ParsedCommand::Config(ConfigCommand::Validate { config_path: None })
        );
    }

    #[test]
    fn parses_doctor_command_with_optional_config() {
        let parsed =
            parse_args(["loon", "doctor", "--config", "demo.toml"]).expect("parse doctor command");
        assert_eq!(
            parsed,
            ParsedCommand::Doctor(DoctorCommand {
                config_path: Some(PathBuf::from("demo.toml")),
            })
        );
    }
}
