use crate::cmd::completion::{render_completion, CompletionCommand};
use crate::cmd::config::{render_config, ConfigCommand};
use crate::cmd::doctor::{render_doctor, DoctorCommand};
use crate::cmd::file::FileArgs;
use crate::cmd::manpages::{render_manpages, ManpagesCommand};
use crate::cmd::ops::OpsArgs;
use crate::cmd::version::render_version;
use crate::error::{CliError, CliResult};
use clap::{CommandFactory, Parser, Subcommand};

const ROOT_AFTER_HELP: &str = "Examples:\n  loon help ops bootstrap-namespace\n  loon ops bootstrap-namespace --config ./loondb-demo.local.toml --namespace demo\n  loon config validate\n  loon doctor\n  loon manpages ./target/man\n\n`bootstrap-namespace` is the supported namespace-init path today.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliOutput {
    Text(String),
    Bytes(Vec<u8>),
}

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
    /// Read authoritative namespace state directly as files and paths.
    File(FileArgs),
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
    File(loon_ops::FileCommand),
    Config(ConfigCommand),
    Doctor(DoctorCommand),
    Completion(CompletionCommand),
    Manpages(ManpagesCommand),
    Version,
}

pub fn run(
    args: impl IntoIterator<Item = impl Into<std::ffi::OsString> + Clone>,
) -> CliResult<CliOutput> {
    match parse_args(args)? {
        ParsedCommand::Ops(command) => loon_ops::run_command(command)
            .map(CliOutput::Text)
            .map_err(CliError::from),
        ParsedCommand::File(command) => loon_ops::run_file_command(command)
            .map(|output| match output {
                loon_ops::FileCommandOutput::Text(text) => CliOutput::Text(text),
                loon_ops::FileCommandOutput::Bytes(bytes) => CliOutput::Bytes(bytes),
            })
            .map_err(CliError::from),
        ParsedCommand::Config(command) => render_config(command)
            .map(CliOutput::Text)
            .map_err(CliError::from),
        ParsedCommand::Doctor(command) => render_doctor(command)
            .map(CliOutput::Text)
            .map_err(CliError::from),
        ParsedCommand::Completion(command) => render_completion(build_command(), command)
            .map(CliOutput::Text)
            .map_err(CliError::from),
        ParsedCommand::Manpages(command) => render_manpages(build_command(), command)
            .map(CliOutput::Text)
            .map_err(CliError::from),
        ParsedCommand::Version => Ok(CliOutput::Text(render_version())),
    }
}

fn parse_args(
    args: impl IntoIterator<Item = impl Into<std::ffi::OsString> + Clone>,
) -> CliResult<ParsedCommand> {
    let cli = Cli::try_parse_from(args).map_err(CliError::from)?;
    match cli.command {
        RootCommand::Ops(args) => Ok(ParsedCommand::Ops(args.into_command())),
        RootCommand::File(args) => Ok(ParsedCommand::File(args.into_command()?)),
        RootCommand::Config(args) => Ok(ParsedCommand::Config(args.into_command())),
        RootCommand::Doctor(args) => Ok(ParsedCommand::Doctor(args.into_command())),
        RootCommand::Completion(args) => Ok(ParsedCommand::Completion(args.into_command())),
        RootCommand::Manpages(args) => Ok(ParsedCommand::Manpages(args.into_command())),
        RootCommand::Version => Ok(ParsedCommand::Version),
    }
}

pub(crate) fn build_command() -> clap::Command {
    Cli::command()
}

#[cfg(test)]
mod tests {
    use super::{CliOutput, ParsedCommand};
    use crate::app::parse_args;
    use crate::cmd::completion::{CompletionCommand, CompletionShell};
    use crate::cmd::config::ConfigCommand;
    use crate::cmd::doctor::DoctorCommand;
    use crate::cmd::manpages::ManpagesCommand;
    use loon_ops::{FileCommand, OpsCommand};
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
    fn parses_file_ls_with_selector() {
        let parsed = parse_args([
            "loon",
            "file",
            "ls",
            "--config",
            "demo.toml",
            "demo:/docs/report.txt",
        ])
        .expect("parse file ls");
        assert_eq!(
            parsed,
            ParsedCommand::File(FileCommand::Ls {
                config_path: std::path::PathBuf::from("demo.toml"),
                selector: "demo:/docs/report.txt".to_owned(),
            })
        );
    }

    #[test]
    fn parses_file_put_with_local_source_and_selector() {
        let parsed = parse_args([
            "loon",
            "file",
            "put",
            "--config",
            "demo.toml",
            "./hello.txt",
            "demo:/docs/hello.txt",
        ])
        .expect("parse file put");
        assert_eq!(
            parsed,
            ParsedCommand::File(FileCommand::Put {
                config_path: PathBuf::from("demo.toml"),
                local_path: PathBuf::from("./hello.txt"),
                selector: "demo:/docs/hello.txt".to_owned(),
                replace: false,
            })
        );
    }

    #[test]
    fn parses_file_put_replace_get_recursive_rm_mv_and_cp() {
        let replace = parse_args([
            "loon",
            "file",
            "put",
            "--replace",
            "--config",
            "demo.toml",
            "./hello-v2.txt",
            "demo:/docs/hello.txt",
        ])
        .expect("parse file put --replace");
        assert_eq!(
            replace,
            ParsedCommand::File(FileCommand::Put {
                config_path: PathBuf::from("demo.toml"),
                local_path: PathBuf::from("./hello-v2.txt"),
                selector: "demo:/docs/hello.txt".to_owned(),
                replace: true,
            })
        );

        let get_recursive = parse_args([
            "loon",
            "file",
            "get",
            "--recursive",
            "--config",
            "demo.toml",
            "demo:/docs",
            "./downloads",
        ])
        .expect("parse file get --recursive");
        assert_eq!(
            get_recursive,
            ParsedCommand::File(FileCommand::Get {
                config_path: PathBuf::from("demo.toml"),
                selector: "demo:/docs".to_owned(),
                local_path: PathBuf::from("./downloads"),
                recursive: true,
            })
        );

        let rm = parse_args([
            "loon",
            "file",
            "rm",
            "--config",
            "demo.toml",
            "--recursive",
            "demo:/docs",
        ])
        .expect("parse file rm");
        assert_eq!(
            rm,
            ParsedCommand::File(FileCommand::Rm {
                config_path: PathBuf::from("demo.toml"),
                selector: "demo:/docs".to_owned(),
                recursive: true,
            })
        );

        let mv = parse_args([
            "loon",
            "file",
            "mv",
            "--config",
            "demo.toml",
            "demo:/docs/a.txt",
            "demo:/docs/b.txt",
        ])
        .expect("parse file mv");
        assert_eq!(
            mv,
            ParsedCommand::File(FileCommand::Mv {
                config_path: PathBuf::from("demo.toml"),
                from_selector: "demo:/docs/a.txt".to_owned(),
                to_selector: "demo:/docs/b.txt".to_owned(),
            })
        );

        let cp = parse_args([
            "loon",
            "file",
            "cp",
            "--replace",
            "--config",
            "demo.toml",
            "demo:/docs/a.txt",
            "demo:/docs/c.txt",
        ])
        .expect("parse file cp");
        assert_eq!(
            cp,
            ParsedCommand::File(FileCommand::Cp {
                config_path: PathBuf::from("demo.toml"),
                from_selector: "demo:/docs/a.txt".to_owned(),
                to_selector: "demo:/docs/c.txt".to_owned(),
                replace: true,
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
    fn cli_output_distinguishes_text_and_bytes() {
        assert_eq!(
            CliOutput::Text("ok".to_owned()),
            CliOutput::Text("ok".to_owned())
        );
        assert_eq!(
            CliOutput::Bytes(vec![1, 2, 3]),
            CliOutput::Bytes(vec![1, 2, 3])
        );
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
