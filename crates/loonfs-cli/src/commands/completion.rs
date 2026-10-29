//! The `completion` command: the shell completion script for the shell that
//! `--shell` or `$SHELL` selects.

use super::context::fail;
use super::{CommandData, CommandFailure, CommandOutput};
use crate::args::{Cli, CompletionArgs};
use crate::error::CliError;
use clap::CommandFactory;
use clap_complete::Shell;
use std::path::Path;

const COMPLETION_SHELL_REQUIRED: &str =
    "pass `--shell` with one of: bash, zsh, fish, powershell, elvish (`$SHELL` is unset or unsupported)";

pub(super) fn run_completion(
    kind: crate::args::CommandKind,
    args: &CompletionArgs,
) -> Result<CommandOutput, CommandFailure> {
    let shell = args
        .shell
        .or_else(completion_shell_from_environment)
        .ok_or_else(|| {
            fail(
                kind,
                None,
                None,
                CliError::invalid_request(COMPLETION_SHELL_REQUIRED).with_param("--shell"),
            )
        })?;
    let mut script = Vec::new();
    clap_complete::generate(shell, &mut Cli::command(), "loonfs", &mut script);
    Ok(CommandOutput {
        kind,
        profile: None,
        mode: None,
        data: CommandData::CompletionScript(script),
    })
}

fn completion_shell_from_environment() -> Option<Shell> {
    let shell = std::env::var_os("SHELL")?;
    match Path::new(&shell).file_name()?.to_str()? {
        "bash" => Some(Shell::Bash),
        "zsh" => Some(Shell::Zsh),
        "fish" => Some(Shell::Fish),
        "pwsh" | "powershell" => Some(Shell::PowerShell),
        "elvish" => Some(Shell::Elvish),
        _ => None,
    }
}
