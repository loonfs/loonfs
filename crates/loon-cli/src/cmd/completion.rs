use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use clap_complete::{generate, Generator};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CompletionShell {
    Bash,
    Zsh,
    Fish,
    Powershell,
    Elvish,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionCommand {
    pub shell: CompletionShell,
}

#[derive(Debug, Clone, Args)]
pub struct CompletionArgs {
    #[arg(value_enum)]
    shell: CompletionShell,
}

impl CompletionArgs {
    pub(crate) fn into_command(self) -> CompletionCommand {
        CompletionCommand { shell: self.shell }
    }
}

pub(crate) fn render_completion(
    command: clap::Command,
    request: CompletionCommand,
) -> Result<String> {
    let mut command = command;
    let mut buffer = Vec::new();
    match request.shell {
        CompletionShell::Bash => {
            generate_completion(clap_complete::Shell::Bash, &mut command, &mut buffer)
        }
        CompletionShell::Zsh => {
            generate_completion(clap_complete::Shell::Zsh, &mut command, &mut buffer)
        }
        CompletionShell::Fish => {
            generate_completion(clap_complete::Shell::Fish, &mut command, &mut buffer)
        }
        CompletionShell::Powershell => {
            generate_completion(clap_complete::Shell::PowerShell, &mut command, &mut buffer)
        }
        CompletionShell::Elvish => {
            generate_completion(clap_complete::Shell::Elvish, &mut command, &mut buffer)
        }
    }
    String::from_utf8(buffer).context("completion output should be valid utf-8")
}

fn generate_completion<G: Generator>(shell: G, command: &mut clap::Command, buffer: &mut Vec<u8>) {
    generate(shell, command, "loon", buffer);
}
