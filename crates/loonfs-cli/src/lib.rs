//! LoonFS command-line entrypoint.
//!
//! The CLI supports embedded profiles that talk directly to object storage and
//! remote profiles that talk to a LoonFS server. It keeps command output stable
//! for humans and scripts.

mod args;
mod backend;
mod backend_error;
mod commands;
mod config;
mod error;
mod profiles;
mod prompt;
mod render;
mod resolve;

use clap::Parser;
use std::process::ExitCode;

pub async fn main() -> ExitCode {
    let cli = args::Cli::parse();
    let runtime = args::RuntimeBehavior::detect(&cli);

    match commands::run(cli, runtime).await {
        Ok(output) => match render::render_success(&output, runtime.json) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                let failure = commands::CommandFailure {
                    kind: output.kind,
                    profile: output.profile.clone(),
                    mode: output.mode,
                    error: Box::new(error::CliError::io(err)),
                };
                let _ = render::render_error(&failure, runtime.json);
                ExitCode::FAILURE
            }
        },
        Err(failure) => {
            let _ = render::render_error(&failure, runtime.json);
            ExitCode::FAILURE
        }
    }
}
