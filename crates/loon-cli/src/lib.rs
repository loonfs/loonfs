//! LoonFS command-line entrypoint.
//!
//! The CLI supports embedded profiles that talk directly to object storage and
//! remote profiles that talk to a LoonFS server. It keeps command output stable
//! for humans and scripts.

#![allow(unreachable_pub)]
// The CLI is a binary crate with library-style internal modules; visibility cleanup is separate.

mod args;
mod backend;
mod commands;
mod config;
mod error;
mod profiles;
mod prompt;
mod render;
mod resolve;

use clap::Parser;
use std::process::ExitCode;

pub fn main() -> ExitCode {
    let cli = args::Cli::parse();
    let runtime = args::RuntimeBehavior::detect(&cli);

    match commands::run(cli, runtime) {
        Ok(output) => match render::render_success(&output, runtime.json) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                let failure = commands::CommandFailure {
                    kind: output.kind,
                    profile: output.profile.clone(),
                    mode: output.mode,
                    error: error::CliError::new("io_error", format!("i/o error: {err}")),
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
