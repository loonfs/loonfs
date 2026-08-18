//! LoonFS command-line entrypoint.
//!
//! The CLI supports embedded profiles that talk directly to object storage and
//! remote profiles that talk to a LoonFS server. It keeps command output stable
//! for humans and scripts.

// The embedded runtime's composed async graph keeps growing; even with
// the boxed entrypoint, rustc's future-layout query depth needs headroom
// here. This is the raise rustc itself prescribes.
#![recursion_limit = "256"]
#![allow(
    clippy::result_large_err,
    reason = "CLI errors preserve backend and request diagnostics as structured fields"
)]
mod args;
mod backend;
mod backend_error;
mod commands;
mod config;
mod error;
mod payload;
mod profiles;
mod progress;
mod prompt;
mod render;
mod resolve;
mod uploads;

use clap::Parser;
use std::process::ExitCode;

/// Exit status for a command line the parser rejected, which is clap's own.
///
/// It stays distinct from the failure status a command that actually ran
/// reports, so a script can tell "this command never started" from "this
/// command started and failed" without reading the message.
const USAGE_EXIT_CODE: u8 = 2;

pub async fn main() -> ExitCode {
    let cli = match args::Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => return render_parse_failure(&error),
    };
    if let Err(error) = args::validate_cli(&cli) {
        return render_parse_failure(&error);
    }
    let runtime = args::RuntimeBehavior::detect(&cli);

    // Boxing keeps the public entrypoint's future shallow for downstream binaries.
    let result = Box::pin(commands::run(cli, runtime)).await;
    match result {
        Ok(output) => match render::render_success(&output, runtime.json) {
            // A recursive transfer renders its per-item outcomes as success
            // data but still exits nonzero when any item failed.
            Ok(()) if output.data.reports_failures() => ExitCode::FAILURE,
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

/// Renders a command line clap rejected, in whichever form the caller asked
/// for.
///
/// `--json` is one of the things clap failed to parse, so whether it was
/// asked for has to be read off the raw arguments. A caller who asked for
/// JSON gets the same envelope a runtime failure produces, and every caller
/// keeps clap's exit status: a parse failure is not a command that ran.
///
/// `--help` and `--version` arrive here as errors too, and are not
/// failures: clap renders them on stdout and exits zero.
fn render_parse_failure(error: &clap::Error) -> ExitCode {
    if !error.use_stderr() {
        error.print().ok();
        return ExitCode::SUCCESS;
    }
    if !json_requested(std::env::args_os()) {
        error.print().ok();
        return ExitCode::from(USAGE_EXIT_CODE);
    }
    let failure = parse_failure_error(error);
    let _ = render::render_parse_error(&failure);
    ExitCode::from(USAGE_EXIT_CODE)
}

fn parse_failure_error(error: &clap::Error) -> error::CliError {
    let failure = error::CliError::invalid_usage(error.render().to_string());
    match parse_error_param(error) {
        Some(param) => failure.with_param(param),
        None => failure,
    }
}

fn parse_error_param(error: &clap::Error) -> Option<String> {
    use clap::error::{ContextKind, ContextValue};

    let value = error.get(ContextKind::InvalidArg)?;
    let rendered = match value {
        ContextValue::String(value) => value.clone(),
        ContextValue::Strings(values) => values.first()?.clone(),
        ContextValue::StyledStr(value) => value.to_string(),
        ContextValue::StyledStrs(values) => values.first()?.to_string(),
        _ => return None,
    };
    let token = rendered.split_whitespace().next()?;
    Some(
        token
            .trim_matches(|character| matches!(character, '`' | '\'' | '[' | ']'))
            .to_owned(),
    )
}

/// Whether the raw arguments asked for `--json`, scanned the way clap would
/// have: a bare `--` ends option parsing, so a `--json` after it is a value,
/// not this flag.
fn json_requested(arguments: impl IntoIterator<Item = std::ffi::OsString>) -> bool {
    for argument in arguments {
        if argument == "--" {
            return false;
        }
        if argument == "--json" {
            return true;
        }
    }
    false
}
