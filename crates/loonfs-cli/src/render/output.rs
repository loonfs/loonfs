//! Writes rendered command outcomes, errors, and warnings to stdout and stderr.

use super::{human, json, json_error};
use crate::commands::{CommandData, CommandFailure, CommandOutput, ListingHeadDrift};
use crate::error::CliError;
use std::io::{self, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputFormat {
    Human,
    Json,
}

pub(crate) fn render_success(output: &CommandOutput, format: OutputFormat) -> io::Result<()> {
    write_success(output, format, io::stdout().lock())
}

pub(crate) fn write_success(
    output: &CommandOutput,
    format: OutputFormat,
    mut writer: impl Write,
) -> io::Result<()> {
    if format == OutputFormat::Json {
        json::write_json_success(output, &mut writer)?;
        writer.write_all(b"\n")?;
        return Ok(());
    }
    match &output.data {
        CommandData::CompletionScript(bytes) => writer.write_all(bytes)?,
        CommandData::StreamedToStdout => {}
        _ => {
            if !human::write_human_success(output, &mut writer)? {
                writer.write_all(b"\n")?;
            }
        }
    }
    Ok(())
}

pub(crate) fn render_error(failure: &CommandFailure, format: OutputFormat) -> io::Result<()> {
    let mut stderr = io::stderr().lock();
    if format == OutputFormat::Json {
        let body = json_error(failure)?;
        stderr.write_all(body.as_bytes())?;
        stderr.write_all(b"\n")?;
    } else {
        stderr.write_all(human_error(&failure.error, failure.mode.as_deref()).as_bytes())?;
        stderr.write_all(b"\n")?;
    }
    Ok(())
}

pub(super) fn human_error(error: &CliError, mode: Option<&str>) -> String {
    let mut rendered = error.message.clone();
    rendered.push_str(&request_id_suffix(error.request_id.as_deref(), mode));
    if let Some(feature) = &error.feature {
        rendered.push_str(&format!("\nfeature: {feature}"));
    }
    if let Some(param) = &error.param {
        rendered.push_str(&format!("\nparam: {param}"));
    }
    rendered
}

pub(super) fn request_id_suffix(request_id: Option<&str>, mode: Option<&str>) -> String {
    if mode == Some("embedded") {
        return String::new();
    }
    request_id.map_or_else(String::new, |request_id| {
        format!(" (request id: {request_id})")
    })
}

pub(crate) fn write_stderr_warning(message: impl std::fmt::Display) {
    let _ = writeln!(io::stderr().lock(), "warning: {message}");
}

/// Per-item progress for long-running recursive transfers, on stderr so
/// stdout stays the machine-readable summary.
pub(crate) fn write_stderr_progress(message: impl std::fmt::Display) {
    let _ = writeln!(io::stderr().lock(), "{message}");
}

pub(crate) fn listing_drift_warning(drift: &ListingHeadDrift) -> String {
    format!(
        "namespace advanced during the listing (head seq {} to {}); entries may mix states; re-run for a settled view",
        drift.first_head_seq.0, drift.last_head_seq.0
    )
}

pub(crate) fn write_listing_drift_warning(drift: &ListingHeadDrift) {
    write_stderr_warning(listing_drift_warning(drift));
}

pub(crate) fn more_entries_hint(cursor: &str) -> String {
    format!("more entries exist; continue with --cursor {cursor} or stream everything with --all")
}
