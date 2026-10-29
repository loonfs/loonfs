//! Writes rendered command outcomes, errors, and warnings to stdout and stderr.

use super::{human, json, json_error};
use crate::commands::{CommandData, CommandFailure, CommandOutput, ListingHeadDrift};
use crate::config::ProfileMode;
use crate::error::CliError;
use loonfs_types::PathEntry;
use serde::Serialize;
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

/// Writes one JSON item per line and flushes the page before the next request.
pub(crate) fn write_jsonl_page<T: Serialize>(items: &[T]) -> io::Result<()> {
    let mut stdout = page_stdout();
    for item in items {
        serde_json::to_writer(&mut stdout, item).map_err(io::Error::other)?;
        stdout.write_all(b"\n")?;
    }
    stdout.flush()
}

/// Writes one listing page and makes it visible before the next fetch.
pub(crate) fn write_path_entries_page(entries: &[PathEntry], jsonl: bool) -> io::Result<()> {
    if jsonl {
        return write_jsonl_page(entries);
    }
    let mut stdout = page_stdout();
    for entry in entries {
        writeln!(stdout, "{}", human::human_path_entry(entry))?;
    }
    stdout.flush()
}

fn page_stdout() -> io::BufWriter<io::Stdout> {
    io::BufWriter::with_capacity(64 * 1024, io::stdout())
}

pub(crate) fn render_error(failure: &CommandFailure, format: OutputFormat) -> io::Result<()> {
    let mut stderr = io::stderr().lock();
    if format == OutputFormat::Json {
        let body = json_error(failure)?;
        stderr.write_all(body.as_bytes())?;
        stderr.write_all(b"\n")?;
    } else {
        stderr.write_all(human_error(&failure.error, failure.mode).as_bytes())?;
        stderr.write_all(b"\n")?;
    }
    Ok(())
}

pub(super) fn human_error(error: &CliError, mode: Option<ProfileMode>) -> String {
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

pub(super) fn request_id_suffix(request_id: Option<&str>, mode: Option<ProfileMode>) -> String {
    if mode == Some(ProfileMode::Embedded) {
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
