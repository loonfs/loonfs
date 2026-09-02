//! Stable JSON envelopes for successful commands and failures.

use super::*;

const CLI_JSON_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
struct JsonEnvelope<'a, T>
where
    T: Serialize,
{
    kind: &'a str,
    format_version: u32,
    profile: Option<&'a str>,
    mode: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a CliError>,
}

pub(crate) fn json_success(output: &CommandOutput) -> io::Result<String> {
    match &output.data {
        CommandData::CompletionScript(_)
        | CommandData::StreamBytes(_)
        | CommandData::StreamedToStdout => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "raw output does not support json rendering",
        )),
        data => serde_json::to_string_pretty(&JsonEnvelope {
            kind: output.kind.as_str(),
            format_version: CLI_JSON_FORMAT_VERSION,
            profile: output.profile.as_deref(),
            mode: output.mode.as_deref(),
            data: Some(data),
            error: None,
        })
        .map_err(io::Error::other),
    }
}

/// `kind` for a command line the parser rejected. Every other envelope names
/// the command it belongs to; this one has none, because clap failed before
/// a command was chosen.
const PARSE_ERROR_KIND: &str = "parse_error";

/// Writes the parse-failure envelope to stderr, in the shape a runtime
/// failure uses.
pub(crate) fn render_parse_error(error: &CliError) -> io::Result<()> {
    let body = json_parse_error(error)?;
    let mut stderr = io::stderr().lock();
    stderr.write_all(body.as_bytes())?;
    stderr.write_all(b"\n")
}

pub(crate) fn json_parse_error(error: &CliError) -> io::Result<String> {
    serde_json::to_string_pretty(&JsonEnvelope::<serde_json::Value> {
        kind: PARSE_ERROR_KIND,
        format_version: CLI_JSON_FORMAT_VERSION,
        profile: None,
        mode: None,
        data: None,
        error: Some(error),
    })
    .map_err(io::Error::other)
}

pub(crate) fn json_error(failure: &CommandFailure) -> io::Result<String> {
    serde_json::to_string_pretty(&JsonEnvelope::<serde_json::Value> {
        kind: failure.kind.as_str(),
        format_version: CLI_JSON_FORMAT_VERSION,
        profile: failure.profile.as_deref(),
        mode: failure.mode.as_deref(),
        data: None,
        error: Some(&failure.error),
    })
    .map_err(io::Error::other)
}
