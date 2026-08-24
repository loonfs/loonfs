//! Tracing subscriber setup from environment variables.
//!
//! The server logs by default. `LOONFS_TRACE` selects the mode: unset or
//! blank means JSON on stdout, `json` means the same thing spelled out, and
//! `off` means no output at all. Any other value fails the process instead
//! of guessing. `RUST_LOG` replaces the default filter when it is set.

use std::env;

use thiserror::Error;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

const TRACE_ENV: &str = "LOONFS_TRACE";
const RUST_LOG_ENV: &str = "RUST_LOG";
const DEFAULT_TRACE_FILTER: &str =
    "loonfs_server=info,loonfs_grep=info,loonfs=info,loonfs_core=info";
/// The `LOONFS_TRACE` value that turns logging off.
const TRACE_MODE_OFF: &str = "off";
/// The `LOONFS_TRACE` value that names the default mode.
const TRACE_MODE_JSON: &str = "json";

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceConfig {
    filter: String,
}

#[derive(Debug, Error)]
pub enum TraceInitError {
    #[error("unsupported LOONFS_TRACE value `{0}`; expected `json` or `off`")]
    UnsupportedMode(String),
    #[error("invalid RUST_LOG filter: {0}")]
    Filter(String),
    #[error("failed to initialize tracing subscriber: {0}")]
    Subscriber(String),
}

/// Installs the JSON subscriber unless `LOONFS_TRACE=off` asks for silence.
pub fn init_tracing_from_env() -> Result<(), TraceInitError> {
    let Some(config) =
        trace_config_from_env(env::var(TRACE_ENV).ok(), env::var(RUST_LOG_ENV).ok())?
    else {
        return Ok(());
    };
    let filter = config
        .filter
        .parse::<EnvFilter>()
        .map_err(|err| TraceInitError::Filter(err.to_string()))?;
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::CLOSE)
        .try_init()
        .map_err(|err| TraceInitError::Subscriber(err.to_string()))
}

/// Resolves the two environment variables into the subscriber to install, or
/// into `None` when the operator asked for no logging.
fn trace_config_from_env(
    trace_env: Option<String>,
    rust_log_env: Option<String>,
) -> Result<Option<TraceConfig>, TraceInitError> {
    // An operator who sets nothing gets logs. Only `off` takes them away.
    let trace_mode = trace_env.unwrap_or_default();
    let trace_mode = trace_mode.trim();
    if trace_mode == TRACE_MODE_OFF {
        return Ok(None);
    }
    if !trace_mode.is_empty() && trace_mode != TRACE_MODE_JSON {
        return Err(TraceInitError::UnsupportedMode(trace_mode.to_owned()));
    }
    Ok(Some(TraceConfig {
        filter: rust_log_env
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_TRACE_FILTER.to_owned()),
    }))
}

#[cfg(test)]
mod tests {
    use super::{trace_config_from_env, DEFAULT_TRACE_FILTER};

    #[test]
    fn tracing_is_enabled_without_env() {
        let unset = trace_config_from_env(None, None)
            .expect("trace config parses")
            .expect("enabled tracing config");
        assert_eq!(unset.filter, DEFAULT_TRACE_FILTER);

        let blank = trace_config_from_env(Some("  ".to_owned()), None)
            .expect("blank trace config parses")
            .expect("enabled tracing config");
        assert_eq!(blank, unset);

        let explicit = trace_config_from_env(Some("json".to_owned()), None)
            .expect("trace config parses")
            .expect("enabled tracing config");
        assert_eq!(explicit, unset);
    }

    #[test]
    fn off_disables_tracing() {
        assert!(trace_config_from_env(Some("off".to_owned()), None)
            .expect("trace config parses")
            .is_none());
        assert!(trace_config_from_env(Some(" off ".to_owned()), None)
            .expect("padded trace config parses")
            .is_none());
    }

    #[test]
    fn off_wins_over_rust_log() {
        assert!(trace_config_from_env(
            Some("off".to_owned()),
            Some("loonfs_core=debug".to_owned())
        )
        .expect("trace config parses")
        .is_none());
    }

    #[test]
    fn default_tracing_uses_rust_log_when_present() {
        let config = trace_config_from_env(None, Some("loonfs_core=debug".to_owned()))
            .expect("trace config parses")
            .expect("enabled tracing config");
        assert_eq!(config.filter, "loonfs_core=debug");
    }

    #[test]
    fn unsupported_trace_mode_is_rejected() {
        assert!(trace_config_from_env(Some("text".to_owned()), None).is_err());
    }
}
