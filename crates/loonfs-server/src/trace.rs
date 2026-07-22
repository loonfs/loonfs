//! Tracing subscriber setup from environment variables.

use std::env;

use thiserror::Error;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

const TRACE_ENV: &str = "LOONFS_TRACE";
const RUST_LOG_ENV: &str = "RUST_LOG";
const DEFAULT_TRACE_FILTER: &str =
    "loonfs_server=info,loonfs_grep=info,loonfs=info,loonfs_core=info";

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceConfig {
    filter: String,
}

#[derive(Debug, Error)]
pub enum TraceInitError {
    #[error("unsupported LOONFS_TRACE value `{0}`; expected `json`")]
    UnsupportedMode(String),
    #[error("invalid RUST_LOG filter: {0}")]
    Filter(String),
    #[error("failed to initialize tracing subscriber: {0}")]
    Subscriber(String),
}

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

fn trace_config_from_env(
    trace_env: Option<String>,
    rust_log_env: Option<String>,
) -> Result<Option<TraceConfig>, TraceInitError> {
    let Some(trace_mode) = trace_env else {
        return Ok(None);
    };
    if trace_mode.trim().is_empty() {
        return Ok(None);
    }
    if trace_mode != "json" {
        return Err(TraceInitError::UnsupportedMode(trace_mode));
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
    fn tracing_is_disabled_without_env() {
        assert!(trace_config_from_env(None, None)
            .expect("trace config parses")
            .is_none());
        assert!(trace_config_from_env(Some(String::new()), None)
            .expect("blank trace config parses")
            .is_none());
    }

    #[test]
    fn json_tracing_uses_default_filter_when_rust_log_missing() {
        let config = trace_config_from_env(Some("json".to_owned()), None)
            .expect("trace config parses")
            .expect("enabled tracing config");
        assert_eq!(config.filter, DEFAULT_TRACE_FILTER);
    }

    #[test]
    fn json_tracing_uses_rust_log_when_present() {
        let config = trace_config_from_env(
            Some("json".to_owned()),
            Some("loonfs_core=debug".to_owned()),
        )
        .expect("trace config parses")
        .expect("enabled tracing config");
        assert_eq!(config.filter, "loonfs_core=debug");
    }

    #[test]
    fn unsupported_trace_mode_is_rejected() {
        assert!(trace_config_from_env(Some("text".to_owned()), None).is_err());
    }
}
