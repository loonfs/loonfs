//! Client configuration: the TOML-loaded [`ClientConfig`] and its
//! validation.

use crate::ClientError;
use http::Uri;
use serde::Deserialize;
use std::fs;
use std::path::Path;

/// Client configuration loaded from TOML or built by the caller.
///
/// Strict like every config struct in the workspace: an unknown key is a
/// decode error, so a typo (`auth_tokn`) fails loudly instead of silently
/// producing an unauthenticated client.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    /// Base URL for the LoonFS server.
    pub server_url: String,
    /// Optional bearer token.
    pub auth_token: Option<String>,
    /// Optional overall per-request deadline in milliseconds. Unset means no
    /// whole-request deadline: requests are bounded only by the built-in
    /// 60-second socket inactivity timeouts, so slow-but-progressing large
    /// transfers are not cut off while a stalled connection still fails.
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
    /// Disables the bounded automatic retry of quick-clearing transient
    /// failures: the retryable-unavailability codes (`server_busy`,
    /// `commit_queue_full`, `shutting_down` — a draining process telling the
    /// caller to retry against the next one) and network-level transport
    /// errors (connect failures, timeouts, resets). Off by default. The retry
    /// applies only to reads, mutations with durable replay identity, and
    /// operations whose repeat semantics are idempotent; lifecycle mutations
    /// and upload-session creation are always single-attempt.
    #[serde(default)]
    pub disable_transient_retry: bool,
}

impl ClientConfig {
    /// Loads and validates a client config from TOML.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ClientError> {
        let bytes =
            fs::read(path.as_ref()).map_err(|err| ClientError::ConfigIo(err.to_string()))?;
        let config: Self = toml::from_str(
            std::str::from_utf8(&bytes)
                .map_err(|err| ClientError::ConfigDecode(err.to_string()))?,
        )
        .map_err(|err| ClientError::ConfigDecode(err.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Validates field invariants. [`Self::load`] and
    /// [`Client::new`](crate::Client::new) both run this, so a file-loaded
    /// config and a directly built one cannot diverge in what they accept.
    pub fn validate(&self) -> Result<(), ClientError> {
        validate_absolute_http_url("server_url", &self.server_url)?;
        if let Some(token) = &self.auth_token {
            if token.trim().is_empty() {
                return Err(ClientError::ConfigValidation {
                    field: "auth_token",
                    reason: "must not be empty".to_owned(),
                });
            }
        }
        if self.request_timeout_ms == Some(0) {
            return Err(ClientError::ConfigValidation {
                field: "request_timeout_ms",
                reason: "must be greater than zero; omit it for no deadline".to_owned(),
            });
        }
        Ok(())
    }
}

fn validate_absolute_http_url(field: &'static str, value: &str) -> Result<(), ClientError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ClientError::MissingConfigField { field });
    }

    let uri: Uri =
        trimmed
            .parse()
            .map_err(|err: http::uri::InvalidUri| ClientError::ConfigValidation {
                field,
                reason: err.to_string(),
            })?;

    match uri.scheme_str() {
        Some("http" | "https") => {}
        Some(other) => {
            return Err(ClientError::ConfigValidation {
                field,
                reason: format!("scheme must be http or https, got `{other}`"),
            });
        }
        None => {
            return Err(ClientError::ConfigValidation {
                field,
                reason: "must be an absolute http or https URL".to_owned(),
            });
        }
    }

    if uri.authority().is_none() {
        return Err(ClientError::ConfigValidation {
            field,
            reason: "must be an absolute http or https URL".to_owned(),
        });
    }

    Ok(())
}
