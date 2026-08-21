//! Client configuration: the TOML-loaded [`ClientConfig`] and its
//! validation.

use crate::{ClientError, Result};
use http::Uri;
use loonfs_api::SecretString;
use serde::Deserialize;
use std::fs;
use std::net::IpAddr;
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
    pub auth_token: Option<SecretString>,
    /// Optional timeout for one HTTP request, in milliseconds.
    ///
    /// When this value is not set, content transfers may continue as long as
    /// the connection makes progress. They still use a 60-second inactivity
    /// timeout and a limited number of attempts. Other replay-safe requests
    /// also have a built-in 90-second total retry limit.
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
    /// Disables the bounded automatic retry of quick-clearing transient
    /// failures: the retryable-unavailability codes (`server_busy`,
    /// `commit_queue_full`, `shutting_down` — a draining process telling the
    /// caller to retry against the next one) and network-level transport
    /// errors (connect failures, timeouts, resets). Off by default. The retry
    /// applies only to reads, commits (which carry a durable replay
    /// identity), and operations whose repeat semantics are idempotent;
    /// lifecycle mutations
    /// and upload-session creation are always single-attempt.
    #[serde(default)]
    pub disable_transient_retry: bool,
    /// PEM bundle of extra certificate authorities to trust for `https`
    /// server URLs, for a server whose certificate a private CA issued.
    /// Added to the platform trust store rather than replacing it, so a
    /// client configured this way still reaches publicly-trusted servers.
    #[serde(default)]
    pub ca_cert_path: Option<String>,
}

impl ClientConfig {
    /// Loads and validates a client config from TOML.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
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
    pub fn validate(&self) -> Result<()> {
        let server_uri = validate_absolute_http_url("server_url", &self.server_url)?;
        if let Some(token) = &self.auth_token {
            if token.is_blank() {
                return Err(ClientError::ConfigValidation {
                    field: "auth_token",
                    reason: "must not be empty".to_owned(),
                });
            }
            if server_uri.scheme_str() == Some("http") && !is_literal_loopback_host(&server_uri) {
                return Err(ClientError::ConfigValidation {
                    field: "server_url",
                    reason: "bearer tokens require https except for loopback http URLs".to_owned(),
                });
            }
        }
        if self.request_timeout_ms == Some(0) {
            return Err(ClientError::ConfigValidation {
                field: "request_timeout_ms",
                reason: "must be greater than zero; omit it for built-in deadlines only".to_owned(),
            });
        }
        if let Some(path) = &self.ca_cert_path {
            if path.trim().is_empty() {
                return Err(ClientError::ConfigValidation {
                    field: "ca_cert_path",
                    reason: "must not be empty; omit it to trust only the platform roots"
                        .to_owned(),
                });
            }
        }
        Ok(())
    }

    /// Reads the configured CA bundle into the certificates reqwest adds to
    /// the trust store. A path that cannot be read or does not hold PEM
    /// certificates fails here, before any request: a client that silently
    /// fell back to the platform roots would fail later and somewhere else.
    pub(crate) fn extra_root_certificates(&self) -> Result<Vec<reqwest::Certificate>> {
        let Some(path) = &self.ca_cert_path else {
            return Ok(Vec::new());
        };
        let path = path.trim();
        let pem = fs::read(path).map_err(|err| ClientError::ConfigValidation {
            field: "ca_cert_path",
            reason: format!("failed to read `{path}`: {err}"),
        })?;
        let certificates = reqwest::Certificate::from_pem_bundle(&pem).map_err(|err| {
            ClientError::ConfigValidation {
                field: "ca_cert_path",
                reason: format!("`{path}` is not a PEM certificate bundle: {err}"),
            }
        })?;
        if certificates.is_empty() {
            return Err(ClientError::ConfigValidation {
                field: "ca_cert_path",
                reason: format!("`{path}` holds no CERTIFICATE section"),
            });
        }
        Ok(certificates)
    }
}

fn validate_absolute_http_url(field: &'static str, value: &str) -> Result<Uri> {
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

    Ok(uri)
}

fn is_literal_loopback_host(uri: &Uri) -> bool {
    let Some(host) = uri.host() else {
        return false;
    };
    if host
        .strip_suffix('.')
        .unwrap_or(host)
        .eq_ignore_ascii_case("localhost")
    {
        return true;
    }
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(server_url: &str, auth_token: Option<&str>) -> ClientConfig {
        ClientConfig {
            server_url: server_url.to_owned(),
            auth_token: auth_token.map(SecretString::from),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        }
    }

    #[test]
    fn bearer_tokens_are_allowed_over_https_and_literal_loopback_http() {
        for server_url in [
            "https://example.internal",
            "http://localhost",
            "http://LOCALHOST",
            "http://localhost.",
            "http://127.0.0.1",
            "http://127.8.9.10",
            "http://[::1]",
        ] {
            let result = config(server_url, Some("test-token")).validate();
            assert!(
                result.is_ok(),
                "{server_url} should be accepted: {result:?}"
            );
        }
    }

    #[test]
    fn bearer_tokens_are_rejected_over_non_loopback_http() {
        for server_url in ["http://example.internal", "http://192.0.2.1"] {
            let error = config(server_url, Some("test-token"))
                .validate()
                .expect_err("non-loopback plaintext URL should be rejected");
            assert!(
                matches!(
                    error,
                    ClientError::ConfigValidation {
                        field: "server_url",
                        ref reason,
                    } if reason == "bearer tokens require https except for loopback http URLs"
                ),
                "unexpected validation error for {server_url}: {error}"
            );
        }
    }

    #[test]
    fn plaintext_http_without_a_bearer_token_is_allowed() {
        config("http://example.internal", None)
            .validate()
            .expect("unauthenticated plaintext deployments remain supported");
    }
}
