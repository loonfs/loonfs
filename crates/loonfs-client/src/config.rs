//! Client configuration: the TOML-loaded [`ClientConfig`] and its
//! validation.

use crate::{ClientError, Result};
use http::Uri;
use loonfs_types::config::{validate_absolute_http_url, ConfigFieldError};
use loonfs_types::SecretString;
use serde::Deserialize;
use std::fs;
use std::net::IpAddr;
use std::num::NonZeroU64;
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
    pub request_timeout_ms: Option<NonZeroU64>,
    /// Disables the bounded automatic retry of quick-clearing transient
    /// failures: the retryable-unavailability codes (`server_busy`,
    /// `commit_queue_full`, and `shutting_down`, which a draining process
    /// returns so that the caller retries against the next one) and
    /// network-level transport errors (connect failures, timeouts, resets).
    /// Off by default. The retry
    /// applies only to reads, commits (which include a durable replay
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

    /// Validates field invariants.
    ///
    /// [`Self::load`] and [`Client::new`](crate::Client::new) both run this, so
    /// a file-loaded config and a directly built one cannot diverge in what
    /// they accept.
    pub fn validate(&self) -> std::result::Result<(), ConfigFieldError> {
        let server_uri = validate_absolute_http_url("server_url", &self.server_url)?;
        if let Some(token) = &self.auth_token {
            if token.is_blank() {
                return Err(ConfigFieldError::Invalid {
                    field: "auth_token",
                    reason: "must not be empty".to_owned(),
                });
            }
            if server_uri.scheme_str() == Some("http") && !is_literal_loopback_host(&server_uri) {
                return Err(ConfigFieldError::Invalid {
                    field: "server_url",
                    reason: "bearer tokens require https except for loopback http URLs".to_owned(),
                });
            }
        }
        if let Some(path) = &self.ca_cert_path {
            if path.trim().is_empty() {
                return Err(ConfigFieldError::Invalid {
                    field: "ca_cert_path",
                    reason: "must not be empty; omit it to trust only the platform roots"
                        .to_owned(),
                });
            }
        }
        Ok(())
    }
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
    use crate::Client;
    use std::assert_matches;
    use tempfile::tempdir;

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
            assert_matches!(
                error,
                ConfigFieldError::Invalid {
                    field: "server_url",
                    ref reason,
                } if reason == "bearer tokens require https except for loopback http URLs",
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

    #[test]
    fn construction_validates_config_like_load_does() {
        let error = Client::new(ClientConfig {
            server_url: "ftp://example.com".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect_err("ftp scheme must be rejected");
        assert_matches!(
            &error,
            ClientError::ConfigField(loonfs_types::config::ConfigFieldError::Invalid {
                field: "server_url",
                ..
            }),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn client_and_config_debug_redact_the_auth_token() {
        let raw_token = "client-debug-secret";
        let config = ClientConfig {
            server_url: "https://example.com".to_owned(),
            auth_token: Some(raw_token.into()),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        };

        let config_debug = format!("{config:?}");
        assert!(!config_debug.contains(raw_token), "{config_debug}");
        assert!(config_debug.contains("<redacted>"), "{config_debug}");

        let client = Client::new(config).expect("valid client config");
        let client_debug = format!("{client:?}");
        assert!(!client_debug.contains(raw_token), "{client_debug}");
        assert!(client_debug.contains("<redacted>"), "{client_debug}");
    }

    #[test]
    fn an_unusable_ca_bundle_fails_construction_and_names_the_path() {
        let dir = tempdir().expect("tempdir");
        let missing = dir.path().join("absent.crt");
        let garbage = dir.path().join("garbage.crt");
        fs::write(&garbage, b"this is not a certificate\n").expect("write garbage");

        for path in [missing, garbage] {
            let display = path.display().to_string();
            let error = Client::new(ClientConfig {
                server_url: "https://example.com".to_owned(),
                auth_token: None,
                request_timeout_ms: None,
                disable_transient_retry: false,
                ca_cert_path: Some(display.clone()),
            })
            .expect_err("unusable ca bundle");
            match &error {
                ClientError::ConfigField(loonfs_types::config::ConfigFieldError::Invalid {
                    field: "ca_cert_path",
                    reason,
                }) => assert!(
                    reason.contains(&display),
                    "the reason must name the path, got: {reason}"
                ),
                other => panic!("unexpected error: {other:?}"),
            }
        }
    }

    #[test]
    fn client_config_rejects_unknown_keys() {
        let error = toml::from_str::<ClientConfig>(
            "server_url = \"http://localhost:1\"\nauth_tokn = \"oops\"\n",
        )
        .expect_err("unknown key must fail decode");
        assert!(error.to_string().contains("auth_tokn"), "{error}");

        let config: ClientConfig =
            toml::from_str("server_url = \"http://localhost:1\"\n").expect("minimal config");
        assert!(config.auth_token.is_none());
    }

    #[test]
    fn load_rejects_blank_auth_token() {
        let path = write_config(
            r#"
server_url = "http://127.0.0.1:9400"
auth_token = "   "
"#,
        );

        let error = ClientConfig::load(&path).expect_err("blank auth token");

        assert_matches!(
            error,
            ClientError::ConfigField(loonfs_types::config::ConfigFieldError::Invalid {
                field,
                ..
            }) if field == "auth_token",
            "expected config validation error, got {error:?}"
        );
    }

    #[test]
    fn load_preserves_missing_file_as_config_io() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("missing.toml");

        let error = ClientConfig::load(&path).expect_err("missing config");

        assert_matches!(error, ClientError::ConfigIo(_));
    }

    #[test]
    fn load_preserves_decode_error() {
        let path = write_config("server_url = [");

        let error = ClientConfig::load(&path).expect_err("decode error");

        assert_matches!(error, ClientError::ConfigDecode(_));
    }

    fn write_config(contents: &str) -> std::path::PathBuf {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("client.toml");
        fs::write(&path, contents).expect("write config");
        let _ = temp_dir.keep();
        path
    }
}
