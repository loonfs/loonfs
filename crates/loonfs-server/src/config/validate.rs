//! The checks that turn a decoded [`ServerConfig`] into a
//! [`ValidatedServerConfig`], and the loaders that decode and check a
//! config file.

use super::memory::read_cgroup_memory_limit;
use super::{AuthConfig, MemorySizing, ServerConfig, TransportConfig};
use crate::{Result, ServerError};
use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs_grep::GramIndexBuildPolicy;
use loonfs_http::AuthPolicy;
use loonfs_types::config::{non_blank, validate_non_empty, ConfigFieldError};
use loonfs_types::env::{AUTH_TOKEN_ENV, CONTENT_TOKEN_SECRET_ENV};
use loonfs_types::{SecretString, WriterId};
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;

/// A server config that passed every check, and the values that the checks
/// derived from it.
///
/// [`ServerConfig::validate`] is the only constructor of a
/// `ValidatedServerConfig`, so the server can't start from a config that
/// skipped a check.
#[derive(Debug)]
pub struct ValidatedServerConfig {
    pub(crate) config: ServerConfig,
    pub(crate) bind: SocketAddr,
    pub(crate) auth_policy: AuthPolicy,
    pub(crate) memory: MemorySizing,
    pub(crate) grep_policy: GramIndexBuildPolicy,
    pub(crate) maintenance_writer_id: WriterId,
}

impl ValidatedServerConfig {
    /// Returns the config that passed the checks.
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    /// Returns the line that `loonfs-server --check-config` prints when the
    /// config loads.
    ///
    /// It names the address, provider, and maintenance settings.
    pub fn check_summary(&self) -> String {
        format!(
            "config ok: bind {}, store {}, maintenance {}",
            self.config.bind.trim(),
            self.config.store.kind().as_str(),
            self.config.maintenance.as_str()
        )
    }
}

impl ServerConfig {
    /// Fills the `[auth]` table's token and `content_token_secret` from the
    /// environment when the file leaves the token or the secret unset or
    /// blank. Non-blank file values win, blank environment values are
    /// ignored, and an `open` auth table takes no token.
    pub(super) fn apply_env_fallbacks(
        &mut self,
        auth_token_env: Option<String>,
        content_token_secret_env: Option<String>,
    ) {
        if let Some(token) = non_blank(auth_token_env) {
            match &mut self.auth {
                None => {
                    self.auth = Some(AuthConfig::BearerToken {
                        token: SecretString::new(token),
                    });
                }
                Some(AuthConfig::BearerToken { token: file_token }) if file_token.is_blank() => {
                    *file_token = SecretString::new(token);
                }
                Some(AuthConfig::BearerToken { .. } | AuthConfig::Open) => {}
            }
        }
        if self.content_token_secret.is_blank() {
            if let Some(secret) = non_blank(content_token_secret_env) {
                self.content_token_secret = SecretString::new(secret);
            }
        }
    }

    /// Checks every setting, and returns the config with the values that
    /// the checks derive: the bind address, the auth policy, the memory
    /// sizing, the grep build policy, and the maintenance writer ID.
    pub fn validate(self) -> Result<ValidatedServerConfig> {
        let bind = validate_socket_addr("bind", &self.bind)?;
        let auth_policy = self.auth_policy(&bind)?;
        self.check_transport(&bind)?;
        for (field, value) in [
            ("max_upload_bytes", self.max_upload_bytes),
            ("request_deadline_ms", self.request_deadline_ms),
            ("shutdown_deadline_ms", self.shutdown_deadline_ms),
            ("max_download_bytes", self.max_download_bytes),
        ] {
            validate_positive(field, value, None)?;
        }
        for (field, value) in [
            ("max_in_flight_requests", self.max_in_flight_requests),
            ("max_connections", self.max_connections),
        ] {
            if value.get() > tokio::sync::Semaphore::MAX_PERMITS {
                return Err(ServerError::Config(ConfigFieldError::Invalid {
                    field,
                    reason: format!("must not exceed {}", tokio::sync::Semaphore::MAX_PERMITS),
                }));
            }
        }
        for (field, interval_ms) in [
            (
                "manifest_revalidation_interval_ms",
                self.manifest_revalidation_interval_ms,
            ),
            ("max_staleness_ms", self.max_staleness_ms),
        ] {
            if interval_ms.is_some_and(|interval_ms| interval_ms > READ_REVALIDATION_BOUND_MS) {
                return Err(ServerError::Config(ConfigFieldError::Invalid {
                    field,
                    reason: format!(
                        "must not exceed {READ_REVALIDATION_BOUND_MS}, the revalidation bound"
                    ),
                }));
            }
        }
        for (field, value) in [
            ("tick_interval_ms", self.tick_interval_ms),
            ("maintenance_interval_ms", self.maintenance_interval_ms),
            ("gc_interval_ms", self.gc_interval_ms),
            ("full_sweep_interval_ms", self.full_sweep_interval_ms),
            (
                "idle_session_close_after_ms",
                self.idle_session_close_after_ms,
            ),
        ] {
            validate_positive(
                field,
                value,
                Some("set `maintenance = \"serve_only\"` to turn the maintenance sweep off"),
            )?;
        }
        let invalid_grep = |reason: String| {
            ServerError::Config(ConfigFieldError::Invalid {
                field: "grep",
                reason,
            })
        };
        self.grep
            .worker
            .validate()
            .map_err(|error| invalid_grep(error.to_string()))?;
        let grep_policy = self
            .grep
            .worker
            .build_policy()
            .map_err(|error| invalid_grep(error.to_string()))?;
        validate_non_empty("content_token_secret", self.content_token_secret.expose())?;
        if let Some(ssd) = &self.metadata_cache.ssd {
            validate_non_empty("metadata_cache.ssd.directory", &ssd.directory)?;
        }
        self.store.validate()?;
        let maintenance_writer_id = WriterId::parse(format!("{}-maintenance", self.writer_id))
            .map_err(|error| {
                ServerError::Config(ConfigFieldError::Invalid {
                    field: "writer_id",
                    reason: error.to_string(),
                })
            })?;

        let cgroup_limit = if self.memory_limit_bytes.is_none() && cfg!(target_os = "linux") {
            read_cgroup_memory_limit(&[
                Path::new("/sys/fs/cgroup/memory.max"),
                Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
            ])
        } else {
            None
        };
        let memory = self.memory_sizing(cgroup_limit)?;
        Ok(ValidatedServerConfig {
            config: self,
            bind,
            auth_policy,
            memory,
            grep_policy,
            maintenance_writer_id,
        })
    }

    /// Returns how the server authenticates requests, and rejects a server
    /// that would serve the network without authentication unless the
    /// config has an `[auth]` table.
    fn auth_policy(&self, bind: &SocketAddr) -> Result<AuthPolicy> {
        match &self.auth {
            Some(AuthConfig::BearerToken { token }) => {
                validate_non_empty("auth.token", token.expose())?;
                Ok(AuthPolicy::BearerToken(token.clone()))
            }
            Some(AuthConfig::Open) => Ok(AuthPolicy::Unauthenticated),
            None if bind_serves_beyond_localhost(bind) => {
                Err(ServerError::Config(ConfigFieldError::Invalid {
                    field: "auth",
                    reason: format!(
                        "bind `{bind}` serves every endpoint to the network without \
                         authentication; set `[auth]` with `kind = \"bearer_token\"` and a \
                         `token` (or `LOONFS_AUTH_TOKEN`), or set `kind = \"open\"` to serve \
                         open on purpose"
                    ),
                }))
            }
            None => Ok(AuthPolicy::Unauthenticated),
        }
    }

    /// Checks the TLS identity's paths, and rejects plaintext beyond
    /// localhost unless the config has a `[transport]` table.
    fn check_transport(&self, bind: &SocketAddr) -> Result<()> {
        match &self.transport {
            Some(TransportConfig::Tls(tls)) => {
                validate_non_empty("transport.cert_path", &tls.cert_path)?;
                validate_non_empty("transport.key_path", &tls.key_path)?;
                Ok(())
            }
            Some(TransportConfig::PlaintextBehindProxy) => Ok(()),
            None if bind_serves_beyond_localhost(bind) => {
                Err(ServerError::Config(ConfigFieldError::Invalid {
                    field: "transport",
                    reason: format!(
                        "bind `{bind}` serves the network in plaintext, exposing the bearer \
                         token and the presigned object-store URLs in upload responses; set \
                         `[transport]` with `kind = \"tls\"`, `cert_path`, and `key_path`, or \
                         with `kind = \"plaintext_behind_proxy\"` when TLS terminates in front \
                         of this process"
                    ),
                }))
            }
            None => Ok(()),
        }
    }
}

/// Loads and validates a server configuration from TOML.
pub fn load_server_config(path: impl AsRef<Path>) -> Result<ValidatedServerConfig> {
    let bytes = fs::read(path.as_ref()).map_err(|err| ServerError::ConfigRead(err.to_string()))?;
    let source =
        std::str::from_utf8(&bytes).map_err(|err| ServerError::ConfigDecode(err.to_string()))?;
    parse_server_config(source)
}

/// Parses and validates a server configuration supplied directly as TOML.
///
/// Container hosts that cannot mount a configuration file use this entry
/// point. Provider and server secrets should still come from their dedicated
/// environment variables instead of being included in `source`.
pub fn parse_server_config(source: &str) -> Result<ValidatedServerConfig> {
    let mut config: ServerConfig =
        toml::from_str(source).map_err(|err| ServerError::ConfigDecode(err.to_string()))?;
    config.apply_env_fallbacks(
        env::var(AUTH_TOKEN_ENV).ok(),
        env::var(CONTENT_TOKEN_SECRET_ENV).ok(),
    );
    config.validate()
}

pub(super) fn validate_positive(field: &'static str, value: u64, hint: Option<&str>) -> Result<()> {
    if value > 0 {
        return Ok(());
    }
    let mut reason = "must be greater than zero".to_owned();
    if let Some(hint) = hint {
        reason.push_str("; ");
        reason.push_str(hint);
    }
    Err(ServerError::Config(ConfigFieldError::Invalid {
        field,
        reason,
    }))
}

fn validate_socket_addr(field: &'static str, value: &str) -> Result<SocketAddr> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ServerError::Config(ConfigFieldError::Missing { field }));
    }
    trimmed.parse::<SocketAddr>().map_err(|err| {
        ServerError::Config(ConfigFieldError::Invalid {
            field,
            reason: err.to_string(),
        })
    })
}

/// Whether a bind address accepts connections from other hosts: any
/// non-loopback ip, including the unspecified addresses (`0.0.0.0`, `[::]`)
/// that bind every interface.
fn bind_serves_beyond_localhost(addr: &SocketAddr) -> bool {
    !addr.ip().is_loopback()
}
