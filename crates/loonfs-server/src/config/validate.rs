//! The checks that turn a decoded [`ServerConfig`] into a
//! [`ValidatedServerConfig`], and the loaders that decode and check a
//! config file.

use super::memory::read_cgroup_memory_limit;
use super::{AuthConfig, MemorySizing, ServerConfig, TransportConfig};
use crate::{Result, ServerError};
use loonfs_grep::GramIndexBuildPolicy;
use loonfs_http::AuthPolicy;
use loonfs_types::config::{non_blank, validate_non_empty, ConfigFieldError};
use loonfs_types::env::{AUTH_TOKEN_ENV, CONTENT_TOKEN_SECRET_ENV};
use loonfs_types::{
    generated_id, SecretString, WriterId, GENERATED_ID_BODY_BYTES, MAX_WRITER_ID_BYTES,
};
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
    /// The server's writer ID: the configured `writer_id`, a hyphen, and an
    /// ID that [`ServerConfig::validate`] generates, so two servers started
    /// from one config write different writer blocks.
    pub(crate) writer_id: WriterId,
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
            "config ok: bind {}, store {}, serve_maintenance_api {}, sweep_store {}",
            self.config.bind.trim(),
            self.config.store.kind().as_str(),
            self.config.serve_maintenance_api,
            self.config.sweep_store
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
    /// sizing, the grep build policy, and the writer IDs.
    ///
    /// Each call generates a new suffix for the server's writer ID, and a
    /// server validates its config once when it starts.
    pub fn validate(self) -> Result<ValidatedServerConfig> {
        let bind = validate_socket_addr("bind", &self.bind)?;
        let auth_policy = self.auth_policy(&bind)?;
        self.check_transport(&bind)?;
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
        // Each setting that the runtime checks has the same path in the
        // server's config as in the runtime's, so the runtime's field is the
        // field that the server reports.
        self.runtime_config().validate()?;
        let invalid_grep = |reason: String| {
            ServerError::Config(ConfigFieldError::Invalid {
                field: "grep",
                reason,
            })
        };
        let grep_worker = self.grep.map(|grep| grep.worker).unwrap_or_default();
        grep_worker
            .validate()
            .map_err(|error| invalid_grep(error.to_string()))?;
        let grep_policy = grep_worker
            .build_policy()
            .map_err(|error| invalid_grep(error.to_string()))?;
        validate_non_empty("content_token_secret", self.content_token_secret.expose())?;
        if let Some(ssd) = &self.metadata_cache.ssd {
            validate_non_empty("metadata_cache.ssd.directory", &ssd.directory)?;
        }
        self.store.validate()?;
        let writer_id = self.writer_id()?;

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
            writer_id,
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
            Some(AuthConfig::Open) => Ok(AuthPolicy::Open),
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
            None => Ok(AuthPolicy::Open),
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

pub(super) fn validate_positive(field: &'static str, value: u64) -> Result<()> {
    if value > 0 {
        return Ok(());
    }
    Err(ServerError::Config(ConfigFieldError::Invalid {
        field,
        reason: "must be greater than zero".to_owned(),
    }))
}

/// The prefix of the ID that the server generates for its writer ID.
const GENERATED_WRITER_ID_PREFIX: &str = "srv";

/// Most bytes of the configured `writer_id`: the writer ID limit minus the
/// hyphen and the generated ID that the server adds.
const MAX_CONFIGURED_WRITER_ID_BYTES: usize =
    MAX_WRITER_ID_BYTES - 1 - GENERATED_WRITER_ID_PREFIX.len() - 1 - GENERATED_ID_BODY_BYTES;

impl ServerConfig {
    /// Returns the server's writer ID, and rejects a configured `writer_id`
    /// too long to take the generated ID.
    fn writer_id(&self) -> Result<WriterId> {
        let configured = self.writer_id.as_str();
        if configured.len() > MAX_CONFIGURED_WRITER_ID_BYTES {
            return Err(ServerError::Config(ConfigFieldError::Invalid {
                field: "writer_id",
                reason: format!(
                    "{configured:?} must be {MAX_CONFIGURED_WRITER_ID_BYTES} bytes or fewer, \
                     because the server adds a generated ID to it"
                ),
            }));
        }
        Ok(WriterId::parse(format!(
            "{configured}-{}",
            generated_id(GENERATED_WRITER_ID_PREFIX)
        ))
        .expect("a configured writer ID within its limit should take the generated ID"))
    }
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
