//! Resolves the active profile and namespace from flags, config defaults,
//! and the environment.

use crate::backend::EmbeddedBackend;
use crate::backend_error::map_runtime_error;
use crate::config::{
    load_config, non_empty_env, CliConfig, ProfileConfig, StoreConfig, ACTOR_ID_ENV,
    ACTOR_KIND_ENV, NAMESPACE_ENV,
};
use crate::error::CliError;
use crate::profiles::{default_namespace, resolve_profile};
use loonfs::{FsAdmin, FsBackgroundWork, FsWriter, SharedObjectStore, TraceStoreKind};
use loonfs_api::env::AUTH_TOKEN_ENV;
use loonfs_api::{ActorId, ActorKind, ActorRef, NamespaceId, SecretString};
use loonfs_client::{Client, ClientConfig};
use loonfs_grep::{GrepBlockCache, GrepService, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES};
use std::sync::Arc;

pub(crate) struct LoadedConfig {
    pub path: std::path::PathBuf,
    pub config: CliConfig,
}

pub(crate) struct ResolvedProfile {
    pub profile_name: String,
    pub target: ResolvedTarget,
}

pub(crate) struct ResolvedNamespace {
    pub namespace: NamespaceId,
}

pub(crate) fn load_cli_config(config_path: &std::path::Path) -> Result<LoadedConfig, CliError> {
    let config = load_config(config_path)?;
    Ok(LoadedConfig {
        path: config_path.to_path_buf(),
        config,
    })
}

pub(crate) async fn resolve_target_profile(
    config_path: &std::path::Path,
    explicit_profile: Option<&str>,
    no_retry: bool,
) -> Result<ResolvedProfile, CliError> {
    let loaded = load_cli_config(config_path)?;
    resolve_target_profile_from_config(&loaded.config, explicit_profile, no_retry).await
}

pub(crate) async fn resolve_target_profile_from_config(
    config: &CliConfig,
    explicit_profile: Option<&str>,
    no_retry: bool,
) -> Result<ResolvedProfile, CliError> {
    let (profile_name, profile) = resolve_profile(config, explicit_profile)?;
    let target = ResolvedTarget::resolve(profile, no_retry).await?;
    Ok(ResolvedProfile {
        profile_name: profile_name.to_owned(),
        target,
    })
}

pub(crate) fn resolve_namespace(
    config: &CliConfig,
    explicit_profile: Option<&str>,
    explicit_namespace: Option<&str>,
) -> Result<ResolvedNamespace, CliError> {
    if let Some(namespace) = explicit_namespace {
        return Ok(ResolvedNamespace {
            namespace: parse_namespace_id(namespace)
                .map_err(|error| error.with_param("--namespace"))?,
        });
    }
    if let Some(namespace) = non_empty_env(NAMESPACE_ENV) {
        return Ok(ResolvedNamespace {
            namespace: parse_namespace_id(&namespace)?,
        });
    }
    let (profile_name, profile) = resolve_profile(config, explicit_profile)?;
    let namespace =
        default_namespace(profile).ok_or_else(|| CliError::no_default_namespace(profile_name))?;
    Ok(ResolvedNamespace {
        namespace: parse_namespace_id(namespace)?,
    })
}

pub(crate) fn resolve_actor(
    profile: &ProfileConfig,
    explicit_kind: Option<ActorKind>,
    explicit_id: Option<&str>,
) -> Result<ActorRef, CliError> {
    if explicit_kind.is_some() || explicit_id.is_some() {
        return actor_from_pair("--actor-kind", explicit_kind, "--actor-id", explicit_id);
    }

    let environment_kind = non_empty_env(ACTOR_KIND_ENV)
        .map(|value| parse_actor_kind(ACTOR_KIND_ENV, &value))
        .transpose()?;
    let environment_id = non_empty_env(ACTOR_ID_ENV);
    if environment_kind.is_some() || environment_id.is_some() {
        return actor_from_pair(
            ACTOR_KIND_ENV,
            environment_kind,
            ACTOR_ID_ENV,
            environment_id.as_deref(),
        );
    }

    Ok(profile.actor().unwrap_or_else(|| {
        ActorRef::service(ActorId::parse("loonfs-cli").expect("the CLI actor id should be valid"))
    }))
}

fn actor_from_pair(
    kind_name: &str,
    kind: Option<ActorKind>,
    id_name: &str,
    id: Option<&str>,
) -> Result<ActorRef, CliError> {
    let kind = kind.ok_or_else(|| {
        named_cli_input_error(
            kind_name,
            format!("{kind_name} and {id_name} must be supplied together"),
        )
    })?;
    let id = id.ok_or_else(|| {
        named_cli_input_error(
            id_name,
            format!("{kind_name} and {id_name} must be supplied together"),
        )
    })?;
    let id = ActorId::parse(id)
        .map_err(|error| named_cli_input_error(id_name, format!("invalid {id_name}: {error}")))?;
    Ok(ActorRef { kind, id })
}

fn parse_actor_kind(name: &str, value: &str) -> Result<ActorKind, CliError> {
    match value {
        "user" => Ok(ActorKind::User),
        "service" => Ok(ActorKind::Service),
        "system" => Ok(ActorKind::System),
        _ => Err(named_cli_input_error(
            name,
            format!("invalid {name}: expected user, service, or system"),
        )),
    }
}

fn named_cli_input_error(name: &str, message: String) -> CliError {
    let error = CliError::invalid_request(message);
    if name.starts_with('-') {
        error.with_param(name)
    } else {
        error
    }
}

/// Parses a namespace ID after argument parsing. Invalid IDs use the shared
/// `invalid_request` code in both embedded and remote modes.
pub(crate) fn parse_namespace_id(namespace: &str) -> Result<NamespaceId, CliError> {
    NamespaceId::parse(namespace).map_err(|error| CliError::invalid_request(error.to_string()))
}

fn default_writer_id() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "loonfs-cli".to_owned())
}

// --- Target resolution ---

pub(crate) enum ResolvedTarget {
    Embedded(Box<EmbeddedTarget>),
    Remote(RemoteTarget),
}

pub(crate) struct EmbeddedTarget {
    pub(super) backend: EmbeddedBackend,
}

pub(crate) struct RemoteTarget {
    pub(super) client: Client,
}

impl ResolvedTarget {
    pub(crate) async fn resolve(profile: &ProfileConfig, no_retry: bool) -> Result<Self, CliError> {
        match profile {
            ProfileConfig::Embedded {
                store, writer_id, ..
            } => Ok(Self::Embedded(Box::new(
                EmbeddedTarget::new(store, writer_id.as_deref()).await?,
            ))),
            ProfileConfig::Remote {
                server_url,
                auth_token,
                ca_cert_path,
                ..
            } => Ok(Self::Remote(RemoteTarget::new(
                server_url,
                resolve_remote_auth_token(auth_token),
                ca_cert_path.as_deref(),
                no_retry,
            )?)),
        }
    }

    pub(crate) fn mode_str(&self) -> &'static str {
        match self {
            ResolvedTarget::Embedded(_) => "embedded",
            ResolvedTarget::Remote(_) => "remote",
        }
    }
}

fn resolve_remote_auth_token(stored: &Option<SecretString>) -> Option<SecretString> {
    resolve_remote_auth_token_from(stored, non_empty_env)
}

fn resolve_remote_auth_token_from(
    stored: &Option<SecretString>,
    lookup: impl Fn(&str) -> Option<String>,
) -> Option<SecretString> {
    stored.clone().or_else(|| {
        lookup(AUTH_TOKEN_ENV)
            .filter(|token| !token.trim().is_empty())
            .map(SecretString::from)
    })
}

impl EmbeddedTarget {
    pub(super) async fn new(
        store_config: &StoreConfig,
        writer_id: Option<&str>,
    ) -> Result<Self, CliError> {
        // One command drives all three handles from one runtime, so they
        // deliberately share one provider client.
        let store: SharedObjectStore = store_config
            .configured_object_store()
            .map_err(|err| CliError::invalid_config(err.public_message().into_owned()))?
            .into_shared();
        Self::over_store(store, writer_id, TraceStoreKind::from(store_config.kind())).await
    }

    /// Opens the runtime handles over a store the caller already holds.
    ///
    /// [`Self::new`] opens the profile's configured store and passes it here.
    /// Callers that need to observe store operations may provide a wrapped
    /// store directly.
    pub(crate) async fn over_store(
        store: SharedObjectStore,
        writer_id: Option<&str>,
        trace_store_kind: TraceStoreKind,
    ) -> Result<Self, CliError> {
        let writer_id = writer_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(default_writer_id);
        let writer = FsWriter::builder_with_store(store.clone())
            .writer_id(writer_id.clone())
            // The server's policy: publishes past the WAL threshold schedule
            // their own step. The backend settles scheduled work after each
            // mutation, so a one-shot command exits with maintenance done
            // rather than stalling at the WAL backpressure cap.
            .background_work(FsBackgroundWork::Enabled)
            // A CLI invocation is one solo mutation: holding the commit
            // window open would only add its full delay to every command.
            .min_publish_interval_ms(0)
            .trace_store_kind(trace_store_kind)
            .build()
            .await
            .map_err(map_runtime_error)?;
        let reader = writer.reader();
        let admin = FsAdmin::builder_with_store(store)
            .actor_id(writer_id)
            .trace_store_kind(trace_store_kind)
            .build()
            .await
            .map_err(map_runtime_error)?;
        let grep_block_cache =
            Arc::new(GrepBlockCache::new(DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES));
        let backend = EmbeddedBackend {
            writer,
            reader,
            admin,
            // Embedded mode composes grep itself: the runtime handles above
            // know nothing about it.
            grep: GrepService::with_block_cache(Arc::clone(&grep_block_cache)),
            grep_block_cache,
        };
        Ok(Self { backend })
    }
}

impl RemoteTarget {
    fn new(
        server_url: &str,
        auth_token: Option<SecretString>,
        ca_cert_path: Option<&str>,
        no_retry: bool,
    ) -> Result<Self, CliError> {
        let client = Client::new(ClientConfig {
            server_url: server_url.to_owned(),
            auth_token,
            request_timeout_ms: None,
            disable_transient_retry: no_retry,
            ca_cert_path: ca_cert_path.map(ToOwned::to_owned),
        })
        .map_err(|error| CliError::invalid_config(error.to_string()))?;
        Ok(Self { client })
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_remote_auth_token_from, RemoteTarget};
    use loonfs_api::env::AUTH_TOKEN_ENV;
    use loonfs_api::SecretString;

    #[test]
    fn remote_auth_prefers_the_stored_token_then_falls_back_to_the_environment() {
        let stored = Some(SecretString::from("stored-token"));
        let resolved = resolve_remote_auth_token_from(&stored, |_| Some("env-token".to_owned()));
        assert_eq!(
            resolved.as_ref().map(SecretString::expose),
            Some("stored-token")
        );

        let resolved = resolve_remote_auth_token_from(&None, |_| Some("env-token".to_owned()));
        assert_eq!(
            resolved.as_ref().map(SecretString::expose),
            Some("env-token")
        );

        assert!(resolve_remote_auth_token_from(&None, |_| Some("  ".to_owned())).is_none());
    }

    #[test]
    fn environment_token_is_rejected_for_non_loopback_http() {
        let auth_token = resolve_remote_auth_token_from(&None, |name| {
            assert_eq!(name, AUTH_TOKEN_ENV);
            Some("environment-token".to_owned())
        });
        let error = RemoteTarget::new("http://example.internal", auth_token, None, false)
            .err()
            .expect("environment token over non-loopback plaintext HTTP should be rejected");

        assert_eq!(error.code, "invalid_config");
        assert!(
            error
                .message
                .contains("bearer tokens require https except for loopback http URLs"),
            "{}",
            error.message
        );
    }
}
