//! Resolves the active profile and namespace from flags, config defaults,
//! and the environment.

use crate::backend::{Backend, EmbeddedBackend, RemoteBackend};
use crate::backend_error::map_runtime_error;
use crate::config::{default_config_path, load_config, CliConfig, ProfileConfig, StoreConfig};
use crate::error::CliError;
use crate::profiles::{default_namespace, resolve_profile};
use loonfs::{FsAdmin, FsBackgroundWork, FsWriter, SharedObjectStore, TraceStoreKind};
use loonfs_api::{ErrorCode, NamespaceId};
use loonfs_client::{Client, ClientConfig};
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

pub(crate) fn load_cli_config() -> Result<LoadedConfig, CliError> {
    let path = default_config_path()?;
    let config = load_config(&path)?;
    Ok(LoadedConfig { path, config })
}

pub(crate) async fn resolve_target_profile(
    explicit_profile: Option<&str>,
    no_retry: bool,
) -> Result<ResolvedProfile, CliError> {
    let loaded = load_cli_config()?;
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
            namespace: parse_namespace_id(namespace)?,
        });
    }
    let (profile_name, profile) = resolve_profile(config, explicit_profile)?;
    let namespace =
        default_namespace(profile).ok_or_else(|| CliError::no_default_namespace(profile_name))?;
    Ok(ResolvedNamespace {
        namespace: parse_namespace_id(namespace)?,
    })
}

/// Parses a namespace argument once at the CLI boundary. A malformed id
/// surfaces its registry code so both profile modes report the same code
/// the server would serve for it.
pub(crate) fn parse_namespace_id(namespace: &str) -> Result<NamespaceId, CliError> {
    NamespaceId::parse(namespace)
        .map_err(|error| CliError::new(ErrorCode::InvalidRequest.as_str(), error.to_string()))
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
    backend: RemoteBackend,
}

impl ResolvedTarget {
    pub(crate) async fn resolve(profile: &ProfileConfig, no_retry: bool) -> Result<Self, CliError> {
        match profile {
            ProfileConfig::Embedded {
                store,
                writer_id,
                writer_version,
                ..
            } => Ok(Self::Embedded(Box::new(
                EmbeddedTarget::new(store, writer_id.as_deref(), writer_version.as_deref()).await?,
            ))),
            ProfileConfig::Remote {
                server_url,
                auth_token,
                ..
            } => Ok(Self::Remote(RemoteTarget::new(
                server_url,
                auth_token.as_ref().map(|token| token.expose()),
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

    pub(crate) fn backend(&self) -> &dyn Backend {
        match self {
            ResolvedTarget::Embedded(target) => &target.backend,
            ResolvedTarget::Remote(target) => &target.backend,
        }
    }
}

impl EmbeddedTarget {
    pub(super) async fn new(
        store_config: &StoreConfig,
        writer_id: Option<&str>,
        writer_version: Option<&str>,
    ) -> Result<Self, CliError> {
        let writer_id = writer_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(default_writer_id);
        let writer_version = writer_version
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("loon/{}", env!("CARGO_PKG_VERSION")));
        let trace_store_kind = TraceStoreKind::from(store_config.kind());
        // One command drives all three handles from one runtime, so they
        // deliberately share one provider client.
        let store: SharedObjectStore = Arc::new(
            store_config
                .configured_object_store()
                .map_err(|err| CliError::invalid_config(format!("invalid store config: {err}")))?,
        );
        let writer = FsWriter::builder_with_store(store.clone())
            .writer_id(writer_id.clone())
            .writer_version(writer_version.clone())
            // The server's policy: publishes past the WAL threshold schedule
            // their own tick. The backend settles scheduled work after each
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
            .actor_version(writer_version)
            .trace_store_kind(trace_store_kind)
            .build()
            .await
            .map_err(map_runtime_error)?;
        let backend = EmbeddedBackend {
            writer,
            reader,
            admin,
        };
        Ok(Self { backend })
    }
}

impl RemoteTarget {
    fn new(server_url: &str, auth_token: Option<&str>, no_retry: bool) -> Result<Self, CliError> {
        let client = Client::new(ClientConfig {
            server_url: server_url.to_owned(),
            auth_token: auth_token.map(ToOwned::to_owned),
            request_timeout_ms: None,
            disable_transient_retry: no_retry,
        })
        .map_err(|error| CliError::invalid_config(error.to_string()))?;
        Ok(Self {
            backend: RemoteBackend::new(client),
        })
    }
}
