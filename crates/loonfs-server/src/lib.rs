//! The LoonFS server.
//!
//! Hosts an embedded [`loonfs`] runtime behind the v0 HTTP API for
//! remote-mode clients, with per-namespace write batching in front of the
//! commit protocol. Embedded-mode callers skip this crate entirely and use
//! `loonfs` directly.

mod config;
mod http;
mod local_cache;
mod trace;

pub use config::{
    load_server_config, GrepConfig, GrepMode, LocalCacheConfig, MaintenanceMode,
    RuntimeCacheConfigOverrides, ServerConfig, ServerConfigError, StoreConfig, TlsServerConfig,
};
pub use http::{app, check_config, serve, serve_with_shutdown, ServeError, TlsConfigError};
#[cfg(feature = "openapi")]
pub use http::{openapi_document, openapi_json_pretty};
pub use local_cache::FoyerStoredMetadataBlockCache;
pub use trace::{init_tracing_from_env, TraceInitError};
