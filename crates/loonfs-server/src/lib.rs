//! The LoonFS server.
//!
//! Hosts an embedded [`loonfs`] runtime behind the v0 HTTP API for
//! remote-mode clients, with per-namespace write batching in front of the
//! commit protocol. Embedded-mode callers skip this crate entirely and use
//! `loonfs` directly.

mod config;
mod http;
mod trace;

pub use config::{
    load_server_config, RuntimeCacheConfigOverrides, ServerConfig, ServerConfigError, StoreConfig,
};
pub use http::{app, serve, serve_with_shutdown, ServeError, ServerLifecycle};
#[cfg(feature = "openapi")]
pub use http::{openapi_document, openapi_json_pretty};
pub use trace::{init_tracing_from_env, TraceInitError};
