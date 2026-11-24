//! The LoonFS server.
//!
//! Hosts an embedded [`loonfs`] runtime behind the v0 HTTP API for
//! remote-mode clients, with per-namespace write batching in front of the
//! commit protocol. Embedded-mode callers skip this crate entirely and use
//! `loonfs` directly.
//!
//! Route handlers and the maintenance scheduler are in `loonfs-http`. This
//! crate adds configuration and TLS listeners, and starts the scheduler's
//! loops.

mod config;
mod error;
mod http;
mod trace;

pub use config::{
    load_server_config, parse_server_config, AuthConfig, GrepConfig, JobMode, MetadataCacheConfig,
    MetadataCacheSsdConfig, ReadConsistency, ServerConfig, StoreConfig, TlsServerConfig,
    TransportConfig, ValidatedServerConfig,
};
pub use error::{Result, ServerError};
pub use http::{app, check_config, probe_store, serve, serve_with_shutdown, AppConfig, AppState};
pub use trace::init_tracing_from_env;
