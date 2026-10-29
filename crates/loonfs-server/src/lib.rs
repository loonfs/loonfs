//! The LoonFS server.
//!
//! Hosts an embedded [`loonfs`] runtime behind the v0 HTTP API for
//! remote-mode clients, with per-namespace write batching in front of the
//! commit protocol. Embedded-mode callers skip this crate entirely and use
//! `loonfs` directly.
//!
//! Route handlers don't belong here: they're in `loonfs-http`, and this crate
//! adds configuration, TLS listeners, and the maintenance sweep.

mod config;
mod error;
mod http;
mod sweep;
mod trace;

pub use config::{
    load_server_config, parse_server_config, AuthConfig, GrepConfig, JobMode, MetadataCacheConfig,
    MetadataCacheSsdConfig, ReadConsistency, ServerConfig, StoreConfig, TlsServerConfig,
    TransportConfig, ValidatedServerConfig,
};
pub use error::{Result, ServerError};
pub use http::{app, check_config, probe_store, serve, serve_with_shutdown, AppConfig, AppState};
pub use sweep::{GarbageCollection, Sweep};
pub use trace::init_tracing_from_env;
