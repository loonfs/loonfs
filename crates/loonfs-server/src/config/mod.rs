//! Server configuration: strict TOML decoding of the listen address, store,
//! metadata cache limits, and runtime overrides, and the checks that a
//! config passes before the server starts from it.

mod memory;
mod server;
mod tables;
#[cfg(test)]
mod tests;
mod validate;

pub use loonfs::ReadConsistency;
pub use loonfs_http::JobMode;
pub use loonfs_objectstore::StoreConfig;
pub(crate) use memory::MemorySizing;
pub use server::{AuthConfig, ServerConfig, TlsServerConfig, TransportConfig};
pub use tables::{GrepConfig, MetadataCacheConfig, MetadataCacheSsdConfig};
pub use validate::{load_server_config, parse_server_config, ValidatedServerConfig};
