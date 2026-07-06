//! LoonFS object-store boundary: provider adapters, durable key layout, and
//! the [`ObjectStore`] trait.

pub mod abs;
mod configured;
pub mod gcs;
pub mod keys;
mod keyspace;
pub mod layout;
pub mod local_fs_store;
pub mod metrics;
pub mod object_store;
pub mod presign;
pub mod probes;
pub mod provider;
mod provider_object_store;
pub mod r2;
pub mod s3;
mod s3_compatible;
mod secret;
mod store_config;

/// Compatibility alias for [`local_fs_store`], kept so existing imports of
/// `loonfs_objectstore::fs::LocalFsStore` keep resolving.
pub use local_fs_store as fs;

pub use configured::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
pub use object_store::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    SharedObjectStore,
};
pub use provider_object_store::{ProviderObjectStore, ProviderObjectStoreConfig};
pub use secret::SecretString;
pub use store_config::{StoreConfig, StoreConfigError};
