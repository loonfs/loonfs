//! The LoonFS object-store boundary.
//!
//! LoonFS assumes only the narrow provider contract the format spec names —
//! create-if-absent, compare-and-swap, read-after-write visibility, prefix
//! listing — and this crate owns that boundary: the [`ObjectStore`] trait,
//! provider adapters for S3, Cloudflare R2, Google Cloud Storage, Azure Blob
//! Storage, and the local filesystem, the durable key layout in [`keys`] and
//! [`layout`], and the conformance [`probes`] that keep provider assumptions
//! honest.

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
mod store_io_runtime;
pub mod timing;
mod transfer_timeouts;

/// Compatibility alias for [`local_fs_store`], kept so existing imports of
/// `loonfs_objectstore::fs::LocalFsStore` keep resolving.
pub use local_fs_store as fs;

pub use configured::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
pub use object_store::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    SharedObjectStore,
};
pub use provider_object_store::{
    ProviderObjectStore, ProviderObjectStoreConfig, PROVIDER_ATTEMPT_TIMEOUT,
    PROVIDER_CONNECT_TIMEOUT, PROVIDER_MULTIPART_PART_BYTES, PROVIDER_MULTIPART_PART_WINDOW,
    PROVIDER_MULTIPART_THRESHOLD_BYTES, PROVIDER_OP_DEADLINE, PROVIDER_TRANSFER_ATTEMPT_TIMEOUT,
};
pub use secret::SecretString;
pub use store_config::{StoreConfig, StoreConfigError};
