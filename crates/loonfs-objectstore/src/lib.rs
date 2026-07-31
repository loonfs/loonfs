//! The LoonFS object-store boundary.
//!
//! LoonFS assumes only the narrow provider contract the format spec names —
//! create-if-absent, compare-and-swap, read-after-write visibility, prefix
//! listing — and this crate owns that boundary: the [`ObjectStore`] trait,
//! provider adapters for S3, Cloudflare R2, Google Cloud Storage, Azure Blob
//! Storage, and the local filesystem, the durable key layout in [`keys`] and
//! [`layout`], and the conformance suite that keeps provider assumptions
//! honest.

#![warn(missing_docs)]

pub mod abs;
mod configured;
pub mod gcs;
mod immutable_write;
pub mod keys;
mod keyspace;
pub mod layout;
pub mod local_fs_store;
pub mod metrics;
pub mod object_store;
pub mod presign;
mod provider_object_store;
mod retry;
pub mod s3_compatible;
mod secret;
mod store_config;
mod store_io_runtime;
#[cfg(test)]
mod test_support;
pub mod timing;
mod transfer_timeouts;

pub use configured::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
pub use immutable_write::ImmutableWriteError;
pub use object_store::ObjectStoreError as Error;
pub use object_store::{
    ByteRange, ByteStream, MultipartCompletion, MultipartPart, ObjectBody, ObjectMetadata,
    ObjectStore, ObjectStoreError, PutMode, Result, SharedObjectStore, StoredObjectChecksum,
};
pub use provider_object_store::{
    ProviderObjectStore, ProviderObjectStoreConfig, PROVIDER_ATTEMPT_TIMEOUT,
    PROVIDER_CONNECT_TIMEOUT, PROVIDER_MULTIPART_PART_BYTES, PROVIDER_MULTIPART_PART_WINDOW,
    PROVIDER_MULTIPART_THRESHOLD_BYTES, PROVIDER_OPERATION_DEADLINE, PROVIDER_STREAMED_PART_WINDOW,
    PROVIDER_TRANSFER_ATTEMPT_TIMEOUT,
};
pub use secret::SecretString;
pub use store_config::{StoreConfig, StoreConfigError};
