//! The LoonFS object-store boundary.
//!
//! LoonFS assumes only the narrow provider contract the format spec names —
//! create-if-absent, compare-and-swap, read-after-write visibility, prefix
//! listing — and this crate owns that boundary: the [`ObjectStore`] trait,
//! provider adapters for S3, Cloudflare R2, Google Cloud Storage, Azure Blob
//! Storage, and the local filesystem, the durable key layout in [`keys`] and
//! [`layout`], and the contract probe in [`probe`] that proves a configured
//! store honours that contract — the same checks the conformance suite runs
//! against real providers.

#![warn(missing_docs)]

pub mod abs;
mod attempts;
mod aws_credentials;
mod configured;
pub mod crypto;
mod endpoint;
pub mod gcs;
mod immutable_write;
pub mod keys;
mod keyspace;
pub mod layout;
pub mod local_fs_store;
pub mod metrics;
pub mod object_store;
pub mod presign;
pub mod probe;
mod provider_object_store;
mod retry;
pub mod s3_compatible;
mod signed_request;
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
    ObjectStore, ObjectStoreError, ObjectStoreErrorClass, PutMode, Result, SharedObjectStore,
    StoredObjectChecksum,
};
pub use probe::{run_store_contract_probe, StoreProbeCheck, StoreProbeOutcome, StoreProbeReport};
pub use provider_object_store::{
    ProviderObjectStore, ProviderObjectStoreConfig, PROVIDER_ATTEMPT_TIMEOUT,
    PROVIDER_CONNECT_TIMEOUT, PROVIDER_MULTIPART_PART_BYTES, PROVIDER_MULTIPART_PART_WINDOW,
    PROVIDER_MULTIPART_THRESHOLD_BYTES, PROVIDER_OPERATION_DEADLINE, PROVIDER_STREAMED_PART_WINDOW,
    PROVIDER_TRANSFER_ATTEMPT_TIMEOUT,
};
pub use store_config::{
    AwsS3Credentials, AzureAbsCredentials, CloudflareR2Credentials, GcpGcsCredentials, StoreConfig,
    StoreConfigError, ACCESS_KEY_ID_ENV, SECRET_ACCESS_KEY_ENV, SESSION_TOKEN_ENV,
};
