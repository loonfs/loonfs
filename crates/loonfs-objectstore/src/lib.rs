//! The LoonFS object-store boundary.
//!
//! LoonFS assumes only the narrow provider contract the format spec names —
//! create-if-absent, compare-and-swap, read-after-write visibility, prefix
//! listing — and this crate owns that boundary: the [`ObjectStore`] trait,
//! provider adapters for S3, Cloudflare R2, Google Cloud Storage, and the
//! local filesystem, the durable key layout in [`keys`] and [`layout`], and
//! the conformance [`probes`] that keep provider assumptions honest.

mod configured;
pub mod fs;
pub mod gcs;
pub mod keys;
mod keyspace;
pub mod layout;
pub mod metrics;
pub mod probes;
pub mod provider;
mod provider_object_store;
pub mod r2;
pub mod s3;
mod s3_compatible;

pub use configured::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
pub use object_store::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    SharedObjectStore,
};
pub use provider_object_store::{ProviderObjectStore, ProviderObjectStoreConfig};

pub mod object_store;
