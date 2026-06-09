#![forbid(unsafe_code)]

mod checksum;
mod configured;
pub mod fs;
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
