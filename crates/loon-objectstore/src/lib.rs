#![forbid(unsafe_code)]

mod configured;
pub mod fs;
pub mod keys;
mod keyspace;
pub mod provider;
pub mod r2;
pub mod s3;
mod s3_compatible;

pub use configured::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
pub use object_store::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};

mod object_store;
