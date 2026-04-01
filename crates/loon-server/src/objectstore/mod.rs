//! Object-store implementations for LoonDB.
//!
//! The [`ObjectStore`] trait and core types are defined in `loon-types` and re-exported here.
//! This module provides concrete implementations for local filesystem, AWS S3, and Cloudflare R2.

mod configured;
pub mod fs;
pub mod keys;
mod keyspace;
pub mod provider;
pub mod r2;
pub mod s3;
mod s3_compatible;

pub use configured::{ConfiguredObjectStore, ConfiguredObjectStoreKind};

// Re-export trait and types from loon-types so existing `loon_server::objectstore::*` paths work.
pub use loon_types::{
    object_store::ObjectStoreError, ByteRange, ObjectMetadata, ObjectStore, PutMode,
};
