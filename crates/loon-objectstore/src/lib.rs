//! Object-store abstraction layer for LoonDB.
//!
//! This crate defines the [`ObjectStore`] trait and provides implementations for local
//! filesystem, AWS S3, and Cloudflare R2. It is the only layer that should know provider
//! quirks — higher layers depend only on the trait, its error types, and opaque compare
//! tokens.

#![forbid(unsafe_code)]

mod configured;
pub mod error;
pub mod fs;
pub mod keys;
mod keyspace;
pub mod provider;
pub mod r2;
pub mod s3;
mod s3_compatible;

use crate::error::ObjectStoreError;
pub use configured::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
use serde::{Deserialize, Serialize};

/// Metadata returned by a successful `head` or `put` call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    /// Opaque compare token for one object version.
    ///
    /// This is suitable for immediate compare-and-swap on the same object key. It is not
    /// canonical content identity and callers must not derive provider-specific meaning from it.
    pub etag: Option<String>,
    pub size_bytes: u64,
}

/// Controls the write semantics of a `put` call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PutMode {
    /// Unconditionally overwrite any existing object.
    Overwrite,
    /// Write only if the key does not already exist. Returns `PreconditionFailed` if it does.
    CreateIfAbsent,
    /// Write only if the current etag matches. Returns `PreconditionFailed` on mismatch.
    CompareAndSwap { expected_etag: String },
}

/// A byte range for partial object reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteRange {
    /// First byte to read (inclusive, zero-based).
    pub start_inclusive: u64,
    /// First byte to exclude (exclusive, zero-based).
    pub end_exclusive: u64,
}

pub trait ObjectStore {
    /// Provider-specific headers, status codes, and SDK errors must terminate inside this trait.
    ///
    /// Higher layers may depend only on the returned bytes, listing results, opaque compare
    /// tokens, and the stable `ObjectStoreError` variants.
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError>;
    fn get(&self, key: &str, range: Option<ByteRange>)
        -> Result<Option<Vec<u8>>, ObjectStoreError>;
    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError>;
    fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError>;

    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put(key, bytes, PutMode::Overwrite)
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put(key, bytes, PutMode::CreateIfAbsent)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: &[u8],
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put(
            key,
            bytes,
            PutMode::CompareAndSwap {
                expected_etag: expected_etag.to_owned(),
            },
        )
    }
}
