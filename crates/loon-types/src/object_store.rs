use serde::{Deserialize, Serialize};
use thiserror::Error;

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

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error("object not found")]
    NotFound,
    #[error("invalid object key: {0}")]
    InvalidKey(String),
    #[error("invalid byte range")]
    InvalidRange,
    #[error("precondition failed")]
    PreconditionFailed,
    #[error("conflict")]
    Conflict,
    #[error("unsupported capability: {0}")]
    Unsupported(&'static str),
    #[error("transport error: {0}")]
    Transport(String),
}

pub trait ObjectStore {
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
