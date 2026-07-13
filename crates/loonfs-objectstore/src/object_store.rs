//! The [`ObjectStore`] contract every provider implements, plus its
//! shared value and error types.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;

pub type SharedObjectStore = Arc<dyn ObjectStore>;

/// Metadata returned by a successful `head`, full-object `get`, or `put` call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectMetadata {
    /// Opaque compare token for one object version.
    ///
    /// This is suitable for immediate compare-and-swap on the same object key. It is not
    /// canonical content identity and callers must not derive provider-specific meaning from it.
    pub etag: Option<String>,
    /// Provider version identifier when available.
    pub version: Option<String>,
    pub size_bytes: u64,
    /// Provider-verified full-object SHA-256 when available, normalized as `sha256:<64hex>`.
    ///
    /// This is part of the object-store contract only when present. Callers must fall back to
    /// reading and hashing object bytes if this field is absent.
    pub checksum_sha256: Option<String>,
    /// Provider last-modified time in unix milliseconds, when available.
    ///
    /// Advisory: garbage collection uses it for grace/reap age checks and
    /// treats an absent value as "young" (retain). Never a validity input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified_ms: Option<u64>,
}

/// Full object bytes returned with metadata from the same read operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectBody {
    pub metadata: ObjectMetadata,
    pub bytes: Vec<u8>,
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

/// Failure of one object-store operation.
///
/// Every object-scoped variant names the object it is about via `object_key`
/// (for list operations, the listed prefix). The exceptions are deliberate:
/// `InvalidContentRef` fails before a key exists, `Unsupported` is about a
/// store capability, and `Configuration` is about store construction.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ObjectStoreError {
    #[error("object not found `{object_key}`")]
    NotFound { object_key: String },
    #[error("invalid object key `{object_key}`: {message}")]
    InvalidKey { object_key: String, message: String },
    /// Not object-scoped: the content ref never resolved to an object key.
    #[error("invalid content ref: {0}")]
    InvalidContentRef(String),
    #[error("invalid byte range for `{object_key}`")]
    InvalidRange { object_key: String },
    #[error("precondition failed for `{object_key}`")]
    PreconditionFailed { object_key: String },
    #[error("conflict for `{object_key}`")]
    Conflict { object_key: String },
    /// Not object-scoped: the store lacks a required capability.
    #[error("unsupported capability: {0}")]
    Unsupported(&'static str),
    /// Not object-scoped: store construction or configuration failed before
    /// any object was addressed.
    #[error("invalid object store configuration: {0}")]
    Configuration(String),
    #[error("transport error for `{object_key}`: {message}")]
    Transport { object_key: String, message: String },
}

impl ObjectStoreError {
    /// Builds a [`ObjectStoreError::Transport`] for an operation on `object_key`.
    pub fn transport(object_key: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Transport {
            object_key: object_key.into(),
            message: message.into(),
        }
    }

    /// Key of the object (or listed prefix) the failing operation targeted,
    /// when the failure is object-scoped.
    pub fn object_key(&self) -> Option<&str> {
        match self {
            Self::NotFound { object_key }
            | Self::InvalidKey { object_key, .. }
            | Self::InvalidRange { object_key }
            | Self::PreconditionFailed { object_key }
            | Self::Conflict { object_key }
            | Self::Transport { object_key, .. } => Some(object_key),
            Self::InvalidContentRef(_) | Self::Unsupported(_) | Self::Configuration(_) => None,
        }
    }

    /// Failure text without the object key, for wrappers that record the key
    /// as their own structured field.
    pub fn message(&self) -> String {
        match self {
            Self::NotFound { .. } => "object not found".to_owned(),
            Self::InvalidKey { message, .. } => format!("invalid object key: {message}"),
            Self::InvalidContentRef(message) => format!("invalid content ref: {message}"),
            Self::InvalidRange { .. } => "invalid byte range".to_owned(),
            Self::PreconditionFailed { .. } => "precondition failed".to_owned(),
            Self::Conflict { .. } => "conflict".to_owned(),
            Self::Unsupported(capability) => format!("unsupported capability: {capability}"),
            Self::Configuration(message) => {
                format!("invalid object store configuration: {message}")
            }
            Self::Transport { message, .. } => message.clone(),
        }
    }
}

#[async_trait]
pub trait ObjectStore: Send + Sync + Debug {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError>;

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError>;

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError>;

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError>;

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>>;

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let mut keys: Vec<String> = self.list_prefix_stream(prefix).try_collect().await?;
        keys.sort();
        Ok(keys)
    }

    async fn put_overwrite(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put(key, bytes, PutMode::Overwrite).await
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put(key, bytes, PutMode::CreateIfAbsent).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put(
            key,
            bytes,
            PutMode::CompareAndSwap {
                expected_etag: expected_etag.to_owned(),
            },
        )
        .await
    }
}

#[async_trait]
impl<T: ObjectStore + ?Sized> ObjectStore for Arc<T> {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.as_ref().head(key).await
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.as_ref().head_with_checksum(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.as_ref().get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.as_ref().get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.as_ref().put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.as_ref().delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.as_ref().list_prefix_stream(prefix)
    }
}

#[async_trait]
impl<T: ObjectStore + ?Sized> ObjectStore for &T {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        (*self).head(key).await
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        (*self).head_with_checksum(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        (*self).get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        (*self).get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        (*self).put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        (*self).delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        (*self).list_prefix_stream(prefix)
    }
}
