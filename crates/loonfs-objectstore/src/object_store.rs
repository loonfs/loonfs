//! The [`ObjectStore`] contract every provider implements, plus its
//! shared value and error types.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;

/// Shares one provider client across handles without changing its storage semantics.
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
    /// Complete object length in bytes at the observed version.
    pub size_bytes: u64,
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
    /// Identity, size, and modification metadata observed with these exact bytes.
    pub metadata: ObjectMetadata,
    /// Complete object payload from the same read as `metadata`.
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
    CompareAndSwap {
        /// Opaque token returned by a prior observation of this same key.
        expected_etag: String,
    },
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
    /// Reports a required object that was absent, distinct from optional-read `None`.
    #[error("object not found `{object_key}`")]
    NotFound {
        /// Logical object key that was required.
        object_key: String,
    },
    /// Reports a logical key that is empty, escaping, malformed, or otherwise outside its scope.
    #[error("invalid object key `{object_key}`: {message}")]
    InvalidKey {
        /// Caller-supplied key rejected before provider IO.
        object_key: String,
        /// Specific key-validation failure.
        message: String,
    },
    /// Not object-scoped: the content ref never resolved to an object key.
    #[error("invalid content ref: {0}")]
    InvalidContentRef(String),
    /// Reports a byte range whose bounds cannot select a valid position in the object.
    #[error("invalid byte range for `{object_key}`")]
    InvalidRange {
        /// Object for which the caller supplied invalid range bounds.
        object_key: String,
    },
    /// Reports a create-if-absent or compare-and-swap condition that did not hold.
    #[error("precondition failed for `{object_key}`")]
    PreconditionFailed {
        /// Object whose current state disagreed with the requested write mode.
        object_key: String,
    },
    /// The provider rejected the caller's identity or authorization —
    /// wrong, expired, or insufficient credentials. Configuration-shaped
    /// and never transient: retrying cannot help, an operator can.
    #[error("permission denied for `{object_key}`: {message}")]
    PermissionDenied {
        /// Object or listing prefix the provider refused to authorize.
        object_key: String,
        /// Sanitized provider explanation with credential material removed.
        message: String,
    },
    /// Not object-scoped: the store lacks a required capability.
    #[error("unsupported capability: {0}")]
    Unsupported(&'static str),
    /// Not object-scoped: store construction or configuration failed before
    /// any object was addressed.
    #[error("invalid object store configuration: {0}")]
    Configuration(String),
    /// Reports an IO, timeout, protocol, or provider failure with ambiguous completion.
    #[error("transport error for `{object_key}`: {message}")]
    Transport {
        /// Object or listing prefix whose operation did not complete observably.
        object_key: String,
        /// Sanitized provider or local-IO diagnostic.
        message: String,
    },
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
            | Self::PermissionDenied { object_key, .. }
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
            Self::PermissionDenied { message, .. } => {
                format!("permission denied: {message}")
            }
            Self::Unsupported(capability) => format!("unsupported capability: {capability}"),
            Self::Configuration(message) => {
                format!("invalid object store configuration: {message}")
            }
            Self::Transport { message, .. } => message.clone(),
        }
    }
}

/// Facade alias: signatures inside this crate use `Result<T>`.
pub type Result<T> = std::result::Result<T, ObjectStoreError>;

/// Defines the provider-independent durability and consistency boundary LoonFS relies on.
///
/// Implementations must satisfy the
/// [required guarantees](../../../docs/specs/format.md#11-required-guarantees).
#[async_trait]
pub trait ObjectStore: Send + Sync + Debug {
    /// Reads metadata for one key, returning `None` when the object is absent.
    ///
    /// The returned compare token belongs to this exact observation. Invalid
    /// keys and provider failures are returned as [`ObjectStoreError`].
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>>;

    /// Reads complete bytes and identity metadata from one self-consistent observation.
    ///
    /// Returns `None` when the object is absent; invalid keys and provider
    /// failures are returned as [`ObjectStoreError`].
    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>>;

    /// Reads a full object or one half-open byte range, returning `None` when absent.
    ///
    /// A range ending beyond the object is truncated; a descending range or
    /// start beyond the object returns [`ObjectStoreError::InvalidRange`].
    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>>;

    /// Writes bytes under the requested overwrite or provider-enforced precondition.
    ///
    /// Successful completion is immediately authoritative. Invalid keys,
    /// failed conditions, permission failures, and ambiguous transport failures are returned.
    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata>;

    /// Deletes a key idempotently and makes its absence immediately authoritative.
    ///
    /// Missing objects succeed; invalid keys, permission failures, and
    /// ambiguous transport failures are returned.
    async fn delete(&self, key: &str) -> Result<()>;

    /// Streams keys under `prefix` in ascending lexicographic order.
    ///
    /// Invalid prefixes and listing failures arrive as stream items.
    fn list_prefix_stream(&self, prefix: &str) -> BoxStream<'static, Result<String>>;

    /// Collects and sorts every key under `prefix`.
    ///
    /// The operation fails if prefix validation or any streamed provider page fails.
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys: Vec<String> = self.list_prefix_stream(prefix).try_collect().await?;
        keys.sort();
        Ok(keys)
    }

    /// Writes bytes unconditionally, replacing any existing object at `key`.
    ///
    /// Invalid keys, permission failures, and ambiguous transport failures are returned.
    async fn put_overwrite(&self, key: &str, bytes: Bytes) -> Result<ObjectMetadata> {
        self.put(key, bytes, PutMode::Overwrite).await
    }

    /// Creates `key` only when no object is present.
    ///
    /// Existing objects return [`ObjectStoreError::PreconditionFailed`];
    /// invalid keys, permission failures, and transport failures are also returned.
    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<ObjectMetadata> {
        self.put(key, bytes, PutMode::CreateIfAbsent).await
    }

    /// Writes `bytes` under an immutable `key` and accepts success only when
    /// the key contains exactly those bytes.
    ///
    /// Payloads below [`crate::PROVIDER_MULTIPART_THRESHOLD_BYTES`] use
    /// create-if-absent; payloads at or above that threshold use the store's
    /// multipart-capable overwrite path. Transport retries are safe only
    /// because every writer allowed to name this immutable key must supply
    /// identical bytes. Mutable keys must use [`Self::put`] and own their
    /// protocol-specific ambiguity resolution.
    async fn put_immutable_verified(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> std::result::Result<(), crate::ImmutableWriteError> {
        crate::immutable_write::put(self, key, bytes).await
    }

    /// Replaces `key` only while its current opaque token equals `expected_etag`.
    ///
    /// A missing object or stale token returns
    /// [`ObjectStoreError::PreconditionFailed`]; invalid keys, permission
    /// failures, and transport failures are also returned.
    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata> {
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
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        self.as_ref().head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        self.as_ref().get_with_metadata(key).await
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        self.as_ref().get(key, range).await
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        self.as_ref().put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.as_ref().delete(key).await
    }

    fn list_prefix_stream(&self, prefix: &str) -> BoxStream<'static, Result<String>> {
        self.as_ref().list_prefix_stream(prefix)
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.as_ref().list_prefix(prefix).await
    }

    async fn put_overwrite(&self, key: &str, bytes: Bytes) -> Result<ObjectMetadata> {
        self.as_ref().put_overwrite(key, bytes).await
    }

    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<ObjectMetadata> {
        self.as_ref().put_if_absent(key, bytes).await
    }

    async fn put_immutable_verified(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> std::result::Result<(), crate::ImmutableWriteError> {
        self.as_ref().put_immutable_verified(key, bytes).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata> {
        self.as_ref()
            .compare_and_swap(key, expected_etag, bytes)
            .await
    }
}

#[async_trait]
impl<T: ObjectStore + ?Sized> ObjectStore for &T {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        (*self).head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        (*self).get_with_metadata(key).await
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        (*self).get(key, range).await
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        (*self).put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        (*self).delete(key).await
    }

    fn list_prefix_stream(&self, prefix: &str) -> BoxStream<'static, Result<String>> {
        (*self).list_prefix_stream(prefix)
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        (*self).list_prefix(prefix).await
    }

    async fn put_overwrite(&self, key: &str, bytes: Bytes) -> Result<ObjectMetadata> {
        (*self).put_overwrite(key, bytes).await
    }

    async fn put_if_absent(&self, key: &str, bytes: Bytes) -> Result<ObjectMetadata> {
        (*self).put_if_absent(key, bytes).await
    }

    async fn put_immutable_verified(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> std::result::Result<(), crate::ImmutableWriteError> {
        (*self).put_immutable_verified(key, bytes).await
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata> {
        (*self).compare_and_swap(key, expected_etag, bytes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug)]
    struct ListOverrideStore {
        override_reached: Arc<AtomicBool>,
    }

    #[async_trait]
    impl ObjectStore for ListOverrideStore {
        async fn head(&self, _key: &str) -> Result<Option<ObjectMetadata>> {
            Ok(None)
        }

        async fn get_with_metadata(&self, _key: &str) -> Result<Option<ObjectBody>> {
            Ok(None)
        }

        async fn get(&self, _key: &str, _range: Option<ByteRange>) -> Result<Option<Bytes>> {
            Ok(None)
        }

        async fn put(&self, key: &str, _bytes: Bytes, _mode: PutMode) -> Result<ObjectMetadata> {
            Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            })
        }

        async fn delete(&self, _key: &str) -> Result<()> {
            Ok(())
        }

        fn list_prefix_stream(&self, _prefix: &str) -> BoxStream<'static, Result<String>> {
            Box::pin(stream::empty())
        }

        async fn list_prefix(&self, _prefix: &str) -> Result<Vec<String>> {
            self.override_reached.store(true, Ordering::SeqCst);
            Ok(vec!["overridden".to_owned()])
        }
    }

    #[tokio::test]
    async fn arc_dyn_store_forwards_overridden_list_prefix() {
        let override_reached = Arc::new(AtomicBool::new(false));
        let store: Arc<dyn ObjectStore> = Arc::new(ListOverrideStore {
            override_reached: Arc::clone(&override_reached),
        });

        let keys = <Arc<dyn ObjectStore> as ObjectStore>::list_prefix(&store, "prefix/")
            .await
            .expect("overridden list should succeed");

        assert_eq!(keys, vec!["overridden"]);
        assert!(override_reached.load(Ordering::SeqCst));
    }
}
