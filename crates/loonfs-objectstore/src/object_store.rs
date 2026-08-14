//! The [`ObjectStore`] contract every provider implements, plus its
//! shared value and error types.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, TryStreamExt};
use loonfs_api::Checksum;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;

/// Shares one provider client across handles without changing its storage semantics.
pub type SharedObjectStore = Arc<dyn ObjectStore>;

/// A payload delivered in pieces, for writes that must not hold it whole.
///
/// Chunk boundaries carry no meaning: an implementation regroups them into
/// whatever units the provider wants. A chunk error ends the write, and the
/// implementation cleans up whatever it had started.
pub type ByteStream = BoxStream<'static, Result<Bytes>>;

/// Metadata returned by a successful `head`, full-object `get`, or `put` call.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub last_modified_ms: Option<u64>,
}

/// Size and the stored full-object checksum for one object, read from a
/// single provider metadata request.
///
/// This is the evidence a completion check needs to decide whether the
/// object at a key is the object that was promised, without downloading it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObjectChecksum {
    /// Complete object length the provider reports.
    pub size_bytes: u64,
    /// Full-object checksum the provider stored with the object.
    pub checksum: Checksum,
}

/// One part of a client-driven multipart upload, as the client observed the
/// provider accept it.
///
/// LoonFS keeps no durable record of any part. Parts are the uploader's
/// bookkeeping, exactly as they are in the provider's own multipart API, and
/// this is the shape they come back in at completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPart {
    /// One-based part number.
    pub part_number: u32,
    /// Entity tag the provider returned for the accepted part.
    pub etag: String,
    /// Checksum the part was signed and accepted with.
    pub checksum: Checksum,
}

/// What a provider said about an attempt to assemble a multipart upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultipartCompletion {
    /// The provider accepted the assembly on this call.
    Assembled,
    /// The provider has no such upload. It was already consumed — by an
    /// earlier completion whose response was lost, or by an abort — so the
    /// object at the key, if any, is the only remaining evidence of what
    /// happened. Providers disagree about this case (AWS S3 replays a
    /// success carrying no checksum, Cloudflare R2 answers `NoSuchUpload`),
    /// which is exactly why the caller resolves it from the object instead.
    UnknownUpload,
}

/// Full object bytes returned with metadata from the same read operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectBody {
    /// Identity, size, and modification metadata observed with these exact bytes.
    pub metadata: ObjectMetadata,
    /// Complete object payload from the same read as `metadata`.
    pub bytes: Vec<u8>,
}

/// Controls the write semantics of a `put` call.
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Reports an object that exists but has no stored full-object checksum.
    ///
    /// This is distinct from [`Self::Unsupported`]: the store can perform
    /// checksum readback, but this particular object carries no checksum it
    /// can honestly return.
    #[error("stored full-object checksum missing for `{object_key}`")]
    StoredChecksumMissing {
        /// Object whose provider metadata carried no stored checksum.
        object_key: String,
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
            | Self::StoredChecksumMissing { object_key }
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
            Self::StoredChecksumMissing { .. } => "stored full-object checksum missing".to_owned(),
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

/// Drains a byte stream into one buffer, for implementations that cannot
/// write incrementally.
pub(crate) async fn collect_stream(mut body: ByteStream) -> Result<Bytes> {
    use futures::StreamExt as _;

    let mut buffered = bytes::BytesMut::new();
    while let Some(chunk) = body.next().await {
        buffered.extend_from_slice(&chunk?);
    }
    Ok(buffered.freeze())
}

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

    /// Reads size and the stored full-object checksum for one key, returning
    /// `None` when the object is absent.
    ///
    /// This is exactly one metadata request. S3-family stores issue
    /// `HeadObject` with checksum mode enabled; `GetObjectAttributes` is
    /// never used anywhere, because Cloudflare R2 answers it with 501 and
    /// code that reaches for it passes its tests against S3 and fails in
    /// production.
    ///
    /// Stores that cannot perform checksum readback return
    /// [`ObjectStoreError::Unsupported`]. A store that can ask but finds an
    /// existing object with no stored checksum returns
    /// [`ObjectStoreError::StoredChecksumMissing`]. That is the same
    /// capability line as presigned direct uploads: a deployment whose
    /// provider cannot show the checksum back also cannot offer `direct_put`,
    /// because completion would have nothing to verify against.
    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>> {
        let _ = key;
        Err(ObjectStoreError::Unsupported(
            "stored full-object checksum readback",
        ))
    }

    /// Opens a provider multipart upload targeting `key`, whose eventual
    /// checksum covers the whole assembled object.
    ///
    /// This is the control half of a client-driven multipart upload: the
    /// bytes travel from the client straight to the provider under signed
    /// per-part capabilities, and the store only opens, closes, and abandons
    /// the upload. Stores that cannot express it return
    /// [`ObjectStoreError::Unsupported`].
    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        let _ = key;
        Err(ObjectStoreError::Unsupported(
            "client-driven multipart upload",
        ))
    }

    /// Asks the provider to assemble `parts` into the object at `key`.
    ///
    /// `checksum` is supplied as a precondition where the
    /// provider honours one. It is not sufficient evidence on its own:
    /// Cloudflare R2 accepts a wrong claim, assembles the object, and
    /// reports the true checksum, so a caller must read the object's stored
    /// checksum back before believing anything about its bytes.
    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<MultipartCompletion> {
        let (_, _, _, _) = (key, provider_upload_id, parts, checksum);
        Err(ObjectStoreError::Unsupported(
            "client-driven multipart upload",
        ))
    }

    /// Abandons a provider multipart upload and the parts it accumulated.
    ///
    /// Aborting an upload that already completed is safe on every provider
    /// LoonFS supports: it succeeds and leaves the assembled object alone.
    /// An upload the provider has never heard of also succeeds, so cleanup
    /// can run without first proving what state it is cleaning up.
    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        let _ = (key, provider_upload_id);
        Err(ObjectStoreError::Unsupported(
            "client-driven multipart upload",
        ))
    }

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

    /// Writes a payload of unknown length without holding it whole, and
    /// reports how many bytes were stored.
    ///
    /// This is for immutable objects at uniquely-named keys — content
    /// blobs. Two properties define it:
    ///
    /// - **Bounded memory.** A store that can write incrementally holds at
    ///   most one internal buffer at a time, whatever the payload's length.
    ///   The default implementation cannot, and says so below.
    /// - **The body is consumed before any precondition is evaluated.** A
    ///   caller folding a digest over the stream as it forwards it therefore
    ///   always ends up with a digest over the complete payload, even when
    ///   the write is refused — which is what lets it tell "these are the
    ///   same bytes again" from "these are different bytes".
    ///
    /// `mode` is enforced by the provider, as part of the write, while the
    /// payload fits inside one internal part. Beyond that, an implementation
    /// that uses a provider multipart upload cannot enforce it that way,
    /// because providers assemble a multipart upload unconditionally. Such
    /// an implementation evaluates the condition against a separate
    /// observation of the key, taken after the payload has been consumed and
    /// immediately before the assembly. A key that was already occupied is
    /// still refused with [`ObjectStoreError::PreconditionFailed`]; a writer
    /// that lands between that observation and the assembly is not, and this
    /// write assembles over it.
    ///
    /// That is why this operation is for immutable keys only: on a key whose
    /// bytes are fixed by its name, the condition is a corruption tripwire
    /// rather than a concurrency control. A caller that must also detect the
    /// concurrent case has to check the object again before it relies on it.
    ///
    /// A failed or abandoned write leaves no provider state behind: an
    /// implementation that opened a multipart upload aborts it.
    ///
    /// The default implementation **buffers the whole stream** and delegates
    /// to [`Self::put`]. It exists so a provider without an incremental
    /// write is honest rather than absent: its memory cost is exactly what
    /// buffering the payload and calling `put` costs today, and it is
    /// bounded only by whatever bounds the caller puts on the stream.
    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        let bytes = collect_stream(body).await?;
        let size_bytes = bytes.len() as u64;
        self.put(key, bytes, mode).await?;
        Ok(size_bytes)
    }

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

    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>> {
        self.as_ref().head_stored_checksum(key).await
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        self.as_ref().create_multipart_upload(key).await
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<MultipartCompletion> {
        self.as_ref()
            .complete_multipart_upload(key, provider_upload_id, parts, checksum)
            .await
    }

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        self.as_ref()
            .abort_multipart_upload(key, provider_upload_id)
            .await
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

    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        self.as_ref().put_streamed(key, body, mode).await
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

    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>> {
        (*self).head_stored_checksum(key).await
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        (*self).create_multipart_upload(key).await
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<MultipartCompletion> {
        (*self)
            .complete_multipart_upload(key, provider_upload_id, parts, checksum)
            .await
    }

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        (*self)
            .abort_multipart_upload(key, provider_upload_id)
            .await
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

    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        (*self).put_streamed(key, body, mode).await
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
