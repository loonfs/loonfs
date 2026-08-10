//! Interface for a node-local cache of encoded metadata SST blocks.
//!
//! This cache sits between the decoded [`MetadataTableCache`] and object
//! storage. It stores the original encoded section bytes, which are decoded
//! and verified before any rows are returned.
//!
//! [`MetadataTableCache`]: super::MetadataTableCache

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

/// Identifies one encoded section of an immutable metadata SST segment.
///
/// `payload_checksum` identifies the segment bytes. `kind` and `offset`
/// identify the section within that segment. Lengths and CRCs are validated by
/// the decoder and therefore are not part of the cache key.
///
/// Implementations may version their own on-disk format. Namespace manifests
/// are excluded because they are keyed by object path rather than content
/// digest.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct StoredMetadataBlockKey {
    pub payload_checksum: String,
    pub kind: StoredMetadataBlockKind,
    pub offset: u64,
}

/// Which stored section of a segment a key names.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum StoredMetadataBlockKind {
    Data,
    Index,
    Filter,
}

/// Error returned when the cache cannot shut down cleanly. Cached bytes are
/// rebuildable, but the host may report this failure during shutdown.
#[derive(Debug, Error)]
#[error("stored metadata block cache failed to close: {0}")]
pub struct StoredMetadataBlockCacheCloseError(String);

impl StoredMetadataBlockCacheCloseError {
    /// Wraps an implementation's shutdown failure message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Node-local cache of encoded metadata SST sections.
///
/// Object storage remains authoritative. Every cache hit is decoded and
/// verified before rows are returned. Cache operations are best effort:
/// lookup failures return `None`, and insert failures are ignored, so the
/// system falls back to object-storage reads.
///
/// After [`close`](Self::close), `get` returns `None` and mutation methods do
/// nothing. Calling `close` more than once is allowed.
#[async_trait]
pub trait StoredMetadataBlockCache: Send + Sync + Debug {
    /// Returns the stored bytes held for `key`, or `None` on a miss.
    async fn get(&self, key: &StoredMetadataBlockKey) -> Option<Bytes>;

    /// Offers `bytes` as the stored bytes for `key`. Whether the cache keeps
    /// them, and for how long, is the implementation's decision.
    fn insert(&self, key: StoredMetadataBlockKey, bytes: Bytes);

    /// Drops whatever is held for `key`.
    ///
    /// The one caller is a hit whose bytes did not decode, so an
    /// implementation reports this as an entry it had to reject rather than
    /// as an ordinary eviction.
    fn invalidate(&self, key: &StoredMetadataBlockKey);

    /// Flushes what the implementation wants to keep and makes the cache
    /// inert. Calling it more than once is allowed and does nothing further.
    async fn close(&self) -> Result<(), StoredMetadataBlockCacheCloseError>;
}
