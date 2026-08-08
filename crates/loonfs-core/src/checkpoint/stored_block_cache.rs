//! The seam a node-local cache of encoded metadata SST blocks plugs into.
//!
//! This tier sits beneath the decoded [`MetadataTableCache`] and above
//! object storage. It holds stored section bytes — the same bytes the
//! segment object holds — so a hit still has to be decoded and verified
//! before any caller sees a row.
//!
//! [`MetadataTableCache`]: super::MetadataTableCache

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

/// Identifies one encoded stored section of one metadata SST segment.
///
/// The identity mirrors the decoded cache's. `payload_checksum` is the
/// segment's `sha256:<hex>` digest, which names immutable bytes, so an entry
/// can never go stale; `kind` and `offset` locate the section inside that
/// segment, so sections of one segment cannot collide.
///
/// Nothing else belongs in the identity. Stored lengths and CRCs are
/// validation inputs carried by the segment's block handles, and the decoder
/// checks them whether the bytes came from this tier or from the store.
/// There is no version field either: an implementation versions its own
/// on-disk directory instead.
///
/// Namespace manifests are deliberately not representable. The decoded cache
/// keys them by object key rather than by content digest, and they stay out
/// of this tier.
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

/// A cache that failed to shut down cleanly.
///
/// Shutdown is the one operation whose failure a caller can observe. The
/// bytes are a cache, so nothing durable is lost, but a host draining on its
/// way out should still be able to report why.
#[derive(Debug, Error)]
#[error("stored metadata block cache failed to close: {0}")]
pub struct StoredMetadataBlockCacheCloseError(String);

impl StoredMetadataBlockCacheCloseError {
    /// Wraps an implementation's shutdown failure message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// A node-local cache of encoded, stored metadata SST sections.
///
/// Object storage remains the only authority. This tier holds a copy of
/// bytes the store already holds, addressed by [`StoredMetadataBlockKey`],
/// and every hit is decoded and verified by the ordinary segment decoder
/// before a caller sees a row. A hit is therefore evidence that the bytes
/// were available locally and nothing more.
///
/// Lookups and inserts are best effort. An implementation records its own
/// failures and answers a failed lookup with `None` and a failed insert by
/// doing nothing, so a full, broken, or unreachable cache degrades to plain
/// object-store reads.
///
/// After [`close`](Self::close) the cache is inert: `get` returns `None`,
/// and `insert` and `invalidate` do nothing. `close` is idempotent.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const CHECKSUM: &str = "sha256:0f0e0d0c0b0a09080706050403020100";
    const OTHER_CHECKSUM: &str = "sha256:00112233445566778899aabbccddeeff";

    fn key(
        payload_checksum: &str,
        kind: StoredMetadataBlockKind,
        offset: u64,
    ) -> StoredMetadataBlockKey {
        StoredMetadataBlockKey {
            payload_checksum: payload_checksum.to_owned(),
            kind,
            offset,
        }
    }

    #[test]
    fn one_segment_section_has_one_key() {
        let first = key(CHECKSUM, StoredMetadataBlockKind::Data, 4096);
        let second = key(CHECKSUM, StoredMetadataBlockKind::Data, 4096);
        assert_eq!(first, second);
        // Implementations index by this key, so equality without a matching
        // hash would still lose the entry.
        let mut keys = HashSet::new();
        keys.insert(first);
        assert!(keys.contains(&second));
    }

    #[test]
    fn checksum_kind_and_offset_each_separate_keys() {
        // Every field varied once against one base key: whichever field
        // stopped counting, one of these would collide with the base.
        let keys: HashSet<StoredMetadataBlockKey> = HashSet::from([
            key(CHECKSUM, StoredMetadataBlockKind::Data, 4096),
            key(OTHER_CHECKSUM, StoredMetadataBlockKind::Data, 4096),
            key(CHECKSUM, StoredMetadataBlockKind::Index, 4096),
            key(CHECKSUM, StoredMetadataBlockKind::Filter, 4096),
            key(CHECKSUM, StoredMetadataBlockKind::Data, 0),
        ]);
        assert_eq!(keys.len(), 5);
    }
}
