//! Whole-value writes for in-memory and test stores.
//!
//! [`ObjectStore::put_streamed`] and [`ObjectStore::assemble`] have no
//! default, so a provider can't buffer a whole value by accident. A store that
//! keeps every object in memory anyway implements them with these functions,
//! which route the write through the store's own [`ObjectStore::put`] and
//! [`ObjectStore::put_immutable_verified`].

use crate::{AssemblySource, ByteStream, ObjectMetadata, ObjectStore, PutMode, Result};
use futures::StreamExt as _;
use loonfs_types::Checksum;

/// Collects `body` into one buffer, writes it with `store`'s own
/// [`ObjectStore::put`], and returns the number of bytes stored.
pub async fn put_streamed<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    mut body: ByteStream,
    mode: PutMode,
) -> Result<u64> {
    let mut buffered = bytes::BytesMut::new();
    while let Some(chunk) = body.next().await {
        buffered.extend_from_slice(&chunk?);
    }
    let size_bytes = buffered.len() as u64;
    store.put(key, buffered.freeze(), mode).await?;
    Ok(size_bytes)
}

/// Reads `sources` and `tail` into one buffer, checks it against `expected`,
/// and creates `key` with `store`'s own
/// [`ObjectStore::put_immutable_verified`].
pub async fn assemble<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    sources: &[AssemblySource],
    tail: Vec<bytes::Bytes>,
    expected: &Checksum,
) -> Result<ObjectMetadata> {
    let bytes = crate::assembly::read_sources(store, sources, tail, expected, key).await?;
    store.put_immutable_verified(key, bytes).await
}
