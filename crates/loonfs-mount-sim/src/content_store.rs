//! The content store: the bytes of every chain, keyed by the chain's owner and content ID.
//!
//! A chain's bytes are kept whole. A revision's content reference reads a prefix of its chain, so
//! one chain serves every revision that an append extended it for.

use loonfs_types::{ContentId, ContentRef, NamespaceId};
use std::collections::BTreeMap;

/// The key of one chain: its owner namespace and content ID.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ChainKey {
    owner_namespace_id: NamespaceId,
    content_id: ContentId,
}

impl ChainKey {
    pub(crate) fn new(owner_namespace_id: &NamespaceId, content_id: &ContentId) -> Self {
        Self {
            owner_namespace_id: owner_namespace_id.clone(),
            content_id: content_id.clone(),
        }
    }

    pub(crate) fn of(content_ref: &ContentRef) -> Self {
        Self::new(&content_ref.owner_namespace_id, &content_ref.content_id)
    }
}

/// The bytes of every chain the server has stored.
#[derive(Debug, Default)]
pub(crate) struct ContentStore {
    chains: BTreeMap<ChainKey, Vec<u8>>,
}

impl ContentStore {
    pub(crate) fn put(&mut self, key: ChainKey, bytes: Vec<u8>) {
        self.chains.insert(key, bytes);
    }

    pub(crate) fn find_chain(&self, key: &ChainKey) -> Option<&[u8]> {
        self.chains.get(key).map(Vec::as_slice)
    }

    /// Returns the bytes that a committed content reference refers to.
    pub(crate) fn read(&self, content_ref: &ContentRef) -> &[u8] {
        prefix(self.find_chain(&ChainKey::of(content_ref)), content_ref)
    }
}

/// The chains that one commit writes, kept apart from the store until the commit succeeds.
#[derive(Debug, Default)]
pub(crate) struct ContentDraft {
    writes: BTreeMap<ChainKey, Vec<u8>>,
}

impl ContentDraft {
    pub(crate) fn write(&mut self, key: ChainKey, bytes: Vec<u8>) {
        self.writes.insert(key, bytes);
    }

    /// Returns the bytes of `content_ref`, from this draft or from the store.
    pub(crate) fn read<'a>(
        &'a self,
        store: &'a ContentStore,
        content_ref: &ContentRef,
    ) -> &'a [u8] {
        let key = ChainKey::of(content_ref);
        let chain = self
            .writes
            .get(&key)
            .map(Vec::as_slice)
            .or_else(|| store.find_chain(&key));
        prefix(chain, content_ref)
    }

    pub(crate) fn publish(self, store: &mut ContentStore) {
        store.chains.extend(self.writes);
    }
}

fn prefix<'a>(chain: Option<&'a [u8]>, content_ref: &ContentRef) -> &'a [u8] {
    let chain = chain.expect("a committed content reference should have a stored chain");
    let size = usize::try_from(content_ref.size_bytes).expect("a simulated chain fits in memory");
    assert!(
        chain.len() >= size,
        "chain `{}` has {} bytes, fewer than a reference to {size} bytes",
        content_ref.content_id,
        chain.len()
    );
    &chain[..size]
}
