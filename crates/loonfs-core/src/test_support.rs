//! Test doubles for this crate's public seams.

use crate::cache::{
    StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Mutex;

/// One call made against a [`RecordingStoredMetadataBlockCache`], in the
/// form that says what the caller asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedStoredMetadataBlockCall {
    /// A lookup, with whether it was answered from the map.
    Get {
        key: StoredMetadataBlockKey,
        hit: bool,
    },
    /// An insert, with the length of the bytes offered.
    Insert {
        key: StoredMetadataBlockKey,
        bytes: usize,
    },
    /// An invalidation.
    Invalidate { key: StoredMetadataBlockKey },
}

impl RecordedStoredMetadataBlockCall {
    /// The key the call addressed.
    pub fn key(&self) -> &StoredMetadataBlockKey {
        match self {
            Self::Get { key, .. } | Self::Insert { key, .. } | Self::Invalidate { key } => key,
        }
    }
}

/// A stored-block cache that serves from an in-memory map and records every
/// call in call order.
///
/// Closing flips the cache inert, as the trait requires: later calls are
/// neither served nor recorded.
#[derive(Debug, Default)]
pub struct RecordingStoredMetadataBlockCache {
    state: Mutex<RecordingState>,
}

#[derive(Debug, Default)]
struct RecordingState {
    entries: HashMap<StoredMetadataBlockKey, Bytes>,
    calls: Vec<RecordedStoredMetadataBlockCall>,
    closed: bool,
}

impl RecordingStoredMetadataBlockCache {
    /// An empty, open cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every call the cache accepted, in call order.
    pub fn calls(&self) -> Vec<RecordedStoredMetadataBlockCall> {
        self.state().calls.clone()
    }

    /// How many calls the cache accepted.
    pub fn call_count(&self) -> usize {
        self.state().calls.len()
    }

    /// How many entries the cache is holding.
    pub fn len(&self) -> usize {
        self.state().entries.len()
    }

    /// Whether the cache is holding nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether [`StoredMetadataBlockCache::close`] has been called.
    pub fn is_closed(&self) -> bool {
        self.state().closed
    }

    /// Replaces what is held for `key` with the same number of bytes that no
    /// longer checksum, the way a lost write or a bad sector leaves an
    /// entry. Does nothing for a key the cache is not holding.
    ///
    /// The replacement is not recorded: it is the test arranging the world,
    /// not a call the code under test made.
    pub fn corrupt(&self, key: &StoredMetadataBlockKey) {
        let mut state = self.state();
        if let Some(held) = state.entries.get_mut(key) {
            *held = held.iter().map(|byte| !byte).collect::<Vec<_>>().into();
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, RecordingState> {
        // Poisoning is propagated as a panic: a poisoned lock means a test
        // thread panicked mid-update, and the recording is no longer a
        // trustworthy account of what the code under test did.
        self.state
            .lock()
            .expect("recording stored-block cache lock should not be poisoned")
    }
}

#[async_trait]
impl StoredMetadataBlockCache for RecordingStoredMetadataBlockCache {
    async fn get(&self, key: &StoredMetadataBlockKey) -> Option<Bytes> {
        let mut state = self.state();
        if state.closed {
            return None;
        }
        let bytes = state.entries.get(key).cloned();
        state.calls.push(RecordedStoredMetadataBlockCall::Get {
            key: key.clone(),
            hit: bytes.is_some(),
        });
        bytes
    }

    fn insert(&self, key: StoredMetadataBlockKey, bytes: Bytes) {
        let mut state = self.state();
        if state.closed {
            return;
        }
        state.calls.push(RecordedStoredMetadataBlockCall::Insert {
            key: key.clone(),
            bytes: bytes.len(),
        });
        state.entries.insert(key, bytes);
    }

    fn invalidate(&self, key: &StoredMetadataBlockKey) {
        let mut state = self.state();
        if state.closed {
            return;
        }
        state
            .calls
            .push(RecordedStoredMetadataBlockCall::Invalidate { key: key.clone() });
        state.entries.remove(key);
    }

    async fn close(&self) -> Result<(), StoredMetadataBlockCacheCloseError> {
        self.state().closed = true;
        Ok(())
    }
}
