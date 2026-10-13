//! Test helpers and recording implementations.

/// Shares the read request bound with tests outside this crate.
pub const STORE_READS_IN_FLIGHT: usize = crate::in_flight::STORE_READS_IN_FLIGHT;

/// A standard monotonic timer, for a test whose deadlines don't matter.
pub fn std_timer() -> std::sync::Arc<dyn loonfs_types::MonotonicTimer> {
    std::sync::Arc::new(loonfs_types::StdMonotonicTimer::default())
}

/// An observation of a standard monotonic timer's current time.
pub fn observed_now() -> crate::time::Observation {
    crate::time::Observation::now(std_timer())
}

/// Compares an opaque token with its golden fixture in `tests/golden/tokens`.
///
/// `UPDATE_GOLDEN=1` rewrites the fixture first.
#[cfg(test)]
pub(crate) fn assert_matches_golden_token(name: &str, actual: &str) {
    let path = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/tokens"))
        .join(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().expect("fixture parent dir"))
            .expect("create golden dir");
        std::fs::write(&path, actual).expect("write golden fixture");
    }
    assert!(
        path.exists(),
        "golden token `{name}` is missing; run with UPDATE_GOLDEN=1 to write it"
    );
    let expected = std::fs::read_to_string(&path).expect("read golden token");
    assert_eq!(expected, actual, "golden token `{name}` changed");
}

/// Builds the context a test mutation runs under: `writer_id` at `now_ms`.
pub fn mutation_context(writer_id: &str, now_ms: u64) -> crate::MutationContext {
    crate::MutationContext {
        writer_id: loonfs_types::WriterId::parse(writer_id).expect("a test writer id should parse"),
        now_ms,
    }
}

// These path-mutation helpers use dev-dependencies and are only needed by
// in-crate checkpoint and planner tests.
#[cfg(test)]
mod content_write;
#[cfg(test)]
pub(crate) mod ops;

use crate::cache::{
    StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
};
use crate::time::{Deadline, StdMonotonicTimer};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

/// Appends one small commit per WAL object without folding the tail.
#[cfg(any(test, feature = "test-support"))]
pub async fn append_wal_objects<S: loonfs_objectstore::ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &loonfs_types::NamespaceId,
    count: u64,
    context: &crate::MutationContext,
) -> crate::error::Result<()> {
    let mut engine = crate::publish::NamespaceCommitEngine::new(
        namespace_id.clone(),
        std::sync::Arc::new(tokio::sync::Semaphore::new(32)),
    );
    for _ in 0..count {
        let commit_id = loonfs_types::CommitId::generate();
        let path = loonfs_types::AbsolutePath::parse(format!("/wal-tail-{commit_id}"))
            .expect("a generated WAL-tail test path should parse");
        let request = crate::publish::CommitIntent::single(
            commit_id,
            loonfs_types::ActorId::loonfs(),
            None,
            crate::publish::FilesystemOperation::CreateDirectory {
                path,
                parents: false,
            },
        );
        let mut results = engine
            .publish_batch(
                store,
                vec![crate::publish::CommitCandidate::new(request)],
                context,
                &Deadline::start(Arc::new(StdMonotonicTimer::default())),
            )
            .await
            .results;
        assert_eq!(
            results.len(),
            1,
            "a single-candidate test publish should return one result"
        );
        results
            .pop()
            .expect("a single-candidate test publish should hold one result")?;
    }
    Ok(())
}

/// A call recorded by [`RecordingStoredMetadataBlockCache`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedStoredMetadataBlockCall {
    /// A lookup and whether it hit the cache.
    Get {
        key: StoredMetadataBlockKey,
        hit: bool,
    },
    /// An insert and its byte length.
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

    /// Corrupts an existing entry without recording a cache operation.
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
