//! Completed upload sessions that garbage collection keeps between passes.

use crate::heap_bytes::{hash_map_table_bytes, HeapBytes};
use crate::manifest::CacheScope;
use loonfs_types::{ContentId, UploadId};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The two facts that garbage collection uses from a completed upload
/// session's record.
///
/// LoonFS refuses every update to a completed upload session record, so both
/// facts stay true for as long as the record exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompletedUploadSession {
    /// The content that the session uploaded, which a pass protects.
    pub(crate) content_id: ContentId,
    /// When the session was completed. The sweep deletes the record once the
    /// content reclamation grace has passed since this time.
    pub(crate) completed_at_ms: u64,
}

/// Completed upload sessions for every scope, under one byte budget.
///
/// When an insert takes the entries over the budget, every entry is
/// dropped. A dropped session costs one record read on a later pass.
pub struct SharedCompletedUploadSessions {
    max_bytes: usize,
    entries: Mutex<Entries>,
}

#[derive(Default)]
struct Entries {
    sessions: HashMap<(CacheScope, UploadId), CompletedUploadSession>,
    /// The heap bytes of the keys and values in `sessions`.
    heap_bytes: usize,
}

impl Entries {
    fn bytes(&self) -> usize {
        hash_map_table_bytes(&self.sessions) + self.heap_bytes
    }
}

impl std::fmt::Debug for SharedCompletedUploadSessions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedCompletedUploadSessions")
            .field("max_bytes", &self.max_bytes)
            .field("sessions", &self.lock().sessions.len())
            .finish()
    }
}

impl SharedCompletedUploadSessions {
    /// Creates an empty set of sessions. A budget of zero keeps none.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            entries: Mutex::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Entries> {
        self.entries
            .lock()
            .expect("completed upload session lock should not be poisoned")
    }
}

/// One runtime's view of shared completed upload sessions.
///
/// An entry is keyed by the runtime's scope and the upload ID, without the
/// namespace ID. An upload ID is 128 random bits, so it's unique across the
/// namespaces of a store, and a pass looks up only the IDs that it just
/// listed under one namespace's prefix. The scope keeps two runtimes that
/// share a metadata cache apart, as it does for their head state.
#[derive(Debug)]
pub struct CompletedUploadSessionCache {
    shared: Arc<SharedCompletedUploadSessions>,
    scope: CacheScope,
}

impl CompletedUploadSessionCache {
    pub fn new(shared: Arc<SharedCompletedUploadSessions>, scope: CacheScope) -> Self {
        Self { shared, scope }
    }

    /// A view over sessions no other view reads.
    #[cfg(any(test, feature = "test-support"))]
    pub fn unshared(max_bytes: usize) -> Self {
        Self::new(
            Arc::new(SharedCompletedUploadSessions::new(max_bytes)),
            CacheScope::new(0),
        )
    }

    /// Returns the completed session, if the cache keeps it.
    pub(crate) fn get(&self, upload_id: &UploadId) -> Option<CompletedUploadSession> {
        self.shared
            .lock()
            .sessions
            .get(&(self.scope, upload_id.clone()))
            .cloned()
    }

    /// Adds a completed session, or drops every entry when the addition
    /// takes the entries over the budget.
    pub(crate) fn insert(&self, upload_id: UploadId, session: CompletedUploadSession) {
        let shared = &*self.shared;
        if shared.max_bytes == 0 {
            return;
        }
        let mut entries = shared.lock();
        let added_bytes = upload_id.heap_bytes() + session.heap_bytes();
        match entries.sessions.entry((self.scope, upload_id)) {
            // The record can't change, so the entry already has these facts.
            Entry::Occupied(_) => return,
            Entry::Vacant(vacant) => {
                vacant.insert(session);
            }
        }
        entries.heap_bytes += added_bytes;
        if entries.bytes() > shared.max_bytes {
            *entries = Entries::default();
        }
    }

    /// Removes a session whose record the sweep deletes.
    pub(crate) fn invalidate(&self, upload_id: &UploadId) {
        let mut entries = self.shared.lock();
        if let Some(((_, upload_id), session)) = entries
            .sessions
            .remove_entry(&(self.scope, upload_id.clone()))
        {
            entries.heap_bytes -= upload_id.heap_bytes() + session.heap_bytes();
        }
    }

    /// Counts the sessions under this view's scope.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.shared
            .lock()
            .sessions
            .keys()
            .filter(|(scope, _)| *scope == self.scope)
            .count()
    }
}
