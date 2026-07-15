//! Session-owned writer state, kept apart from every rebuildable cache.
//!
//! A writer session's acquired epoch and terminal fencing record are facts
//! about this process, not cached views of durable state: nothing in the
//! store can rebuild "this session was fenced". When they lived inside the
//! LRU control cache, eviction erased them (and cache-disabled runs never
//! kept them at all), so a fenced writer could silently bump the epoch back
//! and fence the legitimate writer instead. This registry holds one entry
//! per namespace the session has published to; entries are a few dozen
//! bytes, and they are removed only when the namespace itself is deleted,
//! because a deleted namespace id never rebinds.

use crate::NamespaceId;
use loonfs_core::publish::SharedWriterSessionState;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

/// One runtime core's writer-session states, keyed by namespace.
#[derive(Debug, Default)]
pub(crate) struct WriterSessionRegistry {
    namespaces: Mutex<HashMap<NamespaceId, SharedWriterSessionState>>,
}

impl WriterSessionRegistry {
    /// The session state for one namespace, created on first use. Every
    /// engine built for the namespace shares the returned handle, so a
    /// fenced session stays fenced across engine rebuilds.
    pub(crate) fn state(&self, namespace_id: &NamespaceId) -> SharedWriterSessionState {
        self.lock().entry(namespace_id.clone()).or_default().clone()
    }

    /// Namespace-terminal removal: the namespace is gone and its id never
    /// rebinds, so the session state goes with it.
    pub(crate) fn remove(&self, namespace_id: &NamespaceId) {
        self.lock().remove(namespace_id);
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<NamespaceId, SharedWriterSessionState>> {
        // Poisoning is propagated as a panic, matching the runtime's cache
        // locks: every critical section is a plain map operation.
        self.namespaces
            .lock()
            .expect("writer session registry lock poisoned")
    }
}
