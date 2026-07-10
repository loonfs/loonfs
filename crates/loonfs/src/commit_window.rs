//! Per-namespace commit windows for direct publishes.
//!
//! A direct publish — a path intent or explicit commit submitted through a
//! handle — holds its namespace's window open briefly so concurrent
//! publishes can join, then the opener flushes every buffered submission as
//! one batch: one WAL segment, one head compare-and-swap. Every submitter
//! still awaits its own durable, visible result; the window trades a short
//! wait for shared publication cost, never for weaker semantics.
//!
//! The opener drives the flush inside its own future, so the window needs no
//! spawned task and works on any runtime that drives the callers. The
//! server's batching publisher has its own coalescer and does not pass
//! through here.
//!
//! Submissions buffer metadata only — content a submission references is
//! already durable before it enters the window — so members share the
//! flush's publication fate (a failed WAL write or head CAS rejects the
//! batch) but never each other's uploads. An opener cancelled before
//! flushing closes the window and fails the members that joined it.

use crate::publish::NamespaceMutationCandidate;
use crate::{CommitResponse, CoreError, NamespaceId, Result, RuntimeError};
use futures::channel::oneshot;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use tokio::time::Duration;

#[allow(clippy::disallowed_methods)]
pub(crate) async fn commit_window_delay(window: Duration) {
    // The commit window intentionally waits a short async timer so
    // concurrent direct publishes can join before one flush.
    tokio::time::sleep(window).await;
}

type WindowResults = Vec<Result<CommitResponse>>;

/// One buffered submission: its candidates and the channel its results
/// return on.
pub(crate) struct WindowEntry {
    pub(crate) candidates: Vec<NamespaceMutationCandidate>,
    pub(crate) result_sender: oneshot::Sender<WindowResults>,
}

/// The open commit windows of one runtime core, keyed by namespace.
#[derive(Default)]
pub(crate) struct CommitWindows {
    open: Mutex<HashMap<NamespaceId, Vec<WindowEntry>>>,
}

/// What a submission became when it entered the window.
pub(crate) enum WindowRole<'a> {
    /// First submission for the namespace: the caller now owns the window
    /// and must wait out the delay, then flush the drained entries —
    /// including its own — and distribute results.
    Opener {
        guard: OpenerGuard<'a>,
        result_receiver: oneshot::Receiver<WindowResults>,
    },
    /// Joined an already-open window; the opener publishes on this
    /// submission's behalf and the results arrive on the receiver.
    Joiner(oneshot::Receiver<WindowResults>),
}

impl CommitWindows {
    /// Buffers a submission into the namespace's window, opening one if none
    /// is open.
    pub(crate) fn enter(
        &self,
        namespace_id: &NamespaceId,
        candidates: Vec<NamespaceMutationCandidate>,
    ) -> WindowRole<'_> {
        let (result_sender, result_receiver) = oneshot::channel();
        let entry = WindowEntry {
            candidates,
            result_sender,
        };
        let mut open = self.lock_open();
        if let Some(entries) = open.get_mut(namespace_id) {
            entries.push(entry);
            return WindowRole::Joiner(result_receiver);
        }
        open.insert(namespace_id.clone(), vec![entry]);
        WindowRole::Opener {
            guard: OpenerGuard {
                windows: self,
                namespace_id: namespace_id.clone(),
                armed: true,
            },
            result_receiver,
        }
    }

    fn lock_open(&self) -> MutexGuard<'_, HashMap<NamespaceId, Vec<WindowEntry>>> {
        // Poisoning is propagated as a panic: a poisoned window map means a
        // thread panicked mid-update, and reusing it could wedge or misroute
        // buffered publishes.
        self.open.lock().expect("commit window map lock poisoned")
    }
}

/// Closes the opener's window exactly once: normally by draining it for the
/// flush, or on drop when the opener was cancelled first — dropping the
/// buffered entries then cancels the joiners' receivers, so an abandoned
/// window fails its members instead of wedging the namespace.
pub(crate) struct OpenerGuard<'a> {
    windows: &'a CommitWindows,
    namespace_id: NamespaceId,
    armed: bool,
}

impl OpenerGuard<'_> {
    /// Closes the window and returns its buffered entries in submission
    /// order, the opener's own entry first.
    pub(crate) fn take_entries(mut self) -> Vec<WindowEntry> {
        self.armed = false;
        self.windows
            .lock_open()
            .remove(&self.namespace_id)
            .unwrap_or_default()
    }
}

impl Drop for OpenerGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.windows.lock_open().remove(&self.namespace_id);
        }
    }
}

/// Pads a distributed result slice when the publish returned fewer results
/// than the entry submitted candidates; the batch contract makes this
/// unreachable, but a routing bug must fail the submitter, not hang it.
pub(crate) fn pad_missing_results(mut results: WindowResults, expected: usize) -> WindowResults {
    while results.len() < expected {
        results.push(Err(RuntimeError::Core(CoreError::Internal(
            "commit window flush returned fewer results than candidates".to_owned(),
        ))));
    }
    results
}
