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
//! Failure semantics follow the batch they join: if any member's content
//! durability gate fails — including a submitter cancelled mid-window, which
//! abandons its content write — the flush aborts before the head CAS and
//! every member is rejected. Each member's error says what happened to it:
//! the member whose own content write failed gets that write's error, and
//! the members whose writes succeeded get a distinct error saying a write
//! batched with theirs failed, nothing was committed, and a retry is safe.
//! An opener cancelled before flushing closes the window and fails the
//! members that joined it.

use crate::publish::NamespaceMutationCandidate;
use crate::{CommitResponse, CoreError, NamespaceId, Result, RuntimeError};
use futures::channel::oneshot;
use loonfs_core::publish::ContentDurabilityGate;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::time::Duration;

/// Error the combined gate hands the batch when any member's content write
/// fails. Core stamps it on every aborted member; distribution rewrites it
/// per member, so callers never see this text — each gets its own write's
/// error or [`WINDOW_PEER_FAILURE_MESSAGE`].
const WINDOW_GATE_FAILURE_MESSAGE: &str = "a content write in this commit window failed";

/// Error a member receives when its own content write succeeded but the
/// flush aborted because another member's failed.
const WINDOW_PEER_FAILURE_MESSAGE: &str =
    "a write batched with this one in its commit window failed; nothing was committed; retry";

#[allow(clippy::disallowed_methods)]
pub(crate) async fn commit_window_delay(window: Duration) {
    // The commit window intentionally waits a short async timer so
    // concurrent direct publishes can join before one flush.
    tokio::time::sleep(window).await;
}

type WindowResults = Vec<Result<CommitResponse>>;

/// One buffered submission: its candidates, the durability gate covering any
/// content it is writing concurrently, and the channel its results return on.
pub(crate) struct WindowEntry {
    pub(crate) candidates: Vec<NamespaceMutationCandidate>,
    pub(crate) content_gate: Option<ContentDurabilityGate>,
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
        content_gate: Option<ContentDurabilityGate>,
    ) -> WindowRole<'_> {
        let (result_sender, result_receiver) = oneshot::channel();
        let entry = WindowEntry {
            candidates,
            content_gate,
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

/// Content-write failures the flush's combined gate observed, one slot per
/// window member. Filled before the gate resolves, so the opener may read
/// them once the publish returns.
pub(crate) struct WindowGateFailures {
    slots: Arc<Mutex<Vec<Option<CoreError>>>>,
}

impl WindowGateFailures {
    pub(crate) fn take(&self) -> Vec<Option<CoreError>> {
        // Poisoning is propagated as a panic, matching the window map: a
        // half-updated failure record could misattribute members' errors.
        std::mem::take(
            &mut *self
                .slots
                .lock()
                .expect("window gate failure slots lock poisoned"),
        )
    }
}

/// Combines the members' durability gates into the flush's single gate: the
/// batch's head CAS waits until every member's concurrent content write has
/// acked, and fails — aborting the whole batch — if any of them fail. Each
/// failure is recorded against its member (no fail-fast), so distribution
/// can tell the failing member its own error and the rest that a batched
/// peer failed. A solo member keeps its raw gate: its own error already
/// reaches the right caller, and core traces keep the real failure.
pub(crate) fn combine_member_content_gates(
    member_gates: Vec<(usize, ContentDurabilityGate)>,
    member_count: usize,
) -> (Option<ContentDurabilityGate>, WindowGateFailures) {
    let failures = WindowGateFailures {
        slots: Arc::new(Mutex::new(vec![None; member_count])),
    };
    let mut member_gates = member_gates;
    if member_gates.is_empty() || member_count == 1 {
        return (member_gates.pop().map(|(_, gate)| gate), failures);
    }
    let slots = Arc::clone(&failures.slots);
    let gate: ContentDurabilityGate = Box::pin(async move {
        let results = futures::future::join_all(
            member_gates
                .into_iter()
                .map(|(member_index, gate)| async move { (member_index, gate.await) }),
        )
        .await;
        let mut any_failed = false;
        {
            let mut slots = slots
                .lock()
                .expect("window gate failure slots lock poisoned");
            for (member_index, result) in results {
                if let Err(error) = result {
                    any_failed = true;
                    if let Some(slot) = slots.get_mut(member_index) {
                        *slot = Some(error);
                    }
                }
            }
        }
        if any_failed {
            Err(CoreError::Internal(WINDOW_GATE_FAILURE_MESSAGE.to_owned()))
        } else {
            Ok(())
        }
    });
    (Some(gate), failures)
}

/// Rewrites one member's share of an aborted flush: results stamped with the
/// combined gate's error become the member's own content-write error, or the
/// batched-peer error when this member's write succeeded. Every other result
/// — successes, and errors the member earned itself during validation — is
/// left untouched.
pub(crate) fn attribute_window_gate_failure(
    results: WindowResults,
    own_failure: Option<&CoreError>,
) -> WindowResults {
    results
        .into_iter()
        .map(|result| match result {
            Err(RuntimeError::Core(CoreError::Internal(message)))
                if message == WINDOW_GATE_FAILURE_MESSAGE =>
            {
                Err(RuntimeError::Core(match own_failure {
                    Some(own_failure) => own_failure.clone(),
                    None => CoreError::Internal(WINDOW_PEER_FAILURE_MESSAGE.to_owned()),
                }))
            }
            other => other,
        })
        .collect()
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
