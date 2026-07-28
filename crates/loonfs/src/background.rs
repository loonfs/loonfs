//! Writer-scheduled background maintenance: the policy knob, the
//! per-namespace singleflight, and the task registry a shutdown drains.
//!
//! LoonFS never creates a hidden runtime for maintenance. Work a writer
//! schedules for itself is spawned on the writer's owning Tokio runtime and
//! stays visible to shutdown through the registry here. Only a writer owns
//! one of these: readers and admins schedule nothing.

use crate::{NamespaceId, Result, RuntimeError};
use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use tokio::task::JoinHandle;

/// Writer-initiated background maintenance policy.
///
/// The policy governs only maintenance a write-capable handle schedules for
/// itself after writes: non-destructive checkpoint steps that keep read cost
/// bounded once the WAL tail crosses its threshold. It never enables
/// retention advancement or garbage collection; those stay explicit
/// [`FsAdmin`](crate::FsAdmin) operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsBackgroundWork {
    /// The writer may schedule non-destructive maintenance after writes when
    /// thresholds are reached, spawned on the writer's owning runtime.
    Enabled,
    /// The writer never auto-schedules maintenance after writes. Explicit
    /// [`FsAdmin`](crate::FsAdmin) maintenance calls still work.
    ManualOnly,
}

/// Handle-owned background work: what may be scheduled, which runtime runs
/// it, and what shutdown must wait for.
pub(crate) struct BackgroundWork {
    policy: FsBackgroundWork,
    /// Runtime that owns scheduled tasks. Handle builders pin the runtime
    /// they were opened on; `None` resolves the runtime driving the
    /// triggering write at spawn time.
    runtime: Option<tokio::runtime::Handle>,
    /// Cap on concurrently claimed steps across all namespaces. The
    /// per-namespace singleflight bounds each namespace to one step; this
    /// bounds how many namespaces may step at once, so a write burst across
    /// many namespaces cannot fan out into unbounded maintenance tasks.
    max_concurrent: NonZeroUsize,
    state: Mutex<BackgroundState>,
}

/// One lock over the shutdown flag, the singleflight claims, and the task
/// registry. Registration checks the flag inside the same critical section,
/// so a shutdown that drained an empty registry can never race a spawn into
/// running unobserved (claimed before shutdown, registered after the drain).
struct BackgroundState {
    closed: bool,
    inflight: BTreeSet<NamespaceId>,
    /// Distinct dirty namespaces whose step request arrived while their own
    /// step was running or while the global concurrency cap was full. The
    /// set coalesces repeated requests and is bounded by the number of
    /// distinct dirty namespaces.
    pending: BTreeSet<NamespaceId>,
    tasks: Vec<JoinHandle<()>>,
}

impl BackgroundWork {
    pub(crate) fn new(
        policy: FsBackgroundWork,
        runtime: Option<tokio::runtime::Handle>,
        max_concurrent: NonZeroUsize,
    ) -> Self {
        Self {
            policy,
            runtime,
            max_concurrent,
            state: Mutex::new(BackgroundState {
                closed: false,
                inflight: BTreeSet::new(),
                pending: BTreeSet::new(),
                tasks: Vec::new(),
            }),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, BackgroundState> {
        self.state.lock().expect("background state lock poisoned")
    }

    /// Claims the namespace's singleflight slot.
    ///
    /// Returns true only when the caller must spawn a new task.
    pub(crate) fn try_claim(&self, namespace_id: &NamespaceId) -> bool {
        if self.policy != FsBackgroundWork::Enabled {
            return false;
        }
        let mut state = self.lock_state();
        if state.closed {
            return false;
        }
        if state.inflight.contains(namespace_id) {
            // The running step re-checks pending before releasing its slot,
            // so this request is deferred, not dropped.
            state.pending.insert(namespace_id.clone());
            return false;
        }
        if state.inflight.len() >= self.max_concurrent.get() {
            state.pending.insert(namespace_id.clone());
            tracing::debug!(
                namespace_id = %namespace_id,
                max_concurrent = self.max_concurrent.get(),
                "maintenance step deferred at the concurrency cap"
            );
            return false;
        }
        state.pending.remove(namespace_id);
        state.inflight.insert(namespace_id.clone())
    }

    /// Releases a namespace's singleflight slot.
    pub(crate) fn release(&self, namespace_id: &NamespaceId) {
        let mut state = self.lock_state();
        state.inflight.remove(namespace_id);
    }

    /// Ends one step run and returns the namespace the caller must run next.
    ///
    /// The finishing namespace's own deferred request wins first and keeps
    /// its slot. Otherwise the slot transfers to the first pending namespace
    /// that is not already running. The pending check, release, dequeue, and
    /// new claim share the one state lock, so no request can be lost between
    /// freeing the slot and choosing its successor.
    pub(crate) fn finish_or_rerun(&self, namespace_id: &NamespaceId) -> Option<NamespaceId> {
        let mut state = self.lock_state();
        if !state.closed && state.pending.remove(namespace_id) {
            return Some(namespace_id.clone());
        }
        state.pending.remove(namespace_id);
        state.inflight.remove(namespace_id);
        if state.closed {
            return None;
        }
        let next_namespace_id = state
            .pending
            .iter()
            .find(|candidate| !state.inflight.contains(*candidate))
            .cloned()?;
        state.pending.remove(&next_namespace_id);
        state.inflight.insert(next_namespace_id.clone());
        Some(next_namespace_id)
    }

    /// Rejects further scheduling and discards work still waiting for a
    /// concurrency slot. Already registered tasks remain visible to drain.
    pub(crate) fn shut_down(&self) {
        let mut state = self.lock_state();
        state.closed = true;
        state.pending.clear();
    }

    /// Spawns claimed work on the owning runtime and registers it for
    /// shutdown, or refuses after [`Self::shut_down`]. Dropping the future
    /// without running it must release its claim, so both refusals here
    /// clean up by dropping.
    pub(crate) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let handle = match &self.runtime {
            Some(handle) => handle.clone(),
            None => match tokio::runtime::Handle::try_current() {
                Ok(handle) => handle,
                Err(_) => return,
            },
        };
        let mut state = self.lock_state();
        if state.closed {
            // A shutdown between this work's claim and now must win: the
            // drain may already have observed an empty registry. Release the
            // lock before dropping the future — releasing its claim
            // re-enters this mutex.
            drop(state);
            drop(future);
            return;
        }
        state.tasks.retain(|task| !task.is_finished());
        state.tasks.push(handle.spawn(future));
    }

    /// Waits for every scheduled task to finish, surfacing panics. Loops
    /// because an in-flight write may schedule more work while an open
    /// handle waits, and because a finishing task registers the next queued
    /// namespace before it exits.
    pub(crate) async fn drain(&self) -> Result<()> {
        let mut panicked = 0usize;
        loop {
            let drained = std::mem::take(&mut self.lock_state().tasks);
            if drained.is_empty() {
                break;
            }
            for task in drained {
                if let Err(error) = task.await {
                    if error.is_panic() {
                        panicked += 1;
                    }
                }
            }
        }
        if panicked > 0 {
            return Err(RuntimeError::RuntimeTask(format!(
                "{panicked} background maintenance task(s) panicked"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // The drain test injects a task panic to assert it is surfaced.

    use super::*;
    use loonfs_test_support::ids::{namespace_id, nonzero_usize};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    fn concurrency_cap(value: usize) -> NonZeroUsize {
        nonzero_usize(value)
    }

    /// Sets its flag when dropped, mimicking the claim guard the auto-step
    /// future carries: a refused spawn must drop the future so the guard
    /// releases the singleflight slot.
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_request_during_an_active_step_defers_and_reruns_exactly_once() {
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None, concurrency_cap(8));
        let namespace_id = namespace_id("demo");

        assert!(background.try_claim(&namespace_id), "first claim spawns");
        assert!(
            !background.try_claim(&namespace_id),
            "a request during the active step defers instead of claiming"
        );
        assert_eq!(
            background.finish_or_rerun(&namespace_id),
            Some(namespace_id.clone()),
            "the deferred request keeps the slot held and reruns the step"
        );
        assert!(
            background.finish_or_rerun(&namespace_id).is_none(),
            "a quiet finish releases the slot"
        );
        assert!(
            background.try_claim(&namespace_id),
            "the released slot claims fresh for the next crossing"
        );
        background.release(&namespace_id);
    }

    #[test]
    fn shutdown_wins_over_a_deferred_rerun() {
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None, concurrency_cap(8));
        let namespace_id = namespace_id("demo");

        assert!(background.try_claim(&namespace_id));
        assert!(!background.try_claim(&namespace_id), "defers while active");
        background.shut_down();
        assert!(
            background.finish_or_rerun(&namespace_id).is_none(),
            "a deferred request must not rerun after shutdown closed admission"
        );
    }

    #[tokio::test]
    async fn spawn_after_shut_down_refuses_and_drops_the_future() {
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None, concurrency_cap(8));
        let namespace_id = namespace_id("demo");

        // The racy interleaving a shutdown must win: the claim lands first...
        assert!(background.try_claim(&namespace_id));
        // ...then a close (shut_down + drain) completes while the registry
        // is still empty...
        background.shut_down();
        background.drain().await.expect("nothing scheduled");

        // ...and only afterwards does the claimed work reach spawn. It must
        // be refused and dropped, never registered or run.
        let ran = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = DropFlag(dropped.clone());
        let ran_in_task = ran.clone();
        background.spawn(async move {
            let _guard = guard;
            ran_in_task.store(true, Ordering::SeqCst);
        });

        assert!(
            dropped.load(Ordering::SeqCst),
            "a refused future must be dropped so its claim guard releases"
        );
        assert!(
            !ran.load(Ordering::SeqCst),
            "a refused future must never run"
        );
        background.drain().await.expect("registry stays empty");
    }

    #[tokio::test]
    async fn tasks_spawned_before_shut_down_are_drained() {
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None, concurrency_cap(8));
        let ran = Arc::new(AtomicBool::new(false));
        let ran_in_task = ran.clone();
        background.spawn(async move {
            tokio::task::yield_now().await;
            ran_in_task.store(true, Ordering::SeqCst);
        });
        background.shut_down();
        background.drain().await.expect("drain scheduled task");
        assert!(
            ran.load(Ordering::SeqCst),
            "registered task ran to completion"
        );
    }

    #[tokio::test]
    async fn drain_surfaces_panicked_tasks_as_an_error() {
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None, concurrency_cap(8));
        background.spawn(async {
            panic!("injected background task panic");
        });
        background.shut_down();
        let error = background.drain().await.expect_err("panic must surface");
        assert!(
            error.to_string().contains("panicked"),
            "drain reports panicked tasks: {error}"
        );
    }

    #[tokio::test]
    async fn claims_are_singleflight_and_refused_after_shut_down() {
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None, concurrency_cap(8));
        let namespace_id = namespace_id("demo");
        assert!(background.try_claim(&namespace_id));
        assert!(
            !background.try_claim(&namespace_id),
            "one in-flight step per namespace"
        );
        background.release(&namespace_id);
        assert!(
            background.try_claim(&namespace_id),
            "a released slot is claimable again"
        );
        background.release(&namespace_id);
        background.shut_down();
        assert!(
            !background.try_claim(&namespace_id),
            "no new claims after shutdown"
        );
    }

    #[tokio::test]
    async fn manual_only_policy_never_claims() {
        let background =
            BackgroundWork::new(FsBackgroundWork::ManualOnly, None, concurrency_cap(8));
        assert!(!background.try_claim(&namespace_id("demo")));
    }

    #[tokio::test]
    async fn a_freed_slot_claims_the_next_namespace_at_the_global_cap() {
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None, concurrency_cap(2));
        let first = NamespaceId::parse("first").expect("valid namespace id");
        let second = NamespaceId::parse("second").expect("valid namespace id");
        let third = NamespaceId::parse("third").expect("valid namespace id");

        assert!(background.try_claim(&first));
        assert!(background.try_claim(&second));
        assert!(
            !background.try_claim(&third),
            "a third namespace must wait for a slot"
        );

        assert_eq!(
            background.finish_or_rerun(&first),
            Some(third.clone()),
            "the finishing path transfers its slot to the queued namespace"
        );
        background.release(&second);
        background.release(&third);
    }

    #[test]
    fn repeated_requests_for_one_queued_namespace_coalesce_to_one_run() {
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None, concurrency_cap(1));
        let active = namespace_id("active");
        let queued = namespace_id("queued");

        assert!(background.try_claim(&active));
        for _ in 0..8 {
            assert!(
                !background.try_claim(&queued),
                "the queued namespace cannot claim the occupied slot"
            );
        }
        assert_eq!(
            background.finish_or_rerun(&active),
            Some(queued.clone()),
            "one queued run consumes all coalesced requests"
        );
        assert!(
            background.finish_or_rerun(&queued).is_none(),
            "coalesced requests must not create a second run"
        );
    }

    #[test]
    fn shutdown_clears_namespaces_waiting_at_the_global_cap() {
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None, concurrency_cap(1));
        let active = namespace_id("active");
        let queued = namespace_id("queued");

        assert!(background.try_claim(&active));
        assert!(!background.try_claim(&queued));
        background.shut_down();
        assert!(background.finish_or_rerun(&active).is_none());
        assert!(
            !background.try_claim(&queued),
            "closed admission must not revive the cleared queue"
        );
    }
}
