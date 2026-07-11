//! Writer-scheduled background maintenance: the policy knob, the
//! per-namespace singleflight, and the task registry a shutdown drains.
//!
//! LoonFS never creates a hidden runtime for maintenance. Work a handle
//! schedules for itself is spawned on the handle's owning Tokio runtime and
//! stays visible to shutdown through the registry here.

use crate::{NamespaceId, Result, RuntimeError};
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Mutex;
use tokio::task::JoinHandle;

/// Writer-initiated background maintenance policy.
///
/// The policy governs only maintenance a write-capable handle schedules for
/// itself after writes: non-destructive checkpoint ticks that keep read cost
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
    state: Mutex<BackgroundState>,
}

/// One lock over the shutdown flag, the singleflight claims, and the task
/// registry. Registration checks the flag inside the same critical section,
/// so a shutdown that drained an empty registry can never race a spawn into
/// running unobserved (claimed before shutdown, registered after the drain).
struct BackgroundState {
    closed: bool,
    inflight: BTreeSet<NamespaceId>,
    tasks: Vec<JoinHandle<()>>,
}

impl BackgroundWork {
    pub(crate) fn new(policy: FsBackgroundWork, runtime: Option<tokio::runtime::Handle>) -> Self {
        Self {
            policy,
            runtime,
            state: Mutex::new(BackgroundState {
                closed: false,
                inflight: BTreeSet::new(),
                tasks: Vec::new(),
            }),
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, BackgroundState> {
        self.state.lock().expect("background state lock poisoned")
    }

    /// Claims the namespace's singleflight slot. Returns false when the
    /// policy, a shutdown, or an already in-flight tick for the namespace
    /// forbids scheduling another.
    pub(crate) fn try_claim(&self, namespace_id: &NamespaceId) -> bool {
        if self.policy != FsBackgroundWork::Enabled {
            return false;
        }
        let mut state = self.lock_state();
        !state.closed && state.inflight.insert(namespace_id.clone())
    }

    /// Releases a namespace's singleflight slot.
    pub(crate) fn release(&self, namespace_id: &NamespaceId) {
        self.lock_state().inflight.remove(namespace_id);
    }

    /// Rejects any further background scheduling.
    pub(crate) fn shut_down(&self) {
        self.lock_state().closed = true;
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
    /// handle waits.
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    fn namespace_id() -> NamespaceId {
        NamespaceId::parse("demo").expect("valid namespace id")
    }

    /// Sets its flag when dropped, mimicking the claim guard the auto-tick
    /// future carries: a refused spawn must drop the future so the guard
    /// releases the singleflight slot.
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn spawn_after_shut_down_refuses_and_drops_the_future() {
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None);
        let namespace_id = namespace_id();

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
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None);
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
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None);
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
        let background = BackgroundWork::new(FsBackgroundWork::Enabled, None);
        let namespace_id = namespace_id();
        assert!(background.try_claim(&namespace_id));
        assert!(
            !background.try_claim(&namespace_id),
            "one in-flight tick per namespace"
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
        let background = BackgroundWork::new(FsBackgroundWork::ManualOnly, None);
        assert!(!background.try_claim(&namespace_id()));
    }
}
