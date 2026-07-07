//! Writer-scheduled background maintenance: the policy knob, the
//! per-namespace singleflight, and the task registry a shutdown drains.
//!
//! LoonFS never creates a hidden runtime for maintenance. Work a handle
//! schedules for itself is spawned on the handle's owning Tokio runtime and
//! stays visible to shutdown through the registry here.

use crate::{NamespaceId, Result, RuntimeError};
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
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
    closed: AtomicBool,
    inflight: Mutex<BTreeSet<NamespaceId>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl BackgroundWork {
    pub(crate) fn new(policy: FsBackgroundWork, runtime: Option<tokio::runtime::Handle>) -> Self {
        Self {
            policy,
            runtime,
            closed: AtomicBool::new(false),
            inflight: Mutex::new(BTreeSet::new()),
            tasks: Mutex::new(Vec::new()),
        }
    }

    /// Claims the namespace's singleflight slot. Returns false when the
    /// policy, a shutdown, or an already in-flight tick for the namespace
    /// forbids scheduling another.
    pub(crate) fn try_claim(&self, namespace_id: &NamespaceId) -> bool {
        if self.policy != FsBackgroundWork::Enabled || self.closed.load(Ordering::SeqCst) {
            return false;
        }
        self.inflight
            .lock()
            .expect("background inflight lock poisoned")
            .insert(namespace_id.clone())
    }

    /// Releases a namespace's singleflight slot.
    pub(crate) fn release(&self, namespace_id: &NamespaceId) {
        self.inflight
            .lock()
            .expect("background inflight lock poisoned")
            .remove(namespace_id);
    }

    /// Spawns claimed work on the owning runtime and registers it for
    /// shutdown. Dropping the future without running it must release its
    /// claim, so an unresolvable runtime cleans up by dropping.
    pub(crate) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let handle = match &self.runtime {
            Some(handle) => handle.clone(),
            None => match tokio::runtime::Handle::try_current() {
                Ok(handle) => handle,
                Err(_) => return,
            },
        };
        let mut tasks = self
            .tasks
            .lock()
            .expect("background task registry poisoned");
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle.spawn(future));
    }

    /// Waits for every scheduled task to finish, surfacing panics. Loops
    /// because an in-flight write may schedule more work while an open
    /// handle waits.
    pub(crate) async fn drain(&self) -> Result<()> {
        let mut panicked = 0usize;
        loop {
            let drained = {
                let mut tasks = self
                    .tasks
                    .lock()
                    .expect("background task registry poisoned");
                std::mem::take(&mut *tasks)
            };
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
