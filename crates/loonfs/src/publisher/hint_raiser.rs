//! Raises each namespace's hint after this runtime's manifest publications,
//! with at most one raise running for each namespace.

use super::{lock_state, TaskExit};
use crate::SharedObjectStore;
use loonfs_core::control::{raise_namespace_hint, LoadedHint};
use loonfs_objectstore::timing::MonotonicTimer;
use loonfs_types::{ManifestNo, NamespaceId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use tokio::sync::watch;

/// Raises each namespace's hint to the newest manifest that this runtime
/// published, after the publication has returned (format spec, "The current
/// manifest and WAL tip").
///
/// The store paces compare-and-swap writes to one object (see
/// [`MIN_CAS_WRITE_INTERVAL_MS`](loonfs_objectstore::MIN_CAS_WRITE_INTERVAL_MS)),
/// so a raise can wait for up to that interval, and the publication doesn't
/// wait for the raise. Each namespace has at most one raise task. A
/// publication made while the task runs moves the task's target to the newer
/// manifest, so the task's next raise covers every publication made while its
/// last raise waited.
pub(crate) struct HintRaiser {
    store: SharedObjectStore,
    timer: Arc<dyn MonotonicTimer>,
    runtime: Handle,
    /// The namespaces that have a raise task. A task removes its namespace's
    /// entry when it exits, so the map lists the namespaces with a raise
    /// pending, plus any namespace whose task panicked, until that
    /// namespace's next publication replaces the entry.
    pending: Arc<Mutex<HashMap<NamespaceId, PendingRaise>>>,
}

/// The target and exit state of one namespace's raise task.
struct PendingRaise {
    /// The newest manifest that this runtime published in the namespace.
    target: ManifestNo,
    /// Becomes `true` when the task exits, even when it panics.
    exited: watch::Receiver<bool>,
}

impl HintRaiser {
    pub(crate) fn new(
        store: SharedObjectStore,
        timer: Arc<dyn MonotonicTimer>,
        runtime: Handle,
    ) -> Self {
        Self {
            store,
            timer,
            runtime,
            pending: Arc::default(),
        }
    }

    /// Moves the namespace's raise target to at least `manifest_no`, a
    /// manifest that this runtime published, and starts the namespace's raise
    /// task unless one is running.
    pub(crate) fn raise_to(&self, namespace_id: &NamespaceId, manifest_no: ManifestNo) {
        let exit = {
            let mut pending = lock_state(&self.pending);
            if let Some(raise) = pending
                .get_mut(namespace_id)
                .filter(|raise| !*raise.exited.borrow())
            {
                raise.target = raise.target.max(manifest_no);
                return;
            }
            let (exit, exited) = watch::channel(false);
            pending.insert(
                namespace_id.clone(),
                PendingRaise {
                    target: manifest_no,
                    exited,
                },
            );
            exit
        };
        let task = RaiseTask {
            store: Arc::clone(&self.store),
            timer: Arc::clone(&self.timer),
            pending: Arc::clone(&self.pending),
            namespace_id: namespace_id.clone(),
        };
        self.runtime.spawn(task.run(TaskExit(exit)));
    }

    /// Waits until every raise task that's running now has exited.
    ///
    /// A task whose target moves during the wait raises the hint to the new
    /// target before it exits.
    pub(crate) async fn wait_for_raises(&self) {
        let running: Vec<_> = lock_state(&self.pending)
            .values()
            .map(|raise| raise.exited.clone())
            .collect();
        for mut exited in running {
            // A closed channel means the task has exited.
            let _ = exited.wait_for(|exited| *exited).await;
        }
    }
}

/// One namespace's raise task.
struct RaiseTask {
    store: SharedObjectStore,
    timer: Arc<dyn MonotonicTimer>,
    pending: Arc<Mutex<HashMap<NamespaceId, PendingRaise>>>,
    namespace_id: NamespaceId,
}

impl RaiseTask {
    /// Raises the hint to the namespace's target until a raise reaches the
    /// target, and then removes the namespace's entry and exits.
    ///
    /// Another pass runs only when a publication moved the target during the
    /// last pass, so the task makes at most one pass for each publication.
    /// Each pass after the first starts from the hint that the last pass
    /// wrote or loaded, so the pass sends no read unless another process
    /// raised the hint meanwhile. A failed raise ends the task, and the next
    /// publication in this runtime starts a new task. Until then, the hint
    /// lags, and readers still find the current manifest because discovery
    /// probes forward from the hint.
    async fn run(self, exit: TaskExit) {
        let _exit = exit;
        let mut known: Option<LoadedHint> = None;
        loop {
            let target = self.target();
            let raised = raise_namespace_hint(
                self.store.as_ref(),
                &self.namespace_id,
                target,
                known.take(),
                Arc::clone(&self.timer),
            )
            .await;
            let mut pending = lock_state(&self.pending);
            match raised {
                Ok(raised) if self.target_in(&pending) > target => known = Some(raised),
                Ok(_) => {
                    pending.remove(&self.namespace_id);
                    return;
                }
                Err(error) => {
                    pending.remove(&self.namespace_id);
                    drop(pending);
                    tracing::warn!(
                        namespace_id = %self.namespace_id,
                        manifest_no = %target,
                        error = %error,
                        "the namespace hint raise failed, and the runtime raises the hint again after the next publication"
                    );
                    return;
                }
            }
        }
    }

    fn target(&self) -> ManifestNo {
        self.target_in(&lock_state(&self.pending))
    }

    fn target_in(&self, pending: &HashMap<NamespaceId, PendingRaise>) -> ManifestNo {
        pending
            .get(&self.namespace_id)
            .expect("a running raise task keeps its namespace's entry")
            .target
    }
}
