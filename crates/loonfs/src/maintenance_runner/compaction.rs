//! The streaming metadata compactions this process is running.
//!
//! A maintenance step that plans one starts it here. The job is a background
//! task the runner owns — it takes no admission permit, because it is not a
//! bounded step and holding one for hours would starve every key behind it —
//! and it is registered for the same drain every other spawned task is, so a
//! shutdown cancels it and then waits for it.
//!
//! What this holds is a map from namespace to the one job running for it: the
//! plan, so the steps that follow know not to merge the group underneath it,
//! and the cancellation token, so a shutdown can stop it. There is no
//! lifecycle beyond present and absent. The entry goes when the job ends,
//! however it ends, because the guard that holds it is dropped with the task.

use super::RunnerInner;
use crate::{FsAdmin, NamespaceId};
use loonfs_core::{MetadataCompactionCancellation, MetadataCompactionSpec};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

/// The one job a namespace may have running.
pub(super) struct ActiveCompaction {
    spec: MetadataCompactionSpec,
    cancellation: MetadataCompactionCancellation,
}

/// A handle on this process's running compactions, cloned into every handle
/// whose maintenance steps may start one.
///
/// The map is held strongly and the runner weakly, which is the same shape
/// [`super::MaintenanceHandle`] has and for the same reason: a step may
/// consult the map at any time, and a step that outlives the writer that owns
/// the runner starts nothing.
#[derive(Clone)]
pub(crate) struct BackgroundCompactions {
    active: Arc<Mutex<BTreeMap<NamespaceId, ActiveCompaction>>>,
    runner: Weak<RunnerInner>,
}

/// What a step's plan met when it tried to start a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionStart {
    /// The job is running now.
    Started,
    /// A job is already running for this namespace, so this plan was not
    /// started. One job at a time per namespace is a process-level fact; a
    /// later step plans this group again.
    AlreadyRunning,
    /// The writer that owned the runner is gone, so there is nothing left to
    /// spawn on. Indistinguishable, to the step, from a handle that never had
    /// background work at all.
    NoRunner,
}

impl BackgroundCompactions {
    pub(super) fn new(
        active: Arc<Mutex<BTreeMap<NamespaceId, ActiveCompaction>>>,
        runner: &Arc<RunnerInner>,
    ) -> Self {
        Self {
            active,
            runner: Arc::downgrade(runner),
        }
    }

    /// The plan of the job running for `namespace_id`, for the step that has
    /// to leave its group alone.
    pub(crate) fn active_spec(&self, namespace_id: &NamespaceId) -> Option<MetadataCompactionSpec> {
        self.lock()
            .get(namespace_id)
            .map(|active| active.spec.clone())
    }

    /// Starts `spec` as a background job under `admin`'s identity, unless a
    /// job already holds the namespace's one slot.
    pub(crate) fn start(
        &self,
        admin: &FsAdmin,
        namespace_id: &NamespaceId,
        spec: MetadataCompactionSpec,
    ) -> CompactionStart {
        let Some(runner) = self.runner.upgrade() else {
            return CompactionStart::NoRunner;
        };
        let cancellation = MetadataCompactionCancellation::default();
        {
            let mut active = self.lock();
            if active.contains_key(namespace_id) {
                return CompactionStart::AlreadyRunning;
            }
            active.insert(
                namespace_id.clone(),
                ActiveCompaction {
                    spec: spec.clone(),
                    cancellation: cancellation.clone(),
                },
            );
        }
        // The guard is built before the future so the future owns it from the
        // moment it exists. A spawn a shutdown refuses drops the future
        // without polling it, and that drop is what gives the slot back.
        let guard = CompactionSlot {
            active: Arc::clone(&self.active),
            namespace_id: namespace_id.clone(),
        };
        let admin = admin.clone();
        let namespace_id = namespace_id.clone();
        runner.spawn(async move {
            let _slot = guard;
            admin
                .run_streaming_compaction(&namespace_id, &spec, &cancellation)
                .await;
        });
        CompactionStart::Started
    }

    /// Cancels every running job. The tasks themselves are joined by the
    /// runner's drain; this is what makes that wait short.
    pub(super) fn cancel_all(&self) {
        for active in self.lock().values() {
            active.cancellation.cancel();
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<NamespaceId, ActiveCompaction>> {
        self.active
            .lock()
            .expect("background compaction lock poisoned")
    }
}

/// Holds a namespace's compaction slot and gives it back exactly once.
///
/// Dropping is the only way the slot is released — on a normal finish, on a
/// panic, and on a task discarded with its runtime — so no namespace is ever
/// left claiming a job that is not running.
struct CompactionSlot {
    active: Arc<Mutex<BTreeMap<NamespaceId, ActiveCompaction>>>,
    namespace_id: NamespaceId,
}

impl Drop for CompactionSlot {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.namespace_id);
        }
    }
}
