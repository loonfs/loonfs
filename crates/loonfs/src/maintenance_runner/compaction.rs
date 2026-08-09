//! The streaming metadata compactions this process is running, and the
//! pressure that decides when one has to start.
//!
//! A maintenance step that plans one starts it here. The job is a background
//! task the runner owns — it takes no admission permit, because it is not a
//! bounded step and holding one for hours would starve every key behind it —
//! and it is registered for the same drain every other spawned task is, so a
//! shutdown cancels it and then waits for it.
//!
//! What this holds, per namespace, is two things. The one job running for it:
//! the plan, so the steps that follow know not to merge the group underneath
//! it, and the cancellation token, so a shutdown can stop it. And a count per
//! family group of the maintenance engagements that planned work for that
//! group while its bottom-anchored merge was blocked, which is the one thing
//! a single step cannot see: under sustained writes there is always another
//! pair of delta runs to merge, so a planner deciding from one step's view
//! would take that merge forever and never start the job that unfreezes the
//! group's base.
//!
//! Both are in-memory and single-writer, and both are safe to lose. A restart
//! forgets the running job, and a later step plans it again. A restart
//! forgets the counts, and the next two engagements rebuild them, which
//! delays one cycle and nothing else.

use super::RunnerInner;
use crate::{FsAdmin, NamespaceId};
use loonfs_api::wire::manifest::MetadataTableFamily;
use loonfs_core::{MetadataCompactionCancellation, MetadataCompactionSpec};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

/// The one job a namespace may have running.
pub(super) struct ActiveCompaction {
    spec: MetadataCompactionSpec,
    cancellation: MetadataCompactionCancellation,
}

/// What this process knows about one namespace's compactions.
#[derive(Default)]
pub(super) struct NamespaceCompactions {
    active: Option<ActiveCompaction>,
    /// Engagements that planned work for a family group while its
    /// bottom-anchored merge was blocked, since that group's last completed
    /// job. A group is listed here only while it is blocked, so the map holds
    /// the groups that are stuck and nothing else.
    blocked_engagements: BTreeMap<Vec<MetadataTableFamily>, u32>,
}

impl NamespaceCompactions {
    fn is_empty(&self) -> bool {
        self.active.is_none() && self.blocked_engagements.is_empty()
    }
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
    namespaces: Arc<Mutex<BTreeMap<NamespaceId, NamespaceCompactions>>>,
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

/// What one namespace's planner needs to know, in the shape
/// [`loonfs_core::MetadataCompactionView`] borrows from.
///
/// Owned rather than borrowed because the lock is released before the step
/// runs: a step reads durable state and publishes, which is far too long to
/// hold a map every other step consults. The default is what a namespace
/// nothing is running for reads as.
#[derive(Default)]
pub(crate) struct CompactionPressure {
    active: Option<MetadataCompactionSpec>,
    blocked_engagements: Vec<(Vec<MetadataTableFamily>, u32)>,
}

impl CompactionPressure {
    /// Borrows this as the view a maintenance step reads.
    pub(crate) fn view<'a>(
        &'a self,
        families: &'a [(&'a [MetadataTableFamily], u32)],
    ) -> loonfs_core::MetadataCompactionView<'a> {
        loonfs_core::MetadataCompactionView {
            active: self.active.as_ref(),
            blocked_engagements: families,
        }
    }

    /// The engagement counts as borrowed slices, which is what
    /// [`Self::view`] needs and what the caller has to keep alive around it.
    pub(crate) fn engagements(&self) -> Vec<(&[MetadataTableFamily], u32)> {
        self.blocked_engagements
            .iter()
            .map(|(families, engagements)| (families.as_slice(), *engagements))
            .collect()
    }
}

impl BackgroundCompactions {
    pub(super) fn new(
        namespaces: Arc<Mutex<BTreeMap<NamespaceId, NamespaceCompactions>>>,
        runner: &Arc<RunnerInner>,
    ) -> Self {
        Self {
            namespaces,
            runner: Arc::downgrade(runner),
        }
    }

    /// Everything a step's planner needs about this namespace: the job it
    /// must leave alone, and how long each group has been stuck.
    pub(crate) fn pressure(&self, namespace_id: &NamespaceId) -> CompactionPressure {
        let namespaces = self.lock();
        let Some(entry) = namespaces.get(namespace_id) else {
            return CompactionPressure::default();
        };
        CompactionPressure {
            active: entry.active.as_ref().map(|active| active.spec.clone()),
            blocked_engagements: entry
                .blocked_engagements
                .iter()
                .map(|(families, engagements)| (families.clone(), *engagements))
                .collect(),
        }
    }

    /// Records what one published merge unit said about its group.
    ///
    /// A merge that ran above a frozen base is one more engagement the group
    /// spent without its retention restarting; a merge that started at the
    /// group's oldest run means the base is not frozen, so the count goes.
    pub(crate) fn record_merge(
        &self,
        namespace_id: &NamespaceId,
        families: &[MetadataTableFamily],
        bottom_anchored_merge_blocked: bool,
    ) {
        let mut namespaces = self.lock();
        if !bottom_anchored_merge_blocked {
            let Some(entry) = namespaces.get_mut(namespace_id) else {
                return;
            };
            entry.blocked_engagements.remove(families);
            if entry.is_empty() {
                namespaces.remove(namespace_id);
            }
            return;
        }
        *namespaces
            .entry(namespace_id.clone())
            .or_default()
            .blocked_engagements
            .entry(families.to_vec())
            .or_default() += 1;
    }

    /// Clears a group's engagement count after a job rebuilt it.
    ///
    /// The base is no longer frozen, so the group starts counting again from
    /// nothing.
    pub(crate) fn clear_engagements(
        &self,
        namespace_id: &NamespaceId,
        families: &[MetadataTableFamily],
    ) {
        let mut namespaces = self.lock();
        let Some(entry) = namespaces.get_mut(namespace_id) else {
            return;
        };
        entry.blocked_engagements.remove(families);
        if entry.is_empty() {
            namespaces.remove(namespace_id);
        }
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
            let mut namespaces = self.lock();
            let entry = namespaces.entry(namespace_id.clone()).or_default();
            if entry.active.is_some() {
                return CompactionStart::AlreadyRunning;
            }
            entry.active = Some(ActiveCompaction {
                spec: spec.clone(),
                cancellation: cancellation.clone(),
            });
        }
        // The guard is built before the future so the future owns it from the
        // moment it exists. A spawn a shutdown refuses drops the future
        // without polling it, and that drop is what gives the slot back.
        let guard = CompactionSlot {
            namespaces: Arc::clone(&self.namespaces),
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
        for entry in self.lock().values() {
            if let Some(active) = &entry.active {
                active.cancellation.cancel();
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<NamespaceId, NamespaceCompactions>> {
        self.namespaces
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
    namespaces: Arc<Mutex<BTreeMap<NamespaceId, NamespaceCompactions>>>,
    namespace_id: NamespaceId,
}

impl Drop for CompactionSlot {
    fn drop(&mut self) {
        let Ok(mut namespaces) = self.namespaces.lock() else {
            return;
        };
        let Some(entry) = namespaces.get_mut(&self.namespace_id) else {
            return;
        };
        entry.active = None;
        if entry.is_empty() {
            namespaces.remove(&self.namespace_id);
        }
    }
}
