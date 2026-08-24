//! Tracks background metadata compactions.
//!
//! Each namespace may have one queued or running compaction. A process-wide
//! semaphore limits total concurrency. Jobs do not consume bounded-step
//! permits and are cancelled and joined during writer shutdown.
//!
//! The in-memory state also counts delta merges over each frozen base so the
//! planner eventually requests a full compaction. Losing this state only
//! delays replanning after restart.

use super::{MaintenanceJobId, RunnerInner};
use crate::{FsAdmin, NamespaceId};
use loonfs_core::{
    FrozenBasePolicy, MetadataCompactionCancellation, MetadataCompactionJobOutcome,
    MetadataCompactionSpec, MetadataFamilyGroup,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Maximum concurrent streaming compactions in one process.
pub(crate) const MAX_CONCURRENT_COMPACTIONS: usize = 2;

/// The one job a namespace may have, from the moment it claims the slot to
/// the moment it ends — including the time it spends waiting for a permit.
struct ActiveCompaction {
    spec: MetadataCompactionSpec,
    cancellation: MetadataCompactionCancellation,
}

/// What this process knows about one namespace's compactions.
#[derive(Default)]
pub(super) struct NamespaceCompactions {
    active: Option<ActiveCompaction>,
    /// Delta merges that have published over a family group's frozen base,
    /// since that group's last completed job. A group is listed here only
    /// while it is blocked, so the map holds the groups that are stuck and
    /// nothing else.
    published_delta_merges_over_frozen_base: BTreeMap<MetadataFamilyGroup, u32>,
}

impl NamespaceCompactions {
    fn is_empty(&self) -> bool {
        self.active.is_none() && self.published_delta_merges_over_frozen_base.is_empty()
    }
}

/// Shared access to this process's background compactions.
#[derive(Clone)]
pub(crate) struct BackgroundCompactions {
    namespaces: Arc<Mutex<BTreeMap<NamespaceId, NamespaceCompactions>>>,
    permits: Arc<Semaphore>,
    runner: Weak<RunnerInner>,
}

/// What a step's plan met when it tried to start a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionStart {
    /// The job is running now.
    Started,
    /// The job holds the namespace's slot and is waiting for a process
    /// permit. It starts when one frees, and its group is already left alone.
    Queued,
    /// A job is already running or queued for this namespace, so this plan
    /// was not started. One job at a time per namespace is a process-level
    /// fact; a later step plans this group again.
    AlreadyRunning,
    /// The writer that owned the runner is gone, so there is nothing left to
    /// spawn on. Indistinguishable, to the step, from a handle that never had
    /// background work at all.
    NoRunner,
}

impl BackgroundCompactions {
    pub(super) fn new(runner: &Arc<RunnerInner>) -> Self {
        Self {
            namespaces: Arc::clone(&runner.compactions),
            permits: Arc::clone(&runner.compaction_permits),
            runner: Arc::downgrade(runner),
        }
    }

    /// The plan of the job this namespace has running or queued, which is the
    /// group every step must leave alone.
    ///
    /// Cloned rather than borrowed because the lock is released before the
    /// step runs: a step reads durable state and publishes, which is far too
    /// long to hold a map every other step consults.
    pub(crate) fn active_spec(&self, namespace_id: &NamespaceId) -> Option<MetadataCompactionSpec> {
        self.lock()
            .get(namespace_id)?
            .active
            .as_ref()
            .map(|active| active.spec.clone())
    }

    /// The amortization this process has watched the namespace's stuck groups
    /// spend, as the policy a planner runs under.
    pub(crate) fn amortization(&self, namespace_id: &NamespaceId) -> FrozenBasePolicy {
        FrozenBasePolicy::Amortized {
            published_delta_merges_over_frozen_base: self
                .lock()
                .get(namespace_id)
                .map(|entry| entry.published_delta_merges_over_frozen_base.clone())
                .unwrap_or_default(),
        }
    }

    /// Records what one published merge unit said about its group.
    ///
    /// A merge that ran above a frozen base is one more delta merge the group
    /// published without its retention restarting; a merge that started at the
    /// group's oldest run means the base is not frozen, so the count goes.
    pub(crate) fn record_merge(
        &self,
        namespace_id: &NamespaceId,
        group: MetadataFamilyGroup,
        bottom_anchored_merge_blocked: bool,
    ) {
        if !bottom_anchored_merge_blocked {
            self.clear_published_delta_merges(namespace_id, group);
            return;
        }
        *self
            .lock()
            .entry(namespace_id.clone())
            .or_default()
            .published_delta_merges_over_frozen_base
            .entry(group)
            .or_default() += 1;
    }

    /// Clears a group's published-delta-merge count after a job rebuilt it.
    ///
    /// The base is no longer frozen, so the group starts counting again from
    /// nothing.
    pub(crate) fn clear_published_delta_merges(
        &self,
        namespace_id: &NamespaceId,
        group: MetadataFamilyGroup,
    ) {
        let mut namespaces = self.lock();
        let Some(entry) = namespaces.get_mut(namespace_id) else {
            return;
        };
        entry.published_delta_merges_over_frozen_base.remove(&group);
        if entry.is_empty() {
            namespaces.remove(namespace_id);
        }
    }

    /// Claims the namespace's compaction slot for `spec`, or `None` when a
    /// job already holds it.
    ///
    /// The claim carries a process permit when one was free. When none was,
    /// the claim is queued and [`CompactionClaim::admitted`] is what waits
    /// for one. Either way the plan is in the map from here on, so the next
    /// step leaves this group alone.
    pub(crate) fn claim(
        &self,
        namespace_id: &NamespaceId,
        spec: &MetadataCompactionSpec,
    ) -> Option<CompactionClaim> {
        let cancellation = MetadataCompactionCancellation::default();
        {
            let mut namespaces = self.lock();
            let entry = namespaces.entry(namespace_id.clone()).or_default();
            if entry.active.is_some() {
                return None;
            }
            entry.active = Some(ActiveCompaction {
                spec: spec.clone(),
                cancellation: cancellation.clone(),
            });
        }
        let claim = CompactionClaim {
            // Taken without waiting: whether a permit was free is what tells
            // the step that planned this job apart from a step that queued
            // one, and a claim that waited here would hold the step.
            permit: Arc::clone(&self.permits).try_acquire_owned().ok(),
            slot: CompactionSlot {
                namespaces: Arc::clone(&self.namespaces),
                permits: Arc::clone(&self.permits),
                runner: Weak::clone(&self.runner),
                namespace_id: namespace_id.clone(),
            },
            cancellation,
        };
        claim.slot.report_counts();
        Some(claim)
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
        let Some(mut claim) = self.claim(namespace_id, &spec) else {
            return CompactionStart::AlreadyRunning;
        };
        let queued = claim.is_queued();
        // The claim is moved into the future so the future owns it from the
        // moment it exists. A spawn a shutdown refuses drops the future
        // without polling it, and that drop is what gives the slot and the
        // permit back.
        let admin = admin.clone();
        let namespace_id = namespace_id.clone();
        runner.spawn(async move {
            if !claim.admitted().await {
                admin.core.instruments().compaction_not_admitted();
                return;
            }
            // Every ending is logged inside, including the error one, so what
            // a task nobody awaits needs from it is only what to do next.
            let outcome = admin
                .run_streaming_compaction(&namespace_id, &spec, claim.cancellation())
                .await;
            let published = matches!(outcome, Ok(MetadataCompactionJobOutcome::Published { .. }));
            claim.finished(published);
        });
        if queued {
            CompactionStart::Queued
        } else {
            CompactionStart::Started
        }
    }

    /// Cancels every job this process holds a slot for, running or queued.
    /// The tasks themselves are joined by the runner's drain; this is what
    /// makes that wait short.
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
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// One job's claim on a namespace's compaction slot and on a process permit.
pub(crate) struct CompactionClaim {
    /// Declared before the slot, so it is dropped before the slot is: the
    /// counts the slot reports on its way out then already exclude this job.
    permit: Option<OwnedSemaphorePermit>,
    slot: CompactionSlot,
    cancellation: MetadataCompactionCancellation,
}

impl CompactionClaim {
    /// Whether this claim is waiting for a process permit rather than holding
    /// one.
    pub(crate) fn is_queued(&self) -> bool {
        self.permit.is_none()
    }

    /// The token that stops this job, whether it is queued or running.
    pub(crate) fn cancellation(&self) -> &MetadataCompactionCancellation {
        &self.cancellation
    }

    /// Releases the claim and reschedules metadata maintenance.
    ///
    /// Successful publication reschedules immediately. Other outcomes wait
    /// one reconciliation interval before replanning. The claim is released
    /// before scheduling so the next step can inspect the group.
    pub(crate) fn finished(self, published: bool) {
        let runner = Weak::clone(&self.slot.runner);
        let namespace_id = self.slot.namespace_id.clone();
        drop(self);
        let Some(runner) = runner.upgrade() else {
            return;
        };
        let not_before_ms = (!published).then(|| {
            runner
                .clock
                .now_ms()
                .saturating_add(super::RECONCILE_INTERVAL_MS)
        });
        super::nudge_key(
            &runner,
            MaintenanceJobId::METADATA,
            &namespace_id,
            not_before_ms,
        );
    }

    /// Resolves once this job may run. `false` means it was cancelled while
    /// it waited and must not run at all.
    ///
    /// Waiting is what makes a queued job free: it holds its namespace slot,
    /// so its group is left alone, and it holds nothing else until a permit
    /// frees.
    pub(crate) async fn admitted(&mut self) -> bool {
        if self.permit.is_none() {
            let permits = Arc::clone(&self.slot.permits);
            let cancellation = self.cancellation.clone();
            self.permit = tokio::select! {
                permit = permits.acquire_owned() => permit.ok(),
                () = cancellation.cancelled() => None,
            };
            self.slot.report_counts();
        }
        self.permit.is_some() && !self.cancellation.is_cancelled()
    }
}

/// Releases a namespace's compaction slot when the job ends or is dropped.
struct CompactionSlot {
    namespaces: Arc<Mutex<BTreeMap<NamespaceId, NamespaceCompactions>>>,
    permits: Arc<Semaphore>,
    runner: Weak<RunnerInner>,
    namespace_id: NamespaceId,
}

impl CompactionSlot {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<NamespaceId, NamespaceCompactions>> {
        self.namespaces
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Updates the running and waiting compaction metrics.
    fn report_counts(&self) {
        let Some(runner) = self.runner.upgrade() else {
            return;
        };
        let claimed = self
            .lock()
            .values()
            .filter(|entry| entry.active.is_some())
            .count();
        let running = MAX_CONCURRENT_COMPACTIONS.saturating_sub(self.permits.available_permits());
        runner
            .instruments
            .compactions(running, claimed.saturating_sub(running));
    }
}

impl Drop for CompactionSlot {
    fn drop(&mut self) {
        {
            let mut namespaces = self.lock();
            let Some(entry) = namespaces.get_mut(&self.namespace_id) else {
                return;
            };
            entry.active = None;
            if entry.is_empty() {
                namespaces.remove(&self.namespace_id);
            }
        }
        self.report_counts();
    }
}
