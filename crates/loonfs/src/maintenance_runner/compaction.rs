//! The streaming metadata compactions this process is running, and the
//! pressure that decides when one has to start.
//!
//! A maintenance step that plans one starts it here. The job is a background
//! task the runner owns — it takes no admission permit, because it is not a
//! bounded step and holding one for hours would starve every key behind it —
//! and it is registered for the same drain every other spawned task is, so a
//! shutdown cancels it and then waits for it.
//!
//! Two things admit a job, and they answer different questions. A namespace
//! slot stops two jobs rebuilding one namespace at once, which would waste
//! one of them at finalization. A process-wide semaphore stops this process
//! running one job per namespace it serves: each job holds a probe cache,
//! decoded blocks for its iterators, and a segment builder per family, so the
//! aggregate is what has to be capped whatever the per-namespace rule says. A
//! job claims its slot first and then waits for a permit, and it is queued
//! rather than refused while it waits.
//!
//! What this holds, per namespace, is two things. The one job claiming it:
//! the plan, so the steps that follow know not to merge the group underneath
//! it, and the cancellation token, so a shutdown can stop it. The plan is
//! recorded when the slot is claimed rather than when the job starts reading,
//! so a queued job's group is left alone exactly as a running job's is — and
//! because it is left alone, no merge is published for it, so the count below
//! neither climbs nor resets while the job waits. And a count per family
//! group of the maintenance engagements that planned work for that group
//! while its bottom-anchored merge was blocked, which is the one thing a
//! single step cannot see: under sustained writes there is always another
//! pair of delta runs to merge, so a planner deciding from one step's view
//! would take that merge forever and never start the job that unfreezes the
//! group's base.
//!
//! Both are in-memory and single-writer, and both are safe to lose. A restart
//! forgets the running job, and a later step plans it again. A restart
//! forgets the counts, and the next two engagements rebuild them, which
//! delays one cycle and nothing else.

use super::{MaintenanceJobId, RunnerInner};
use crate::{FsAdmin, NamespaceId};
use loonfs_api::wire::manifest::MetadataTableFamily;
use loonfs_core::{
    MetadataCompactionCancellation, MetadataCompactionJobOutcome, MetadataCompactionSpec,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Streaming compactions this process runs at once, across every namespace it
/// serves.
///
/// Small and fixed. A job is unpaced by design and holds real memory while it
/// runs, so the useful question is how many of them a process may hold at
/// once, and two is enough to keep one namespace's job from stalling every
/// other namespace while still bounding the total. It is deliberately not a
/// configuration knob: an operator's lever over maintenance is how often it
/// runs, and a second concurrency number would only let a deployment raise
/// this one until its memory answered for it.
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
/// The map and the permits are held strongly and the runner weakly, which is
/// the same shape [`super::MaintenanceHandle`] has and for the same reason: a
/// step may consult the map at any time, and a step that outlives the writer
/// that owns the runner starts nothing.
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
    pub(super) fn new(runner: &Arc<RunnerInner>) -> Self {
        Self {
            namespaces: Arc::clone(&runner.compactions),
            permits: Arc::clone(&runner.compaction_permits),
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
                return;
            }
            // Every ending is logged inside, including the error one, so what
            // a task nobody awaits needs from it is only what to do next.
            let published = matches!(
                admin
                    .run_streaming_compaction(&namespace_id, &spec, claim.cancellation())
                    .await,
                Ok(MetadataCompactionJobOutcome::Published { .. })
            );
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
            .expect("background compaction lock poisoned")
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

    /// Gives the claim back and puts the namespace's metadata maintenance
    /// back in the queue, which is what a job ending is: the moment the
    /// condition that blocked that maintenance changed.
    ///
    /// A publication means the group is rebuilt and whatever arrived while
    /// the job ran can fold now, so the key is nudged at once. Any other
    /// ending means the job is to be planned again, and immediately would
    /// only start the same long job again, so the key waits one
    /// reconciliation interval. The sweep remains the net under both: these
    /// nudges are hints like every other, and a lost one costs the delay to
    /// the next sweep.
    ///
    /// The claim goes back before the nudge, and that order is the whole
    /// reason this is one call. A step that reached the planner while the
    /// slot was still held would leave alone the very group the job just
    /// rebuilt, and then park.
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

/// Holds a namespace's compaction slot and gives it back exactly once.
///
/// Dropping is the only way the slot is released — on a normal finish, on a
/// panic, and on a task discarded with its runtime — so no namespace is ever
/// left claiming a job that is not running.
struct CompactionSlot {
    namespaces: Arc<Mutex<BTreeMap<NamespaceId, NamespaceCompactions>>>,
    permits: Arc<Semaphore>,
    runner: Weak<RunnerInner>,
    namespace_id: NamespaceId,
}

impl CompactionSlot {
    /// Reports how many jobs are running and how many are waiting for a
    /// permit, from the two facts that decide it: the slots claimed, and the
    /// permits taken.
    fn report_counts(&self) {
        let Some(runner) = self.runner.upgrade() else {
            return;
        };
        let claimed = self
            .namespaces
            .lock()
            .map(|namespaces| {
                namespaces
                    .values()
                    .filter(|entry| entry.active.is_some())
                    .count()
            })
            .unwrap_or(0);
        let running = MAX_CONCURRENT_COMPACTIONS.saturating_sub(self.permits.available_permits());
        runner
            .instruments
            .compactions(running, claimed.saturating_sub(running));
    }
}

impl Drop for CompactionSlot {
    fn drop(&mut self) {
        {
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
        self.report_counts();
    }
}
