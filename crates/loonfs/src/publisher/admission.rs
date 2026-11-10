//! Admission of one runtime's publication requests: its per-namespace limits,
//! and the totals of the execution budget it shares.

use super::queue::PreparedCandidate;
use crate::execution_budget::{AdmissionChange, AdmissionCharge, BudgetPermit};
use crate::publish::CommitCandidate;
use crate::CoreError;
use crate::{ExecutionBudget, PublicationPolicy};
use loonfs_core::{check_planned_bytes, MergeAllowance, MergeReservation};
use loonfs_types::format::wal::{MAX_WAL_OBJECT_BYTES, WAL_OBJECT_OVERHEAD_BYTES};
use loonfs_types::NamespaceId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::oneshot;

/// The estimated peak memory of publishing a merge, as a multiple of the
/// merge's planned WAL bytes. Publication builds the commit's deltas and
/// encodes its WAL object while the merge's operations are still in memory.
/// In measurements of merges of file creates, the peak was 1.8 times the
/// planned bytes with 13-byte names, and 3.1 times with 255-byte names.
const MERGE_PEAK_PER_PLANNED_BYTE: usize = 3;

/// The bytes of the merge allowance that a merge reserves before it plans,
/// or the whole allowance when the allowance is smaller. At the default
/// allowance of 512 MiB, at most 64 merges plan at once.
pub(super) const MIN_MERGE_RESERVED_BYTES: usize = 8 * 1024 * 1024;

/// Admits one runtime's publication requests against its per-namespace
/// limits and the totals of its execution budget.
///
/// The per-namespace usage stays in the runtime because a namespace id names
/// a namespace only within one store, and runtimes over other stores may
/// share the budget. Lock order: this runtime's usage, a permit's charge,
/// then the budget's totals. No lock stays locked across an await.
pub(super) struct PublicationAdmission {
    limits: PublicationPolicy,
    namespaces: Mutex<HashMap<NamespaceId, RequestWeight>>,
    budget: ExecutionBudget,
}

#[derive(Default)]
struct RequestWeight {
    requests: usize,
    estimated_bytes: usize,
    inline_bytes: usize,
}

impl PublicationAdmission {
    pub(super) fn new(limits: PublicationPolicy, budget: ExecutionBudget) -> Self {
        Self {
            limits,
            namespaces: Mutex::default(),
            budget,
        }
    }

    fn lock_namespaces(&self) -> MutexGuard<'_, HashMap<NamespaceId, RequestWeight>> {
        self.namespaces
            .lock()
            .expect("the namespace admission lock should not be poisoned")
    }

    #[cfg(test)]
    pub(super) fn used_requests(&self) -> usize {
        self.lock_namespaces()
            .values()
            .map(|namespace| namespace.requests)
            .sum()
    }

    #[cfg(test)]
    pub(super) fn namespace_usage(&self, namespace_id: &NamespaceId) -> (usize, usize, usize) {
        self.lock_namespaces()
            .get(namespace_id)
            .map(|usage| (usage.requests, usage.estimated_bytes, usage.inline_bytes))
            .unwrap_or_default()
    }

    pub(super) async fn publication_permit(&self) -> BudgetPermit<'_> {
        self.budget.publication_permit().await
    }

    pub(super) fn write_stop_wait_budget_ms(&self) -> u64 {
        self.limits.write_stop_wait_budget_ms
    }

    pub(super) fn acquire_candidate(
        self: &Arc<Self>,
        namespace_id: &NamespaceId,
        candidate: &PreparedCandidate,
    ) -> Result<Arc<AdmissionPermit>, CoreError> {
        if candidate.candidate.is_merge() {
            let permit = self.reserve_merge(namespace_id)?;
            Self::validate_merge(
                &candidate.candidate,
                candidate.wal_record_bytes_upper_bound,
                &permit,
            )?;
            return Ok(permit);
        }
        Self::validate_candidate(candidate)?;
        let estimated_bytes =
            estimated_admission_bytes(namespace_id, candidate.estimated_retained_bytes);
        self.charge(namespace_id, AdmissionCharge::Request { estimated_bytes })
    }

    /// Reserves [`MIN_MERGE_RESERVED_BYTES`] of the merge allowance, or the
    /// whole allowance when it's smaller, for a merge into `namespace_id`.
    /// When the other merges' reservations leave no room, the merge gets
    /// `server_busy`. The reservation lasts until the permit is dropped.
    pub(super) fn reserve_merge(
        self: &Arc<Self>,
        namespace_id: &NamespaceId,
    ) -> Result<Arc<AdmissionPermit>, CoreError> {
        let reserved_bytes = MIN_MERGE_RESERVED_BYTES.min(self.budget.max_merge_bytes());
        self.charge(namespace_id, AdmissionCharge::Merge { reserved_bytes })
    }

    /// Refuses a merge whose planned WAL bytes, `planned_bytes`, wouldn't fit
    /// one WAL object, or whose estimated peak exceeds the merge allowance,
    /// with `merge_too_large`, and grows the reservation that `permit` keeps
    /// to the estimate. The estimate is [`MERGE_PEAK_PER_PLANNED_BYTE`] times
    /// the planned bytes. A merge that's too large would be refused on every
    /// retry, so its caller merges a smaller range of the fork's commits
    /// instead.
    pub(super) fn validate_merge(
        candidate: &CommitCandidate,
        planned_bytes: usize,
        permit: &Arc<AdmissionPermit>,
    ) -> Result<(), CoreError> {
        candidate.validate_appended_content()?;
        check_planned_bytes(planned_bytes, &permit.merge_allowance())
    }

    pub(super) fn validate_candidate(candidate: &PreparedCandidate) -> Result<(), CoreError> {
        Self::validate_wal_object(&candidate.candidate, candidate.wal_record_bytes_upper_bound)
    }

    /// Refuses a commit whose WAL record, bounded by `wal_record_bytes`,
    /// wouldn't fit one WAL object.
    fn validate_wal_object(
        candidate: &CommitCandidate,
        wal_record_bytes: usize,
    ) -> Result<(), CoreError> {
        candidate.validate_appended_content()?;
        let document_bytes = wal_record_bytes.saturating_add(WAL_OBJECT_OVERHEAD_BYTES);
        if document_bytes > MAX_WAL_OBJECT_BYTES {
            return Err(candidate.too_large_error(document_bytes, MAX_WAL_OBJECT_BYTES));
        }
        Ok(())
    }

    pub(super) fn acquire(
        self: &Arc<Self>,
        namespace_id: &NamespaceId,
        estimated_request_bytes: usize,
    ) -> Result<Arc<AdmissionPermit>, CoreError> {
        self.charge(
            namespace_id,
            AdmissionCharge::Request {
                estimated_bytes: estimated_admission_bytes(namespace_id, estimated_request_bytes),
            },
        )
    }

    /// Charges one request to its namespace's limits and to the budget's
    /// totals. A merge is charged to its namespace's request count and to
    /// the merge allowance, and to none of the byte limits.
    fn charge(
        self: &Arc<Self>,
        namespace_id: &NamespaceId,
        charge: AdmissionCharge,
    ) -> Result<Arc<AdmissionPermit>, CoreError> {
        let estimated_bytes = charge.estimated_bytes();
        let change = {
            let mut namespaces = self.lock_namespaces();
            let empty = RequestWeight::default();
            let namespace = namespaces.get(namespace_id).unwrap_or(&empty);
            if namespace.requests >= self.limits.max_requests_per_namespace.get()
                || estimated_bytes
                    > self
                        .limits
                        .max_estimated_bytes_per_namespace
                        .get()
                        .saturating_sub(namespace.estimated_bytes)
            {
                return Err(CoreError::CommitQueueFull);
            }
            let change = self.budget.charge_admission(charge);
            if matches!(change, AdmissionChange::Admitted(_)) {
                let namespace = namespaces.entry(namespace_id.clone()).or_default();
                namespace.requests += 1;
                namespace.estimated_bytes += estimated_bytes;
            }
            change
        };
        // The recorder is caller code, so it runs after both locks are released.
        self.budget.report_admission(change);
        match change {
            AdmissionChange::Admitted(_) => {}
            AdmissionChange::Rejected => return Err(CoreError::CommitQueueFull),
            AdmissionChange::MergeAllowanceInUse => return Err(CoreError::ServerBusy),
        }
        Ok(Arc::new(AdmissionPermit {
            admission: Arc::clone(self),
            namespace_id: namespace_id.clone(),
            charge: Mutex::new(charge),
            inline_bytes: AtomicUsize::new(0),
        }))
    }
}

/// Returns the bytes that admission charges for a request whose own
/// estimate is `estimated_request_bytes`.
pub(super) fn estimated_admission_bytes(
    namespace_id: &NamespaceId,
    estimated_request_bytes: usize,
) -> usize {
    // Include the channel, ID copies, fingerprint and queue bookkeeping.
    // Deletes include only fixed-size options. Candidate bytes include any
    // proof or rejection data retained while waiting to publish.
    estimated_request_bytes
        .saturating_add(512)
        .saturating_add(namespace_id.as_str().len())
}

pub(super) struct AdmissionPermit {
    admission: Arc<PublicationAdmission>,
    namespace_id: NamespaceId,
    /// What the request is charged, to its namespace's limits and to the
    /// budget's totals alike. Only [`Self::reweigh`] changes the charge, under
    /// the admission's namespace lock.
    charge: Mutex<AdmissionCharge>,
    inline_bytes: AtomicUsize,
}

impl AdmissionPermit {
    /// Moves a request's charge to the admission bytes of
    /// `estimated_request_bytes`, the weight of the candidate that the queue
    /// retains once its inline values are staged. A growth past the
    /// namespace's byte limit or the budget's byte total fails with
    /// `commit_queue_full` and leaves the charge as it was, for the permit's
    /// drop to refund. A merge's charge stays as it is.
    pub(super) fn reweigh(&self, estimated_request_bytes: usize) -> Result<(), CoreError> {
        let estimated_bytes =
            estimated_admission_bytes(&self.namespace_id, estimated_request_bytes);
        let change = {
            let mut namespaces = self.admission.lock_namespaces();
            let mut charge = self
                .charge
                .lock()
                .expect("the admission charge lock should not be poisoned");
            let AdmissionCharge::Request {
                estimated_bytes: charged_bytes,
            } = *charge
            else {
                return Ok(());
            };
            let namespace = namespaces
                .get_mut(&self.namespace_id)
                .expect("admitted namespace should have usage");
            let others = namespace.estimated_bytes - charged_bytes;
            if estimated_bytes
                > self
                    .admission
                    .limits
                    .max_estimated_bytes_per_namespace
                    .get()
                    .saturating_sub(others)
            {
                return Err(CoreError::CommitQueueFull);
            }
            let change = self
                .admission
                .budget
                .reweigh_admission(charged_bytes, estimated_bytes);
            if matches!(change, AdmissionChange::Admitted(_)) {
                namespace.estimated_bytes = others + estimated_bytes;
                *charge = AdmissionCharge::Request { estimated_bytes };
            }
            change
        };
        self.admission.budget.report_admission(change);
        if !matches!(change, AdmissionChange::Admitted(_)) {
            return Err(CoreError::CommitQueueFull);
        }
        Ok(())
    }

    /// Returns the merge allowance, with the reservation of the merge that
    /// this permit admits, which the merge planner and admission check.
    pub(super) fn merge_allowance(self: &Arc<Self>) -> MergeAllowance {
        MergeAllowance {
            max_bytes: self.admission.budget.max_merge_bytes(),
            peak_per_planned_byte: MERGE_PEAK_PER_PLANNED_BYTE,
            reservation: Arc::clone(self) as Arc<dyn MergeReservation>,
        }
    }

    pub(super) fn reserve_inline(
        &self,
        sizes: impl Iterator<Item = usize>,
        unfolded_bytes: usize,
        limit: usize,
    ) -> usize {
        let mut namespaces = self.admission.lock_namespaces();
        let namespace = namespaces
            .get_mut(&self.namespace_id)
            .expect("admitted namespace should have usage");
        let mut remaining = limit
            .saturating_sub(unfolded_bytes)
            .saturating_sub(namespace.inline_bytes);
        let mut kept = 0;
        let mut reserved = 0;
        for size in sizes {
            if size > remaining {
                break;
            }
            remaining -= size;
            reserved += size;
            kept += 1;
        }
        namespace.inline_bytes += reserved;
        self.inline_bytes.store(reserved, Ordering::Relaxed);
        kept
    }

    pub(super) fn release_inline(&self) {
        let mut namespaces = self.admission.lock_namespaces();
        let bytes = self.inline_bytes.swap(0, Ordering::Relaxed);
        if let Some(namespace) = namespaces.get_mut(&self.namespace_id) {
            namespace.inline_bytes -= bytes;
        }
    }
}

impl MergeReservation for AdmissionPermit {
    fn grow(&self, estimated_bytes: usize) -> bool {
        let change = {
            let mut charge = self
                .charge
                .lock()
                .expect("the admission charge lock should not be poisoned");
            assert!(
                matches!(*charge, AdmissionCharge::Merge { .. }),
                "only a merge's permit should reserve the merge allowance"
            );
            let reserved_bytes = charge.reserved_merge_bytes();
            if estimated_bytes <= reserved_bytes {
                return true;
            }
            let change = self
                .admission
                .budget
                .grow_merge_reservation(reserved_bytes, estimated_bytes);
            if matches!(change, AdmissionChange::Admitted(_)) {
                *charge = AdmissionCharge::Merge {
                    reserved_bytes: estimated_bytes,
                };
            }
            change
        };
        self.admission.budget.report_admission(change);
        matches!(change, AdmissionChange::Admitted(_))
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let charge = *self
            .charge
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        let change = {
            // A drop can run while a panic unwinds, and a second panic then
            // aborts the process, so this recovers a poisoned lock instead of
            // panicking. The critical section only updates counts.
            let mut namespaces = self
                .admission
                .namespaces
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(namespace) = namespaces.get_mut(&self.namespace_id) {
                namespace.requests -= 1;
                namespace.estimated_bytes -= charge.estimated_bytes();
                namespace.inline_bytes -= self.inline_bytes.load(Ordering::Relaxed);
                if namespace.requests == 0 {
                    namespaces.remove(&self.namespace_id);
                }
            }
            // Refunded under this runtime's lock, as it was charged, so this
            // runtime never sees its namespace refunded while the total is not.
            self.admission.budget.refund_admission(charge)
        };
        self.admission.budget.report_admission(change);
    }
}

/// The worker retains admission until delivery, even if the receiver is gone.
/// A contending caller also keeps this permit while retaining its candidate;
/// retries share it instead of competing for a second admission slot.
pub(super) struct AdmittedWaiter<T> {
    sender: oneshot::Sender<T>,
    pub(super) permit: Arc<AdmissionPermit>,
}

impl<T> AdmittedWaiter<T> {
    pub(super) fn new(sender: oneshot::Sender<T>, permit: &Arc<AdmissionPermit>) -> Self {
        Self {
            sender,
            permit: Arc::clone(permit),
        }
    }

    pub(super) fn send(self, value: T) -> Result<(), T> {
        self.sender.send(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{DefaultMetricsRecorder, MetricValue};
    use crate::publish::CommitCandidate;
    use crate::ExecutionBudgetStats;
    use std::assert_matches;
    use std::num::NonZeroUsize;

    fn limit(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("nonzero test limit")
    }

    fn runtime_admission(
        limits: PublicationPolicy,
        budget: &ExecutionBudget,
    ) -> Arc<PublicationAdmission> {
        Arc::new(PublicationAdmission::new(limits, budget.clone()))
    }

    fn admitted(budget: &ExecutionBudget) -> (usize, usize) {
        let stats = budget.stats();
        (stats.admitted_requests, stats.admitted_bytes)
    }

    #[test]
    fn an_oversized_commit_is_rejected_before_queue_capacity() {
        let budget = ExecutionBudget::builder()
            .max_admitted_requests(limit(1))
            .build();
        let admission = runtime_admission(PublicationPolicy::default(), &budget);
        let namespace_id = NamespaceId::parse("large").expect("namespace");
        let permit = admission.acquire(&namespace_id, 0).expect("fill queue");
        let mut candidate = PreparedCandidate::new(CommitCandidate::new(
            super::super::tests::create_directory_request("large", "large"),
        ));
        candidate.wal_record_bytes_upper_bound =
            MAX_WAL_OBJECT_BYTES - WAL_OBJECT_OVERHEAD_BYTES + 1;
        let error = admission
            .acquire_candidate(&namespace_id, &candidate)
            .err()
            .expect("oversized commit");
        assert_eq!(error.code(), loonfs_types::ErrorCode::ContentTooLarge);
        assert!(error.to_string().contains("too large for one WAL object"));
        assert_matches!(
            error,
            CoreError::CommitTooLarge { estimated_bytes, max_bytes }
            if estimated_bytes == MAX_WAL_OBJECT_BYTES + 1 && max_bytes == MAX_WAL_OBJECT_BYTES
        );
        drop(permit);
        assert_eq!(admission.used_requests(), 0);
        assert_eq!(budget.stats(), ExecutionBudgetStats::default());
    }

    /// A merge under the namespace `main` whose planned WAL bytes are
    /// `planned_bytes`, so the merge allowance is charged three times those
    /// bytes.
    fn merge(planned_bytes: usize) -> PreparedCandidate {
        let mut candidate = PreparedCandidate::new(loonfs_core::test_support::merge_candidate(
            CommitCandidate::new(super::super::tests::create_directory_request(
                "merge", "merge",
            )),
            loonfs_types::format::wal::MergeRecord {
                source_namespace_id: NamespaceId::parse("fork").expect("namespace"),
                source_after_seq: loonfs_types::ChangeSeq(3),
                source_through_seq: loonfs_types::ChangeSeq(7),
            },
            loonfs_types::ChangeSeq(3),
            std::collections::BTreeMap::from([(
                0,
                loonfs_types::InodeId::generate(
                    loonfs_types::ChangeSeq(4),
                    std::num::NonZeroU32::MIN,
                ),
            )]),
        ));
        candidate.wal_record_bytes_upper_bound = planned_bytes;
        candidate
    }

    /// The value of the metric `name` that `recorder` keeps.
    fn metric(recorder: &DefaultMetricsRecorder, name: &str) -> MetricValue {
        recorder
            .snapshot()
            .by_name(name)
            .next()
            .unwrap_or_else(|| panic!("no `{name}` metric registered"))
            .value
            .clone()
    }

    /// Checks that a merge whose estimated peak is above its namespace's byte
    /// allowance and above the budget's byte total is admitted when the peak
    /// fits the merge allowance, while the namespace allowance still refuses
    /// an ordinary request above it. The merge counts as one request and
    /// reserves the whole allowance, which is smaller than
    /// [`MIN_MERGE_RESERVED_BYTES`], and no byte limit is charged for it, so
    /// an ordinary request still fits the byte total beside it. The merge's
    /// permit keeps the inline reservation calls working, and it refunds
    /// exactly what it charged.
    #[test]
    fn a_merge_is_held_to_the_merge_allowance_and_to_no_byte_limit() {
        let namespace_id = NamespaceId::parse("main").expect("namespace");
        let overhead = 512 + namespace_id.as_str().len();
        let budget = ExecutionBudget::builder()
            .max_admitted_bytes(limit(4096))
            .max_merge_bytes(16384)
            .build();
        let admission = runtime_admission(
            PublicationPolicy {
                max_estimated_bytes_per_namespace: limit(1024),
                ..PublicationPolicy::default()
            },
            &budget,
        );
        let mut ordinary = PreparedCandidate::new(CommitCandidate::new(
            super::super::tests::create_directory_request("ordinary", "ordinary"),
        ));
        ordinary.estimated_retained_bytes = 2048;
        assert!(matches!(
            admission.acquire_candidate(&namespace_id, &ordinary),
            Err(CoreError::CommitQueueFull)
        ));
        assert_eq!(budget.stats(), ExecutionBudgetStats::default());

        let mut large = merge(4096);
        large.estimated_retained_bytes = 8192;
        let permit = admission
            .acquire_candidate(&namespace_id, &large)
            .expect("a merge above the namespace allowance and the byte total");
        let merge_only = ExecutionBudgetStats {
            admitted_requests: 1,
            admitted_merges: 1,
            admitted_merge_bytes: 16384,
            ..ExecutionBudgetStats::default()
        };
        assert_eq!(budget.stats(), merge_only);
        assert_eq!(admission.namespace_usage(&namespace_id), (1, 0, 0));
        let beside = admission
            .acquire(&namespace_id, 0)
            .expect("an ordinary request beside the merge");
        assert_eq!(admitted(&budget), (2, overhead));
        drop(beside);
        assert_eq!(permit.reserve_inline(std::iter::empty(), 0, 1024), 0);
        permit.release_inline();
        drop(permit);
        assert!(admission.lock_namespaces().is_empty());
        assert_eq!(budget.stats(), ExecutionBudgetStats::default());
    }

    /// Checks that merges share the allowance by their reservations. A merge
    /// whose estimated peak, three times its planned WAL bytes, exceeds the
    /// whole allowance fails with `merge_too_large`, reports the peak and the
    /// allowance, and charges nothing. Two merges from two runtimes that
    /// share a budget each reserve [`MIN_MERGE_RESERVED_BYTES`] at once, and
    /// a third, whose reservation doesn't fit, gets `server_busy`, which the
    /// admission refusal counter doesn't record. A merge whose growth past
    /// the floor doesn't fit beside another merge's reservation gets
    /// `server_busy` and returns its floor to the allowance, and the same
    /// merge alone grows its reservation to its peak.
    #[test]
    fn merges_share_the_allowance_and_a_merge_that_cant_grow_is_busy() {
        let namespace_id = NamespaceId::parse("main").expect("namespace");
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let allowance = 2 * MIN_MERGE_RESERVED_BYTES;
        let budget = ExecutionBudget::builder()
            .max_merge_bytes(allowance)
            .metrics_recorder(recorder.clone())
            .build();
        let first_runtime = runtime_admission(PublicationPolicy::default(), &budget);
        let second_runtime = runtime_admission(PublicationPolicy::default(), &budget);
        let gauges = |merges: i64, bytes: usize| {
            assert_eq!(
                metric(&recorder, "loonfs.execution_budget.admitted_merges"),
                MetricValue::Gauge(merges)
            );
            assert_eq!(
                metric(&recorder, "loonfs.execution_budget.admitted_merge_bytes"),
                MetricValue::Gauge(i64::try_from(bytes).expect("gauge"))
            );
        };

        let error = first_runtime
            .acquire_candidate(&namespace_id, &merge(allowance / 3 + 1))
            .err()
            .expect("a merge above the allowance");
        assert_matches!(
            error,
            CoreError::MergeTooLarge {
                bound: loonfs_core::MergeBound::MergeAllowance,
                estimated_bytes,
                max_bytes,
                split_at_seq: None,
            } if estimated_bytes == 3 * (allowance / 3 + 1) && max_bytes == allowance
        );
        assert_eq!(budget.stats(), ExecutionBudgetStats::default());

        let first = first_runtime
            .acquire_candidate(&namespace_id, &merge(0))
            .expect("a first merge");
        let second = second_runtime
            .acquire_candidate(&namespace_id, &merge(0))
            .expect("a second merge beside the first");
        assert_eq!(budget.stats().admitted_merges, 2);
        assert_eq!(budget.stats().admitted_merge_bytes, allowance);
        gauges(2, allowance);
        let both = budget.stats();
        for runtime in [&first_runtime, &second_runtime] {
            let error = runtime
                .acquire_candidate(&namespace_id, &merge(0))
                .err()
                .expect("no room for a third merge");
            assert_eq!(error.code(), loonfs_types::ErrorCode::ServerBusy);
        }
        assert_eq!(budget.stats(), both, "a refused merge charges nothing");
        assert_eq!(
            metric(&recorder, "loonfs.execution_budget.admission_rejections"),
            MetricValue::Counter(0)
        );

        drop(second);
        let one = budget.stats();
        let growing = merge(MIN_MERGE_RESERVED_BYTES / 3 + 1);
        let error = second_runtime
            .acquire_candidate(&namespace_id, &growing)
            .err()
            .expect("no room to grow beside the first merge");
        assert_eq!(error.code(), loonfs_types::ErrorCode::ServerBusy);
        assert_eq!(budget.stats(), one, "the refused merge returns its floor");
        drop(first);
        let alone = second_runtime
            .acquire_candidate(&namespace_id, &growing)
            .expect("the same merge alone");
        let peak = 3 * (MIN_MERGE_RESERVED_BYTES / 3 + 1);
        assert_eq!(budget.stats().admitted_merge_bytes, peak);
        gauges(1, peak);
        drop(alone);
        assert_eq!(budget.stats(), ExecutionBudgetStats::default());
        gauges(0, 0);
    }

    /// Checks that a budget whose merge allowance is zero refuses every
    /// merge candidate, even one that plans no bytes, with
    /// `merge_too_large`, and charges nothing.
    #[test]
    fn a_zero_allowance_admits_no_merge() {
        let namespace_id = NamespaceId::parse("main").expect("namespace");
        let budget = ExecutionBudget::builder().max_merge_bytes(0).build();
        let admission = runtime_admission(PublicationPolicy::default(), &budget);
        for planned_bytes in [0, 1] {
            let error = admission
                .acquire_candidate(&namespace_id, &merge(planned_bytes))
                .err()
                .expect("merges are off");
            assert_matches!(error, CoreError::MergeTooLarge { max_bytes: 0, .. });
        }
        assert_eq!(budget.stats(), ExecutionBudgetStats::default());
    }

    #[test]
    fn global_and_namespace_limits_share_one_charge_until_last_owner_drops() {
        let budget = ExecutionBudget::builder()
            .max_admitted_requests(limit(3))
            .build();
        let admission = runtime_admission(
            PublicationPolicy {
                max_requests_per_namespace: limit(2),
                ..PublicationPolicy::default()
            },
            &budget,
        );
        let a = NamespaceId::parse("a").expect("namespace");
        let b = NamespaceId::parse("b").expect("namespace");
        let first = admission.acquire(&a, 0).expect("first");
        let second = admission.acquire(&a, 0).expect("second");
        assert!(matches!(
            admission.acquire(&a, 0),
            Err(CoreError::CommitQueueFull)
        ));
        let third = admission.acquire(&b, 0).expect("other namespace has room");
        assert!(matches!(
            admission.acquire(&b, 0),
            Err(CoreError::CommitQueueFull)
        ));
        let (sender, receiver) = oneshot::channel();
        let waiter = AdmittedWaiter::new(sender, &first);
        drop(receiver);
        drop(first);
        assert_eq!(
            admission.used_requests(),
            3,
            "disconnect keeps worker charged"
        );
        assert_eq!(budget.stats().admitted_requests, 3);
        let _ = waiter.send(());
        assert_eq!(admission.used_requests(), 2);
        assert_eq!(budget.stats().admitted_requests, 2);
        drop((second, third));
        assert!(
            admission.lock_namespaces().is_empty(),
            "closed namespaces leave no accounting entries"
        );
        assert_eq!(budget.stats(), ExecutionBudgetStats::default());
    }

    #[test]
    fn namespace_byte_limit_leaves_room_for_another_tenant() {
        let a = NamespaceId::parse("a").expect("namespace");
        let b = NamespaceId::parse("b").expect("namespace");
        let budget = ExecutionBudget::builder()
            .max_admitted_bytes(limit(1026))
            .build();
        let admission = runtime_admission(
            PublicationPolicy {
                max_estimated_bytes_per_namespace: limit(513),
                ..PublicationPolicy::default()
            },
            &budget,
        );
        let first = admission.acquire(&a, 0).expect("first tenant");
        assert!(matches!(
            admission.acquire(&a, 0),
            Err(CoreError::CommitQueueFull)
        ));
        let second = admission.acquire(&b, 0).expect("other tenant has room");
        assert_eq!(admitted(&budget), (2, 1026));
        drop((first, second));
        assert_eq!(admission.used_requests(), 0);
        assert_eq!(admitted(&budget), (0, 0));
    }

    #[test]
    fn byte_limit_rejects_before_count_limit_and_refunds_exactly() {
        let namespace = NamespaceId::parse("bytes").expect("namespace");
        let candidate = CommitCandidate::new(super::super::tests::create_directory_request(
            "wide", "wide",
        ));
        let charge = candidate.estimated_retained_bytes() + 512 + namespace.as_str().len();
        let budget = ExecutionBudget::builder()
            .max_admitted_bytes(limit(charge))
            .build();
        let admission = runtime_admission(PublicationPolicy::default(), &budget);
        let permit = admission
            .acquire(&namespace, candidate.estimated_retained_bytes())
            .expect("exact fit");
        assert_eq!(admitted(&budget), (1, charge));
        assert!(matches!(
            admission.acquire(&namespace, 0),
            Err(CoreError::CommitQueueFull)
        ));
        drop(permit);
        assert!(admission
            .acquire(&namespace, candidate.estimated_retained_bytes())
            .is_ok());
        let too_small = ExecutionBudget::builder()
            .max_admitted_bytes(limit(charge - 1))
            .build();
        let refused = runtime_admission(PublicationPolicy::default(), &too_small);
        assert!(matches!(
            refused.acquire(&namespace, candidate.estimated_retained_bytes()),
            Err(CoreError::CommitQueueFull)
        ));
        assert!(refused.lock_namespaces().is_empty());
        assert_eq!(too_small.stats(), ExecutionBudgetStats::default());
    }

    #[test]
    fn a_refused_admission_charges_nothing() {
        let budget = ExecutionBudget::builder()
            .max_admitted_requests(limit(2))
            .build();
        let one_per_namespace = PublicationPolicy {
            max_requests_per_namespace: limit(1),
            ..PublicationPolicy::default()
        };
        let first_runtime = runtime_admission(one_per_namespace.clone(), &budget);
        let second_runtime = runtime_admission(one_per_namespace, &budget);
        let documents = NamespaceId::parse("documents").expect("namespace");
        let notes = NamespaceId::parse("notes").expect("namespace");
        let held = first_runtime.acquire(&documents, 0).expect("first request");
        let charged = admitted(&budget);

        assert!(matches!(
            first_runtime.acquire(&documents, 0),
            Err(CoreError::CommitQueueFull)
        ));
        assert_eq!(
            admitted(&budget),
            charged,
            "a namespace refusal charges no total"
        );
        let other = second_runtime.acquire(&notes, 0).expect("second request");
        let full = admitted(&budget);
        assert!(matches!(
            second_runtime.acquire(&documents, 0),
            Err(CoreError::CommitQueueFull)
        ));
        assert_eq!(admitted(&budget), full, "a total refusal charges no total");
        assert_eq!(
            second_runtime.used_requests(),
            1,
            "a total refusal charges no namespace"
        );
        assert!(!second_runtime.lock_namespaces().contains_key(&documents));

        drop((held, other));
        assert_eq!(first_runtime.used_requests(), 0);
        assert_eq!(second_runtime.used_requests(), 0);
        assert_eq!(budget.stats(), ExecutionBudgetStats::default());
    }

    #[tokio::test]
    async fn a_runtime_without_a_budget_admits_and_runs_the_default_totals() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(
            loonfs_objectstore::local_fs_store::LocalFsStore::new(directory.path()).expect("store"),
        );
        let writer = crate::LoonFs::builder_with_store(store)
            .writer_id(loonfs_test_support::ids::writer_id("default-budget"))
            .build()
            .await
            .expect("writer");
        let admission = &writer.mode.publisher.shared.context.admission;
        let budget = writer.execution_budget();
        let namespaces = (0..9)
            .map(|index| NamespaceId::parse(format!("ns-{index}")).expect("namespace"))
            .collect::<Vec<_>>();
        let overhead = 512 + namespaces[0].as_str().len();

        let mut permits = Vec::new();
        for namespace_id in &namespaces[..8] {
            for _ in 0..1024 {
                permits.push(admission.acquire(namespace_id, 0).expect("default room"));
            }
            assert!(matches!(
                admission.acquire(namespace_id, 0),
                Err(CoreError::CommitQueueFull)
            ));
        }
        assert_eq!(budget.stats().admitted_requests, 8192);
        assert!(matches!(
            admission.acquire(&namespaces[8], 0),
            Err(CoreError::CommitQueueFull)
        ));
        permits.clear();

        for namespace_id in &namespaces[..8] {
            permits.push(
                admission
                    .acquire(namespace_id, 8 * 1024 * 1024 - overhead)
                    .expect("a full namespace byte allowance"),
            );
        }
        assert_eq!(admitted(budget), (8, 64 * 1024 * 1024));
        assert!(matches!(
            admission.acquire(&namespaces[8], 0),
            Err(CoreError::CommitQueueFull)
        ));
        permits.clear();
        assert_eq!(admitted(budget), (0, 0));

        let allowance = 512 * 1024 * 1024;
        assert_matches!(
            admission
                .acquire_candidate(&namespaces[0], &merge(allowance / 3 + 1))
                .err(),
            Some(CoreError::MergeTooLarge { max_bytes, .. }) if max_bytes == allowance
        );
        drop(
            admission
                .acquire_candidate(&namespaces[0], &merge(allowance / 3))
                .expect("a merge within the default allowance"),
        );

        let mut running = Vec::new();
        for _ in 0..8 {
            running.push(admission.publication_permit().await);
        }
        let mut ninth = Box::pin(admission.publication_permit());
        assert!(futures::poll!(ninth.as_mut()).is_pending());
        assert_eq!(budget.stats().publications_running, 8);
        assert_eq!(budget.stats().publications_waiting, 1);
        drop(ninth);
        drop(running);
        assert_eq!(budget.stats(), ExecutionBudgetStats::default());
        writer.shutdown().await.expect("shut down");
    }
}
