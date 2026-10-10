//! Admission of one runtime's publication requests: its per-namespace limits,
//! and the totals of the execution budget it shares.

use super::{CoreError, NamespaceId, PreparedCandidate};
use crate::execution_budget::BudgetPermit;
use crate::{ExecutionBudget, PublicationLimits};
use loonfs_types::format::wal::{MAX_WAL_OBJECT_BYTES, WAL_OBJECT_OVERHEAD_BYTES};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::oneshot;

/// Admits one runtime's publication requests against its per-namespace
/// limits and the totals of its execution budget.
///
/// The per-namespace usage stays in the runtime because a namespace id names
/// a namespace only within one store, and runtimes over other stores may
/// share the budget. Lock order: this runtime's usage, then the budget's
/// totals. Neither lock is held across an await.
pub(super) struct PublicationAdmission {
    limits: PublicationLimits,
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
    pub(super) fn new(limits: PublicationLimits, budget: ExecutionBudget) -> Self {
        Self {
            limits,
            namespaces: Mutex::default(),
            budget,
        }
    }

    fn lock_namespaces(&self) -> MutexGuard<'_, HashMap<NamespaceId, RequestWeight>> {
        self.namespaces
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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

    pub(super) fn acquire_candidate(
        self: &Arc<Self>,
        namespace_id: &NamespaceId,
        candidate: &PreparedCandidate,
    ) -> Result<Arc<AdmissionPermit>, CoreError> {
        Self::validate_candidate(candidate)?;
        self.acquire(namespace_id, candidate.estimated_retained_bytes)
    }

    pub(super) fn validate_candidate(candidate: &PreparedCandidate) -> Result<(), CoreError> {
        candidate.candidate.validate_appended_content()?;
        let document_bytes = candidate
            .wal_record_bytes_upper_bound
            .saturating_add(WAL_OBJECT_OVERHEAD_BYTES);
        if document_bytes > MAX_WAL_OBJECT_BYTES {
            return Err(CoreError::CommitTooLarge {
                estimated_bytes: document_bytes,
                max_bytes: MAX_WAL_OBJECT_BYTES,
            });
        }
        Ok(())
    }

    pub(super) fn acquire(
        self: &Arc<Self>,
        namespace_id: &NamespaceId,
        estimated_request_bytes: usize,
    ) -> Result<Arc<AdmissionPermit>, CoreError> {
        // Include the channel, ID copies, fingerprint and queue bookkeeping.
        // Deletes carry only fixed-size options. Candidate bytes include any
        // proof or rejection data retained while waiting to publish.
        let estimated_bytes = estimated_request_bytes
            .saturating_add(512)
            .saturating_add(namespace_id.as_str().len());
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
        self.budget.charge_admission(estimated_bytes)?;
        let namespace = namespaces.entry(namespace_id.clone()).or_default();
        namespace.requests += 1;
        namespace.estimated_bytes += estimated_bytes;
        Ok(Arc::new(AdmissionPermit {
            admission: Arc::clone(self),
            namespace_id: namespace_id.clone(),
            estimated_bytes,
            inline_bytes: AtomicUsize::new(0),
        }))
    }
}

pub(super) struct AdmissionPermit {
    admission: Arc<PublicationAdmission>,
    namespace_id: NamespaceId,
    estimated_bytes: usize,
    inline_bytes: AtomicUsize,
}

impl AdmissionPermit {
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

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let mut namespaces = self.admission.lock_namespaces();
        if let Some(namespace) = namespaces.get_mut(&self.namespace_id) {
            namespace.requests -= 1;
            namespace.estimated_bytes -= self.estimated_bytes;
            namespace.inline_bytes -= self.inline_bytes.load(Ordering::Relaxed);
            if namespace.requests == 0 {
                namespaces.remove(&self.namespace_id);
            }
        }
        // Refunded under this runtime's lock, as it was charged, so this
        // runtime never sees its namespace refunded while the total is not.
        self.admission.budget.refund_admission(self.estimated_bytes);
    }
}

/// The worker retains admission until delivery, even if the receiver is gone.
/// A contending caller also holds this permit while retaining its candidate;
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
    use crate::publish::CommitCandidate;
    use crate::ExecutionBudgetStats;
    use std::num::NonZeroUsize;

    fn limit(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("nonzero test limit")
    }

    fn runtime_admission(
        limits: PublicationLimits,
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
        let admission = runtime_admission(PublicationLimits::default(), &budget);
        let namespace_id = NamespaceId::parse("large").expect("namespace");
        let permit = admission.acquire(&namespace_id, 0).expect("fill queue");
        let mut candidate = PreparedCandidate::new(CommitCandidate::new(
            super::super::tests::create_directory_request("large", "large"),
        ))
        .expect("prepare");
        candidate.wal_record_bytes_upper_bound =
            MAX_WAL_OBJECT_BYTES - WAL_OBJECT_OVERHEAD_BYTES + 1;
        let error = admission
            .acquire_candidate(&namespace_id, &candidate)
            .err()
            .expect("oversized commit");
        assert_eq!(error.code(), loonfs_types::ErrorCode::ContentTooLarge);
        assert!(error.to_string().contains("too large for one WAL object"));
        assert!(
            matches!(error, CoreError::CommitTooLarge { estimated_bytes, max_bytes }
            if estimated_bytes == MAX_WAL_OBJECT_BYTES + 1 && max_bytes == MAX_WAL_OBJECT_BYTES)
        );
        drop(permit);
        assert_eq!(admission.used_requests(), 0);
        assert_eq!(budget.stats(), ExecutionBudgetStats::default());
    }

    #[test]
    fn global_and_namespace_limits_share_one_charge_until_last_owner_drops() {
        let budget = ExecutionBudget::builder()
            .max_admitted_requests(limit(3))
            .build();
        let admission = runtime_admission(
            PublicationLimits {
                max_requests_per_namespace: limit(2),
                ..PublicationLimits::default()
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
            PublicationLimits {
                max_estimated_bytes_per_namespace: limit(513),
                ..PublicationLimits::default()
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
        let charge = candidate
            .estimated_retained_bytes()
            .expect("request weight")
            + 512
            + namespace.as_str().len();
        let budget = ExecutionBudget::builder()
            .max_admitted_bytes(limit(charge))
            .build();
        let admission = runtime_admission(PublicationLimits::default(), &budget);
        let permit = admission
            .acquire(
                &namespace,
                candidate.estimated_retained_bytes().expect("weight"),
            )
            .expect("exact fit");
        assert_eq!(admitted(&budget), (1, charge));
        assert!(matches!(
            admission.acquire(&namespace, 0),
            Err(CoreError::CommitQueueFull)
        ));
        drop(permit);
        assert!(admission
            .acquire(
                &namespace,
                candidate.estimated_retained_bytes().expect("weight")
            )
            .is_ok());
        let too_small = ExecutionBudget::builder()
            .max_admitted_bytes(limit(charge - 1))
            .build();
        let refused = runtime_admission(PublicationLimits::default(), &too_small);
        assert!(matches!(
            refused.acquire(
                &namespace,
                candidate.estimated_retained_bytes().expect("weight")
            ),
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
        let one_per_namespace = PublicationLimits {
            max_requests_per_namespace: limit(1),
            ..PublicationLimits::default()
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
            .writer_id("default-budget")
            .build()
            .await
            .expect("writer");
        let admission = &writer.mode.publisher.shared.admission;
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
