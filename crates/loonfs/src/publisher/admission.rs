//! One budget for every caller whose publication has been admitted.

use super::{CoreError, NamespaceId};
use crate::PublicationLimits;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, Semaphore};

pub(super) struct PublicationAdmission {
    limits: PublicationLimits,
    usage: Mutex<Usage>,
    pub(super) publications: Semaphore,
}

#[derive(Default)]
struct Usage {
    total: RequestWeight,
    namespaces: HashMap<NamespaceId, RequestWeight>,
}

#[derive(Default)]
struct RequestWeight {
    requests: usize,
    estimated_bytes: usize,
}

impl PublicationAdmission {
    pub(super) fn new(limits: PublicationLimits) -> Self {
        Self {
            publications: Semaphore::new(limits.max_concurrent_publications.get()),
            limits,
            usage: Mutex::new(Usage::default()),
        }
    }

    fn lock_usage(&self) -> std::sync::MutexGuard<'_, Usage> {
        self.usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(super) fn used_requests(&self) -> usize {
        self.lock_usage().total.requests
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
        let mut usage = self.lock_usage();
        let empty = RequestWeight::default();
        let namespace = usage.namespaces.get(namespace_id).unwrap_or(&empty);
        if usage.total.requests >= self.limits.max_requests.get()
            || namespace.requests >= self.limits.max_requests_per_namespace.get()
            || estimated_bytes
                > self
                    .limits
                    .max_estimated_bytes_per_namespace
                    .get()
                    .saturating_sub(namespace.estimated_bytes)
            || estimated_bytes
                > self
                    .limits
                    .max_estimated_bytes
                    .get()
                    .saturating_sub(usage.total.estimated_bytes)
        {
            return Err(CoreError::CommitQueueFull);
        }
        usage.total.requests += 1;
        usage.total.estimated_bytes += estimated_bytes;
        let namespace = usage.namespaces.entry(namespace_id.clone()).or_default();
        namespace.requests += 1;
        namespace.estimated_bytes += estimated_bytes;
        Ok(Arc::new(AdmissionPermit {
            budget: Arc::clone(self),
            namespace_id: namespace_id.clone(),
            estimated_bytes,
        }))
    }
}

pub(super) struct AdmissionPermit {
    budget: Arc<PublicationAdmission>,
    namespace_id: NamespaceId,
    estimated_bytes: usize,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let mut usage = self.budget.lock_usage();
        usage.total.requests -= 1;
        usage.total.estimated_bytes -= self.estimated_bytes;
        if let Some(namespace) = usage.namespaces.get_mut(&self.namespace_id) {
            namespace.requests -= 1;
            namespace.estimated_bytes -= self.estimated_bytes;
            if namespace.requests == 0 {
                usage.namespaces.remove(&self.namespace_id);
            }
        }
    }
}

/// The worker retains admission until delivery, even if the receiver is gone.
/// A contending caller also holds this permit while retaining its candidate;
/// retries share it instead of competing for a second admission slot.
pub(super) struct AdmittedWaiter<T> {
    sender: oneshot::Sender<T>,
    _permit: Arc<AdmissionPermit>,
}

impl<T> AdmittedWaiter<T> {
    pub(super) fn new(sender: oneshot::Sender<T>, permit: &Arc<AdmissionPermit>) -> Self {
        Self {
            sender,
            _permit: Arc::clone(permit),
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
    use std::num::NonZeroUsize;

    fn limit(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).expect("nonzero test limit")
    }

    #[test]
    fn global_and_namespace_limits_share_one_charge_until_last_owner_drops() {
        let budget = Arc::new(PublicationAdmission::new(PublicationLimits {
            max_requests: limit(3),
            max_requests_per_namespace: limit(2),
            ..PublicationLimits::default()
        }));
        let a = NamespaceId::parse("a").expect("namespace");
        let b = NamespaceId::parse("b").expect("namespace");
        let first = budget.acquire(&a, 0).expect("first");
        let second = budget.acquire(&a, 0).expect("second");
        assert!(matches!(
            budget.acquire(&a, 0),
            Err(CoreError::CommitQueueFull)
        ));
        let third = budget.acquire(&b, 0).expect("other namespace has room");
        assert!(matches!(
            budget.acquire(&b, 0),
            Err(CoreError::CommitQueueFull)
        ));
        let (sender, receiver) = oneshot::channel();
        let waiter = AdmittedWaiter::new(sender, &first);
        drop(receiver);
        drop(first);
        assert_eq!(
            budget.lock_usage().total.requests,
            3,
            "disconnect keeps worker charged"
        );
        let _ = waiter.send(());
        assert_eq!(budget.lock_usage().total.requests, 2);
        drop((second, third));
        let usage = budget.lock_usage();
        assert_eq!(usage.total.requests, 0);
        assert_eq!(usage.total.estimated_bytes, 0);
        assert!(
            usage.namespaces.is_empty(),
            "closed namespaces leave no accounting entries"
        );
    }

    #[test]
    fn namespace_byte_limit_leaves_room_for_another_tenant() {
        let a = NamespaceId::parse("a").expect("namespace");
        let b = NamespaceId::parse("b").expect("namespace");
        let budget = Arc::new(PublicationAdmission::new(PublicationLimits {
            max_estimated_bytes: limit(1026),
            max_estimated_bytes_per_namespace: limit(513),
            ..PublicationLimits::default()
        }));
        let first = budget.acquire(&a, 0).expect("first tenant");
        assert!(matches!(
            budget.acquire(&a, 0),
            Err(CoreError::CommitQueueFull)
        ));
        let second = budget.acquire(&b, 0).expect("other tenant has room");
        drop((first, second));
        assert_eq!(budget.used_requests(), 0);
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
        let budget = Arc::new(PublicationAdmission::new(PublicationLimits {
            max_estimated_bytes: limit(charge),
            ..PublicationLimits::default()
        }));
        let permit = budget
            .acquire(
                &namespace,
                candidate.estimated_retained_bytes().expect("weight"),
            )
            .expect("exact fit");
        assert!(matches!(
            budget.acquire(&namespace, 0),
            Err(CoreError::CommitQueueFull)
        ));
        drop(permit);
        assert!(budget
            .acquire(
                &namespace,
                candidate.estimated_retained_bytes().expect("weight")
            )
            .is_ok());
        let too_small = Arc::new(PublicationAdmission::new(PublicationLimits {
            max_estimated_bytes: limit(charge - 1),
            ..PublicationLimits::default()
        }));
        assert!(matches!(
            too_small.acquire(
                &namespace,
                candidate.estimated_retained_bytes().expect("weight")
            ),
            Err(CoreError::CommitQueueFull)
        ));
        assert!(too_small.lock_usage().namespaces.is_empty());
    }
}
