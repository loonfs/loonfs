//! Permits for one kind of work, and the counts of the work that has them
//! or waits for them.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::{Semaphore, SemaphorePermit};

/// Receives the running and waiting counts of one set of permits after
/// every change.
pub trait PermitObserver: Send + Sync {
    fn report(&self, running: usize, waiting: usize);
}

/// The permits for one kind of work, and the count of the work that has
/// them or waits for them.
///
/// Waits are first come, first served.
pub struct Permits {
    permits: Semaphore,
    counts: Mutex<PermitCounts>,
    observer: Option<Arc<dyn PermitObserver>>,
}

/// The work that has a permit from a [`Permits`] and the work that waits
/// for one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PermitCounts {
    pub running: usize,
    pub waiting: usize,
}

impl std::fmt::Debug for Permits {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Permits")
            .field("counts", &self.counts())
            .finish_non_exhaustive()
    }
}

impl Permits {
    /// Creates `limit` permits whose counts are reported to `observer`.
    pub fn new(limit: NonZeroUsize, observer: Option<Arc<dyn PermitObserver>>) -> Self {
        Self {
            permits: Semaphore::new(limit.get().min(Semaphore::MAX_PERMITS)),
            counts: Mutex::default(),
            observer,
        }
    }

    /// Waits for a permit. Dropping the future gives up its place.
    pub async fn acquire(&self) -> BudgetPermit<'_> {
        self.update(|counts| counts.waiting += 1);
        let mut permit = BudgetPermit {
            permits: self,
            permit: None,
        };
        let acquired = self
            .permits
            .acquire()
            .await
            .expect("the permits should remain open");
        self.update(|counts| {
            counts.waiting -= 1;
            counts.running += 1;
        });
        permit.permit = Some(acquired);
        permit
    }

    /// Snapshots the work that has a permit and the work that waits for
    /// one.
    pub fn counts(&self) -> PermitCounts {
        *self
            .counts
            .lock()
            .expect("the permit counts lock should not be poisoned")
    }

    /// Changes the counts, then reports them after the lock is released,
    /// because the observer is caller code.
    fn update(&self, change: impl FnOnce(&mut PermitCounts)) {
        let counts = {
            let mut counts = self
                .counts
                .lock()
                .expect("the permit counts lock should not be poisoned");
            change(&mut counts);
            *counts
        };
        self.report(counts);
    }

    fn report(&self, counts: PermitCounts) {
        if let Some(observer) = &self.observer {
            observer.report(counts.running, counts.waiting);
        }
    }
}

/// A permit from a [`Permits`], or a place in the queue for one.
/// Dropping it returns the permit or gives up the place.
pub struct BudgetPermit<'a> {
    permits: &'a Permits,
    permit: Option<SemaphorePermit<'a>>,
}

impl Drop for BudgetPermit<'_> {
    fn drop(&mut self) {
        let permit = self.permit.take();
        let running = permit.is_some();
        // A drop can run while a panic unwinds, and a second panic then
        // aborts the process, so this recovers a poisoned lock instead of
        // panicking. The critical section only updates two counts.
        let counts = {
            let mut counts = self
                .permits
                .counts
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if running {
                counts.running -= 1;
            } else {
                counts.waiting -= 1;
            }
            *counts
        };
        self.permits.report(counts);
        // The count falls before the permit returns, so the running count
        // never passes the limit.
        drop(permit);
    }
}
