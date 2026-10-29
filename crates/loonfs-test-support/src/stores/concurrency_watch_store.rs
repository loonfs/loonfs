//! Watches how many requests a path keeps in flight at once.
//!
//! A batched fetch's or a write wave's whole promise is that its round trips
//! overlap, and the only place that is visible is the store boundary: request
//! counts and call order both look identical whether a path issued its
//! requests one at a time or all at once. This wrapper counts matching reads,
//! puts, and deletes as they enter and leave, and records the high-water mark,
//! so a test can pin the width of a wave rather than only its size.
//!
//! Each watched request yields once before it delegates. That is what makes
//! the mark a measurement rather than a race: a batch polled onto the same
//! task has all of its members inside the wrapper before the first of them
//! can finish, whatever the wrapped store does.

use super::{
    Intercept, InterceptStore, Interceptor, KeyPredicate, OperationContext, OperationSelector,
};
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Concurrent requests observed for one operation class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreConcurrency {
    /// Most matching requests in flight at any instant.
    pub peak_in_flight: usize,
    /// Matching requests issued in total.
    pub total: usize,
}

#[derive(Debug, Default)]
struct InFlight {
    current: AtomicUsize,
    peak: AtomicUsize,
    total: AtomicUsize,
}

impl InFlight {
    fn snapshot(&self) -> StoreConcurrency {
        StoreConcurrency {
            peak_in_flight: self.peak.load(Ordering::SeqCst),
            total: self.total.load(Ordering::SeqCst),
        }
    }
}

/// Releases one in-flight count when the watched request finishes, fails, or
/// is dropped.
#[derive(Debug)]
pub struct InFlightGuard(Arc<InFlight>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.current.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Counts reads, puts, and deletes separately as they enter and leave.
#[derive(Debug)]
pub struct ConcurrencyWatchInterceptor {
    keys: KeyPredicate,
    reads: Arc<InFlight>,
    puts: Arc<InFlight>,
    deletes: Arc<InFlight>,
}

impl ConcurrencyWatchInterceptor {
    fn requests(&self, context: &OperationContext<'_>) -> Option<&Arc<InFlight>> {
        if !self.keys.matches(context.key()) {
            return None;
        }
        let kind = context.kind();
        if OperationSelector::Head.matches(kind) || OperationSelector::Read.matches(kind) {
            Some(&self.reads)
        } else if OperationSelector::Put.matches(kind) {
            Some(&self.puts)
        } else if OperationSelector::Delete.matches(kind) {
            Some(&self.deletes)
        } else {
            None
        }
    }
}

/// Measures concurrency separately for reads, puts, and deletes.
pub type ConcurrencyWatchStore<S> = InterceptStore<S, ConcurrencyWatchInterceptor>;

impl<S> InterceptStore<S, ConcurrencyWatchInterceptor> {
    /// Watches requests for keys selected by `keys`.
    pub fn new(inner: S, keys: KeyPredicate) -> Self {
        Self::with_interceptor(
            inner,
            ConcurrencyWatchInterceptor {
                keys,
                reads: Arc::default(),
                puts: Arc::default(),
                deletes: Arc::default(),
            },
        )
    }

    /// What has been observed so far.
    pub fn reads(&self) -> StoreConcurrency {
        self.interceptor().reads.snapshot()
    }

    /// Includes cancelled and failed puts in `total`.
    pub fn puts(&self) -> StoreConcurrency {
        self.interceptor().puts.snapshot()
    }

    /// Includes cancelled and failed deletes in `total`.
    pub fn deletes(&self) -> StoreConcurrency {
        self.interceptor().deletes.snapshot()
    }
}

#[async_trait]
impl Interceptor for ConcurrencyWatchInterceptor {
    type Guard = Option<InFlightGuard>;

    async fn before(&self, context: &OperationContext<'_>) -> (Intercept, Self::Guard) {
        let Some(requests) = self.requests(context) else {
            return (Intercept::Continue, None);
        };
        requests.total.fetch_add(1, Ordering::SeqCst);
        let current = requests.current.fetch_add(1, Ordering::SeqCst) + 1;
        requests.peak.fetch_max(current, Ordering::SeqCst);
        let guard = InFlightGuard(Arc::clone(requests));
        tokio::task::yield_now().await;
        (Intercept::Continue, Some(guard))
    }
}
