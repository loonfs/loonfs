//! Watches how many namespaces keep a selected request in flight at once.
//!
//! A process-wide permit is meant to serialize work across namespaces, and
//! the store boundary is where that shows: two namespaces' segment writes or
//! content reads in flight together mean two holders. Each selected request
//! is held for a moment before it is forwarded, so a second holder has time
//! to arrive and be counted.

use super::{Intercept, InterceptStore, Interceptor, OperationContext, Outcome};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type NamespaceOf = dyn for<'a> Fn(&OperationContext<'a>) -> Option<String> + Send + Sync;

/// How long a selected request is held before it is forwarded.
const HOLD: Duration = Duration::from_millis(5);

/// Holds selected requests and records the most namespaces with one in
/// flight at the same time.
pub struct NamespaceOverlapInterceptor {
    namespace_of: Arc<NamespaceOf>,
    in_flight: Mutex<BTreeMap<String, usize>>,
    peak_namespaces: AtomicUsize,
}

impl fmt::Debug for NamespaceOverlapInterceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NamespaceOverlapInterceptor")
            .field("in_flight", &self.in_flight)
            .field("peak_namespaces", &self.peak_namespaces)
            .finish_non_exhaustive()
    }
}

/// Records the most namespaces with a selected request in flight at once.
pub type NamespaceOverlapStore<S> = InterceptStore<S, NamespaceOverlapInterceptor>;

impl<S> InterceptStore<S, NamespaceOverlapInterceptor> {
    /// Watches every operation that `namespace_of` assigns to a namespace.
    pub fn new(
        inner: S,
        namespace_of: impl for<'a> Fn(&OperationContext<'a>) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self::with_interceptor(
            inner,
            NamespaceOverlapInterceptor {
                namespace_of: Arc::new(namespace_of),
                in_flight: Mutex::default(),
                peak_namespaces: AtomicUsize::new(0),
            },
        )
    }

    /// Returns the most namespaces seen with a selected request in flight at
    /// once since the last reset.
    pub fn peak_namespaces(&self) -> usize {
        self.interceptor().peak_namespaces.load(Ordering::SeqCst)
    }

    /// Forgets the recorded peak.
    pub fn reset_peak(&self) {
        self.interceptor()
            .peak_namespaces
            .store(0, Ordering::SeqCst);
    }
}

#[allow(
    clippy::disallowed_methods,
    reason = "a test-side pause on a watched request leaves other namespaces time to start theirs"
)]
async fn hold() {
    tokio::time::sleep(HOLD).await;
}

#[async_trait]
impl Interceptor for NamespaceOverlapInterceptor {
    async fn before(&self, context: &OperationContext<'_>) -> Intercept {
        let Some(namespace) = (self.namespace_of)(context) else {
            return Intercept::Continue;
        };
        {
            let mut in_flight = self.in_flight.lock().expect("in-flight lock");
            *in_flight.entry(namespace).or_default() += 1;
            self.peak_namespaces
                .fetch_max(in_flight.len(), Ordering::SeqCst);
        }
        hold().await;
        Intercept::ContinueWithAfter
    }

    fn after(&self, context: &OperationContext<'_>, _outcome: &Outcome) {
        let Some(namespace) = (self.namespace_of)(context) else {
            return;
        };
        let mut in_flight = self.in_flight.lock().expect("in-flight lock");
        let count = in_flight
            .get_mut(&namespace)
            .expect("a watched namespace is in flight");
        *count -= 1;
        if *count == 0 {
            in_flight.remove(&namespace);
        }
    }
}
