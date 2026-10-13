//! A fixed delay for selected store reads.

use super::{
    Intercept, InterceptStore, Interceptor, KeyPredicate, OperationContext, OperationSelector,
};
use async_trait::async_trait;
use loonfs_objectstore::timing::StdMonotonicTimer;
use loonfs_types::MonotonicTimer;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug)]
pub struct LatencyInterceptor {
    keys: KeyPredicate,
    latency: Duration,
    timer: StdMonotonicTimer,
    read_started_at_ms: Mutex<Vec<u64>>,
}

pub type LatencyStore<S> = InterceptStore<S, LatencyInterceptor>;

impl<S> InterceptStore<S, LatencyInterceptor> {
    pub fn new(inner: S, keys: KeyPredicate, latency: Duration) -> Self {
        Self::with_interceptor(
            inner,
            LatencyInterceptor {
                keys,
                latency,
                timer: StdMonotonicTimer::default(),
                read_started_at_ms: Mutex::default(),
            },
        )
    }

    pub fn read_started_at_ms(&self) -> Vec<u64> {
        self.interceptor()
            .read_started_at_ms
            .lock()
            .expect("read timeline lock should not be poisoned")
            .clone()
    }
}

#[async_trait]
impl Interceptor for LatencyInterceptor {
    #[allow(
        clippy::disallowed_methods,
        reason = "the test store delay models provider latency without changing protocol time"
    )]
    async fn before(&self, context: &OperationContext<'_>) -> Intercept {
        if self.keys.matches(context.key()) && OperationSelector::Read.matches(context.kind()) {
            self.read_started_at_ms
                .lock()
                .expect("read timeline lock should not be poisoned")
                .push(self.timer.monotonic_now_ms());
            tokio::time::sleep(self.latency).await;
        }
        Intercept::Continue
    }
}
