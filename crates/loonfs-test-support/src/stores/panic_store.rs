//! A store that panics on selected operations while a test arms it.

use super::{Intercept, InterceptStore, Interceptor, OperationContext};
use async_trait::async_trait;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

type Predicate = dyn for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync;

/// Panics on each selected operation while armed, and counts the selected
/// operations whether it's armed or not.
pub struct PanicInterceptor {
    predicate: Box<Predicate>,
    armed: AtomicBool,
    attempts: AtomicUsize,
}

impl fmt::Debug for PanicInterceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PanicInterceptor")
            .field("armed", &self.armed)
            .field("attempts", &self.attempts)
            .finish_non_exhaustive()
    }
}

/// Panics on selected operations while armed.
pub type PanicStore<S> = InterceptStore<S, PanicInterceptor>;

impl<S> InterceptStore<S, PanicInterceptor> {
    /// Selects operations with an operation predicate. The store starts
    /// disarmed.
    pub fn panicking_on(
        inner: S,
        predicate: impl for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self::with_interceptor(
            inner,
            PanicInterceptor {
                predicate: Box::new(predicate),
                armed: AtomicBool::new(false),
                attempts: AtomicUsize::new(0),
            },
        )
    }

    /// Panics on every selected operation until [`Self::disarm`] is called,
    /// and resets the attempt count.
    pub fn arm(&self) {
        self.interceptor().attempts.store(0, Ordering::SeqCst);
        self.interceptor().armed.store(true, Ordering::SeqCst);
    }

    /// Forwards every operation again.
    pub fn disarm(&self) {
        self.interceptor().armed.store(false, Ordering::SeqCst);
    }

    /// Returns the number of selected operations since the most recent
    /// arming, including the ones that panicked.
    pub fn attempts(&self) -> usize {
        self.interceptor().attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Interceptor for PanicInterceptor {
    type Guard = ();

    #[allow(
        clippy::panic,
        reason = "the injected panic is what the test that armed this store checks"
    )]
    async fn before(&self, context: &OperationContext<'_>) -> (Intercept, ()) {
        if (self.predicate)(context) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.armed.load(Ordering::SeqCst) {
                panic!("injected panic at `{}`", context.key());
            }
        }
        (Intercept::Continue, ())
    }
}
