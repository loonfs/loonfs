//! A check of a cached view, or a load of a cached value, that the callers
//! which need one share.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

/// One check of a view at a time, whose outcome the reads that waited for
/// it share.
///
/// A read that needs a check waits for the check in flight, because that
/// check might have sent its requests before the read started. The read
/// then shares the next check, which the first read to stop waiting runs.
/// So a check that has finished never serves a read that started after it
/// finished, and a wave of concurrent reads costs at most two checks: the
/// one in flight when the wave arrives, and the one that the wave shares.
#[derive(Debug)]
pub struct SharedCheck<T> {
    /// How many checks have started. A read compares the count when it
    /// arrives with the number of the last finished check to tell whether
    /// that check started after the read did.
    started: AtomicU64,
    /// The number and outcome of the last check that finished. A read that
    /// runs a check keeps this locked until the check ends, so one check
    /// runs at a time and the reads that need one wait for it. (It's a
    /// `tokio` mutex because it stays locked across the check's requests.)
    finished: tokio::sync::Mutex<Option<(u64, T)>>,
}

impl<T> Default for SharedCheck<T> {
    fn default() -> Self {
        Self {
            started: AtomicU64::new(0),
            finished: tokio::sync::Mutex::new(None),
        }
    }
}

impl<T: Clone> SharedCheck<T> {
    /// Waits for the check in flight, if any, and then returns `current()`
    /// when it's `Some`, or else the outcome of a check that started after
    /// this call did: one that another read ran while this one waited, or
    /// `check`, which this call runs.
    pub async fn run<F: Future<Output = T>>(
        &self,
        current: impl FnOnce() -> Option<T>,
        check: impl FnOnce() -> F,
    ) -> T {
        let arrived = self.started.load(Ordering::SeqCst);
        let mut finished = self.finished.lock().await;
        if let Some(current) = current() {
            return current;
        }
        if let Some((_, outcome)) = finished.as_ref().filter(|(number, _)| *number > arrived) {
            return outcome.clone();
        }
        let number = self.started.fetch_add(1, Ordering::SeqCst) + 1;
        let outcome = check().await;
        *finished = Some((number, outcome.clone()));
        outcome
    }
}

impl<V: Clone, E: Clone> SharedCheck<Result<V, E>> {
    /// Waits for the load in flight, if any, and then returns the value of a
    /// load that succeeded, or else `current()` when it's `Some`, or else the
    /// value that `load` returns, which this call runs.
    ///
    /// Unlike a check, a load serves the callers that arrived while it ran,
    /// because the value under the key of an immutable object can't change.
    /// A load that fails shares nothing, so the next caller to stop waiting
    /// runs its own.
    pub(crate) async fn load<L, F: Future<Output = Result<V, L>>>(
        &self,
        current: impl FnOnce() -> Option<V>,
        load: impl FnOnce() -> F,
    ) -> Result<V, L> {
        let mut finished = self.finished.lock().await;
        if let Some((_, Ok(value))) = finished.as_ref() {
            return Ok(value.clone());
        }
        if let Some(value) = current() {
            return Ok(value);
        }
        let number = self.started.fetch_add(1, Ordering::SeqCst) + 1;
        let value = load().await?;
        *finished = Some((number, Ok(value.clone())));
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::SharedCheck;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Checks that reads that arrive while a check runs wait for it and then
    /// share one more check, even when that check finishes before they stop
    /// waiting, that a read whose view the check in flight confirmed takes
    /// it, and that a read after both checks finished runs its own.
    #[tokio::test]
    async fn a_wave_of_reads_shares_at_most_two_checks() {
        let join = SharedCheck::<usize>::default();
        let started = AtomicUsize::new(0);
        let (release, released) = tokio::sync::watch::channel(false);
        let check = || async {
            let number = started.fetch_add(1, Ordering::SeqCst) + 1;
            released
                .clone()
                .wait_for(|released| *released)
                .await
                .expect("the test keeps the sender");
            number
        };
        let read = || join.run(|| None, check);
        let mut first = std::pin::pin!(read());
        assert!(futures::poll!(first.as_mut()).is_pending());
        let mut wave = std::pin::pin!(futures::future::join(
            futures::future::join_all((0..4).map(|_| read())),
            join.run(|| Some(0), check),
        ));
        assert!(futures::poll!(wave.as_mut()).is_pending());
        release.send(true).expect("the checks wait for the release");
        let (first, (wave, current)) = futures::join!(first, wave);
        assert_eq!(first, 1);
        assert_eq!(wave, [2, 2, 2, 2], "the wave shares the second check");
        assert_eq!(current, 0, "a read that the first check confirmed takes it");
        assert_eq!(read().await, 3, "a finished check serves no later read");
        assert_eq!(started.load(Ordering::SeqCst), 3);
    }
}
