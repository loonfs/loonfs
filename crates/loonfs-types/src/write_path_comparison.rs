//! Records the random draws of one future and replays them to another, so
//! that `loonfs-core` can prepare one commit twice and compare the results.
//!
//! This module is temporary. It exists only for the comparison of the write
//! path with its frozen copy in `loonfs-core`, and it's removed with that
//! frozen copy before the branch merges.
//!
//! A scope is visible only while its future is being polled: the wrapper sets
//! a thread-local for the length of each poll and restores the previous value
//! when the poll returns or panics. Generated IDs that other tasks draw at the
//! same time come from the installed entropy source as usual.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

thread_local! {
    static SCOPE: RefCell<Option<Scope>> = const { RefCell::new(None) };
}

/// Whether a scope records the draws of its future or replays them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Record,
    Replay,
}

/// The draws of one scoped future.
#[derive(Debug)]
struct Scope {
    mode: Mode,
    /// The draws recorded so far, or the draws not yet replayed.
    draws: VecDeque<Vec<u8>>,
    /// How many draws a replaying future asked for that weren't recorded,
    /// or whose length differed from the recorded draw.
    missing_draws: usize,
}

/// How a replaying future used the draws that it was given.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayOutcome {
    /// How many draws the future asked for that weren't recorded, or whose
    /// length differed from the recorded draw.
    pub missing_draws: usize,
    /// How many recorded draws the future didn't ask for.
    pub leftover_draws: usize,
}

/// Runs `future` and returns its output with every random draw it made, in
/// order.
///
/// The future is boxed before the returned future is built, so the returned
/// future's layout doesn't contain the layout of `future`.
pub fn record_draws<F: Future>(future: F) -> impl Future<Output = (F::Output, Vec<Vec<u8>>)> {
    let scoped = Scoped::new(future, Mode::Record, VecDeque::new());
    async move {
        let (output, scope) = scoped.await;
        (output, scope.draws.into())
    }
}

/// Runs `future` with `draws` in place of fresh random bytes, in order, and
/// returns its output with how the future used them.
///
/// A draw that wasn't recorded, or whose length differs, comes from the
/// installed entropy source and is reported in the outcome.
pub fn replay_draws<F: Future>(
    future: F,
    draws: Vec<Vec<u8>>,
) -> impl Future<Output = (F::Output, ReplayOutcome)> {
    let scoped = Scoped::new(future, Mode::Replay, draws.into());
    async move {
        let (output, scope) = scoped.await;
        let outcome = ReplayOutcome {
            missing_draws: scope.missing_draws,
            leftover_draws: scope.draws.len(),
        };
        (output, outcome)
    }
}

/// Returns whether the current poll belongs to a future that replays draws.
pub fn replaying() -> bool {
    SCOPE.with(|scope| {
        scope
            .borrow()
            .as_ref()
            .is_some_and(|scope| scope.mode == Mode::Replay)
    })
}

/// Fills `bytes` from the current scope, or returns `false` when no scope
/// applies and the caller draws from the installed entropy source.
pub(crate) fn fill_scoped(bytes: &mut [u8]) -> bool {
    SCOPE.with(|scope| {
        let mut scope = scope.borrow_mut();
        let Some(scope) = scope.as_mut() else {
            return false;
        };
        match scope.mode {
            Mode::Record => {
                crate::installed_entropy().fill(bytes);
                scope.draws.push_back(bytes.to_vec());
            }
            Mode::Replay => match scope.draws.pop_front() {
                Some(draw) if draw.len() == bytes.len() => bytes.copy_from_slice(&draw),
                _ => {
                    crate::installed_entropy().fill(bytes);
                    scope.missing_draws += 1;
                }
            },
        }
        true
    })
}

/// A future whose scope is installed for the length of each poll.
struct Scoped<F> {
    future: Pin<Box<F>>,
    scope: Option<Scope>,
}

impl<F: Future> Scoped<F> {
    fn new(future: F, mode: Mode, draws: VecDeque<Vec<u8>>) -> Self {
        Self {
            future: Box::pin(future),
            scope: Some(Scope {
                mode,
                draws,
                missing_draws: 0,
            }),
        }
    }
}

impl<F: Future> Future for Scoped<F> {
    type Output = (F::Output, Scope);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let scope = this
            .scope
            .take()
            .expect("a scoped future should not be polled after it completes");
        let previous = SCOPE.with(|cell| cell.replace(Some(scope)));
        let restore = Restore {
            previous: Some(previous),
            slot: &mut this.scope,
        };
        let poll = this.future.as_mut().poll(cx);
        drop(restore);
        poll.map(|output| {
            let scope = this
                .scope
                .take()
                .expect("a scoped future's scope should be restored after each poll");
            (output, scope)
        })
    }
}

/// Puts the previous scope back and the future's own scope into its slot
/// when a poll returns or panics.
struct Restore<'s> {
    previous: Option<Option<Scope>>,
    slot: &'s mut Option<Scope>,
}

impl Drop for Restore<'_> {
    fn drop(&mut self) {
        let previous = self.previous.take().flatten();
        *self.slot = SCOPE.with(|cell| cell.replace(previous));
    }
}

#[cfg(test)]
mod tests {
    use super::{record_draws, replay_draws, replaying, ReplayOutcome};
    use crate::ids::random_u64;
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    fn run<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
        }
    }

    #[test]
    fn a_replay_repeats_the_recorded_draws_and_reports_missing_and_leftover_ones() {
        let (recorded, draws) = run(record_draws(async { [random_u64(), random_u64()] }));
        assert_eq!(draws.len(), 2);
        let (replayed, outcome) = run(replay_draws(
            async { (replaying(), [random_u64(), random_u64()]) },
            draws.clone(),
        ));
        assert_eq!(replayed, (true, recorded));
        assert_eq!(
            outcome,
            ReplayOutcome {
                missing_draws: 0,
                leftover_draws: 0
            }
        );
        let (_, outcome) = run(replay_draws(async { random_u64() }, draws.clone()));
        assert_eq!(
            outcome,
            ReplayOutcome {
                missing_draws: 0,
                leftover_draws: 1
            }
        );
        let (_, outcome) = run(replay_draws(
            async { [random_u64(), random_u64(), random_u64()] },
            draws,
        ));
        assert_eq!(
            outcome,
            ReplayOutcome {
                missing_draws: 1,
                leftover_draws: 0
            }
        );
        assert!(!replaying());
    }
}
