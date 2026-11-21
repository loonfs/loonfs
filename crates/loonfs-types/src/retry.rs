//! Bounded retry policy for replay-safe transport operations.

use crate::clock::MonotonicTimer;
use crate::entropy::{installed_entropy, Entropy};

/// Bounded retry configuration for replay-safe transport operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportRetryPolicy {
    /// Maximum retries after the first attempt.
    pub max_retries: u32,
    /// Backoff before the first retry, in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum backoff between attempts, in milliseconds.
    pub max_backoff_ms: u64,
    /// Total time allowed for the logical operation, in milliseconds.
    pub operation_deadline_ms: u64,
}

impl TransportRetryPolicy {
    /// Grants one retry under the policy's retry count and the operation's
    /// deadline, and counts it in `retries` when it is granted.
    ///
    /// Positive backoffs use equal jitter (`[cap / 2, cap]`) drawn from the
    /// process [`Entropy`] source so concurrent callers don't retry in
    /// lockstep while seeded runs stay repeatable. The backoff never runs past
    /// the deadline.
    pub fn next_backoff(
        &self,
        retries: &mut u32,
        deadline: Option<&OperationDeadline<'_>>,
    ) -> TransportRetryOutcome {
        if *retries >= self.max_retries {
            return TransportRetryOutcome::RetriesExhausted;
        }
        let remaining_ms = match deadline {
            Some(deadline) => match deadline.remaining_ms() {
                Some(remaining_ms) => remaining_ms,
                None => return TransportRetryOutcome::DeadlinePassed,
            },
            None => u64::MAX,
        };
        *retries += 1;
        let cap_ms = self.backoff_ms(*retries);
        let backoff_ms = if cap_ms == 0 {
            0
        } else {
            let entropy = installed_entropy();
            equal_jitter_ms(cap_ms, entropy.as_ref())
        };
        TransportRetryOutcome::Wait {
            backoff_ms: backoff_ms.min(remaining_ms),
        }
    }

    /// Returns the capped exponential backoff ceiling before retry number
    /// `retry`, counting from one, in milliseconds.
    pub fn backoff_ms(&self, retry: u32) -> u64 {
        doubling_backoff_ms(self.initial_backoff_ms, retry, self.max_backoff_ms)
    }
}

/// Applies equal jitter in `[cap_ms / 2, cap_ms]` using `entropy`.
fn equal_jitter_ms(cap_ms: u64, entropy: &dyn Entropy) -> u64 {
    let half = cap_ms / 2;
    let span = cap_ms - half;
    if span == 0 {
        return cap_ms;
    }
    let mut bytes = [0u8; 8];
    entropy.fill(&mut bytes);
    let sample = u64::from_be_bytes(bytes);
    half + (sample % (span + 1))
}

/// Returns the backoff before attempt number `attempt`, counting from one:
/// `first_backoff_ms`, doubled for each attempt after the first, at most
/// `max_backoff_ms`.
pub fn doubling_backoff_ms(first_backoff_ms: u64, attempt: u32, max_backoff_ms: u64) -> u64 {
    1_u64
        .checked_shl(attempt.saturating_sub(1))
        .map_or(u64::MAX, |factor| first_backoff_ms.saturating_mul(factor))
        .min(max_backoff_ms)
}

/// How one retry decision under a [`TransportRetryPolicy`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportRetryOutcome {
    /// The retry is granted after `backoff_ms`.
    Wait {
        /// Delay before the retry, in milliseconds.
        backoff_ms: u64,
    },
    /// Every retry the policy allows has been used.
    RetriesExhausted,
    /// The operation deadline has passed.
    DeadlinePassed,
}

/// Elapsed-time state for one logical operation's retry loop.
pub struct OperationDeadline<'timer> {
    timer: &'timer dyn MonotonicTimer,
    started_at_ms: u64,
    deadline_ms: u64,
}

impl<'timer> OperationDeadline<'timer> {
    /// Starts a deadline at the timer's current monotonic reading.
    pub fn start(timer: &'timer dyn MonotonicTimer, deadline_ms: u64) -> Self {
        Self {
            timer,
            started_at_ms: timer.monotonic_now_ms(),
            deadline_ms,
        }
    }

    /// Returns the remaining milliseconds, or `None` once the deadline expires.
    pub fn remaining_ms(&self) -> Option<u64> {
        let elapsed_ms = self
            .timer
            .monotonic_now_ms()
            .saturating_sub(self.started_at_ms);
        self.deadline_ms
            .checked_sub(elapsed_ms)
            .filter(|remaining_ms| *remaining_ms > 0)
    }

    /// Returns the total milliseconds assigned to the operation.
    pub fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SeededEntropy;

    #[test]
    fn equal_jitter_stays_in_half_to_cap_and_is_repeatable_with_seeded_entropy() {
        let first = SeededEntropy::from_seed(42);
        let second = SeededEntropy::from_seed(42);
        for cap_ms in [1, 2, 100, 1_000, 10_000] {
            let a = equal_jitter_ms(cap_ms, &first);
            let b = equal_jitter_ms(cap_ms, &second);
            assert_eq!(a, b);
            assert!(
                (cap_ms / 2..=cap_ms).contains(&a),
                "jittered {a} out of range for cap {cap_ms}"
            );
        }
    }
}
