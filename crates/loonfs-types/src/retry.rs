//! Bounded retry policy for replay-safe transport operations.

use crate::clock::MonotonicTimer;

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
    /// The backoff never runs past the deadline.
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
        TransportRetryOutcome::Wait {
            backoff_ms: self.backoff_ms(*retries).min(remaining_ms),
        }
    }

    /// Returns the capped exponential backoff before retry number `retry`,
    /// counting from one, in milliseconds.
    pub fn backoff_ms(&self, retry: u32) -> u64 {
        let doublings = retry.saturating_sub(1).min(16);
        self.initial_backoff_ms
            .saturating_mul(1 << doublings)
            .min(self.max_backoff_ms)
    }
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
