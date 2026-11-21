//! Unit tests for the transport retry policy.

use super::*;
use crate::SeededEntropy;

/// A timer that stays at one reading.
#[derive(Debug)]
struct FixedTimer(u64);

impl MonotonicTimer for FixedTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.0
    }
}

const POLICY: TransportRetryPolicy = TransportRetryPolicy {
    max_retries: 3,
    initial_backoff_ms: 100,
    max_backoff_ms: 400,
    operation_deadline_ms: 1_000,
};

#[test]
fn a_requested_wait_longer_than_the_backoff_is_waited_in_full() {
    let timer = FixedTimer(0);
    let deadline = OperationDeadline::start(&timer, POLICY.operation_deadline_ms);
    let mut retries = 0;
    assert_eq!(
        POLICY.next_backoff(&mut retries, Some(&deadline), Some(900)),
        TransportRetryOutcome::Wait { backoff_ms: 900 },
        "the requested wait is above the backoff cap and is not cut to it"
    );
    assert_eq!(retries, 1);
    assert_eq!(
        POLICY.next_backoff(&mut retries, None, Some(60_000)),
        TransportRetryOutcome::Wait { backoff_ms: 60_000 },
    );
    assert_eq!(retries, 2);
}

#[test]
fn a_requested_wait_shorter_than_the_backoff_waits_the_backoff() {
    let timer = FixedTimer(0);
    let deadline = OperationDeadline::start(&timer, POLICY.operation_deadline_ms);
    let mut retries = 1;
    assert!(matches!(
        POLICY.next_backoff(&mut retries, Some(&deadline), Some(50)),
        TransportRetryOutcome::Wait {
            backoff_ms: 100..=200
        }
    ));
    assert_eq!(retries, 2);
}

#[test]
fn a_requested_wait_the_deadline_cannot_fit_is_not_counted() {
    let timer = FixedTimer(0);
    let deadline = OperationDeadline::start(&timer, POLICY.operation_deadline_ms);
    let mut retries = 0;
    for requested_ms in [1_000, 5_000] {
        assert_eq!(
            POLICY.next_backoff(&mut retries, Some(&deadline), Some(requested_ms)),
            TransportRetryOutcome::DeadlinePassed,
        );
        assert_eq!(retries, 0, "a retry the client can't take is not counted");
    }
    assert_eq!(
        POLICY.next_backoff(&mut retries, Some(&deadline), Some(999)),
        TransportRetryOutcome::Wait { backoff_ms: 999 },
    );
    assert_eq!(retries, 1);
}

#[test]
fn the_policys_own_backoff_is_cut_short_at_the_deadline() {
    let timer = FixedTimer(0);
    let deadline = OperationDeadline::start(&timer, 50);
    let mut retries = 1;
    assert_eq!(
        POLICY.next_backoff(&mut retries, Some(&deadline), None),
        TransportRetryOutcome::Wait { backoff_ms: 50 },
    );
    assert_eq!(retries, 2);
}

#[test]
fn exhausted_retries_win_over_a_requested_wait() {
    let mut retries = POLICY.max_retries;
    assert_eq!(
        POLICY.next_backoff(&mut retries, None, Some(1)),
        TransportRetryOutcome::RetriesExhausted,
    );
    assert_eq!(retries, POLICY.max_retries);
}

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
