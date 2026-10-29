//! How a sweep records each call's outcome on a held namespace, and when it
//! makes a failed call again.

use crate::sweep::lock;
use crate::sweep::Sweep;
use crate::sweep::SweepRetry;
use crate::sweep::SweepState;
use crate::sweep::MAX_SWEEP_BACKOFF_MS;

use loonfs::{ErrorCode, NamespaceId};
use loonfs_http::HeldNamespace;

use std::sync::Mutex;

/// How one call failed on one namespace.
pub(super) enum CallFailure {
    /// The call returned an error.
    Error { code: ErrorCode, error: String },
    /// The call panicked, which counts as a `server_error` failure.
    Panic { panic: String },
}

impl CallFailure {
    pub(super) fn error(code: ErrorCode, error: impl std::fmt::Display) -> Self {
        Self::Error {
            code,
            error: error.to_string(),
        }
    }

    fn code(&self) -> ErrorCode {
        match self {
            Self::Error { code, .. } => *code,
            Self::Panic { .. } => ErrorCode::ServerError,
        }
    }

    /// Whether the failure counts against the call. A namespace that is
    /// gone, or a server that is shutting down, doesn't.
    fn counts(&self) -> bool {
        !matches!(
            self.code(),
            ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted | ErrorCode::ShuttingDown
        )
    }
}

/// Where a call stands on a held namespace after one attempt.
#[derive(Debug, Clone, Copy)]
struct Retry {
    /// Failures in a row before the attempt.
    failures_before: u32,
    /// Failures in a row after it, zero when it didn't fail.
    attempts: u32,
    /// How long the sweep waits before it makes the call again.
    wait_ms: u64,
}

/// The calls one visit makes. Failures are counted by these names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SweepCall {
    Metadata,
    GrepIndex,
    /// Garbage collection, which runs the core collector and then the grep index's collector
    /// through [`loonfs_http::gc_namespace`].
    Gc,
    /// The retention advance, which the full pass makes, or leaves to a
    /// visit that has the namespace when the pass reaches it.
    Retention,
    /// The acknowledgment of the retention floor, which the visit that
    /// makes the retention advance makes first.
    RetentionAcknowledgment,
}

impl SweepCall {
    pub(super) const ALL: [Self; 5] = [
        Self::Metadata,
        Self::GrepIndex,
        Self::Gc,
        Self::Retention,
        Self::RetentionAcknowledgment,
    ];

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::GrepIndex => "grep_index",
            Self::Gc => "gc",
            Self::Retention => "retention",
            Self::RetentionAcknowledgment => "retention_acknowledgment",
        }
    }

    /// The call's retry state on a held entry. The retention advance and
    /// the acknowledgment keep none, because ticks never retry them, so a
    /// failed one waits for the next full pass like a call on a namespace
    /// without a held entry.
    pub(super) fn retry(self, state: &SweepState) -> Option<SweepRetry> {
        match self {
            Self::Metadata => Some(state.metadata_retry),
            Self::GrepIndex => Some(state.index_retry),
            Self::Gc => Some(state.gc_retry),
            Self::Retention | Self::RetentionAcknowledgment => None,
        }
    }

    fn retry_mut(self, state: &mut SweepState) -> Option<&mut SweepRetry> {
        match self {
            Self::Metadata => Some(&mut state.metadata_retry),
            Self::GrepIndex => Some(&mut state.index_retry),
            Self::Gc => Some(&mut state.gc_retry),
            Self::Retention | Self::RetentionAcknowledgment => None,
        }
    }
}

impl Sweep {
    /// Whether `retry` is a call that keeps failing on a held namespace and
    /// whose backoff has reached [`MAX_SWEEP_BACKOFF_MS`]. An idle session
    /// doesn't wait for such a call before it closes. Below the cap, the
    /// session stays held so that ticks retry the call on its backoff,
    /// instead of the next full pass.
    pub(super) fn backoff_at_cap(&self, retry: SweepRetry) -> bool {
        retry.attempts > 0
            && backoff_ms(self.first_backoff_ms(), retry.attempts) >= MAX_SWEEP_BACKOFF_MS
    }

    /// The backoff after a call's first failure: one tick interval.
    fn first_backoff_ms(&self) -> u64 {
        self.inner.config.tick_interval_ms
    }

    /// Records how one call ended on a namespace, and returns the call's
    /// output when it succeeded. A failure counts in `sweep_visit_failures`,
    /// and no code ends the sweep's maintenance of a namespace.
    pub(super) fn record_call<T>(
        &self,
        namespace_id: &NamespaceId,
        entry: Option<&Mutex<HeldNamespace<SweepState>>>,
        call: SweepCall,
        result: Result<T, CallFailure>,
        work_wait_ms: u64,
    ) -> Option<T> {
        let (output, failure) = match result {
            Ok(output) => (Some(output), None),
            Err(failure) => (None, Some(failure).filter(CallFailure::counts)),
        };
        let retry =
            entry.and_then(|entry| self.schedule(entry, call, failure.is_some(), work_wait_ms));
        match (&failure, retry) {
            (Some(failure), retry) => self.record_failure(namespace_id, call, failure, retry),
            (None, Some(retry)) if output.is_some() && retry.failures_before > 0 => {
                tracing::info!(
                    namespace_id = %namespace_id,
                    call = call.as_str(),
                    attempts = retry.failures_before,
                    "maintenance sweep call succeeded after failing; its backoff is cleared"
                );
            }
            (None, _) => {}
        }
        output
    }

    /// Sets when the sweep next makes `call` on a held namespace, the one
    /// place that computes it. Returns `None` for a call that keeps no retry
    /// state.
    ///
    /// The call waits `work_wait_ms`, the wait its own work asks for. After
    /// a failure, it waits the later of that and its failure backoff
    /// ([`backoff_ms`]), so a failed metadata call, whose work remains, waits
    /// at least `maintenance_interval_ms`.
    fn schedule(
        &self,
        entry: &Mutex<HeldNamespace<SweepState>>,
        call: SweepCall,
        failed: bool,
        work_wait_ms: u64,
    ) -> Option<Retry> {
        let now_ms = self.inner.timer.monotonic_now_ms();
        let first_backoff_ms = self.first_backoff_ms();
        let mut held = lock(entry);
        let state = call.retry_mut(&mut held.host_state)?;
        let failures_before = state.attempts;
        let (attempts, wait_ms) = if failed {
            let attempts = failures_before.saturating_add(1);
            let backoff_ms = backoff_ms(first_backoff_ms, attempts);
            (attempts, work_wait_ms.max(backoff_ms))
        } else {
            (0, work_wait_ms)
        };
        *state = SweepRetry {
            retry_at_ms: now_ms.saturating_add(wait_ms),
            attempts,
        };
        Some(Retry {
            failures_before,
            attempts,
            wait_ms,
        })
    }

    /// Counts a failed call, and logs it with where the call stands on a
    /// held namespace.
    ///
    /// On a held namespace, the first failure in a row logs at error level
    /// and later ones at warning level, and the doubling backoff limits how
    /// often they repeat. A namespace that no host keeps open has no retry
    /// state, and neither do the retention advance and its acknowledgment,
    /// because the next full pass is their retry. Each full pass that finds
    /// one failing logs one error line, so the namespace logs once per
    /// `full_sweep_interval_ms` (a day by default). The rule of logging a
    /// repeating failure once allows one line per interval, so a failure
    /// that's retried once a day can log on every retry. Every panic logs
    /// at error level.
    fn record_failure(
        &self,
        namespace_id: &NamespaceId,
        call: SweepCall,
        failure: &CallFailure,
        retry: Option<Retry>,
    ) {
        let metrics = &self.inner.metrics;
        metrics.visit_failures[call as usize].increment(1);
        let code = failure.code();
        let attempts = retry.map(|retry| retry.attempts);
        let retry_after_ms = retry.map(|retry| retry.wait_ms);
        match failure {
            CallFailure::Panic { panic } => {
                metrics.panics.increment(1);
                tracing::error!(
                    namespace_id = %namespace_id,
                    call = call.as_str(),
                    code = %code,
                    attempts,
                    retry_after_ms,
                    panic = panic.as_str(),
                    "maintenance sweep call panicked; the sweep retries it"
                );
            }
            CallFailure::Error { error, .. } if attempts.is_none_or(|attempts| attempts == 1) => {
                tracing::error!(
                    namespace_id = %namespace_id,
                    call = call.as_str(),
                    code = %code,
                    attempts,
                    retry_after_ms,
                    error = error.as_str(),
                    "maintenance sweep call failed; the sweep retries it"
                );
            }
            CallFailure::Error { error, .. } => tracing::warn!(
                namespace_id = %namespace_id,
                call = call.as_str(),
                code = %code,
                attempts,
                retry_after_ms,
                error = error.as_str(),
                "maintenance sweep call failed again; the sweep retries it"
            ),
        }
    }
}

/// The failure backoff after the `attempts`th failure in a row:
/// `first_backoff_ms`, doubled for each failure after the first, at most
/// [`MAX_SWEEP_BACKOFF_MS`].
pub(super) fn backoff_ms(first_backoff_ms: u64, attempts: u32) -> u64 {
    let doublings = attempts.saturating_sub(1);
    1_u64
        .checked_shl(doublings)
        .map_or(u64::MAX, |factor| first_backoff_ms.saturating_mul(factor))
        .min(MAX_SWEEP_BACKOFF_MS)
}
