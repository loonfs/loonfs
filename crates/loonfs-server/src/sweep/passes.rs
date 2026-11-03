//! The steps that a tick runs for each held session, and the two host
//! intervals.

use super::retries::SweepCall;
use super::visit::Overlap;
use super::{index_pending, lock, GarbageCollection, RunningSweep, Sweep, SweepState};
use futures::{FutureExt as _, StreamExt as _};
use loonfs::{
    ChangeSeq, ErrorCode, MetadataMaintenanceOutcome, NamespaceId, NamespaceSessionStatus,
};
use loonfs_http::{panic_message, HeldNamespace};
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tokio::time::MissedTickBehavior;

/// The step that a tick runs after the session's metadata step.
enum Due {
    Collection,
    Index,
    Close(Close),
}

/// Why a tick closes a session.
#[derive(Debug, Clone, Copy)]
enum Close {
    /// The session is idle, and nothing else is due. `metadata_settled` is
    /// whether the session's metadata needs no more ticks: this tick's
    /// metadata step found it caught up, or the session has no current
    /// writer position, so the daily pass maintains its metadata.
    Idle { metadata_settled: bool },
    /// The session can't maintain its namespace any more, because it was
    /// closed or fenced, or its namespace is gone.
    Ended,
}

impl Sweep {
    /// Runs the due steps of each held session, without cloning writer
    /// handles.
    pub async fn tick(&self) {
        let inner = &self.inner;
        let mut after = None;
        let held = std::iter::from_fn(|| {
            let namespace_id = inner.namespaces.next_held_id(after.as_ref())?;
            after = Some(namespace_id.clone());
            Some(namespace_id)
        });
        futures::stream::iter(held)
            .take_until(inner.stop.cancelled())
            .for_each_concurrent(
                inner.config.max_concurrent_maintenance.get(),
                |namespace_id| self.tick_session(namespace_id).boxed(),
            )
            .await;
    }

    /// Runs the session's due steps, then the retention advance that a full
    /// pass left to this visit, if it left one.
    async fn tick_session(&self, namespace_id: NamespaceId) {
        let Some((visit, _permit)) = self.claim(&namespace_id, Overlap::Skip).await else {
            return;
        };
        // Boxed for the same reason as the retention advance: the spawned
        // sweep's `Send` check overflows on the unboxed due step inside this
        // future.
        self.run_due_step(&namespace_id).boxed().await;
        self.finish_visit(visit).await;
    }

    /// Runs the session's metadata step, and then the first due step of
    /// collection, index, and close.
    ///
    /// The metadata step runs when the session has a current writer
    /// position and the step's retry time has come, and once when the
    /// session's tail reaches its idle fold age, whatever the position. A
    /// session without a current position isn't writing, so otherwise the
    /// daily pass maintains its namespace's metadata, as it does for a
    /// namespace without a session. Collection comes before the index step
    /// because it's due at most once per `gc_interval_ms`, and a session
    /// with a commit on every tick has index work on every tick. A session
    /// that was closed or fenced, or whose namespace is gone, closes as soon
    /// as no caller has its handle.
    async fn run_due_step(&self, namespace_id: &NamespaceId) {
        let inner = &self.inner;
        let Some(entry) = inner.namespaces.entry(namespace_id) else {
            return;
        };
        let (seq, open, positioned, (retry_due, idle_fold_due)) = {
            let held = lock(&entry);
            (
                held.handle.last_published_seq(),
                held.handle.session_status() == NamespaceSessionStatus::Open,
                held.handle.has_current_writer_position(),
                self.metadata_due(&held, inner.timer.monotonic_now_ms()),
            )
        };
        let metadata = if open && (idle_fold_due || positioned && retry_due) {
            Some(self.maintain_metadata(namespace_id, Some(&entry)).await)
        } else {
            None
        };
        let due = match metadata {
            _ if !open => Due::Close(Close::Ended),
            Some(Err(ErrorCode::NamespaceDeleted | ErrorCode::NamespaceNotFound)) => {
                Due::Close(Close::Ended)
            }
            metadata => {
                let metadata_settled = metadata.map_or(!positioned, |answer| {
                    answer == Ok(MetadataMaintenanceOutcome::CaughtUp)
                });
                let held = lock(&entry);
                let now_ms = inner.timer.monotonic_now_ms();
                if seq.is_some()
                    && held
                        .host_state
                        .collection_pending(seq, now_ms, inner.config.gc_interval_ms)
                    && now_ms >= held.host_state.gc_retry.retry_at_ms
                {
                    Due::Collection
                } else if inner.grep.is_some()
                    && index_pending(&held, seq)
                    && now_ms >= held.host_state.index_retry.retry_at_ms
                {
                    Due::Index
                } else if metadata_settled || self.backoff_at_cap(held.host_state.metadata_retry) {
                    Due::Close(Close::Idle { metadata_settled })
                } else {
                    return;
                }
            }
        };
        // A collection or an index step that finds the namespace gone ends
        // the session, as the metadata step does.
        let close = match due {
            Due::Collection => self
                .gc(namespace_id, Some(&entry), seq)
                .await
                .then_some(Close::Ended),
            Due::Index => match &inner.grep {
                Some(grep) => self
                    .maintain_index(grep, namespace_id, Some(&entry), seq)
                    .await
                    .then_some(Close::Ended),
                None => None,
            },
            Due::Close(close) => Some(close),
        };
        if let Some(close) = close {
            let closing = AssertUnwindSafe(self.close_session(namespace_id, seq, close));
            if let Err(payload) = closing.catch_unwind().await {
                inner.metrics.panics.increment(1);
                tracing::error!(
                    namespace_id = %namespace_id,
                    call = "close",
                    panic = panic_message(payload.as_ref()),
                    "closing a session panicked; the next tick tries again"
                );
            }
        }
    }

    /// Returns whether the session's metadata step's retry time has come,
    /// and whether the session's tail has reached its idle fold age with no
    /// step since.
    fn metadata_due(&self, held: &HeldNamespace<SweepState>, now_ms: u64) -> (bool, bool) {
        let interval_ms = self.inner.config.maintenance_interval_ms;
        let metadata_retry = held.host_state.metadata_retry;
        // The idle fold allows one metadata step before the retry time, when
        // no step has run since the tail reached the idle fold age. A step
        // that ended without a current position sets its retry time an
        // interval later, so the idle fold of a session without a position
        // runs once. While the step keeps failing, its retry time also
        // includes its backoff, which the idle fold doesn't cut short.
        let idle_fold_due = metadata_retry.attempts == 0
            && held
                .handle
                .idle_fold_due_at_ms()
                .is_some_and(|fold_due_at_ms| {
                    now_ms >= fold_due_at_ms
                        && metadata_retry.retry_at_ms.saturating_sub(interval_ms) < fold_due_at_ms
                });
        (now_ms >= metadata_retry.retry_at_ms, idle_fold_due)
    }

    /// Closes the session if no caller has its handle and, for
    /// [`Close::Idle`], it's still idle with nothing due.
    async fn close_session(
        &self,
        namespace_id: &NamespaceId,
        seq: Option<ChangeSeq>,
        close: Close,
    ) {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return;
        }
        let closed = match close {
            Close::Ended => {
                inner
                    .namespaces
                    .close_if_idle(namespace_id, seq, 0, |_| true)
                    .await
            }
            Close::Idle { metadata_settled } => {
                inner
                    .namespaces
                    .close_if_idle(
                        namespace_id,
                        seq,
                        inner.config.idle_session_close_after_ms,
                        |held| {
                            // A failing call keeps an idle session held, so
                            // ticks retry it on its backoff, until its
                            // backoff reaches the cap. Then the session
                            // closes and the next full pass retries the call.
                            let at_cap = |call: SweepCall| {
                                call.retry(&held.host_state)
                                    .is_some_and(|retry| self.backoff_at_cap(retry))
                            };
                            (metadata_settled || at_cap(SweepCall::Metadata))
                                && (inner.grep.is_none()
                                    || !index_pending(held, seq)
                                    || at_cap(SweepCall::GrepIndex))
                                && (!held.host_state.collection_pending(
                                    seq,
                                    inner.timer.monotonic_now_ms(),
                                    inner.config.gc_interval_ms,
                                ) || at_cap(SweepCall::Gc))
                        },
                    )
                    .await
            }
        };
        if let Err(error) = closed {
            tracing::warn!(%namespace_id, error = %error, "session close failed");
        }
    }

    pub(crate) fn start(&self) -> RunningSweep {
        let inner = &self.inner;
        let config = &inner.config;
        tracing::info!(
            tick_interval_ms = config.tick_interval_ms,
            maintenance_interval_ms = config.maintenance_interval_ms,
            gc_interval_ms = config.gc_interval_ms,
            full_sweep_interval_ms = config.full_sweep_interval_ms,
            retention_window_ms = config.retention_window_ms.map(std::num::NonZeroU64::get),
            max_concurrent_maintenance = config.max_concurrent_maintenance.get(),
            maintains_grep_index = inner.grep.is_some(),
            "maintenance sweep started"
        );
        let sweep = self.clone();
        RunningSweep {
            stop: inner.stop.clone(),
            task: tokio::spawn(async move {
                tokio::join!(sweep.run_passes(), sweep.run_ticks());
            }),
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the configured full-pass interval, so time paces collection passes and decides no outcome"
    )]
    async fn run_passes(&self) {
        let inner = &self.inner;
        let mut ticks =
            tokio::time::interval(Duration::from_millis(inner.config.full_sweep_interval_ms));
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = inner.stop.cancelled() => return,
                _ = ticks.tick() => {}
            }
            let _ = self.run_pass(GarbageCollection::Collect).await;
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the configured session tick interval, so time paces session ticks and decides no outcome"
    )]
    async fn run_ticks(&self) {
        let inner = &self.inner;
        let mut ticks = tokio::time::interval(Duration::from_millis(inner.config.tick_interval_ms));
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = inner.stop.cancelled() => return,
                _ = ticks.tick() => {}
            }
            self.tick().await;
        }
    }
}
