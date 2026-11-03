//! The metadata call and the first due step for each held session, and the
//! two host intervals.

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

/// The step that a tick runs after it asks the session for its metadata
/// work.
enum Due {
    Collection,
    Index,
    Close(Close),
}

/// Why a tick closes a session.
#[derive(Debug, Clone, Copy)]
enum Close {
    /// The session has been idle long enough, and nothing is due.
    /// `metadata_caught_up` is whether the session's metadata call on this
    /// tick found its metadata caught up.
    Idle { metadata_caught_up: bool },
    /// The session can't maintain its namespace any more, because another
    /// writer fenced it or the namespace is gone. `code` is the error that
    /// says so.
    Ended { code: ErrorCode },
}

impl Sweep {
    /// Asks each held session for its metadata work and runs at most one other
    /// due step for it, without cloning writer handles.
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

    /// Runs the session's metadata call and first due step, then the
    /// retention advance that a full pass left to this visit, if it left one.
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

    /// Asks the session for its metadata work, unless the last call's
    /// outcome or failure set a retry time that hasn't come, and then runs
    /// the first due step of collection, index, and close. Collection comes
    /// first because it's due at most once per `gc_interval_ms`, and a
    /// session with a commit on every tick has index work on every tick.
    ///
    /// The session closes after its metadata call on this tick finds its
    /// metadata caught up, or once its failing metadata call's backoff
    /// reaches the cap. A fenced session, or the session of a namespace that
    /// is gone, closes as soon as no caller has its handle, and until then
    /// the ticks don't ask a fenced session for metadata work.
    async fn run_due_step(&self, namespace_id: &NamespaceId) {
        let inner = &self.inner;
        let Some(entry) = inner.namespaces.entry(namespace_id) else {
            return;
        };
        let (seq, fenced, ask_metadata) = {
            let held = lock(&entry);
            let now_ms = inner.timer.monotonic_now_ms();
            (
                held.handle.last_published_seq(),
                held.handle.session_status() == NamespaceSessionStatus::Fenced,
                self.metadata_retry_due(&held, now_ms),
            )
        };
        // A tick that doesn't ask leaves the answer of an earlier call, which
        // left work due or failed.
        let metadata = if fenced {
            Err(ErrorCode::WriterFenced)
        } else if ask_metadata {
            self.maintain_metadata(namespace_id, Some(&entry)).await
        } else {
            Ok(MetadataMaintenanceOutcome::Due)
        };
        let due = match metadata {
            Err(
                code @ (ErrorCode::WriterFenced
                | ErrorCode::NamespaceDeleted
                | ErrorCode::NamespaceNotFound),
            ) => Due::Close(Close::Ended { code }),
            answer => {
                let metadata_caught_up = answer == Ok(MetadataMaintenanceOutcome::CaughtUp);
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
                } else if metadata_caught_up || self.backoff_at_cap(held.host_state.metadata_retry)
                {
                    Due::Close(Close::Idle { metadata_caught_up })
                } else {
                    return;
                }
            }
        };
        match due {
            Due::Collection => self.gc(namespace_id, Some(&entry), seq).await,
            Due::Index => {
                if let Some(grep) = &inner.grep {
                    self.maintain_index(grep, namespace_id, Some(&entry), seq)
                        .await;
                }
            }
            Due::Close(close) => {
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
    }

    /// Whether a tick asks the session for its metadata work, because the
    /// retry time that the last call's outcome or failure set has come, or
    /// because the session's tail has just reached its idle fold age.
    fn metadata_retry_due(&self, held: &HeldNamespace<SweepState>, now_ms: u64) -> bool {
        let interval_ms = self.inner.config.maintenance_interval_ms;
        let metadata_retry = held.host_state.metadata_retry;
        // The idle fold allows one metadata call before the retry time, when
        // no call has run since the tail reached the idle fold age. While the
        // call keeps failing, its retry time also includes its backoff, which
        // the idle fold doesn't cut short.
        let idle_fold_due = metadata_retry.attempts == 0
            && held
                .handle
                .idle_fold_due_at_ms()
                .is_some_and(|fold_due_at_ms| {
                    now_ms >= fold_due_at_ms
                        && metadata_retry.retry_at_ms.saturating_sub(interval_ms) < fold_due_at_ms
                });
        now_ms >= metadata_retry.retry_at_ms || idle_fold_due
    }

    /// Closes the session if no caller has its handle and, for `Close::Idle`,
    /// it's still idle with nothing due.
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
        let metadata_caught_up = match close {
            Close::Idle { metadata_caught_up } => metadata_caught_up,
            Close::Ended { code } => {
                match inner
                    .namespaces
                    .close_if_idle(namespace_id, seq, 0, |_| true)
                    .await
                {
                    Ok(Some(_)) => tracing::info!(
                        %namespace_id,
                        %code,
                        "closed a writer session that can't maintain its namespace"
                    ),
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(%namespace_id, error = %error, "session close failed");
                    }
                }
                return;
            }
        };
        if let Err(error) = inner
            .namespaces
            .close_if_idle(
                namespace_id,
                seq,
                inner.config.idle_session_close_after_ms,
                |held| {
                    // A failing call keeps an idle session held, so ticks
                    // retry it on its backoff, until its backoff reaches the
                    // cap. Then the session closes and the next full pass
                    // retries the call.
                    let at_cap = |call: SweepCall| {
                        call.retry(&held.host_state)
                            .is_some_and(|retry| self.backoff_at_cap(retry))
                    };
                    (metadata_caught_up || at_cap(SweepCall::Metadata))
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
        {
            tracing::warn!(%namespace_id, error = %error, "idle session close failed");
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
