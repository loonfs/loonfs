//! The steps that the scheduler runs for each held session, and the two
//! host intervals.

use super::calls::Overlap;
use super::retries::MaintenanceCall;
use super::{index_pending, lock, RunningScheduler, Scheduler, SchedulerState};
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use loonfs::{CommitSeq, MetadataMaintenanceOutcome, NamespaceId, NamespaceSessionStatus};
use loonfs_http::{panic_message, HeldNamespace};
use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tokio::time::MissedTickBehavior;

/// The step that the scheduler runs after the session's metadata step.
enum Due {
    Collection,
    Index,
    Close(Close),
}

/// Why the scheduler closes a session.
#[derive(Debug, Clone, Copy)]
enum Close {
    /// The session is idle, and nothing else is due. `metadata_settled` is
    /// whether the session's metadata needs no more steps: the metadata step
    /// in the same run found it caught up, or the session has no current
    /// writer position, so the sweep maintains its metadata.
    Idle { metadata_settled: bool },
    /// The session can't maintain its namespace any more, because it was
    /// closed or fenced, or its namespace is gone.
    Ended,
}

impl Scheduler {
    /// Runs the due steps of each held session once, without cloning writer
    /// handles. Returns when the last session's steps end. The scheduler's
    /// own loop doesn't wait for one interval's steps before it starts the
    /// next interval's.
    pub async fn run_due_steps(&self) {
        let inner = &self.inner;
        futures::stream::iter(self.held_ids())
            .take_until(inner.stop.cancelled())
            .for_each_concurrent(
                inner.config.max_concurrent_maintenance.get(),
                |namespace_id| self.run_session(namespace_id).boxed(),
            )
            .await;
    }

    /// Walks the IDs of the held sessions, without cloning writer handles.
    fn held_ids(&self) -> impl Iterator<Item = NamespaceId> + '_ {
        let namespaces = &self.inner.namespaces;
        let mut after = None;
        std::iter::from_fn(move || {
            let namespace_id = namespaces.next_held_id(after.as_ref())?;
            after = Some(namespace_id.clone());
            Some(namespace_id)
        })
    }

    /// Runs the session's due steps, then the retention advance that a sweep
    /// left to these steps, if it left one.
    async fn run_session(&self, namespace_id: NamespaceId) {
        let Some((claim, _permit)) = self.claim(&namespace_id, Overlap::Skip).await else {
            return;
        };
        // Boxed for the same reason as the retention advance: the spawned
        // scheduler's `Send` check overflows on the unboxed due step inside
        // this future.
        self.run_session_steps(&namespace_id).boxed().await;
        self.finish_claim(claim).await;
    }

    /// Runs the session's metadata step, and then the first due step of
    /// collection, index, and close.
    ///
    /// The metadata step runs when its retry time has come and the session has
    /// a writer position, and once when the session's tail reaches the
    /// idle fold age that its last step reported, whatever the position (see
    /// [`metadata_due`]). A session without a position isn't writing,
    /// so otherwise the sweep maintains its namespace's metadata, as it does
    /// for a namespace without a session. Collection comes before the index
    /// step because it's due at most once per `gc_interval_ms`, and a session
    /// with a commit at every interval has index work at every interval. A
    /// session that was closed or fenced, or whose namespace is gone, closes as
    /// soon as no caller has its handle.
    async fn run_session_steps(&self, namespace_id: &NamespaceId) {
        let inner = &self.inner;
        let Some(entry) = inner.namespaces.entry(namespace_id) else {
            return;
        };
        let (seq, open, positioned, metadata_due) = {
            let held = lock(&entry);
            let positioned = held.handle.has_current_writer_position();
            (
                held.handle.last_published_seq(),
                held.handle.session_status() == NamespaceSessionStatus::Open,
                positioned,
                metadata_due(&held, positioned, inner.timer.monotonic_now_ms()),
            )
        };
        let metadata = if open && metadata_due {
            Some(self.maintain_metadata(namespace_id, Some(&entry)).await)
        } else {
            None
        };
        let due = match metadata {
            _ if !open => Due::Close(Close::Ended),
            Some(Err(code)) if code.is_namespace_gone() => Due::Close(Close::Ended),
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
                    "closing a session panicked; the next maintenance interval tries again"
                );
            }
        }
    }

    /// Closes the session if no caller has its handle and, for
    /// [`Close::Idle`], it's still idle with nothing due.
    async fn close_session(
        &self,
        namespace_id: &NamespaceId,
        seq: Option<CommitSeq>,
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
                            // A failing step keeps an idle session held, so
                            // the scheduler retries the step on its backoff,
                            // until its backoff reaches the cap. Then the
                            // session closes and the next sweep retries the
                            // step.
                            let at_cap = |call: MaintenanceCall| {
                                call.retry(&held.host_state)
                                    .is_some_and(|retry| self.backoff_at_cap(retry))
                            };
                            (metadata_settled || at_cap(MaintenanceCall::Metadata))
                                && (inner.grep.is_none()
                                    || !index_pending(held, seq)
                                    || at_cap(MaintenanceCall::GrepIndex))
                                && (!held.host_state.collection_pending(
                                    seq,
                                    inner.timer.monotonic_now_ms(),
                                    inner.config.gc_interval_ms,
                                ) || at_cap(MaintenanceCall::Gc))
                        },
                    )
                    .await
            }
        };
        if let Err(error) = closed {
            tracing::warn!(%namespace_id, error = %error, "session close failed");
        }
    }

    pub(crate) fn start(&self) -> RunningScheduler {
        let inner = &self.inner;
        let config = &inner.config;
        tracing::info!(
            maintenance_interval_ms = config.maintenance_interval_ms,
            metadata_retry_interval_ms = config.metadata_retry_interval_ms,
            gc_interval_ms = config.gc_interval_ms,
            sweep_interval_ms = config.sweep_interval_ms,
            max_concurrent_maintenance = config.max_concurrent_maintenance.get(),
            maintains_grep_index = inner.grep.is_some(),
            "maintenance scheduler started"
        );
        let scheduler = self.clone();
        RunningScheduler {
            stop: inner.stop.clone(),
            task: tokio::spawn(async move {
                tokio::join!(scheduler.run_sweeps(), scheduler.run_sessions());
            }),
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the configured sweep interval, so time paces sweeps and decides no outcome"
    )]
    async fn run_sweeps(&self) {
        let inner = &self.inner;
        let mut interval =
            tokio::time::interval(Duration::from_millis(inner.config.sweep_interval_ms));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = inner.stop.cancelled() => return,
                _ = interval.tick() => {}
            }
            let _ = self.run_sweep().await;
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the configured maintenance interval, so time paces session steps and decides no outcome"
    )]
    async fn run_sessions(&self) {
        let inner = &self.inner;
        let mut interval =
            tokio::time::interval(Duration::from_millis(inner.config.maintenance_interval_ms));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Each interval starts the steps of every held session that has no
        // steps still pending or running, so a session runs one set of steps
        // at a time and the pending runs never outnumber the held sessions.
        // The pending runs are polled before an interval starts more. The
        // claim skips a session that a maintenance call or a sweep has.
        let mut runs = FuturesUnordered::new();
        let mut running = HashSet::new();
        loop {
            tokio::select! {
                biased;
                () = inner.stop.cancelled() => break,
                Some(namespace_id) = runs.next() => {
                    running.remove(&namespace_id);
                }
                _ = interval.tick() => {
                    for namespace_id in self.held_ids() {
                        if running.insert(namespace_id.clone()) {
                            let run = self.run_session(namespace_id.clone());
                            runs.push(run.map(move |()| namespace_id).boxed());
                        }
                    }
                }
            }
        }
        // The running steps see the stop at their next check and return.
        while runs.next().await.is_some() {}
    }
}

/// Returns whether a held session's metadata step is due at `now_ms`: its
/// retry time has come, and the session has a writer position, or
/// its tail has reached the idle fold age that its last step reported, or
/// the session has committed since its last step. A step without a position
/// reads the store, so it runs only for that idle fold, or once to date a
/// tail that no step has seen, and its own report replaces the due time.
fn metadata_due(held: &HeldNamespace<SchedulerState>, positioned: bool, now_ms: u64) -> bool {
    let state = &held.host_state;
    now_ms >= state.metadata_retry.retry_at_ms
        && (positioned
            || held.handle.last_published_seq() != state.last_step_seq
            || state
                .idle_fold_due_at_ms
                .is_some_and(|due_at_ms| now_ms >= due_at_ms))
}
