//! The steps that the scheduler runs for each held session, and the two
//! host intervals.

use super::calls::Overlap;
use super::retries::{CallFailure, MaintenanceCall};
use super::{contain, lock, RunningScheduler, Scheduler, SchedulerState};
use crate::HeldNamespace;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use loonfs::{
    CommitSeq, ErrorCode, MetadataMaintenanceOutcome, NamespaceId, NamespaceSessionStatus,
};
use std::collections::HashSet;
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
        held_ids(&self.inner.namespaces)
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
    /// fold age that its last step reported, whatever the position (see
    /// [`metadata_due`]). A session without a position isn't writing,
    /// so otherwise the sweep maintains its namespace's metadata, as it does
    /// for a namespace without a session. Collection comes before the index
    /// step because it's due at most once per `gc_interval_ms`, and a session
    /// with a commit at every interval has index work at every interval. A
    /// session that was closed or fenced, or whose namespace is gone, closes as
    /// soon as no caller has its handle. The metadata and collection steps
    /// run on the session's writable handle, which the step clones and drops
    /// before the close step.
    ///
    /// A close that fails or panics counts as a failed `close` call, and the
    /// next maintenance interval tries again.
    async fn run_session_steps(&self, namespace_id: &NamespaceId) {
        let inner = &self.inner;
        let Some(entry) = inner.namespaces.entry(namespace_id) else {
            return;
        };
        let (writer, seq, open, positioned, metadata_due) = {
            let held = lock(&entry);
            let positioned = held.handle.has_current_writer_position();
            (
                held.handle.clone(),
                held.handle.last_published_seq(),
                held.handle.session_status() == NamespaceSessionStatus::Open,
                positioned,
                metadata_due(&held, positioned, inner.timer.monotonic_now_ms()),
            )
        };
        let metadata = if open && metadata_due {
            Some(
                self.maintain_metadata(&writer, namespace_id, Some(&entry))
                    .await,
            )
        } else {
            None
        };
        // The close step checks that the table's handle is the only one.
        drop(writer);
        let due = match metadata {
            _ if !open => Due::Close(Close::Ended),
            Some(Err(code)) if code.is_namespace_gone() => Due::Close(Close::Ended),
            metadata => {
                let metadata_settled = metadata.map_or(!positioned, |answer| {
                    answer == Ok(MetadataMaintenanceOutcome::CaughtUp)
                });
                let held = lock(&entry);
                let now_ms = inner.timer.monotonic_now_ms();
                if self.collection_pending(&held, seq)
                    && now_ms >= held.host_state.gc_retry.retry_at_ms
                {
                    Due::Collection
                } else if self.index_pending(&held, seq)
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
                .gc(&inner.maintenance, namespace_id, Some(&entry), seq)
                .await
                .is_err_and(ErrorCode::is_namespace_gone)
                .then_some(Close::Ended),
            Due::Index => match &inner.grep {
                Some(grep) => {
                    let writer = lock(&entry).handle.clone();
                    self.maintain_index(grep, &writer, namespace_id, Some(&entry), seq)
                        .await
                        .then_some(Close::Ended)
                }
                None => None,
            },
            Due::Close(close) => Some(close),
        };
        if let Some(close) = close {
            let result = contain(
                self.close_session(namespace_id, seq, close)
                    .map(|result| result.map_err(|error| CallFailure::error(error.code(), error))),
            )
            .await;
            self.record_call(
                namespace_id,
                Some(&entry),
                MaintenanceCall::Close,
                result,
                0,
            );
        }
    }

    /// Whether a collection step is pending for a held session at `seq`:
    /// this host sweeps the store, the session has published, its seq moved
    /// since the last collection, and `gc_interval_ms` passed since that
    /// collection.
    fn collection_pending(
        &self,
        held: &HeldNamespace<SchedulerState>,
        seq: Option<CommitSeq>,
    ) -> bool {
        let inner = &self.inner;
        inner.config.sweep_store
            && seq.is_some()
            && held.host_state.collection_pending(
                seq,
                inner.timer.monotonic_now_ms(),
                inner.config.gc_interval_ms.get(),
            )
    }

    /// Whether an index step is pending for a held session at `seq`: this
    /// host builds the grep index, and the index is marked dirty or the
    /// scheduler last brought it up to date at another seq.
    fn index_pending(&self, held: &HeldNamespace<SchedulerState>, seq: Option<CommitSeq>) -> bool {
        self.inner.grep.is_some() && (held.index_dirty || held.host_state.indexed_seq != seq)
    }

    /// Closes the session if no caller has its handle and, for
    /// [`Close::Idle`], it's still idle with nothing due.
    async fn close_session(
        &self,
        namespace_id: &NamespaceId,
        seq: Option<CommitSeq>,
        close: Close,
    ) -> loonfs::Result<()> {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return Ok(());
        }
        match close {
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
                        inner.config.idle_session_close_after_ms.get(),
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
                                && (!self.index_pending(held, seq)
                                    || at_cap(MaintenanceCall::GrepIndex))
                                && (!self.collection_pending(held, seq)
                                    || at_cap(MaintenanceCall::Gc))
                        },
                    )
                    .await
            }
        }
        .map(|_| ())
    }

    /// Starts the scheduler's two loops on the current Tokio runtime: a sweep
    /// at start and then every `sweep_interval_ms`, and the due steps of each
    /// held session every `maintenance_interval_ms`. The reference server
    /// starts them when it serves.
    pub fn start(&self) -> RunningScheduler {
        let inner = &self.inner;
        let config = &inner.config;
        tracing::info!(
            maintenance_interval_ms = config.maintenance_interval_ms.get(),
            metadata_retry_interval_ms = config.metadata_retry_interval_ms.get(),
            gc_interval_ms = config.gc_interval_ms.get(),
            sweep_interval_ms = config.sweep_interval_ms.get(),
            max_concurrent_maintenance = config.max_concurrent_maintenance.get(),
            sweep_store = config.sweep_store,
            builds_grep_index = inner.grep.is_some(),
            "maintenance scheduler started"
        );
        let scheduler = self.clone();
        RunningScheduler {
            scheduler: self.clone(),
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
            tokio::time::interval(Duration::from_millis(inner.config.sweep_interval_ms.get()));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = inner.stop.cancelled() => return,
                _ = interval.tick() => {}
            }
            self.run_sweep().await;
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the configured maintenance interval, so time paces session steps and decides no outcome"
    )]
    async fn run_sessions(&self) {
        let inner = &self.inner;
        let mut interval = tokio::time::interval(Duration::from_millis(
            inner.config.maintenance_interval_ms.get(),
        ));
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

/// Walks the IDs of the held sessions, without cloning writer handles.
pub(super) fn held_ids(
    namespaces: &crate::Namespaces<SchedulerState>,
) -> impl Iterator<Item = NamespaceId> + '_ {
    let mut after = None;
    std::iter::from_fn(move || {
        let namespace_id = namespaces.next_held_id(after.as_ref())?;
        after = Some(namespace_id.clone());
        Some(namespace_id)
    })
}

/// Returns whether a held session's metadata step is due at `now_ms`: its
/// retry time has come, and the session has a writer position, or
/// its tail has reached the fold age that its last step reported, or
/// the session has committed since its last step. A step without a position
/// reads the store, so it runs only for that fold, or once to date a
/// tail that no step has seen, and its own report replaces the due time.
fn metadata_due(held: &HeldNamespace<SchedulerState>, positioned: bool, now_ms: u64) -> bool {
    let state = &held.host_state;
    now_ms >= state.metadata_retry.retry_at_ms
        && (positioned
            || held.handle.last_published_seq() != state.last_step_seq
            || state
                .fold_due_at_ms
                .is_some_and(|due_at_ms| now_ms >= due_at_ms))
}
