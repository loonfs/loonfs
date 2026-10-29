//! The first due step for each held session and the two host intervals.

use super::{lock, GarbageCollection, Overlap, RunningSweep, Sweep, SweepCall};
use futures::{FutureExt as _, StreamExt as _};
use loonfs::{ChangeSeq, NamespaceId};
use loonfs_http::panic_message;
use std::panic::AssertUnwindSafe;
use std::time::Duration;
use tokio::time::MissedTickBehavior;

enum Due {
    Metadata,
    Index,
    Collection,
    Close,
}

impl Sweep {
    /// Runs at most one due step per held session without cloning writer handles.
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

    /// Runs the session's first due step, then the retention advance that a
    /// full pass left to this visit, if it left one.
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

    async fn run_due_step(&self, namespace_id: &NamespaceId) {
        let inner = &self.inner;
        let now_ms = inner.namespaces.now_ms();
        let Some(entry) = inner.namespaces.entry(namespace_id) else {
            return;
        };
        let (seq, due) = {
            let held = lock(&entry);
            let seq = held.handle.last_published_seq();
            if held
                .handle
                .last_published_at_ms()
                .is_some_and(|published_at_ms| {
                    now_ms.saturating_sub(published_at_ms) < inner.config.tick_interval_ms
                })
            {
                return;
            }
            let metadata_caught_up = held.handle.metadata_caught_up();
            let metadata_retry = held.metadata_retry;
            // The idle fold allows one metadata call before the retry time,
            // when no call has run since the tail reached the idle fold age.
            // While the call keeps failing, its retry time also includes its
            // backoff, which the idle fold doesn't cut short.
            let idle_fold_due = metadata_retry.attempts == 0
                && held
                    .handle
                    .idle_fold_due_at_ms()
                    .is_some_and(|fold_due_at_ms| {
                        now_ms >= fold_due_at_ms
                            && metadata_retry
                                .retry_at_ms
                                .saturating_sub(inner.config.maintenance_interval_ms)
                                < fold_due_at_ms
                    });
            let metadata_due =
                !metadata_caught_up && (now_ms >= metadata_retry.retry_at_ms || idle_fold_due);
            let due = if metadata_due {
                Due::Metadata
            } else if inner.grep.is_some()
                && held.index_pending(seq)
                && now_ms >= held.index_retry.retry_at_ms
            {
                Due::Index
            } else if seq.is_some()
                && held.collection_pending(seq, now_ms, inner.config.gc_interval_ms)
                && now_ms >= held.gc_retry.retry_at_ms
            {
                Due::Collection
            } else if metadata_caught_up || self.backoff_at_cap(metadata_retry) {
                Due::Close
            } else {
                return;
            };
            (seq, due)
        };
        match due {
            Due::Metadata => {
                self.maintain_metadata(namespace_id, Some(&entry), seq)
                    .await;
            }
            Due::Index => {
                if let Some(grep) = &inner.grep {
                    self.maintain_index(grep, namespace_id, Some(&entry), seq)
                        .await;
                }
            }
            Due::Collection => self.gc(namespace_id, Some(&entry), seq).await,
            Due::Close => {
                let close = AssertUnwindSafe(self.close_idle_session(namespace_id, seq));
                if let Err(payload) = close.catch_unwind().await {
                    inner.metrics.panics.increment(1);
                    tracing::error!(
                        namespace_id = %namespace_id,
                        call = "close",
                        panic = panic_message(payload.as_ref()),
                        "closing an idle session panicked; the next tick tries again"
                    );
                }
            }
        }
    }

    async fn close_idle_session(&self, namespace_id: &NamespaceId, seq: Option<ChangeSeq>) {
        let inner = &self.inner;
        if inner.stop.is_cancelled() {
            return;
        }
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
                        call.retry(held)
                            .is_some_and(|retry| self.backoff_at_cap(retry))
                    };
                    (held.handle.metadata_caught_up() || at_cap(SweepCall::Metadata))
                        && (inner.grep.is_none()
                            || !held.index_pending(seq)
                            || at_cap(SweepCall::GrepIndex))
                        && (!held.collection_pending(
                            seq,
                            inner.namespaces.now_ms(),
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
