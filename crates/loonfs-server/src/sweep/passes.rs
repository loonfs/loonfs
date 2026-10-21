//! The first due step for each held session and the two host intervals.

use super::{lock, panic_message, GarbageCollection, RunningSweep, Sweep, SweepCall};
use futures::{FutureExt as _, StreamExt as _};
use loonfs::{ChangeSeq, NamespaceId};
use std::panic::AssertUnwindSafe;
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
            .for_each_concurrent(inner.max_concurrent_visits, |namespace_id| {
                self.tick_session(namespace_id).boxed()
            })
            .await;
    }

    async fn tick_session(&self, namespace_id: NamespaceId) {
        let inner = &self.inner;
        let Some(_visit) = self.claim(&namespace_id).await else {
            return;
        };
        let now_ms = inner.namespaces.now_ms();
        let Some(entry) = inner.namespaces.entry(&namespace_id) else {
            return;
        };
        let (seq, due) = {
            let held = lock(&entry);
            let seq = held.handle.last_published_seq();
            if held
                .handle
                .last_published_at_ms()
                .is_some_and(|published_at_ms| {
                    u128::from(now_ms.saturating_sub(published_at_ms))
                        < inner.tick_interval.as_millis()
                })
            {
                return;
            }
            let metadata_caught_up = held.handle.metadata_caught_up();
            // The idle fold allows one metadata call before the retry time,
            // when no call has run since the tail reached the idle fold age.
            let idle_fold_due = held
                .handle
                .idle_fold_due_at_ms()
                .is_some_and(|fold_due_at_ms| {
                    now_ms >= fold_due_at_ms
                        && held
                            .metadata_retry_at_ms
                            .saturating_sub(inner.metadata_retry_interval_ms)
                            < fold_due_at_ms
                });
            let due = if !metadata_caught_up
                && (now_ms >= held.metadata_retry_at_ms || idle_fold_due)
                && !self.backed_off(&namespace_id, SweepCall::Metadata)
            {
                Due::Metadata
            } else if inner.grep.is_some()
                && (held.index_dirty || held.indexed_seq != seq)
                && !self.backed_off(&namespace_id, SweepCall::GrepIndex)
            {
                Due::Index
            } else if seq.is_some()
                && seq != held.collected_seq
                && now_ms.saturating_sub(held.collected_at_ms) >= inner.collection_interval_ms
                && !self.backed_off(&namespace_id, SweepCall::Gc)
            {
                Due::Collection
            } else if metadata_caught_up || self.backoff_at_cap(&namespace_id, SweepCall::Metadata)
            {
                Due::Close
            } else {
                return;
            };
            (seq, due)
        };
        match due {
            Due::Metadata => {
                let caught_up = self.maintain_metadata(&namespace_id).await;
                self.record_metadata(&entry, seq, caught_up);
            }
            Due::Index => {
                if let Some(grep) = &inner.grep {
                    self.maintain_index(grep, &namespace_id, Some(&entry), seq)
                        .await;
                }
            }
            Due::Collection => {
                if self.gc(&namespace_id).await {
                    self.record_collection(&entry, seq);
                }
            }
            Due::Close => {
                let close = AssertUnwindSafe(self.close_idle_session(&namespace_id, seq));
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
                inner.idle_session_close_after_ms,
                |held| {
                    // A failing call keeps an idle session held, so ticks
                    // retry it on its backoff, until its wait reaches the
                    // cap. Then the session closes and the next full pass
                    // retries the call.
                    (held.handle.metadata_caught_up()
                        || self.backoff_at_cap(namespace_id, SweepCall::Metadata))
                        && (inner.grep.is_none()
                            || (!held.index_dirty && held.indexed_seq == seq)
                            || self.backoff_at_cap(namespace_id, SweepCall::GrepIndex))
                        && (held.collected_seq == seq
                            || inner
                                .namespaces
                                .now_ms()
                                .saturating_sub(held.collected_at_ms)
                                < inner.collection_interval_ms
                            || self.backoff_at_cap(namespace_id, SweepCall::Gc))
                },
            )
            .await
        {
            tracing::warn!(%namespace_id, error = %error, "idle session close failed");
        }
    }

    pub(crate) fn start(&self) -> RunningSweep {
        let inner = &self.inner;
        tracing::info!(
            tick_interval_ms = loonfs_types::saturating_duration_ms(inner.tick_interval),
            maintenance_interval_ms = inner.metadata_retry_interval_ms,
            gc_interval_ms = inner.collection_interval_ms,
            full_sweep_interval_ms = loonfs_types::saturating_duration_ms(inner.full_interval),
            max_concurrent_maintenance = inner.max_concurrent_visits,
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
        let mut ticks = tokio::time::interval(inner.full_interval);
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
        let mut ticks = tokio::time::interval(inner.tick_interval);
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
