//! The first due step for each held session and the two host intervals.

use super::{lock, RunningSweep, Sweep};
use futures::{FutureExt as _, StreamExt as _};
use loonfs::{ChangeSeq, NamespaceId};
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
            if held.handle.last_published_ms().is_some_and(|published_ms| {
                u128::from(now_ms.saturating_sub(published_ms)) < inner.tick_interval.as_millis()
            }) {
                return;
            }
            let metadata_caught_up = held.handle.metadata_caught_up();
            let idle_fold_due = inner.metadata.idle_fold_after_ms != 0
                && held.handle.last_published_ms().is_some_and(|published_ms| {
                    let fold_after_ms =
                        published_ms.saturating_add(inner.metadata.idle_fold_after_ms);
                    now_ms >= fold_after_ms
                        && held
                            .metadata_retry_after_ms
                            .saturating_sub(inner.metadata_retry_ms)
                            < fold_after_ms
                });
            let due = if !metadata_caught_up
                && (now_ms >= held.metadata_retry_after_ms || idle_fold_due)
            {
                Due::Metadata
            } else if inner.grep.is_some() && (held.index_dirty || held.indexed_seq != seq) {
                Due::Index
            } else if seq.is_some()
                && seq != held.collected_seq
                && now_ms.saturating_sub(held.collected_ms) >= inner.collection_interval_ms
            {
                Due::Collection
            } else if metadata_caught_up {
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
                if self.collect(&namespace_id).await {
                    self.record_collection(&entry, seq);
                }
            }
            Due::Close => self.close_idle_session(&namespace_id, seq).await,
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
                    held.handle.metadata_caught_up()
                        && (inner.grep.is_none() || (!held.index_dirty && held.indexed_seq == seq))
                        && (held.collected_seq == seq
                            || inner.namespaces.now_ms().saturating_sub(held.collected_ms)
                                < inner.collection_interval_ms)
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
            tick_interval_ms = u64::try_from(inner.tick_interval.as_millis()).unwrap_or(u64::MAX),
            maintenance_interval_ms = inner.metadata_retry_ms,
            gc_interval_ms = inner.collection_interval_ms,
            full_sweep_interval_ms =
                u64::try_from(inner.full_interval.as_millis()).unwrap_or(u64::MAX),
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
            let _ = self.run_pass(true).await;
        }
    }

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
