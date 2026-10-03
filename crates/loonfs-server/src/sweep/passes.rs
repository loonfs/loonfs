//! Session selection and the sweep's pass schedule.

use super::{lock, RunningSweep, Sweep, SweepCall};
use futures::{FutureExt as _, StreamExt as _};
use loonfs::{ChangeSeq, NamespaceId};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::MissedTickBehavior;

const INDEX_PASS_INTERVAL: Duration = Duration::from_secs(5);

impl Sweep {
    /// Visits held sessions until metadata and grep are caught up with their
    /// published seq. With `collect_garbage`, also visits sessions whose seq
    /// moved since collection. Skipped sessions cost no store request.
    pub async fn run_session_pass(&self, collect_garbage: bool) {
        let inner = &self.inner;
        let _pass = inner.pass.lock().await;
        let held = self.held_seqs();
        let visits: Vec<_> = {
            let sessions = lock(&inner.sessions);
            held.into_iter()
                .filter_map(|(namespace_id, seq)| {
                    let previous = sessions.get(&namespace_id);
                    let collect = collect_garbage
                        && previous.and_then(|progress| progress.collected) != Some(seq);
                    (collect || previous.and_then(|progress| progress.maintained) != Some(seq))
                        .then_some((namespace_id, seq, collect))
                })
                .collect()
        };
        futures::stream::iter(visits)
            .take_until(inner.stop.cancelled())
            .for_each_concurrent(
                inner.max_concurrent_visits,
                |(namespace_id, seq, collect)| self.visit(namespace_id, Some(seq), collect).boxed(),
            )
            .await;
    }

    pub(super) fn held_seqs(&self) -> HashMap<NamespaceId, ChangeSeq> {
        let held: HashMap<_, _> = self
            .inner
            .namespaces
            .held()
            .iter()
            .filter_map(|namespace| Some((namespace.id().clone(), namespace.last_published_seq()?)))
            .collect();
        lock(&self.inner.sessions).retain(|namespace_id, _| held.contains_key(namespace_id));
        held
    }

    /// Builds the grep index of each writer session this process holds
    /// whose last published seq moved since this pass last indexed it, one
    /// session at a time. Also builds namespaces whose lifecycle changed
    /// through this worker, even when no held session has published a seq.
    /// Unfinished builds continue on later passes while held.
    ///
    /// Does nothing on a server that does not maintain the grep index. A
    /// session with no change costs no store request. A namespace whose
    /// index a sweep visit is building is left to that visit.
    pub async fn run_index_pass(&self) {
        let inner = &self.inner;
        let Some(grep) = &inner.grep else {
            return;
        };
        let lifecycle_changes = grep.worker.drain_lifecycle_changes();
        let held: HashMap<NamespaceId, Option<ChangeSeq>> = inner
            .namespaces
            .held()
            .iter()
            .map(|namespace| (namespace.id().clone(), namespace.last_published_seq()))
            .collect();
        let selected: HashMap<NamespaceId, Option<ChangeSeq>> = {
            let mut progress = lock(&grep.progress);
            for namespace_id in &lifecycle_changes {
                progress.indexed.remove(namespace_id);
            }
            progress
                .indexed
                .retain(|namespace_id, _| held.get(namespace_id).is_some_and(Option::is_some));
            progress.pending.retain(|namespace_id| {
                held.contains_key(namespace_id) || lifecycle_changes.contains(namespace_id)
            });
            let mut selected: HashMap<_, _> = held
                .iter()
                .filter_map(|(namespace_id, seq)| {
                    let seq = (*seq)?;
                    (progress.indexed.get(namespace_id) != Some(&seq))
                        .then(|| (namespace_id.clone(), Some(seq)))
                })
                .collect();
            for namespace_id in lifecycle_changes.iter().chain(&progress.pending) {
                selected
                    .entry(namespace_id.clone())
                    .or_insert(held.get(namespace_id).copied().flatten());
            }
            selected
        };
        for (namespace_id, seq) in selected {
            if inner.stop.is_cancelled() {
                return;
            }
            let Some(_building) = grep.claim(&namespace_id) else {
                continue;
            };
            // Boxed for the same reason as a sweep visit.
            let caught_up = match self.build_index(grep, &namespace_id).boxed().await {
                Ok(caught_up) => caught_up,
                Err(error) => {
                    self.record_failure(&namespace_id, SweepCall::GrepIndex, error.code(), &error);
                    // A failed build waits for the session's next commit or the
                    // next sweep pass instead of retrying every few seconds.
                    true
                }
            };
            let mut progress = lock(&grep.progress);
            if caught_up {
                progress.pending.remove(&namespace_id);
                if let Some(seq) = seq {
                    progress.indexed.insert(namespace_id, seq);
                }
            } else {
                progress.pending.insert(namespace_id);
            }
        }
    }

    /// Starts the maintenance loop with a full pass and collection, and
    /// the separate index loop when this server maintains the grep index.
    pub(crate) fn start(&self) -> RunningSweep {
        let inner = &self.inner;
        tracing::info!(
            maintenance_interval_ms = u64::try_from(inner.interval.as_millis()).unwrap_or(u64::MAX),
            gc_interval_ms =
                u64::try_from(inner.collection_interval.as_millis()).unwrap_or(u64::MAX),
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
                tokio::join!(sweep.run_passes(), sweep.run_index_passes());
            }),
        }
    }

    async fn run_passes(&self) {
        let inner = &self.inner;
        let mut ticks = tokio::time::interval(inner.interval);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut full_ticks = tokio::time::interval(inner.full_interval);
        full_ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let started = full_ticks.tick().await;
        let mut last_collection = self.run_pass(true).await.is_ok().then_some(started);
        loop {
            tokio::select! {
                () = inner.stop.cancelled() => return,
                started = full_ticks.tick() => {
                    if self.run_pass(true).await.is_ok() {
                        last_collection = Some(started);
                    }
                }
                started = ticks.tick() => {
                    let collect = last_collection
                        .is_none_or(|at| started.saturating_duration_since(at) >= inner.collection_interval);
                    self.run_session_pass(collect).await;
                    if collect {
                        last_collection = Some(started);
                    }
                }
            }
        }
    }

    async fn run_index_passes(&self) {
        let inner = &self.inner;
        if inner.grep.is_none() {
            return;
        }
        let mut ticks = tokio::time::interval(INDEX_PASS_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = inner.stop.cancelled() => return,
                _ = ticks.tick() => {}
            }
            self.run_index_pass().await;
        }
    }
}
