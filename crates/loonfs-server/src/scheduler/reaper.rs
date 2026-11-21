//! Closes idle and ended writer sessions when the maintenance scheduler is off.
//!
//! A server whose maintenance mode is `serve_only` or `disabled` delegates
//! folds, compactions, sweeps, and garbage collection to another process, but
//! still opens a local writer session for each namespace that receives a
//! mutation. Without a reaper, those sessions stay open for the lifetime of the
//! process.

use super::steps::held_ids;
use super::{lock, RunningScheduler, SchedulerState};
use loonfs::{MaintenanceCancellation, NamespaceSessionStatus};
use loonfs_http::Namespaces;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::MissedTickBehavior;

/// Periodically closes writer sessions that have been idle past
/// `idle_session_close_after_ms` or can no longer write, without running any
/// store maintenance.
#[derive(Clone)]
pub(crate) struct SessionReaper {
    pub(super) namespaces: Arc<Namespaces<SchedulerState>>,
    interval_ms: u64,
    idle_session_close_after_ms: u64,
    stop: MaintenanceCancellation,
}

impl SessionReaper {
    pub(crate) fn new(
        namespaces: Arc<Namespaces<SchedulerState>>,
        interval_ms: u64,
        idle_session_close_after_ms: u64,
    ) -> Self {
        Self {
            namespaces,
            interval_ms,
            idle_session_close_after_ms,
            stop: MaintenanceCancellation::new(),
        }
    }

    /// Walks every held session once and closes any session that's ended or
    /// has been idle longer than `idle_session_close_after_ms`, costing zero
    /// store requests.
    pub(crate) async fn reap_once(&self) {
        for namespace_id in held_ids(&self.namespaces) {
            if self.stop.is_cancelled() {
                return;
            }
            let Some(entry) = self.namespaces.entry(&namespace_id) else {
                continue;
            };
            let (seq, open) = {
                let held = lock(&entry);
                (
                    held.handle.last_published_seq(),
                    held.handle.session_status() == NamespaceSessionStatus::Open,
                )
            };
            let idle_after_ms = if open {
                self.idle_session_close_after_ms
            } else {
                0
            };
            if let Err(error) = self
                .namespaces
                .close_if_idle(&namespace_id, seq, idle_after_ms, |_| true)
                .await
            {
                tracing::warn!(%namespace_id, error = %error, "session close failed");
            }
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the configured interval paces idle session reaping and decides no outcome"
    )]
    pub(crate) fn start(&self) -> RunningScheduler {
        let reaper = self.clone();
        RunningScheduler {
            stop: self.stop.clone(),
            task: tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(reaper.interval_ms));
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        biased;
                        () = reaper.stop.cancelled() => return,
                        _ = interval.tick() => {}
                    }
                    reaper.reap_once().await;
                }
            }),
        }
    }
}
