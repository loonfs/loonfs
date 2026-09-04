use super::{MaintenanceJobId, NamespacePublication};
use crate::NamespaceId;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Best-effort request to consider maintenance work.
pub enum MaintenanceHint {
    /// A namespace publication was attempted.
    Published(NamespacePublication),
    /// A namespace WAL-tail fold attempt finished.
    WalFoldFinished {
        /// Namespace whose WAL-tail fold attempt finished.
        namespace_id: NamespaceId,
    },
    /// A job becomes eligible at a durable deadline.
    DueAt {
        /// Namespace to maintain.
        namespace_id: NamespaceId,
        /// Job to run.
        job: MaintenanceJobId,
        /// Earliest Unix millisecond for the run.
        not_before_ms: u64,
    },
}

/// Synchronous, non-blocking maintenance hint callback.
pub type MaintenanceHintObserver = Arc<dyn Fn(MaintenanceHint) + Send + Sync + 'static>;

static HINTS_DROPPED: AtomicU64 = AtomicU64::new(0);

/// Creates bounded observer and receiver pairs.
pub struct MaintenanceHintRelay;

/// Receiving side attached to a maintenance runner.
pub struct MaintenanceHintReceiver {
    pub(crate) receiver: tokio::sync::mpsc::Receiver<MaintenanceHint>,
    pub(crate) dropped_at_creation: u64,
}

impl MaintenanceHintRelay {
    /// Creates a relay whose full channel drops new hints.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(capacity: NonZeroUsize) -> (MaintenanceHintObserver, MaintenanceHintReceiver) {
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity.get());
        let observer = Arc::new(move |hint| {
            if sender.try_send(hint).is_err() {
                HINTS_DROPPED.fetch_add(1, Ordering::Relaxed);
            }
        });
        (
            observer,
            MaintenanceHintReceiver {
                receiver,
                dropped_at_creation: HINTS_DROPPED.load(Ordering::Relaxed),
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn dropped() -> u64 {
        HINTS_DROPPED.load(Ordering::Relaxed)
    }
}

pub(crate) fn dropped_hints() -> u64 {
    HINTS_DROPPED.load(Ordering::Relaxed)
}
