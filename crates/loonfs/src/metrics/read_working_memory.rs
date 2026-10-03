//! Gauges for retained read working memory and failed reservations.

use super::{GaugeHandle, MetricsRecorder};
use loonfs_core::cache::ReadWorkingMemoryObserver;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct ReadWorkingMemoryGauges {
    bytes: Arc<dyn GaugeHandle>,
    failures: Arc<dyn GaugeHandle>,
    failed_reservations: AtomicUsize,
}

pub(super) fn register(recorder: &dyn MetricsRecorder) -> Arc<dyn ReadWorkingMemoryObserver> {
    Arc::new(ReadWorkingMemoryGauges {
        bytes: recorder.register_gauge(
            "loonfs.execution_budget.read_working_bytes",
            "Bytes retained by metadata block memos",
            &[],
        ),
        failures: recorder.register_gauge(
            "loonfs.execution_budget.read_working_reservation_failures",
            "Read working memory reservations that could not fit",
            &[],
        ),
        failed_reservations: AtomicUsize::new(0),
    })
}

impl ReadWorkingMemoryObserver for ReadWorkingMemoryGauges {
    fn in_use(&self, bytes: usize) {
        self.bytes.set(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    fn reservation_failed(&self) {
        self.failed_reservations.fetch_add(1, Ordering::SeqCst);
        loop {
            let failures = self.failed_reservations.load(Ordering::SeqCst);
            self.failures
                .set(i64::try_from(failures).unwrap_or(i64::MAX));
            if self.failed_reservations.load(Ordering::SeqCst) == failures {
                break;
            }
        }
    }
}
