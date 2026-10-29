//! Shared reservations for bytes that reads keep: metadata block memos and
//! the collector's table of content roots.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub trait ReadWorkingMemoryObserver: Send + Sync {
    fn in_use(&self, bytes: usize);
    fn reservation_failed(&self);
}

pub struct ReadWorkingMemory {
    limit: usize,
    in_use: AtomicUsize,
    observer: Option<Arc<dyn ReadWorkingMemoryObserver>>,
}

impl std::fmt::Debug for ReadWorkingMemory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadWorkingMemory")
            .field("limit", &self.limit)
            .field("in_use", &self.in_use())
            .finish_non_exhaustive()
    }
}

impl ReadWorkingMemory {
    pub fn new(limit: usize, observer: Option<Arc<dyn ReadWorkingMemoryObserver>>) -> Self {
        Self {
            limit,
            in_use: AtomicUsize::new(0),
            observer,
        }
    }

    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::SeqCst)
    }

    /// The most bytes reservations may reserve at once.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Returns the bytes that reservations can take before the limit refuses
    /// one.
    pub fn available_bytes(&self) -> usize {
        self.limit.saturating_sub(self.in_use())
    }

    pub fn try_reserve(&self, bytes: usize) -> bool {
        let reserved = self
            .in_use
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |in_use| {
                in_use
                    .checked_add(bytes)
                    .filter(|total| *total <= self.limit)
            });
        if reserved.is_err() {
            if let Some(observer) = &self.observer {
                observer.reservation_failed();
            }
            return false;
        }
        self.report();
        true
    }

    pub fn release(&self, bytes: usize) {
        self.in_use.fetch_sub(bytes, Ordering::SeqCst);
        self.report();
    }

    fn report(&self) {
        let Some(observer) = &self.observer else {
            return;
        };
        loop {
            let bytes = self.in_use();
            observer.in_use(bytes);
            // A report that read the counter earlier can set the gauge after
            // a newer report did; re-reading leaves the gauge current. Each
            // pass repeats only because another reservation or release
            // changed the counter meanwhile.
            if self.in_use() == bytes {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, Mutex};

    /// An observer that holds its first report until the test releases it,
    /// and records every reported value in order.
    struct HeldFirstReport {
        reported: Mutex<Vec<usize>>,
        reports: AtomicUsize,
        first_report_entered: Barrier,
        first_report_released: Barrier,
    }

    impl ReadWorkingMemoryObserver for HeldFirstReport {
        fn in_use(&self, bytes: usize) {
            if self.reports.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first_report_entered.wait();
                self.first_report_released.wait();
            }
            self.reported.lock().expect("reported values").push(bytes);
        }

        fn reservation_failed(&self) {}
    }

    #[test]
    fn a_report_that_finishes_after_a_newer_one_leaves_the_gauge_current() {
        let observer = Arc::new(HeldFirstReport {
            reported: Mutex::new(Vec::new()),
            reports: AtomicUsize::new(0),
            first_report_entered: Barrier::new(2),
            first_report_released: Barrier::new(2),
        });
        let pool = ReadWorkingMemory::new(4096, Some(Arc::clone(&observer) as _));
        std::thread::scope(|scope| {
            scope.spawn(|| assert!(pool.try_reserve(100)));
            // The first reservation read 100 bytes and is inside its report.
            observer.first_report_entered.wait();
            assert!(pool.try_reserve(200));
            // The newer report set 300; now the held report sets 100.
            observer.first_report_released.wait();
        });
        let reported = observer.reported.lock().expect("reported values");
        assert_eq!(reported.as_slice(), &[300, 100, 300]);
    }

    #[test]
    fn concurrent_reservations_share_one_limit_and_release_every_byte() {
        let pool = ReadWorkingMemory::new(4096, None);
        let barrier = Barrier::new(9);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    let reserved = pool.try_reserve(1024);
                    barrier.wait();
                    barrier.wait();
                    if reserved {
                        pool.release(1024);
                    }
                });
            }
            barrier.wait();
            assert_eq!(pool.in_use(), 4096);
            assert!(!pool.try_reserve(usize::MAX));
            barrier.wait();
        });
        assert_eq!(pool.in_use(), 0);
        assert!(pool.try_reserve(4096));
        pool.release(4096);
        assert_eq!(pool.in_use(), 0);
    }
}
