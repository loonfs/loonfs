//! Shared reservations for bytes that reads keep: metadata block memos, the
//! blocks of a scan's refill, and the roots that a garbage collection keeps.

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

    /// Reserves `min_bytes` whatever the limit, plus as much of the rest of
    /// `wanted_bytes` as fits under the limit, in one update, and returns the
    /// reservation, which releases its bytes when it is dropped.
    ///
    /// The bytes in use can exceed the limit by `min_bytes`. A grant smaller
    /// than `wanted_bytes` is reported as a failed reservation.
    pub fn reserve(self: &Arc<Self>, wanted_bytes: usize, min_bytes: usize) -> ReadReservation {
        assert!(
            min_bytes <= wanted_bytes,
            "a reservation should want at least its minimum"
        );
        let granted_bytes = |in_use: usize| {
            let floor = in_use + min_bytes;
            min_bytes + (wanted_bytes - min_bytes).min(self.limit.saturating_sub(floor))
        };
        let previous = self
            .in_use
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |in_use| {
                Some(in_use + granted_bytes(in_use))
            })
            .expect("an update that always returns a value should succeed");
        let granted = granted_bytes(previous);
        if granted < wanted_bytes {
            if let Some(observer) = &self.observer {
                observer.reservation_failed();
            }
        }
        self.report();
        ReadReservation {
            pool: Arc::clone(self),
            bytes: granted,
        }
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

/// Bytes reserved from a read working memory, released when the reservation
/// is dropped.
#[derive(Debug)]
pub struct ReadReservation {
    pool: Arc<ReadWorkingMemory>,
    bytes: usize,
}

impl ReadReservation {
    /// Returns the number of bytes reserved.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Releases the bytes of the reservation above `bytes`.
    pub fn shrink_to(&mut self, bytes: usize) {
        assert!(
            bytes <= self.bytes,
            "a reservation should shrink to at most its reserved bytes"
        );
        self.pool.release(self.bytes - bytes);
        self.bytes = bytes;
    }
}

impl Drop for ReadReservation {
    fn drop(&mut self) {
        self.pool.release(self.bytes);
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
    fn a_reservation_takes_its_minimum_and_what_is_free_and_releases_both() {
        let pool = Arc::new(ReadWorkingMemory::new(4096, None));
        assert!(pool.try_reserve(1024));
        let mut reservation = pool.reserve(1000, 0);
        assert_eq!(reservation.bytes(), 1000);
        assert_eq!(pool.in_use(), 2024);
        let rest = pool.reserve(4096, 100);
        assert_eq!(rest.bytes(), 2072);
        assert_eq!(pool.in_use(), 4096);
        // A full pool still grants the minimum, above the limit.
        let over = pool.reserve(500, 200);
        assert_eq!(over.bytes(), 200);
        assert_eq!(pool.in_use(), 4296);
        assert_eq!(pool.available_bytes(), 0);
        reservation.shrink_to(400);
        assert_eq!(reservation.bytes(), 400);
        assert_eq!(pool.in_use(), 3696);
        drop(over);
        drop(rest);
        drop(reservation);
        assert_eq!(pool.in_use(), 1024);
        pool.release(1024);
        assert_eq!(pool.in_use(), 0);
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
