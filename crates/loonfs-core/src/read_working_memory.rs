//! Shared reservations for bytes that reads hold: metadata block memos and
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

impl Default for ReadWorkingMemory {
    fn default() -> Self {
        Self::new(64 * 1024 * 1024, None)
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

    /// The most bytes reservations may hold at once.
    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn try_reserve(&self, bytes: usize) -> bool {
        // `fetch_update` is deprecated since Rust 1.99 for `try_update`, which the 1.88 MSRV lacks.
        #[allow(deprecated)]
        let reserved = self
            .in_use
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |in_use| {
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
            // A concurrent report may finish first; leave the gauge current.
            if self.in_use() == bytes {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

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
