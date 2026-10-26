//! The read window of one read handle, which decides what a read fetches and
//! what the handle reads ahead.

use std::ops::Range;

const MIB: u64 = 1 << 20;
/// The largest file that the first read fetches whole.
pub(crate) const MAX_WHOLE_FILE_BYTES: u64 = 8 * MIB;
/// The first window of a larger file, and the window after a seek.
pub(crate) const FIRST_WINDOW_BYTES: u64 = MIB;
/// The largest window that sequential reads grow to.
pub(crate) const MAX_WINDOW_BYTES: u64 = 64 * MIB;

/// What one read needs fetched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowPlan {
    /// The bytes that the read waits for: the window that contains it.
    pub(crate) demand: Range<u64>,
    /// The window after it, which the handle keeps in flight ahead of a
    /// sequential reader.
    pub(crate) ahead: Option<Range<u64>>,
}

/// The read window of one handle.
///
/// A file of at most 8 MiB is fetched whole. A larger file starts with a
/// window of 1 MiB, aligned to 1 MiB. A read that continues where the last
/// one ended is sequential, and when it reaches the next window, that window
/// is twice as large, up to 64 MiB. A sequential reader keeps the window after
/// its current one in flight. A read at another offset starts again with a
/// window of 1 MiB.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReadWindow {
    /// The current window, or `None` before the first read.
    current: Option<Range<u64>>,
    /// Where the last read ended.
    next_offset: u64,
}

impl ReadWindow {
    /// Plans a read of `range` from a revision of `size_bytes` bytes.
    pub(crate) fn plan(&mut self, range: Range<u64>, size_bytes: u64) -> WindowPlan {
        if size_bytes <= MAX_WHOLE_FILE_BYTES {
            self.next_offset = range.end;
            return WindowPlan {
                demand: 0..size_bytes,
                ahead: None,
            };
        }
        let sequential = self.current.is_some() && range.start == self.next_offset;
        let mut window = match (&self.current, sequential) {
            (Some(current), true) => current.clone(),
            _ => {
                let start = range.start - range.start % FIRST_WINDOW_BYTES;
                start..start + FIRST_WINDOW_BYTES
            }
        };
        while sequential && range.start >= window.end {
            let length = (window.end - window.start)
                .saturating_mul(2)
                .min(MAX_WINDOW_BYTES);
            window = window.end..window.end + length;
        }
        self.current = Some(window.clone());
        self.next_offset = range.end;
        let demand = window.start..window.end.max(range.end).min(size_bytes);
        let ahead = (sequential && demand.end < size_bytes).then(|| {
            let length = (window.end - window.start)
                .saturating_mul(2)
                .min(MAX_WINDOW_BYTES);
            demand.end..(demand.end + length).min(size_bytes)
        });
        WindowPlan { demand, ahead }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that sequential reads double the window up to 64 MiB and keep
    /// the next window in flight, and that a seek starts again at 1 MiB.
    #[test]
    fn sequential_reads_grow_the_window_and_a_seek_resets_it() {
        let size = 1 << 30;
        let read = 128 * 1024;
        let mut window = ReadWindow::default();
        let first = window.plan(0..read, size);
        assert_eq!(first.demand, 0..MIB);
        assert_eq!(first.ahead, None, "a first read doesn't read ahead");

        let second = window.plan(read..2 * read, size);
        assert_eq!(second.demand, 0..MIB);
        assert_eq!(second.ahead, Some(MIB..3 * MIB));

        let mut offset = 2 * read;
        let mut windows = Vec::new();
        while offset < 300 * MIB {
            let plan = window.plan(offset..offset + read, size);
            if windows.last() != Some(&plan.demand) {
                windows.push(plan.demand.clone());
            }
            offset += read;
        }
        let lengths: Vec<u64> = windows
            .iter()
            .map(|window| (window.end - window.start) / MIB)
            .collect();
        assert_eq!(lengths[..8], [1, 2, 4, 8, 16, 32, 64, 64]);

        let seek = window.plan(500 * MIB + 7..500 * MIB + 7 + read, size);
        assert_eq!(seek.demand, 500 * MIB..501 * MIB);
        assert_eq!(seek.ahead, None);
        let after_seek = window.plan(500 * MIB + 7 + read..500 * MIB + 7 + 2 * read, size);
        assert_eq!(after_seek.ahead, Some(501 * MIB..503 * MIB));
    }

    #[test]
    fn a_small_file_is_fetched_whole_and_a_window_stops_at_the_end_of_the_file() {
        let mut window = ReadWindow::default();
        assert_eq!(
            window.plan(4096..8192, MAX_WHOLE_FILE_BYTES),
            WindowPlan {
                demand: 0..MAX_WHOLE_FILE_BYTES,
                ahead: None,
            }
        );
        let size = 9 * MIB + 10;
        let plan = window.plan(9 * MIB..9 * MIB + 10, size);
        assert_eq!(plan.demand, 9 * MIB..size);
    }
}
