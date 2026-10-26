//! The read window of one read handle, which decides what a read fetches and
//! what the handle reads ahead.

use crate::config::MountConfig;
use std::ops::Range;

/// The sizes that decide what a read fetches, from the mount's settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FetchSizes {
    /// The largest file that the first read fetches whole.
    pub(crate) max_whole_file_bytes: u64,
    /// The first window of a larger file, and the window after a seek.
    pub(crate) first_window_bytes: u64,
    /// The largest window that sequential reads grow to.
    pub(crate) max_window_bytes: u64,
    /// The largest fetch that's a ranged read through the server.
    pub(crate) max_ranged_read_bytes: u64,
}

impl FetchSizes {
    pub(crate) fn of(config: &MountConfig) -> Self {
        Self {
            max_whole_file_bytes: config.max_whole_file_bytes,
            first_window_bytes: config.first_window_bytes,
            max_window_bytes: config.max_window_bytes,
            max_ranged_read_bytes: config.max_ranged_read_bytes,
        }
    }
}

/// What one read needs fetched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowPlan {
    /// The bytes that the read waits for: the window that contains it.
    pub(crate) demand: Range<u64>,
    /// The next window, when this read made the handle ask for it.
    pub(crate) ahead: Option<Range<u64>>,
}

/// A run of sequential reads, from the read after a seek.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Run {
    /// Where the run's first read started.
    start_offset: u64,
    /// Where the last read ended.
    next_offset: u64,
    /// The window that the reader is in.
    current: Range<u64>,
    /// The window after it, once the handle asked for it.
    next: Option<Range<u64>>,
}

/// The read window of one handle.
///
/// The sizes below are the defaults of the mount's settings. A file of at
/// most 8 MiB is fetched whole. A larger file starts with a window of 1 MiB,
/// aligned to 1 MiB, and a read at another offset than where the last one
/// ended starts again with a window of 1 MiB. Once a sequential
/// reader has read half of its window, the handle asks for the next window,
/// so one window stays in flight ahead of the reader.
///
/// The next window is the smallest of twice the current window, 64 MiB, and
/// the bytes the reader has consumed less the bytes fetched that it hasn't
/// read yet. So the bytes fetched ahead of the reader never exceed the bytes
/// it consumed, and a run of reads that consumes `c` bytes fetches at most
/// `2c` bytes plus the part of its first window before its first read, which
/// is less than the first window. The handle waits until that allows a
/// window of at least half the current one, so the windows don't shrink to a
/// few bytes after a read that starts in the middle of its first window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadWindow {
    sizes: FetchSizes,
    /// The current run, or `None` before the first read.
    run: Option<Run>,
}

fn length(range: &Range<u64>) -> u64 {
    range.end - range.start
}

impl ReadWindow {
    pub(crate) fn new(sizes: FetchSizes) -> Self {
        Self { sizes, run: None }
    }

    /// Plans a read of `range` from a revision of `size_bytes` bytes.
    pub(crate) fn plan(&mut self, range: Range<u64>, size_bytes: u64) -> WindowPlan {
        let FetchSizes {
            max_whole_file_bytes,
            first_window_bytes,
            max_window_bytes,
            ..
        } = self.sizes;
        if size_bytes <= max_whole_file_bytes {
            return WindowPlan {
                demand: 0..size_bytes,
                ahead: None,
            };
        }
        let mut run = match self.run.take() {
            Some(run) if run.next_offset == range.start => run,
            _ => {
                let start = range.start - range.start % first_window_bytes;
                Run {
                    start_offset: range.start,
                    next_offset: range.start,
                    current: start..start + first_window_bytes,
                    next: None,
                }
            }
        };
        while range.start >= run.current.end {
            // A reader that passed its window before the handle asked for the
            // next one waits for a window sized from what it consumed, which
            // is every byte fetched so far.
            let consumed = range.start - run.start_offset;
            run.current = run.next.take().unwrap_or_else(|| {
                let next_length = (2 * length(&run.current))
                    .min(max_window_bytes)
                    .min(consumed);
                run.current.end..run.current.end + next_length
            });
        }
        let demand = run.current.start..run.current.end.max(range.end).min(size_bytes);
        let half_read = range.end >= run.current.start + length(&run.current) / 2;
        let mut ahead = None;
        if run.next.is_none() && half_read && demand.end < size_bytes {
            let consumed = range.end - run.start_offset;
            let unread = demand.end - range.end;
            let rest_of_file = size_bytes - demand.end;
            let next_length = (2 * length(&run.current))
                .min(max_window_bytes)
                .min(consumed.saturating_sub(unread))
                .min(rest_of_file);
            let least_length = (length(&run.current) / 2).min(rest_of_file);
            if next_length >= least_length {
                let next = demand.end..demand.end + next_length;
                run.next = Some(next.clone());
                ahead = Some(next);
            }
        }
        run.next_offset = range.end;
        self.run = Some(run);
        WindowPlan { demand, ahead }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ranges::RangeSet;

    const MIB: u64 = 1 << 20;
    const MAX_WHOLE_FILE_BYTES: u64 = 8 * MIB;
    const FIRST_WINDOW_BYTES: u64 = MIB;
    const MAX_WINDOW_BYTES: u64 = 64 * MIB;

    /// Returns a read window with the defaults of the mount's settings.
    fn default_window() -> ReadWindow {
        ReadWindow::new(FetchSizes {
            max_whole_file_bytes: MAX_WHOLE_FILE_BYTES,
            first_window_bytes: FIRST_WINDOW_BYTES,
            max_window_bytes: MAX_WINDOW_BYTES,
            max_ranged_read_bytes: 8 * MIB,
        })
    }

    /// Drives sequential reads of `read_lengths` from `start`, and checks
    /// after every read that the requested bytes are contiguous and never
    /// exceed twice the bytes consumed plus 1 MiB. Returns the windows that
    /// the reads waited for or asked for, in order.
    fn read_sequentially(
        window: &mut ReadWindow,
        start: u64,
        read_lengths: impl IntoIterator<Item = u64>,
        size: u64,
    ) -> Vec<Range<u64>> {
        let mut requested = RangeSet::default();
        let mut windows: Vec<Range<u64>> = Vec::new();
        let first_window_start = start - start % FIRST_WINDOW_BYTES;
        let mut requested_end = start;
        let mut offset = start;
        for read_length in read_lengths {
            let plan = window.plan(offset..offset + read_length, size);
            for range in std::iter::once(plan.demand).chain(plan.ahead) {
                if !windows.contains(&range) {
                    windows.push(range.clone());
                }
                requested_end = requested_end.max(range.end);
                requested.insert(range);
            }
            offset += read_length;
            let consumed = offset - start;
            assert_eq!(
                requested.missing(first_window_start..requested_end),
                Vec::<Range<u64>>::new()
            );
            assert!(
                requested.len_bytes() <= 2 * consumed + MIB,
                "{} bytes requested after {consumed} consumed",
                requested.len_bytes()
            );
        }
        windows
    }

    /// Checks that a sequential reader fetches at most twice what it consumed
    /// plus 1 MiB after every read, asks for the next window once it has
    /// read half of the current one, grows its windows to 64 MiB, and that a
    /// seek starts again at 1 MiB.
    #[test]
    fn read_ahead_never_fetches_more_than_twice_what_was_consumed() {
        let size = 1 << 30;
        let mut window = default_window();
        let first = window.plan(0..128 * 1024, size);
        assert_eq!((first.demand, first.ahead), (0..MIB, None));
        let half = window.plan(128 * 1024..512 * 1024, size);
        assert_eq!(half.ahead, None, "nothing consumed is left over yet");
        let too_small = window.plan(512 * 1024..640 * 1024, size);
        assert_eq!(too_small.ahead, None, "a window of 256 KiB is too small");
        let next = window.plan(640 * 1024..768 * 1024, size);
        assert_eq!(next.ahead, Some(MIB..MIB + 512 * 1024));

        let mut window = default_window();
        let reads = std::iter::repeat_n(4 * 1024, 64)
            .chain(std::iter::repeat_n(128 * 1024, 2_000))
            .chain(std::iter::repeat_n(MIB, 300));
        let windows = read_sequentially(&mut window, 0, reads, size);
        assert!(windows
            .iter()
            .any(|window| length(window) == MAX_WINDOW_BYTES));

        let seek = 700 * MIB + 7;
        let after_seek = read_sequentially(&mut window, seek, [128 * 1024; 64], size);
        assert_eq!(after_seek[0], 700 * MIB..701 * MIB);
        assert!(
            after_seek
                .windows(2)
                .all(|pair| length(&pair[1]) >= length(&pair[0]) / 2),
            "no window shrinks below half the one before: {after_seek:?}"
        );
    }

    #[test]
    fn a_small_file_is_fetched_whole_and_a_window_stops_at_the_end_of_the_file() {
        let mut window = default_window();
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
