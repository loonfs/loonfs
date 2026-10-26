//! The fetches that fill the cache, how a missing range becomes fetches, and
//! the shared limit on the fetches in flight.

use crate::cache_file::CacheFile;
use crate::read_handle::HandleId;
use crate::window::FetchSizes;
use loonfs_types::{ContentId, InodeId, RevisionNo};
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;

const MIB: u64 = 1 << 20;
/// The most bytes that the fetches of one mount have in flight.
pub(crate) const MAX_FETCH_BYTES_IN_FLIGHT: u64 = 256 * MIB;
/// The most fetch requests that one mount has in flight.
pub(crate) const MAX_FETCH_REQUESTS_IN_FLIGHT: usize = 32;

/// The identity of one fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct FetchId(pub(crate) u64);

/// How a fetch reads its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FetchTransport {
    /// A proxied read of the whole revision, which the server verifies.
    WholeFile,
    /// A ranged read of the revision through the server.
    Ranged,
    /// A download grant from the fetch's offset, read through the client's
    /// download stream.
    Grant,
}

/// Which fetches start first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FetchPriority {
    /// A read waits for the bytes.
    Demand,
    /// A read handle reads ahead, or the engine fetches the rest of a
    /// revision that open descriptors keep.
    Background,
}

/// One fetch of one range of one revision into its cache file.
#[derive(Debug, Clone)]
pub(crate) struct FetchJob {
    pub(crate) fetch_id: FetchId,
    pub(crate) content_id: ContentId,
    pub(crate) file: Arc<CacheFile>,
    pub(crate) inode_id: InodeId,
    pub(crate) revision_no: RevisionNo,
    pub(crate) range: Range<u64>,
    pub(crate) transport: FetchTransport,
    /// The read handle that keeps a grant's stream between its windows.
    pub(crate) stream_owner: Option<HandleId>,
}

/// Splits the missing ranges of a revision into fetches: one whole read of a
/// small revision that has nothing cached, a ranged read for each range of
/// at most the ranged-read limit (8 MiB by default), and a grant for a
/// larger range when the deployment advertises grants, or ranged reads of at
/// most the limit when it doesn't.
pub(crate) fn plan_transfers(
    missing: &[Range<u64>],
    size_bytes: u64,
    nothing_cached: bool,
    direct_get: bool,
    sizes: &FetchSizes,
) -> Vec<(Range<u64>, FetchTransport)> {
    let max_ranged_read_bytes = sizes.max_ranged_read_bytes;
    if nothing_cached && size_bytes <= sizes.max_whole_file_bytes && !missing.is_empty() {
        return vec![(0..size_bytes, FetchTransport::WholeFile)];
    }
    let mut transfers = Vec::new();
    for range in missing {
        if range.end - range.start <= max_ranged_read_bytes {
            transfers.push((range.clone(), FetchTransport::Ranged));
        } else if direct_get {
            transfers.push((range.clone(), FetchTransport::Grant));
        } else {
            let mut start = range.start;
            while start < range.end {
                let end = (start + max_ranged_read_bytes).min(range.end);
                transfers.push((start..end, FetchTransport::Ranged));
                start = end;
            }
        }
    }
    transfers
}

/// A fetch that's running.
#[derive(Debug, Clone)]
pub(crate) struct RunningFetch {
    pub(crate) content_id: ContentId,
    pub(crate) file: Arc<CacheFile>,
    pub(crate) range: Range<u64>,
}

/// The queued and running fetches of one mount.
///
/// The queues are bounded by the reads and handles that requested them:
/// each read requests at most two windows, and each handle whose revision
/// was replaced requests the rest of its revision once.
#[derive(Debug, Default)]
pub(crate) struct FetchQueue {
    demand: VecDeque<FetchJob>,
    background: VecDeque<FetchJob>,
    running: HashMap<FetchId, RunningFetch>,
    running_bytes: u64,
    next_fetch_id: u64,
}

impl FetchQueue {
    /// Returns a new fetch ID.
    pub(crate) fn next_id(&mut self) -> FetchId {
        self.next_fetch_id += 1;
        FetchId(self.next_fetch_id)
    }

    /// Queues a fetch.
    pub(crate) fn push(&mut self, job: FetchJob, priority: FetchPriority) {
        match priority {
            FetchPriority::Demand => self.demand.push_back(job),
            FetchPriority::Background => self.background.push_back(job),
        }
    }

    /// Moves queued background fetches of `content_id` that overlap `range`
    /// ahead of other background fetches, because a read now waits for them.
    pub(crate) fn promote(&mut self, content_id: &ContentId, range: &Range<u64>) {
        let (promoted, kept): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut self.background)
            .into_iter()
            .partition(|job| {
                &job.content_id == content_id
                    && job.range.start < range.end
                    && range.start < job.range.end
            });
        self.background = kept;
        self.demand.extend(promoted);
    }

    /// Returns the queued fetches that fit the shared limit, demand first,
    /// and records them as running.
    pub(crate) fn start_ready(&mut self) -> Vec<FetchJob> {
        let mut started = Vec::new();
        loop {
            let queue = if self.demand.is_empty() {
                &mut self.background
            } else {
                &mut self.demand
            };
            let Some(job) = queue.front() else {
                return started;
            };
            let bytes = job.range.end - job.range.start;
            let fits = self.running.len() < MAX_FETCH_REQUESTS_IN_FLIGHT
                && (self.running.is_empty()
                    || self.running_bytes + bytes <= MAX_FETCH_BYTES_IN_FLIGHT);
            if !fits {
                return started;
            }
            let job = queue.pop_front().expect("the queue's front was just read");
            self.running_bytes += bytes;
            self.running.insert(
                job.fetch_id,
                RunningFetch {
                    content_id: job.content_id.clone(),
                    file: job.file.clone(),
                    range: job.range.clone(),
                },
            );
            started.push(job);
        }
    }

    /// Records that a fetch ended, and returns what it fetched.
    pub(crate) fn finish(&mut self, fetch_id: FetchId) -> Option<RunningFetch> {
        let running = self.running.remove(&fetch_id)?;
        self.running_bytes -= running.range.end - running.range.start;
        Some(running)
    }

    /// Removes the queued fetches of a file that was replaced.
    pub(crate) fn drop_queued(&mut self, content_id: &ContentId) {
        for queue in [&mut self.demand, &mut self.background] {
            queue.retain(|job| &job.content_id != content_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_file::CacheFile;

    const DEFAULT_SIZES: FetchSizes = FetchSizes {
        max_whole_file_bytes: 8 * MIB,
        first_window_bytes: MIB,
        max_window_bytes: 64 * MIB,
        max_ranged_read_bytes: 8 * MIB,
    };

    /// Checks that demand fetches start before background ones, and that no
    /// more than 32 requests and 256 MiB are in flight.
    #[test]
    fn the_shared_limit_starts_demand_first_and_bounds_requests_and_bytes() {
        let file = CacheFile::new(tempfile::tempfile().expect("a temporary file"), 0);
        let content_id = ContentId::generate();
        let mut queue = FetchQueue::default();
        let push = |queue: &mut FetchQueue, length: u64, priority: FetchPriority| {
            let job = FetchJob {
                fetch_id: queue.next_id(),
                content_id: content_id.clone(),
                file: file.clone(),
                inode_id: InodeId(2),
                revision_no: RevisionNo(1),
                range: 0..length,
                transport: FetchTransport::Ranged,
                stream_owner: None,
            };
            queue.push(job, priority);
        };
        push(&mut queue, MIB, FetchPriority::Background);
        for _ in 0..40 {
            push(&mut queue, MIB, FetchPriority::Demand);
        }
        let first = queue.start_ready();
        assert_eq!(first.len(), MAX_FETCH_REQUESTS_IN_FLIGHT);
        for job in &first[..8] {
            queue.finish(job.fetch_id);
        }
        let second = queue.start_ready();
        assert_eq!(second.len(), 8, "the rest of the demand fetches");
        for job in first[8..].iter().chain(&second) {
            queue.finish(job.fetch_id);
        }
        let background = queue.start_ready();
        assert_eq!(background.len(), 1);
        queue.finish(background[0].fetch_id);
        assert_eq!(queue.start_ready().len(), 0);

        for _ in 0..5 {
            push(&mut queue, 64 * MIB, FetchPriority::Background);
        }
        assert_eq!(queue.start_ready().len(), 4, "256 MiB in flight");
    }

    #[test]
    fn missing_ranges_become_whole_ranged_and_granted_fetches() {
        assert_eq!(
            plan_transfers(
                std::slice::from_ref(&(4096..8192)),
                5 * MIB,
                true,
                true,
                &DEFAULT_SIZES
            ),
            vec![(0..5 * MIB, FetchTransport::WholeFile)]
        );
        assert_eq!(
            plan_transfers(
                &[0..MIB, 2 * MIB..34 * MIB],
                100 * MIB,
                false,
                true,
                &DEFAULT_SIZES
            ),
            vec![
                (0..MIB, FetchTransport::Ranged),
                (2 * MIB..34 * MIB, FetchTransport::Grant)
            ]
        );
        assert_eq!(
            plan_transfers(
                std::slice::from_ref(&(0..20 * MIB)),
                100 * MIB,
                true,
                false,
                &DEFAULT_SIZES
            ),
            vec![
                (0..8 * MIB, FetchTransport::Ranged),
                (8 * MIB..16 * MIB, FetchTransport::Ranged),
                (16 * MIB..20 * MIB, FetchTransport::Ranged)
            ]
        );
    }
}
