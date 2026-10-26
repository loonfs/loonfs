//! The reads that wait for bytes of a cache file.

use crate::cache_file::CacheFile;
use crate::error::Result;
use crate::read_handle::HandleId;
use loonfs_types::ContentId;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::oneshot;

/// A read that waits for bytes of one content.
pub(super) struct ReadWaiter {
    pub(super) handle: HandleId,
    pub(super) needed: Range<u64>,
    pub(super) reply: oneshot::Sender<Result<Arc<CacheFile>>>,
}

/// The reads that wait for bytes, by the content that they read.
///
/// It's bounded by the reads in flight: each waiter is one read of an open
/// handle, and it leaves when the read gets its reply, when its caller stops
/// waiting, or when its handle closes.
#[derive(Default)]
pub(super) struct ReadWaiters(HashMap<ContentId, Vec<ReadWaiter>>);

impl ReadWaiters {
    /// Adds a read, and drops the reads of the same content whose callers
    /// stopped waiting.
    pub(super) fn push(&mut self, content_id: ContentId, waiter: ReadWaiter) {
        let waiters = self.0.entry(content_id).or_default();
        waiters.retain(|waiter| !waiter.reply.is_closed());
        waiters.push(waiter);
    }

    /// Removes and returns the reads of `content_id`.
    pub(super) fn take(&mut self, content_id: &ContentId) -> Vec<ReadWaiter> {
        self.0.remove(content_id).unwrap_or_default()
    }

    /// Puts back the reads of `content_id` that still wait.
    pub(super) fn restore(&mut self, content_id: &ContentId, waiters: Vec<ReadWaiter>) {
        let waiters: Vec<_> = waiters
            .into_iter()
            .filter(|waiter| !waiter.reply.is_closed())
            .collect();
        if !waiters.is_empty() {
            self.0
                .entry(content_id.clone())
                .or_default()
                .extend(waiters);
        }
    }

    /// Returns the number of reads that wait.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.0.values().map(Vec::len).sum()
    }

    /// Drops the reads of a handle that closed.
    pub(super) fn remove_handle(&mut self, handle: HandleId) {
        self.0.retain(|_, waiters| {
            waiters.retain(|waiter| waiter.handle != handle);
            !waiters.is_empty()
        });
    }
}
