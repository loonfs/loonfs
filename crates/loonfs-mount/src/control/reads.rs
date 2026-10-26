//! The read handles of the control task, and the fetches that their reads
//! wait for.

use super::read_waiters::ReadWaiter;
use super::ControlTask;
use crate::cache::{CacheEntryState, FillAttempt};
use crate::cache_file::CacheFile;
use crate::database;
use crate::error::{MountError, Result};
use crate::fetch::{plan_transfers, FetchId, FetchJob, FetchPriority, FetchTransport};
use crate::item::{ItemId, ItemKind};
use crate::item_table::{content_in_use, find_item};
use crate::read_handle::{HandleId, HandleView, OpenedRead, PinnedRevision};
use crate::window::FetchSizes;
use crate::workers::{Fetched, Work};
use loonfs_types::ContentId;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// One open read handle.
pub(super) struct OpenHandle {
    pub(super) item: ItemId,
    view: Arc<Mutex<HandleView>>,
}

impl OpenHandle {
    pub(super) fn view(&self) -> std::sync::MutexGuard<'_, HandleView> {
        self.view
            .lock()
            .expect("a read handle's view lock should not be poisoned")
    }
}

/// Which bytes a request fetches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestShape {
    /// The windows that reads asked for.
    Windows,
    /// The rest of a revision that open descriptors keep after it stopped
    /// being current: one grant from the first missing byte when the
    /// deployment advertises grants.
    Rest,
}

impl ControlTask {
    /// Opens a read handle on the revision that a file shows now.
    pub(super) fn open_read(&mut self, item: ItemId) -> Result<OpenedRead> {
        let record = find_item(self.database.connection(), item)?
            .ok_or(MountError::ItemNotFound { item })?;
        let (Some(content), Some(inode_id), ItemKind::File) = (
            record.committed.content,
            record.remote_inode_id,
            record.kind,
        ) else {
            return Err(MountError::NotAFile { item });
        };
        let revision = PinnedRevision {
            inode_id,
            revision_no: content.revision_no,
            content_ref: content.content_ref,
        };
        let content_id = revision.content_ref.content_id.clone();
        self.cache.touch(&content_id);
        let file = self
            .cache
            .entry(&content_id)
            .filter(|entry| !matches!(entry.state, CacheEntryState::Failed))
            .map(|entry| entry.file.clone());
        self.next_handle_id += 1;
        let handle = HandleId(self.next_handle_id);
        *self.pins.entry(content_id).or_default() += 1;
        let view = Arc::new(Mutex::new(HandleView { revision, file }));
        self.handles.insert(
            handle,
            OpenHandle {
                item,
                view: view.clone(),
            },
        );
        Ok(OpenedRead {
            handle,
            item,
            view,
            sizes: FetchSizes::of(&self.config),
        })
    }

    /// Replies to a read once its bytes are in the cache, fetching the window
    /// that contains it first.
    pub(super) fn on_read(
        &mut self,
        handle: HandleId,
        demand: Range<u64>,
        needed: Range<u64>,
        reply: oneshot::Sender<Result<Arc<CacheFile>>>,
    ) {
        let Some(revision) = self
            .handles
            .get(&handle)
            .map(|open| open.view().revision.clone())
        else {
            self.outbox.reply(reply, Err(MountError::Stopped));
            return;
        };
        let content_id = revision.content_ref.content_id.clone();
        let entry = match self.cache.prepare(&revision.content_ref) {
            Ok(entry) => entry,
            Err(error) => {
                self.outbox.reply(reply, Err(error));
                return;
            }
        };
        if matches!(entry.state, CacheEntryState::Failed) {
            self.outbox
                .reply(reply, Err(MountError::ContentMismatch { content_id }));
            return;
        }
        let present = entry.contains(&needed).then(|| entry.file.clone());
        self.set_view_files(&content_id);
        if let Some(file) = present {
            self.outbox.reply(reply, Ok(file));
            return;
        }
        let range = demand.start.min(needed.start)..demand.end.max(needed.end);
        if let Err(error) = self.request(
            &revision,
            range,
            FetchPriority::Demand,
            Some(handle),
            RequestShape::Windows,
        ) {
            self.outbox.reply(reply, Err(error));
            return;
        }
        self.fetches.promote(&content_id, &needed);
        self.read_waiters.push(
            content_id,
            ReadWaiter {
                handle,
                needed,
                reply,
            },
        );
        self.start_fetches();
    }

    /// Fetches the window that a sequential reader will read next.
    pub(super) fn on_read_ahead(&mut self, handle: HandleId, range: Range<u64>) {
        let Some(revision) = self
            .handles
            .get(&handle)
            .map(|open| open.view().revision.clone())
        else {
            return;
        };
        if let Err(error) = self.request(
            &revision,
            range,
            FetchPriority::Background,
            Some(handle),
            RequestShape::Windows,
        ) {
            tracing::debug!(%error, "a read-ahead wasn't requested");
        }
        self.start_fetches();
    }

    /// Closes a read handle. A revision that no item and no handle uses any
    /// more leaves the cache.
    pub(super) fn on_close_read(&mut self, handle: HandleId) {
        let Some(open) = self.handles.remove(&handle) else {
            return;
        };
        let content_id = open.view().revision.content_ref.content_id.clone();
        self.read_waiters.remove_handle(handle);
        self.unpin(&content_id);
        self.workers.start(Work::DropStream { handle });
        self.sweep_tree();
        self.evict(0);
    }

    pub(super) fn unpin(&mut self, content_id: &ContentId) {
        let Some(pins) = self.pins.get_mut(content_id) else {
            return;
        };
        *pins -= 1;
        if *pins > 0 {
            return;
        }
        self.pins.remove(content_id);
        let failed = self
            .cache
            .entry(content_id)
            .is_some_and(|entry| matches!(entry.state, CacheEntryState::Failed));
        let unused = matches!(
            content_in_use(self.database.connection(), content_id),
            Ok(false)
        );
        if failed || unused {
            self.remove_cache_file(content_id);
        }
    }

    pub(super) fn is_pinned(&self, content_id: &ContentId) -> bool {
        self.pins.get(content_id).is_some_and(|&pins| pins > 0)
    }

    /// Points every handle that reads `content_id` at its current cache
    /// file.
    pub(super) fn set_view_files(&mut self, content_id: &ContentId) {
        let file = self.cache.entry(content_id).map(|entry| entry.file.clone());
        for open in self.handles.values() {
            let mut view = open.view();
            if &view.revision.content_ref.content_id == content_id {
                view.file = file.clone();
            }
        }
    }

    /// Queues fetches for the bytes of `range` that the cache neither has
    /// nor fetches already.
    pub(super) fn request(
        &mut self,
        revision: &PinnedRevision,
        range: Range<u64>,
        priority: FetchPriority,
        owner: Option<HandleId>,
        shape: RequestShape,
    ) -> Result<()> {
        let size_bytes = revision.content_ref.size_bytes;
        let range = range.start..range.end.min(size_bytes);
        let pinned_bytes = self.pinned_bytes();
        let direct_get = self.transports.direct_get;
        let sizes = FetchSizes::of(&self.config);
        let max_pinned_bytes = self.config.max_pinned_bytes;
        let entry = self.cache.prepare(&revision.content_ref)?;
        let file = entry.file.clone();
        let Some(partial) = entry.state.partial_mut() else {
            return Ok(());
        };
        // A read of a file that's fetched again waits for the whole file, so
        // a request fetches every missing byte of the file's target.
        let range = match partial.attempt {
            FillAttempt::First => range,
            FillAttempt::Refetch => 0..partial.target.size_bytes.min(size_bytes),
        };
        let missing: Vec<Range<u64>> = partial
            .present
            .missing(range)
            .into_iter()
            .flat_map(|gap| partial.requested.missing(gap))
            .collect();
        let Some(first_missing) = missing.first().map(|gap| gap.start) else {
            return Ok(());
        };
        let nothing_cached = partial.present.is_empty() && partial.requested.is_empty();
        let whole_file = nothing_cached && size_bytes <= sizes.max_whole_file_bytes;
        let transfers = match shape {
            RequestShape::Rest if direct_get && !whole_file => {
                vec![(first_missing..size_bytes, FetchTransport::Grant)]
            }
            RequestShape::Rest | RequestShape::Windows => {
                plan_transfers(&missing, size_bytes, nothing_cached, direct_get, &sizes)
            }
        };
        let new_bytes: u64 = transfers
            .iter()
            .map(|(range, _)| range.end - range.start)
            .sum();
        if pinned_bytes.saturating_add(new_bytes) > max_pinned_bytes {
            return Err(MountError::NoSpace {
                reason: format!(
                    "pinned bytes would grow to {} past `--pinned-max-bytes` ({max_pinned_bytes})",
                    pinned_bytes.saturating_add(new_bytes)
                ),
            });
        }
        for (range, transport) in transfers {
            partial.requested.insert(range.clone());
            let job = FetchJob {
                fetch_id: self.fetches.next_id(),
                content_id: revision.content_ref.content_id.clone(),
                file: file.clone(),
                inode_id: revision.inode_id,
                revision_no: revision.revision_no,
                range,
                transport,
                stream_owner: match transport {
                    FetchTransport::Grant => owner,
                    FetchTransport::WholeFile | FetchTransport::Ranged => None,
                },
            };
            self.fetches.push(job, priority);
        }
        Ok(())
    }

    /// Starts the queued fetches that fit the shared limit and the free
    /// space that the mount keeps.
    pub(super) fn start_fetches(&mut self) {
        for job in self.fetches.start_ready() {
            let bytes = job.range.end - job.range.start;
            let has_space = self.evict(bytes);
            let still_cached = self
                .cache
                .entry(&job.content_id)
                .is_some_and(|entry| Arc::ptr_eq(&entry.file, &job.file));
            if !still_cached {
                // Eviction removed the partial file that the fetch would fill.
                self.fetches.finish(job.fetch_id);
                continue;
            }
            if has_space {
                self.workers.start(Work::Fetch(job));
                continue;
            }
            self.fetches.finish(job.fetch_id);
            if let Some(partial) = self
                .cache
                .entry_mut(&job.content_id)
                .and_then(|entry| entry.state.partial_mut())
            {
                partial.requested.remove(job.range.clone());
            }
            self.fail_waiters(
                &job.content_id,
                &job.range,
                &MountError::NoSpace {
                    reason: format!(
                        "the state directory's filesystem has less than `--cache-min-free` \
                         ({}) bytes free",
                        self.config.min_free_bytes
                    ),
                },
            );
        }
    }

    /// Records what a fetch wrote, replies to the reads it satisfied, and
    /// verifies the file once it has every byte.
    pub(super) fn on_fetched(&mut self, fetch_id: FetchId, result: Result<Fetched>) {
        let Some(running) = self.fetches.finish(fetch_id) else {
            return;
        };
        if let Some(handle) = running
            .stream_owner
            .filter(|handle| !self.handles.contains_key(handle))
        {
            // The handle closed while its grant fetch ran, and the fetch kept
            // the grant's stream for it after the drop that the close sent.
            self.workers.start(Work::DropStream { handle });
        }
        let content_id = running.content_id.clone();
        let current = self
            .cache
            .entry(&content_id)
            .is_some_and(|entry| Arc::ptr_eq(&entry.file, &running.file));
        if current {
            if let Some(partial) = self
                .cache
                .entry_mut(&content_id)
                .and_then(|entry| entry.state.partial_mut())
            {
                partial.requested.remove(running.range.clone());
            }
            match result {
                Ok(fetched) => self.cache.record_present(&content_id, fetched.written),
                Err(error) => {
                    tracing::warn!(%error, %content_id, "a fetch failed");
                    self.fail_waiters(&content_id, &running.range, &error);
                }
            }
            self.wake_waiters(&content_id);
            self.verify_when_present(&content_id);
        }
        self.start_fetches();
        self.evict(0);
    }

    pub(super) fn complete_cache_file(&mut self, content_id: &ContentId) {
        let completed = self.cache.complete(content_id).and_then(|row| {
            database::write_cache_file(self.database.connection(), &row).map_err(MountError::from)
        });
        if let Err(error) = completed {
            tracing::error!(%error, %content_id, "completing a cache file failed");
        }
    }

    pub(super) fn wake_waiters(&mut self, content_id: &ContentId) {
        let Some(entry) = self.cache.entry(content_id) else {
            return;
        };
        let failed = matches!(entry.state, CacheEntryState::Failed);
        let mut waiting = Vec::new();
        for waiter in self.read_waiters.take(content_id) {
            if failed {
                self.outbox.reply(
                    waiter.reply,
                    Err(MountError::ContentMismatch {
                        content_id: content_id.clone(),
                    }),
                );
            } else if entry.contains(&waiter.needed) {
                self.outbox.reply(waiter.reply, Ok(entry.file.clone()));
            } else {
                waiting.push(waiter);
            }
        }
        self.read_waiters.restore(content_id, waiting);
    }

    /// Fails the reads that wait for the bytes of `range`, or every read of
    /// a file that's fetched again, since those reads wait for the whole
    /// file.
    pub(super) fn fail_waiters(
        &mut self,
        content_id: &ContentId,
        range: &Range<u64>,
        error: &MountError,
    ) {
        let refetching = self
            .cache
            .entry(content_id)
            .is_some_and(|entry| entry.is_refetching());
        let mut waiting = Vec::new();
        for waiter in self.read_waiters.take(content_id) {
            let overlaps =
                refetching || (waiter.needed.start < range.end && range.start < waiter.needed.end);
            if overlaps {
                self.outbox.reply(waiter.reply, Err(error.clone()));
            } else {
                waiting.push(waiter);
            }
        }
        self.read_waiters.restore(content_id, waiting);
    }
}
