//! The cache files of the control task: verification, the refetch after a
//! mismatch, appends and replacements of open files, and eviction.

use super::reads::RequestShape;
use super::ControlTask;
use crate::cache::Verification as VerificationState;
use crate::cache::{FillAttempt, VerifiedPrefix};
use crate::cache_file::CacheFile;
use crate::database;
use crate::error::{MountError, Result};
use crate::fetch::FetchPriority;
use crate::invalidation::{CachedPages, Invalidation};
use crate::item::ItemId;
use crate::item_table::{content_in_use, items_with_content};
use crate::read_handle::PinnedRevision;
use crate::verify::{Verification, VerifyJob};
use crate::workers::Work;
use loonfs_types::{ContentId, ContentRef};
use std::sync::Arc;

impl ControlTask {
    /// Starts verifying a partial file once its present ranges cover its
    /// revision.
    pub(super) fn verify_when_present(&mut self, content_id: &ContentId) {
        let Some(entry) = self.cache.entry_mut(content_id) else {
            return;
        };
        let file = entry.file.clone();
        let Some(partial) = entry.state.partial_mut() else {
            return;
        };
        if partial.verification == VerificationState::Running
            || !partial.present.contains(&(0..partial.target.size_bytes))
        {
            return;
        }
        partial.verification = VerificationState::Running;
        let job = VerifyJob {
            content_id: content_id.clone(),
            file,
            target: partial.target.clone(),
            verified_prefix: partial.verified_prefix.clone(),
        };
        self.workers.start(Work::Verify(job));
    }

    /// Completes a file that matched its checksum, and replies to the reads
    /// that wait for it. A file that didn't match is fetched again once, its
    /// reads wait for the second fetch, and they fail after a second
    /// mismatch. Bytes that reads had before the first mismatch aren't
    /// checked again.
    pub(super) fn on_verified(
        &mut self,
        content_id: &ContentId,
        file: &Arc<CacheFile>,
        target: &ContentRef,
        result: Result<Verification>,
    ) {
        let Some(entry) = self.cache.entry_mut(content_id) else {
            return;
        };
        if !Arc::ptr_eq(&entry.file, file) {
            return;
        }
        let Some(partial) = entry.state.partial_mut() else {
            return;
        };
        partial.verification = VerificationState::Idle;
        match result {
            Ok(Verification::Matches) if target.size_bytes == partial.target.size_bytes => {
                self.complete_cache_file(content_id);
                self.wake_waiters(content_id);
                self.evict(0);
            }
            Ok(Verification::Matches) => {
                // An append extended the file while it was verified, so the
                // verified bytes are a prefix of the new revision.
                partial.verified_prefix = Some(VerifiedPrefix {
                    size_bytes: target.size_bytes,
                    checksum: target.checksum.clone(),
                });
                self.verify_when_present(content_id);
            }
            Ok(Verification::Mismatch) | Err(_) => {
                let attempt = partial.attempt;
                let size_bytes = partial.target.size_bytes;
                self.mismatched_files += 1;
                tracing::warn!(%content_id, ?attempt, "a cache file didn't match its checksum");
                self.invalidate_content(content_id, size_bytes);
                match attempt {
                    FillAttempt::First => self.refetch(content_id),
                    FillAttempt::Refetch => {
                        self.cache.fail(content_id);
                        self.set_view_files(content_id);
                        self.wake_waiters(content_id);
                    }
                }
            }
        }
    }

    /// Drops the kernel's cached pages of every item that shows or reads
    /// `content_id`, because reads returned the content's bytes before they
    /// were verified, and the next open would keep those pages.
    fn invalidate_content(&mut self, content_id: &ContentId, size_bytes: u64) {
        let mut items: Vec<ItemId> = self
            .handles
            .values()
            .filter(|open| &open.view().revision.content_ref.content_id == content_id)
            .map(|open| open.item)
            .collect();
        match items_with_content(self.database.connection(), content_id) {
            Ok(shown) => items.extend(shown),
            Err(error) => {
                tracing::error!(%error, %content_id, "finding the items of a content failed")
            }
        }
        items.sort_unstable();
        items.dedup();
        for item in items {
            self.invalidations.push(Invalidation::Inode {
                item,
                pages: CachedPages::Range {
                    offset: 0,
                    length: size_bytes,
                },
            });
        }
        self.flush_invalidations();
    }

    /// Deletes a file that didn't match its checksum, and fetches its
    /// revision again for the handles that read it.
    fn refetch(&mut self, content_id: &ContentId) {
        // The queued fetches would write the deleted file.
        self.fetches.drop_queued(content_id);
        if let Err(error) = self.cache.restart(content_id) {
            tracing::error!(%error, %content_id, "restarting a cache file failed");
            self.cache.fail(content_id);
            self.set_view_files(content_id);
            self.wake_waiters(content_id);
            return;
        }
        self.set_view_files(content_id);
        let Some(target_size) = self
            .cache
            .entry_mut(content_id)
            .and_then(|entry| entry.state.partial_mut())
            .map(|partial| partial.target.size_bytes)
        else {
            return;
        };
        // The file is verified against its own target, so the second fetch
        // reads the target's bytes from a revision that has all of them.
        let revision = self
            .handles
            .values()
            .map(|open| open.view().revision.clone())
            .filter(|revision| {
                &revision.content_ref.content_id == content_id
                    && revision.content_ref.size_bytes >= target_size
            })
            .min_by_key(|revision| revision.content_ref.size_bytes);
        let Some(revision) = revision else {
            // The handles that read the file read shorter revisions only,
            // so no fetch can fill the file for its verification, and its
            // reads fail.
            tracing::warn!(%content_id, "no open revision covers a cache file that's fetched again");
            self.cache.fail(content_id);
            self.set_view_files(content_id);
            self.wake_waiters(content_id);
            return;
        };
        if let Err(error) = self.request(
            &revision,
            0..target_size,
            FetchPriority::Demand,
            None,
            RequestShape::Windows,
        ) {
            tracing::warn!(%error, %content_id, "fetching a cache file again failed");
        }
        self.start_fetches();
    }

    /// Moves a file's open handles to the revision that an append made, and
    /// fetches the appended bytes at once when a handle has the file open.
    pub(super) fn on_appended(
        &mut self,
        item: ItemId,
        previous: &ContentRef,
        current: PinnedRevision,
    ) {
        let mut followed = false;
        for open in self.handles.values().filter(|open| open.item == item) {
            let mut view = open.view();
            let previous_content = view.revision.content_ref.content_id.clone();
            view.revision = current.clone();
            drop(view);
            followed = true;
            if let Some(pins) = self.pins.get_mut(&previous_content) {
                *pins -= 1;
            }
            *self
                .pins
                .entry(current.content_ref.content_id.clone())
                .or_default() += 1;
        }
        self.pins.retain(|_, pins| *pins > 0);
        let new_chain = previous.content_id != current.content_ref.content_id;
        if new_chain && !self.is_pinned(&previous.content_id) {
            if let Err(error) = self.cache.continue_chain(previous, &current.content_ref) {
                tracing::warn!(%error, "moving a cache file to a new chain failed");
            }
        }
        if !followed {
            return;
        }
        if let Err(error) = self.request(
            &current,
            previous.size_bytes..current.content_ref.size_bytes,
            FetchPriority::Background,
            None,
            RequestShape::Windows,
        ) {
            tracing::warn!(%error, "fetching an appended range failed");
        }
        self.set_view_files(&current.content_ref.content_id);
        self.start_fetches();
    }

    /// Fetches the rest of a revision that stopped being current for the
    /// handles that keep it, and deletes its cache file when nothing keeps
    /// it.
    pub(super) fn on_retired(&mut self, item: ItemId, content_id: &ContentId) {
        let revisions: Vec<PinnedRevision> = self
            .handles
            .values()
            .filter(|open| open.item == item)
            .map(|open| open.view().revision.clone())
            .collect();
        for revision in revisions {
            let complete = self
                .cache
                .entry(&revision.content_ref.content_id)
                .is_some_and(|entry| entry.contains(&(0..revision.content_ref.size_bytes)));
            if complete {
                continue;
            }
            let size_bytes = revision.content_ref.size_bytes;
            if let Err(error) = self.request(
                &revision,
                0..size_bytes,
                FetchPriority::Background,
                None,
                RequestShape::Rest,
            ) {
                tracing::warn!(%error, "fetching the rest of a replaced revision failed");
            }
            self.set_view_files(&revision.content_ref.content_id);
        }
        self.start_fetches();
        if !self.is_pinned(content_id)
            && matches!(
                content_in_use(self.database.connection(), content_id),
                Ok(false)
            )
        {
            self.remove_cache_file(content_id);
        }
    }

    pub(super) fn remove_cache_file(&mut self, content_id: &ContentId) {
        // A queued fetch of a removed partial file would write a deleted file.
        self.fetches.drop_queued(content_id);
        let removed = self.cache.remove(content_id).and_then(|()| {
            database::delete_cache_file(self.database.connection(), content_id)
                .map_err(MountError::from)
        });
        if let Err(error) = removed {
            tracing::error!(%error, %content_id, "deleting a cache file failed");
        }
        // No fetch writes the removed file, so a read that waits for it
        // fails instead of waiting for good.
        self.fail_waiters(
            content_id,
            &(0..u64::MAX),
            &MountError::CacheFileRemoved {
                content_id: content_id.clone(),
            },
        );
    }

    /// Evicts files, partial or complete, that no handle has open, least
    /// recently used first, until evictable bytes are under
    /// `--cache-max-bytes` and the filesystem has `--cache-min-free` free plus
    /// `incoming_bytes`. Returns whether it has that much free.
    pub(super) fn evict(&mut self, incoming_bytes: u64) -> bool {
        let needed_free = self.config.min_free_bytes.saturating_add(incoming_bytes);
        loop {
            let available = match self
                .environment
                .disk
                .available_bytes(self.cache.directory())
            {
                Ok(available) => available,
                Err(error) => {
                    tracing::warn!(%error, "reading the cache's free space failed");
                    u64::MAX
                }
            };
            let pins = &self.pins;
            let pinned =
                |content_id: &ContentId| pins.get(content_id).is_some_and(|&pins| pins > 0);
            let over_target = self.cache.evictable_bytes(pinned) > self.config.max_cache_bytes;
            if !over_target && available >= needed_free {
                return true;
            }
            let Some(victim) = self.cache.least_recently_used(pinned) else {
                return available >= needed_free;
            };
            self.remove_cache_file(&victim);
        }
    }
}
