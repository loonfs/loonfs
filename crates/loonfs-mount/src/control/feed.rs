//! Following the change feed: strong polls every poll interval and after a
//! page that ends before the head, and one transaction per commit.

use super::changes::{Changes, Source};
use super::events::{apply_commit_events, CommitOutcome, ReadKey, ReadResult, ReadResults};
use super::rebuild::RebuildState;
use super::ControlTask;
use crate::database;
use crate::error::{MountError, Result};
use crate::status::PublicationStop;
use crate::workers::{SnapshotRef, Work};
use loonfs_client::ClientError;
use loonfs_types::api::v0::{Commit, ListChangesResponse};
use loonfs_types::{ChangeSeq, ErrorCode};
use std::collections::{HashSet, VecDeque};
use tokio::sync::oneshot;

/// Where the engine is in following the feed.
pub(super) enum FeedState {
    Idle,
    Polling,
    /// A commit of the current page needs a read before it can be applied.
    WaitingForRead(PageProgress),
    Rebuilding(RebuildState),
    /// A rebuild failed, and the next tick starts it again.
    RebuildNeeded,
}

impl FeedState {
    pub(super) fn is_rebuilding(&self) -> bool {
        matches!(self, Self::Rebuilding(_) | Self::RebuildNeeded)
    }
}

/// The commits of a page that aren't applied yet.
pub(super) struct PageProgress {
    pub(super) changes: VecDeque<Commit>,
    /// Whether the page ended before the head.
    pub(super) more: bool,
    /// The reads that the first commit in `changes` needed.
    pub(super) reads: ReadResults,
    /// The read that the first commit waits for.
    pub(super) awaiting: Option<ReadKey>,
}

/// A `refresh_at_least` call that waits.
pub(super) struct RefreshWaiter {
    seq: ChangeSeq,
    /// The poll that has to complete first: the first one that starts after
    /// the call.
    poll: u64,
    reply: oneshot::Sender<Result<()>>,
}

impl ControlTask {
    /// Polls the feed now, or as soon as the current poll and page are done.
    pub(super) fn request_poll(&mut self) {
        match self.feed {
            FeedState::Idle => self.start_poll(),
            FeedState::RebuildNeeded => self.start_rebuild(),
            FeedState::Polling | FeedState::WaitingForRead(_) | FeedState::Rebuilding(_) => {
                self.poll_requested = true;
            }
        }
    }

    fn start_poll(&mut self) {
        self.poll_requested = false;
        self.started_polls += 1;
        self.feed = FeedState::Polling;
        self.workers.start(Work::PollFeed {
            after_seq: self.applied_seq,
        });
    }

    /// Applies a feed page, or handles the error that the poll returned.
    pub(super) fn on_feed_page(
        &mut self,
        page: std::result::Result<ListChangesResponse, ClientError>,
    ) {
        if !matches!(self.feed, FeedState::Polling) {
            return;
        }
        match page {
            Ok(page) => {
                self.feed_error = None;
                self.continue_page(PageProgress {
                    more: page.next_after_seq.is_some(),
                    changes: page.changes.into(),
                    reads: ReadResults::new(),
                    awaiting: None,
                });
            }
            Err(error) if error.code() == Some(ErrorCode::RebootstrapRequired) => {
                tracing::warn!(
                    applied_seq = self.applied_seq.0,
                    "the feed passed the retention floor, so the mount lists its tree again"
                );
                self.start_rebuild();
            }
            Err(error) if error.code() == Some(ErrorCode::NamespaceDeleted) => {
                self.stop_publication(PublicationStop::NamespaceDeleted);
                self.end_poll(Some(MountError::from(error)));
            }
            Err(error) => {
                self.feed_error = Some(error.to_string());
                self.end_poll(Some(MountError::from(error)));
            }
        }
    }

    /// Applies the page's commits in order, one transaction each, until the
    /// page ends or a commit needs a read.
    pub(super) fn continue_page(&mut self, mut progress: PageProgress) {
        while let Some(commit) = progress.changes.front() {
            if commit.committed_seq <= self.applied_seq {
                progress.changes.pop_front();
                continue;
            }
            match self.apply_commit(commit, &progress.reads) {
                Ok(CommitOutcome::Applied) => {
                    let committed_at_ms = commit.committed_at_ms;
                    progress.changes.pop_front();
                    progress.reads.clear();
                    if let Err(error) = self.reveal_entering(Some(committed_at_ms)) {
                        self.fail_page(error);
                        return;
                    }
                }
                Ok(CommitOutcome::NeedsRead(key)) => {
                    let seq = commit.committed_seq;
                    self.start_read(&key, seq);
                    progress.awaiting = Some(key);
                    self.feed = FeedState::WaitingForRead(progress);
                    return;
                }
                Err(error) => {
                    self.fail_page(error);
                    return;
                }
            }
        }
        self.flush_invalidations();
        if progress.more {
            self.start_poll();
        } else {
            self.end_poll(None);
        }
    }

    /// Records a read that the current page waited for, and continues the
    /// page.
    pub(super) fn resume_page(&mut self, key: ReadKey, result: ReadResult) {
        if !matches!(self.feed, FeedState::WaitingForRead(_)) {
            return;
        }
        let FeedState::WaitingForRead(mut progress) =
            std::mem::replace(&mut self.feed, FeedState::Idle)
        else {
            return;
        };
        let awaited = progress.awaiting == Some(key);
        progress.reads.insert(key, result);
        if !awaited {
            self.feed = FeedState::WaitingForRead(progress);
            return;
        }
        progress.awaiting = None;
        self.continue_page(progress);
    }

    /// Abandons the current page after an error. The next poll reads it
    /// again from the applied sequence.
    pub(super) fn fail_page(&mut self, error: MountError) {
        tracing::error!(%error, "applying a feed page failed");
        self.feed_error = Some(error.to_string());
        self.flush_invalidations();
        self.end_poll(Some(error));
    }

    /// Ends the current poll, and replies to the `refresh_at_least` calls that
    /// it satisfies, or fails the ones it was for.
    pub(super) fn end_poll(&mut self, error: Option<MountError>) {
        self.feed = FeedState::Idle;
        self.completed_polls = self.started_polls;
        let completed = self.completed_polls;
        let applied_seq = self.applied_seq;
        let waiters = std::mem::take(&mut self.refresh_waiters);
        for waiter in waiters {
            if waiter.poll > completed {
                self.refresh_waiters.push(waiter);
            } else if let Some(error) = &error {
                self.outbox.reply(waiter.reply, Err(error.clone()));
            } else if waiter.seq <= applied_seq {
                self.outbox.reply(waiter.reply, Ok(()));
            } else {
                self.refresh_waiters.push(RefreshWaiter {
                    poll: completed + 1,
                    ..waiter
                });
            }
        }
        // A call that waits for a later sequence waits for the next tick's
        // poll, so it never makes the engine poll in a loop.
        if self.poll_requested {
            self.request_poll();
        }
    }

    /// Waits until a poll that starts after now completes with the applied
    /// sequence at `seq` or later.
    pub(super) fn on_refresh(&mut self, seq: ChangeSeq, reply: oneshot::Sender<Result<()>>) {
        self.refresh_waiters.push(RefreshWaiter {
            seq,
            poll: self.started_polls + 1,
            reply,
        });
        self.request_poll();
    }

    /// Records why publication stopped, the first time it stops.
    pub(super) fn stop_publication(&mut self, stop: PublicationStop) {
        if self.publication_stop.is_some() {
            return;
        }
        tracing::error!(?stop, "the mount stopped publishing");
        if let Err(error) = database::write_publication_stop(self.database.connection(), &stop) {
            tracing::error!(%error, "recording why publication stopped failed");
        }
        self.publication_stop = Some(stop);
    }

    /// Applies one commit in one transaction, or rolls it back when an event
    /// needs a read first.
    fn apply_commit(&mut self, commit: &Commit, reads: &ReadResults) -> Result<CommitOutcome> {
        let withheld: HashSet<_> = self
            .entering
            .iter()
            .map(|entering| entering.root_item_id())
            .collect();
        let mut root = self.root.clone();
        let transaction = self.database.transaction()?;
        let source = Source::Commit {
            committed_at_ms: commit.committed_at_ms,
        };
        let mut changes = Changes::new(&transaction, commit.committed_seq, source, &withheld);
        let mut stop = None;
        if let Some(key) = apply_commit_events(&mut changes, &mut root, &mut stop, commit, reads)? {
            return Ok(CommitOutcome::NeedsRead(key));
        }
        if root != self.root {
            database::write_root_path(&transaction, &root.path)?;
            database::write_ancestors(&transaction, &root.ancestors)?;
        }
        for item in &changes.left_entering {
            database::delete_entering_directory(&transaction, *item)?;
        }
        database::write_applied_seq(&transaction, commit.committed_seq)?;
        let applied = changes.finish();
        transaction.commit()?;
        self.applied_seq = commit.committed_seq;
        self.root = root;
        if applied.reserved.is_some() {
            self.reserved_name = applied.reserved;
        }
        self.entries = self.entries.saturating_add_signed(applied.entry_change);
        if let Some(stop) = stop {
            self.stop_publication(stop);
        }
        self.forget_entering(&applied.left_entering);
        self.apply_effects(applied.effects);
        Ok(CommitOutcome::Applied)
    }

    /// Starts the read that a commit waits for. A read of an item between a
    /// listing's commit and its snapshot reads at that snapshot.
    pub(super) fn start_read(&mut self, key: &ReadKey, seq: ChangeSeq) {
        match key {
            ReadKey::Item(inode_id) => {
                let snapshot = self
                    .entering
                    .iter()
                    .filter(|entering| entering.captured_seq() >= seq)
                    .min_by_key(|entering| entering.captured_seq())
                    .map(|entering| SnapshotRef {
                        snapshot_id: entering.snapshot_id().clone(),
                        captured_seq: entering.captured_seq(),
                    });
                self.workers.start(Work::ReadEnteringItem {
                    inode_id: *inode_id,
                    snapshot,
                });
            }
            ReadKey::Ancestors => self.workers.start(Work::ReadAncestors {
                root_inode_id: self.root.inode_id,
            }),
        }
    }
}
