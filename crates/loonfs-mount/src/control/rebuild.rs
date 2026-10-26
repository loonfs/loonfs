//! The rebuild after `rebootstrap_required`, which lists the root's subtree
//! at a new snapshot and brings every item to the listed state.

use super::changes::{Changes, Source};
use super::feed::FeedState;
use super::ControlTask;
use crate::database;
use crate::error::Result;
use crate::item::ItemId;
use crate::item_table::{find_item, for_each_item};
use crate::records::RemoteState;
use crate::status::PublicationStop;
use crate::workers::{RebuildBasis, Work};
use loonfs_types::api::v0::PathEntry;
use loonfs_types::{ChangeSeq, InodeId};
use std::collections::HashSet;

/// The inodes that a rebuild's listing returned so far.
pub(super) struct RebuildState {
    listed: HashSet<InodeId>,
}

impl ControlTask {
    /// Starts listing the tree again at a new snapshot. For a mount without
    /// local changes, a rebuild is a new scan.
    pub(super) fn start_rebuild(&mut self) {
        if let Err(error) = self.discard_entering() {
            tracing::error!(%error, "discarding unshown directories before a rebuild failed");
        }
        self.feed = FeedState::Rebuilding(RebuildState {
            listed: HashSet::from([self.root.inode_id]),
        });
        self.workers.start(Work::Rebuild {
            root_inode_id: self.root.inode_id,
        });
    }

    /// Writes one page of the rebuild's listing.
    pub(super) fn write_rebuild_page(&mut self, entries: &[PathEntry]) -> Result<()> {
        let FeedState::Rebuilding(rebuild) = &mut self.feed else {
            return Ok(());
        };
        rebuild
            .listed
            .extend(entries.iter().map(|entry| entry.inode_id));
        let withheld = HashSet::new();
        let transaction = self.database.transaction()?;
        let mut changes = Changes::new(&transaction, self.applied_seq, Source::Listing, &withheld);
        for entry in entries {
            changes.upsert_entry(entry, None)?;
        }
        let applied = changes.finish();
        transaction.commit()?;
        self.entries = self.entries.saturating_add_signed(applied.entry_change);
        self.apply_effects(applied.effects);
        self.flush_invalidations();
        Ok(())
    }

    /// Finishes a rebuild: removes the items that the listing didn't return,
    /// and moves the applied sequence to the snapshot's.
    pub(super) fn on_rebuild_done(&mut self, result: Result<RebuildBasis>) {
        let FeedState::Rebuilding(rebuild) = std::mem::replace(&mut self.feed, FeedState::Idle)
        else {
            return;
        };
        let basis = match result {
            Ok(basis) => basis,
            Err(error) => {
                tracing::error!(%error, "a rebuild failed, and the next tick starts it again");
                self.feed_error = Some(error.to_string());
                self.feed = FeedState::RebuildNeeded;
                return;
            }
        };
        if let Err(error) = self.finish_rebuild(&rebuild.listed, &basis) {
            tracing::error!(%error, "finishing a rebuild failed");
            self.feed_error = Some(error.to_string());
            self.feed = FeedState::RebuildNeeded;
        }
        self.workers.start(Work::DeleteSnapshot {
            snapshot_id: basis.snapshot.snapshot_id.clone(),
        });
        if self.feed.is_rebuilding() {
            return;
        }
        match &basis.root {
            Some(_) => self.workers.start(Work::ReadAncestors {
                root_inode_id: self.root.inode_id,
            }),
            None => self.stop_publication(PublicationStop::RootDeleted {
                inode_id: self.root.inode_id,
                deleted_seq: basis.snapshot.captured_seq,
            }),
        }
        self.end_poll(None);
        self.request_poll();
    }

    fn finish_rebuild(&mut self, listed: &HashSet<InodeId>, basis: &RebuildBasis) -> Result<()> {
        let mut unlisted = Vec::new();
        for_each_item(self.database.connection(), |record| {
            let listed = record
                .remote_inode_id
                .is_some_and(|inode_id| listed.contains(&inode_id));
            if !listed && record.item_id != ItemId::ROOT {
                unlisted.push(record.item_id);
            }
        })?;
        let withheld = HashSet::new();
        let captured_seq: ChangeSeq = basis.snapshot.captured_seq;
        let transaction = self.database.transaction()?;
        let mut changes = Changes::new(&transaction, captured_seq, Source::Listing, &withheld);
        if let Some(root) = &basis.root {
            if let Some(record) = find_item(&transaction, ItemId::ROOT)? {
                if let Some((kind, state)) = RemoteState::from_entry(root, None) {
                    changes.reconcile(record, kind, state)?;
                }
            }
        }
        for item in unlisted {
            changes.remove_subtree(item, InodeId(0))?;
        }
        database::write_applied_seq(&transaction, captured_seq)?;
        let applied = changes.finish();
        transaction.commit()?;
        self.applied_seq = captured_seq;
        self.entries = self.entries.saturating_add_signed(applied.entry_change);
        self.apply_effects(applied.effects);
        self.flush_invalidations();
        Ok(())
    }
}
