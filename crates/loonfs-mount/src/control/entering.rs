//! Items that a move or an undelete brings into the mount, and the
//! directories among them, which the Local tree doesn't show until the
//! applied sequence reaches the snapshot they were listed at.

use super::changes::{Changes, Source};
use super::events::{ReadKey, ReadResult};
use super::ControlTask;
use crate::database::{self, Database, EnteringDirectory};
use crate::error::Result;
use crate::item::ItemId;
use crate::item_table::{find_item, find_item_id_by_inode};
use crate::records::{Binding, RemoteState};
use crate::scan::SNAPSHOT_TTL_MS;
use crate::workers::{EnteringItem, Listing, RootAncestry, SnapshotRef, Work};
use loonfs_types::api::v0::PathEntry;
use loonfs_types::{ChangeSeq, InodeId, PinId};
use std::collections::HashSet;

/// A directory that entered the mount and isn't shown yet.
#[derive(Debug, Clone)]
pub(super) struct EnteringState {
    record: EnteringDirectory,
    inode_id: InodeId,
    extended_at_ms: u64,
}

impl EnteringState {
    pub(super) fn root_item_id(&self) -> ItemId {
        self.record.root_item_id
    }

    pub(super) fn captured_seq(&self) -> ChangeSeq {
        self.record.captured_seq
    }

    pub(super) fn snapshot_id(&self) -> &PinId {
        &self.record.snapshot_id
    }
}

/// Loads the directories that entered the mount. A directory whose listing
/// a restart interrupted is removed, because the commit that brought it in
/// wasn't applied, so the feed brings it in again.
pub(super) fn load(database: &mut Database) -> Result<Vec<EnteringState>> {
    let records = database::read_entering_directories(database.connection())?;
    let mut entering = Vec::new();
    let transaction = database.transaction()?;
    let withheld = HashSet::new();
    let mut changes = Changes::new(&transaction, ChangeSeq(0), Source::Listing, &withheld);
    for record in records {
        let root = find_item(&transaction, record.root_item_id)?;
        match (record.listed, root.and_then(|root| root.remote_inode_id)) {
            (true, Some(inode_id)) => entering.push(EnteringState {
                record,
                inode_id,
                extended_at_ms: 0,
            }),
            _ => {
                changes.remove_subtree(record.root_item_id, InodeId(0))?;
                database::delete_entering_directory(&transaction, record.root_item_id)?;
            }
        }
    }
    drop(changes);
    transaction.commit()?;
    Ok(entering)
}

impl ControlTask {
    /// Records what a worker read for an item that entered the mount, and
    /// continues the page that waited for it.
    ///
    /// A read at a waiting directory's snapshot fails with
    /// `snapshot_not_found` once that snapshot expires, for example after the
    /// daemon was stopped for longer than its lifetime. Every directory that
    /// waits at the snapshot is then discarded and listed again at a new
    /// snapshot, and the page's read starts again, so the feed doesn't stop
    /// at the commit.
    pub(super) fn on_entering_item_read(
        &mut self,
        inode_id: InodeId,
        snapshot: Option<SnapshotRef>,
        result: Result<EnteringItem>,
    ) {
        let relisted = self.relisting.remove(&inode_id);
        match result {
            Ok(EnteringItem::Listed) => {
                let marked = self
                    .mark_listed(inode_id)
                    .and_then(|()| self.reveal_entering(None));
                if let Err(error) = marked {
                    if relisted {
                        tracing::warn!(%error, %inode_id, "recording a listing again failed");
                        if let Err(error) = self.drop_entering(inode_id) {
                            tracing::error!(%error, "removing an interrupted listing failed");
                        }
                        self.relist_on_tick.insert(inode_id);
                    } else {
                        self.fail_page(error);
                    }
                    return;
                }
                self.resume_page(
                    ReadKey::Item(inode_id),
                    ReadResult::Item(EnteringItem::Listed),
                );
            }
            Ok(item) => self.resume_page(ReadKey::Item(inode_id), ReadResult::Item(item)),
            Err(error) => {
                if let Err(error) = self.drop_entering(inode_id) {
                    tracing::error!(%error, "removing an interrupted listing failed");
                }
                let expired = snapshot.filter(|_| {
                    error.client_code() == Some(loonfs_types::ErrorCode::SnapshotNotFound)
                });
                if let Some(expired) = expired {
                    tracing::warn!(
                        snapshot_id = %expired.snapshot_id,
                        "a waiting directory's snapshot expired, so the mount lists it again"
                    );
                    match self.relist_expired(&expired.snapshot_id) {
                        Ok(()) => self.retry_awaited_read(),
                        Err(error) => self.fail_page(error),
                    }
                } else if relisted {
                    // No page waits for this listing, so the next tick
                    // starts it again.
                    tracing::warn!(%error, %inode_id, "listing a directory again failed");
                    self.relist_on_tick.insert(inode_id);
                } else {
                    self.fail_page(error);
                }
            }
        }
    }

    /// Lists again the directories whose listing at a new snapshot failed.
    pub(super) fn relist_failed_listings(&mut self) {
        for inode_id in std::mem::take(&mut self.relist_on_tick) {
            self.relist(inode_id);
        }
    }

    fn relist(&mut self, inode_id: InodeId) {
        self.relisting.insert(inode_id);
        self.workers.start(Work::ReadEnteringItem {
            inode_id,
            snapshot: None,
        });
    }

    /// Discards the directories that wait at an expired snapshot, and lists
    /// each one again at a new snapshot.
    fn relist_expired(&mut self, snapshot_id: &PinId) -> Result<()> {
        let expired: Vec<_> = self
            .entering
            .iter()
            .filter(|entering| entering.snapshot_id() == snapshot_id)
            .cloned()
            .collect();
        let roots: Vec<_> = expired.iter().map(EnteringState::root_item_id).collect();
        self.remove_entering_rows(&roots)?;
        self.entering
            .retain(|entering| entering.snapshot_id() != snapshot_id);
        for entering in expired {
            self.relist(entering.inode_id);
        }
        Ok(())
    }

    fn mark_listed(&mut self, inode_id: InodeId) -> Result<()> {
        let Some(entering) = self
            .entering
            .iter_mut()
            .find(|entering| entering.inode_id == inode_id)
        else {
            return Ok(());
        };
        entering.record.listed = true;
        database::write_entering_directory(self.database.connection(), &entering.record)?;
        Ok(())
    }

    /// Adds the root of a directory that entered the mount, withheld from the
    /// Local tree, before its listing's pages arrive.
    pub(super) fn on_entering_directory(
        &mut self,
        inode_id: InodeId,
        entry: &PathEntry,
        snapshot: SnapshotRef,
        created: bool,
    ) {
        if let Err(error) = self.add_entering_directory(inode_id, entry, &snapshot) {
            tracing::error!(%error, "adding a directory that entered the mount failed");
            if created {
                self.workers.start(Work::DeleteSnapshot {
                    snapshot_id: snapshot.snapshot_id,
                });
            }
        }
    }

    fn add_entering_directory(
        &mut self,
        inode_id: InodeId,
        entry: &PathEntry,
        snapshot: &SnapshotRef,
    ) -> Result<()> {
        let withheld = HashSet::new();
        let transaction = self.database.transaction()?;
        let mut changes = Changes::new(
            &transaction,
            snapshot.captured_seq,
            Source::Listing,
            &withheld,
        );
        let parent = match entry.parent_inode_id {
            Some(parent) => find_item_id_by_inode(&transaction, parent)?,
            None => None,
        };
        let binding = match (parent, &entry.display_name, &entry.binding_version) {
            (Some(parent), Some(name), Some(binding_version)) => Some(Binding {
                parent,
                name: name.as_str().to_owned(),
                binding_version: binding_version.clone(),
            }),
            _ => None,
        };
        let Some((kind, state)) = RemoteState::from_entry(entry, binding) else {
            return Ok(());
        };
        let root_item_id = changes.insert_withheld(inode_id, kind, state)?;
        let record = EnteringDirectory {
            root_item_id,
            snapshot_id: snapshot.snapshot_id.clone(),
            captured_seq: snapshot.captured_seq,
            listed: false,
        };
        database::write_entering_directory(&transaction, &record)?;
        let applied = changes.finish();
        transaction.commit()?;
        self.entries = self.entries.saturating_add_signed(applied.entry_change);
        self.entering.push(EnteringState {
            record,
            inode_id,
            extended_at_ms: self.environment.timer.monotonic_now_ms(),
        });
        self.apply_effects(applied.effects);
        Ok(())
    }

    /// Writes one page of a listing.
    pub(super) fn on_listing_page(&mut self, listing: Listing, entries: &[PathEntry]) {
        let written = match listing {
            Listing::Rebuild => self.write_rebuild_page(entries),
            Listing::Entering(_) => self.write_entering_page(entries),
        };
        if let Err(error) = written {
            tracing::error!(%error, "writing a listing page failed");
            self.feed_error = Some(error.to_string());
        }
    }

    fn write_entering_page(&mut self, entries: &[PathEntry]) -> Result<()> {
        let withheld = HashSet::new();
        let transaction = self.database.transaction()?;
        let mut changes = Changes::new(&transaction, ChangeSeq(0), Source::Listing, &withheld);
        for entry in entries {
            changes.upsert_entry(entry, None)?;
        }
        let applied = changes.finish();
        transaction.commit()?;
        self.entries = self.entries.saturating_add_signed(applied.entry_change);
        self.apply_effects(applied.effects);
        Ok(())
    }

    /// Removes a directory whose listing failed, so the feed reads it again.
    fn drop_entering(&mut self, inode_id: InodeId) -> Result<()> {
        let Some(position) = self
            .entering
            .iter()
            .position(|entering| entering.inode_id == inode_id && !entering.record.listed)
        else {
            return Ok(());
        };
        let entering = self.entering.remove(position);
        self.remove_entering_rows(&[entering.record.root_item_id])?;
        self.release_snapshot(entering.record.snapshot_id);
        Ok(())
    }

    fn remove_entering_rows(&mut self, roots: &[ItemId]) -> Result<()> {
        let withheld = HashSet::new();
        let transaction = self.database.transaction()?;
        let mut changes = Changes::new(&transaction, ChangeSeq(0), Source::Listing, &withheld);
        for root in roots {
            changes.remove_subtree(*root, InodeId(0))?;
            database::delete_entering_directory(&transaction, *root)?;
        }
        let applied = changes.finish();
        transaction.commit()?;
        self.entries = self.entries.saturating_add_signed(applied.entry_change);
        self.apply_effects(applied.effects);
        Ok(())
    }

    /// Removes every directory that entered the mount and isn't shown, as a
    /// rebuild does before it lists the whole tree again.
    pub(super) fn discard_entering(&mut self) -> Result<()> {
        let entering = std::mem::take(&mut self.entering);
        let roots: Vec<_> = entering.iter().map(EnteringState::root_item_id).collect();
        self.remove_entering_rows(&roots)?;
        let snapshots: HashSet<_> = entering
            .into_iter()
            .map(|entering| entering.record.snapshot_id)
            .collect();
        for snapshot_id in snapshots {
            self.release_snapshot(snapshot_id);
        }
        Ok(())
    }

    /// Forgets directories that a commit removed before they were shown.
    pub(super) fn forget_entering(&mut self, roots: &[ItemId]) {
        for root in roots {
            if let Some(position) = self
                .entering
                .iter()
                .position(|entering| entering.root_item_id() == *root)
            {
                let entering = self.entering.remove(position);
                self.release_snapshot(entering.record.snapshot_id);
            }
        }
    }

    /// Shows the directories whose snapshot the applied sequence reached.
    /// Each one's parent takes `committed_at_ms`, the time of the commit
    /// that the feed just applied, as its modification time, or keeps its
    /// time when it's `None`, at a restart.
    pub(super) fn reveal_entering(&mut self, committed_at_ms: Option<u64>) -> Result<()> {
        let applied_seq = self.applied_seq;
        let (ready, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.entering)
            .into_iter()
            .partition(|entering| entering.record.listed && entering.captured_seq() <= applied_seq);
        self.entering = waiting;
        if ready.is_empty() {
            return Ok(());
        }
        let withheld = HashSet::new();
        let source = committed_at_ms.map_or(Source::Listing, |committed_at_ms| Source::Commit {
            committed_at_ms,
        });
        let transaction = self.database.transaction()?;
        let mut changes = Changes::new(&transaction, applied_seq, source, &withheld);
        for entering in &ready {
            database::delete_entering_directory(&transaction, entering.root_item_id())?;
            if let Some(mut record) = find_item(&transaction, entering.root_item_id())? {
                changes.adopt_binding(&mut record)?;
                changes.write(&record)?;
            }
        }
        let applied = changes.finish();
        transaction.commit()?;
        self.apply_effects(applied.effects);
        for entering in ready {
            self.release_snapshot(entering.record.snapshot_id);
        }
        Ok(())
    }

    /// Deletes a snapshot once no directory that entered the mount reads at
    /// it.
    fn release_snapshot(&mut self, snapshot_id: PinId) {
        if self
            .entering
            .iter()
            .all(|entering| entering.record.snapshot_id != snapshot_id)
        {
            self.workers.start(Work::DeleteSnapshot { snapshot_id });
        }
    }

    /// Extends the snapshots of directories that wait for the feed, when
    /// less than half of their lifetime is left.
    pub(super) fn keep_entering_alive(&mut self) {
        let now_ms = self.environment.timer.monotonic_now_ms();
        let mut extended = HashSet::new();
        for entering in &mut self.entering {
            if now_ms.saturating_sub(entering.extended_at_ms) < SNAPSHOT_TTL_MS / 2 {
                continue;
            }
            entering.extended_at_ms = now_ms;
            if extended.insert(entering.record.snapshot_id.clone()) {
                self.workers.start(Work::ExtendSnapshot {
                    snapshot_id: entering.record.snapshot_id.clone(),
                });
            }
        }
    }

    /// Records the root's location that a worker read: for the commit that
    /// waits for it, or directly after a rebuild.
    pub(super) fn on_ancestors_read(&mut self, result: Result<RootAncestry>) {
        let location = match result {
            Ok(location) => Some(Box::new(location)),
            Err(error) => {
                tracing::warn!(%error, "reading the root's ancestors failed, so the mount keeps the ancestors it had");
                None
            }
        };
        if matches!(self.feed, super::feed::FeedState::WaitingForRead(_)) {
            self.resume_page(ReadKey::Ancestors, ReadResult::Ancestors(location));
            return;
        }
        let Some(location) = location else {
            return;
        };
        let written = database::write_root_path(self.database.connection(), &location.root.path)
            .and_then(|()| {
                database::write_ancestors(self.database.connection(), &location.ancestors)
            });
        match written {
            Ok(()) => {
                self.root.path = location.root.path;
                self.root.ancestors = location.ancestors;
            }
            Err(error) => tracing::error!(%error, "recording the root's ancestors failed"),
        }
    }
}
