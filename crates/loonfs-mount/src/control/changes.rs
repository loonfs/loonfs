//! The row changes that feed events and listings share: inserting an item,
//! reconciling an item with a newer state, replacing it with a new item,
//! and removing a subtree.

use super::effects::Effect;
use crate::config::CONTROL_DIRECTORY_NAME;
use crate::error::Result;
use crate::invalidation::{CachedPages, Invalidation};
use crate::item::{ItemId, ItemKind};
use crate::item_table::ItemTable;
use crate::read_handle::PinnedRevision;
use crate::records::{
    Binding, FileContent, HiddenBy, ItemRecord, RemoteState, Visibility, CONTENT_DIRTY,
};
use crate::tree::ItemView;
use loonfs_types::api::v0::PathEntry;
use loonfs_types::{ChangeSeq, InodeId};
use rusqlite::Connection;
use std::collections::HashSet;
use std::ffi::OsString;

/// The changes of one transaction: one commit's events, or one page of a
/// listing.
pub(super) struct Changes<'a> {
    /// The `items` table, whose statements the changes prepare once.
    pub(super) items: ItemTable<'a>,
    /// The commit's sequence, or the sequence that a listing's snapshot
    /// captured.
    pub(super) seq: ChangeSeq,
    pub(super) source: Source,
    /// The roots of directories that entered the mount and aren't shown yet.
    pub(super) withheld: &'a HashSet<ItemId>,
    pub(super) effects: Vec<Effect>,
    /// Entering roots that the changes removed.
    pub(super) left_entering: Vec<ItemId>,
    /// The `.loonfs` entry at the root that the changes withheld.
    pub(super) reserved: Option<InodeId>,
    /// Items added less items removed.
    pub(super) entry_change: i64,
}

/// Where a transaction's changes come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Source {
    /// A commit of the feed, made at `committed_at_ms`. A new file shows
    /// that time as its modification time, and so does each directory that
    /// the commit changes, so the tree follows from the feed alone.
    Commit { committed_at_ms: u64 },
    /// A page of a listing, whose entries include their own times.
    Listing,
}

/// What a transaction's changes did, once it commits.
pub(super) struct AppliedChanges {
    pub(super) effects: Vec<Effect>,
    pub(super) left_entering: Vec<ItemId>,
    pub(super) reserved: Option<InodeId>,
    pub(super) entry_change: i64,
}

/// How a newer state of a file's content relates to the state it replaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentChange {
    /// The content didn't change.
    None,
    /// The newer revision extends the same chain.
    Extended,
    /// The newer revision has other bytes.
    Replaced,
}

impl<'a> Changes<'a> {
    pub(super) fn new(
        connection: &'a Connection,
        seq: ChangeSeq,
        source: Source,
        withheld: &'a HashSet<ItemId>,
    ) -> Self {
        Self {
            items: ItemTable::new(connection),
            seq,
            source,
            withheld,
            effects: Vec::new(),
            left_entering: Vec::new(),
            reserved: None,
            entry_change: 0,
        }
    }

    /// Returns when the commit was made, or `None` for a listing.
    pub(super) fn committed_at_ms(&self) -> Option<u64> {
        match self.source {
            Source::Commit { committed_at_ms } => Some(committed_at_ms),
            Source::Listing => None,
        }
    }

    pub(super) fn finish(self) -> AppliedChanges {
        AppliedChanges {
            effects: self.effects,
            left_entering: self.left_entering,
            reserved: self.reserved,
            entry_change: self.entry_change,
        }
    }

    /// Returns whether the Local tree shows an item at `binding`.
    fn visibility(
        &mut self,
        item: ItemId,
        inode_id: InodeId,
        binding: Option<&Binding>,
    ) -> Visibility {
        if self.withheld.contains(&item) {
            return Visibility::Withheld;
        }
        match binding {
            Some(binding)
                if binding.parent == ItemId::ROOT && binding.name == CONTROL_DIRECTORY_NAME =>
            {
                self.reserved = Some(inode_id);
                Visibility::Withheld
            }
            _ => Visibility::Shown,
        }
    }

    /// Writes a record and shows it.
    pub(super) fn write(&mut self, record: &ItemRecord) -> Result<()> {
        self.items.write(record)?;
        self.effects.push(Effect::Show(ItemView::of(record)));
        Ok(())
    }

    /// Sets a directory's modification time to the time of the commit that
    /// changed an entry in it. A listing's entries include their own times, so
    /// a listing doesn't change it.
    fn touch(&mut self, directory: ItemId) -> Result<()> {
        let Source::Commit { committed_at_ms } = self.source else {
            return Ok(());
        };
        let Some(mut record) = self.items.find(directory)? else {
            return Ok(());
        };
        record.local.mtime_ms = committed_at_ms;
        self.write(&record)?;
        self.effects.push(Effect::Invalidate(Invalidation::Inode {
            item: directory,
            pages: CachedPages::None,
        }));
        Ok(())
    }

    /// Queues the invalidations of a binding that changed from `previous` to
    /// `current`, and touches both directories.
    fn rebound(
        &mut self,
        item: ItemId,
        previous: Option<(ItemId, String)>,
        current: Option<(ItemId, String)>,
    ) -> Result<()> {
        if previous == current {
            return Ok(());
        }
        if let Some((parent, name)) = previous {
            self.effects.push(Effect::Invalidate(Invalidation::Delete {
                parent,
                child: item,
                name: OsString::from(name),
            }));
            self.touch(parent)?;
        }
        if let Some((parent, name)) = current {
            self.effects.push(Effect::Invalidate(Invalidation::Entry {
                parent,
                name: OsString::from(name),
            }));
            self.touch(parent)?;
        }
        Ok(())
    }

    /// Adds a new item.
    pub(super) fn insert(
        &mut self,
        inode_id: InodeId,
        kind: ItemKind,
        state: RemoteState,
    ) -> Result<ItemId> {
        let item = self.items.allocate_id()?;
        let visibility = self.visibility(item, inode_id, state.binding.as_ref());
        let record = ItemRecord::clean(item, Some(inode_id), kind, state, visibility);
        self.write(&record)?;
        self.entry_change += 1;
        self.rebound(item, None, record.local.binding.clone())?;
        Ok(item)
    }

    /// Adds the root of a directory that entered the mount, which the Local
    /// tree doesn't show until the feed reaches its listing.
    pub(super) fn insert_withheld(
        &mut self,
        inode_id: InodeId,
        kind: ItemKind,
        state: RemoteState,
    ) -> Result<ItemId> {
        let item = self.items.allocate_id()?;
        let record = ItemRecord::clean(item, Some(inode_id), kind, state, Visibility::Withheld);
        self.write(&record)?;
        self.entry_change += 1;
        Ok(item)
    }

    /// Adds the item that an entry describes, or brings the item that has
    /// its inode to the entry's state. The entry's binding is `binding`, or
    /// its parent and name when its parent is in the mount. An entry whose
    /// parent isn't in the mount is skipped.
    pub(super) fn upsert_entry(
        &mut self,
        entry: &PathEntry,
        binding: Option<Binding>,
    ) -> Result<Option<ItemId>> {
        let binding = match binding {
            Some(binding) => binding,
            None => {
                let parent = match entry.parent_inode_id {
                    Some(parent) => self.items.find_id_by_inode(parent)?,
                    None => None,
                };
                let (Some(parent), Some(name), Some(binding_version)) = (
                    parent,
                    entry.display_name.as_ref(),
                    entry.binding_version.as_ref(),
                ) else {
                    return Ok(None);
                };
                Binding {
                    parent,
                    name: name.as_str().to_owned(),
                    binding_version: binding_version.clone(),
                }
            }
        };
        let Some((kind, state)) = RemoteState::from_entry(entry, Some(binding)) else {
            return Ok(None);
        };
        self.upsert_state(entry.inode_id, kind, state).map(Some)
    }

    /// Adds an item in `state`, or brings the item that has its inode to it.
    pub(super) fn upsert_state(
        &mut self,
        inode_id: InodeId,
        kind: ItemKind,
        state: RemoteState,
    ) -> Result<ItemId> {
        match self.items.find_by_inode(inode_id)? {
            Some(record) => self.reconcile(record, kind, state),
            None => self.insert(inode_id, kind, state),
        }
    }

    /// Brings an item to a newer state, group by group. A newer revision of
    /// other bytes, or a new kind, makes a new item.
    pub(super) fn reconcile(
        &mut self,
        mut record: ItemRecord,
        kind: ItemKind,
        state: RemoteState,
    ) -> Result<ItemId> {
        if record.server.binding != state.binding {
            record.server.binding = state.binding.clone();
            self.adopt_binding(&mut record)?;
        }
        let content_change = match (&record.server.content, &state.content) {
            (Some(current), Some(newer)) if newer.revision_no > current.revision_no => {
                if newer.content_ref.content_id == current.content_ref.content_id
                    && newer.content_ref.size_bytes >= current.content_ref.size_bytes
                {
                    ContentChange::Extended
                } else {
                    ContentChange::Replaced
                }
            }
            _ => ContentChange::None,
        };
        let newer_attributes = state.attributes.revision_no > record.server.attributes.revision_no;
        // A creation event has no attributes, so the kind of a file comes
        // from the attributes that the item keeps.
        let attributes = if newer_attributes {
            &state.attributes
        } else {
            &record.server.attributes
        };
        let kind = match kind {
            ItemKind::Directory => ItemKind::Directory,
            ItemKind::File | ItemKind::Symlink if attributes.target.is_some() => ItemKind::Symlink,
            ItemKind::File | ItemKind::Symlink => ItemKind::File,
        };
        if kind != record.kind || content_change == ContentChange::Replaced {
            let mut replacement = record.server.clone();
            if content_change == ContentChange::Replaced {
                replacement.content = state.content.clone();
            }
            if newer_attributes {
                replacement.attributes = state.attributes.clone();
            }
            if let Some(content) = replacement.content.clone() {
                replacement.set_content(content);
            }
            return self.replace(record, kind, replacement);
        }
        if content_change == ContentChange::Extended {
            let content = state
                .content
                .clone()
                .expect("an extended revision has content");
            self.extend(&mut record, content);
        }
        if newer_attributes {
            let content = record.server.content.clone();
            record.server.attributes = state.attributes;
            if let Some(content) = content {
                record.server.set_content(content);
            }
            self.adopt_attributes(&mut record);
        }
        self.write(&record)?;
        Ok(record.item_id)
    }

    /// Moves the server-observed binding into the Local tree when the
    /// binding is clean.
    pub(super) fn adopt_binding(&mut self, record: &mut ItemRecord) -> Result<()> {
        let inode_id = record
            .remote_inode_id
            .expect("an item that the feed changes has an inode");
        let visibility = self.visibility(record.item_id, inode_id, record.server.binding.as_ref());
        let previous = record.local.binding.clone();
        if record.adopt_binding(visibility) {
            self.rebound(record.item_id, previous, record.local.binding.clone())?;
        }
        Ok(())
    }

    /// Moves the server-observed attributes into the Local tree when they're
    /// clean.
    pub(super) fn adopt_attributes(&mut self, record: &mut ItemRecord) {
        if record.adopt_attributes() {
            self.effects.push(Effect::Invalidate(Invalidation::Inode {
                item: record.item_id,
                pages: CachedPages::None,
            }));
        }
    }

    /// Applies a revision that extends the file's chain: an append.
    pub(super) fn extend(&mut self, record: &mut ItemRecord, content: FileContent) {
        let previous = record
            .server
            .content
            .clone()
            .expect("an extended file has content");
        record.server.set_content(content.clone());
        if record.adopt_content() {
            let offset = previous.content_ref.size_bytes;
            self.effects.push(Effect::Appended {
                item: record.item_id,
                previous: previous.content_ref,
                current: PinnedRevision {
                    inode_id: record
                        .remote_inode_id
                        .expect("an appended file has an inode"),
                    revision_no: content.revision_no,
                    content_ref: content.content_ref.clone(),
                },
            });
            self.effects.push(Effect::Invalidate(Invalidation::Inode {
                item: record.item_id,
                pages: CachedPages::Range {
                    offset,
                    length: content.content_ref.size_bytes.saturating_sub(offset),
                },
            }));
        }
        // An append that starts at the previous size keeps `append_only` as
        // it was, also for an item with a local content change, whose own
        // append is re-based on the remote appends.
    }

    /// Makes the item's inode a new item in `state`, and keeps the old item
    /// for the descriptors that have it open, on the revision they opened.
    pub(super) fn replace(
        &mut self,
        record: ItemRecord,
        kind: ItemKind,
        state: RemoteState,
    ) -> Result<ItemId> {
        if record.write.dirty_groups & CONTENT_DIRTY != 0 {
            let mut record = record;
            record.server = state;
            record.append_only = false;
            self.items.write(&record)?;
            return Ok(record.item_id);
        }
        let inode_id = record
            .remote_inode_id
            .expect("a replaced item has an inode");
        self.items.delete(record.item_id)?;
        self.retire(&record);
        let item = self.items.allocate_id()?;
        let visibility = self.visibility(item, inode_id, state.binding.as_ref());
        let replacement = ItemRecord::clean(item, Some(inode_id), kind, state, visibility);
        self.write(&replacement)?;
        if let Some((parent, name)) = &record.local.binding {
            self.effects.push(Effect::Invalidate(Invalidation::Delete {
                parent: *parent,
                child: record.item_id,
                name: OsString::from(name),
            }));
            self.touch(*parent)?;
        }
        if replacement.local.binding != record.local.binding {
            self.rebound(item, None, replacement.local.binding.clone())?;
        }
        Ok(item)
    }

    /// Queues the effects of an item that left the tree.
    fn retire(&mut self, record: &ItemRecord) {
        self.effects.push(Effect::Detach(record.item_id));
        if let Some(content) = &record.committed.content {
            self.effects.push(Effect::Retired {
                item: record.item_id,
                content_id: content.content_ref.content_id.clone(),
            });
        }
        if self.withheld.contains(&record.item_id) {
            self.left_entering.push(record.item_id);
        }
    }

    /// Removes an item and everything below it, as a recursive delete does.
    /// An item with local changes stays, hidden by the deletion.
    pub(super) fn remove_subtree(&mut self, top: ItemId, deleted_by: InodeId) -> Result<()> {
        let Some(top_record) = self.items.find(top)? else {
            return Ok(());
        };
        let mut queue = vec![top];
        while let Some(item) = queue.pop() {
            queue.extend(self.items.server_children(item)?);
            let Some(mut record) = self.items.find(item)? else {
                continue;
            };
            if record.write.dirty_groups != 0 {
                record.hidden_by = Some(HiddenBy {
                    inode_id: deleted_by,
                    deleted_seq: self.seq,
                });
                self.items.write(&record)?;
                continue;
            }
            self.items.delete(item)?;
            self.entry_change -= 1;
            self.retire(&record);
        }
        self.rebound(top, top_record.local.binding.clone(), None)
    }
}

/// Writes the rows of one page of admission's listing, before the control
/// task starts.
pub(crate) fn write_listing(connection: &Connection, entries: &[PathEntry]) -> Result<()> {
    let withheld = HashSet::new();
    let mut changes = Changes::new(connection, ChangeSeq(0), Source::Listing, &withheld);
    for entry in entries {
        changes.upsert_entry(entry, None)?;
    }
    Ok(())
}
