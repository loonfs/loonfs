//! The in-memory copy of the Local tree, which serves `lookup`, `getattr`,
//! `readdir`, and `readlink` under a read lock.

use crate::item::{Attr, DirEntry, Entry, ItemId, ItemKind, LocalChanges};
use crate::records::ItemRecord;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::sync::atomic::{AtomicU64, Ordering};

/// What the Local tree shows for one item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ItemView {
    pub(crate) item: ItemId,
    /// The parent and name, or `None` for the root and for an item that the
    /// Local tree doesn't show.
    pub(crate) binding: Option<(ItemId, OsString)>,
    pub(crate) kind: ItemKind,
    pub(crate) size_bytes: u64,
    pub(crate) mode: u32,
    pub(crate) mtime_ms: u64,
    pub(crate) target: Option<OsString>,
    pub(crate) changes: LocalChanges,
}

impl ItemView {
    pub(crate) fn of(record: &ItemRecord) -> Self {
        let local = &record.local;
        Self {
            item: record.item_id,
            binding: local
                .binding
                .as_ref()
                .map(|(parent, name)| (*parent, OsString::from(name))),
            kind: record.kind,
            size_bytes: local.size_bytes,
            mode: local.mode,
            mtime_ms: local.mtime_ms,
            target: local.target.as_ref().map(OsString::from),
            changes: if record.write.dirty_groups == 0 {
                LocalChanges::None
            } else {
                LocalChanges::Unpublished
            },
        }
    }
}

struct TreeItem {
    binding: Option<(ItemId, OsString)>,
    /// The directory that the item is in, or was last in. The root's parent
    /// is the root.
    parent: ItemId,
    kind: ItemKind,
    size_bytes: u64,
    mode: u32,
    mtime_ms: u64,
    target: Option<OsString>,
    changes: LocalChanges,
    /// The item's position in its parent's `readdir` order.
    cookie: u64,
    /// The kernel's lookup count.
    lookups: AtomicU64,
    children: Option<Box<Children>>,
}

/// A directory's entries, by name for `lookup` and by cookie for `readdir`.
///
/// A cookie is assigned when a name is bound and never changes, so a
/// `readdir` offset stays valid while other entries are added and removed.
#[derive(Default)]
struct Children {
    by_name: BTreeMap<OsString, ItemId>,
    by_cookie: BTreeMap<u64, ItemId>,
}

impl TreeItem {
    fn attr(&self) -> Attr {
        Attr {
            kind: self.kind,
            parent: self.parent,
            size_bytes: self.size_bytes,
            mode: self.mode,
            mtime_ms: self.mtime_ms,
            changes: self.changes,
        }
    }
}

/// The Local tree in memory.
#[derive(Default)]
pub(crate) struct Tree {
    items: HashMap<ItemId, TreeItem>,
    /// Items that the tree doesn't show but keeps, because the kernel still
    /// has lookups or descriptors for them.
    detached: HashSet<ItemId>,
    next_cookie: u64,
}

impl Tree {
    /// Shows an item as `view` describes it, binding it under its parent and
    /// unbinding it from where it was.
    ///
    /// If another item has the name, that item stops being shown until a
    /// later change binds it again.
    pub(crate) fn show(&mut self, view: ItemView) {
        let existing_binding = self
            .items
            .get(&view.item)
            .and_then(|item| item.binding.clone());
        let rebinding = existing_binding != view.binding;
        if rebinding {
            self.unbind(view.item);
        }
        let item = self.items.entry(view.item).or_insert_with(|| TreeItem {
            binding: None,
            parent: ItemId::ROOT,
            kind: view.kind,
            size_bytes: 0,
            mode: 0,
            mtime_ms: 0,
            target: None,
            changes: LocalChanges::None,
            cookie: 0,
            lookups: AtomicU64::new(0),
            children: None,
        });
        assert!(
            item.kind == view.kind,
            "an item's kind should never change, because a new kind is a new item"
        );
        item.size_bytes = view.size_bytes;
        item.mode = view.mode;
        item.mtime_ms = view.mtime_ms;
        item.target = view.target;
        item.changes = view.changes;
        if let Some((parent, _)) = &view.binding {
            item.parent = *parent;
        }
        if view.kind == ItemKind::Directory && item.children.is_none() {
            item.children = Some(Box::default());
        }
        if rebinding {
            if let Some((parent, name)) = view.binding {
                self.bind(view.item, parent, name);
            }
        }
        if self.items[&view.item].binding.is_some() {
            self.detached.remove(&view.item);
        }
    }

    fn bind(&mut self, item: ItemId, parent: ItemId, name: OsString) {
        let occupant = self
            .items
            .get(&parent)
            .and_then(|parent| parent.children.as_ref())
            .and_then(|children| children.by_name.get(&name).copied());
        if let Some(occupant) = occupant {
            self.unbind(occupant);
        }
        let cookie = self.next_cookie + 1;
        self.next_cookie = cookie;
        let Some(children) = self
            .items
            .get_mut(&parent)
            .and_then(|parent| parent.children.as_mut())
        else {
            // The parent isn't in the tree yet, which happens while a listing
            // writes its rows. The item stays unbound until the parent is
            // shown and the item is shown again.
            return;
        };
        children.by_name.insert(name.clone(), item);
        children.by_cookie.insert(cookie, item);
        let item = self
            .items
            .get_mut(&item)
            .expect("a bound item should be in the tree");
        item.binding = Some((parent, name));
        item.cookie = cookie;
    }

    fn unbind(&mut self, item: ItemId) {
        let Some(tree_item) = self.items.get_mut(&item) else {
            return;
        };
        let Some((parent, name)) = tree_item.binding.take() else {
            return;
        };
        let cookie = tree_item.cookie;
        if let Some(children) = self
            .items
            .get_mut(&parent)
            .and_then(|parent| parent.children.as_mut())
        {
            if children.by_name.get(&name) == Some(&item) {
                children.by_name.remove(&name);
            }
            children.by_cookie.remove(&cookie);
        }
    }

    /// Stops showing an item that left the namespace view, and keeps it
    /// while the kernel or a descriptor uses it. [`Self::sweep`] removes it
    /// after that.
    pub(crate) fn detach(&mut self, item: ItemId) {
        if !self.items.contains_key(&item) {
            return;
        }
        self.unbind(item);
        let children = self
            .items
            .get_mut(&item)
            .and_then(|item| item.children.as_mut())
            .map(|children| std::mem::take(&mut children.by_name));
        for child in children.into_iter().flat_map(BTreeMap::into_values) {
            if let Some(child) = self.items.get_mut(&child) {
                child.binding = None;
            }
        }
        if let Some(children) = self
            .items
            .get_mut(&item)
            .and_then(|item| item.children.as_mut())
        {
            children.by_cookie.clear();
        }
        self.detached.insert(item);
    }

    /// Removes the detached items that have no kernel lookups and that
    /// `in_use` doesn't keep.
    pub(crate) fn sweep(&mut self, in_use: impl Fn(ItemId) -> bool) {
        let removable: Vec<_> = self
            .detached
            .iter()
            .copied()
            .filter(|item| {
                !in_use(*item)
                    && self.items.get(item).is_some_and(|tree_item| {
                        tree_item.binding.is_none() && tree_item.lookups.load(Ordering::SeqCst) == 0
                    })
            })
            .collect();
        for item in removable {
            self.detached.remove(&item);
            self.items.remove(&item);
        }
    }

    /// Returns the item that `name` in `parent` refers to, and counts a
    /// kernel lookup of it.
    pub(crate) fn lookup(&self, parent: ItemId, name: &OsStr) -> Option<Entry> {
        let item = *self
            .items
            .get(&parent)?
            .children
            .as_ref()?
            .by_name
            .get(name)?;
        let tree_item = self.items.get(&item)?;
        tree_item.lookups.fetch_add(1, Ordering::SeqCst);
        Some(Entry {
            item,
            attr: tree_item.attr(),
        })
    }

    /// Returns an item's attributes, including an item that the tree keeps
    /// only for the kernel.
    pub(crate) fn getattr(&self, item: ItemId) -> Option<Attr> {
        self.items.get(&item).map(TreeItem::attr)
    }

    /// Returns a symlink's target.
    pub(crate) fn readlink(&self, item: ItemId) -> Option<OsString> {
        self.items.get(&item)?.target.clone()
    }

    /// Returns up to `max_entries` entries of a directory after the entry
    /// whose offset is `offset`, or from the start when `offset` is zero.
    pub(crate) fn read_directory(
        &self,
        item: ItemId,
        offset: u64,
        max_entries: usize,
    ) -> Vec<DirEntry> {
        let Some(children) = self
            .items
            .get(&item)
            .and_then(|item| item.children.as_ref())
        else {
            return Vec::new();
        };
        children
            .by_cookie
            .range(offset.saturating_add(1)..)
            .take(max_entries)
            .filter_map(|(cookie, child)| {
                let tree_item = self.items.get(child)?;
                let (_, name) = tree_item.binding.as_ref()?;
                Some(DirEntry {
                    item: *child,
                    kind: tree_item.kind,
                    name: name.clone(),
                    offset: *cookie,
                })
            })
            .collect()
    }

    /// Subtracts `lookups` from the kernel's lookup count of an item, and
    /// returns whether the item left the tree and has no lookups left.
    pub(crate) fn forget(&self, item: ItemId, lookups: u64) -> bool {
        if let Some(tree_item) = self.items.get(&item) {
            let previous = tree_item
                .lookups
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                    Some(count.saturating_sub(lookups))
                })
                .expect("the update function always returns a count");
            assert!(
                previous >= lookups,
                "the kernel should forget no more lookups than it made"
            );
            return previous == lookups && self.detached.contains(&item);
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(item: u64, parent: Option<(u64, &str)>, kind: ItemKind) -> ItemView {
        ItemView {
            item: ItemId(item),
            binding: parent.map(|(parent, name)| (ItemId(parent), OsString::from(name))),
            kind,
            size_bytes: 0,
            mode: 0o644,
            mtime_ms: 0,
            target: None,
            changes: LocalChanges::None,
        }
    }

    /// Checks that `readdir` offsets stay valid while entries are added and
    /// removed, that a rename moves an entry, and that a detached item keeps
    /// its attributes until the kernel forgets it.
    #[test]
    fn readdir_offsets_survive_changes_and_detached_items_stay_until_forgotten() {
        let mut tree = Tree::default();
        tree.show(view(1, None, ItemKind::Directory));
        for (item, name) in [(2, "a"), (3, "b"), (4, "c")] {
            tree.show(view(item, Some((1, name)), ItemKind::File));
        }
        let first = tree.read_directory(ItemId(1), 0, 2);
        assert_eq!(
            first.iter().map(|entry| entry.item).collect::<Vec<_>>(),
            [ItemId(2), ItemId(3)]
        );
        tree.show(view(5, Some((1, "d")), ItemKind::File));
        tree.detach(ItemId(2));
        let rest = tree.read_directory(ItemId(1), first[1].offset, 10);
        assert_eq!(
            rest.iter().map(|entry| entry.item).collect::<Vec<_>>(),
            [ItemId(4), ItemId(5)]
        );

        tree.show(view(4, Some((1, "e")), ItemKind::File));
        assert!(tree.lookup(ItemId(1), OsStr::new("c")).is_none());
        let entry = tree
            .lookup(ItemId(1), OsStr::new("e"))
            .expect("the renamed entry");
        assert_eq!(entry.item, ItemId(4));

        tree.lookup(ItemId(1), OsStr::new("b"));
        tree.detach(ItemId(3));
        tree.sweep(|_| false);
        assert!(tree.getattr(ItemId(3)).is_some(), "the kernel has a lookup");
        assert!(tree.getattr(ItemId(2)).is_none());
        tree.forget(ItemId(3), 1);
        tree.sweep(|_| false);
        assert!(tree.getattr(ItemId(3)).is_none());
        assert!(tree.getattr(ItemId(4)).is_some());
    }

    /// Checks that the root's parent is the root, that an item's parent
    /// follows its moves, and that an item that left the tree keeps the
    /// directory it was last in.
    #[test]
    fn an_items_parent_follows_its_moves_and_stays_after_it_leaves() {
        let mut tree = Tree::default();
        tree.show(view(1, None, ItemKind::Directory));
        tree.show(view(2, Some((1, "src")), ItemKind::Directory));
        tree.show(view(3, Some((1, "a.txt")), ItemKind::File));
        let parent = |tree: &Tree, item| tree.getattr(ItemId(item)).expect("an item").parent;
        assert_eq!(parent(&tree, 1), ItemId::ROOT);
        assert_eq!(parent(&tree, 2), ItemId::ROOT);
        assert_eq!(parent(&tree, 3), ItemId::ROOT);

        tree.show(view(3, Some((2, "a.txt")), ItemKind::File));
        assert_eq!(parent(&tree, 3), ItemId(2));
        tree.lookup(ItemId(2), OsStr::new("a.txt"));
        tree.detach(ItemId(3));
        assert_eq!(parent(&tree, 3), ItemId(2));
    }
}
