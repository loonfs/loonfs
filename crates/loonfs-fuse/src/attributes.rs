//! The attributes that the kernel caches for an item, and how long it caches
//! them.

use fuser::{FileAttr, FileType, Generation, INodeNo};
use loonfs_mount::{Attr, ItemId, ItemKind, LocalChanges, Status};
use std::time::{Duration, UNIX_EPOCH};

/// How long the kernel caches the entry and attributes of an item without
/// local changes. The engine invalidates a cached item as soon as the feed
/// changes the item, so this bounds how long the kernel can show stale
/// attributes after an invalidation is lost.
pub(crate) const CLEAN_ITEM_LIFETIME: Duration = Duration::from_secs(1);

/// The generation of every inode number, because the engine never reuses a
/// number.
pub(crate) const GENERATION: Generation = Generation(0);

/// The block size that `stat` and `statfs` report.
pub(crate) const BLOCK_BYTES: u32 = 4096;

/// Returns how long the kernel caches an item's entry and attributes: one
/// second for an item without local changes, and zero for an item with
/// them, so `stat` stays exact while a program writes.
pub(crate) fn cache_lifetime(changes: LocalChanges) -> Duration {
    match changes {
        LocalChanges::None => CLEAN_ITEM_LIFETIME,
        LocalChanges::Unpublished => Duration::ZERO,
    }
}

/// The owner that every item in a mount shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Owner {
    pub(crate) uid: u32,
    pub(crate) gid: u32,
}

impl Owner {
    /// Returns the owner that a mount's status reports: `--uid` and
    /// `--gid`, or the user and group that run the daemon.
    pub(crate) fn of(status: &Status) -> Self {
        Self {
            uid: status.uid,
            gid: status.gid,
        }
    }
}

/// Returns the kernel's file type of an item kind.
pub(crate) fn file_type(kind: ItemKind) -> FileType {
    match kind {
        ItemKind::File => FileType::RegularFile,
        ItemKind::Directory => FileType::Directory,
        ItemKind::Symlink => FileType::Symlink,
    }
}

/// Returns the `stat` fields of an item.
pub(crate) fn file_attr(item: ItemId, attr: &Attr, owner: Owner) -> FileAttr {
    let mtime = UNIX_EPOCH + Duration::from_millis(attr.mtime_ms);
    FileAttr {
        ino: INodeNo(item.0),
        size: attr.size_bytes,
        blocks: attr.size_bytes.div_ceil(512),
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: mtime,
        kind: file_type(attr.kind),
        perm: u16::try_from(attr.mode & 0o7777).expect("permission bits fit in 16 bits"),
        nlink: match attr.kind {
            ItemKind::Directory => 2,
            ItemKind::File | ItemKind::Symlink => 1,
        },
        uid: owner.uid,
        gid: owner.gid,
        rdev: 0,
        blksize: BLOCK_BYTES,
        flags: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that an item's lifetime follows whether it has local changes,
    /// and that its attributes show the mount's owner.
    #[test]
    fn lifetimes_follow_local_changes_and_attributes_show_the_owner() {
        assert_eq!(cache_lifetime(LocalChanges::None), Duration::from_secs(1));
        assert_eq!(cache_lifetime(LocalChanges::Unpublished), Duration::ZERO);

        let owner = Owner {
            uid: 1000,
            gid: 100,
        };
        let attr = Attr {
            kind: ItemKind::File,
            parent: ItemId::ROOT,
            size_bytes: 1025,
            mode: 0o100_640,
            mtime_ms: 1_500,
            changes: LocalChanges::None,
        };
        let stat = file_attr(ItemId(7), &attr, owner);
        assert_eq!(stat.ino, INodeNo(7));
        assert_eq!((stat.uid, stat.gid), (1000, 100));
        assert_eq!(stat.perm, 0o640);
        assert_eq!(stat.blocks, 3);
        assert_eq!(stat.kind, FileType::RegularFile);
        assert_eq!(stat.mtime, UNIX_EPOCH + Duration::from_millis(1_500));

        let directory = file_attr(
            ItemId(8),
            &Attr {
                kind: ItemKind::Directory,
                ..attr
            },
            owner,
        );
        assert_eq!((directory.kind, directory.nlink), (FileType::Directory, 2));
    }
}
