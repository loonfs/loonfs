//! The mount's control directory, `.loonfs` at its root: a read-only
//! directory that the adapter shows beside the engine's tree.
//!
//! It contains `status.json`, the report that `loonfs status --json` prints
//! for the mount, and `seq`, the mount's applied sequence. Each open of one
//! of its files reads the report as it is at that moment.

use loonfs_mount::{Attr, ItemId, ItemKind, LocalChanges, Status, CONTROL_DIRECTORY_NAME};
use std::ffi::OsStr;

/// The first inode number of the control directory's items.
///
/// The engine numbers its items upward from one and stores each number in a
/// signed 64-bit database column, so it never reaches the top half of the
/// numbers, where the control directory's items are.
const FIRST_CONTROL_ITEM: u64 = 1 << 63;

/// One item of the control directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControlItem {
    /// `.loonfs` itself.
    Directory,
    /// `.loonfs/status.json`.
    StatusFile,
    /// `.loonfs/seq`.
    SeqFile,
}

impl ControlItem {
    const ALL: [Self; 3] = [Self::Directory, Self::StatusFile, Self::SeqFile];

    /// The control directory's files, in the order that `readdir` lists
    /// them.
    pub(crate) const FILES: [Self; 2] = [Self::StatusFile, Self::SeqFile];

    /// Returns the control item with an inode number, or `None` for an item
    /// of the engine.
    pub(crate) fn of(item: ItemId) -> Option<Self> {
        Self::ALL.into_iter().find(|control| control.item() == item)
    }

    /// Returns the control item that `name` in `parent` refers to.
    pub(crate) fn lookup(parent: ItemId, name: &OsStr) -> Option<Self> {
        match Self::of(parent) {
            None if parent == ItemId::ROOT && name == CONTROL_DIRECTORY_NAME => {
                Some(Self::Directory)
            }
            Some(Self::Directory) => Self::FILES
                .into_iter()
                .find(|control| name == control.name()),
            _ => None,
        }
    }

    /// Returns the item's inode number.
    pub(crate) fn item(self) -> ItemId {
        ItemId(
            FIRST_CONTROL_ITEM
                + match self {
                    Self::Directory => 0,
                    Self::StatusFile => 1,
                    Self::SeqFile => 2,
                },
        )
    }

    /// Returns the item's name in its directory.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Directory => CONTROL_DIRECTORY_NAME,
            Self::StatusFile => "status.json",
            Self::SeqFile => "seq",
        }
    }

    /// Returns the item's kind.
    pub(crate) fn kind(self) -> ItemKind {
        match self {
            Self::Directory => ItemKind::Directory,
            Self::StatusFile | Self::SeqFile => ItemKind::File,
        }
    }

    /// Returns a file's bytes for `status`, or `None` for the directory.
    pub(crate) fn contents(self, status: &Status) -> Option<Vec<u8>> {
        match self {
            Self::Directory => None,
            Self::StatusFile => {
                let mut bytes =
                    serde_json::to_vec_pretty(status).expect("a mount's status should encode");
                bytes.push(b'\n');
                Some(bytes)
            }
            Self::SeqFile => Some(format!("{}\n", status.applied_seq.0).into_bytes()),
        }
    }

    /// Returns the item's attributes for `status`, with the modification
    /// time of the mount's root.
    pub(crate) fn attr(self, status: &Status, root_mtime_ms: u64) -> Attr {
        let (mode, size_bytes) = match self.contents(status) {
            None => (0o555, 0),
            Some(bytes) => (0o444, bytes.len() as u64),
        };
        Attr {
            kind: self.kind(),
            parent: match self {
                Self::Directory => ItemId::ROOT,
                Self::StatusFile | Self::SeqFile => Self::Directory.item(),
            },
            size_bytes,
            mode,
            mtime_ms: root_mtime_ms,
            changes: LocalChanges::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_mount::{
        CacheStatus, MountAccess, MountMethod, Readiness, ReflinkSupport, Transports,
    };
    use loonfs_types::{AbsolutePath, ChangeSeq, NamespaceId};

    fn status() -> Status {
        Status {
            namespace_id: NamespaceId::parse("demo").expect("a namespace ID"),
            path: AbsolutePath::parse("/work").expect("a path"),
            access: MountAccess::ReadOnly,
            readiness: Readiness::Ready,
            mount_method: Some(MountMethod::Fusermount3),
            applied_seq: ChangeSeq(31),
            dirty_items: 0,
            oldest_unpublished_change_age_ms: None,
            pending_batch: None,
            uploads_in_flight: 0,
            cache: CacheStatus::default(),
            entries: 4,
            max_entries: 100_000,
            reflinks: ReflinkSupport::Supported,
            inline_limit_bytes: 0,
            transports: Transports::default(),
            conflicts: Vec::new(),
            refused_publications: Vec::new(),
            publication_stop: None,
            warnings: Vec::new(),
            feed_error: None,
            dropped_invalidations: 0,
        }
    }

    /// Checks that `status.json` is the engine's report, that `seq` is the
    /// applied sequence, and that each file's size is its content's.
    #[test]
    fn control_files_show_the_status_report_and_the_applied_sequence() {
        let status = status();
        let report = ControlItem::StatusFile
            .contents(&status)
            .expect("a file's contents");
        let decoded: Status = serde_json::from_slice(&report).expect("the report decodes");
        assert_eq!(decoded, status);
        assert_eq!(report.last(), Some(&b'\n'));
        assert_eq!(
            ControlItem::SeqFile.contents(&status),
            Some(b"31\n".to_vec())
        );
        for file in ControlItem::FILES {
            let attr = file.attr(&status, 9);
            let size = file.contents(&status).expect("a file's contents").len() as u64;
            assert_eq!(
                (attr.kind, attr.size_bytes, attr.mode),
                (ItemKind::File, size, 0o444)
            );
        }
        let directory = ControlItem::Directory.attr(&status, 9);
        assert_eq!(
            (directory.kind, directory.mode),
            (ItemKind::Directory, 0o555)
        );
    }

    /// Checks that `.loonfs` is found only at the mount's root, that its
    /// files are found only in it, and that its inode numbers map back.
    #[test]
    fn control_items_are_found_by_name_and_by_number() {
        let directory = ControlItem::lookup(ItemId::ROOT, OsStr::new(".loonfs"));
        assert_eq!(directory, Some(ControlItem::Directory));
        assert_eq!(ControlItem::lookup(ItemId(5), OsStr::new(".loonfs")), None);
        assert_eq!(
            ControlItem::lookup(ControlItem::Directory.item(), OsStr::new("seq")),
            Some(ControlItem::SeqFile)
        );
        assert_eq!(
            ControlItem::lookup(ControlItem::Directory.item(), OsStr::new("conflicts")),
            None
        );
        assert_eq!(ControlItem::lookup(ItemId::ROOT, OsStr::new("seq")), None);
        for control in ControlItem::ALL {
            assert_eq!(ControlItem::of(control.item()), Some(control));
        }
        assert_eq!(ControlItem::of(ItemId::ROOT), None);
        assert_eq!(ControlItem::of(ItemId(i64::MAX as u64)), None);
    }
}
