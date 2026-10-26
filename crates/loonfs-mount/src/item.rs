//! Item identities, and the metadata that the engine returns for each item.

use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fmt;

/// The local number of one item (a file, directory, or symlink) in a mount.
///
/// It's also the inode number that the kernel shows. The engine assigns a new
/// number to each item and never reuses one, and the mount's root is always
/// [`ItemId::ROOT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ItemId(pub u64);

impl ItemId {
    /// The mount's root directory.
    pub const ROOT: ItemId = ItemId(1);
}

impl fmt::Display for ItemId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// What kind of item an item is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemKind {
    /// A regular file.
    File,
    /// A directory.
    Directory,
    /// A symbolic link, which LoonFS stores as an empty file with the
    /// `fs.type` and `fs.target` attributes.
    Symlink,
}

impl ItemKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
            Self::Symlink => "symlink",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "file" => Some(Self::File),
            "directory" => Some(Self::Directory),
            "symlink" => Some(Self::Symlink),
            _ => None,
        }
    }
}

/// Whether an item has changes on this host that aren't published yet.
///
/// The adapter sets the kernel's cache lifetime to one second for an item
/// without local changes, and to zero for an item with them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalChanges {
    /// The item shows its published state.
    None,
    /// The item has changes that aren't published yet.
    Unpublished,
}

/// The attributes of one item, as the Local tree shows them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attr {
    /// What kind of item it is.
    pub kind: ItemKind,
    /// The directory that contains the item, which a listing shows as `..`.
    /// The root's parent is the root, and an item that left the tree keeps
    /// the directory it was last in.
    pub parent: ItemId,
    /// The file's size, the length of a symlink's target, or zero for a
    /// directory.
    pub size_bytes: u64,
    /// The permission bits, such as `0o644`.
    pub mode: u32,
    /// The modification time, in milliseconds since the Unix epoch.
    pub mtime_ms: u64,
    /// Whether the item has unpublished local changes.
    pub changes: LocalChanges,
}

/// An item that a name in a directory refers to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The item.
    pub item: ItemId,
    /// The item's attributes.
    pub attr: Attr,
}

/// One entry of a directory listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    /// The item that the name refers to.
    pub item: ItemId,
    /// What kind of item it is.
    pub kind: ItemKind,
    /// The entry's name.
    pub name: OsString,
    /// The offset to pass to `readdir` to continue after this entry.
    pub offset: u64,
}
