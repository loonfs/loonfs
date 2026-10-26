//! The requests and handle of the engine's write path, which a read-only
//! mount refuses.

use crate::error::Result;
use crate::item::ItemId;
use bytes::Bytes;
use std::convert::Infallible;
use std::ffi::OsString;

/// What a write open opens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteTarget {
    /// An existing file.
    Existing(ItemId),
    /// A new file, created by the open.
    New {
        /// The new file's parent directory.
        parent: ItemId,
        /// The new file's name.
        name: OsString,
        /// The new file's permission bits.
        mode: u32,
    },
}

/// Whether a write open truncates the file (`O_TRUNC`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truncation {
    /// The file keeps its bytes.
    Keep,
    /// The file starts empty.
    Truncate,
}

/// A request to open a file for writing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenWrite {
    /// The file to open.
    pub target: WriteTarget,
    /// Whether the open truncates the file.
    pub truncation: Truncation,
}

/// A request to create a directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mkdir {
    /// The new directory's parent directory.
    pub parent: ItemId,
    /// The new directory's name.
    pub name: OsString,
    /// The new directory's permission bits.
    pub mode: u32,
}

/// A request to create a symlink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Symlink {
    /// The new symlink's parent directory.
    pub parent: ItemId,
    /// The new symlink's name.
    pub name: OsString,
    /// The path that the symlink refers to.
    pub target: OsString,
}

/// A request to move or rename an item, replacing what the destination name
/// refers to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rename {
    /// The directory that contains the item.
    pub parent: ItemId,
    /// The item's name.
    pub name: OsString,
    /// The directory to move the item to.
    pub new_parent: ItemId,
    /// The item's new name.
    pub new_name: OsString,
}

/// A request to change an item's attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetAttr {
    /// The item to change.
    pub item: ItemId,
    /// The size to truncate or extend a file to.
    pub size_bytes: Option<u64>,
    /// The new permission bits.
    pub mode: Option<u32>,
    /// The modification time that a program set, in milliseconds since the
    /// Unix epoch.
    pub mtime_ms: Option<u64>,
}

/// One open descriptor of a file that a program writes.
///
/// A read-only mount has no working files, so the engine never creates a
/// write handle.
#[derive(Debug)]
pub struct WriteHandle {
    never: Infallible,
}

impl WriteHandle {
    /// Returns the item that the handle writes.
    pub fn item(&self) -> ItemId {
        match self.never {}
    }

    /// Reads up to `length` bytes of the working file from `offset`.
    pub async fn read(&self, _offset: u64, _length: u64) -> Result<Bytes> {
        match self.never {}
    }

    /// Writes `bytes` to the working file at `offset`.
    pub async fn write(&self, _offset: u64, _bytes: &[u8]) -> Result<u64> {
        match self.never {}
    }
}
