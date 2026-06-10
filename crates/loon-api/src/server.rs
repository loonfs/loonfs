use crate::{ChangeSeq, ContentRef, InodeId, InodeKind, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};

/// Authoritative metadata for one visible path.
///
/// This is the result shape for stat/list style reads. File entries include
/// revision and content summary fields; directory entries leave those empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativePathEntry {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Absolute path as rendered from stored display names.
    pub absolute_path: String,
    /// Stable inode identity for this item.
    pub inode_id: InodeId,
    /// Whether the item is a file or directory.
    pub inode_kind: InodeKind,
    /// Namespace head sequence this answer was read from.
    pub head_seq: ChangeSeq,
    /// Parent directory inode, or `None` for the root.
    pub parent_inode_id: Option<InodeId>,
    /// Stored display name for this path component.
    pub display_name: String,
    /// Current file revision number, for files.
    pub revision_no: Option<RevisionNo>,
    /// Current file size in bytes, for files.
    pub size_bytes: Option<u64>,
    /// Current content reference, for files.
    pub content_ref: Option<ContentRef>,
}

/// One directory listing and the namespace head it was answered at.
///
/// The envelope names the listing target and head so an empty directory
/// still tells the caller which state it observed, and so the response can
/// grow (for example, pagination cursors) without reshaping `entries`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListPathEntriesResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Absolute path of the listed directory.
    pub absolute_path: String,
    /// Namespace head sequence this listing was read from.
    pub head_seq: ChangeSeq,
    /// Directory entries in display-name order.
    pub entries: Vec<AuthoritativePathEntry>,
}

/// File bytes plus the metadata entry they came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeFileBytes {
    /// Authoritative metadata for the file that was read.
    pub entry: AuthoritativePathEntry,
    /// Validated file bytes.
    pub bytes: Vec<u8>,
}
