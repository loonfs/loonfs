use crate::{ChangeSeq, ContentRef, InodeId, InodeKind, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Authoritative metadata for one visible path.
///
/// This is the result shape for stat/list style reads. File entries include
/// revision and content summary fields; directory entries leave those empty.
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
    pub authoritative_head_seq: ChangeSeq,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// File bytes plus the metadata entry they came from.
pub struct AuthoritativeFileBytes {
    /// Authoritative metadata for the file that was read.
    pub entry: AuthoritativePathEntry,
    /// Validated file bytes.
    pub bytes: Vec<u8>,
}
