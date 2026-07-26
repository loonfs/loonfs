//! Authoritative read-result shapes for the v0 HTTP API: the stat/list
//! entry, the directory-listing envelope, and the file-bytes read result.
//! The mutating operation shapes live in [`super::operations`].

use crate::{
    AbsolutePath, ChangeSeq, ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamespaceId,
    RevisionNo,
};
use serde::{Deserialize, Serialize};

/// Authoritative metadata for one visible path.
///
/// This is the result shape for stat/list style reads. File entries include
/// revision and content summary fields; directory entries leave those empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AuthoritativePathEntry {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Absolute path as rendered from stored display names.
    pub absolute_path: AbsolutePath,
    /// Stable inode identity for this item.
    pub inode_id: InodeId,
    /// Whether the item is a file or directory.
    pub inode_kind: InodeKind,
    /// Namespace head sequence this answer was read from.
    pub head_seq: ChangeSeq,
    /// Parent directory inode, or `None` for the root.
    pub parent_inode_id: Option<InodeId>,
    /// Stored display name for this path component, absent for the nameless root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<DisplayName>,
    /// Current file revision number, for files.
    pub revision_no: Option<RevisionNo>,
    /// Current file size in bytes, for files.
    pub size_bytes: Option<u64>,
    /// Current content reference, for files.
    pub content_ref: Option<ContentRef>,
    /// Wall-clock stamp of the commit that created the current revision,
    /// for files; directories carry no modification time in v0.
    /// Observational: `head_seq` and revision sequences are the order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_at_ms: Option<u64>,
}

/// One directory listing and the namespace head it was answered at.
///
/// The envelope names the listing target and head so an empty directory
/// still tells the caller which state it observed, and so the response can
/// grow without reshaping `entries`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListPathEntriesResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Absolute path of the listed directory.
    pub absolute_path: AbsolutePath,
    /// Namespace head sequence this listing was read from.
    pub head_seq: ChangeSeq,
    /// Directory entries for this page.
    ///
    /// Entries are returned in canonical name-key order. Higher-level display
    /// surfaces may sort entries separately for presentation.
    pub entries: Vec<AuthoritativePathEntry>,
    /// Cursor for the next page, if more entries remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// File bytes plus the metadata entry they came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AuthoritativeFileBytes {
    /// Authoritative metadata for the file that was read.
    pub entry: AuthoritativePathEntry,
    /// Validated file bytes.
    pub bytes: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(
        path: &str,
        parent_inode_id: Option<InodeId>,
        display_name: Option<&str>,
    ) -> AuthoritativePathEntry {
        AuthoritativePathEntry {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            absolute_path: AbsolutePath::parse(path).expect("absolute path"),
            inode_id: InodeId(if parent_inode_id.is_some() { 2 } else { 1 }),
            inode_kind: InodeKind::Directory,
            head_seq: ChangeSeq(3),
            parent_inode_id,
            display_name: display_name.map(|name| DisplayName::parse(name).expect("display name")),
            revision_no: None,
            size_bytes: None,
            content_ref: None,
            committed_at_ms: None,
        }
    }

    #[test]
    fn authoritative_entry_paths_keep_the_plain_string_wire_shape() {
        let named = entry("/docs", Some(InodeId(1)), Some("docs"));
        assert_eq!(
            serde_json::to_value(&named).expect("serialize named entry"),
            serde_json::json!({
                "namespace_id": "demo",
                "absolute_path": "/docs",
                "inode_id": 2,
                "inode_kind": "dir",
                "head_seq": 3,
                "parent_inode_id": 1,
                "display_name": "docs",
                "revision_no": null,
                "size_bytes": null,
                "content_ref": null
            })
        );

        let response = ListPathEntriesResponse {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            absolute_path: AbsolutePath::parse("/").expect("absolute path"),
            head_seq: ChangeSeq(3),
            entries: vec![named],
            next_cursor: None,
        };
        assert_eq!(
            serde_json::to_value(response).expect("serialize listing"),
            serde_json::json!({
                "namespace_id": "demo",
                "absolute_path": "/",
                "head_seq": 3,
                "entries": [{
                    "namespace_id": "demo",
                    "absolute_path": "/docs",
                    "inode_id": 2,
                    "inode_kind": "dir",
                    "head_seq": 3,
                    "parent_inode_id": 1,
                    "display_name": "docs",
                    "revision_no": null,
                    "size_bytes": null,
                    "content_ref": null
                }]
            })
        );
    }

    #[test]
    fn nameless_root_omits_display_name_while_named_entries_carry_it() {
        let root_json = serde_json::to_value(entry("/", None, None)).expect("serialize root");
        assert!(root_json.get("display_name").is_none());

        let named_json = serde_json::to_value(entry("/docs", Some(InodeId(1)), Some("docs")))
            .expect("serialize named entry");
        assert_eq!(named_json["display_name"], "docs");
    }
}

/// One recoverable deletion: an active subtree tombstone plus the identity
/// of the binding it deleted, when the delete recorded one. Entries with no
/// recorded name predate the enriched tombstone rows; their inode and
/// sequence still form a complete `undelete` handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TrashEntry {
    /// Root inode the deletion hid; half of the recovery handle.
    pub root_inode_id: InodeId,
    /// Commit sequence of the deletion; the other half of the handle.
    pub deleted_at_seq: ChangeSeq,
    /// Wall-clock stamp of the deleting commit. Observational.
    pub deleted_at_ms: u64,
    /// Directory that held the deleted binding, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_inode_id: Option<InodeId>,
    /// Canonical key of the deleted binding, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_key: Option<NameKey>,
    /// User-facing spelling of the deleted binding, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<DisplayName>,
}

/// One trash listing page: the namespace's recoverable deletions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListTrashResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Head sequence this page was evaluated at.
    pub head_seq: ChangeSeq,
    /// Active deletions in ascending root-inode order.
    pub entries: Vec<TrashEntry>,
    /// Present when another page follows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}
