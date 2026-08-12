//! Authoritative read-result shapes for the v0 HTTP API: the stat/list
//! entry, the directory-listing envelope, and the file-bytes read result.
//! The mutating operation shapes live in [`super::operations`].

use crate::{
    AbsolutePath, AttributeRevisionNo, Attributes, ChangeSeq, ContentRef, DisplayName, InodeId,
    InodeKind, NameKey, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

/// Authoritative metadata for one visible path.
///
/// This is the result shape for stat/list style reads. The entry kind carries
/// the file-only revision and content summary, so a directory cannot carry a
/// partial file payload. Attributes are likewise projected as one value or
/// omitted as one value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AuthoritativePathEntry {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Absolute path as rendered from stored display names.
    pub absolute_path: AbsolutePath,
    /// Stable inode identity for this item.
    pub inode_id: InodeId,
    /// File-or-directory classification and its kind-specific payload.
    #[serde(flatten)]
    pub kind: AuthoritativePathEntryKind,
    /// Namespace head sequence this answer was read from.
    pub head_seq: ChangeSeq,
    /// Parent directory inode, or `None` for the root.
    pub parent_inode_id: Option<InodeId>,
    /// Stored display name for this path component, absent for the nameless root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<DisplayName>,
    /// The inode's attribute projection, when requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<AuthoritativeAttributes>,
}

impl AuthoritativePathEntry {
    /// Returns whether this entry is a file or directory.
    pub const fn inode_kind(&self) -> InodeKind {
        self.kind.inode_kind()
    }

    /// Returns the current revision number for a file entry.
    pub const fn revision_no(&self) -> Option<RevisionNo> {
        match &self.kind {
            AuthoritativePathEntryKind::Directory {} => None,
            AuthoritativePathEntryKind::File { revision_no, .. } => Some(*revision_no),
        }
    }

    /// Returns the current byte length for a file entry.
    pub const fn size_bytes(&self) -> Option<u64> {
        match &self.kind {
            AuthoritativePathEntryKind::Directory {} => None,
            AuthoritativePathEntryKind::File { size_bytes, .. } => Some(*size_bytes),
        }
    }

    /// Returns the current content reference for a file entry.
    pub const fn content_ref(&self) -> Option<&ContentRef> {
        match &self.kind {
            AuthoritativePathEntryKind::Directory {} => None,
            AuthoritativePathEntryKind::File { content_ref, .. } => Some(content_ref),
        }
    }

    /// Returns the current revision's commit stamp for a file entry.
    pub const fn committed_at_ms(&self) -> Option<u64> {
        match &self.kind {
            AuthoritativePathEntryKind::Directory {} => None,
            AuthoritativePathEntryKind::File {
                committed_at_ms, ..
            } => Some(*committed_at_ms),
        }
    }
}

/// Kind-specific metadata for an authoritative path entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "inode_kind", rename_all = "snake_case")]
pub enum AuthoritativePathEntryKind {
    /// A directory, which has no revision payload in v0.
    #[cfg_attr(feature = "openapi", schema(title = "AuthoritativePathEntryDirectory"))]
    Directory {},
    /// A file and its current revision summary.
    #[cfg_attr(feature = "openapi", schema(title = "AuthoritativePathEntryFile"))]
    File {
        /// Current file revision number.
        revision_no: RevisionNo,
        /// Current file size in bytes.
        ///
        /// This remains explicit even though `content_ref` also carries the
        /// length because callers sort directory listings by this field.
        size_bytes: u64,
        /// Current content reference.
        content_ref: ContentRef,
        /// Wall-clock stamp of the commit that created the current revision.
        /// Observational: `head_seq` and revision sequences are the order.
        committed_at_ms: u64,
    },
}

impl AuthoritativePathEntryKind {
    /// Returns the stable inode classification represented by this payload.
    pub const fn inode_kind(&self) -> InodeKind {
        match self {
            Self::Directory {} => InodeKind::Directory,
            Self::File { .. } => InodeKind::File,
        }
    }
}

/// One inode's complete projected attribute state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AuthoritativeAttributes {
    /// The attribute revision this projection represents.
    pub revision_no: AttributeRevisionNo,
    /// The complete attribute map at `revision_no`.
    ///
    /// An inode that has never had attributes written is at revision 0 with
    /// an empty map.
    pub attributes: Attributes,
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
            kind: AuthoritativePathEntryKind::Directory {},
            head_seq: ChangeSeq(3),
            parent_inode_id,
            display_name: display_name.map(|name| DisplayName::parse(name).expect("display name")),
            attributes: None,
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
                "inode_kind": "directory",
                "head_seq": 3,
                "parent_inode_id": 1,
                "display_name": "docs"
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
                    "inode_kind": "directory",
                    "head_seq": 3,
                    "parent_inode_id": 1,
                    "display_name": "docs"
                }]
            })
        );
    }

    #[test]
    fn a_file_entry_serializes_its_required_payload_with_the_kind() {
        let content_ref = ContentRef::blob_v1(crate::ContentId::generate(), b"hello");
        let mut file = entry("/report.txt", Some(InodeId(1)), Some("report.txt"));
        file.kind = AuthoritativePathEntryKind::File {
            revision_no: RevisionNo(7),
            size_bytes: 5,
            content_ref: content_ref.clone(),
            committed_at_ms: 1_752_624_000_000,
        };

        assert_eq!(
            serde_json::to_value(file).expect("serialize file entry"),
            serde_json::json!({
                "namespace_id": "demo",
                "absolute_path": "/report.txt",
                "inode_id": 2,
                "inode_kind": "file",
                "revision_no": 7,
                "size_bytes": 5,
                "content_ref": content_ref,
                "committed_at_ms": 1_752_624_000_000_u64,
                "head_seq": 3,
                "parent_inode_id": 1,
                "display_name": "report.txt"
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

    /// An unprojected entry omits attributes; a projected one carries the
    /// map and revision together, and an empty map is a projected answer.
    #[test]
    fn attributes_serialize_as_one_optional_projection() {
        let unprojected = entry("/docs", Some(InodeId(1)), Some("docs"));
        let unprojected_json =
            serde_json::to_value(&unprojected).expect("serialize unprojected entry");
        assert!(unprojected_json.get("attributes").is_none());

        let mut projected = unprojected;
        projected.attributes = Some(AuthoritativeAttributes {
            revision_no: crate::AttributeRevisionNo(0),
            attributes: crate::Attributes::default(),
        });
        let projected_json = serde_json::to_value(&projected).expect("serialize projected entry");
        assert_eq!(
            projected_json["attributes"],
            serde_json::json!({ "revision_no": 0, "attributes": {} })
        );
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
    /// Recoverable deletions, oldest deletion first.
    pub entries: Vec<TrashEntry>,
    /// Present when another page follows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}
