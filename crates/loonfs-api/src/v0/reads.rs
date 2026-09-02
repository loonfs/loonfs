//! Read response shapes for the v0 HTTP API.

use super::DirectoryBinding;
use crate::{
    AbsolutePath, ActorRef, AttributeRevisionNo, Attributes, ChangeSeq, ContentRef, DisplayName,
    InodeId, InodeKind, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

/// Metadata for one path returned by stat and directory listings.
///
/// Attribute fields are included only when requested.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PathEntry {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Absolute path as rendered from stored display names.
    pub path: AbsolutePath,
    /// Stable inode identity for this item.
    #[serde(with = "crate::public_inode_id")]
    #[cfg_attr(
        feature = "openapi",
        schema(schema_with = crate::public_inode_id::schema)
    )]
    pub inode_id: InodeId,
    /// Actor that created this inode, as supplied by the application.
    pub created_by: ActorRef,
    /// The inode creation time in Unix milliseconds.
    pub created_at_ms: u64,
    /// File-or-directory classification and its kind-specific payload.
    #[serde(flatten)]
    pub kind: PathEntryKind,
    /// Namespace head sequence this answer was read from.
    pub head_seq: ChangeSeq,
    /// Parent directory inode, or `None` for the root.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(
        feature = "openapi",
        schema(schema_with = crate::public_inode_id::schema)
    )]
    pub parent_inode_id: Option<InodeId>,
    /// Stored display name for this path component, absent for the nameless root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub display_name: Option<DisplayName>,
    /// The opaque ID for the current parent and name binding, or `None` for the namespace root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub binding_generation: Option<String>,
    /// The inode's attribute projection, when requested.
    #[serde(flatten, default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub attributes: Option<AttributesProjection>,
}

impl PathEntry {
    /// Returns whether this entry is a file or directory.
    pub const fn inode_kind(&self) -> InodeKind {
        self.kind.inode_kind()
    }

    /// Returns the current revision number for a file entry.
    pub const fn revision_no(&self) -> Option<RevisionNo> {
        match &self.kind {
            PathEntryKind::Directory {} => None,
            PathEntryKind::File { revision_no, .. } => Some(*revision_no),
        }
    }

    /// Returns the current byte length for a file entry.
    pub const fn size_bytes(&self) -> Option<u64> {
        match &self.kind {
            PathEntryKind::Directory {} => None,
            PathEntryKind::File { size_bytes, .. } => Some(*size_bytes),
        }
    }

    /// Returns the current content reference for a file entry.
    pub const fn content_ref(&self) -> Option<&ContentRef> {
        match &self.kind {
            PathEntryKind::Directory {} => None,
            PathEntryKind::File { content_ref, .. } => Some(content_ref),
        }
    }

    /// Returns the current revision's commit stamp for a file entry.
    pub const fn revision_committed_at_ms(&self) -> Option<u64> {
        match &self.kind {
            PathEntryKind::Directory {} => None,
            PathEntryKind::File {
                revision_committed_at_ms,
                ..
            } => Some(*revision_committed_at_ms),
        }
    }
}

/// Kind-specific metadata for a path entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "inode_kind", rename_all = "snake_case")]
pub enum PathEntryKind {
    /// A directory without a revision payload.
    #[serde(rename = "dir")]
    #[cfg_attr(feature = "openapi", schema(title = "PathEntryDirectory"))]
    Directory {},
    /// A file and its current revision summary.
    #[cfg_attr(feature = "openapi", schema(title = "PathEntryFile"))]
    File {
        /// Current file revision number.
        revision_no: RevisionNo,
        /// The current file size in bytes.
        size_bytes: u64,
        /// Current content reference.
        content_ref: ContentRef,
        /// Actor responsible for the current revision.
        revision_committed_by: ActorRef,
        /// The current revision time in Unix milliseconds.
        revision_committed_at_ms: u64,
    },
}

impl PathEntryKind {
    /// Returns the stable inode classification represented by this payload.
    pub const fn inode_kind(&self) -> InodeKind {
        match self {
            Self::Directory {} => InodeKind::Directory,
            Self::File { .. } => InodeKind::File,
        }
    }

    /// Returns the actor responsible for the current file revision.
    /// Directories return `None`.
    pub const fn revision_committed_by(&self) -> Option<&ActorRef> {
        match self {
            Self::Directory {} => None,
            Self::File {
                revision_committed_by,
                ..
            } => Some(revision_committed_by),
        }
    }
}

/// The optional attributes returned for one inode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AttributesProjection {
    /// The attribute revision this projection represents.
    #[cfg_attr(feature = "openapi", schema(required = false))]
    pub attributes_revision_no: AttributeRevisionNo,
    /// The actor responsible for the latest attribute update, or `None` for the
    /// initial empty state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub attributes_updated_by: Option<ActorRef>,
    /// The latest attribute update time in Unix milliseconds, or `None` for the
    /// initial empty state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes_updated_at_ms: Option<u64>,
    /// The complete attribute map at `attributes_revision_no`, including an empty map
    /// for the initial state.
    #[cfg_attr(feature = "openapi", schema(required = false))]
    pub attributes: Attributes,
}

/// One directory listing and the namespace head used to read it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListPathEntriesResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Absolute path of the listed directory.
    pub path: AbsolutePath,
    /// Namespace head sequence this listing was read from.
    pub head_seq: ChangeSeq,
    /// The directory entries in canonical name-key order.
    pub entries: Vec<PathEntry>,
    /// Cursor for the next page, if more entries remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// One directory listing addressed by parent inode and the namespace head used to read it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListInodeChildrenResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Directory inode whose children were returned.
    #[serde(with = "crate::public_inode_id")]
    #[cfg_attr(
        feature = "openapi",
        schema(schema_with = crate::public_inode_id::schema)
    )]
    pub parent_inode_id: InodeId,
    /// Namespace head sequence this listing was read from.
    pub head_seq: ChangeSeq,
    /// The directory entries in canonical name-key order.
    pub entries: Vec<PathEntry>,
    /// Cursor for the next page, if more entries remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// File bytes plus the metadata entry they came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FileBytes {
    /// Authoritative metadata for the file that was read.
    pub entry: PathEntry,
    /// Validated file bytes.
    pub bytes: Vec<u8>,
}

/// One recoverable deletion and its removed directory binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TrashEntry {
    /// Inode hidden by the deletion.
    #[serde(with = "crate::public_inode_id")]
    #[cfg_attr(
        feature = "openapi",
        schema(schema_with = crate::public_inode_id::schema)
    )]
    pub inode_id: InodeId,
    /// Commit sequence that identifies this deletion.
    pub deletion_seq: ChangeSeq,
    /// Time of the deletion, in Unix milliseconds.
    pub deleted_at_ms: u64,
    /// Actor responsible for the deletion.
    pub deleted_by: ActorRef,
    /// Directory binding removed by the deletion.
    pub deleted_binding: DirectoryBinding,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NameKey;

    fn binding_generation() -> String {
        "generation".to_owned()
    }

    fn entry(
        path: &str,
        parent_inode_id: Option<InodeId>,
        display_name: Option<&str>,
    ) -> PathEntry {
        PathEntry {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            path: AbsolutePath::parse(path).expect("absolute path"),
            inode_id: InodeId(if parent_inode_id.is_some() { 2 } else { 1 }),
            created_by: ActorRef::loonfs_system(),
            created_at_ms: 1_752_624_000_000,
            kind: PathEntryKind::Directory {},
            head_seq: ChangeSeq(3),
            parent_inode_id,
            display_name: display_name.map(|name| DisplayName::parse(name).expect("display name")),
            binding_generation: parent_inode_id.map(|_| binding_generation()),
            attributes: None,
        }
    }

    #[test]
    fn path_entries_keep_the_plain_string_wire_shape() {
        let named = entry("/docs", Some(InodeId(1)), Some("docs"));
        assert_eq!(
            serde_json::to_value(&named).expect("serialize named entry"),
            serde_json::json!({
                "namespace_id": "demo",
                "path": "/docs",
                "inode_id": "ino_2",
                "created_by": { "kind": "system", "id": "loonfs" },
                "created_at_ms": 1_752_624_000_000_u64,
                "inode_kind": "dir",
                "head_seq": 3,
                "parent_inode_id": "ino_1",
                "display_name": "docs",
                "binding_generation": binding_generation()
            })
        );

        let response = ListPathEntriesResponse {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            path: AbsolutePath::parse("/").expect("absolute path"),
            head_seq: ChangeSeq(3),
            entries: vec![named],
            next_cursor: None,
        };
        assert_eq!(
            serde_json::to_value(response).expect("serialize listing"),
            serde_json::json!({
                "namespace_id": "demo",
                "path": "/",
                "head_seq": 3,
                "entries": [{
                    "namespace_id": "demo",
                    "path": "/docs",
                    "inode_id": "ino_2",
                    "created_by": { "kind": "system", "id": "loonfs" },
                    "created_at_ms": 1_752_624_000_000_u64,
                    "inode_kind": "dir",
                    "head_seq": 3,
                    "parent_inode_id": "ino_1",
                    "display_name": "docs",
                    "binding_generation": binding_generation()
                }]
            })
        );
    }

    #[test]
    fn a_file_entry_serializes_its_required_payload_with_the_kind() {
        let content_ref = ContentRef::blob_v1(crate::ContentId::generate(), b"hello");
        let mut file = entry("/report.txt", Some(InodeId(1)), Some("report.txt"));
        file.kind = PathEntryKind::File {
            revision_no: RevisionNo(7),
            size_bytes: 5,
            content_ref: content_ref.clone(),
            revision_committed_by: ActorRef::loonfs_system(),
            revision_committed_at_ms: 1_752_624_000_000,
        };

        assert_eq!(
            serde_json::to_value(file).expect("serialize file entry"),
            serde_json::json!({
                "namespace_id": "demo",
                "path": "/report.txt",
                "inode_id": "ino_2",
                "created_by": { "kind": "system", "id": "loonfs" },
                "created_at_ms": 1_752_624_000_000_u64,
                "inode_kind": "file",
                "revision_no": 7,
                "size_bytes": 5,
                "content_ref": content_ref,
                "revision_committed_by": { "kind": "system", "id": "loonfs" },
                "revision_committed_at_ms": 1_752_624_000_000_u64,
                "head_seq": 3,
                "parent_inode_id": "ino_1",
                "display_name": "report.txt",
                "binding_generation": binding_generation()
            })
        );
    }

    #[test]
    fn nameless_root_omits_parent_inode_id_and_display_name() {
        let root_json = serde_json::to_value(entry("/", None, None)).expect("serialize root");
        assert!(root_json.get("parent_inode_id").is_none());
        assert!(root_json.get("display_name").is_none());
        assert!(root_json.get("binding_generation").is_none());

        let decoded: PathEntry =
            serde_json::from_value(root_json).expect("decode root without optional fields");
        assert_eq!(decoded.parent_inode_id, None);
        assert_eq!(decoded.display_name, None);
        assert_eq!(decoded.binding_generation, None);

        let named_json = serde_json::to_value(entry("/docs", Some(InodeId(1)), Some("docs")))
            .expect("serialize named entry");
        assert_eq!(named_json["parent_inode_id"], "ino_1");
        assert_eq!(named_json["display_name"], "docs");
        assert_eq!(named_json["binding_generation"], binding_generation());
    }

    #[test]
    fn path_entry_kinds_share_inode_kind_wire_values() {
        let directory = PathEntryKind::Directory {};
        assert_eq!(
            serde_json::to_value(directory).expect("serialize directory entry kind")["inode_kind"],
            serde_json::to_value(InodeKind::Directory).expect("serialize directory inode kind")
        );

        let content_ref = ContentRef::blob_v1(crate::ContentId::generate(), b"hello");
        let file = PathEntryKind::File {
            revision_no: RevisionNo(1),
            size_bytes: 5,
            content_ref,
            revision_committed_by: ActorRef::loonfs_system(),
            revision_committed_at_ms: 1,
        };
        assert_eq!(
            serde_json::to_value(file).expect("serialize file entry kind")["inode_kind"],
            serde_json::to_value(InodeKind::File).expect("serialize file inode kind")
        );
    }

    #[test]
    fn requested_attributes_serialize_as_flat_prefixed_siblings() {
        let mut projected = entry("/docs", Some(InodeId(1)), Some("docs"));
        projected.attributes = Some(AttributesProjection {
            attributes_revision_no: crate::AttributeRevisionNo(7),
            attributes_updated_by: Some(ActorRef::loonfs_system()),
            attributes_updated_at_ms: Some(1_752_624_000_000),
            attributes: crate::Attributes::new(std::collections::BTreeMap::from([(
                crate::AttributeKey::parse("owner").expect("attribute key"),
                crate::AttributeValue::parse("finance").expect("attribute value"),
            )]))
            .expect("attributes"),
        });

        let projected_json = serde_json::to_value(&projected).expect("serialize projected entry");
        assert_eq!(projected_json["attributes_revision_no"], 7);
        assert_eq!(
            projected_json["attributes"],
            serde_json::json!({ "owner": "finance" })
        );
        assert_eq!(
            projected_json["attributes_updated_by"],
            serde_json::json!({ "kind": "system", "id": "loonfs" })
        );
        assert_eq!(
            projected_json["attributes_updated_at_ms"],
            1_752_624_000_000_u64
        );

        let decoded: PathEntry =
            serde_json::from_value(projected_json).expect("decode projected entry");
        let projection = decoded.attributes.expect("projected attributes");
        assert_eq!(
            projection.attributes_revision_no,
            crate::AttributeRevisionNo(7)
        );
    }

    #[test]
    fn unrequested_attributes_omit_both_wire_keys() {
        let unprojected = entry("/docs", Some(InodeId(1)), Some("docs"));
        let unprojected_json =
            serde_json::to_value(&unprojected).expect("serialize unprojected entry");
        assert!(unprojected_json.get("attributes").is_none());
        assert!(unprojected_json.get("attributes_revision_no").is_none());

        let decoded: PathEntry =
            serde_json::from_value(unprojected_json).expect("decode unprojected entry");
        assert!(decoded.attributes.is_none());
    }

    #[test]
    fn never_written_attributes_serialize_as_revision_zero_and_empty_map() {
        let mut projected = entry("/docs", Some(InodeId(1)), Some("docs"));
        projected.attributes = Some(AttributesProjection {
            attributes_revision_no: crate::AttributeRevisionNo(0),
            attributes_updated_by: None,
            attributes_updated_at_ms: None,
            attributes: crate::Attributes::default(),
        });
        let projected_json = serde_json::to_value(&projected).expect("serialize projected entry");
        assert_eq!(projected_json["attributes_revision_no"], 0);
        assert_eq!(projected_json["attributes"], serde_json::json!({}));
        assert!(projected_json.get("attributes_updated_by").is_none());
        assert!(projected_json.get("attributes_updated_at_ms").is_none());
    }

    #[test]
    fn a_trash_entry_nests_the_binding_the_deletion_removed() {
        let trash = TrashEntry {
            inode_id: InodeId(42),
            deletion_seq: ChangeSeq(417),
            deleted_at_ms: 1,
            deleted_by: ActorRef::loonfs_system(),
            deleted_binding: DirectoryBinding {
                parent_inode_id: InodeId(7),
                name_key: NameKey::parse("report.txt").expect("name key"),
                display_name: DisplayName::parse("report.txt").expect("display name"),
            },
        };
        assert_eq!(
            serde_json::to_value(&trash).expect("serialize trash entry"),
            serde_json::json!({
                "inode_id": "ino_42",
                "deletion_seq": 417,
                "deleted_at_ms": 1,
                "deleted_by": { "kind": "system", "id": "loonfs" },
                "deleted_binding": {
                    "parent_inode_id": "ino_7",
                    "name_key": "report.txt",
                    "display_name": "report.txt"
                }
            })
        );
    }

    #[test]
    fn trash_handle_copies_directly_into_an_undelete_operation() {
        let trash = TrashEntry {
            inode_id: InodeId(42),
            deletion_seq: ChangeSeq(417),
            deleted_at_ms: 1_752_625_000_000,
            deleted_by: ActorRef::loonfs_system(),
            deleted_binding: DirectoryBinding {
                parent_inode_id: InodeId(7),
                name_key: NameKey::parse("report.txt").expect("name key"),
                display_name: DisplayName::parse("Report.txt").expect("display name"),
            },
        };
        let trash_json = serde_json::to_value(trash).expect("serialize trash entry");
        assert_eq!(trash_json["inode_id"], serde_json::json!("ino_42"));
        assert_eq!(trash_json["deletion_seq"], serde_json::json!(417));
        assert!(trash_json.get("root_inode_id").is_none());
        assert!(trash_json.get("deleted_at_seq").is_none());

        let operation_json = serde_json::json!({
            "kind": "undelete",
            "inode_id": trash_json["inode_id"].clone(),
            "deletion_seq": trash_json["deletion_seq"].clone()
        });
        let operation: crate::v0::FilesystemOperation =
            serde_json::from_value(operation_json).expect("decode copied trash handle");
        assert!(matches!(
            operation,
            crate::v0::FilesystemOperation::Undelete {
                inode_id: InodeId(42),
                deletion_seq: ChangeSeq(417),
                path: None,
            }
        ));

        assert!(
            serde_json::from_value::<crate::v0::FilesystemOperation>(serde_json::json!({
                "kind": "undelete",
                "inode_id": 42,
                "deleted_at_seq": 417
            }))
            .is_err(),
            "the retired deletion handle must not decode"
        );
    }
}
