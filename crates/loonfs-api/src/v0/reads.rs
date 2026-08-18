//! Authoritative read-result shapes for the v0 HTTP API: the stat/list
//! entry, the directory-listing envelope, and the file-bytes read result.
//! The mutating operation shapes live in [`super::operations`].

use crate::{
    AbsolutePath, ActorRef, AttributeRevisionNo, Attributes, ChangeSeq, ContentRef, DisplayName,
    InodeId, InodeKind, NameKey, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

/// Authoritative metadata for one visible path.
///
/// This is the result shape for stat/list style reads. The entry kind carries
/// the file-only revision and content summary, so a directory cannot carry a
/// partial file payload. Attributes are likewise projected as one value or
/// omitted as one value, while serializing as prefixed sibling fields.
/// The attribute revision is read independently — clients feed it to
/// `expected_attributes_revision_no` on the next write without touching the
/// values — so this is a prefixed-sibling projection rather than a value
/// consumed as one nested unit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AuthoritativePathEntry {
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
    /// Time the inode was created, in Unix milliseconds. Sequence numbers
    /// determine order.
    pub created_at_ms: u64,
    /// File-or-directory classification and its kind-specific payload.
    #[serde(flatten)]
    pub kind: AuthoritativePathEntryKind,
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
        schema(schema_with = crate::public_inode_id::optional_schema)
    )]
    pub parent_inode_id: Option<InodeId>,
    /// Stored display name for this path component, absent for the nameless root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<DisplayName>,
    /// The inode's attribute projection, when requested.
    #[serde(flatten, default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<AttributesProjection>,
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
    ///
    /// The entry tag reuses [`InodeKind`]'s wire vocabulary.
    #[serde(rename = "dir")]
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
        /// Actor responsible for the current revision.
        revision_actor: ActorRef,
        /// Time of the current revision, in Unix milliseconds. Revision
        /// sequences determine order.
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

    /// Returns the actor responsible for the current file revision.
    /// Directories return `None`.
    pub const fn revision_actor(&self) -> Option<&ActorRef> {
        match self {
            Self::Directory {} => None,
            Self::File { revision_actor, .. } => Some(revision_actor),
        }
    }
}

/// One inode's structurally complete attribute projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AttributesProjection {
    /// The attribute revision this projection represents.
    pub attributes_revision_no: AttributeRevisionNo,
    /// Actor responsible for the latest attribute update. This is `None` for
    /// the initial empty state at revision 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes_updated_by: Option<ActorRef>,
    /// Time of the latest attribute update, in Unix milliseconds. This is
    /// `None` for the initial empty state at revision 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes_updated_at_ms: Option<u64>,
    /// The complete attribute map at `attributes_revision_no`.
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
    pub path: AbsolutePath,
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

/// One deletion that can still be restored.
///
/// `inode_id` and `deletion_seq` are sufficient to restore it. The original
/// parent and name are included when they were recorded.
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
    /// Directory that held the deleted binding, when recorded.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(
        feature = "openapi",
        schema(schema_with = crate::public_inode_id::optional_schema)
    )]
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
            path: AbsolutePath::parse(path).expect("absolute path"),
            inode_id: InodeId(if parent_inode_id.is_some() { 2 } else { 1 }),
            created_by: ActorRef::loonfs_system(),
            created_at_ms: 1_752_624_000_000,
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
                "path": "/docs",
                "inode_id": "ino_2",
                "created_by": { "kind": "system", "id": "loonfs" },
                "created_at_ms": 1_752_624_000_000_u64,
                "inode_kind": "dir",
                "head_seq": 3,
                "parent_inode_id": "ino_1",
                "display_name": "docs"
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
            revision_actor: ActorRef::loonfs_system(),
            committed_at_ms: 1_752_624_000_000,
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
                "revision_actor": { "kind": "system", "id": "loonfs" },
                "committed_at_ms": 1_752_624_000_000_u64,
                "head_seq": 3,
                "parent_inode_id": "ino_1",
                "display_name": "report.txt"
            })
        );
    }

    #[test]
    fn nameless_root_omits_parent_inode_id_and_display_name() {
        let root_json = serde_json::to_value(entry("/", None, None)).expect("serialize root");
        assert!(root_json.get("parent_inode_id").is_none());
        assert!(root_json.get("display_name").is_none());

        let decoded: AuthoritativePathEntry =
            serde_json::from_value(root_json).expect("decode root without optional fields");
        assert_eq!(decoded.parent_inode_id, None);
        assert_eq!(decoded.display_name, None);

        let named_json = serde_json::to_value(entry("/docs", Some(InodeId(1)), Some("docs")))
            .expect("serialize named entry");
        assert_eq!(named_json["parent_inode_id"], "ino_1");
        assert_eq!(named_json["display_name"], "docs");
    }

    #[test]
    fn authoritative_entry_kinds_share_inode_kind_wire_values() {
        let directory = AuthoritativePathEntryKind::Directory {};
        assert_eq!(
            serde_json::to_value(directory).expect("serialize directory entry kind")["inode_kind"],
            serde_json::to_value(InodeKind::Directory).expect("serialize directory inode kind")
        );

        let content_ref = ContentRef::blob_v1(crate::ContentId::generate(), b"hello");
        let file = AuthoritativePathEntryKind::File {
            revision_no: RevisionNo(1),
            size_bytes: 5,
            content_ref,
            revision_actor: ActorRef::loonfs_system(),
            committed_at_ms: 1,
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

        let decoded: AuthoritativePathEntry =
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

        let decoded: AuthoritativePathEntry =
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
    fn serialized_entries_never_nest_attributes_inside_attributes() {
        let mut projected = entry("/docs", Some(InodeId(1)), Some("docs"));
        projected.attributes = Some(AttributesProjection {
            attributes_revision_no: crate::AttributeRevisionNo(1),
            attributes_updated_by: None,
            attributes_updated_at_ms: None,
            attributes: crate::Attributes::new(std::collections::BTreeMap::from([(
                crate::AttributeKey::parse("owner").expect("attribute key"),
                crate::AttributeValue::parse("finance").expect("attribute value"),
            )]))
            .expect("attributes"),
        });

        let projected_json = serde_json::to_value(projected).expect("serialize projected entry");
        assert!(projected_json.pointer("/attributes/attributes").is_none());
    }

    #[test]
    fn trash_handle_copies_directly_into_an_undelete_operation() {
        let trash = TrashEntry {
            inode_id: InodeId(42),
            deletion_seq: ChangeSeq(417),
            deleted_at_ms: 1_752_625_000_000,
            deleted_by: ActorRef::loonfs_system(),
            parent_inode_id: Some(InodeId(7)),
            name_key: Some(NameKey::parse("report.txt").expect("name key")),
            display_name: Some(DisplayName::parse("Report.txt").expect("display name")),
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
