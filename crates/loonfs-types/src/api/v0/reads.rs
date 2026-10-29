//! Read response shapes for the v0 HTTP API.

use super::DirectoryBinding;
use crate::{
    AbsolutePath, ActorId, Attributes, AttributesRevisionNo, BindingVersion, ChangeSeq, CommitId,
    ContentRef, DisplayName, InodeId, InodeKind, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};
use std::fmt;

/// The metadata for one path, as stat and directory listings return it.
///
/// The response includes attribute fields only when the request asks for them.
///
/// A newer server can return an inode kind that isn't listed here. Clients should read only the
/// fields that every entry includes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct PathEntry {
    /// The namespace that was read.
    pub namespace_id: NamespaceId,
    /// The absolute path, built from the stored display names.
    #[serde(deserialize_with = "crate::path::deserialize_derived_path")]
    pub path: AbsolutePath,
    /// The entry's stable inode ID.
    #[serde(with = "crate::public_inode_id")]
    pub inode_id: InodeId,
    /// The actor that created this inode, as the application supplied it.
    pub created_by: ActorId,
    /// The time the inode was created, in Unix milliseconds.
    pub created_at_ms: u64,
    /// The inode's kind and the fields that are specific to that kind.
    #[serde(flatten)]
    pub kind: PathEntryKind,
    /// The namespace head sequence that this response was read at.
    pub head_seq: ChangeSeq,
    /// The parent directory's inode. It's absent for the root.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::public_inode_id::option"
    )]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub parent_inode_id: Option<InodeId>,
    /// The stored display name of this path component. It's absent for the root, which has no name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub display_name: Option<DisplayName>,
    /// The opaque ID of the current binding of the entry's parent and name. It's absent for the
    /// namespace root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub binding_version: Option<BindingVersion>,
    /// The inode's attributes, when the request asks for them.
    #[serde(flatten, default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub attributes: Option<AttributesProjection>,
}

impl PathEntry {
    /// Returns the entry's inode kind, which is `Unknown` for a kind added by a newer server.
    pub const fn inode_kind(&self) -> EntryInodeKind {
        self.kind.inode_kind()
    }

    /// Returns the current revision number for a file entry.
    pub const fn revision_no(&self) -> Option<RevisionNo> {
        match &self.kind {
            PathEntryKind::Directory {} | PathEntryKind::Unknown => None,
            PathEntryKind::File { revision_no, .. } => Some(*revision_no),
        }
    }

    /// Returns the current byte length for a file entry.
    pub const fn size_bytes(&self) -> Option<u64> {
        match &self.kind {
            PathEntryKind::Directory {} | PathEntryKind::Unknown => None,
            PathEntryKind::File { size_bytes, .. } => Some(*size_bytes),
        }
    }

    /// Returns the current content reference for a file entry.
    pub const fn content_ref(&self) -> Option<&ContentRef> {
        match &self.kind {
            PathEntryKind::Directory {} | PathEntryKind::Unknown => None,
            PathEntryKind::File { content_ref, .. } => Some(content_ref),
        }
    }

    /// Returns the current revision's commit stamp for a file entry.
    pub const fn revision_committed_at_ms(&self) -> Option<u64> {
        match &self.kind {
            PathEntryKind::Directory {} | PathEntryKind::Unknown => None,
            PathEntryKind::File {
                revision_committed_at_ms,
                ..
            } => Some(*revision_committed_at_ms),
        }
    }
}

/// The metadata that is specific to a path entry's kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "inode_kind", rename_all = "snake_case")]
pub enum PathEntryKind {
    /// A directory, which has no revision fields.
    #[serde(rename = "dir")]
    #[cfg_attr(feature = "openapi", schema(title = "PathEntryDirectory"))]
    Directory {},
    /// A file, with a summary of its current revision.
    #[cfg_attr(feature = "openapi", schema(title = "PathEntryFile"))]
    File {
        /// The file's current revision number.
        revision_no: RevisionNo,
        /// The file's current size in bytes.
        size_bytes: u64,
        /// The file's current content.
        content_ref: ContentRef,
        /// The actor responsible for the current revision.
        revision_committed_by: ActorId,
        /// The time the current revision was committed, in Unix milliseconds.
        revision_committed_at_ms: u64,
    },
    /// An inode kind from a newer server. This variant doesn't include the kind's own fields.
    ///
    /// It serializes as `unknown` because the reported kind isn't kept.
    #[serde(other)]
    Unknown,
}

impl PathEntryKind {
    /// Returns the inode kind this payload represents.
    pub const fn inode_kind(&self) -> EntryInodeKind {
        match self {
            Self::Directory {} => EntryInodeKind::Directory,
            Self::File { .. } => EntryInodeKind::File,
            Self::Unknown => EntryInodeKind::Unknown,
        }
    }

    /// Returns the actor responsible for the current file revision.
    ///
    /// Other kinds return `None`.
    pub const fn revision_committed_by(&self) -> Option<&ActorId> {
        match self {
            Self::Directory {} | Self::Unknown => None,
            Self::File {
                revision_committed_by,
                ..
            } => Some(revision_committed_by),
        }
    }
}

/// The inode kind that a response reports.
///
/// The stored [`InodeKind`] has a closed set of values. A response can come from a newer server
/// that stores more kinds, so this enum ends in `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryInodeKind {
    /// A file, which has a revision history.
    File,
    /// A directory, which has child bindings.
    ///
    /// The wire value is always `"dir"`, and only the Rust variant name spells out the whole word.
    #[serde(rename = "dir")]
    Directory,
    /// An inode kind from a newer server. It serializes as `unknown`.
    #[serde(other)]
    Unknown,
}

impl From<InodeKind> for EntryInodeKind {
    fn from(kind: InodeKind) -> Self {
        match kind {
            InodeKind::File => Self::File,
            InodeKind::Directory => Self::Directory,
        }
    }
}

impl fmt::Display for EntryInodeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::File => "file",
            Self::Directory => "dir",
            Self::Unknown => "unknown",
        })
    }
}

/// The optional attributes that a read returns for one inode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AttributesProjection {
    /// The attribute revision that these attributes come from.
    #[cfg_attr(feature = "openapi", schema(required = false))]
    pub attributes_revision_no: AttributesRevisionNo,
    /// The actor responsible for the latest attribute update. It's absent before the first update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub attributes_updated_by: Option<ActorId>,
    /// The time of the latest attribute update, in Unix milliseconds. It's absent before the first
    /// update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub attributes_updated_at_ms: Option<u64>,
    /// The complete attribute map at `attributes_revision_no`. The map is empty before the first
    /// update.
    #[cfg_attr(feature = "openapi", schema(required = false))]
    pub attributes: Attributes,
}

/// One page of a directory's entries, with the namespace head that the read used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListPathEntriesResponse {
    /// The namespace that was read.
    pub namespace_id: NamespaceId,
    /// The absolute path of the listed directory.
    pub path: AbsolutePath,
    /// The namespace head sequence that this listing was read at.
    pub head_seq: ChangeSeq,
    /// The directory entries, in canonical name key order.
    pub entries: Vec<PathEntry>,
    /// The cursor for the next page, if more entries remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// One page of a directory's children, addressed by the parent inode, with the namespace head that
/// the read used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListInodeChildrenResponse {
    /// The namespace that was read.
    pub namespace_id: NamespaceId,
    /// The directory inode whose children were returned.
    #[serde(with = "crate::public_inode_id")]
    pub parent_inode_id: InodeId,
    /// The namespace head sequence that this listing was read at.
    pub head_seq: ChangeSeq,
    /// The directory entries, in canonical name key order.
    pub entries: Vec<PathEntry>,
    /// The cursor for the next page, if more entries remain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// One recoverable deletion and the directory binding that it removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TrashEntry {
    /// The inode that the deletion hides.
    #[serde(with = "crate::public_inode_id")]
    pub inode_id: InodeId,
    /// Whether the deleted root is a file or a directory. A newer server can return another kind.
    #[cfg_attr(feature = "openapi", schema(value_type = InodeKind))]
    pub inode_kind: EntryInodeKind,
    /// The commit sequence that identifies this deletion.
    pub deletion_seq: ChangeSeq,
    /// The time of the deletion, in Unix milliseconds.
    pub deleted_at_ms: u64,
    /// The actor responsible for the deletion.
    pub deleted_by: ActorId,
    /// The directory binding that the deletion removed.
    pub deleted_binding: DirectoryBinding,
}

/// One page of the namespace's recoverable deletions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListTrashResponse {
    /// The namespace that was read.
    pub namespace_id: NamespaceId,
    /// The head sequence that this page was evaluated at.
    pub head_seq: ChangeSeq,
    /// The recoverable deletions, oldest first.
    pub entries: Vec<TrashEntry>,
    /// The cursor for the next page. It's present when another page follows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

/// One immutable revision of a file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FileRevision {
    /// The file inode that this revision belongs to.
    #[serde(with = "crate::public_inode_id")]
    pub inode_id: InodeId,
    /// The revision number within the file inode.
    pub revision_no: RevisionNo,
    /// The namespace sequence of the commit that created this revision.
    pub committed_seq: ChangeSeq,
    /// The ID of the commit that created this revision.
    pub commit_id: CommitId,
    /// The time the revision was committed, in Unix milliseconds. Commit order comes from
    /// `committed_seq`, not from this time.
    pub committed_at_ms: u64,
    /// The actor responsible for this revision, as the application supplied it.
    pub committed_by: crate::ActorId,
    /// The content stored for this revision.
    pub content_ref: ContentRef,
}

/// One page of a file's revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListFileRevisionsResponse {
    /// The namespace that was read.
    pub namespace_id: NamespaceId,
    /// The file inode whose revisions were returned.
    #[serde(with = "crate::public_inode_id")]
    pub inode_id: InodeId,
    /// The namespace head sequence that the read used.
    pub head_seq: ChangeSeq,
    /// The retained revisions, in order.
    pub revisions: Vec<FileRevision>,
    /// An opaque cursor for the next page, if more revisions are available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_cursor: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NameKey;
    use std::assert_matches;

    fn binding_version() -> crate::BindingVersion {
        crate::BindingVersion::parse("abcdef").expect("binding version")
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
            created_by: ActorId::loonfs(),
            created_at_ms: 1_752_624_000_000,
            kind: PathEntryKind::Directory {},
            head_seq: ChangeSeq(3),
            parent_inode_id,
            display_name: display_name.map(|name| DisplayName::parse(name).expect("display name")),
            binding_version: parent_inode_id.map(|_| binding_version()),
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
                "created_by": "loonfs",
                "created_at_ms": 1_752_624_000_000_u64,
                "inode_kind": "dir",
                "head_seq": 3,
                "parent_inode_id": "ino_1",
                "display_name": "docs",
                "binding_version": binding_version()
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
                    "created_by": "loonfs",
                    "created_at_ms": 1_752_624_000_000_u64,
                    "inode_kind": "dir",
                    "head_seq": 3,
                    "parent_inode_id": "ino_1",
                    "display_name": "docs",
                    "binding_version": binding_version()
                }]
            })
        );
    }

    #[test]
    fn a_path_entry_decodes_a_derived_path_over_the_request_limits() {
        let name = DisplayName::parse("d").expect("display name");
        let mut derived = entry("/d", Some(InodeId(1)), Some("d"));
        derived.path =
            (0..=crate::MAX_PATH_DEPTH).fold(AbsolutePath::root(), |path, _| path.join(&name));
        let mut wire = serde_json::to_value(&derived).expect("serialize entry");
        assert_eq!(
            serde_json::from_value::<PathEntry>(wire.clone()).expect("decode the derived path"),
            derived
        );
        serde_json::from_value::<AbsolutePath>(wire["path"].clone())
            .expect_err("a request path keeps the limits");
        wire["path"] = serde_json::json!("d/d");
        serde_json::from_value::<PathEntry>(wire).expect_err("a derived path keeps the grammar");
    }

    #[test]
    fn a_file_entry_serializes_its_required_payload_with_the_kind() {
        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            crate::ContentId::generate(),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        );
        let mut file = entry("/report.txt", Some(InodeId(1)), Some("report.txt"));
        file.kind = PathEntryKind::File {
            revision_no: RevisionNo(7),
            size_bytes: 5,
            content_ref: content_ref.clone(),
            revision_committed_by: ActorId::loonfs(),
            revision_committed_at_ms: 1_752_624_000_000,
        };

        assert_eq!(
            serde_json::to_value(file).expect("serialize file entry"),
            serde_json::json!({
                "namespace_id": "demo",
                "path": "/report.txt",
                "inode_id": "ino_2",
                "created_by": "loonfs",
                "created_at_ms": 1_752_624_000_000_u64,
                "inode_kind": "file",
                "revision_no": 7,
                "size_bytes": 5,
                "content_ref": content_ref,
                "revision_committed_by": "loonfs",
                "revision_committed_at_ms": 1_752_624_000_000_u64,
                "head_seq": 3,
                "parent_inode_id": "ino_1",
                "display_name": "report.txt",
                "binding_version": binding_version()
            })
        );
    }

    #[test]
    fn nameless_root_omits_parent_inode_id_and_display_name() {
        let root_json = serde_json::to_value(entry("/", None, None)).expect("serialize root");
        assert!(root_json.get("parent_inode_id").is_none());
        assert!(root_json.get("display_name").is_none());
        assert!(root_json.get("binding_version").is_none());

        let decoded: PathEntry =
            serde_json::from_value(root_json).expect("decode root without optional fields");
        assert_eq!(decoded.parent_inode_id, None);
        assert_eq!(decoded.display_name, None);
        assert_eq!(decoded.binding_version, None);

        let named_json = serde_json::to_value(entry("/docs", Some(InodeId(1)), Some("docs")))
            .expect("serialize named entry");
        assert_eq!(named_json["parent_inode_id"], "ino_1");
        assert_eq!(named_json["display_name"], "docs");
        assert_eq!(named_json["binding_version"], binding_version().as_str());
    }

    #[test]
    fn entry_kinds_share_inode_kind_wire_values() {
        let directory = PathEntryKind::Directory {};
        assert_eq!(
            serde_json::to_value(directory).expect("serialize directory entry kind")["inode_kind"],
            serde_json::to_value(InodeKind::Directory).expect("serialize directory inode kind")
        );

        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            crate::ContentId::generate(),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        );
        let file = PathEntryKind::File {
            revision_no: RevisionNo(1),
            size_bytes: 5,
            content_ref,
            revision_committed_by: ActorId::loonfs(),
            revision_committed_at_ms: 1,
        };
        assert_eq!(
            serde_json::to_value(file).expect("serialize file entry kind")["inode_kind"],
            serde_json::to_value(InodeKind::File).expect("serialize file inode kind")
        );

        for kind in [InodeKind::Directory, InodeKind::File] {
            let wire = serde_json::to_value(kind).expect("serialize inode kind");
            assert_eq!(
                serde_json::to_value(EntryInodeKind::from(kind)).expect("serialize entry kind"),
                wire
            );
            assert_eq!(
                serde_json::from_value::<EntryInodeKind>(wire).expect("decode entry kind"),
                EntryInodeKind::from(kind)
            );
        }
    }

    #[test]
    fn requested_attributes_serialize_as_flat_prefixed_siblings() {
        let mut projected = entry("/docs", Some(InodeId(1)), Some("docs"));
        projected.attributes = Some(AttributesProjection {
            attributes_revision_no: crate::AttributesRevisionNo(7),
            attributes_updated_by: Some(ActorId::loonfs()),
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
            serde_json::json!("loonfs")
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
            crate::AttributesRevisionNo(7)
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
            attributes_revision_no: crate::AttributesRevisionNo(0),
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
            inode_kind: EntryInodeKind::File,
            deletion_seq: ChangeSeq(417),
            deleted_at_ms: 1,
            deleted_by: ActorId::loonfs(),
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
                "inode_kind": "file",
                "deletion_seq": 417,
                "deleted_at_ms": 1,
                "deleted_by": "loonfs",
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
            inode_kind: EntryInodeKind::File,
            deletion_seq: ChangeSeq(417),
            deleted_at_ms: 1_752_625_000_000,
            deleted_by: ActorId::loonfs(),
            deleted_binding: DirectoryBinding {
                parent_inode_id: InodeId(7),
                name_key: NameKey::parse("report.txt").expect("name key"),
                display_name: DisplayName::parse("Report.txt").expect("display name"),
            },
        };
        let trash_json = serde_json::to_value(trash).expect("serialize trash entry");
        assert_eq!(trash_json["inode_id"], serde_json::json!("ino_42"));
        assert_eq!(trash_json["deletion_seq"], serde_json::json!(417));

        let operation_json = serde_json::json!({
            "kind": "undelete",
            "inode_id": trash_json["inode_id"].clone(),
            "deletion_seq": trash_json["deletion_seq"].clone()
        });
        let operation: crate::FilesystemOperation =
            serde_json::from_value(operation_json).expect("decode copied trash handle");
        assert_matches!(
            operation,
            crate::FilesystemOperation::Undelete {
                inode_id: InodeId(42),
                deletion_seq: ChangeSeq(417),
                destination_path: None,
                destination_parent_inode_id: None,
                destination_display_name: None,
            }
        );
    }

    #[test]
    fn file_revision_provenance_fields_are_pinned_on_the_wire() {
        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            crate::ContentId::generate(),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        );
        let revision = FileRevision {
            inode_id: InodeId(2),
            revision_no: RevisionNo(3),
            committed_seq: ChangeSeq(7),
            commit_id: CommitId::parse("c_revision_owner").expect("commit id"),
            committed_at_ms: 1_752_624_000_000,
            committed_by: crate::ActorId::loonfs(),
            content_ref: content_ref.clone(),
        };

        assert_eq!(
            serde_json::to_value(revision).expect("serialize file revision"),
            serde_json::json!({
                "inode_id": "ino_2",
                "revision_no": 3,
                "committed_seq": 7,
                "commit_id": "c_revision_owner",
                "committed_at_ms": 1_752_624_000_000_u64,
                "committed_by": "loonfs",
                "content_ref": content_ref,
            })
        );
    }
}
