//! Commit responses and change-feed shapes for the v0 HTTP API.

use crate::{
    AccessGrants, AccessRevisionNo, AttributeRevisionNo, Attributes, BindingGeneration, ChangeSeq,
    CommitId, ContentRef, DisplayName, InodeId, NameKey, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

/// One committed logical commit: its identity and the events it applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Commit {
    /// Namespace that changed.
    pub namespace_id: NamespaceId,
    /// The idempotency key for the commit.
    pub commit_id: CommitId,
    /// Sequence number where the commit became visible.
    pub committed_seq: ChangeSeq,
    /// Actor responsible for the commit, as supplied by the application.
    pub committed_by: crate::ActorId,
    /// The commit time in Unix milliseconds; `committed_seq` defines commit order.
    pub committed_at_ms: u64,
    /// The optional caller annotation for the commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub message: Option<String>,
    /// Present on the change feed and on a replayed commit response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub events: Option<Vec<FilesystemChange>>,
}

/// A directory entry's parent and name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DirectoryBinding {
    /// Parent directory containing the entry.
    #[serde(with = "crate::public_inode_id")]
    pub parent_inode_id: InodeId,
    /// Name used to look up the entry.
    pub name_key: NameKey,
    /// Name shown to users.
    pub display_name: DisplayName,
}

/// One filesystem change within a commit.
///
/// One request operation can produce multiple changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FilesystemChange {
    /// A directory was created.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemChangeDirectoryCreated")
    )]
    DirectoryCreated {
        /// Newly allocated namespace-scoped inode identity.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Directory the new entry was bound under.
        #[serde(with = "crate::public_inode_id")]
        parent_inode_id: InodeId,
        /// User-facing spelling of the new entry.
        display_name: DisplayName,
        /// Opaque identifier for the binding created by this event.
        binding_generation: BindingGeneration,
    },
    /// A file and its first revision were created.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeFileCreated"))]
    FileCreated {
        /// Newly allocated namespace-scoped inode identity.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Directory the new entry was bound under.
        #[serde(with = "crate::public_inode_id")]
        parent_inode_id: InodeId,
        /// User-facing spelling of the new entry.
        display_name: DisplayName,
        /// Opaque identifier for the binding created by this event.
        binding_generation: BindingGeneration,
        /// First revision number.
        revision_no: RevisionNo,
        /// Content of the first revision.
        content_ref: ContentRef,
    },
    /// A file received a new current revision from a put or revision restore.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeContentChanged"))]
    ContentChanged {
        /// File inode whose history advanced.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// New monotonic position in that file's revision history.
        revision_no: RevisionNo,
        /// Immutable content published by the revision.
        content_ref: ContentRef,
    },
    /// An inode moved to a new parent directory or name.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeMoved"))]
    Moved {
        /// Inode whose binding changed.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Directory that held the removed binding.
        #[serde(with = "crate::public_inode_id")]
        source_parent_inode_id: InodeId,
        /// Spelling of the removed binding.
        source_display_name: DisplayName,
        /// Directory holding the new binding.
        #[serde(with = "crate::public_inode_id")]
        destination_parent_inode_id: InodeId,
        /// Spelling of the new binding.
        destination_display_name: DisplayName,
        /// Opaque identifier for the binding created by this event.
        binding_generation: BindingGeneration,
    },
    /// A file or directory subtree was deleted.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeDeleted"))]
    Deleted {
        /// Inode at the root of the deleted subtree.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Directory binding removed by the deletion.
        deleted_binding: DirectoryBinding,
    },
    /// A deleted inode was recovered and re-bound.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeUndeleted"))]
    Undeleted {
        /// Recovered inode.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Directory the recovered entry was bound under.
        #[serde(with = "crate::public_inode_id")]
        parent_inode_id: InodeId,
        /// Spelling of the recovered binding.
        display_name: DisplayName,
        /// Opaque identifier for the binding created by this event.
        binding_generation: BindingGeneration,
    },
    /// An inode's attributes changed.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemChangeAttributesChanged")
    )]
    AttributesChanged {
        /// Inode whose attributes advanced.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// New attribute revision for that inode.
        attributes_revision_no: AttributeRevisionNo,
        /// The inode's complete attribute map after the update, including an empty map
        /// when all attributes were cleared.
        attributes: Attributes,
    },
    /// An inode's access row was replaced. `grants` is the complete
    /// direct grant map after the update.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeAccessChanged"))]
    AccessChanged {
        /// Inode whose access state advanced.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Revision published by the update.
        access_revision_no: AccessRevisionNo,
        /// Whether the directory stops inheritance from its ancestors.
        boundary: bool,
        /// The inode's complete direct grants after this update.
        grants: AccessGrants,
    },
}

/// Change-feed response after a cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListChangesResponse {
    /// Namespace whose ordered commit stream was read.
    pub namespace_id: NamespaceId,
    /// Exclusive cursor supplied by the caller, or the endpoint's initial position.
    pub after_seq: ChangeSeq,
    /// Snapshot head through which this page was evaluated.
    pub through_seq: ChangeSeq,
    /// Cursor to request when another page remains, or `None` at `through_seq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_after_seq: Option<ChangeSeq>,
    /// Logical commits after `after_seq`, ordered by ascending namespace sequence.
    pub changes: Vec<Commit>,
}

#[cfg(test)]
mod tests {
    use super::{Commit, FilesystemChange};
    use crate::{AccessRevisionNo, InodeId};

    fn binding_generation() -> crate::BindingGeneration {
        crate::BindingGeneration::parse("abcdef").expect("binding generation")
    }

    #[test]
    fn commit_uses_committed_by_on_the_wire() {
        let change = Commit {
            namespace_id: crate::NamespaceId::parse("demo").expect("valid namespace id"),
            committed_seq: crate::ChangeSeq(7),
            commit_id: crate::CommitId::parse("example-commit").expect("valid commit id"),
            committed_by: crate::ActorId::loonfs(),
            committed_at_ms: 1_752_624_000_000,
            message: None,
            events: Some(Vec::new()),
        };

        assert_eq!(
            serde_json::to_value(change).expect("serialize committed change"),
            serde_json::json!({
                "namespace_id": "demo",
                "committed_seq": 7,
                "commit_id": "example-commit",
                "committed_by": "loonfs",
                "committed_at_ms": 1_752_624_000_000_u64,
                "events": [],
            })
        );
    }

    #[test]
    fn a_commit_carries_events_at_the_top_level() {
        let response = Commit {
            namespace_id: crate::NamespaceId::parse("demo").expect("valid namespace id"),
            committed_seq: crate::ChangeSeq(419),
            commit_id: crate::CommitId::parse("example-commit").expect("valid commit id"),
            committed_by: crate::ActorId::loonfs(),
            committed_at_ms: 1_752_624_000_000,
            message: Some("import the reports".to_owned()),
            events: Some(vec![FilesystemChange::DirectoryCreated {
                inode_id: InodeId(43),
                parent_inode_id: InodeId(1),
                display_name: crate::DisplayName::parse("docs").expect("valid display name"),
                binding_generation: binding_generation(),
            }]),
        };

        assert_eq!(
            serde_json::to_value(response).expect("serialize commit response"),
            serde_json::json!({
                "namespace_id": "demo",
                "commit_id": "example-commit",
                "committed_seq": 419,
                "committed_by": "loonfs",
                "committed_at_ms": 1_752_624_000_000_u64,
                "message": "import the reports",
                "events": [{
                    "kind": "directory_created",
                    "inode_id": "ino_43",
                    "parent_inode_id": "ino_1",
                    "display_name": "docs",
                    "binding_generation": binding_generation(),
                }],
            })
        );
    }

    #[test]
    fn a_commit_omits_absent_events_and_message() {
        let response = Commit {
            namespace_id: crate::NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: crate::CommitId::parse("example-commit").expect("valid commit id"),
            committed_seq: crate::ChangeSeq(419),
            committed_by: crate::ActorId::loonfs(),
            committed_at_ms: 1_752_624_000_000,
            message: None,
            events: None,
        };

        assert_eq!(
            serde_json::to_value(response).expect("serialize commit response"),
            serde_json::json!({
                "namespace_id": "demo",
                "commit_id": "example-commit",
                "committed_seq": 419,
                "committed_by": "loonfs",
                "committed_at_ms": 1_752_624_000_000_u64,
            })
        );
    }

    #[test]
    fn filesystem_change_events_use_snake_case_kind_tags() {
        let sample_content_ref = crate::ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            crate::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                .expect("valid content id"),
            b"hello",
        );
        let sample_content_ref_json = r#"{"kind":"blob_v1","owner_namespace_id":"demo","content_id":"con_0123456789abcdef0123456789abcdef","size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}"#;

        let generation = binding_generation();
        let directory_created = FilesystemChange::DirectoryCreated {
            inode_id: InodeId(2),
            parent_inode_id: InodeId(1),
            display_name: crate::DisplayName::parse("Docs").expect("valid display name"),
            binding_generation: generation.clone(),
        };
        assert_eq!(
            serde_json::to_string(&directory_created).expect("serialize directory-created event"),
            format!(
                r#"{{"kind":"directory_created","inode_id":"ino_2","parent_inode_id":"ino_1","display_name":"Docs","binding_generation":"{generation}"}}"#
            )
        );

        let file_created = FilesystemChange::FileCreated {
            inode_id: InodeId(2),
            parent_inode_id: InodeId(1),
            display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            binding_generation: generation.clone(),
            revision_no: crate::RevisionNo(1),
            content_ref: sample_content_ref.clone(),
        };
        assert_eq!(
            serde_json::to_string(&file_created).expect("serialize file-created event"),
            format!(
                r#"{{"kind":"file_created","inode_id":"ino_2","parent_inode_id":"ino_1","display_name":"a.txt","binding_generation":"{generation}","revision_no":1,"content_ref":{sample_content_ref_json}}}"#
            )
        );

        let missing_content_ref = r#"{"kind":"file_created","inode_id":"ino_2","parent_inode_id":"ino_1","display_name":"a.txt","revision_no":1}"#;
        assert!(serde_json::from_str::<FilesystemChange>(missing_content_ref).is_err());

        let retired_creation = serde_json::json!({
            "kind": (["cre", "ated"].concat()),
            "inode_id": "ino_2",
            "inode_kind": "file",
            "parent_inode_id": "ino_1",
            "display_name": "a.txt",
            "revision_no": 1,
        });
        assert!(serde_json::from_value::<FilesystemChange>(retired_creation).is_err());

        let content_changed = FilesystemChange::ContentChanged {
            inode_id: InodeId(2),
            revision_no: crate::RevisionNo(3),
            content_ref: sample_content_ref,
        };
        assert_eq!(
            serde_json::to_string(&content_changed).expect("serialize content changed event"),
            format!(
                r#"{{"kind":"content_changed","inode_id":"ino_2","revision_no":3,"content_ref":{sample_content_ref_json}}}"#
            )
        );

        let moved = FilesystemChange::Moved {
            inode_id: InodeId(2),
            source_parent_inode_id: InodeId(1),
            source_display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            destination_parent_inode_id: InodeId(3),
            destination_display_name: crate::DisplayName::parse("b.txt")
                .expect("valid display name"),
            binding_generation: generation.clone(),
        };
        assert_eq!(
            serde_json::from_value::<FilesystemChange>(serde_json::json!({
                "kind": "moved",
                "inode_id": "ino_2",
                "source_parent_inode_id": "ino_1",
                "source_display_name": "a.txt",
                "destination_parent_inode_id": "ino_3",
                "destination_display_name": "b.txt",
                "binding_generation": generation
            }))
            .expect("decode moved event"),
            moved
        );
        assert_eq!(
            serde_json::to_string(&moved).expect("serialize moved event"),
            format!(
                r#"{{"kind":"moved","inode_id":"ino_2","source_parent_inode_id":"ino_1","source_display_name":"a.txt","destination_parent_inode_id":"ino_3","destination_display_name":"b.txt","binding_generation":"{generation}"}}"#
            )
        );

        let deleted = FilesystemChange::Deleted {
            inode_id: InodeId(2),
            deleted_binding: super::DirectoryBinding {
                parent_inode_id: InodeId(1),
                name_key: crate::NameKey::parse("a.txt").expect("valid name key"),
                display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            },
        };
        assert_eq!(
            serde_json::to_string(&deleted).expect("serialize deleted event"),
            r#"{"kind":"deleted","inode_id":"ino_2","deleted_binding":{"parent_inode_id":"ino_1","name_key":"a.txt","display_name":"a.txt"}}"#
        );

        let undeleted = FilesystemChange::Undeleted {
            inode_id: InodeId(2),
            parent_inode_id: InodeId(1),
            display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            binding_generation: generation.clone(),
        };
        assert_eq!(
            serde_json::to_string(&undeleted).expect("serialize undeleted event"),
            format!(
                r#"{{"kind":"undeleted","inode_id":"ino_2","parent_inode_id":"ino_1","display_name":"a.txt","binding_generation":"{generation}"}}"#
            )
        );

        let attributes_changed = FilesystemChange::AttributesChanged {
            inode_id: InodeId(2),
            attributes_revision_no: crate::AttributeRevisionNo(4),
            attributes: crate::Attributes::new(std::collections::BTreeMap::from([(
                crate::AttributeKey::parse("owner").expect("valid attribute key"),
                crate::AttributeValue::parse("ada").expect("valid attribute value"),
            )]))
            .expect("valid attribute map"),
        };
        assert_eq!(
            serde_json::to_string(&attributes_changed).expect("serialize attributes event"),
            r#"{"kind":"attributes_changed","inode_id":"ino_2","attributes_revision_no":4,"attributes":{"owner":"ada"}}"#
        );

        // A clear is a real event carrying the empty map, not an absence.
        let cleared = FilesystemChange::AttributesChanged {
            inode_id: InodeId(2),
            attributes_revision_no: crate::AttributeRevisionNo(5),
            attributes: crate::Attributes::default(),
        };
        assert_eq!(
            serde_json::to_string(&cleared).expect("serialize cleared attributes event"),
            r#"{"kind":"attributes_changed","inode_id":"ino_2","attributes_revision_no":5,"attributes":{}}"#
        );

        let access_changed = FilesystemChange::AccessChanged {
            inode_id: InodeId(2),
            access_revision_no: AccessRevisionNo(3),
            boundary: true,
            grants: serde_json::from_value(serde_json::json!({"prn_ada": ["read", "write"]}))
                .expect("grants"),
        };
        assert_eq!(
            serde_json::to_string(&access_changed).expect("serialize access event"),
            r#"{"kind":"access_changed","inode_id":"ino_2","access_revision_no":3,"boundary":true,"grants":{"prn_ada":["read","write"]}}"#
        );
    }
}
