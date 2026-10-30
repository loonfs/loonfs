//! Commit requests, commit responses, and change-feed shapes for the v0 HTTP API.

use super::{ContentToken, FilesystemOperation};
use crate::options::CommitOptions;
use crate::{
    AbsolutePath, AccessGrants, AccessRevisionNo, Attributes, AttributesRevisionNo, BindingVersion,
    ChangeSeq, CommitId, ContentRef, DisplayName, InodeId, NameKey, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

/// One commit, including its identity and the events it applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Commit {
    /// The namespace that changed.
    pub namespace_id: NamespaceId,
    /// The commit's idempotency key.
    pub commit_id: CommitId,
    /// The sequence at which the commit became visible.
    pub committed_seq: ChangeSeq,
    /// The actor responsible for the commit, as the application supplied it.
    pub committed_by: crate::ActorId,
    /// The time the commit was made, in Unix milliseconds. Commit order comes from `committed_seq`,
    /// not from this time.
    pub committed_at_ms: u64,
    /// The caller's optional annotation for the commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub message: Option<String>,
    /// The semantic filesystem changes that the commit applied, in request order.
    pub events: Vec<FilesystemChange>,
}

/// A directory entry's parent and name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DirectoryBinding {
    /// The parent directory that contains the entry.
    #[serde(with = "crate::inode_id::public")]
    pub parent_inode_id: InodeId,
    /// The name key that lookups use to find the entry.
    pub name_key: NameKey,
    /// The name that users see.
    pub display_name: DisplayName,
}

/// One filesystem change within a commit.
///
/// One request operation can produce several changes.
///
/// A newer server can return a change kind that isn't listed here. Clients should skip a change
/// whose kind they don't recognize.
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
        /// The new inode, unique within the namespace.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The directory that the new entry was bound under.
        #[serde(with = "crate::inode_id::public")]
        parent_inode_id: InodeId,
        /// The display name of the new entry.
        display_name: DisplayName,
        /// The binding that this event created. The value is opaque.
        binding_version: BindingVersion,
    },
    /// A file and its first revision were created.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeFileCreated"))]
    FileCreated {
        /// The new inode, unique within the namespace.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The directory that the new entry was bound under.
        #[serde(with = "crate::inode_id::public")]
        parent_inode_id: InodeId,
        /// The display name of the new entry.
        display_name: DisplayName,
        /// The binding that this event created. The value is opaque.
        binding_version: BindingVersion,
        /// The file's first revision number.
        revision_no: RevisionNo,
        /// The content of the first revision.
        content_ref: ContentRef,
    },
    /// A file received a new current revision from a put, a copy, an append, or a revision
    /// restore.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeContentChanged"))]
    ContentChanged {
        /// The file inode whose history advanced.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The file's new revision number, which is higher than the number of every earlier
        /// revision.
        revision_no: RevisionNo,
        /// The immutable content that the revision published.
        content_ref: ContentRef,
        /// The offset where the appended bytes begin, present when an append published the
        /// revision.
        ///
        /// It equals the base revision's `size_bytes`, so the new revision's bytes are the base
        /// revision's bytes followed by the bytes from this offset to `size_bytes`. It's
        /// present for every append, including one that starts a new content ID, and absent for
        /// a put, a copy, or a revision restore.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        append_offset: Option<u64>,
    },
    /// An inode moved to a new parent directory or name.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeMoved"))]
    Moved {
        /// The inode whose binding changed.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The directory that contained the removed binding.
        #[serde(with = "crate::inode_id::public")]
        source_parent_inode_id: InodeId,
        /// The display name of the removed binding.
        source_display_name: DisplayName,
        /// The directory that contains the new binding.
        #[serde(with = "crate::inode_id::public")]
        destination_parent_inode_id: InodeId,
        /// The display name of the new binding.
        destination_display_name: DisplayName,
        /// The binding that this event created. The value is opaque.
        binding_version: BindingVersion,
    },
    /// A file or directory subtree was deleted.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeDeleted"))]
    Deleted {
        /// The inode at the root of the deleted subtree.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The directory binding that the deletion removed.
        deleted_binding: DirectoryBinding,
    },
    /// A deleted inode was recovered and re-bound.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeUndeleted"))]
    Undeleted {
        /// The recovered inode.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The directory that the recovered entry was bound under.
        #[serde(with = "crate::inode_id::public")]
        parent_inode_id: InodeId,
        /// The display name of the recovered binding.
        display_name: DisplayName,
        /// The binding that this event created. The value is opaque.
        binding_version: BindingVersion,
    },
    /// An inode's attributes changed.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemChangeAttributesChanged")
    )]
    AttributesChanged {
        /// The inode whose attributes changed.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The inode's new attribute revision.
        attributes_revision_no: AttributesRevisionNo,
        /// The inode's complete attribute map after the update. The map is empty when the update
        /// removed every attribute.
        attributes: Attributes,
    },
    /// An inode's access row was replaced.
    ///
    /// `grants` is the complete direct grant map after the update.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeAccessChanged"))]
    AccessChanged {
        /// The inode whose access row changed.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The access revision that the update published.
        access_revision_no: AccessRevisionNo,
        /// Whether the directory stops inheriting grants from its ancestors.
        boundary: bool,
        /// Every direct grant that the inode has after this update.
        grants: AccessGrants,
    },
    /// A change kind from a newer server. This variant doesn't include the kind's own fields.
    ///
    /// It serializes as `unknown` because the reported kind isn't kept.
    #[serde(other)]
    Unknown,
}

/// The commits after a change feed cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListChangesResponse {
    /// The namespace whose change feed was read.
    pub namespace_id: NamespaceId,
    /// The exclusive cursor that this page starts after. It's the cursor that the caller supplied,
    /// or the endpoint's initial position.
    pub after_seq: ChangeSeq,
    /// The namespace head sequence that this page was read at.
    pub head_seq: ChangeSeq,
    /// The cursor for the next page, when another page remains. It's absent on the page that
    /// reaches `head_seq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub next_after_seq: Option<ChangeSeq>,
    /// The logical commits after `after_seq`, in ascending namespace sequence order.
    pub changes: Vec<Commit>,
}

/// A condition that a commit must meet before its operations run.
///
/// The server checks each condition against the state before the commit. When several commits are
/// applied as one batch, that state's head sequence is the sequence of the last commit admitted
/// earlier in the batch, or the batch's starting head sequence if no earlier commit was admitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitPrecondition {
    /// A requirement that the head sequence before the commit equals `expected_head_seq`.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionNamespaceHead"))]
    NamespaceHead {
        /// The head sequence that the caller observed when it read its inputs.
        expected_head_seq: ChangeSeq,
    },
    /// A requirement that the inode is visible and has the content revision that the caller read.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionFileRevision"))]
    FileRevision {
        /// The inode whose state the caller read.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The content revision that the caller observed.
        expected_revision_no: RevisionNo,
    },
    /// A requirement that the path still has the binding that the caller read.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionPathBinding"))]
    PathBinding {
        /// The absolute path to check, which can be the root.
        path: AbsolutePath,
        /// The inode that the path must be bound to.
        #[serde(with = "crate::inode_id::public")]
        expected_inode_id: InodeId,
        /// The binding version that the path must also have, if set. This check detects an inode
        /// that was moved away from the path and then back.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_binding_version: Option<BindingVersion>,
    },
    /// A requirement that a visible inode still has the binding that the caller read.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionInodeBinding"))]
    InodeBinding {
        /// The inode whose binding the caller read.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The binding version that the caller observed.
        expected_binding_version: BindingVersion,
    },
    /// A requirement that the inode is visible and has the attribute revision that the caller read.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionAttributesRevision")
    )]
    AttributesRevision {
        /// The inode whose state the caller read.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The attribute revision that the caller observed.
        expected_attributes_revision_no: AttributesRevisionNo,
    },
    /// A requirement that the inode is visible and has the access revision that the caller read.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionAccessRevision")
    )]
    AccessRevision {
        /// The inode whose state the caller read.
        #[serde(with = "crate::inode_id::public")]
        inode_id: InodeId,
        /// The access revision that the caller observed.
        expected_access_revision_no: AccessRevisionNo,
    },
    /// A requirement that no visible entry exists at the full path.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionPathAbsence"))]
    PathAbsence {
        /// The absolute path to check, which can be the root.
        path: AbsolutePath,
    },
    /// A requirement that no visible entry with this name exists under the parent inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionNameAbsence"))]
    NameAbsence {
        /// The directory to check.
        #[serde(with = "crate::inode_id::public")]
        parent_inode_id: InodeId,
        /// The name to check.
        display_name: DisplayName,
    },
}

/// A request to commit one or more filesystem operations atomically, in order.
///
/// The server rejects unknown fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    /// The caller's idempotency key for the whole request.
    pub commit_id: CommitId,
    /// An annotation from the caller. The message is part of the commit's identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub message: Option<String>,
    /// One content token for each uploaded `content_ref` that the request uses.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_tokens: Vec<ContentToken>,
    /// The conditions that the server checks, in order, before it runs any operation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preconditions: Vec<CommitPrecondition>,
    /// The operations to commit atomically, in order. The list can't be empty.
    pub operations: Vec<FilesystemOperation>,
}

impl CommitRequest {
    /// Builds a request for `operations` under the commit ID, message, and
    /// preconditions of `commit`, with a generated commit ID when `commit`
    /// has none.
    pub fn with_operations(commit: &CommitOptions, operations: Vec<FilesystemOperation>) -> Self {
        Self {
            commit_id: commit.commit_id_or_generate(),
            message: commit.message.clone(),
            content_tokens: Vec::new(),
            preconditions: commit.preconditions.clone(),
            operations,
        }
    }

    /// Sets the admission conditions in caller order.
    pub fn preconditions(mut self, preconditions: Vec<CommitPrecondition>) -> Self {
        self.preconditions = preconditions;
        self
    }

    /// Builds a request with exactly one operation.
    pub fn single(
        commit_id: CommitId,
        message: Option<String>,
        operation: FilesystemOperation,
    ) -> Self {
        Self {
            commit_id,
            message,
            content_tokens: Vec::new(),
            preconditions: Vec::new(),
            operations: vec![operation],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Commit, CommitPrecondition, CommitRequest, FilesystemChange, FilesystemOperation};
    use crate::{AccessRevisionNo, ContentId, ContentRef, InodeId, RevisionNo};
    use std::assert_matches;

    fn binding_version() -> crate::BindingVersion {
        crate::BindingVersion::parse("abcdef").expect("binding version")
    }

    fn sample_content_ref() -> ContentRef {
        ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("valid content id"),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        )
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
            events: vec![FilesystemChange::DirectoryCreated {
                inode_id: InodeId(43),
                parent_inode_id: crate::ROOT_INODE_ID,
                display_name: crate::DisplayName::parse("docs").expect("valid display name"),
                binding_version: binding_version(),
            }],
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
                    "inode_id": "ino_0000000000000000000000001b",
                    "parent_inode_id": "ino_00000000000000000000000000",
                    "display_name": "docs",
                    "binding_version": binding_version(),
                }],
            })
        );
    }

    #[test]
    fn a_commit_omits_an_absent_message() {
        let response = Commit {
            namespace_id: crate::NamespaceId::parse("demo").expect("valid namespace id"),
            commit_id: crate::CommitId::parse("example-commit").expect("valid commit id"),
            committed_seq: crate::ChangeSeq(419),
            committed_by: crate::ActorId::loonfs(),
            committed_at_ms: 1_752_624_000_000,
            message: None,
            events: vec![FilesystemChange::DirectoryCreated {
                inode_id: InodeId(43),
                parent_inode_id: crate::ROOT_INODE_ID,
                display_name: crate::DisplayName::parse("docs").expect("valid display name"),
                binding_version: binding_version(),
            }],
        };

        assert_eq!(
            serde_json::to_value(response).expect("serialize commit response"),
            serde_json::json!({
                "namespace_id": "demo",
                "commit_id": "example-commit",
                "committed_seq": 419,
                "committed_by": "loonfs",
                "committed_at_ms": 1_752_624_000_000_u64,
                "events": [{
                    "kind": "directory_created",
                    "inode_id": "ino_0000000000000000000000001b",
                    "parent_inode_id": "ino_00000000000000000000000000",
                    "display_name": "docs",
                    "binding_version": binding_version(),
                }],
            })
        );
    }

    #[test]
    fn filesystem_change_events_use_snake_case_kind_tags() {
        let sample_content_ref = crate::ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            crate::ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("valid content id"),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        );
        let sample_content_ref_json = r#"{"kind":"blob_v1","owner_namespace_id":"demo","content_id":"con_014d2pf2dbsqqg28t5cy4tqkff","size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"3377857006524257"}}"#;

        let version = binding_version();
        let directory_created = FilesystemChange::DirectoryCreated {
            inode_id: InodeId(2),
            parent_inode_id: crate::ROOT_INODE_ID,
            display_name: crate::DisplayName::parse("Docs").expect("valid display name"),
            binding_version: version.clone(),
        };
        assert_eq!(
            serde_json::to_string(&directory_created).expect("serialize directory-created event"),
            format!(
                r#"{{"kind":"directory_created","inode_id":"ino_00000000000000000000000002","parent_inode_id":"ino_00000000000000000000000000","display_name":"Docs","binding_version":"{version}"}}"#
            )
        );

        let file_created = FilesystemChange::FileCreated {
            inode_id: InodeId(2),
            parent_inode_id: crate::ROOT_INODE_ID,
            display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            binding_version: version.clone(),
            revision_no: crate::RevisionNo(1),
            content_ref: sample_content_ref.clone(),
        };
        assert_eq!(
            serde_json::to_string(&file_created).expect("serialize file-created event"),
            format!(
                r#"{{"kind":"file_created","inode_id":"ino_00000000000000000000000002","parent_inode_id":"ino_00000000000000000000000000","display_name":"a.txt","binding_version":"{version}","revision_no":1,"content_ref":{sample_content_ref_json}}}"#
            )
        );

        let missing_content_ref = r#"{"kind":"file_created","inode_id":"ino_00000000000000000000000002","parent_inode_id":"ino_00000000000000000000000000","display_name":"a.txt","revision_no":1}"#;
        serde_json::from_str::<FilesystemChange>(missing_content_ref)
            .expect_err("an event without its content ref should be rejected");

        let content_changed = FilesystemChange::ContentChanged {
            inode_id: InodeId(2),
            revision_no: crate::RevisionNo(3),
            content_ref: sample_content_ref.clone(),
            append_offset: None,
        };
        assert_eq!(
            serde_json::to_string(&content_changed).expect("serialize content changed event"),
            format!(
                r#"{{"kind":"content_changed","inode_id":"ino_00000000000000000000000002","revision_no":3,"content_ref":{sample_content_ref_json}}}"#
            )
        );
        let appended = FilesystemChange::ContentChanged {
            inode_id: InodeId(2),
            revision_no: crate::RevisionNo(4),
            content_ref: sample_content_ref,
            append_offset: Some(3),
        };
        let appended_json = format!(
            r#"{{"kind":"content_changed","inode_id":"ino_00000000000000000000000002","revision_no":4,"content_ref":{sample_content_ref_json},"append_offset":3}}"#
        );
        assert_eq!(
            serde_json::to_string(&appended).expect("serialize appended event"),
            appended_json
        );
        assert_eq!(
            serde_json::from_str::<FilesystemChange>(&appended_json)
                .expect("decode appended event"),
            appended
        );

        let moved = FilesystemChange::Moved {
            inode_id: InodeId(2),
            source_parent_inode_id: crate::ROOT_INODE_ID,
            source_display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            destination_parent_inode_id: InodeId(3),
            destination_display_name: crate::DisplayName::parse("b.txt")
                .expect("valid display name"),
            binding_version: version.clone(),
        };
        assert_eq!(
            serde_json::from_value::<FilesystemChange>(serde_json::json!({
                "kind": "moved",
                "inode_id": "ino_00000000000000000000000002",
                "source_parent_inode_id": "ino_00000000000000000000000000",
                "source_display_name": "a.txt",
                "destination_parent_inode_id": "ino_00000000000000000000000003",
                "destination_display_name": "b.txt",
                "binding_version": version
            }))
            .expect("decode moved event"),
            moved
        );
        assert_eq!(
            serde_json::to_string(&moved).expect("serialize moved event"),
            format!(
                r#"{{"kind":"moved","inode_id":"ino_00000000000000000000000002","source_parent_inode_id":"ino_00000000000000000000000000","source_display_name":"a.txt","destination_parent_inode_id":"ino_00000000000000000000000003","destination_display_name":"b.txt","binding_version":"{version}"}}"#
            )
        );

        let deleted = FilesystemChange::Deleted {
            inode_id: InodeId(2),
            deleted_binding: super::DirectoryBinding {
                parent_inode_id: crate::ROOT_INODE_ID,
                name_key: crate::NameKey::parse("a.txt").expect("valid name key"),
                display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            },
        };
        assert_eq!(
            serde_json::to_string(&deleted).expect("serialize deleted event"),
            r#"{"kind":"deleted","inode_id":"ino_00000000000000000000000002","deleted_binding":{"parent_inode_id":"ino_00000000000000000000000000","name_key":"a.txt","display_name":"a.txt"}}"#
        );

        let undeleted = FilesystemChange::Undeleted {
            inode_id: InodeId(2),
            parent_inode_id: crate::ROOT_INODE_ID,
            display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            binding_version: version.clone(),
        };
        assert_eq!(
            serde_json::to_string(&undeleted).expect("serialize undeleted event"),
            format!(
                r#"{{"kind":"undeleted","inode_id":"ino_00000000000000000000000002","parent_inode_id":"ino_00000000000000000000000000","display_name":"a.txt","binding_version":"{version}"}}"#
            )
        );

        let attributes_changed = FilesystemChange::AttributesChanged {
            inode_id: InodeId(2),
            attributes_revision_no: crate::AttributesRevisionNo(4),
            attributes: crate::Attributes::new(std::collections::BTreeMap::from([(
                crate::AttributeKey::parse("owner").expect("valid attribute key"),
                crate::AttributeValue::parse("ada").expect("valid attribute value"),
            )]))
            .expect("valid attribute map"),
        };
        assert_eq!(
            serde_json::to_string(&attributes_changed).expect("serialize attributes event"),
            r#"{"kind":"attributes_changed","inode_id":"ino_00000000000000000000000002","attributes_revision_no":4,"attributes":{"owner":"ada"}}"#
        );

        // A clear is a real event carrying the empty map, not an absence.
        let cleared = FilesystemChange::AttributesChanged {
            inode_id: InodeId(2),
            attributes_revision_no: crate::AttributesRevisionNo(5),
            attributes: crate::Attributes::default(),
        };
        assert_eq!(
            serde_json::to_string(&cleared).expect("serialize cleared attributes event"),
            r#"{"kind":"attributes_changed","inode_id":"ino_00000000000000000000000002","attributes_revision_no":5,"attributes":{}}"#
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
            r#"{"kind":"access_changed","inode_id":"ino_00000000000000000000000002","access_revision_no":3,"boundary":true,"grants":{"prn_ada":["read","write"]}}"#
        );
    }

    #[test]
    fn path_preconditions_reject_ambiguous_shapes() {
        let missing = serde_json::from_value::<CommitPrecondition>(
            serde_json::json!({"kind": "path_binding", "path": "/docs/input"}),
        )
        .expect_err("binding requires an inode");
        assert!(
            missing.to_string().contains("expected_inode_id"),
            "{missing}"
        );
        serde_json::from_value::<CommitPrecondition>(serde_json::json!({
            "kind": "path_binding", "path": "/docs/input", "expected_inode_id": null
        }))
        .expect_err("a null inode is not an absence check");
        let error = serde_json::from_value::<CommitPrecondition>(serde_json::json!({
            "kind": "path_absence", "path": "/docs/input", "expected_inode_id": "ino_0000000000000000000000001a"
        }))
        .expect_err("absence accepts only a path");
        assert!(
            error
                .to_string()
                .contains("unknown field `expected_inode_id`"),
            "{error}"
        );
    }

    #[test]
    fn a_misspelled_precondition_does_not_decode() {
        let put = |precondition: &str| {
            let mut operation = serde_json::json!({
                "kind": "put_file",
                "path": "/docs/a.txt",
                "content_ref": sample_content_ref(),
                "behavior": "replace",
                "expected_inode_id": "ino_00000000000000000000000007"
            });
            operation[precondition] = serde_json::json!(3);
            serde_json::json!({
                "commit_id": "with_preconditions-put",
                "operations": [operation]
            })
        };

        let spelled: CommitRequest = serde_json::from_value(put("expected_revision_no"))
            .expect("the precondition spelled correctly decodes");
        assert_matches!(
            spelled.operations.as_slice(),
            [FilesystemOperation::PutFile {
                expected_revision_no: Some(RevisionNo(3)),
                ..
            }]
        );

        for misspelling in ["expected_revsion_no", "expectedRevisionNo"] {
            serde_json::from_value::<CommitRequest>(put(misspelling)).expect_err(&format!(
                "`{misspelling}` decoded instead of failing the request"
            ));
        }
    }

    #[test]
    fn expected_revision_no_must_fit_the_public_integer_range() {
        let body = |expected_revision_no: u64| {
            serde_json::json!({
                "commit_id": "bounded-revision-precondition",
                "operations": [{
                    "kind": "put_file",
                    "path": "/docs/a.txt",
                    "content_ref": sample_content_ref(),
                    "behavior": "replace",
                    "expected_inode_id": "ino_00000000000000000000000007",
                    "expected_revision_no": expected_revision_no
                }]
            })
        };

        let request: CommitRequest = serde_json::from_value(body(crate::MAX_PUBLIC_INTEGER))
            .expect("deserialize the maximum revision number");
        assert_matches!(
            request.operations.as_slice(),
            [FilesystemOperation::PutFile {
                expected_revision_no: Some(RevisionNo(value)),
                ..
            }] if *value == crate::MAX_PUBLIC_INTEGER
        );

        let error = serde_json::from_value::<CommitRequest>(body(crate::MAX_PUBLIC_INTEGER + 1))
            .expect_err("reject a revision number above the public limit");
        assert!(
            error
                .to_string()
                .contains("must be an integer from 0 through 9007199254740991"),
            "unexpected range error: {error}"
        );
    }

    #[test]
    fn a_commit_request_rejects_unknown_fields_at_every_level() {
        let valid = || {
            serde_json::json!({
                "commit_id": "strict-commit",
                "content_tokens": [{
                    "content_ref": sample_content_ref(),
                    "token": "opaque-proof"
                }],
                "operations": [{
                    "kind": "update_attributes",
                    "path": "/docs/a.txt",
                    "set": {"owner": "ada"},
                    "expected_inode_id": "ino_00000000000000000000000007"
                }]
            })
        };
        serde_json::from_value::<CommitRequest>(valid())
            .expect("the same body without a typo decodes");

        let mut at_root = valid();
        at_root["mesage"] = serde_json::json!("a note");

        let mut in_operation = valid();
        in_operation["operations"][0]["expectedAttributesRevisionNo"] = serde_json::json!(3);

        let mut in_content_token = valid();
        in_content_token["content_tokens"][0]["expires_at_ms"] = serde_json::json!(1);

        let mut in_content_ref = valid();
        in_content_ref["content_tokens"][0]["content_ref"]["sizeBytes"] = serde_json::json!(5);

        for (level, body) in [
            ("the request root", at_root),
            ("an operation variant", in_operation),
            ("a nested content token", in_content_token),
            ("a content ref below that", in_content_ref),
        ] {
            serde_json::from_value::<CommitRequest>(body).expect_err(&format!(
                "an unknown field in {level} decoded instead of failing the request"
            ));
        }
    }
}
