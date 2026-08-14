//! The commit shapes for the v0 HTTP API: the envelope every
//! commit resolves to, and the ordered feed of semantic filesystem events
//! those commits produce. Path-oriented request shapes live in
//! [`super::operations`].

use crate::{
    manifest::DeletedDirentry, AttributeRevisionNo, Attributes, ChangeSeq, CommitId, ContentRef,
    DisplayName, InodeId, InodeKind, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

/// Result of one commit.
///
/// Every commit resolves to this envelope — path-oriented operations and
/// explicit commits, embedded or remote. The commit id is the caller's
/// reconciliation handle: resubmitting the same request with the same id
/// replays this result instead of committing twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommitResponse {
    /// Namespace that changed.
    pub namespace_id: NamespaceId,
    /// Idempotency key the commit landed under: caller-supplied, or
    /// generated on the caller's behalf when the request carried none.
    pub commit_id: CommitId,
    /// Sequence number where the commit became visible.
    pub committed_seq: ChangeSeq,
}

/// One semantic filesystem change inside a commit.
///
/// A commit's events are the operations it applied, in the order it applied
/// them. One request operation can apply several: creating missing parent
/// directories, or replacing a file by moving over it, each produce an event
/// per directory created or file replaced. So a request with three
/// operations may report more than three events, and the events stay in
/// request order. Events name inodes and their parent-directory bindings
/// rather than full paths; a consumer that needs paths can stat the inode or
/// maintain its own binding projection from this feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FilesystemChange {
    /// A file or directory was created.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeCreated"))]
    Created {
        /// Newly allocated namespace-scoped inode identity.
        inode_id: InodeId,
        /// File-or-directory classification fixed at creation.
        inode_kind: InodeKind,
        /// Directory the new entry was bound under.
        parent_inode_id: InodeId,
        /// User-facing spelling of the new entry.
        display_name: DisplayName,
        /// First revision number, for file creations.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        revision_no: Option<RevisionNo>,
        /// Content of the first revision, for file creations.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_ref: Option<ContentRef>,
    },
    /// A file received a new current revision — a put over an existing
    /// file, or a revision restore (one durable fact for both).
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeContentChanged"))]
    ContentChanged {
        /// File inode whose history advanced.
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
        inode_id: InodeId,
        /// Directory that held the old binding.
        from_parent_inode_id: InodeId,
        /// Spelling of the old binding.
        from_display_name: DisplayName,
        /// Directory holding the new binding.
        to_parent_inode_id: InodeId,
        /// Spelling of the new binding.
        to_display_name: DisplayName,
    },
    /// A file or directory subtree was deleted. Use the enclosing change's
    /// `committed_seq` as `deletion_seq` when restoring it.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeDeleted"))]
    Deleted {
        /// Inode at the root of the deleted subtree.
        inode_id: InodeId,
        /// Directory binding removed by the deletion, when the delete
        /// recorded one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deleted_direntry: Option<DeletedDirentry>,
    },
    /// A deleted inode was recovered and re-bound.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemChangeUndeleted"))]
    Undeleted {
        /// Recovered inode.
        inode_id: InodeId,
        /// Directory the recovered entry was bound under.
        parent_inode_id: InodeId,
        /// Spelling of the recovered binding.
        display_name: DisplayName,
    },
    /// An inode's attributes changed.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemChangeAttributesChanged")
    )]
    AttributesChanged {
        /// Inode whose attributes advanced.
        inode_id: InodeId,
        /// New attribute revision for that inode.
        attributes_revision_no: AttributeRevisionNo,
        /// The inode's complete attribute map after the update, so a consumer
        /// projects it without reading anything back. An empty map is the
        /// cleared state.
        attributes: Attributes,
    },
}

/// One committed change in namespace order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommittedChange {
    /// Namespace sequence for this logical commit.
    pub committed_seq: ChangeSeq,
    /// Client idempotency key for this logical commit.
    pub commit_id: CommitId,
    /// Actor responsible for the commit, as supplied by the application.
    pub actor: crate::ActorRef,
    /// Wall-clock stamp of the commit, in Unix milliseconds.
    /// Observational: `committed_seq` is the order.
    pub committed_at_ms: u64,
    /// Caller annotation, omitted when absent and carrying no filesystem semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Semantic filesystem events for this commit, in the order the commit
    /// applied them. One request operation may produce more than one event
    /// (see [`FilesystemChange`]).
    pub events: Vec<FilesystemChange>,
}

/// Change-feed response after a cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ChangesResponse {
    /// Namespace whose ordered commit stream was read.
    pub namespace_id: NamespaceId,
    /// Exclusive cursor supplied by the caller, or the endpoint's initial position.
    pub after_seq: ChangeSeq,
    /// Snapshot head through which this page was evaluated.
    pub through_seq: ChangeSeq,
    /// Cursor to request when another page remains, or `None` at `through_seq`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_seq: Option<ChangeSeq>,
    /// Logical commits after `after_seq`, ordered by ascending namespace sequence.
    pub changes: Vec<CommittedChange>,
}

#[cfg(test)]
mod tests {
    use super::FilesystemChange;
    use crate::{InodeId, InodeKind};

    #[test]
    fn filesystem_change_events_use_snake_case_kind_tags() {
        let sample_content_ref = crate::ContentRef::blob_v1(
            crate::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                .expect("valid content id"),
            b"hello",
        );
        let sample_content_ref_json = r#"{"kind":"blob_v1","content_id":"con_0123456789abcdef0123456789abcdef","size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}"#;

        let created = FilesystemChange::Created {
            inode_id: InodeId(2),
            inode_kind: InodeKind::Directory,
            parent_inode_id: InodeId(1),
            display_name: crate::DisplayName::parse("Docs").expect("valid display name"),
            revision_no: None,
            content_ref: None,
        };
        assert_eq!(
            serde_json::to_string(&created).expect("serialize created event"),
            r#"{"kind":"created","inode_id":2,"inode_kind":"dir","parent_inode_id":1,"display_name":"Docs"}"#
        );

        let created_file = FilesystemChange::Created {
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
            parent_inode_id: InodeId(1),
            display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            revision_no: Some(crate::RevisionNo(1)),
            content_ref: Some(sample_content_ref.clone()),
        };
        assert_eq!(
            serde_json::to_string(&created_file).expect("serialize created file event"),
            format!(
                r#"{{"kind":"created","inode_id":2,"inode_kind":"file","parent_inode_id":1,"display_name":"a.txt","revision_no":1,"content_ref":{sample_content_ref_json}}}"#
            )
        );

        let content_changed = FilesystemChange::ContentChanged {
            inode_id: InodeId(2),
            revision_no: crate::RevisionNo(3),
            content_ref: sample_content_ref,
        };
        assert_eq!(
            serde_json::to_string(&content_changed).expect("serialize content changed event"),
            format!(
                r#"{{"kind":"content_changed","inode_id":2,"revision_no":3,"content_ref":{sample_content_ref_json}}}"#
            )
        );

        let moved = FilesystemChange::Moved {
            inode_id: InodeId(2),
            from_parent_inode_id: InodeId(1),
            from_display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            to_parent_inode_id: InodeId(3),
            to_display_name: crate::DisplayName::parse("b.txt").expect("valid display name"),
        };
        assert_eq!(
            serde_json::to_string(&moved).expect("serialize moved event"),
            r#"{"kind":"moved","inode_id":2,"from_parent_inode_id":1,"from_display_name":"a.txt","to_parent_inode_id":3,"to_display_name":"b.txt"}"#
        );

        let deleted = FilesystemChange::Deleted {
            inode_id: InodeId(2),
            deleted_direntry: Some(crate::manifest::DeletedDirentry {
                parent_inode_id: InodeId(1),
                name_key: crate::NameKey::parse("a.txt").expect("valid name key"),
                display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
            }),
        };
        assert_eq!(
            serde_json::to_string(&deleted).expect("serialize deleted event"),
            r#"{"kind":"deleted","inode_id":2,"deleted_direntry":{"parent_inode_id":1,"name_key":"a.txt","display_name":"a.txt"}}"#
        );

        let undeleted = FilesystemChange::Undeleted {
            inode_id: InodeId(2),
            parent_inode_id: InodeId(1),
            display_name: crate::DisplayName::parse("a.txt").expect("valid display name"),
        };
        assert_eq!(
            serde_json::to_string(&undeleted).expect("serialize undeleted event"),
            r#"{"kind":"undeleted","inode_id":2,"parent_inode_id":1,"display_name":"a.txt"}"#
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
            r#"{"kind":"attributes_changed","inode_id":2,"attributes_revision_no":4,"attributes":{"owner":"ada"}}"#
        );

        // A clear is a real event carrying the empty map, not an absence.
        let cleared = FilesystemChange::AttributesChanged {
            inode_id: InodeId(2),
            attributes_revision_no: crate::AttributeRevisionNo(5),
            attributes: crate::Attributes::default(),
        };
        assert_eq!(
            serde_json::to_string(&cleared).expect("serialize cleared attributes event"),
            r#"{"kind":"attributes_changed","inode_id":2,"attributes_revision_no":5,"attributes":{}}"#
        );
    }
}
