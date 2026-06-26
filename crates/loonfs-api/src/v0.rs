use crate::{
    ChangeSeq, CommitId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamePolicy,
    NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub type CommitAnnotations = BTreeMap<String, Value>;

/// Move behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum MoveBehavior {
    /// Move only if the destination name is absent.
    #[default]
    NoReplace,
    /// Reserved for a future version.
    Replace,
    /// Reserved for a future version.
    Exchange,
}

/// Upload transport mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum UploadMode {
    /// The service receives bytes and writes content to object storage.
    #[default]
    ServiceProxied,
    /// The service mints a short-lived presigned PUT URL for the content object.
    DirectPut,
}

impl UploadMode {
    pub fn is_service_proxied(&self) -> bool {
        matches!(self, Self::ServiceProxied)
    }
}

/// Request for starting an upload session.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BeginUploadRequest {
    /// Requested upload transport. Absent keeps the existing service-proxied path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<UploadMode>,
    /// Required for `direct_put`; the server signs exactly this content object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_ref: Option<ContentRef>,
}

/// Client-facing direct transfer capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ObjectTransferAccess {
    /// Short-lived URL plus required headers for one object-store write.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "ObjectTransferAccessPresignedUrl")
    )]
    PresignedUrl {
        /// HTTP method the client must use.
        method: String,
        /// Full presigned URL.
        url: String,
        /// Headers that are covered by the signature and must be sent.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        headers: BTreeMap<String, String>,
        /// Expiration timestamp in Unix milliseconds.
        expires_at_ms: u64,
    },
}

/// Presigned direct_put upload details. The raw object key is intentionally not public.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DirectPutUpload {
    pub content_ref: ContentRef,
    pub access: ObjectTransferAccess,
}

/// Stateless proof that a LoonFS server already validated a content ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ValidatedContentToken {
    pub content_ref: ContentRef,
    /// Opaque, server-signed token. Clients must not parse it.
    pub token: String,
}

/// Response for starting an upload session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BeginUploadResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: String,
    pub mode: UploadMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_put: Option<DirectPutUpload>,
}

/// Response after uploading bytes into a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct UploadContentResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: String,
    pub content_ref: ContentRef,
}

/// Request to complete an upload with the expected content ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CompleteUploadRequest {
    pub content_ref: ContentRef,
}

/// Response after an upload session is completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CompleteUploadResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: String,
    pub content_ref: ContentRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validated_content_token: Option<String>,
}

/// Explicit semantic commit request.
///
/// Use this lower-level shape when you need one commit id, optional
/// preconditions, and multiple ordered operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommitRequest {
    /// Client idempotency key for this logical commit.
    pub commit_id: CommitId,
    /// Optional race checks evaluated before mutation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub preconditions: Vec<CommitPrecondition>,
    /// Ordered semantic operations.
    pub ops: Vec<CommitOp>,
    /// Optional human-readable note.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Optional structured metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(value_type = Object))]
    pub annotations: Option<CommitAnnotations>,
}

/// Response for a committed explicit request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommitResponse {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub committed_seq: ChangeSeq,
    pub results: Vec<CommitOpResult>,
}

/// Semantic operation inside a commit request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum CommitOp {
    /// Create a directory under a parent inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpCreateDir"))]
    CreateDir {
        parent_inode: InodeId,
        display_name: String,
    },
    /// Create a file under a parent inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpCreateFile"))]
    CreateFile {
        parent_inode: InodeId,
        display_name: String,
        content_ref: ContentRef,
    },
    /// Append a new revision to an existing file.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpReplaceFile"))]
    ReplaceFile {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    /// Restore a prior revision as a new current revision.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpRestoreRevision"))]
    RestoreRevision {
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
    },
    /// Delete a file inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpDeleteFile"))]
    DeleteFile { inode_id: InodeId },
    /// Rename or move an inode.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpRename"))]
    Rename {
        inode_id: InodeId,
        new_parent_inode: InodeId,
        new_display_name: String,
        #[serde(default)]
        behavior: MoveBehavior,
    },
    /// Delete a directory subtree.
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpDeleteSubtree"))]
    DeleteSubtree { root_inode: InodeId },
}

/// Race check evaluated before a commit is accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommitPrecondition {
    /// File inode is still at this revision.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionInodeRevisionIs")
    )]
    InodeRevisionIs {
        inode_id: InodeId,
        revision_no: RevisionNo,
    },
    /// Inode ancestors have not been subtree-deleted.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionAncestorsNotSubtreeDeleted")
    )]
    AncestorsNotSubtreeDeleted { inode_id: InodeId },
    /// Directory child name is still absent.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionChildNameAbsent")
    )]
    ChildNameAbsent {
        parent_inode: InodeId,
        name_key: NameKey,
    },
    /// Directory binding is still exactly the binding the caller saw.
    #[cfg_attr(feature = "openapi", schema(title = "CommitPreconditionBindingIs"))]
    BindingIs {
        parent_inode: InodeId,
        name_key: NameKey,
        child_inode: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    },
    /// Directory is still empty.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CommitPreconditionDirectoryEmpty")
    )]
    DirectoryEmpty { inode_id: InodeId },
}

/// Per-operation result returned after a commit succeeds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum CommitOpResult {
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpResultCreateDir"))]
    CreateDir { op_index: u32, inode_id: InodeId },
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpResultCreateFile"))]
    CreateFile {
        op_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpResultReplaceFile"))]
    ReplaceFile {
        op_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpResultRestoreRevision"))]
    RestoreRevision {
        op_index: u32,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpResultDeleteFile"))]
    DeleteFile { op_index: u32, inode_id: InodeId },
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpResultRename"))]
    Rename { op_index: u32, inode_id: InodeId },
    #[cfg_attr(feature = "openapi", schema(title = "CommitOpResultDeleteSubtree"))]
    DeleteSubtree { op_index: u32, root_inode: InodeId },
}

/// Durable metadata fact exposed through the change feed.
///
/// Most clients should use semantic operation results. Sync and projection
/// clients can apply deltas directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "delta", rename_all = "snake_case")]
pub enum CommitDelta {
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaCreateInode"))]
    CreateInode {
        semantic_op_index: u32,
        delta_index: u32,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaBindDirentry"))]
    BindDirentry {
        semantic_op_index: u32,
        delta_index: u32,
        parent_inode: InodeId,
        name_key: NameKey,
        display_name: String,
        child_inode: InodeId,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaUnbindDirentry"))]
    UnbindDirentry {
        semantic_op_index: u32,
        delta_index: u32,
        parent_inode: InodeId,
        name_key: NameKey,
        child_inode: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaAppendFileRevision"))]
    AppendFileRevision {
        semantic_op_index: u32,
        delta_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    #[cfg_attr(feature = "openapi", schema(title = "CommitDeltaTombstoneSubtree"))]
    TombstoneSubtree {
        semantic_op_index: u32,
        delta_index: u32,
        root_inode: InodeId,
    },
}

/// One committed change in namespace order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommittedChange {
    /// Namespace sequence for this logical commit.
    pub seq: ChangeSeq,
    /// Client idempotency key for this logical commit.
    pub commit_id: CommitId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(value_type = Object))]
    pub annotations: Option<CommitAnnotations>,
    /// Semantic operation results.
    pub ops: Vec<CommitOpResult>,
    /// Materialized metadata deltas.
    pub deltas: Vec<CommitDelta>,
}

/// Change-feed response after a cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ChangesResponse {
    pub namespace_id: NamespaceId,
    pub after_seq: ChangeSeq,
    pub through_seq: ChangeSeq,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after_seq: Option<ChangeSeq>,
    pub changes: Vec<CommittedChange>,
}

impl CommitPrecondition {
    pub fn child_name_absent(parent_inode: InodeId, name_key: NameKey) -> Self {
        Self::ChildNameAbsent {
            parent_inode,
            name_key,
        }
    }

    pub fn child_display_name_absent(
        parent_inode: InodeId,
        name_policy: NamePolicy,
        display_name: &DisplayName,
    ) -> Self {
        Self::child_name_absent(
            parent_inode,
            NameKey::for_display_name(name_policy, display_name),
        )
    }

    pub fn binding_is(
        parent_inode: InodeId,
        name_key: NameKey,
        child_inode: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    ) -> Self {
        Self::BindingIs {
            parent_inode,
            name_key,
            child_inode,
            bind_seq,
            bind_delta_index,
        }
    }

    pub fn display_name_binding_is(
        parent_inode: InodeId,
        name_policy: NamePolicy,
        display_name: &DisplayName,
        child_inode: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    ) -> Self {
        Self::binding_is(
            parent_inode,
            NameKey::for_display_name(name_policy, display_name),
            child_inode,
            bind_seq,
            bind_delta_index,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BeginUploadRequest, BeginUploadResponse, CommitDelta, CommitOp, CommitPrecondition,
        DirectPutUpload, MoveBehavior, ObjectTransferAccess, UploadMode,
    };
    use crate::{ChangeSeq, ContentRef, InodeId, InodeKind, NameKey, NamespaceId};
    use std::collections::BTreeMap;

    #[test]
    fn direct_put_upload_mode_serializes_as_expected() {
        assert_eq!(
            serde_json::to_string(&UploadMode::DirectPut).expect("serialize mode"),
            r#""direct_put""#
        );
    }

    #[test]
    fn begin_upload_request_keeps_service_proxied_as_default() {
        let request: BeginUploadRequest = serde_json::from_str("{}").expect("decode request");
        assert_eq!(request.mode.unwrap_or_default(), UploadMode::ServiceProxied);
    }

    #[test]
    fn direct_put_response_exposes_only_presigned_access() {
        let response = BeginUploadResponse {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            upload_id: "upl_00000000000000000000000000000001".to_owned(),
            mode: UploadMode::DirectPut,
            direct_put: Some(DirectPutUpload {
                content_ref: ContentRef::whole_file_v0(b"hello"),
                access: ObjectTransferAccess::PresignedUrl {
                    method: "PUT".to_owned(),
                    url: "https://bucket.example/object?X-Amz-Signature=abc".to_owned(),
                    headers: BTreeMap::from([
                        ("if-none-match".to_owned(), "*".to_owned()),
                        (
                            "x-provider-checksum".to_owned(),
                            "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=".to_owned(),
                        ),
                    ]),
                    expires_at_ms: 1,
                },
            }),
        };

        let json = serde_json::to_string(&response).expect("serialize response");
        assert!(json.contains(r#""kind":"presigned_url""#));
        assert!(!json.contains("object_key"));
    }

    #[test]
    fn commit_precondition_name_key_serializes_as_plain_string() {
        let precondition = CommitPrecondition::ChildNameAbsent {
            parent_inode: InodeId(1),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
        };

        assert_eq!(
            serde_json::to_string(&precondition).expect("serialize precondition"),
            r#"{"type":"child_name_absent","parent_inode":1,"name_key":"report.txt"}"#
        );
    }

    #[test]
    fn commit_delta_name_key_serializes_as_plain_string() {
        let delta = CommitDelta::BindDirentry {
            semantic_op_index: 0,
            delta_index: 1,
            parent_inode: InodeId(1),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            display_name: "Report.txt".to_owned(),
            child_inode: InodeId(2),
        };

        assert_eq!(
            serde_json::to_string(&delta).expect("serialize delta"),
            r#"{"delta":"bind_direntry","semantic_op_index":0,"delta_index":1,"parent_inode":1,"name_key":"report.txt","display_name":"Report.txt","child_inode":2}"#
        );

        let unbind = CommitDelta::UnbindDirentry {
            semantic_op_index: 0,
            delta_index: 2,
            parent_inode: InodeId(1),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            child_inode: InodeId(2),
            bind_seq: ChangeSeq(7),
            bind_delta_index: 1,
        };
        assert_eq!(
            serde_json::to_string(&unbind).expect("serialize unbind delta"),
            r#"{"delta":"unbind_direntry","semantic_op_index":0,"delta_index":2,"parent_inode":1,"name_key":"report.txt","child_inode":2,"bind_seq":7,"bind_delta_index":1}"#
        );

        let create_inode = CommitDelta::CreateInode {
            semantic_op_index: 0,
            delta_index: 0,
            inode_id: InodeId(2),
            inode_kind: InodeKind::File,
        };
        assert_eq!(
            serde_json::to_string(&create_inode).expect("serialize create inode"),
            r#"{"delta":"create_inode","semantic_op_index":0,"delta_index":0,"inode_id":2,"inode_kind":"file"}"#
        );
    }

    #[test]
    fn commit_rename_defaults_omitted_behavior_to_no_replace() {
        assert_eq!(MoveBehavior::default(), MoveBehavior::NoReplace);

        let op: CommitOp = serde_json::from_value(serde_json::json!({
            "op": "rename",
            "inode_id": 2,
            "new_parent_inode": 1,
            "new_display_name": "renamed.txt"
        }))
        .expect("rename defaults behavior");

        assert_eq!(
            op,
            CommitOp::Rename {
                inode_id: InodeId(2),
                new_parent_inode: InodeId(1),
                new_display_name: "renamed.txt".to_owned(),
                behavior: MoveBehavior::NoReplace,
            }
        );
    }
}
