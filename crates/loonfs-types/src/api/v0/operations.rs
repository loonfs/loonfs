//! Filesystem operations for the v0 HTTP API.

use super::{DestinationBehavior, DestinationPrecondition};
use crate::options::{
    AccessState, AppendFileByInodeOptions, AppendFileOptions, AttributeChanges, CopyOptions,
    CreateDirectoryOptions, DeleteByInodeOptions, DeleteOptions, MoveOptions, PutFileOptions,
    UndeleteDestination, UpdateAccessByInodeOptions, UpdateAccessOptions,
    UpdateAttributesByInodeOptions, UpdateAttributesOptions,
};
use crate::{
    AbsolutePath, AccessGrants, AccessRevisionNo, AttributeKey, AttributeValue,
    AttributesRevisionNo, BindingVersion, ChangeSeq, ContentRef, DisplayName, InodeId, RevisionNo,
};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

/// Whether a delete can remove a non-empty directory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DeleteDirectoryBehavior {
    /// A delete that fails if the target is a non-empty directory.
    #[default]
    NonRecursive,
    /// A delete that removes the whole directory subtree.
    Recursive,
}

/// The content that a file write attaches: uploaded content or the file's bytes.
///
/// On the wire, the operation includes exactly one of `content_ref` and `inline_content`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileContent {
    /// Uploaded content, which a content token must cover.
    Ref(ContentRef),
    /// The complete file bytes.
    Inline(Vec<u8>),
}

/// The wire fields of [`FileContent`], which a write includes beside its other fields.
#[derive(Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
struct FileContentFields {
    /// The uploaded content, which a content token must cover. Set this field or
    /// `inline_content`, but not both.
    #[serde(default)]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    content_ref: Option<ContentRef>,
    /// The complete file bytes as base64. Set this field or `content_ref`, but not both.
    #[serde(default, deserialize_with = "crate::base64_bytes::option::deserialize")]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = Byte, nullable = false))]
    inline_content: Option<Vec<u8>>,
}

impl Serialize for FileContent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut fields = serializer.serialize_struct("FileContent", 1)?;
        match self {
            Self::Ref(content_ref) => fields.serialize_field("content_ref", content_ref)?,
            Self::Inline(bytes) => {
                fields.serialize_field("inline_content", &crate::base64_bytes::encode(bytes))?
            }
        }
        fields.end()
    }
}

impl<'de> Deserialize<'de> for FileContent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fields = FileContentFields::deserialize(deserializer)?;
        match (fields.content_ref, fields.inline_content) {
            (Some(content_ref), None) => Ok(Self::Ref(content_ref)),
            (None, Some(bytes)) => Ok(Self::Inline(bytes)),
            _ => Err(serde::de::Error::custom(
                "exactly one of `content_ref` and `inline_content` is required",
            )),
        }
    }
}

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for FileContent {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        <FileContentFields as utoipa::PartialSchema>::schema()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for FileContent {
    fn schemas(
        schemas: &mut Vec<(
            String,
            utoipa::openapi::RefOr<utoipa::openapi::schema::Schema>,
        )>,
    ) {
        <FileContentFields as utoipa::ToSchema>::schemas(schemas);
    }
}

/// One filesystem operation.
///
/// The server rejects unknown fields, and a variant without fields requires an empty object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FilesystemOperation {
    /// An operation that creates one directory.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationCreateDirectory")
    )]
    CreateDirectory {
        /// The absolute path of the new directory. The operation fails if the path is invalid or
        /// already bound.
        path: AbsolutePath,
        /// Whether the operation creates any missing ancestor directories. The final path component
        /// must still be new.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        parents: bool,
    },
    /// An operation that creates a directory under an existing parent inode.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationCreateDirectoryByInode")
    )]
    CreateDirectoryByInode {
        /// The parent directory.
        #[serde(with = "crate::public_inode_id")]
        parent_inode_id: InodeId,
        /// The name of the new directory.
        display_name: DisplayName,
    },
    /// An operation that creates or replaces one file from uploaded or inline content.
    ///
    /// The operation requires exactly one of `content_ref` and `inline_content`.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationPutFile"))]
    PutFile {
        /// The absolute destination path. The operation creates any missing ancestor directories.
        path: AbsolutePath,
        /// The uploaded content or the complete file bytes.
        #[serde(flatten)]
        content: FileContent,
        /// Whether the operation adds a new revision to an existing file or fails with a conflict.
        #[serde(default)]
        behavior: DestinationBehavior,
        /// The inode that the path must contain. It applies only with `replace` behavior.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
        /// The content revision that must be the file's current revision. It applies only with
        /// `replace` behavior and requires `expected_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_revision_no: Option<RevisionNo>,
    },
    /// An operation that creates a file with an unused name under an existing parent inode.
    ///
    /// The operation requires exactly one of `content_ref` and `inline_content`.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationCreateFileByInode")
    )]
    CreateFileByInode {
        /// The parent directory.
        #[serde(with = "crate::public_inode_id")]
        parent_inode_id: InodeId,
        /// The name of the new file.
        display_name: DisplayName,
        /// The uploaded content or the complete file bytes.
        #[serde(flatten)]
        content: FileContent,
    },
    /// An operation that adds a revision to a file inode if the inode's current revision is
    /// `expected_revision_no`.
    ///
    /// The operation requires exactly one of `content_ref` and `inline_content`.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationPutFileRevisionByInode")
    )]
    PutFileRevisionByInode {
        /// The file to update.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The uploaded content or the complete file bytes.
        #[serde(flatten)]
        content: FileContent,
        /// The revision that must be the file's current revision for the write to succeed.
        expected_revision_no: RevisionNo,
    },
    /// An operation that adds bytes to the end of a file as its next revision.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationAppendFile"))]
    AppendFile {
        /// The absolute path, which must resolve to a visible file.
        path: AbsolutePath,
        /// The bytes to add, as base64, from 1 byte to 256 KiB.
        #[serde(with = "crate::base64_bytes")]
        #[cfg_attr(feature = "openapi", schema(value_type = String, format = Byte))]
        inline_content: Vec<u8>,
        /// The inode that the path must still resolve to.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
        /// The content revision that must still be current. It requires `expected_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_revision_no: Option<RevisionNo>,
    },
    /// An operation that adds bytes to the end of a file inode as its next revision.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationAppendFileByInode")
    )]
    AppendFileByInode {
        /// The file to append to.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The bytes to add, as base64, from 1 byte to 256 KiB.
        #[serde(with = "crate::base64_bytes")]
        #[cfg_attr(feature = "openapi", schema(value_type = String, format = Byte))]
        inline_content: Vec<u8>,
        /// The content revision that must still be current.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_revision_no: Option<RevisionNo>,
    },
    /// An operation that deletes the file or directory at one path.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationDeletePath"))]
    DeletePath {
        /// The absolute path, which must resolve to a visible inode.
        path: AbsolutePath,
        /// Whether the operation can recursively delete a non-empty directory.
        #[serde(default)]
        behavior: DeleteDirectoryBehavior,
        /// The inode that the path must still resolve to before the delete.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
    },
    /// An operation that deletes an inode if its current binding version is
    /// `expected_binding_version`.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationDeleteByInode")
    )]
    DeleteByInode {
        /// The inode to delete.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The binding version that the inode must currently have for the delete to succeed.
        expected_binding_version: BindingVersion,
        /// Whether the operation can recursively delete a non-empty directory.
        #[serde(default)]
        behavior: DeleteDirectoryBehavior,
    },
    /// An operation that moves a file or directory from one path to another.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationMovePath"))]
    MovePath {
        /// The absolute source path, which must resolve to a visible inode.
        source_path: AbsolutePath,
        /// The absolute destination path. The destination's parent must be visible and writable.
        destination_path: AbsolutePath,
        /// The replacement behavior and the optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
    },
    /// An operation that moves an inode if its current binding version is
    /// `expected_binding_version`.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationMoveByInode"))]
    MoveByInode {
        /// The inode to move.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The binding version that the inode must currently have for the move to succeed.
        expected_binding_version: BindingVersion,
        /// The destination directory.
        #[serde(with = "crate::public_inode_id")]
        destination_parent_inode_id: InodeId,
        /// The inode's new name.
        destination_display_name: DisplayName,
        /// The replacement behavior and the optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
    },
    /// An operation that copies a file from one path to another.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationCopyPath"))]
    CopyPath {
        /// The absolute source path, which must resolve to a visible file.
        source_path: AbsolutePath,
        /// The absolute destination path. The destination's parent must be visible and writable.
        destination_path: AbsolutePath,
        /// The replacement behavior and the optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
    },
    /// An operation that copies a file inode to a new name under a parent inode.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationCopyByInode"))]
    CopyByInode {
        /// The file to copy.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The destination directory.
        #[serde(with = "crate::public_inode_id")]
        destination_parent_inode_id: InodeId,
        /// The name of the copy.
        destination_display_name: DisplayName,
        /// The replacement behavior and the optional destination state.
        #[serde(flatten)]
        precondition: DestinationPrecondition,
    },
    /// An operation that restores the deletion that `inode_id` and `deletion_seq` identify.
    ///
    /// To choose the destination, set `destination_path`, or set `destination_parent_inode_id` and
    /// `destination_display_name`. If you set neither, the undelete uses the parent and name
    /// recorded with the deletion.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationUndelete"))]
    Undelete {
        /// The deleted inode to make reachable again.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The deletion sequence that the caller observed. It prevents the undelete from cancelling
        /// a newer deletion of the inode.
        deletion_seq: ChangeSeq,
        /// The absolute path to restore the inode to. It can't be combined with
        /// `destination_parent_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        destination_path: Option<AbsolutePath>,
        /// The directory to restore the inode into. It requires `destination_display_name`.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        destination_parent_inode_id: Option<InodeId>,
        /// The inode's name in `destination_parent_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        destination_display_name: Option<DisplayName>,
    },
    /// An operation that restores an older revision as the current revision of the file at a path.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationRestoreRevision")
    )]
    RestoreRevision {
        /// The absolute path, which must resolve to a visible file.
        path: AbsolutePath,
        /// An existing older revision. The operation copies its content into a new current
        /// revision.
        source_revision_no: RevisionNo,
    },
    /// An operation that restores an older revision as the current revision of a file inode.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationRestoreRevisionByInode")
    )]
    RestoreRevisionByInode {
        /// The visible file to restore.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// An existing older revision. The operation copies its content into a new current
        /// revision.
        source_revision_no: RevisionNo,
    },
    /// An operation that writes and removes attributes on the inode that a path resolves to.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationUpdateAttributes")
    )]
    UpdateAttributes {
        /// The absolute path, which must resolve to a visible file or directory.
        path: AbsolutePath,
        /// The attributes to write. A new value replaces the current value of the same key, and
        /// other keys don't change.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        set: BTreeMap<AttributeKey, AttributeValue>,
        /// The attribute keys to remove. The server rejects a list that repeats a key.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        remove: Vec<AttributeKey>,
        /// The inode that the path must still resolve to before the update.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
        /// The attribute revision that must still be current. It requires `expected_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_attributes_revision_no: Option<AttributesRevisionNo>,
    },
    /// An operation that writes and removes attributes on a visible inode.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationUpdateAttributesByInode")
    )]
    UpdateAttributesByInode {
        /// The visible file or directory to update.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// The attributes to write. A new value replaces the current value of the same key, and
        /// other keys don't change.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        set: BTreeMap<AttributeKey, AttributeValue>,
        /// The attribute keys to remove. The server rejects a list that repeats a key.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        remove: Vec<AttributeKey>,
        /// The attribute revision that must still be current.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_attributes_revision_no: Option<AttributesRevisionNo>,
    },
    /// An operation that replaces the access row of the inode that a path resolves to.
    ///
    /// The root path is a valid target.
    #[cfg_attr(feature = "openapi", schema(title = "FilesystemOperationUpdateAccess"))]
    UpdateAccess {
        /// The absolute path, which must resolve to a visible file or directory.
        path: AbsolutePath,
        /// Whether the directory stops inheriting grants from its ancestors.
        boundary: bool,
        /// Every direct grant that the inode has after this update.
        grants: AccessGrants,
        /// The inode that the path must still resolve to before the update.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_inode_id: Option<InodeId>,
        /// The access revision that must still be current. It requires `expected_inode_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_access_revision_no: Option<AccessRevisionNo>,
    },
    /// An operation that replaces the access row of a visible inode.
    ///
    /// The root inode is a valid target.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "FilesystemOperationUpdateAccessByInode")
    )]
    UpdateAccessByInode {
        /// The visible file or directory to update.
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
        /// Whether the directory stops inheriting grants from its ancestors.
        boundary: bool,
        /// Every direct grant that the inode has after this update.
        grants: AccessGrants,
        /// The access revision that must still be current.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        expected_access_revision_no: Option<AccessRevisionNo>,
    },
}

impl FilesystemOperation {
    /// Builds a creation of the directory at `path`.
    pub fn create_directory(path: AbsolutePath, options: &CreateDirectoryOptions) -> Self {
        Self::CreateDirectory {
            path,
            parents: options.parents,
        }
    }

    /// Builds a creation of the directory `display_name` under `parent_inode_id`.
    pub fn create_directory_by_inode(parent_inode_id: InodeId, display_name: DisplayName) -> Self {
        Self::CreateDirectoryByInode {
            parent_inode_id,
            display_name,
        }
    }

    /// Builds a write of `content` to the file at `path`.
    pub fn put_file(path: AbsolutePath, content: FileContent, options: &PutFileOptions) -> Self {
        Self::PutFile {
            path,
            content,
            behavior: options.behavior,
            expected_inode_id: options.expected_inode_id,
            expected_revision_no: options.expected_revision_no,
        }
    }

    /// Builds a creation of the file `display_name` under `parent_inode_id`.
    pub fn create_file_by_inode(
        parent_inode_id: InodeId,
        display_name: DisplayName,
        content: FileContent,
    ) -> Self {
        Self::CreateFileByInode {
            parent_inode_id,
            display_name,
            content,
        }
    }

    /// Builds a write of `content` as the next revision of `inode_id`.
    pub fn put_file_revision_by_inode(
        inode_id: InodeId,
        content: FileContent,
        expected_revision_no: RevisionNo,
    ) -> Self {
        Self::PutFileRevisionByInode {
            inode_id,
            content,
            expected_revision_no,
        }
    }

    /// Builds an append of `bytes` to the file at `path`.
    pub fn append_file(path: AbsolutePath, bytes: Vec<u8>, options: &AppendFileOptions) -> Self {
        Self::AppendFile {
            path,
            inline_content: bytes,
            expected_inode_id: options.expected_inode_id,
            expected_revision_no: options.expected_revision_no,
        }
    }

    /// Builds an append of `bytes` to the file `inode_id`.
    pub fn append_file_by_inode(
        inode_id: InodeId,
        bytes: Vec<u8>,
        options: &AppendFileByInodeOptions,
    ) -> Self {
        Self::AppendFileByInode {
            inode_id,
            inline_content: bytes,
            expected_revision_no: options.expected_revision_no,
        }
    }

    /// Builds a deletion of the file or directory at `path`.
    pub fn delete_path(path: AbsolutePath, options: &DeleteOptions) -> Self {
        Self::DeletePath {
            path,
            behavior: options.behavior,
            expected_inode_id: options.expected_inode_id,
        }
    }

    /// Builds a deletion of `inode_id` at `expected_binding_version`.
    pub fn delete_by_inode(
        inode_id: InodeId,
        expected_binding_version: BindingVersion,
        options: &DeleteByInodeOptions,
    ) -> Self {
        Self::DeleteByInode {
            inode_id,
            expected_binding_version,
            behavior: options.behavior,
        }
    }

    /// Builds a move from `source_path` to `destination_path`.
    pub fn move_path(
        source_path: AbsolutePath,
        destination_path: AbsolutePath,
        options: &MoveOptions,
    ) -> Self {
        Self::MovePath {
            source_path,
            destination_path,
            precondition: options.destination_precondition(),
        }
    }

    /// Builds a move of `inode_id` at `expected_binding_version` to
    /// `destination_display_name` under `destination_parent_inode_id`.
    pub fn move_by_inode(
        inode_id: InodeId,
        expected_binding_version: BindingVersion,
        destination_parent_inode_id: InodeId,
        destination_display_name: DisplayName,
        options: &MoveOptions,
    ) -> Self {
        Self::MoveByInode {
            inode_id,
            expected_binding_version,
            destination_parent_inode_id,
            destination_display_name,
            precondition: options.destination_precondition(),
        }
    }

    /// Builds a copy from `source_path` to `destination_path`.
    pub fn copy_path(
        source_path: AbsolutePath,
        destination_path: AbsolutePath,
        options: &CopyOptions,
    ) -> Self {
        Self::CopyPath {
            source_path,
            destination_path,
            precondition: options.destination_precondition(),
        }
    }

    /// Builds a copy of `inode_id` to `destination_display_name` under
    /// `destination_parent_inode_id`.
    pub fn copy_by_inode(
        inode_id: InodeId,
        destination_parent_inode_id: InodeId,
        destination_display_name: DisplayName,
        options: &CopyOptions,
    ) -> Self {
        Self::CopyByInode {
            inode_id,
            destination_parent_inode_id,
            destination_display_name,
            precondition: options.destination_precondition(),
        }
    }

    /// Builds a restore of `source_revision_no` as the current revision of
    /// the file at `path`.
    pub fn restore_revision(path: AbsolutePath, source_revision_no: RevisionNo) -> Self {
        Self::RestoreRevision {
            path,
            source_revision_no,
        }
    }

    /// Builds a restore of `source_revision_no` as the current revision of
    /// `inode_id`.
    pub fn restore_revision_by_inode(inode_id: InodeId, source_revision_no: RevisionNo) -> Self {
        Self::RestoreRevisionByInode {
            inode_id,
            source_revision_no,
        }
    }

    /// Builds an attribute update of the inode at `path`.
    pub fn update_attributes(
        path: AbsolutePath,
        changes: AttributeChanges,
        options: &UpdateAttributesOptions,
    ) -> Self {
        Self::UpdateAttributes {
            path,
            set: changes.set,
            remove: changes.remove,
            expected_inode_id: options.expected_inode_id,
            expected_attributes_revision_no: options.expected_attributes_revision_no,
        }
    }

    /// Builds an attribute update of `inode_id`.
    pub fn update_attributes_by_inode(
        inode_id: InodeId,
        changes: AttributeChanges,
        options: &UpdateAttributesByInodeOptions,
    ) -> Self {
        Self::UpdateAttributesByInode {
            inode_id,
            set: changes.set,
            remove: changes.remove,
            expected_attributes_revision_no: options.expected_attributes_revision_no,
        }
    }

    /// Builds a replacement of the access row of the inode at `path`.
    pub fn update_access(
        path: AbsolutePath,
        access: AccessState,
        options: &UpdateAccessOptions,
    ) -> Self {
        Self::UpdateAccess {
            path,
            boundary: access.boundary,
            grants: access.grants,
            expected_inode_id: options.expected_inode_id,
            expected_access_revision_no: options.expected_access_revision_no,
        }
    }

    /// Builds a replacement of the access row of `inode_id`.
    pub fn update_access_by_inode(
        inode_id: InodeId,
        access: AccessState,
        options: &UpdateAccessByInodeOptions,
    ) -> Self {
        Self::UpdateAccessByInode {
            inode_id,
            boundary: access.boundary,
            grants: access.grants,
            expected_access_revision_no: options.expected_access_revision_no,
        }
    }

    /// Builds an undelete of the deletion of `inode_id` committed at
    /// `deletion_seq`, bound where `destination` says.
    pub fn undelete(
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        destination: &UndeleteDestination,
    ) -> Self {
        let (destination_path, destination_parent_inode_id, destination_display_name) =
            match destination {
                UndeleteDestination::Recorded => (None, None, None),
                UndeleteDestination::Path(path) => (Some(path.clone()), None, None),
                UndeleteDestination::Name {
                    parent_inode_id,
                    display_name,
                } => (None, Some(*parent_inode_id), Some(display_name.clone())),
            };
        Self::Undelete {
            inode_id,
            deletion_seq,
            destination_path,
            destination_parent_inode_id,
            destination_display_name,
        }
    }

    /// Returns the uploaded content written by this operation, if any.
    pub const fn content_ref(&self) -> Option<&ContentRef> {
        match self {
            Self::PutFile { content, .. }
            | Self::CreateFileByInode { content, .. }
            | Self::PutFileRevisionByInode { content, .. } => match content {
                FileContent::Ref(content_ref) => Some(content_ref),
                FileContent::Inline(_) => None,
            },
            Self::CreateDirectory { .. }
            | Self::CreateDirectoryByInode { .. }
            | Self::AppendFile { .. }
            | Self::AppendFileByInode { .. }
            | Self::DeletePath { .. }
            | Self::DeleteByInode { .. }
            | Self::MovePath { .. }
            | Self::MoveByInode { .. }
            | Self::CopyPath { .. }
            | Self::CopyByInode { .. }
            | Self::Undelete { .. }
            | Self::RestoreRevision { .. }
            | Self::RestoreRevisionByInode { .. }
            | Self::UpdateAttributes { .. }
            | Self::UpdateAttributesByInode { .. }
            | Self::UpdateAccess { .. }
            | Self::UpdateAccessByInode { .. } => None,
        }
    }

    /// Returns the bytes this operation adds to the end of a file, if any.
    pub fn appended_content(&self) -> Option<&[u8]> {
        match self {
            Self::AppendFile { inline_content, .. }
            | Self::AppendFileByInode { inline_content, .. } => Some(inline_content),
            Self::CreateDirectory { .. }
            | Self::CreateDirectoryByInode { .. }
            | Self::PutFile { .. }
            | Self::CreateFileByInode { .. }
            | Self::PutFileRevisionByInode { .. }
            | Self::DeletePath { .. }
            | Self::DeleteByInode { .. }
            | Self::MovePath { .. }
            | Self::MoveByInode { .. }
            | Self::CopyPath { .. }
            | Self::CopyByInode { .. }
            | Self::Undelete { .. }
            | Self::RestoreRevision { .. }
            | Self::RestoreRevisionByInode { .. }
            | Self::UpdateAttributes { .. }
            | Self::UpdateAttributesByInode { .. }
            | Self::UpdateAccess { .. }
            | Self::UpdateAccessByInode { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CommitPrecondition, ContentId};
    use std::assert_matches;

    fn path(value: &str) -> AbsolutePath {
        AbsolutePath::parse(value).expect("valid test path")
    }

    fn attribute_key(value: &str) -> AttributeKey {
        AttributeKey::parse(value).expect("valid test attribute key")
    }

    #[test]
    fn behavior_enums_use_snake_case_wire_values() {
        assert_eq!(
            DestinationBehavior::default(),
            DestinationBehavior::NoReplace
        );
        assert_eq!(
            DeleteDirectoryBehavior::default(),
            DeleteDirectoryBehavior::NonRecursive
        );
        assert_eq!(
            serde_json::to_value(DestinationBehavior::NoReplace)
                .expect("destination behavior json"),
            serde_json::json!("no_replace")
        );
        assert_eq!(
            serde_json::to_value(DestinationBehavior::Replace).expect("destination behavior json"),
            serde_json::json!("replace")
        );
        assert_eq!(
            serde_json::to_value(DeleteDirectoryBehavior::NonRecursive)
                .expect("delete behavior json"),
            serde_json::json!("non_recursive")
        );
        assert_eq!(
            serde_json::to_value(DeleteDirectoryBehavior::Recursive).expect("delete behavior json"),
            serde_json::json!("recursive")
        );
    }

    #[test]
    fn filesystem_delete_and_move_operations_use_behavior_field() {
        let create_directory = FilesystemOperation::CreateDirectory {
            path: path("/docs"),
            parents: false,
        };
        assert_eq!(
            serde_json::to_value(&create_directory).expect("create directory op json"),
            serde_json::json!({
                "kind": "create_directory",
                "path": "/docs"
            })
        );

        let create_directory_with_parents = FilesystemOperation::CreateDirectory {
            path: path("/docs/notes"),
            parents: true,
        };
        assert_eq!(
            serde_json::to_value(&create_directory_with_parents)
                .expect("create directory with parents op json"),
            serde_json::json!({
                "kind": "create_directory",
                "path": "/docs/notes",
                "parents": true
            })
        );

        let delete = FilesystemOperation::DeletePath {
            path: path("/docs"),
            behavior: DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        };
        assert_eq!(
            serde_json::to_value(&delete).expect("delete op json"),
            serde_json::json!({
                "kind": "delete_path",
                "path": "/docs",
                "behavior": "recursive"
            })
        );

        let move_path = FilesystemOperation::MovePath {
            source_path: path("/docs/a.txt"),
            destination_path: path("/docs/b.txt"),
            precondition: crate::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(InodeId(7)),
                expected_revision_no: Some(RevisionNo(3)),
            },
        };
        assert_eq!(
            serde_json::to_value(&move_path).expect("move op json"),
            serde_json::json!({
                "kind": "move_path",
                "source_path": "/docs/a.txt",
                "destination_path": "/docs/b.txt",
                "behavior": "replace",
                "expected_destination_inode_id": "ino_00000000000000000000000007",
                "expected_destination_revision_no": 3
            })
        );

        let copy_path = FilesystemOperation::CopyPath {
            source_path: path("/docs/a.txt"),
            destination_path: path("/docs/b.txt"),
            precondition: crate::DestinationPrecondition {
                behavior: DestinationBehavior::Replace,
                expected_inode_id: Some(InodeId(7)),
                expected_revision_no: Some(RevisionNo(3)),
            },
        };
        assert_eq!(
            serde_json::to_value(&copy_path).expect("copy op json"),
            serde_json::json!({
                "kind": "copy_path",
                "source_path": "/docs/a.txt",
                "destination_path": "/docs/b.txt",
                "behavior": "replace",
                "expected_destination_inode_id": "ino_00000000000000000000000007",
                "expected_destination_revision_no": 3
            })
        );

        let update_attributes = FilesystemOperation::UpdateAttributes {
            path: path("/docs/a.txt"),
            set: BTreeMap::from([(
                attribute_key("owner"),
                AttributeValue::parse("ada").expect("valid attribute value"),
            )]),
            remove: vec![attribute_key("draft")],
            expected_inode_id: Some(InodeId(7)),
            expected_attributes_revision_no: Some(AttributesRevisionNo(3)),
        };
        assert_eq!(
            serde_json::to_value(&update_attributes).expect("update attributes op json"),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "set": {"owner": "ada"},
                "remove": ["draft"],
                "expected_inode_id": "ino_00000000000000000000000007",
                "expected_attributes_revision_no": 3
            })
        );
    }

    #[test]
    fn update_attributes_omits_empty_collections_and_absent_preconditions() {
        let set_only = FilesystemOperation::UpdateAttributes {
            path: path("/docs/a.txt"),
            set: BTreeMap::from([(
                attribute_key("owner"),
                AttributeValue::parse("ada,grace").expect("valid attribute value"),
            )]),
            remove: Vec::new(),
            expected_inode_id: None,
            expected_attributes_revision_no: None,
        };
        assert_eq!(
            serde_json::to_value(&set_only).expect("set-only op json"),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "set": {"owner": "ada,grace"}
            })
        );

        let decoded: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "update_attributes",
            "path": "/docs/a.txt",
            "remove": ["draft"]
        }))
        .expect("remove-only op defaults the set map and both preconditions");
        assert_eq!(
            decoded,
            FilesystemOperation::UpdateAttributes {
                path: path("/docs/a.txt"),
                set: BTreeMap::new(),
                remove: vec![attribute_key("draft")],
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            }
        );
    }

    #[test]
    fn update_attributes_validates_keys_and_values_during_deserialization() {
        // The key grammar and the value shape are enforced on the way in, so
        // a malformed update never reaches planning.
        for encoded in [
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "set": {"": "ada"}
            }),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "set": {"owner": {"kind": "string", "value": "ada"}}
            }),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "remove": ["a\u{0}b"]
            }),
        ] {
            serde_json::from_value::<FilesystemOperation>(encoded)
                .expect_err("an invalid attribute key or value should be rejected");
        }
    }

    #[test]
    fn filesystem_operations_default_omitted_behavior_fields() {
        let put: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "put_file",
            "path": "/docs/a.txt",
            "content_ref": {
                "kind": "blob_v1",
                "owner_namespace_id": "demo",
                "content_id": "con_014d2pf2dbsqqg28t5cy4tqkff",
                "size_bytes": 1,
                "checksum": {
                    "algorithm": "crc64nvme",
                    "value": "aaaaaaaaaaaaaaaa"
                }
            }
        }))
        .expect("put op defaults behavior");
        assert_matches!(
            put,
            FilesystemOperation::PutFile {
                behavior: DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
                ..
            }
        );

        let delete: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "delete_path",
            "path": "/docs"
        }))
        .expect("delete op defaults behavior");
        assert_eq!(
            delete,
            FilesystemOperation::DeletePath {
                path: path("/docs"),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: None,
            }
        );

        let move_path: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "move_path",
            "source_path": "/docs/a.txt",
            "destination_path": "/docs/b.txt"
        }))
        .expect("move op defaults behavior");
        assert_eq!(
            move_path,
            FilesystemOperation::MovePath {
                source_path: path("/docs/a.txt"),
                destination_path: path("/docs/b.txt"),
                precondition: crate::DestinationPrecondition {
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            }
        );

        let copy_path: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "copy_path",
            "source_path": "/docs/a.txt",
            "destination_path": "/docs/b.txt"
        }))
        .expect("copy op defaults behavior");
        assert_eq!(
            copy_path,
            FilesystemOperation::CopyPath {
                source_path: path("/docs/a.txt"),
                destination_path: path("/docs/b.txt"),
                precondition: crate::DestinationPrecondition {
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            }
        );

        let move_by_inode: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "move_by_inode",
            "inode_id": "ino_00000000000000000000000007",
            "expected_binding_version": "aaaa",
            "destination_parent_inode_id": "ino_00000000000000000000000000",
            "destination_display_name": "b.txt"
        }))
        .expect("inode move defaults behavior");
        assert_eq!(
            move_by_inode,
            FilesystemOperation::MoveByInode {
                inode_id: InodeId(7),
                expected_binding_version: BindingVersion::parse("aaaa").expect("binding version"),
                destination_parent_inode_id: crate::ROOT_INODE_ID,
                destination_display_name: DisplayName::parse("b.txt").expect("display name"),
                precondition: DestinationPrecondition::default(),
            }
        );
    }

    #[test]
    fn filesystem_operation_paths_keep_the_plain_string_wire_shape() {
        let content_ref = ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::generate(),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        );
        let cases = [
            (
                FilesystemOperation::PutFile {
                    path: path("/docs/a.txt"),
                    content: FileContent::Ref(content_ref.clone()),
                    behavior: DestinationBehavior::NoReplace,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
                serde_json::json!({
                    "kind": "put_file",
                    "path": "/docs/a.txt",
                    "content_ref": content_ref,
                    "behavior": "no_replace"
                }),
            ),
            (
                FilesystemOperation::Undelete {
                    inode_id: InodeId(7),
                    deletion_seq: ChangeSeq(8),
                    destination_path: Some(path("/docs/restored")),
                    destination_parent_inode_id: None,
                    destination_display_name: None,
                },
                serde_json::json!({
                    "kind": "undelete",
                    "inode_id": "ino_00000000000000000000000007",
                    "deletion_seq": 8,
                    "destination_path": "/docs/restored"
                }),
            ),
            (
                FilesystemOperation::RestoreRevision {
                    path: path("/docs/a.txt"),
                    source_revision_no: RevisionNo(2),
                },
                serde_json::json!({
                    "kind": "restore_revision",
                    "path": "/docs/a.txt",
                    "source_revision_no": 2
                }),
            ),
            (
                FilesystemOperation::UpdateAttributes {
                    path: path("/docs/a.txt"),
                    set: BTreeMap::new(),
                    remove: vec![attribute_key("draft")],
                    expected_inode_id: None,
                    expected_attributes_revision_no: None,
                },
                serde_json::json!({
                    "kind": "update_attributes",
                    "path": "/docs/a.txt",
                    "remove": ["draft"]
                }),
            ),
        ];

        for (operation, string_shaped_json) in cases {
            assert_eq!(
                serde_json::to_value(operation).expect("serialize filesystem operation"),
                string_shaped_json
            );
        }
    }

    #[test]
    fn appends_carry_their_bytes_as_required_base64() {
        let cases = [
            (
                FilesystemOperation::AppendFile {
                    path: path("/logs/app.log"),
                    inline_content: b"hi".to_vec(),
                    expected_inode_id: Some(InodeId(7)),
                    expected_revision_no: Some(RevisionNo(3)),
                },
                serde_json::json!({
                    "kind": "append_file",
                    "path": "/logs/app.log",
                    "inline_content": "aGk=",
                    "expected_inode_id": "ino_00000000000000000000000007",
                    "expected_revision_no": 3
                }),
            ),
            (
                FilesystemOperation::AppendFileByInode {
                    inode_id: InodeId(7),
                    inline_content: b"hi".to_vec(),
                    expected_revision_no: None,
                },
                serde_json::json!({
                    "kind": "append_file_by_inode",
                    "inode_id": "ino_00000000000000000000000007",
                    "inline_content": "aGk="
                }),
            ),
        ];
        for (operation, encoded) in cases {
            assert_eq!(
                serde_json::to_value(&operation).expect("serialize append"),
                encoded
            );
            assert_eq!(
                serde_json::from_value::<FilesystemOperation>(encoded.clone())
                    .expect("deserialize append"),
                operation
            );
            let mut missing = encoded;
            missing
                .as_object_mut()
                .expect("an operation is an object")
                .remove("inline_content");
            serde_json::from_value::<FilesystemOperation>(missing)
                .expect_err("an append without its bytes should be rejected");
        }
    }

    #[test]
    fn filesystem_operation_paths_validate_during_deserialization() {
        for encoded in [
            serde_json::json!({"kind": "create_directory", "path": "relative", "parents": false}),
            serde_json::json!({
                "kind": "put_file",
                "path": "relative",
                "content_ref": ContentRef::blob_v1(crate::NamespaceId::parse("demo").expect("namespace id"), ContentId::generate(), b"hello", crate::ChecksumAlgorithm::Crc64nvme)
            }),
            serde_json::json!({"kind": "delete_path", "path": "relative"}),
            serde_json::json!({
                "kind": "move_path",
                "source_path": "relative",
                "destination_path": "/target"
            }),
            serde_json::json!({
                "kind": "copy_path",
                "source_path": "/source",
                "destination_path": "relative"
            }),
            serde_json::json!({
                "kind": "undelete",
                "inode_id": "ino_00000000000000000000000007",
                "deletion_seq": 8,
                "destination_path": "relative"
            }),
            serde_json::json!({
                "kind": "restore_revision",
                "path": "relative",
                "source_revision_no": 2
            }),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "relative",
                "remove": ["draft"]
            }),
        ] {
            serde_json::from_value::<FilesystemOperation>(encoded)
                .expect_err("an invalid path should be rejected");
        }
    }

    #[test]
    fn inode_request_fields_accept_only_the_public_format() {
        let operations = [
            serde_json::json!({
                "kind": "delete_path",
                "path": "/docs/a.txt",
                "expected_inode_id": "ino_0000000000000000000000000v"
            }),
            serde_json::json!({
                "kind": "undelete",
                "inode_id": "ino_0000000000000000000000000v",
                "deletion_seq": 8,
                "destination_path": "/docs/restored"
            }),
            serde_json::json!({
                "kind": "update_attributes",
                "path": "/docs/a.txt",
                "expected_inode_id": "ino_0000000000000000000000000v"
            }),
        ];

        for operation in operations {
            serde_json::from_value::<FilesystemOperation>(operation.clone())
                .expect("valid public inode ID");

            let inode_key = if operation["kind"] == "undelete" {
                "inode_id"
            } else {
                "expected_inode_id"
            };
            for invalid in [serde_json::json!(27), serde_json::json!("27")] {
                let mut invalid_operation = operation.clone();
                invalid_operation[inode_key] = invalid;
                serde_json::from_value::<FilesystemOperation>(invalid_operation)
                    .expect_err(&format!("{inode_key} accepted an invalid inode ID"));
            }
        }
    }

    #[test]
    fn update_access_round_trips_and_requires_boundary_and_grants() {
        let operation = FilesystemOperation::UpdateAccess {
            path: AbsolutePath::parse("/docs/secret").expect("path"),
            boundary: true,
            grants: serde_json::from_value(serde_json::json!({"prn_ada": ["read", "write"]}))
                .expect("grants"),
            expected_inode_id: Some(InodeId(9)),
            expected_access_revision_no: Some(AccessRevisionNo(2)),
        };
        let json = serde_json::json!({
            "kind": "update_access",
            "path": "/docs/secret",
            "boundary": true,
            "grants": {"prn_ada": ["read", "write"]},
            "expected_inode_id": "ino_00000000000000000000000009",
            "expected_access_revision_no": 2
        });
        assert_eq!(serde_json::to_value(&operation).expect("serialize"), json);
        assert_eq!(
            serde_json::from_value::<FilesystemOperation>(json.clone()).expect("decode"),
            operation
        );
        for field in ["boundary", "grants"] {
            let mut missing = json.clone();
            missing.as_object_mut().expect("object").remove(field);
            serde_json::from_value::<FilesystemOperation>(missing)
                .expect_err(&format!("missing {field}"));
        }
        assert_eq!(
            serde_json::to_value(CommitPrecondition::AccessRevision {
                inode_id: InodeId(9),
                expected_access_revision_no: AccessRevisionNo(2),
            })
            .expect("serialize precondition"),
            serde_json::json!({"kind": "access_revision", "inode_id": "ino_00000000000000000000000009", "expected_access_revision_no": 2})
        );
    }
}
