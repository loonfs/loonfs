//! Filesystem mutations addressed by inode ID.

use super::*;
use crate::mutations::single_operation;
use loonfs_types::{ActorId, BindingVersion, DisplayName};

impl Client {
    /// Writes file bytes to a new name under a parent directory inode,
    /// refusing a name that is already bound.
    pub async fn create_file_by_inode(
        &self,
        namespace_id: &NamespaceId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        bytes: &[u8],
        actor: &ActorId,
    ) -> Result<Commit> {
        self.create_file_by_inode_with_options(
            namespace_id,
            parent_inode_id,
            display_name,
            bytes,
            actor,
            &CommitOptions::default(),
        )
        .await
    }

    /// Writes file bytes to a new name under a parent directory inode, under
    /// the given commit settings.
    ///
    /// Content is prepared as [`Self::put_file_with_options`] prepares it. At
    /// or under the advertised inline limit, a rerun with the same bytes and
    /// commit ID replays. Larger content uploads again, so a rerun conflicts.
    pub async fn create_file_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        bytes: &[u8],
        actor: &ActorId,
        options: &CommitOptions,
    ) -> Result<Commit> {
        let prepared_content = self.prepare_content(namespace_id, bytes).await?;
        self.commit_prepared_operation(
            namespace_id,
            prepared_content,
            actor,
            options,
            None,
            |content_ref, inline_content| FilesystemOperation::CreateFileByInode {
                parent_inode_id,
                display_name: display_name.clone(),
                content_ref,
                inline_content,
            },
        )
        .await
    }

    /// Writes file bytes as the next revision of a file inode, wherever it is
    /// bound, while `expected_revision_no` is still its current revision.
    pub async fn put_file_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        expected_revision_no: RevisionNo,
        bytes: &[u8],
        actor: &ActorId,
    ) -> Result<Commit> {
        self.put_file_by_inode_with_options(
            namespace_id,
            inode_id,
            expected_revision_no,
            bytes,
            actor,
            &CommitOptions::default(),
        )
        .await
    }

    /// Writes file bytes as the next revision of a file inode, under the
    /// given commit settings.
    ///
    /// Content is prepared as [`Self::put_file_with_options`] prepares it. At
    /// or under the advertised inline limit, a rerun with the same bytes and
    /// commit ID replays. Larger content uploads again, so a rerun conflicts.
    pub async fn put_file_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        expected_revision_no: RevisionNo,
        bytes: &[u8],
        actor: &ActorId,
        options: &CommitOptions,
    ) -> Result<Commit> {
        let prepared_content = self.prepare_content(namespace_id, bytes).await?;
        self.commit_prepared_operation(
            namespace_id,
            prepared_content,
            actor,
            options,
            None,
            |content_ref, inline_content| FilesystemOperation::PutFileRevisionByInode {
                inode_id,
                content_ref,
                inline_content,
                expected_revision_no,
            },
        )
        .await
    }

    /// Adds bytes to the end of a file inode as its next revision, wherever
    /// it is bound.
    pub async fn append_file_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        bytes: &[u8],
        actor: &ActorId,
    ) -> Result<Commit> {
        self.append_file_by_inode_with_options(
            namespace_id,
            inode_id,
            bytes,
            actor,
            &AppendFileByInodeOptions::default(),
        )
        .await
    }

    /// Adds bytes to the end of a file inode as its next revision, as
    /// [`Self::append_file_with_options`] adds them to a path.
    pub async fn append_file_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        bytes: &[u8],
        actor: &ActorId,
        options: &AppendFileByInodeOptions,
    ) -> Result<Commit> {
        self.commit(
            namespace_id,
            actor,
            &single_operation(
                &options.commit,
                FilesystemOperation::AppendFileByInode {
                    inode_id,
                    inline_content: bytes.to_vec(),
                    expected_revision_no: options.expected_revision_no,
                },
            ),
        )
        .await
    }

    /// Creates a directory at a new name under a parent directory inode.
    pub async fn create_directory_by_inode(
        &self,
        namespace_id: &NamespaceId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.create_directory_by_inode_with_options(
            namespace_id,
            parent_inode_id,
            display_name,
            actor,
            &CommitOptions::default(),
        )
        .await
    }

    /// Creates a directory at a new name under a parent directory inode,
    /// under the given commit settings.
    pub async fn create_directory_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        actor: &ActorId,
        options: &CommitOptions,
    ) -> Result<Commit> {
        self.commit(
            namespace_id,
            actor,
            &single_operation(
                options,
                FilesystemOperation::CreateDirectoryByInode {
                    parent_inode_id,
                    display_name: display_name.clone(),
                },
            ),
        )
        .await
    }

    /// Deletes a file or empty directory inode while
    /// `expected_binding_version` is still its binding.
    pub async fn delete_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.delete_by_inode_with_options(
            namespace_id,
            inode_id,
            expected_binding_version,
            actor,
            &DeleteByInodeOptions::default(),
        )
        .await
    }

    /// Deletes a file or directory inode while `expected_binding_version` is
    /// still its binding.
    pub async fn delete_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
        actor: &ActorId,
        options: &DeleteByInodeOptions,
    ) -> Result<Commit> {
        self.commit(
            namespace_id,
            actor,
            &single_operation(
                &options.commit,
                FilesystemOperation::DeleteByInode {
                    inode_id,
                    expected_binding_version: expected_binding_version.clone(),
                    behavior: options.behavior,
                },
            ),
        )
        .await
    }

    /// Writes and removes attributes on a visible inode.
    pub async fn update_attributes_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        actor: &ActorId,
        changes: AttributeChanges,
    ) -> Result<Commit> {
        self.update_attributes_by_inode_with_options(
            namespace_id,
            inode_id,
            actor,
            changes,
            &UpdateAttributesByInodeOptions::default(),
        )
        .await
    }

    /// Writes and removes attributes on a visible file or directory inode,
    /// under an optional attribute revision precondition.
    pub async fn update_attributes_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        actor: &ActorId,
        changes: AttributeChanges,
        options: &UpdateAttributesByInodeOptions,
    ) -> Result<Commit> {
        self.commit(
            namespace_id,
            actor,
            &single_operation(
                &options.commit,
                FilesystemOperation::UpdateAttributesByInode {
                    inode_id,
                    set: changes.set,
                    remove: changes.remove,
                    expected_attributes_revision_no: options.expected_attributes_revision_no,
                },
            ),
        )
        .await
    }

    /// Replaces a visible inode's access row, including the root's.
    pub async fn update_access_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        actor: &ActorId,
        access: AccessState,
    ) -> Result<Commit> {
        self.update_access_by_inode_with_options(
            namespace_id,
            inode_id,
            actor,
            access,
            &UpdateAccessByInodeOptions::default(),
        )
        .await
    }

    /// Replaces a visible inode's access row, including the root's, under an
    /// optional access revision precondition.
    pub async fn update_access_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        actor: &ActorId,
        access: AccessState,
        options: &UpdateAccessByInodeOptions,
    ) -> Result<Commit> {
        self.commit(
            namespace_id,
            actor,
            &single_operation(
                &options.commit,
                FilesystemOperation::UpdateAccessByInode {
                    inode_id,
                    boundary: access.boundary,
                    grants: access.grants,
                    expected_access_revision_no: options.expected_access_revision_no,
                },
            ),
        )
        .await
    }

    /// Moves an inode to a name under a parent inode while
    /// `expected_binding_version` is still its binding, refusing to replace
    /// an existing entry.
    pub async fn move_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
        destination_parent_inode_id: InodeId,
        destination_display_name: &DisplayName,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.move_by_inode_with_options(
            namespace_id,
            inode_id,
            expected_binding_version,
            destination_parent_inode_id,
            destination_display_name,
            actor,
            &MoveOptions::default(),
        )
        .await
    }

    /// Moves an inode to a name under a parent inode while
    /// `expected_binding_version` is still its binding.
    #[allow(
        clippy::too_many_arguments,
        reason = "a move names its inode, binding version, and destination, as the wire operation does"
    )]
    pub async fn move_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
        destination_parent_inode_id: InodeId,
        destination_display_name: &DisplayName,
        actor: &ActorId,
        options: &MoveOptions,
    ) -> Result<Commit> {
        self.commit(
            namespace_id,
            actor,
            &single_operation(
                &options.commit,
                FilesystemOperation::MoveByInode {
                    inode_id,
                    expected_binding_version: expected_binding_version.clone(),
                    destination_parent_inode_id,
                    destination_display_name: destination_display_name.clone(),
                    precondition: loonfs_types::DestinationPrecondition {
                        behavior: options.behavior,
                        expected_inode_id: options.expected_destination_inode_id,
                        expected_revision_no: options.expected_destination_revision_no,
                    },
                },
            ),
        )
        .await
    }

    /// Copies a file inode to a name under a parent inode, refusing to
    /// replace an existing entry.
    pub async fn copy_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        destination_parent_inode_id: InodeId,
        destination_display_name: &DisplayName,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.copy_by_inode_with_options(
            namespace_id,
            inode_id,
            destination_parent_inode_id,
            destination_display_name,
            actor,
            &CopyOptions::default(),
        )
        .await
    }

    /// Copies a file inode to a name under a parent inode.
    pub async fn copy_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        destination_parent_inode_id: InodeId,
        destination_display_name: &DisplayName,
        actor: &ActorId,
        options: &CopyOptions,
    ) -> Result<Commit> {
        self.commit(
            namespace_id,
            actor,
            &single_operation(
                &options.commit,
                FilesystemOperation::CopyByInode {
                    inode_id,
                    destination_parent_inode_id,
                    destination_display_name: destination_display_name.clone(),
                    precondition: loonfs_types::DestinationPrecondition {
                        behavior: options.behavior,
                        expected_inode_id: options.expected_destination_inode_id,
                        expected_revision_no: options.expected_destination_revision_no,
                    },
                },
            ),
        )
        .await
    }

    /// Makes an earlier revision of a file inode the current revision.
    pub async fn restore_revision_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.restore_revision_by_inode_with_options(
            namespace_id,
            inode_id,
            source_revision_no,
            actor,
            &CommitOptions::default(),
        )
        .await
    }

    /// Makes an earlier revision of a file inode the current revision, under
    /// the given commit settings.
    pub async fn restore_revision_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        actor: &ActorId,
        options: &CommitOptions,
    ) -> Result<Commit> {
        self.commit(
            namespace_id,
            actor,
            &single_operation(
                options,
                FilesystemOperation::RestoreRevisionByInode {
                    inode_id,
                    source_revision_no,
                },
            ),
        )
        .await
    }
}
