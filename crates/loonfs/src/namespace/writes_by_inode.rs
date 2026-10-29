//! The writable [`Namespace`] handle's mutations addressed by inode ID.

use crate::publish::FilesystemOperation;
use crate::trace::{self, operation_span};
use crate::Result;
use crate::{
    AccessState, ActorId, AppendFileByInodeOptions, AttributeChanges, BindingVersion, Commit,
    CommitOptions, CopyOptions, DeleteByInodeOptions, DisplayName, InodeId, MoveOptions,
    RevisionNo, UpdateAccessByInodeOptions, UpdateAttributesByInodeOptions,
};
use crate::{Namespace, Writable};
use loonfs_types::u64_from_usize;
use loonfs_types::FileContent;

impl Namespace<Writable> {
    /// Writes file bytes to a new name under a parent directory inode,
    /// refusing a name that is already bound.
    pub async fn create_file_by_inode(
        &self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        bytes: &[u8],
        actor: &ActorId,
    ) -> Result<Commit> {
        self.create_file_by_inode_with_options(
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
    /// or under the configured inline threshold, a rerun with the same bytes
    /// and commit ID replays. Larger content stages again, so a rerun
    /// conflicts. At every size, retain [`Self::prepare_content`]'s result
    /// and retry through [`Self::commit_prepared`] with the same commit ID.
    pub async fn create_file_by_inode_with_options(
        &self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        bytes: &[u8],
        actor: &ActorId,
        options: &CommitOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(
                self.core,
                "create_file_by_inode",
                self.namespace_id,
                payload_class = trace::payload_class(u64_from_usize(bytes.len()))
            ),
            async {
                let prepared_content = self.prepare_content_inner(bytes).await?;
                let operation = FilesystemOperation::create_file_by_inode(
                    parent_inode_id,
                    display_name.clone(),
                    FileContent::Ref(prepared_content.content_ref().clone()),
                );
                self.commit_prepared_one(actor, options, operation, prepared_content)
                    .await
            },
        )
        .await
    }

    /// Writes file bytes as the next revision of a file inode, wherever it is
    /// bound, while `expected_revision_no` is still its current revision.
    pub async fn put_file_by_inode(
        &self,
        inode_id: InodeId,
        expected_revision_no: RevisionNo,
        bytes: &[u8],
        actor: &ActorId,
    ) -> Result<Commit> {
        self.put_file_by_inode_with_options(
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
    /// or under the configured inline threshold, a rerun with the same bytes
    /// and commit ID replays. Larger content stages again, so a rerun
    /// conflicts. At every size, retain [`Self::prepare_content`]'s result
    /// and retry through [`Self::commit_prepared`] with the same commit ID.
    pub async fn put_file_by_inode_with_options(
        &self,
        inode_id: InodeId,
        expected_revision_no: RevisionNo,
        bytes: &[u8],
        actor: &ActorId,
        options: &CommitOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(
                self.core,
                "put_file_by_inode",
                self.namespace_id,
                payload_class = trace::payload_class(u64_from_usize(bytes.len()))
            ),
            async {
                let prepared_content = self.prepare_content_inner(bytes).await?;
                let operation = FilesystemOperation::put_file_revision_by_inode(
                    inode_id,
                    FileContent::Ref(prepared_content.content_ref().clone()),
                    expected_revision_no,
                );
                self.commit_prepared_one(actor, options, operation, prepared_content)
                    .await
            },
        )
        .await
    }

    /// Adds bytes to the end of a file inode as its next revision, wherever
    /// it is bound.
    pub async fn append_file_by_inode(
        &self,
        inode_id: InodeId,
        bytes: &[u8],
        actor: &ActorId,
    ) -> Result<Commit> {
        self.append_file_by_inode_with_options(
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
        inode_id: InodeId,
        bytes: &[u8],
        actor: &ActorId,
        options: &AppendFileByInodeOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(
                self.core,
                "append_file_by_inode",
                self.namespace_id,
                payload_class = trace::payload_class(u64_from_usize(bytes.len()))
            ),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::append_file_by_inode(inode_id, bytes.to_vec(), options),
            ),
        )
        .await
    }

    /// Creates a directory at a new name under a parent directory inode.
    pub async fn create_directory_by_inode(
        &self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.create_directory_by_inode_with_options(
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
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        actor: &ActorId,
        options: &CommitOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "create_directory_by_inode", self.namespace_id),
            self.commit_one(
                actor,
                options,
                FilesystemOperation::create_directory_by_inode(
                    parent_inode_id,
                    display_name.clone(),
                ),
            ),
        )
        .await
    }

    /// Deletes a file or empty directory inode while
    /// `expected_binding_version` is still its binding.
    pub async fn delete_by_inode(
        &self,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.delete_by_inode_with_options(
            inode_id,
            expected_binding_version,
            actor,
            &DeleteByInodeOptions::default(),
        )
        .await
    }

    /// Deletes a file or directory inode while `expected_binding_version` is
    /// still its binding. Deletion is tombstone-first, as
    /// [`Self::delete_path_with_options`] describes.
    pub async fn delete_by_inode_with_options(
        &self,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
        actor: &ActorId,
        options: &DeleteByInodeOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "delete_by_inode", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::delete_by_inode(
                    inode_id,
                    expected_binding_version.clone(),
                    options,
                ),
            ),
        )
        .await
    }

    /// Moves an inode to a name under a parent inode while
    /// `expected_binding_version` is still its binding, refusing to replace
    /// an existing entry.
    pub async fn move_by_inode(
        &self,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
        destination_parent_inode_id: InodeId,
        destination_display_name: &DisplayName,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.move_by_inode_with_options(
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
    pub async fn move_by_inode_with_options(
        &self,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
        destination_parent_inode_id: InodeId,
        destination_display_name: &DisplayName,
        actor: &ActorId,
        options: &MoveOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "move_by_inode", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::move_by_inode(
                    inode_id,
                    expected_binding_version.clone(),
                    destination_parent_inode_id,
                    destination_display_name.clone(),
                    options,
                ),
            ),
        )
        .await
    }

    /// Copies a file inode to a name under a parent inode, refusing to
    /// replace an existing entry.
    pub async fn copy_by_inode(
        &self,
        inode_id: InodeId,
        destination_parent_inode_id: InodeId,
        destination_display_name: &DisplayName,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.copy_by_inode_with_options(
            inode_id,
            destination_parent_inode_id,
            destination_display_name,
            actor,
            &CopyOptions::default(),
        )
        .await
    }

    /// Copies a file inode to a name under a parent inode. The new file
    /// reuses the source revision's content reference: no bytes are copied.
    pub async fn copy_by_inode_with_options(
        &self,
        inode_id: InodeId,
        destination_parent_inode_id: InodeId,
        destination_display_name: &DisplayName,
        actor: &ActorId,
        options: &CopyOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "copy_by_inode", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::copy_by_inode(
                    inode_id,
                    destination_parent_inode_id,
                    destination_display_name.clone(),
                    options,
                ),
            ),
        )
        .await
    }

    /// Restores a prior revision of a file inode by appending a new current
    /// revision.
    pub async fn restore_revision_by_inode(
        &self,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.restore_revision_by_inode_with_options(
            inode_id,
            source_revision_no,
            actor,
            &CommitOptions::default(),
        )
        .await
    }

    /// Restores a prior revision of a file inode by appending a new current
    /// revision, under the given commit settings.
    pub async fn restore_revision_by_inode_with_options(
        &self,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        actor: &ActorId,
        options: &CommitOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "restore_revision_by_inode", self.namespace_id),
            self.commit_one(
                actor,
                options,
                FilesystemOperation::restore_revision_by_inode(inode_id, source_revision_no),
            ),
        )
        .await
    }

    /// Writes and removes attributes on a visible inode.
    pub async fn update_attributes_by_inode(
        &self,
        inode_id: InodeId,
        actor: &ActorId,
        changes: AttributeChanges,
    ) -> Result<Commit> {
        self.update_attributes_by_inode_with_options(
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
        inode_id: InodeId,
        actor: &ActorId,
        changes: AttributeChanges,
        options: &UpdateAttributesByInodeOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "update_attributes_by_inode", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::update_attributes_by_inode(inode_id, changes, options),
            ),
        )
        .await
    }

    /// Replaces a visible inode's access row, including the root's.
    pub async fn update_access_by_inode(
        &self,
        inode_id: InodeId,
        actor: &ActorId,
        access: AccessState,
    ) -> Result<Commit> {
        self.update_access_by_inode_with_options(
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
        inode_id: InodeId,
        actor: &ActorId,
        access: AccessState,
        options: &UpdateAccessByInodeOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "update_access_by_inode", self.namespace_id),
            self.commit_one(
                actor,
                &options.commit,
                FilesystemOperation::update_access_by_inode(inode_id, access, options),
            ),
        )
        .await
    }
}
