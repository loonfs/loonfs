//! Listings of the files a checkpoint records, on a [`Namespace`] handle in
//! any mode.

use crate::pagers::CheckpointFilesPager;
use crate::trace::{self, operation_span};
use crate::{
    CheckpointFilesPage, CheckpointFilesPageCursor, Consistency, ListCheckpointFilesOptions,
    Namespace, PinId, Result,
};
use loonfs_types::PageRequest;

impl<M: crate::Mode> Namespace<M> {
    /// Lists files visible at a snapshot in ascending inode-ID order.
    ///
    /// The pinned manifest is read without replaying later WAL entries.
    /// Directories are omitted. A missing or expired snapshot returns
    /// `snapshot_not_found` rather than falling back to current state.
    pub fn list_checkpoint_files(&self, checkpoint_id: &PinId) -> CheckpointFilesPager {
        self.list_checkpoint_files_with_options(
            checkpoint_id,
            &ListCheckpointFilesOptions::default(),
        )
    }

    /// Lists files at a snapshot as `options` asks. With
    /// [`ListCheckpointFilesOptions::include_deleted`], the pages also list
    /// deleted files and files under a deleted directory.
    pub fn list_checkpoint_files_with_options(
        &self,
        checkpoint_id: &PinId,
        options: &ListCheckpointFilesOptions,
    ) -> CheckpointFilesPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), checkpoint_id.clone(), *options),
            |(reader, checkpoint_id, options), request| async move {
                reader
                    .checkpoint_files_page(&checkpoint_id, request, options)
                    .await
            },
        )
    }

    async fn checkpoint_files_page(
        &self,
        checkpoint_id: &PinId,
        request: PageRequest<CheckpointFilesPageCursor>,
        options: ListCheckpointFilesOptions,
    ) -> Result<CheckpointFilesPage> {
        trace::in_operation(
            operation_span!(self.core, "list_checkpoint_files", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let pinned = self
                    .core
                    .pinned_read(&self.namespace_id, Consistency::Strong)
                    .await?;
                Ok(pinned
                    .engine
                    .list_checkpoint_files_page(checkpoint_id, request, options, &pinned.context)
                    .await?)
            },
        )
        .await
    }
}
