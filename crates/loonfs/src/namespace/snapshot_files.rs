//! Listings of the files a snapshot records, on a [`Namespace`] handle in
//! any mode.

use crate::pagers::SnapshotFilesPager;
use crate::trace::{self, operation_span};
use crate::{
    Consistency, ListSnapshotFilesOptions, Namespace, PinId, Result, SnapshotFilesPage,
    SnapshotFilesPageCursor,
};
use loonfs_types::PageRequest;

impl<M: crate::Mode> Namespace<M> {
    /// Lists files visible at a snapshot in ascending inode-ID order.
    ///
    /// The pinned manifest is read without replaying later WAL entries.
    /// Directories are omitted. A missing or expired snapshot returns
    /// `snapshot_not_found` rather than falling back to current state. A
    /// subject that isn't an administrator gets `forbidden`.
    pub fn list_snapshot_files(&self, snapshot_id: &PinId) -> SnapshotFilesPager {
        self.list_snapshot_files_with_options(snapshot_id, &ListSnapshotFilesOptions::default())
    }

    /// Lists files at a snapshot as `options` asks. With
    /// [`ListSnapshotFilesOptions::include_deleted`], the pages also list
    /// deleted files and files under a deleted directory.
    pub fn list_snapshot_files_with_options(
        &self,
        snapshot_id: &PinId,
        options: &ListSnapshotFilesOptions,
    ) -> SnapshotFilesPager {
        loonfs_types::Pager::with_state(
            (self.read_only(), snapshot_id.clone(), *options),
            |(reader, snapshot_id, options), request| async move {
                reader
                    .snapshot_files_page(&snapshot_id, request, options)
                    .await
            },
        )
    }

    async fn snapshot_files_page(
        &self,
        snapshot_id: &PinId,
        request: PageRequest<SnapshotFilesPageCursor>,
        options: ListSnapshotFilesOptions,
    ) -> Result<SnapshotFilesPage> {
        trace::in_operation(
            operation_span!(self.core, "list_snapshot_files", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let pinned = self
                    .core
                    .pinned_read(&self.namespace_id, Consistency::Strong)
                    .await?;
                pinned.engine.ensure_administrator(&pinned.context).await?;
                Ok(pinned
                    .engine
                    .list_snapshot_files_page(snapshot_id, request, options, &pinned.context)
                    .await?)
            },
        )
        .await
    }
}
