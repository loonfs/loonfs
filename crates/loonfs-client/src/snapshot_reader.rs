//! Reads of one snapshot through the server.

use crate::operations::{self, Operation};
use crate::read_at_fields::ReadAtFields;
use crate::reads::{ChangesPager, FileRevisionsPager, InodeChildrenPager, PathEntriesPager};
use crate::{Client, FileRangeStream, NamespacePath, PayloadStream, Result};
use loonfs_types::options::{
    DownloadOptions, ListChangesOptions, ListOptions, ReadRange, StatOptions,
};
use loonfs_types::{
    CommitSeq, CreateDownloadByInodeResponse, CreateDownloadResponse, InodeId, NamespaceId,
    PathEntry, RevisionNo, SnapshotId,
};

/// A reader of one snapshot, or of one of a namespace's fork snapshots,
/// from [`Client::snapshot_reader`].
///
/// Each read sends the request that the [`Client`] method of the same name
/// sends, with the snapshot's `snapshot_id`, so the server reads the state
/// that the snapshot captured. A missing or expired snapshot returns
/// `snapshot_not_found`. A listing's cursor is bound to the snapshot, so the
/// caller reads a later page through a reader of the same snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotReader {
    client: Client,
    snapshot_id: SnapshotId,
}

impl SnapshotReader {
    pub(crate) fn new(client: Client, snapshot_id: SnapshotId) -> Self {
        Self {
            client,
            snapshot_id,
        }
    }

    fn fields(&self) -> ReadAtFields {
        ReadAtFields::snapshot(self.snapshot_id.clone())
    }

    async fn read_bytes(&self, operation: Operation, url: &str) -> Result<Vec<u8>> {
        Ok(self
            .client
            .call(&self.client.request(operation, url))
            .await?
            .bytes)
    }

    /// Returns the bytes of the revision of a file that the snapshot
    /// captured, as [`Client::read_file`] does.
    pub async fn read_file(&self, spec: &NamespacePath) -> Result<Vec<u8>> {
        let url = self.client.file_content_url(spec, None, &self.fields());
        self.read_bytes(operations::GET_FILE_CONTENT, &url).await
    }

    /// Returns the bytes of the revision of a file inode that the snapshot
    /// captured, as [`Client::read_file_by_inode`] does.
    pub async fn read_file_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<Vec<u8>> {
        let url = self
            .client
            .inode_content_url(namespace_id, inode_id, &self.fields());
        self.read_bytes(operations::GET_FILE_CONTENT_BY_INODE, &url)
            .await
    }

    /// Returns the bytes of one revision of a file inode that the snapshot's
    /// manifest keeps, as [`Client::read_file_revision_by_inode`] does.
    pub async fn read_file_revision_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let url = self.client.inode_revision_content_url(
            namespace_id,
            inode_id,
            revision_no,
            &self.fields(),
        );
        self.read_bytes(operations::GET_FILE_REVISION_CONTENT_BY_INODE, &url)
            .await
    }

    /// Streams one byte range of the revision of a file that the snapshot
    /// captured, as [`Client::read_file_range_stream`] does.
    pub async fn read_file_range_stream(
        &self,
        spec: &NamespacePath,
        range: ReadRange,
    ) -> Result<FileRangeStream> {
        let url = self.client.file_content_url(spec, None, &self.fields());
        self.client
            .read_range(operations::GET_FILE_CONTENT, &url, range)
            .await
    }

    /// Streams one byte range of the revision of a file inode that the
    /// snapshot captured, as [`Client::read_file_range_stream_by_inode`]
    /// does.
    pub async fn read_file_range_stream_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        range: ReadRange,
    ) -> Result<FileRangeStream> {
        let url = self
            .client
            .inode_content_url(namespace_id, inode_id, &self.fields());
        self.client
            .read_range(operations::GET_FILE_CONTENT_BY_INODE, &url, range)
            .await
    }

    /// Streams one byte range of one revision of a file inode that the
    /// snapshot's manifest keeps, as
    /// [`Client::read_file_revision_range_stream_by_inode`] does.
    pub async fn read_file_revision_range_stream_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
        range: ReadRange,
    ) -> Result<FileRangeStream> {
        let url = self.client.inode_revision_content_url(
            namespace_id,
            inode_id,
            revision_no,
            &self.fields(),
        );
        self.client
            .read_range(operations::GET_FILE_REVISION_CONTENT_BY_INODE, &url, range)
            .await
    }

    /// Lists a directory in the snapshot, as [`Client::list_with_options`]
    /// does.
    pub fn list_with_options(
        &self,
        spec: &NamespacePath,
        options: &ListOptions,
    ) -> PathEntriesPager {
        self.client.list_at(spec, options, self.fields())
    }

    /// Returns path metadata in the snapshot, as
    /// [`Client::stat_with_options`] does.
    pub async fn stat_with_options(
        &self,
        spec: &NamespacePath,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        self.client.stat_at(spec, options, &self.fields()).await
    }

    /// Returns an inode's entry in the snapshot, as
    /// [`Client::stat_by_inode_with_options`] does.
    pub async fn stat_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &StatOptions,
    ) -> Result<PathEntry> {
        self.client
            .stat_by_inode_at(namespace_id, inode_id, options, &self.fields())
            .await
    }

    /// Lists a directory's children by inode in the snapshot, as
    /// [`Client::list_by_inode_with_options`] does.
    pub fn list_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &ListOptions,
    ) -> InodeChildrenPager {
        self.client
            .list_by_inode_at(namespace_id, inode_id, options, self.fields())
    }

    /// Lists the revisions that the snapshot's manifest keeps of the file at
    /// a path in the snapshot, newest first from the revision that the
    /// snapshot captured.
    pub fn list_file_revisions(&self, spec: &NamespacePath) -> FileRevisionsPager {
        self.client.list_file_revisions_at(spec, self.fields())
    }

    /// Lists the revisions that the snapshot's manifest keeps of a file
    /// inode, newest first from the revision that the snapshot captured.
    pub fn list_file_revisions_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> FileRevisionsPager {
        self.client
            .list_file_revisions_by_inode_at(namespace_id, inode_id, self.fields())
    }

    /// Lists the change feed after `after_seq` through the sequence that the
    /// snapshot captured, as [`Client::list_changes_with_options`] does.
    pub fn list_changes_with_options(
        &self,
        namespace_id: &NamespaceId,
        after_seq: CommitSeq,
        options: &ListChangesOptions,
    ) -> ChangesPager {
        self.client
            .list_changes_at(namespace_id, after_seq, options, self.fields())
    }

    /// Streams the revision of a file that the snapshot captured, as
    /// [`Client::read_file_stream`] does.
    pub async fn read_file_stream(&self, spec: &NamespacePath) -> Result<PayloadStream> {
        self.client.read_file_stream_at(spec, &self.fields()).await
    }

    /// Streams the revision of a file inode that the snapshot captured, as
    /// [`Client::read_file_stream_by_inode`] does.
    pub async fn read_file_stream_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<PayloadStream> {
        self.client
            .read_file_stream_by_inode_at(namespace_id, inode_id, &self.fields())
            .await
    }

    /// Streams one revision of a file inode that the snapshot's manifest
    /// keeps, as [`Client::read_file_revision_stream_by_inode`] does.
    pub async fn read_file_revision_stream_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<PayloadStream> {
        self.client
            .read_file_revision_stream_by_inode_at(
                namespace_id,
                inode_id,
                revision_no,
                &self.fields(),
            )
            .await
    }

    /// Requests direct access to the revision of a file that the snapshot
    /// captured, as [`Client::create_download_with_options`] does.
    pub async fn create_download_with_options(
        &self,
        spec: &NamespacePath,
        options: &DownloadOptions,
    ) -> Result<CreateDownloadResponse> {
        self.client
            .request_download(spec, None, options, self.fields())
            .await
    }

    /// Requests direct access to the revision of a file inode that the
    /// snapshot captured, as [`Client::create_download_by_inode_with_options`]
    /// does.
    pub async fn create_download_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &DownloadOptions,
    ) -> Result<CreateDownloadByInodeResponse> {
        self.client
            .create_download_by_inode_at(namespace_id, inode_id, options, self.fields())
            .await
    }

    /// Requests direct access to one revision of a file inode that the
    /// snapshot's manifest keeps, as
    /// [`Client::create_revision_download_by_inode_with_options`] does.
    pub async fn create_revision_download_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: &DownloadOptions,
    ) -> Result<CreateDownloadByInodeResponse> {
        self.client
            .create_revision_download_by_inode_at(
                namespace_id,
                inode_id,
                revision_no,
                options,
                self.fields(),
            )
            .await
    }
}
