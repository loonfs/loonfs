//! CLI option and transfer handling over the shared client.

use crate::commands::download::FileDownload;
use crate::error::CliError;
use crate::payload::LocalPayload;
use crate::progress::ProgressReporter;
use crate::resolve::ResolvedTarget;
use crate::uploads::UploadJournal;
use loonfs_api::v0::ListChangesResponse;
use loonfs_api::{
    ChangeSeq, Commit, InodeId, ListPathEntriesResponse, NamespaceId, PathEntry, PinId, RevisionNo,
};
use loonfs_client::{
    DownloadOptions, ListChangesOptions, ListPathEntriesOptions, NamespacePath, PutFileOptions,
    ReadFileOptions, StatPathOptions,
};
use std::sync::Arc;

impl ResolvedTarget {
    pub(crate) async fn list_path_entries_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
        snapshot_id: Option<&PinId>,
    ) -> Result<ListPathEntriesResponse, CliError> {
        Ok(self
            .client
            .list_path_entries_page(
                spec,
                limit,
                cursor,
                &ListPathEntriesOptions {
                    snapshot_id: snapshot_id.cloned(),
                    ..ListPathEntriesOptions::default()
                },
            )
            .await?)
    }

    pub(crate) async fn get_path_entry_at_snapshot(
        &self,
        spec: &NamespacePath,
        snapshot_id: Option<&PinId>,
    ) -> Result<PathEntry, CliError> {
        self.get_path_entry_projected(
            spec,
            &StatPathOptions {
                snapshot_id: snapshot_id.cloned(),
                ..StatPathOptions::default()
            },
        )
        .await
    }

    pub(crate) async fn get_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        snapshot_id: Option<&PinId>,
    ) -> Result<PathEntry, CliError> {
        Ok(self
            .client
            .get_inode(
                namespace_id,
                inode_id,
                &StatPathOptions {
                    snapshot_id: snapshot_id.cloned(),
                    ..StatPathOptions::default()
                },
            )
            .await?)
    }

    pub(crate) async fn get_path_entry_without_attributes(
        &self,
        spec: &NamespacePath,
    ) -> Result<PathEntry, CliError> {
        self.get_path_entry_without_attributes_at_snapshot(spec, None)
            .await
    }

    pub(crate) async fn get_path_entry_without_attributes_at_snapshot(
        &self,
        spec: &NamespacePath,
        snapshot_id: Option<&PinId>,
    ) -> Result<PathEntry, CliError> {
        self.get_path_entry_projected(
            spec,
            &StatPathOptions {
                include_attributes: loonfs_api::AttributeInclusion::Omit,
                snapshot_id: snapshot_id.cloned(),
            },
        )
        .await
    }

    async fn get_path_entry_projected(
        &self,
        spec: &NamespacePath,
        options: &StatPathOptions,
    ) -> Result<PathEntry, CliError> {
        Ok(self.client.get_path_entry(spec, options).await?)
    }

    pub(crate) async fn open_file_download(
        &self,
        spec: &NamespacePath,
        revision_no: Option<RevisionNo>,
        snapshot_id: Option<&PinId>,
        start_offset: u64,
    ) -> Result<FileDownload, CliError> {
        if self.client.offers_direct_download().await? {
            let grant = self
                .client
                .create_download(
                    spec,
                    &DownloadOptions {
                        revision_no,
                        snapshot_id: snapshot_id.cloned(),
                    },
                )
                .await?;
            return Ok(FileDownload::Direct {
                revision_no: grant.revision_no,
                stream: Box::new(
                    self.client
                        .open_direct_download_at(&grant, start_offset)
                        .await?,
                ),
                resumed_from: start_offset,
            });
        }
        Ok(FileDownload::Proxied(
            self.client
                .read_file_stream(
                    spec,
                    &ReadFileOptions {
                        revision_no,
                        snapshot_id: snapshot_id.cloned(),
                    },
                )
                .await?,
        ))
    }

    pub(crate) async fn put_file_stream(
        &self,
        spec: &NamespacePath,
        payload: &LocalPayload,
        options: &PutFileOptions,
        progress: &Arc<ProgressReporter>,
        journal: Option<&UploadJournal>,
    ) -> Result<Commit, CliError> {
        let source = payload.open_source(progress).await?;
        let Some(journal) = journal else {
            return Ok(self.client.put_file_stream(spec, source, options).await?);
        };
        let resume = journal.resume();
        Ok(self
            .client
            .put_file_stream_resumable(spec, source, options, journal, resume.as_ref())
            .await?)
    }

    pub(crate) async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
        snapshot_id: Option<&PinId>,
    ) -> Result<ListChangesResponse, CliError> {
        Ok(self
            .client
            .list_changes(
                namespace_id,
                after_seq,
                &ListChangesOptions {
                    limit,
                    snapshot_id: snapshot_id.cloned(),
                },
            )
            .await?)
    }
}
