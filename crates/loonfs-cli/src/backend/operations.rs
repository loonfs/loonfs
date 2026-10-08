//! CLI option and transfer handling over the shared client.

use crate::commands::download::FileDownload;
use crate::error::CliError;
use crate::payload::LocalPayload;
use crate::progress::ProgressReporter;
use crate::resolve::ResolvedTarget;
use crate::uploads::UploadJournal;
use loonfs_client::{
    ChangesPager, DownloadOptions, ListChangesOptions, ListOptions, NamespacePath,
    PathEntriesPager, PutFileOptions, ReadFileOptions, StatOptions,
};
use loonfs_types::{
    ActorId, ChangeSeq, Commit, InodeId, NamespaceId, PathEntry, PinId, RevisionNo,
};
use std::sync::Arc;

impl ResolvedTarget {
    pub(crate) fn list_at_snapshot(
        &self,
        spec: &NamespacePath,
        snapshot_id: Option<&PinId>,
    ) -> PathEntriesPager {
        self.client.list_with_options(
            spec,
            &ListOptions {
                snapshot_id: snapshot_id.cloned(),
                ..ListOptions::default()
            },
        )
    }

    pub(crate) async fn stat_at_snapshot(
        &self,
        spec: &NamespacePath,
        snapshot_id: Option<&PinId>,
    ) -> Result<PathEntry, CliError> {
        self.stat_with_options(
            spec,
            &StatOptions {
                snapshot_id: snapshot_id.cloned(),
                ..StatOptions::default()
            },
        )
        .await
    }

    pub(crate) async fn stat_by_inode_at_snapshot(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        snapshot_id: Option<&PinId>,
    ) -> Result<PathEntry, CliError> {
        Ok(self
            .client
            .stat_by_inode_with_options(
                namespace_id,
                inode_id,
                &StatOptions {
                    snapshot_id: snapshot_id.cloned(),
                    ..StatOptions::default()
                },
            )
            .await?)
    }

    pub(crate) async fn stat_without_attributes(
        &self,
        spec: &NamespacePath,
    ) -> Result<PathEntry, CliError> {
        self.stat_without_attributes_at_snapshot(spec, None).await
    }

    pub(crate) async fn stat_without_attributes_at_snapshot(
        &self,
        spec: &NamespacePath,
        snapshot_id: Option<&PinId>,
    ) -> Result<PathEntry, CliError> {
        self.stat_with_options(
            spec,
            &StatOptions {
                include_attributes: loonfs_types::AttributeInclusion::Omit,
                snapshot_id: snapshot_id.cloned(),
            },
        )
        .await
    }

    async fn stat_with_options(
        &self,
        spec: &NamespacePath,
        options: &StatOptions,
    ) -> Result<PathEntry, CliError> {
        Ok(self.client.stat_with_options(spec, options).await?)
    }

    pub(crate) async fn open_file_download(
        &self,
        spec: &NamespacePath,
        revision_no: Option<RevisionNo>,
        snapshot_id: Option<&PinId>,
        start_offset: u64,
    ) -> Result<FileDownload, CliError> {
        if self.client.offers_direct_download().await? {
            let grant = match revision_no {
                Some(revision_no) => {
                    self.client
                        .create_revision_download(spec, revision_no)
                        .await?
                }
                None => {
                    self.client
                        .create_download_with_options(
                            spec,
                            &DownloadOptions {
                                snapshot_id: snapshot_id.cloned(),
                            },
                        )
                        .await?
                }
            };
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
                .read_file_stream_with_options(
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
        actor: &ActorId,
        options: &PutFileOptions,
        progress: &Arc<ProgressReporter>,
        journal: Option<&UploadJournal>,
    ) -> Result<Commit, CliError> {
        let source = payload.open_source(progress).await?;
        let Some(journal) = journal else {
            return Ok(self
                .client
                .put_file_stream_with_options(spec, source, actor, options)
                .await?);
        };
        let resume = journal.resume();
        Ok(self
            .client
            .put_file_stream_resumable(spec, source, actor, options, journal, resume.as_ref())
            .await?)
    }

    pub(crate) fn list_changes_at_snapshot(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        snapshot_id: Option<&PinId>,
    ) -> ChangesPager {
        self.client.list_changes_with_options(
            namespace_id,
            after_seq,
            &ListChangesOptions {
                snapshot_id: snapshot_id.cloned(),
            },
        )
    }
}
