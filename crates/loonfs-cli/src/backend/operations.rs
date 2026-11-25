//! CLI option and transfer handling over the shared client.

use crate::commands::download::{FileDownload, FileVersion};
use crate::error::Result;
use crate::payload::LocalPayload;
use crate::progress::ProgressReporter;
use crate::resolve::ResolvedTarget;
use crate::uploads::UploadJournal;
use loonfs_client::{DownloadOptions, NamespacePath, PutFileOptions, StatOptions};
use loonfs_types::{Commit, PathEntry, SnapshotId};
use std::sync::Arc;

impl ResolvedTarget {
    pub(crate) async fn stat_without_attributes(&self, spec: &NamespacePath) -> Result<PathEntry> {
        self.stat_without_attributes_at_snapshot(spec, None).await
    }

    pub(crate) async fn stat_without_attributes_at_snapshot(
        &self,
        spec: &NamespacePath,
        snapshot_id: Option<&SnapshotId>,
    ) -> Result<PathEntry> {
        let options = StatOptions {
            include_attributes: loonfs_types::options::AttributeInclusion::Omit,
        };
        Ok(match snapshot_id {
            Some(snapshot_id) => {
                self.client
                    .snapshot_reader(snapshot_id.clone())
                    .stat_with_options(spec, &options)
                    .await?
            }
            None => self.client.stat_with_options(spec, &options).await?,
        })
    }

    pub(crate) async fn open_file_download(
        &self,
        spec: &NamespacePath,
        version: &FileVersion,
    ) -> Result<FileDownload> {
        if self.client.offers_direct_download().await? {
            let grant = match version {
                FileVersion::Revision(revision_no) => {
                    self.client
                        .create_revision_download(spec, *revision_no)
                        .await?
                }
                FileVersion::Current => self.client.create_download(spec).await?,
                FileVersion::Snapshot(snapshot_id) => {
                    self.client
                        .snapshot_reader(snapshot_id.clone())
                        .create_download_with_options(spec, &DownloadOptions::default())
                        .await?
                }
            };
            return Ok(FileDownload::Direct(Box::new(
                self.client.open_direct_download(&grant).await?,
            )));
        }
        let stream = match version {
            FileVersion::Revision(revision_no) => {
                self.client
                    .read_file_revision_stream(spec, *revision_no)
                    .await?
            }
            FileVersion::Current => self.client.read_file_stream(spec).await?,
            FileVersion::Snapshot(snapshot_id) => {
                self.client
                    .snapshot_reader(snapshot_id.clone())
                    .read_file_stream(spec)
                    .await?
            }
        };
        Ok(FileDownload::Proxied(stream))
    }

    pub(crate) async fn put_file_stream(
        &self,
        spec: &NamespacePath,
        payload: &LocalPayload,
        options: &PutFileOptions,
        progress: &Arc<ProgressReporter>,
        journal: Option<&UploadJournal>,
    ) -> Result<Commit> {
        let source = payload.open_source(progress).await?;
        let Some(journal) = journal else {
            return Ok(self
                .client
                .put_file_stream_with_options(spec, source, options)
                .await?);
        };
        let resume = journal.resume();
        Ok(self
            .client
            .put_file_stream_resumable(spec, source, options, journal, resume.as_ref())
            .await?)
    }
}
