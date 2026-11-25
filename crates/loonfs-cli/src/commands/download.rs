//! Client download streams used by local file transfers.

use crate::error::{CliError, Result};
use bytes::Bytes;
use futures::StreamExt;
use loonfs_client::{DirectDownloadStream, PayloadStream};
use loonfs_types::{RevisionNo, SnapshotId};

/// Which revision of a file a download reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FileVersion {
    /// The current revision at the live head.
    Current,
    /// The revision that a snapshot captured.
    Snapshot(SnapshotId),
    /// One retained revision.
    Revision(RevisionNo),
}

impl FileVersion {
    /// Converts the `--revision` and `--snapshot-id` flags, which clap makes
    /// exclusive.
    pub(crate) fn from_flags(
        revision_no: Option<RevisionNo>,
        snapshot_id: Option<SnapshotId>,
    ) -> Self {
        match (revision_no, snapshot_id) {
            (Some(revision_no), _) => Self::Revision(revision_no),
            (None, Some(snapshot_id)) => Self::Snapshot(snapshot_id),
            (None, None) => Self::Current,
        }
    }
}

pub(crate) enum FileDownload {
    Direct(Box<DirectDownloadStream>),
    Proxied(PayloadStream),
}

impl FileDownload {
    /// Returns the size of the content a direct download fetches. A proxied
    /// download doesn't know it before the stream ends.
    pub(crate) fn content_size(&self) -> Option<u64> {
        match self {
            Self::Direct(stream) => Some(stream.content_ref().size_bytes),
            Self::Proxied(_) => None,
        }
    }

    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        match self {
            Self::Direct(stream) => stream.next_chunk().await.map_err(CliError::from),
            Self::Proxied(stream) => stream.next().await.transpose().map_err(CliError::io),
        }
    }
}
