//! Client download streams used by local file transfers.

use crate::error::CliError;
use bytes::Bytes;
use futures::StreamExt;
use loonfs_client::{DirectDownloadStream, PayloadStream};
use loonfs_types::{ContentRef, RevisionNo};

/// The content and revision that a direct download resolved, which a resumed
/// download has to match.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ResumeIdentity<'a> {
    pub(crate) content_ref: &'a ContentRef,
    pub(crate) revision_no: RevisionNo,
}

pub(crate) enum FileDownload {
    Direct {
        stream: Box<DirectDownloadStream>,
        revision_no: RevisionNo,
        resumed_from: u64,
    },
    Proxied(PayloadStream),
}

impl FileDownload {
    pub(crate) fn resume_identity(&self) -> Option<ResumeIdentity<'_>> {
        match self {
            Self::Direct {
                stream,
                revision_no,
                ..
            } => Some(ResumeIdentity {
                content_ref: stream.content_ref(),
                revision_no: *revision_no,
            }),
            Self::Proxied(_) => None,
        }
    }

    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Bytes>, CliError> {
        match self {
            Self::Direct { stream, .. } => stream.next_chunk().await.map_err(CliError::from),
            Self::Proxied(stream) => stream.next().await.transpose().map_err(CliError::io),
        }
    }

    pub(crate) fn resumed_from(&self) -> u64 {
        match self {
            Self::Direct { resumed_from, .. } => *resumed_from,
            Self::Proxied(_) => 0,
        }
    }

    pub(crate) fn fold_resumed_prefix(&mut self, bytes: &[u8]) -> Result<(), CliError> {
        match self {
            Self::Direct { stream, .. } => {
                stream.fold_resumed_prefix(bytes).map_err(CliError::from)
            }
            Self::Proxied(_) => Ok(()),
        }
    }
}
