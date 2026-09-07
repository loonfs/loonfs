//! Download results shared by embedded and remote profiles.

use crate::backend_error::map_namespace_scoped_runtime_error;
use crate::error::CliError;
use bytes::Bytes;
use futures::StreamExt;
use loonfs::{FileContentStream, RuntimeError, SharedObjectStore};
use loonfs_api::{ContentRef, NamespaceId, RevisionNo};
use loonfs_client::{DirectDownloadStream, PayloadStream};

/// File content returned by either profile type.
///
/// The result is an embedded, direct, or service-proxied stream.
/// [`FileDownload::next_chunk`] presents all three as a sequence of chunks.
pub(crate) enum FileDownload {
    /// A bounded stream from the embedded runtime.
    Streamed {
        namespace_id: NamespaceId,
        stream: Box<FileContentStream<SharedObjectStore>>,
        resumed_from: u64,
    },
    /// A remote object-store response, streamed and verified by the client
    /// against the content reference in its download grant.
    Direct {
        stream: Box<DirectDownloadStream>,
        revision_no: RevisionNo,
        resumed_from: u64,
    },
    /// A server response whose successful completion follows verification.
    Proxied(PayloadStream),
}

impl FileDownload {
    /// Authoritative metadata for transports that can resume.
    pub(crate) fn resume_identity(&self) -> Option<(&ContentRef, RevisionNo)> {
        match self {
            Self::Streamed { stream, .. } => {
                Some((stream.content_ref(), stream.entry()?.revision_no()?))
            }
            Self::Direct {
                stream,
                revision_no,
                ..
            } => Some((stream.content_ref(), *revision_no)),
            Self::Proxied(_) => None,
        }
    }

    /// Returns the next chunk, or `None` after full verification.
    ///
    /// Streamed downloads verify length and checksum when the final call reaches
    /// the end. Stopping early does not complete verification. Proxied streams
    /// rely on the server aborting the response on a verification failure.
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Bytes>, CliError> {
        match self {
            Self::Streamed {
                namespace_id,
                stream,
                ..
            } => stream.next_chunk().await.map_err(|error| {
                map_namespace_scoped_runtime_error(namespace_id, RuntimeError::Core(error))
            }),
            Self::Direct { stream, .. } => stream.next_chunk().await.map_err(CliError::from),
            Self::Proxied(stream) => stream.next().await.transpose().map_err(CliError::io),
        }
    }

    /// Returns the offset where this response starts.
    ///
    /// Proxied responses cannot resume, so they always start at zero.
    pub(crate) fn resumed_from(&self) -> u64 {
        match self {
            Self::Streamed { resumed_from, .. } | Self::Direct { resumed_from, .. } => {
                *resumed_from
            }
            Self::Proxied(_) => 0,
        }
    }

    /// Adds the existing prefix to a resumed download's checksum.
    ///
    /// Streaming downloads verify the complete object, including bytes read
    /// by an earlier attempt. Proxied responses do not resume and ignore the
    /// prefix.
    pub(crate) fn fold_resumed_prefix(&mut self, bytes: &[u8]) -> Result<(), CliError> {
        match self {
            Self::Streamed {
                namespace_id,
                stream,
                ..
            } => stream.fold_resumed_prefix(bytes).map_err(|error| {
                map_namespace_scoped_runtime_error(namespace_id, RuntimeError::Core(error))
            }),
            Self::Direct { stream, .. } => {
                stream.fold_resumed_prefix(bytes);
                Ok(())
            }
            Self::Proxied(_) => Ok(()),
        }
    }
}
