//! Download results shared by embedded and remote profiles.

use crate::backend_error::{map_namespace_scoped_runtime_error, BackendError};
use bytes::Bytes;
use loonfs::{FileContentStream, RuntimeError, SharedObjectStore};
use loonfs_api::NamespaceId;
use loonfs_client::DirectDownloadStream;

/// File content returned by either profile type.
///
/// The result may be an embedded stream, a remote stream, or buffered bytes.
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
        resumed_from: u64,
    },
    /// A complete response already held in memory.
    Whole(Vec<u8>),
}

impl FileDownload {
    /// Returns the next chunk, or `None` after full verification.
    ///
    /// Streamed downloads verify length and checksum when the final call reaches
    /// the end. Stopping early does not complete verification. Buffered bytes
    /// were verified before this method receives them.
    pub(crate) async fn next_chunk(&mut self) -> Result<Option<Bytes>, BackendError> {
        match self {
            Self::Streamed {
                namespace_id,
                stream,
                ..
            } => stream.next_chunk().await.map_err(|error| {
                map_namespace_scoped_runtime_error(namespace_id, RuntimeError::Core(error))
            }),
            Self::Direct { stream, .. } => stream.next_chunk().await.map_err(BackendError::from),
            Self::Whole(bytes) if bytes.is_empty() => Ok(None),
            Self::Whole(bytes) => Ok(Some(Bytes::from(std::mem::take(bytes)))),
        }
    }

    /// Returns the offset where this response starts.
    ///
    /// Buffered responses cannot resume, so they always start at zero.
    pub(crate) fn resumed_from(&self) -> u64 {
        match self {
            Self::Streamed { resumed_from, .. } | Self::Direct { resumed_from, .. } => {
                *resumed_from
            }
            Self::Whole(_) => 0,
        }
    }

    /// Adds the existing prefix to a resumed download's checksum.
    ///
    /// Streaming downloads verify the complete object, including bytes read
    /// by an earlier attempt. Buffered responses do not resume and ignore the
    /// prefix.
    pub(crate) fn fold_resumed_prefix(&mut self, bytes: &[u8]) -> Result<(), BackendError> {
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
            Self::Whole(_) => Ok(()),
        }
    }
}
