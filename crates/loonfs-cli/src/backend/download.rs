//! The common download stream shape shared by embedded and remote profiles.

use crate::backend_error::{map_namespace_scoped_runtime_error, BackendError};
use bytes::Bytes;
use loonfs::{FileContentStream, RuntimeError, SharedObjectStore};
use loonfs_api::NamespaceId;
use loonfs_client::DirectDownloadStream;

/// File content returned by either CLI transport.
///
/// Embedded and remote profiles expose different stream types, while small
/// reads may already be buffered. [`FileDownload::next_chunk`] gives command
/// code one common iteration model.
pub(crate) enum FileDownload {
    /// The embedded runtime's bounded stream. Boxed because a stream carries
    /// its object key, reference, and running digest, and the whole arm is a
    /// pointer beside it.
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
    /// Bytes the transport already holds whole.
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

    /// Where this download actually starts, which is not always where it was
    /// asked to: a transport that answers whole has no partial answer to
    /// pick up from and begins at zero however much the caller holds.
    pub(crate) fn resumed_from(&self) -> u64 {
        match self {
            Self::Streamed { resumed_from, .. } | Self::Direct { resumed_from, .. } => {
                *resumed_from
            }
            Self::Whole(_) => 0,
        }
    }

    /// Hands a resumed download the bytes below its start, so its
    /// verification still covers the whole file.
    ///
    /// Both streaming arms check a digest over the complete object, so a
    /// download that skipped a prefix refuses to read until it has been told
    /// what the prefix was. A held answer never resumed, so it has nothing
    /// to be told.
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
