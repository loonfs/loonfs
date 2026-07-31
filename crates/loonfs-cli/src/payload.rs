//! Where a `put` reads its bytes, and how each transport is handed them.
//!
//! One description of the payload serves all three transports. It has to be
//! opened once per transport rather than shared, because the two runtimes
//! spell a stream differently — the embedded runtime in the object store's
//! terms, the client in its own — but what is read, and how much of it is
//! held at a time, is the same either way.

use crate::backend_error::BackendError;
use crate::error::CliError;
use futures::stream::StreamExt;
use loonfs::{ByteStream, ObjectStoreError};
use loonfs_client::{PayloadSource, STREAMING_PUT_MIN_BYTES};
use std::path::{Path, PathBuf};

/// Standard input's spelling on the command line.
pub(crate) const STDIN_PATH: &str = "-";

/// Where one `put` reads its payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocalPayload {
    /// A file on disk, whose length is known before the first byte moves.
    File { path: PathBuf, size_bytes: u64 },
    /// Standard input: bytes of no knowable length, which is why it can
    /// only be uploaded by a path that never needs to know one.
    Stdin,
}

impl LocalPayload {
    /// Describes a local file that is already known to exist.
    pub(crate) fn file(path: impl Into<PathBuf>, size_bytes: u64) -> Self {
        Self::File {
            path: path.into(),
            size_bytes,
        }
    }

    /// The file this payload names, when it is small enough to hold whole.
    ///
    /// This is the one decision about how a put travels. A payload under
    /// the threshold takes the buffered path it always took; anything at or
    /// past it, and anything whose length nobody knows, is read once
    /// instead — and a source with no length can never answer here, which
    /// is exactly right, because it cannot be held.
    pub(crate) fn holdable_file(&self) -> Option<&Path> {
        match self {
            Self::File { path, size_bytes } if *size_bytes < STREAMING_PUT_MIN_BYTES => Some(path),
            _ => None,
        }
    }

    /// Opens the payload for the in-process runtime.
    pub(crate) async fn open_byte_stream(&self) -> Result<ByteStream, BackendError> {
        Ok(as_object_store_stream(self.open_source().await?))
    }

    /// Opens the payload for the HTTP client.
    pub(crate) async fn open_source(&self) -> Result<PayloadSource, BackendError> {
        match self {
            Self::File { path, .. } => PayloadSource::open_file(path).await.map_err(|error| {
                BackendError::io_error(format!("i/o error for `{}`: {error}", path.display()))
            }),
            Self::Stdin => Ok(PayloadSource::reader(tokio::io::stdin())),
        }
    }
}

/// Restates a source in the object store's terms, which is how the embedded
/// runtime's staging path takes a payload.
///
/// A read failure becomes a transport error against the same "upload body"
/// name the server's own streaming path uses, so a truncated local read and
/// a truncated request body read alike.
fn as_object_store_stream(source: PayloadSource) -> ByteStream {
    let (stream, _) = source.into_stream();
    stream
        .map(|chunk| {
            chunk.map_err(|error| ObjectStoreError::transport("upload body", error.to_string()))
        })
        .boxed()
}

/// Bytes of a payload the caller decided to hold whole.
pub(crate) async fn read_whole_file(path: &Path) -> Result<Vec<u8>, CliError> {
    tokio::fs::read(path)
        .await
        .map_err(|error| CliError::io_for_path(path, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The threshold is a property of the payload, not of the transport:
    /// the same file streams or does not regardless of which arm takes it.
    #[test]
    fn only_large_or_unmeasured_payloads_stream() {
        assert!(LocalPayload::file("/tmp/small", 0)
            .holdable_file()
            .is_some());
        assert!(
            LocalPayload::file("/tmp/small", STREAMING_PUT_MIN_BYTES - 1)
                .holdable_file()
                .is_some()
        );
        assert!(LocalPayload::file("/tmp/big", STREAMING_PUT_MIN_BYTES)
            .holdable_file()
            .is_none());
        assert!(
            LocalPayload::Stdin.holdable_file().is_none(),
            "a source of unknown length can never be held"
        );
    }
}
