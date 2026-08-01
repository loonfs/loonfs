//! Where a `put` reads its bytes, and how each transport is handed them.
//!
//! One description of the payload serves all three transports. It has to be
//! opened once per transport rather than shared, because the two runtimes
//! spell a stream differently — the embedded runtime in the object store's
//! terms, the client in its own — but what is read, and how much of it is
//! held at a time, is the same either way.

use crate::backend_error::BackendError;
use crate::error::CliError;
use crate::progress::ProgressReporter;
use futures::stream::StreamExt;
use loonfs::{ByteStream, ObjectStoreError};
use loonfs_client::{PayloadSource, STREAMING_PUT_MIN_BYTES};
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

    /// The local file an interrupted upload of this payload could pick up
    /// from, when there is one.
    ///
    /// Two things have to hold. The payload must travel in parts — a
    /// smaller one is a single request, which either happened or did not —
    /// and its source must be openable a second time, because a resumed
    /// upload reads every byte again to fold the checksum the assembly is
    /// verified against. A pipe fails the second on its own.
    pub(crate) fn resumable_source(&self) -> Option<&Path> {
        match self {
            Self::File { path, size_bytes } if *size_bytes >= STREAMING_PUT_MIN_BYTES => Some(path),
            _ => None,
        }
    }

    /// Opens the payload for the in-process runtime.
    pub(crate) async fn open_byte_stream(
        &self,
        progress: &Arc<ProgressReporter>,
    ) -> Result<ByteStream, BackendError> {
        Ok(as_object_store_stream(self.open_source(progress).await?))
    }

    /// Opens the payload for the HTTP client.
    ///
    /// The source counts itself as it is read, which is the one place an
    /// upload's bytes are observable without reaching into a transport: past
    /// here they belong to the runtime that is staging them. That puts the
    /// count slightly ahead of the wire — a direct multipart upload holds a
    /// few parts in flight — so what it reports is payload read, which is
    /// what a caller watching their own file wants to know.
    pub(crate) async fn open_source(
        &self,
        progress: &Arc<ProgressReporter>,
    ) -> Result<PayloadSource, BackendError> {
        let source = match self {
            Self::File { path, .. } => PayloadSource::open_file(path).await.map_err(|error| {
                BackendError::io_error(format!("i/o error for `{}`: {error}", path.display()))
            })?,
            Self::Stdin => PayloadSource::reader(tokio::io::stdin()),
        };
        Ok(counted_source(source, Arc::clone(progress)))
    }
}

/// Wraps a source so every byte read off it is counted, and the end of the
/// payload announces the phase that follows it.
///
/// The commit is the part a finished upload waits on with no bytes moving,
/// so the transition is reported where it actually happens: when the source
/// runs dry.
fn counted_source(source: PayloadSource, progress: Arc<ProgressReporter>) -> PayloadSource {
    if !progress.enabled() {
        return source;
    }
    let (stream, size_bytes) = source.into_stream();
    let counted =
        futures::stream::unfold((stream, progress), |(mut stream, progress)| async move {
            match stream.next().await {
                Some(Ok(chunk)) => {
                    progress.advance(chunk.len() as u64);
                    Some((Ok(chunk), (stream, progress)))
                }
                Some(Err(error)) => Some((Err(error), (stream, progress))),
                None => {
                    progress.phase("committing");
                    None
                }
            }
        })
        .boxed();
    match size_bytes {
        Some(size_bytes) => PayloadSource::sized_stream(counted, size_bytes),
        None => PayloadSource::stream(counted),
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
