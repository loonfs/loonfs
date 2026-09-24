//! Payload sources shared by embedded and remote `put` operations.

use crate::error::CliError;
use crate::progress::ProgressReporter;
use futures::stream::StreamExt;
use loonfs::{ByteStream, ObjectStoreError};
use loonfs_client::PayloadSource;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Standard input's spelling on the command line.
pub(crate) const STDIN_PATH: &str = "-";

/// Where one `put` reads its payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocalPayload {
    /// A file with a known length.
    File { path: PathBuf, size_bytes: u64 },
    /// Standard input, whose length is unknown.
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

    /// Returns a file that may be reopened after an interrupted PUT.
    pub(crate) fn file_path(&self) -> Option<&Path> {
        match self {
            Self::File { path, .. } => Some(path),
            _ => None,
        }
    }

    /// Opens the payload for the in-process runtime.
    pub(crate) async fn open_byte_stream(
        &self,
        progress: &Arc<ProgressReporter>,
    ) -> Result<ByteStream, CliError> {
        Ok(as_object_store_stream(self.open_source(progress).await?))
    }

    /// Opens the payload for the HTTP client.
    ///
    /// Progress measures bytes read from the source and may be slightly ahead
    /// of bytes sent when multipart uploads have parts in flight.
    pub(crate) async fn open_source(
        &self,
        progress: &Arc<ProgressReporter>,
    ) -> Result<PayloadSource, CliError> {
        let source = match self {
            Self::File { path, .. } => PayloadSource::open_file(path).await.map_err(|error| {
                CliError::io_error(format!("i/o error for `{}`: {error}", path.display()))
            })?,
            Self::Stdin => PayloadSource::reader(tokio::io::stdin()),
        };
        Ok(counted_source(source, Arc::clone(progress)))
    }
}

/// Counts bytes read from a source and reports the commit phase at EOF.
fn counted_source(source: PayloadSource, progress: Arc<ProgressReporter>) -> PayloadSource {
    if !progress.enabled() {
        return source;
    }
    source.map_stream(|stream| {
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
        .boxed()
    })
}

/// Converts a client payload source into an object-store byte stream.
fn as_object_store_stream(source: PayloadSource) -> ByteStream {
    let (stream, _) = source.into_stream();
    stream
        .map(|chunk| {
            chunk.map_err(|error| ObjectStoreError::transport("upload body", error.to_string()))
        })
        .boxed()
}
