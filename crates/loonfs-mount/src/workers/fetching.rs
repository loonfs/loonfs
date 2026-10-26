//! Fetches into cache files: whole reads, ranged reads, and grants whose
//! streams a read handle keeps between its windows, retried while the
//! server or the network is unavailable.

use super::{Fetched, WorkerContext};
use crate::cache_file::CacheFile;
use crate::clock;
use crate::error::{MountError, Result};
use crate::fetch::{FetchJob, FetchTransport};
use bytes::Bytes;
use futures::StreamExt as _;
use loonfs_client::{ClientError, DirectDownloadStream, DownloadOptions, ReadRange};
use loonfs_types::{ContentId, ErrorCode};
use std::num::NonZeroU64;

/// How long a fetch retries a server or network that's unavailable before
/// the reads that wait for it fail.
const FETCH_DEADLINE_MS: u64 = 30_000;
/// The first wait before a fetch is retried, which doubles on each retry.
const FIRST_FETCH_BACKOFF_MS: u64 = 250;
/// The longest wait between two attempts of a fetch.
const MAX_FETCH_BACKOFF_MS: u64 = 4_000;

/// A grant's stream that a read handle keeps between its windows.
pub(super) struct ParkedStream {
    content_id: ContentId,
    next_offset: u64,
    stream: DirectDownloadStream,
}

impl WorkerContext {
    /// Fetches a range, and retries a failure that a later attempt can fix
    /// until 30 seconds pass, which bounds the attempts to about a dozen.
    pub(super) async fn fetch_with_retry(&self, job: &FetchJob) -> Result<Fetched> {
        let started_at_ms = self.timer.monotonic_now_ms();
        let mut backoff_ms = FIRST_FETCH_BACKOFF_MS;
        loop {
            let result = self.fetch(job.clone()).await;
            let elapsed_ms = self.timer.monotonic_now_ms().saturating_sub(started_at_ms);
            match result {
                Err(error)
                    if is_transient(&error)
                        && elapsed_ms.saturating_add(backoff_ms) < FETCH_DEADLINE_MS =>
                {
                    tracing::warn!(%error, backoff_ms, "a fetch failed, and the engine tries it again");
                    clock::delay(backoff_ms).await;
                    backoff_ms = backoff_ms.saturating_mul(2).min(MAX_FETCH_BACKOFF_MS);
                }
                result => return result,
            }
        }
    }

    async fn fetch(&self, job: FetchJob) -> Result<Fetched> {
        match job.transport {
            FetchTransport::WholeFile => {
                let mut body = self
                    .client
                    .read_file_revision_stream_by_inode(
                        &self.namespace_id,
                        job.inode_id,
                        job.revision_no,
                    )
                    .await?;
                let mut offset = job.range.start;
                while let Some(chunk) = body.next().await {
                    let chunk = chunk.map_err(|error| ClientError::Io(error.to_string()))?;
                    write_chunk(&job.file, offset, &chunk)?;
                    offset += chunk.len() as u64;
                }
                if offset != job.range.end {
                    return Err(MountError::from(ClientError::Protocol(format!(
                        "a read of revision {} of inode {} returned {offset} bytes instead of {}",
                        job.revision_no, job.inode_id, job.range.end
                    ))));
                }
                Ok(Fetched { written: job.range })
            }
            FetchTransport::Ranged => {
                let length = NonZeroU64::new(job.range.end - job.range.start)
                    .expect("a fetch should have bytes");
                let range = self
                    .client
                    .read_file_revision_range_stream_by_inode(
                        &self.namespace_id,
                        job.inode_id,
                        job.revision_no,
                        ReadRange {
                            start_offset: job.range.start,
                            length: Some(length),
                        },
                    )
                    .await?;
                let mut body = range.body;
                let mut offset = range.start_offset;
                while let Some(chunk) = body.next().await {
                    let chunk = chunk.map_err(|error| ClientError::Io(error.to_string()))?;
                    write_chunk(&job.file, offset, &chunk)?;
                    offset += chunk.len() as u64;
                }
                Ok(Fetched {
                    written: range.start_offset..offset,
                })
            }
            FetchTransport::Grant => self.fetch_granted(job).await,
        }
    }

    async fn fetch_granted(&self, job: FetchJob) -> Result<Fetched> {
        let parked = job.stream_owner.and_then(|handle| {
            self.streams
                .lock()
                .expect("the stream lock should not be poisoned")
                .remove(&handle)
        });
        let (mut stream, mut kept) = match parked {
            Some(parked)
                if parked.content_id == job.content_id && parked.next_offset == job.range.start =>
            {
                (parked.stream, true)
            }
            _ => (self.open_grant(&job).await?, false),
        };
        // The grant covers the rest of the revision. The stream requests no
        // range that starts at or past the window's end, so the only bytes it
        // reads past the window are the rest of the range that the window
        // ends in.
        stream.set_end_offset(job.range.end);
        let mut offset = job.range.start;
        let mut ended = false;
        while offset < job.range.end {
            let chunk = match stream.next_chunk().await {
                Ok(chunk) => chunk,
                // A kept stream's response can time out while its reader
                // pauses, so a kept stream that fails before its first byte
                // is replaced by a new grant.
                Err(_) if kept => {
                    kept = false;
                    stream = self.open_grant(&job).await?;
                    stream.set_end_offset(job.range.end);
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            kept = false;
            let Some(chunk) = chunk else {
                ended = true;
                break;
            };
            write_chunk(&job.file, offset, &chunk)?;
            offset += chunk.len() as u64;
        }
        if let (Some(handle), false) = (job.stream_owner, ended) {
            self.streams
                .lock()
                .expect("the stream lock should not be poisoned")
                .insert(
                    handle,
                    ParkedStream {
                        content_id: job.content_id.clone(),
                        next_offset: offset,
                        stream,
                    },
                );
        }
        Ok(Fetched {
            written: job.range.start..offset,
        })
    }

    async fn open_grant(&self, job: &FetchJob) -> Result<DirectDownloadStream> {
        let grant = self
            .client
            .create_revision_download_by_inode_with_options(
                &self.namespace_id,
                job.inode_id,
                job.revision_no,
                &DownloadOptions {
                    start_offset: job.range.start,
                    ..DownloadOptions::default()
                },
            )
            .await?;
        Ok(self
            .client
            .open_unverified_direct_download_by_inode(&grant)
            .await?)
    }
}

/// Returns whether a later attempt can succeed where a fetch failed: a
/// network failure, or a server that's busy or shutting down.
fn is_transient(error: &MountError) -> bool {
    let MountError::Client(error) = error else {
        return false;
    };
    match error.as_ref() {
        ClientError::Http(_) | ClientError::Io(_) => true,
        error => error
            .code()
            .is_some_and(ErrorCode::retryable_without_operator_action),
    }
}

fn write_chunk(file: &CacheFile, offset: u64, chunk: &Bytes) -> Result<()> {
    file.write(offset, chunk).map_err(|error| MountError::Io {
        path: "cache".to_owned(),
        message: error.to_string(),
    })
}
