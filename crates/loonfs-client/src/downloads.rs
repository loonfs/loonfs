//! Direct-download negotiation, grants, and verified response streams.

use super::*;
use crate::operations;
use crate::transport::SendPolicy;
use futures::future::BoxFuture;
use futures::stream::Fuse;
use futures::FutureExt as _;
use loonfs_types::api::v0::{CreateDownloadByInodeRequest, DownloadRange};
use loonfs_types::ContentVerifier;
use std::collections::VecDeque;
use std::task::{Context, Poll};

/// The most range responses a download holds open at once: the one being
/// read and the ones requested behind it, in range order.
const RANGE_RESPONSES_IN_FLIGHT: usize = 4;

/// Options for a download of a file's content by path or by inode.
///
/// `snapshot_id` has no field in the runtime's `DownloadOptions`, which
/// reads a snapshot through a read view.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DownloadOptions {
    /// Download the file revision captured by this snapshot instead of the
    /// current one.
    pub snapshot_id: Option<PinId>,
    /// The first byte the grant reads, for a caller that already holds the
    /// bytes below it. A grant names `[start_offset, size_bytes)` and
    /// nothing else, so a resume asks for a new grant. An offset at or past
    /// the end answers `invalid_request`; a revision of zero bytes takes
    /// only 0.
    pub start_offset: u64,
}

/// A direct download returned in verified, bounded chunks.
///
/// Verification completes only when [`Self::next_chunk`] returns `None`.
/// A caller that stops earlier has received provisional bytes, just as with
/// any streaming read whose digest cannot be known until the end.
///
/// The ranges after the current one are requested while it is read, up to a
/// fixed window of open responses.
pub struct DirectDownloadStream {
    body: Fuse<payload::PayloadStream>,
    next: VecDeque<NextRange>,
    client: Client,
    ranges: VecDeque<DownloadRange>,
    range_remaining: u64,
    expected: ContentRef,
    target: String,
    verifier: ContentVerifier,
    /// The first error a read returned. The stream is over once it fails,
    /// so every later read returns it again.
    failed: Option<ClientError>,
}

enum NextRange {
    Opening {
        length: u64,
        open: BoxFuture<'static, Result<payload::PayloadStream>>,
    },
    Open {
        length: u64,
        body: payload::PayloadStream,
    },
    Failed(ClientError),
}

impl DirectDownloadStream {
    /// Returns the immutable content claim this stream verifies.
    pub fn content_ref(&self) -> &ContentRef {
        &self.expected
    }

    /// Adds existing prefix bytes to the checksum for a resumed download.
    ///
    /// The caller must provide bytes in order from the start of the revision,
    /// before the first [`Self::next_chunk`]. A prefix that runs past the
    /// start offset, or that arrives after the stream has fetched content or
    /// reached its end, is refused. Incorrect prefix bytes cause final
    /// checksum verification to fail.
    pub fn fold_resumed_prefix(&mut self, bytes: &[u8]) -> Result<()> {
        self.verifier.fold_resumed_prefix(bytes).map_err(|refused| {
            ClientError::Http(format!("download of `{}`: {refused}", self.target))
        })
    }

    /// Returns the next response-body chunk, or `None` once the complete
    /// revision has passed its length and checksum checks.
    ///
    /// After a read fails, whether a range open, a body read, or a check, every
    /// later call returns the same error.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        self.verifier
            .ensure_resumed_prefix_complete()
            .map_err(|incomplete| {
                ClientError::Http(format!("download of `{}`: {incomplete}", self.target))
            })?;
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let read = self.read_next_chunk().await;
        if let Err(error) = &read {
            self.failed = Some(error.clone());
        }
        read
    }

    async fn read_next_chunk(&mut self) -> Result<Option<Bytes>> {
        loop {
            let polled = tokio::select! {
                biased;
                () = std::future::poll_fn(|context| poll_opens(&mut self.next, context)) => continue,
                polled = self.body.next() => polled,
            };
            match polled {
                Some(Ok(chunk)) => {
                    let length = chunk.len() as u64;
                    if length > self.range_remaining {
                        return Err(ClientError::Protocol(format!(
                            "direct read of `{}` exceeded the range length the grant named",
                            self.target
                        )));
                    }
                    self.range_remaining -= length;
                    self.verifier.update(&chunk).map_err(|after_end| {
                        ClientError::Protocol(format!(
                            "direct read of `{}`: {after_end}",
                            self.target
                        ))
                    })?;
                    return Ok(Some(chunk));
                }
                Some(Err(error)) => {
                    return Err(ClientError::Io(format!(
                        "read of `{}` failed: {error}",
                        self.target
                    )));
                }
                None => {
                    if self.range_remaining != 0 {
                        return Err(ClientError::Protocol(format!(
                            "direct read of `{}` lacks {} bytes from the range the grant named",
                            self.target, self.range_remaining
                        )));
                    }
                    if let Some(next) = self.next.pop_front() {
                        let (length, body) = match next {
                            NextRange::Opening { length, open } => (length, open.await?),
                            NextRange::Open { length, body } => (length, body),
                            NextRange::Failed(error) => return Err(error),
                        };
                        self.body = body.fuse();
                        self.range_remaining = length;
                        self.request_next_range();
                        continue;
                    }
                    return self.verifier.finish().map(|()| None).map_err(|mismatch| {
                        ClientError::Protocol(format!(
                            "direct download of `{}` is not what the grant named: {mismatch}",
                            self.target
                        ))
                    });
                }
            }
        }
    }

    fn request_next_range(&mut self) {
        while self.next.len() < RANGE_RESPONSES_IN_FLIGHT - 1 {
            let Some(range) = self.ranges.pop_front() else {
                return;
            };
            let client = self.client.clone();
            self.next.push_back(NextRange::Opening {
                length: range.length,
                open: Box::pin(async move { client.open_download_range(&range).await }),
            });
        }
    }
}

/// Polls every pending open once and resolves when any of them completed.
fn poll_opens(next: &mut VecDeque<NextRange>, context: &mut Context<'_>) -> Poll<()> {
    let mut changed = false;
    for entry in next.iter_mut() {
        let NextRange::Opening { length, open } = entry else {
            continue;
        };
        let Poll::Ready(opened) = open.poll_unpin(context) else {
            continue;
        };
        let length = *length;
        *entry = match opened {
            Ok(body) => NextRange::Open { length, body },
            Err(error) => NextRange::Failed(error),
        };
        changed = true;
    }
    if changed {
        Poll::Ready(())
    } else {
        Poll::Pending
    }
}

impl Client {
    /// Returns whether the deployment offers direct object-store downloads.
    ///
    /// Selection does not depend on the size of a different, current revision.
    pub async fn offers_direct_download(&self) -> Result<bool> {
        Ok(self
            .get_capabilities()
            .await?
            .supports(FEATURE_DOWNLOADS_DIRECT_GET))
    }

    /// Requests short-lived direct access to a file's current content.
    pub async fn create_download(&self, spec: &NamespacePath) -> Result<CreateDownloadResponse> {
        self.create_download_with_options(spec, &DownloadOptions::default())
            .await
    }

    /// Requests short-lived direct access to a file's content: the current
    /// revision, or the revision a snapshot captured when the options name
    /// one, from the offset the options name.
    pub async fn create_download_with_options(
        &self,
        spec: &NamespacePath,
        options: &DownloadOptions,
    ) -> Result<CreateDownloadResponse> {
        self.request_download(spec, None, options).await
    }

    /// Requests short-lived direct access to one retained revision of a file.
    pub async fn create_revision_download(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<CreateDownloadResponse> {
        self.create_revision_download_with_options(spec, revision_no, &DownloadOptions::default())
            .await
    }

    /// Requests short-lived direct access to one retained revision of a file,
    /// from the offset the options name.
    ///
    /// A revision cannot be combined with a snapshot.
    pub async fn create_revision_download_with_options(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
        options: &DownloadOptions,
    ) -> Result<CreateDownloadResponse> {
        self.request_download(spec, Some(revision_no), options)
            .await
    }

    async fn request_download(
        &self,
        spec: &NamespacePath,
        revision_no: Option<RevisionNo>,
        options: &DownloadOptions,
    ) -> Result<CreateDownloadResponse> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/downloads",
            self.base_url,
            spec.namespace().as_str()
        );
        let request = CreateDownloadRequest {
            path: spec.absolute_path().clone(),
            revision_no,
            snapshot_id: options.snapshot_id.clone(),
            start_offset: options.start_offset,
        };
        // Repeating the request writes only the same immutable extent bytes.
        self.request_json::<_, CreateDownloadResponse>(
            self.post(&url),
            Some(&request),
            operations::CREATE_DOWNLOAD,
        )
        .await
    }

    /// Requests direct access to the current content of a visible file
    /// inode, wherever it is bound.
    pub async fn create_download_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
    ) -> Result<CreateDownloadByInodeResponse> {
        self.create_download_by_inode_with_options(
            namespace_id,
            inode_id,
            &DownloadOptions::default(),
        )
        .await
    }

    /// Requests direct access to the content of a visible file inode,
    /// wherever it is bound: the current revision, or the revision a snapshot
    /// captured when the options name one, from the offset the options name.
    pub async fn create_download_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        options: &DownloadOptions,
    ) -> Result<CreateDownloadByInodeResponse> {
        let inode_id = loonfs_types::public_inode_id::encode(inode_id);
        self.request_download_by_inode(
            format!(
                "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/downloads",
                self.base_url
            ),
            options,
            operations::CREATE_DOWNLOAD_BY_INODE,
        )
        .await
    }

    /// Requests direct access to one retained inode revision, without
    /// requiring a current path.
    pub async fn create_revision_download_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<CreateDownloadByInodeResponse> {
        self.create_revision_download_by_inode_with_options(
            namespace_id,
            inode_id,
            revision_no,
            &DownloadOptions::default(),
        )
        .await
    }

    /// Requests direct access to one retained inode revision, without requiring
    /// a current path, from the offset the options name.
    ///
    /// A revision cannot be combined with a snapshot.
    pub async fn create_revision_download_by_inode_with_options(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
        options: &DownloadOptions,
    ) -> Result<CreateDownloadByInodeResponse> {
        let inode_id = loonfs_types::public_inode_id::encode(inode_id);
        self.request_download_by_inode(
            format!(
                "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
                self.base_url
            ),
            options,
            operations::CREATE_REVISION_DOWNLOAD_BY_INODE,
        )
        .await
    }

    async fn request_download_by_inode(
        &self,
        url: String,
        options: &DownloadOptions,
        policy: SendPolicy,
    ) -> Result<CreateDownloadByInodeResponse> {
        let request = CreateDownloadByInodeRequest {
            snapshot_id: options.snapshot_id.clone(),
            start_offset: options.start_offset,
        };
        self.request_json::<_, CreateDownloadByInodeResponse>(
            self.post(&url),
            Some(&request),
            policy,
        )
        .await
    }

    /// Opens a download grant, sending the headers it carries unchanged.
    ///
    /// The grant names `[start_offset, size_bytes)` of the revision and the
    /// stream starts where it does. For a grant from a nonzero offset, call
    /// [`DirectDownloadStream::fold_resumed_prefix`] with the bytes below it
    /// before reading, so the final checksum covers the complete revision.
    pub async fn open_direct_download(
        &self,
        download: &CreateDownloadResponse,
    ) -> Result<DirectDownloadStream> {
        self.open_direct_download_target(
            &download.ranges,
            &download.content_ref,
            download.path.to_string(),
        )
        .await
    }

    /// Opens an inode download grant.
    ///
    /// See [`Self::open_direct_download`].
    pub async fn open_direct_download_by_inode(
        &self,
        download: &CreateDownloadByInodeResponse,
    ) -> Result<DirectDownloadStream> {
        self.open_direct_download_target(
            &download.ranges,
            &download.content_ref,
            format!(
                "inode {} revision {}",
                loonfs_types::public_inode_id::encode(download.inode_id),
                download.revision_no
            ),
        )
        .await
    }

    async fn open_direct_download_target(
        &self,
        ranges: &[DownloadRange],
        content_ref: &ContentRef,
        target: String,
    ) -> Result<DirectDownloadStream> {
        let start_offset = granted_start(ranges, content_ref, &target)?;
        let (first, remaining) = ranges
            .split_first()
            .expect("validated grant should contain a range");
        let body = self.open_download_range(first).await?;
        let mut stream = DirectDownloadStream {
            body: body.fuse(),
            next: VecDeque::new(),
            client: self.clone(),
            ranges: remaining.iter().cloned().collect(),
            range_remaining: first.length,
            expected: content_ref.clone(),
            target,
            verifier: ContentVerifier::new(content_ref, start_offset),
            failed: None,
        };
        stream.request_next_range();
        Ok(stream)
    }

    async fn open_download_range(&self, range: &DownloadRange) -> Result<payload::PayloadStream> {
        if range.length == 0 {
            return Ok(futures::stream::empty().boxed());
        }
        let ObjectTransferAccess::PresignedUrl { url, headers, .. } = &range.access;
        let mut request = WireRequest::presigned(http::Method::GET, url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        self.call_for_response_stream(&request).await
    }

    /// Streams the granted ranges into `sink` and verifies the complete revision.
    ///
    /// Requires a grant from offset 0. Resume with [`Self::open_direct_download`]
    /// and [`DirectDownloadStream::fold_resumed_prefix`] instead.
    /// The bytes are hashed and written as they arrive. Callers must treat the
    /// sink as provisional until this returns successfully.
    pub async fn download_via_presigned_url<W>(
        &self,
        download: &CreateDownloadResponse,
        sink: &mut W,
    ) -> Result<u64>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::AsyncWriteExt as _;
        let path = &download.path;
        let start_offset = granted_start(&download.ranges, &download.content_ref, path.as_str())?;
        if start_offset != 0 {
            return Err(ClientError::Protocol(format!(
                "the grant for `{path}` starts at offset {start_offset}; \
                 `download_via_presigned_url` takes a grant from offset 0, so resume with \
                 `open_direct_download`"
            )));
        }
        let mut download = self.open_direct_download(download).await?;
        let mut size_bytes = 0u64;
        while let Some(chunk) = download.next_chunk().await? {
            size_bytes += chunk.len() as u64;
            sink.write_all(&chunk)
                .await
                .map_err(|err| ClientError::Io(format!("write of `{path}` failed: {err}")))?;
        }
        sink.flush()
            .await
            .map_err(|err| ClientError::Io(format!("write of `{path}` failed: {err}")))?;
        Ok(size_bytes)
    }
}

fn granted_start(ranges: &[DownloadRange], content_ref: &ContentRef, target: &str) -> Result<u64> {
    let invalid = || ClientError::Protocol(format!("the grant for `{target}` has invalid ranges"));
    let first = ranges.first().ok_or_else(invalid)?;
    let start = first.start_offset;
    if (content_ref.size_bytes == 0 && (start != 0 || ranges.len() != 1))
        || (content_ref.size_bytes != 0 && start >= content_ref.size_bytes)
    {
        return Err(invalid());
    }
    let mut offset = start;
    for range in ranges {
        let ObjectTransferAccess::PresignedUrl { method, .. } = &range.access;
        if method != "GET" {
            return Err(ClientError::Protocol(format!(
                "unsupported presigned download method `{method}`"
            )));
        }
        if range.start_offset != offset || (range.length == 0 && content_ref.size_bytes != 0) {
            return Err(invalid());
        }
        offset = offset.checked_add(range.length).ok_or_else(invalid)?;
    }
    if offset != content_ref.size_bytes {
        return Err(invalid());
    }
    Ok(start)
}

#[cfg(test)]
mod tests;
