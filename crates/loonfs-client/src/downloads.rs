//! Direct-download negotiation, grants, and verified response streams.

use super::*;
use crate::transport::{QueryBuilder, SendPolicy};
use loonfs_types::api::v0::DownloadRange;

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
pub struct DirectDownloadStream {
    body: payload::PayloadStream,
    client: Client,
    ranges: std::collections::VecDeque<DownloadRange>,
    range_remaining: u64,
    expected: ContentRef,
    target: String,
    /// Running checksum for the complete revision.
    digest: StreamingChecksum,
    size_bytes: u64,
    /// Requested starting offset. Zero means a complete download.
    resumed_from: u64,
    /// Number of prefix bytes included in checksum verification so far.
    prefix_folded: u64,
    finished: bool,
}

impl DirectDownloadStream {
    /// The immutable content claim this stream verifies.
    pub fn content_ref(&self) -> &ContentRef {
        &self.expected
    }

    /// Adds existing prefix bytes to the checksum for a resumed download.
    ///
    /// The caller must provide bytes in order from the start of the revision.
    /// Incorrect prefix bytes cause final checksum verification to fail.
    pub fn fold_resumed_prefix(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
        self.prefix_folded = self.prefix_folded.saturating_add(bytes.len() as u64);
    }

    /// Returns the next response-body chunk, or `None` once the complete
    /// revision has passed its length and checksum checks.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.prefix_folded != self.resumed_from {
            return Err(ClientError::Http(format!(
                "a download of `{}` resumed at offset {} was given {} bytes of what it \
                 skipped; verification covers the whole revision, so all of them are needed \
                 first",
                self.target, self.resumed_from, self.prefix_folded
            )));
        }
        if self.finished {
            return Ok(None);
        }
        loop {
            match self.body.next().await {
                Some(Ok(chunk)) => {
                    let length = chunk.len() as u64;
                    if length > self.range_remaining {
                        self.finished = true;
                        return Err(ClientError::Protocol(format!(
                            "direct read of `{}` exceeded the range length the grant named",
                            self.target
                        )));
                    }
                    self.range_remaining -= length;
                    self.size_bytes += length;
                    self.digest.update(&chunk);
                    return Ok(Some(chunk));
                }
                Some(Err(error)) => {
                    self.finished = true;
                    return Err(ClientError::Io(format!(
                        "read of `{}` failed: {error}",
                        self.target
                    )));
                }
                None => {
                    if self.range_remaining != 0 {
                        self.finished = true;
                        return Err(ClientError::Protocol(format!(
                            "direct read of `{}` lacks {} bytes from the range the grant named",
                            self.target, self.range_remaining
                        )));
                    }
                    if let Some(range) = self.ranges.pop_front() {
                        self.finished = true;
                        self.body = self.client.open_download_range(&range).await?;
                        self.range_remaining = range.length;
                        self.finished = false;
                        continue;
                    }
                    self.finished = true;
                    if self.size_bytes != self.expected.size_bytes {
                        return Err(ClientError::Protocol(format!(
                            "direct download of `{}` ended after {} bytes, not the {} the grant named",
                            self.target, self.size_bytes, self.expected.size_bytes
                        )));
                    }
                    let expected = &self.expected.checksum;
                    // Finalizing consumes the checksum state. `finished` prevents
                    // this branch from running again.
                    let observed = std::mem::replace(
                        &mut self.digest,
                        StreamingChecksum::for_algorithm(expected.algorithm),
                    )
                    .finish();
                    if observed != *expected {
                        return Err(ClientError::Protocol(format!(
                            "direct download of `{}` produced {}:{}, not the {}:{} the grant named",
                            self.target,
                            observed.algorithm,
                            observed.value,
                            expected.algorithm,
                            expected.value
                        )));
                    }
                    return Ok(None);
                }
            }
        }
    }
}

impl Client {
    /// Returns whether the deployment offers direct object-store downloads.
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

    /// Requests short-lived direct access to one retained revision of a
    /// file, from the offset the options name. A revision cannot be
    /// combined with a snapshot.
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
            SendPolicy::Retry,
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

    /// Requests direct access to one retained inode revision, without
    /// requiring a current path, from the offset the options name. A
    /// revision cannot be combined with a snapshot.
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
        )
        .await
    }

    async fn request_download_by_inode(
        &self,
        url: String,
        options: &DownloadOptions,
    ) -> Result<CreateDownloadByInodeResponse> {
        let mut query = QueryBuilder::new(url);
        if let Some(snapshot_id) = &options.snapshot_id {
            query.push("snapshot_id", snapshot_id.as_str());
        }
        if options.start_offset != 0 {
            query.push("start_offset", options.start_offset);
        }
        let url = query.finish();
        self.request_json::<(), CreateDownloadByInodeResponse>(
            self.post(&url),
            None,
            SendPolicy::Retry,
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

    /// Opens an inode download grant. See [`Self::open_direct_download`].
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
        Ok(DirectDownloadStream {
            body,
            client: self.clone(),
            ranges: remaining.iter().cloned().collect(),
            range_remaining: first.length,
            expected: content_ref.clone(),
            target,
            digest: StreamingChecksum::for_algorithm(content_ref.checksum.algorithm),
            size_bytes: start_offset,
            resumed_from: start_offset,
            prefix_folded: 0,
            finished: false,
        })
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
