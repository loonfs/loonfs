//! Direct-download negotiation, grants, and verified response streams.

use super::*;

/// A direct download returned in verified, bounded chunks.
///
/// Verification completes only when [`Self::next_chunk`] returns `None`.
/// A caller that stops earlier has received provisional bytes, just as with
/// any streaming read whose digest cannot be known until the end.
pub struct DirectDownloadStream {
    body: payload::PayloadStream,
    expected: ContentRef,
    target: String,
    /// Running checksum for the complete object.
    digest: StreamingChecksum,
    size_bytes: u64,
    /// Requested starting offset. Zero means a complete download.
    resumed_from: u64,
    /// Number of prefix bytes included in checksum verification so far.
    prefix_folded: u64,
    finished: bool,
}

impl DirectDownloadStream {
    /// Adds existing prefix bytes to the checksum for a resumed download.
    ///
    /// The caller must provide bytes in order from the start of the object.
    /// Incorrect prefix bytes cause final checksum verification to fail.
    pub fn fold_resumed_prefix(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
        self.prefix_folded = self.prefix_folded.saturating_add(bytes.len() as u64);
    }

    /// Returns the next response-body chunk, or `None` once the complete
    /// object has passed its declared-length and whole-file digest checks.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.prefix_folded != self.resumed_from {
            return Err(ClientError::Http(format!(
                "a download of `{}` resumed at offset {} was given {} bytes of what it \
                 skipped; verification covers the whole object, so all of them are needed \
                 first",
                self.target, self.resumed_from, self.prefix_folded
            )));
        }
        if self.finished {
            return Ok(None);
        }
        match self.body.next().await {
            Some(Ok(chunk)) => {
                self.size_bytes = self.size_bytes.saturating_add(chunk.len() as u64);
                if self.size_bytes > self.expected.size_bytes {
                    self.finished = true;
                    return Err(ClientError::Protocol(format!(
                        "direct download of `{}` sent more than the {} bytes the grant named",
                        self.target, self.expected.size_bytes
                    )));
                }
                self.digest.update(&chunk);
                Ok(Some(chunk))
            }
            Some(Err(error)) => {
                self.finished = true;
                Err(ClientError::Io(format!(
                    "read of `{}` failed: {error}",
                    self.target
                )))
            }
            None => {
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
                Ok(None)
            }
        }
    }
}

impl Client {
    /// Returns whether this file should use a direct download.
    ///
    /// Files within the proxy limit use the simpler proxied path. Larger files
    /// use direct object-store access when the deployment advertises it.
    ///
    /// If the server advertises no proxy limit, this keeps the proxied path.
    pub async fn offers_direct_download(&self, size_bytes: u64) -> Result<bool> {
        let capabilities = self.capabilities().await?;
        Ok(capabilities.supports(FEATURE_DOWNLOADS_DIRECT_GET)
            && capabilities
                .limits
                .get(LIMIT_DOWNLOAD_MAX_CONTENT_BYTES)
                .is_some_and(|proxy_cap| size_bytes > *proxy_cap))
    }

    /// Requests short-lived direct access to a file's content object.
    pub async fn begin_download(
        &self,
        spec: &NamespacePath,
        revision_no: Option<RevisionNo>,
    ) -> Result<BeginDownloadResponse> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/downloads",
            self.base_url,
            spec.namespace().as_str()
        );
        let request = match revision_no {
            Some(revision_no) => {
                BeginDownloadRequest::for_revision(spec.absolute_path().clone(), revision_no)
            }
            None => BeginDownloadRequest::for_path(spec.absolute_path().clone()),
        };
        // A grant creates nothing and names nothing new, so asking twice
        // costs two URLs and changes no state: this one may be resent.
        self.request_json::<_, BeginDownloadResponse>(self.post(&url), Some(&request))
            .await
    }

    /// Requests direct access to one retained inode revision.
    pub async fn begin_download_by_inode(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<BeginDownloadByInodeResponse> {
        let inode_id = loonfs_api::public_inode_id::encode(inode_id);
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
            self.base_url
        );
        self.request_json::<_, BeginDownloadByInodeResponse>(
            self.post(&url),
            Some(&BeginDownloadByInodeRequest {}),
        )
        .await
    }

    /// Opens a download grant from the start of the object.
    pub async fn open_direct_download(
        &self,
        download: &BeginDownloadResponse,
    ) -> Result<DirectDownloadStream> {
        self.open_direct_download_at(download, 0).await
    }

    /// Opens a download grant at `start_offset`.
    ///
    /// The offset is sent in an unsigned `Range` header, so the same grant can
    /// serve a complete or resumed download. For a nonzero offset, call
    /// [`DirectDownloadStream::fold_resumed_prefix`] before reading so the
    /// final checksum covers the complete object.
    pub async fn open_direct_download_at(
        &self,
        download: &BeginDownloadResponse,
        start_offset: u64,
    ) -> Result<DirectDownloadStream> {
        self.open_direct_download_target(
            &download.access,
            &download.content_ref,
            download.path.to_string(),
            start_offset,
        )
        .await
    }

    /// Opens an inode download from the start of the object.
    pub async fn open_direct_download_by_inode(
        &self,
        download: &BeginDownloadByInodeResponse,
    ) -> Result<DirectDownloadStream> {
        self.open_direct_download_by_inode_at(download, 0).await
    }

    /// Opens an inode download from `start_offset`.
    pub async fn open_direct_download_by_inode_at(
        &self,
        download: &BeginDownloadByInodeResponse,
        start_offset: u64,
    ) -> Result<DirectDownloadStream> {
        self.open_direct_download_target(
            &download.access,
            &download.content_ref,
            format!(
                "inode {} revision {}",
                loonfs_api::public_inode_id::encode(download.inode_id),
                download.revision_no
            ),
            start_offset,
        )
        .await
    }

    async fn open_direct_download_target(
        &self,
        access: &ObjectTransferAccess,
        content_ref: &ContentRef,
        target: String,
        start_offset: u64,
    ) -> Result<DirectDownloadStream> {
        let ObjectTransferAccess::PresignedUrl {
            method,
            url,
            headers,
            ..
        } = access;
        if method != "GET" {
            return Err(ClientError::Protocol(format!(
                "unsupported presigned download method `{method}`"
            )));
        }
        if start_offset > content_ref.size_bytes {
            return Err(ClientError::Http(format!(
                "cannot resume a download of `{target}` at offset {start_offset} of {} bytes",
                content_ref.size_bytes
            )));
        }
        let mut request = WireRequest::presigned(reqwest::Method::GET, url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if start_offset > 0 {
            request = request.header("range", format!("bytes={start_offset}-"));
        }
        let body = self.call_for_response_stream(&request).await?;
        Ok(DirectDownloadStream {
            body,
            expected: content_ref.clone(),
            target,
            digest: StreamingChecksum::for_algorithm(content_ref.checksum.algorithm),
            // The counter measures the whole object, not this response, so
            // the length check at the end lands where it always did.
            size_bytes: start_offset,
            resumed_from: start_offset,
            prefix_folded: 0,
            finished: false,
        })
    }

    /// Streams a granted object's bytes into `sink`, checking them against
    /// the reference the grant carried, and reports how many arrived.
    ///
    /// The payload is never held: each chunk is hashed and written as it
    /// arrives, so this costs one chunk of memory whatever the object's
    /// length. That is the entire reason the grant exists — a file past the
    /// deployment's proxy cap has no other way home.
    ///
    /// Verification is what keeps a direct read no weaker than a proxied
    /// one: length and the reference's complete-payload checksum are checked
    /// for every supported algorithm.
    ///
    /// A failure is reported *after* the sink has already received bytes,
    /// because that is the only order a streamed read allows. Callers must
    /// treat the sink as provisional until this returns: write to a
    /// temporary and install it on success.
    pub async fn download_via_presigned_url<W>(
        &self,
        download: &BeginDownloadResponse,
        sink: &mut W,
    ) -> Result<u64>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::AsyncWriteExt as _;
        let path = &download.path;
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

#[cfg(test)]
mod tests;
