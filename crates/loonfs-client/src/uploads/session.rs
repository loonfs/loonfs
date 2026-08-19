//! Upload-session endpoints and presigned transfer requests.

use super::super::*;
use super::staging::presigned_put_request;

impl Client {
    /// Starts an upload session using the transport selected by the request.
    pub async fn begin_upload(
        &self,
        namespace_id: &NamespaceId,
        request: &BeginUploadRequest,
    ) -> Result<BeginUploadResponse> {
        let url = format!("{}/v0/namespaces/{namespace_id}/uploads", self.base_url);
        // Do not retry automatically because each request creates a session.
        self.request_json_once::<_, BeginUploadResponse>(self.post(&url), Some(request))
            .await
    }

    /// Starts a direct upload of bytes the caller already has.
    ///
    /// The claim describes the caller's bytes. The returned content reference
    /// identifies the object used by completion and the later commit.
    pub async fn begin_direct_put(
        &self,
        namespace_id: &NamespaceId,
        claim: UploadContentClaim,
    ) -> Result<BeginUploadResponse> {
        self.begin_upload(
            namespace_id,
            &BeginUploadRequest::DirectPut { content: claim },
        )
        .await
    }

    /// Opens a direct multipart upload session.
    ///
    /// The request does not need a payload length or checksum. The server
    /// returns the part size and checksum algorithm; provider details remain
    /// private and the content reference is returned at completion.
    pub async fn begin_direct_multipart(
        &self,
        namespace_id: &NamespaceId,
        options: DirectMultipartUploadOptions,
    ) -> Result<BeginUploadResponse> {
        self.begin_upload(
            namespace_id,
            &BeginUploadRequest::DirectMultipart {
                multipart: Some(options),
            },
        )
        .await
    }

    /// Requests upload authorization for a batch of parts.
    ///
    /// Requesting authorization again is safe. Uploading the same part number
    /// replaces that part at the provider.
    pub async fn sign_upload_parts(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        parts: Vec<UploadPartChecksumClaim>,
    ) -> Result<SignUploadPartsResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/parts",
            self.base_url
        );
        // Signing does not change upload state, so this request may be retried.
        self.request_json::<_, SignUploadPartsResponse>(
            self.post(&url),
            Some(&SignUploadPartsRequest { parts }),
        )
        .await
    }

    /// Uploads one part and reports what the provider recorded for it.
    ///
    /// The etag comes back to the caller rather than to the server: parts
    /// are the uploader's bookkeeping all the way to completion, exactly as
    /// they are in the provider's own multipart API.
    pub async fn upload_part_via_presigned_url(
        &self,
        part_number: u32,
        access: &ObjectTransferAccess,
        checksum: Checksum,
        bytes: Bytes,
    ) -> Result<CompletedUploadPart> {
        let ObjectTransferAccess::PresignedUrl {
            method,
            url,
            headers,
            ..
        } = access;
        if method != "PUT" {
            return Err(ClientError::Protocol(format!(
                "unsupported presigned part method `{method}`"
            )));
        }
        let mut request = WireRequest::presigned(reqwest::Method::PUT, url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        // A part upload is safe to repeat: it is not create-only, the
        // provider takes the last write, and the checksum rides the
        // signature either way.
        let response = self
            .call_content_with_transport_retry_headers(&request, Some(&bytes))
            .await?;
        let etag = response
            .get(http::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| {
                ClientError::Protocol(format!("part {part_number} upload returned no etag"))
            })?
            .to_owned();
        Ok(CompletedUploadPart {
            part_number,
            etag,
            checksum,
        })
    }

    /// Uploads in-memory bytes directly to object storage using a presigned URL.
    pub async fn upload_via_presigned_url(
        &self,
        access: &ObjectTransferAccess,
        bytes: &[u8],
    ) -> Result<()> {
        let request = presigned_put_request(access)?;
        // A successful create-only PUT may replay as a provider precondition error, not success.
        self.call_once(&request, Some(&Bytes::copy_from_slice(bytes)))
            .await
            .map(|_| ())
    }

    /// Writes one whole object to a presigned URL from a source read in
    /// pieces, so the payload crosses the network without ever being held.
    ///
    /// Like the buffered form this never retries: a create-only PUT that
    /// succeeded can come back as a provider precondition error on a second
    /// attempt, and a source is consumed by the attempt that reads it.
    pub async fn upload_streamed_via_presigned_url(
        &self,
        access: &ObjectTransferAccess,
        source: PayloadSource,
    ) -> Result<()> {
        let request = presigned_put_request(access)?;
        let (body, size_bytes) = source.into_stream();
        self.call_streamed_once(&request, body, size_bytes)
            .await
            .map(|_| ())
    }

    /// Uploads in-memory bytes through the server for a service-proxied session.
    pub async fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        let request = self.upload_content_request(namespace_id, upload_id);
        // Proxied uploads are the request most likely to hit the server's
        // concurrency cap; staging the same bytes again is idempotent.
        let response = self
            .call_content_with_transport_retry(&request, Some(&Bytes::copy_from_slice(bytes)))
            .await?;
        serde_json::from_slice(&response).map_err(|err| ClientError::Json(err.to_string()))
    }

    /// Stages a payload that arrives in pieces, forwarding it to the server
    /// as it is read.
    ///
    /// This is [`Self::upload_content`] for a caller that does not hold its
    /// bytes: the payload crosses the client in bounded chunks and the
    /// server hashes it as it forwards it on, so neither side ever holds the
    /// object. A source whose length is unknown is sent with chunked
    /// transfer encoding, and the server's own limit is what bounds it.
    ///
    /// Unlike the buffered call this one never resends: a stream is consumed
    /// by the attempt that reads it, so a failure here is the caller's to
    /// handle with a fresh source.
    pub async fn upload_streamed_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        source: PayloadSource,
    ) -> Result<UploadContentResponse> {
        let request = self.upload_content_request(namespace_id, upload_id);
        let (stream, size_bytes) = source.into_stream();
        let response = self
            .call_streamed_once(&request, stream, size_bytes)
            .await?;
        serde_json::from_slice(&response).map_err(|err| ClientError::Json(err.to_string()))
    }

    fn upload_content_request(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> WireRequest {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
            self.base_url
        );
        self.put(&url)
            .header("content-type", "application/octet-stream")
    }

    /// Ends an open upload session and deletes the object it was writing.
    ///
    /// Repeating it succeeds and reports the abort that stands. This is what
    /// a one-pass upload does when its source fails partway: the session it
    /// opened must not be left holding a half-written object.
    pub async fn abort_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<UploadSessionResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/abort",
            self.base_url
        );
        // Aborting is idempotent: a repeat reports the abort that stands.
        self.request_json::<(), UploadSessionResponse>(self.post(&url), None)
            .await
    }

    /// Returns the current state of an upload session.
    ///
    /// A completed session returns its content reference and a fresh content
    /// token. A caller that kept the upload id can therefore recover from a
    /// lost completion response without uploading the content again.
    pub async fn get_upload_status(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<UploadSessionResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}",
            self.base_url
        );
        self.request_json::<(), UploadSessionResponse>(self.get(&url), None)
            .await
    }

    /// Completes an upload session with a request tagged by its stored mode.
    pub async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<UploadSessionResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete",
            self.base_url
        );
        // The durable completed-session record replays an identical completion without new effect.
        self.request_json::<_, UploadSessionResponse>(self.post(&url), Some(request))
            .await
    }

    /// Completes a direct-multipart upload session.
    pub async fn complete_multipart_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteMultipartUploadRequest,
    ) -> Result<UploadSessionResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete",
            self.base_url
        );
        self.request_json::<_, UploadSessionResponse>(
            self.post(&url),
            Some(&CompleteUploadRequest::DirectMultipart {
                content: request.content.clone(),
                parts: request.parts.clone(),
            }),
        )
        .await
    }
}
