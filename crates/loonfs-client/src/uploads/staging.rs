//! Upload transport selection and resumable multipart state.

use super::super::*;
use futures::StreamExt as _;
use std::sync::{Arc, Mutex};

/// Minimum payload size for streaming and multipart uploads.
///
/// This matches the provider multipart part size. Smaller payloads would use
/// one part and gain nothing from multipart overhead. Payloads of unknown size
/// also use the streaming path.
pub const STREAMING_PUT_MIN_BYTES: u64 = 8 * 1024 * 1024;

/// Maximum number of multipart payloads held in memory at once.
const DIRECT_MULTIPART_PARTS_IN_FLIGHT: usize = 4;

/// Maximum attempts for one part, including refreshed authorization.
const DIRECT_MULTIPART_PART_ATTEMPTS: usize = 3;

/// State required to resume an interrupted direct multipart upload.
///
/// The server records session geometry but not completed parts, so the
/// client retains the upload id, part size, and accepted part metadata.
/// Missing entries are safely re-uploaded; incorrect entries cause completion
/// to fail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUploadResume {
    /// Upload session ID returned by the server.
    pub upload_id: UploadId,
    /// Required part size. A resumed upload must preserve the original
    /// boundaries so new parts align with completed parts.
    pub part_size_bytes: u64,
    /// Checksum algorithm chosen when the upload session began.
    pub checksum_algorithm: ChecksumAlgorithm,
    /// Metadata for parts that have already been uploaded.
    pub parts: Vec<CompletedUploadPart>,
}

/// Saves direct multipart upload progress so an interrupted upload can resume.
///
/// The client calls these methods synchronously after opening the session and
/// after each successful part upload. Implementations may persist this data
/// before the next network request starts.
pub trait MultipartUploadJournal: Send + Sync {
    /// Records a newly opened session and its required part size and checksum algorithm.
    fn began(
        &self,
        upload_id: &UploadId,
        part_size_bytes: u64,
        checksum_algorithm: ChecksumAlgorithm,
    );
    /// Records a part after it has been uploaded successfully.
    fn part_completed(&self, part: &CompletedUploadPart);
}

/// Optional state for resuming and recording a multipart upload.
#[derive(Clone, Copy, Default)]
pub(crate) struct UploadContinuity<'a> {
    pub(crate) resume: Option<&'a MultipartUploadResume>,
    pub(crate) journal: Option<&'a dyn MultipartUploadJournal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedContent {
    pub(crate) content_ref: ContentRef,
    pub(crate) content_token: Option<ContentToken>,
}

/// Upload path selected from the server capability document.
///
/// Direct PUT and direct multipart are independent capabilities; a
/// deployment may support either one without the other. Proxied upload is
/// used when no suitable direct path is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UploadTransport {
    /// Multipart upload directly to object storage.
    Multipart,
    /// One checksummed request directly to object storage.
    DirectPut,
    /// Stream through the server.
    Proxied,
}

/// No advertised transport accepts the measured payload size.
struct NoTransportFits {
    /// Description of the relevant limits.
    reason: String,
    /// Whether direct PUT is supported.
    direct_put_supported: bool,
}

/// Payload for a direct whole-object upload.
enum DirectPutBody<'a> {
    /// Bytes the caller already holds.
    Held(&'a [u8]),
    /// A payload read in pieces during upload.
    Streamed(PayloadSource),
}

impl DirectPutBody<'_> {
    fn size_hint(&self) -> Option<u64> {
        match self {
            Self::Held(bytes) => Some(bytes.len() as u64),
            Self::Streamed(source) => source.size_bytes(),
        }
    }
}

/// Size and checksum of the bytes sent in one direct PUT.
struct DirectPutObservation {
    size_bytes: u64,
    checksum: Option<StreamingChecksum>,
}

impl DirectPutObservation {
    fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            size_bytes: 0,
            checksum: Some(StreamingChecksum::for_algorithm(algorithm)),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        self.size_bytes += bytes.len() as u64;
        self.checksum
            .as_mut()
            .expect("direct PUT checksum is unfinished")
            .update(bytes);
    }

    fn finish(&mut self) -> UploadContentClaim {
        UploadContentClaim {
            size_bytes: self.size_bytes,
            checksum: self
                .checksum
                .take()
                .expect("direct PUT checksum is finished once")
                .finish(),
        }
    }
}

fn observed_direct_put_source(
    source: PayloadSource,
    algorithm: ChecksumAlgorithm,
) -> (PayloadSource, Arc<Mutex<DirectPutObservation>>) {
    let observation = Arc::new(Mutex::new(DirectPutObservation::new(algorithm)));
    let stream_observation = Arc::clone(&observation);
    let source = source.map_stream(move |stream| {
        stream
            .map(move |chunk| {
                if let Ok(bytes) = &chunk {
                    stream_observation
                        .lock()
                        .expect("direct PUT observation lock")
                        .update(bytes);
                }
                chunk
            })
            .boxed()
    });
    (source, observation)
}

/// Builds a presigned whole-object PUT request.
pub(super) fn presigned_put_request(access: &ObjectTransferAccess) -> Result<WireRequest> {
    let ObjectTransferAccess::PresignedUrl {
        method,
        url,
        headers,
        ..
    } = access;
    if method != "PUT" {
        return Err(ClientError::Protocol(format!(
            "unsupported presigned upload method `{method}`"
        )));
    }
    let mut request = WireRequest::presigned(reqwest::Method::PUT, url);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    Ok(request)
}

/// In-memory or streaming input for a multipart upload.
enum MultipartPayload<'a> {
    /// A payload the caller already holds.
    Held(&'a [u8]),
    /// A payload read in chunks during upload.
    Streamed(PayloadStream),
}

impl<'a> MultipartPayload<'a> {
    /// Splits the payload using the server's required part size.
    fn into_parts_of(self, part_bytes: usize) -> MultipartParts<'a> {
        match self {
            Self::Held(bytes) => MultipartParts::Held {
                bytes,
                offset: 0,
                part_bytes: part_bytes.max(1),
            },
            Self::Streamed(stream) => MultipartParts::Streamed(PartReader::new(stream, part_bytes)),
        }
    }
}

/// A payload cut into the parts it will be uploaded as.
enum MultipartParts<'a> {
    Held {
        bytes: &'a [u8],
        offset: usize,
        part_bytes: usize,
    },
    Streamed(PartReader),
}

impl MultipartParts<'_> {
    /// Returns the next part, or `None` at the end of the payload.
    async fn next_part(&mut self) -> Result<Option<Bytes>> {
        match self {
            Self::Held {
                bytes,
                offset,
                part_bytes,
            } => {
                if *offset >= bytes.len() {
                    return Ok(None);
                }
                let end = bytes.len().min(*offset + *part_bytes);
                let part = Bytes::copy_from_slice(&bytes[*offset..end]);
                *offset = end;
                Ok(Some(part))
            }
            Self::Streamed(reader) => reader
                .next_part()
                .await
                .map_err(|error| ClientError::Io(format!("reading the payload failed: {error}"))),
        }
    }
}

/// One part waiting to be uploaded, with the checksum its URL is signed
/// against.
struct PendingPart {
    claim: UploadPartChecksumClaim,
    bytes: Bytes,
}

/// Result of reading and uploading a complete multipart payload.
struct UploadedObject {
    size_bytes: u64,
    checksum: Checksum,
    parts: Vec<CompletedUploadPart>,
}

/// Returns the authorization for one part.
fn signed_access(signed: &[SignedUploadPart], part_number: u32) -> Result<ObjectTransferAccess> {
    signed
        .iter()
        .find(|part| part.part_number == part_number)
        .map(|part| part.access.clone())
        .ok_or_else(|| {
            ClientError::Protocol(format!(
                "server authorized no upload for part {part_number}"
            ))
        })
}

/// Reports a server response that disagrees with the negotiated upload mode.
fn negotiated_a_different_upload_mode() -> ClientError {
    ClientError::Protocol(
        "the server answered with a different upload mode than negotiated".to_owned(),
    )
}

impl Client {
    /// Uploads bytes using the best supported transport.
    ///
    /// Large payloads use direct multipart upload when available. The result
    /// includes the content reference and any token required by a commit.
    pub(crate) async fn stage_bytes_as_content_ref(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<StagedContent> {
        // The exact size is known, so transport limits can reject it now.
        match self.transport_for_measured(bytes.len() as u64).await? {
            UploadTransport::Multipart => {
                self.stage_via_multipart(
                    namespace_id,
                    MultipartPayload::Held(bytes),
                    UploadContinuity::default(),
                )
                .await
            }
            UploadTransport::DirectPut => {
                self.direct_put_transfer(namespace_id, DirectPutBody::Held(bytes))
                    .await
            }
            UploadTransport::Proxied => self.stage_bytes_via_server(namespace_id, bytes).await,
        }
    }

    /// Uploads a source stream using the best supported transport.
    ///
    /// The source is read forward without buffering the complete payload.
    pub(crate) async fn stage_source_as_content_ref(
        &self,
        namespace_id: &NamespaceId,
        source: PayloadSource,
        continuity: UploadContinuity<'_>,
    ) -> Result<StagedContent> {
        // A declared length is only a routing hint. Rejection requires a size
        // measured while reading the payload.
        match self.provisional_transport(source.size_bytes()).await? {
            UploadTransport::Multipart => {
                let (stream, _) = source.into_stream();
                self.stage_via_multipart(
                    namespace_id,
                    MultipartPayload::Streamed(stream),
                    continuity,
                )
                .await
            }
            UploadTransport::DirectPut => {
                self.direct_put_transfer(namespace_id, DirectPutBody::Streamed(source))
                    .await
            }
            // Proxied uploads are one request and have no resumable parts.
            UploadTransport::Proxied => self.stage_source_via_server(namespace_id, source).await,
        }
    }

    /// Selects an initial transport from an optional size hint.
    ///
    /// Size hints do not cause local rejection. The server checks the final
    /// size for direct PUT and proxied uploads.
    async fn provisional_transport(&self, size_hint: Option<u64>) -> Result<UploadTransport> {
        let capabilities = self.capabilities().await?;
        Ok(match Self::transport_for(&capabilities, size_hint) {
            Ok(transport) => transport,
            Err(no_fit) if no_fit.direct_put_supported => UploadTransport::DirectPut,
            Err(_) => UploadTransport::Proxied,
        })
    }

    /// Selects a transport for a measured payload size.
    ///
    /// Returns `UploadTooLarge` before transfer when no transport accepts the
    /// measured size.
    async fn transport_for_measured(&self, size_bytes: u64) -> Result<UploadTransport> {
        let capabilities = self.capabilities().await?;
        Self::transport_for(&capabilities, Some(size_bytes)).map_err(|no_fit| {
            ClientError::UploadTooLarge {
                size_bytes,
                reason: no_fit.reason,
            }
        })
    }

    /// Selects a transport from the deployment's advertised capabilities.
    ///
    /// Multipart is preferred for payloads large enough to split. Direct PUT
    /// is next when available and within its limit. Other payloads use the
    /// proxied server path.
    ///
    /// All limits come from the cached capability document.
    fn transport_for(
        capabilities: &CapabilityDocument,
        size_bytes: Option<u64>,
    ) -> std::result::Result<UploadTransport, NoTransportFits> {
        // Small known payloads do not benefit from multipart. Unknown sizes
        // remain eligible because they may exceed one part.
        let worth_cutting = size_bytes.is_none_or(|size| size >= STREAMING_PUT_MIN_BYTES);
        if worth_cutting && capabilities.supports(FEATURE_UPLOADS_DIRECT_MULTIPART) {
            return Ok(UploadTransport::Multipart);
        }
        let direct_put_supported = capabilities.supports(FEATURE_UPLOADS_DIRECT_PUT);
        // Unknown sizes cannot be checked against a limit. Prefer direct PUT
        // when available; otherwise let the server enforce the proxied limit.
        let Some(size_bytes) = size_bytes else {
            return Ok(if direct_put_supported {
                UploadTransport::DirectPut
            } else {
                UploadTransport::Proxied
            });
        };
        let proxy_cap = capabilities
            .limits
            .get(LIMIT_UPLOAD_MAX_CONTENT_BYTES)
            .copied();
        let fits_proxy = proxy_cap.is_none_or(|cap| size_bytes <= cap);
        let direct_put_cap = capabilities.direct_put_max_content_bytes();
        if (worth_cutting || !fits_proxy)
            && direct_put_supported
            && direct_put_cap.is_none_or(|cap| size_bytes <= cap)
        {
            return Ok(UploadTransport::DirectPut);
        }
        if fits_proxy {
            return Ok(UploadTransport::Proxied);
        }
        // Only a deployment that advertises a proxy cap reaches here.
        let proxy_cap = proxy_cap.unwrap_or(size_bytes);
        Err(NoTransportFits {
            reason: match direct_put_cap {
                Some(cap) => format!(
                    "the service takes at most {proxy_cap} bytes \
                     (`{LIMIT_UPLOAD_MAX_CONTENT_BYTES}`), `direct_put` at most {cap} \
                     (`{LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES}`), and \
                     `{FEATURE_UPLOADS_DIRECT_MULTIPART}` is not advertised"
                ),
                None => format!(
                    "the service takes at most {proxy_cap} bytes \
                     (`{LIMIT_UPLOAD_MAX_CONTENT_BYTES}`), and neither \
                     `{FEATURE_UPLOADS_DIRECT_MULTIPART}` nor `{FEATURE_UPLOADS_DIRECT_PUT}` is \
                     advertised"
                ),
            },
            direct_put_supported,
        })
    }

    /// Opens a `direct_put` session, writes its object, and completes it.
    ///
    /// Aborts the session if the transfer fails.
    async fn direct_put_transfer(
        &self,
        namespace_id: &NamespaceId,
        body: DirectPutBody<'_>,
    ) -> Result<StagedContent> {
        let begin = self
            .begin_direct_put(namespace_id, body.size_hint())
            .await?;
        let BeginUploadResponse::DirectPut {
            upload_id,
            checksum_algorithm,
            access,
            ..
        } = begin
        else {
            return Err(negotiated_a_different_upload_mode());
        };
        let (written, content) = match body {
            DirectPutBody::Held(bytes) => {
                let content = UploadContentClaim {
                    size_bytes: bytes.len() as u64,
                    checksum: Checksum::compute(checksum_algorithm, bytes),
                };
                (self.upload_via_presigned_url(&access, bytes).await, content)
            }
            DirectPutBody::Streamed(source) => {
                let (source, observation) = observed_direct_put_source(source, checksum_algorithm);
                let written = self
                    .upload_streamed_via_presigned_url(&access, source)
                    .await;
                let content = observation
                    .lock()
                    .expect("direct PUT observation lock")
                    .finish();
                (written, content)
            }
        };
        if let Err(error) = written {
            let _ = self.abort_upload(namespace_id, &upload_id).await;
            return Err(error);
        }
        let response = self
            .complete_upload(
                namespace_id,
                &upload_id,
                &CompleteUploadRequest::DirectPut { content },
            )
            .await?;
        Self::staged_from_completion(response)
    }

    /// Uploads one object directly in bounded batches of parts.
    ///
    /// One pass computes part checksums and the final object checksum while
    /// reading the payload. The total size does not need to be known first.
    ///
    /// A session that fails partway is aborted rather than left open.
    async fn stage_via_multipart(
        &self,
        namespace_id: &NamespaceId,
        payload: MultipartPayload<'_>,
        continuity: UploadContinuity<'_>,
    ) -> Result<StagedContent> {
        // Resume with the original session and part size so completed parts
        // remain usable.
        let (upload_id, part_size_bytes, checksum_algorithm) = match continuity.resume {
            Some(resume) => (
                resume.upload_id.clone(),
                resume.part_size_bytes,
                resume.checksum_algorithm,
            ),
            None => {
                let begin = self
                    .begin_direct_multipart(namespace_id, DirectMultipartUploadOptions::default())
                    .await?;
                let BeginUploadResponse::DirectMultipart {
                    upload_id,
                    part_size_bytes,
                    checksum_algorithm,
                    ..
                } = begin
                else {
                    return Err(negotiated_a_different_upload_mode());
                };
                if let Some(journal) = continuity.journal {
                    journal.began(&upload_id, part_size_bytes, checksum_algorithm);
                }
                (upload_id, part_size_bytes, checksum_algorithm)
            }
        };
        let uploaded = match self
            .upload_every_part(
                namespace_id,
                &upload_id,
                payload,
                part_size_bytes,
                checksum_algorithm,
                continuity,
            )
            .await
        {
            Ok(uploaded) => uploaded,
            Err(error) => {
                // Abort best-effort and preserve the original upload error.
                let _ = self.abort_upload(namespace_id, &upload_id).await;
                return Err(error);
            }
        };
        if uploaded.parts.is_empty() {
            // Providers cannot assemble zero parts, so use the proxied empty
            // upload path instead.
            let _ = self.abort_upload(namespace_id, &upload_id).await;
            return self.stage_bytes_via_server(namespace_id, &[]).await;
        }

        let response = self
            .complete_multipart_upload(
                namespace_id,
                &upload_id,
                &CompleteMultipartUploadRequest {
                    content: UploadContentClaim {
                        size_bytes: uploaded.size_bytes,
                        checksum: uploaded.checksum,
                    },
                    parts: uploaded.parts,
                },
            )
            .await?;
        Self::staged_from_completion(response)
    }

    /// Cuts the payload into parts and uploads them, holding at most
    /// [`DIRECT_MULTIPART_PARTS_IN_FLIGHT`] of them at a time.
    ///
    /// Each in-flight part owns one buffer, which bounds memory use. A resumed
    /// upload still reads every byte to compute the final checksum, but it
    /// skips network transfer for completed parts.
    async fn upload_every_part(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        payload: MultipartPayload<'_>,
        part_size_bytes: u64,
        checksum_algorithm: ChecksumAlgorithm,
        continuity: UploadContinuity<'_>,
    ) -> Result<UploadedObject> {
        let part_size = usize::try_from(part_size_bytes).map_err(|_| {
            ClientError::Protocol("part size does not fit this platform".to_owned())
        })?;
        let landed = continuity
            .resume
            .map_or::<&[CompletedUploadPart], _>(&[], |resume| &resume.parts);
        let mut source = payload.into_parts_of(part_size);
        let mut whole_object = StreamingChecksum::for_algorithm(checksum_algorithm);
        let mut size_bytes = 0u64;
        let mut parts = Vec::new();
        let mut next_part_number = 1u32;

        loop {
            let mut wave = Vec::with_capacity(DIRECT_MULTIPART_PARTS_IN_FLIGHT);
            let mut source_ended = false;
            while wave.len() < DIRECT_MULTIPART_PARTS_IN_FLIGHT {
                let Some(bytes) = source.next_part().await? else {
                    source_ended = true;
                    break;
                };
                whole_object.update(&bytes);
                size_bytes += bytes.len() as u64;
                let part_number = next_part_number;
                next_part_number += 1;
                if let Some(landed) = landed.iter().find(|part| part.part_number == part_number) {
                    parts.push(landed.clone());
                    continue;
                }
                wave.push(PendingPart {
                    claim: UploadPartChecksumClaim {
                        part_number,
                        checksum: Checksum::compute(checksum_algorithm, &bytes),
                    },
                    bytes,
                });
            }
            if !wave.is_empty() {
                let uploaded = self.upload_wave(namespace_id, upload_id, wave).await?;
                if let Some(journal) = continuity.journal {
                    for part in &uploaded {
                        journal.part_completed(part);
                    }
                }
                parts.extend(uploaded);
            }
            if source_ended {
                break;
            }
        }

        parts.sort_by_key(|part| part.part_number);
        Ok(UploadedObject {
            size_bytes,
            checksum: whole_object.finish(),
            parts,
        })
    }

    /// Authorizes and uploads one wave of parts together.
    async fn upload_wave(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        wave: Vec<PendingPart>,
    ) -> Result<Vec<CompletedUploadPart>> {
        let claims = wave.iter().map(|part| part.claim.clone()).collect();
        let signed = self
            .sign_upload_parts(namespace_id, upload_id, claims)
            .await?;
        let authorized = wave
            .into_iter()
            .map(|part| {
                let access = signed_access(&signed.parts, part.claim.part_number)?;
                Ok((part, access))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut in_flight = tokio::task::JoinSet::new();
        for (part, access) in authorized {
            let client = self.clone();
            let namespace_id = namespace_id.clone();
            let upload_id = upload_id.clone();
            in_flight.spawn(async move {
                client
                    .upload_one_part(&namespace_id, &upload_id, part, access)
                    .await
            });
        }
        let mut uploaded = Vec::new();
        let mut failure = None;
        while let Some(joined) = in_flight.join_next().await {
            match joined.map_err(|err| ClientError::Io(format!("part upload task failed: {err}"))) {
                // Every task is drained before the first failure surfaces,
                // so no part upload outlives the wave that started it.
                Ok(Ok(part)) => uploaded.push(part),
                Ok(Err(error)) | Err(error) => failure = failure.or(Some(error)),
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(uploaded),
        }
    }

    /// Uploads one part, re-asking for its URL if the upload fails.
    ///
    /// Re-asking is the retry: a part's signature is the first thing about
    /// it that goes stale, and a repeated part is last-write-wins at the
    /// provider, so nothing is lost by writing it again.
    async fn upload_one_part(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        part: PendingPart,
        mut access: ObjectTransferAccess,
    ) -> Result<CompletedUploadPart> {
        let part_number = part.claim.part_number;
        for attempt in 1..DIRECT_MULTIPART_PART_ATTEMPTS {
            match self
                .upload_part_via_presigned_url(
                    part_number,
                    &access,
                    part.claim.checksum.clone(),
                    part.bytes.clone(),
                )
                .await
            {
                Ok(uploaded) => return Ok(uploaded),
                Err(error) => {
                    tracing::warn!(
                        part_number,
                        attempt,
                        error = %error,
                        "part upload failed; refreshing authorization before retry"
                    );
                    let signed = self
                        .sign_upload_parts(namespace_id, upload_id, vec![part.claim.clone()])
                        .await?;
                    access = signed_access(&signed.parts, part_number)?;
                }
            }
        }
        self.upload_part_via_presigned_url(part_number, &access, part.claim.checksum, part.bytes)
            .await
    }

    async fn stage_bytes_via_server(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<StagedContent> {
        let upload = self
            .begin_upload(namespace_id, &BeginUploadRequest::ServiceProxied {})
            .await?;
        self.upload_content(namespace_id, upload.upload_id(), bytes)
            .await?;
        self.complete_staged(namespace_id, upload.upload_id()).await
    }

    /// Stages a streamed payload through the server, which hashes it as it
    /// forwards it on.
    ///
    /// The session is aborted if the transfer fails, for the same reason a
    /// multipart session is: it owns an object nothing will finish writing.
    async fn stage_source_via_server(
        &self,
        namespace_id: &NamespaceId,
        source: PayloadSource,
    ) -> Result<StagedContent> {
        let upload = self
            .begin_upload(namespace_id, &BeginUploadRequest::ServiceProxied {})
            .await?;
        let staged = self
            .upload_streamed_content(namespace_id, upload.upload_id(), source)
            .await;
        if let Err(error) = staged {
            let _ = self.abort_upload(namespace_id, upload.upload_id()).await;
            return Err(error);
        }
        self.complete_staged(namespace_id, upload.upload_id()).await
    }

    pub(crate) async fn complete_staged(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<StagedContent> {
        let response = self
            .complete_upload(
                namespace_id,
                upload_id,
                &CompleteUploadRequest::ServiceProxied {},
            )
            .await?;
        Self::staged_from_completion(response)
    }

    pub(crate) fn staged_from_completion(response: UploadSession) -> Result<StagedContent> {
        let status = match response.status {
            UploadSessionStatus::Completed {
                content_ref,
                content_token,
                ..
            } => {
                return Ok(StagedContent {
                    content_ref,
                    content_token,
                });
            }
            UploadSessionStatus::Open { .. } => "open",
            UploadSessionStatus::Aborted { .. } => "aborted",
        };
        Err(ClientError::Protocol(format!(
            "completion of upload `{}` returned status `{status}`",
            response.upload_id
        )))
    }
}

#[cfg(test)]
mod tests;
