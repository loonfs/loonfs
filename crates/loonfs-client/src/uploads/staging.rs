//! Upload transport selection and resumable multipart state.

use crate::transport::{SendPolicy, WireRequest};
use crate::{Client, ClientError, PayloadSource, Result};

use crate::uploads::staging::multipart::MultipartPayload;
use futures::StreamExt as _;
use loonfs_types::{
    CapabilityDocument, Checksum, ChecksumAlgorithm, ChecksumDigest, CommitRequest, ContentRef,
    NamespaceId, UploadId, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES, LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES,
};
use loonfs_types::{
    CompleteUploadBody, CompletedUploadPart, ContentToken, CreateUploadBody, ObjectTransferAccess,
    UploadContentClaim, UploadMode, UploadSession, UploadSessionStatus,
};
use std::sync::{Arc, Mutex};

/// Minimum payload size for streaming and multipart uploads.
///
/// Smaller payloads are sent in one request, where multipart would add
/// requests and save nothing. Payloads of unknown size also use the
/// streaming path.
pub const MIN_STREAMING_PUT_BYTES: u64 = 8 * 1024 * 1024;

/// State required to resume an interrupted direct multipart upload.
///
/// The server records session geometry but not completed parts, so the
/// client retains the upload id, part size, and accepted part metadata.
/// Missing entries are safely re-uploaded; incorrect entries cause completion
/// to fail.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Saves upload progress and the exact file commit request before submission.
///
/// Multipart transfers record session geometry and completed parts. Every
/// transport records its final commit request, including the chosen commit ID
/// and content proof. A journal error stops the operation before the next
/// request; uploaded content remains available for recovery or expiry cleanup.
pub trait PutFileJournal: Send + Sync {
    /// Records a newly opened session and its required part size and checksum algorithm.
    fn began(
        &self,
        upload_id: &UploadId,
        part_size_bytes: u64,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> std::io::Result<()>;
    /// Records a part after it has been uploaded successfully.
    fn part_completed(&self, part: &CompletedUploadPart) -> std::io::Result<()>;
    /// Persists the complete request before any attempt to submit its commit.
    /// Includes the upload id when content was staged, so a caller can renew its token.
    /// Replay this request and actor with [`Client::commit`] after an interruption.
    fn commit_prepared(
        &self,
        request: &CommitRequest,
        actor_id: &loonfs_types::ActorId,
        upload_id: Option<&UploadId>,
    ) -> std::io::Result<()>;
}

/// Optional multipart resume state and a journal for the complete PUT attempt.
#[derive(Clone, Copy, Default)]
pub(crate) struct UploadContinuity<'a> {
    pub(crate) resume: Option<&'a MultipartUploadResume>,
    pub(crate) journal: Option<&'a dyn PutFileJournal>,
}

/// Content ready for a file commit, kept inline or uploaded with its token.
///
/// Clone this value to retry publication with the same commit ID and options.
/// Inline content has no expiry. Preparation does not extend an upload's lifetime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedContent {
    pub(crate) kind: PreparedContentKind,
}

/// The core's `PreparedContentKind` has a content reference for inline bytes
/// and a staged expiry, while this one has raw inline bytes and the content
/// token, because the server assigns the reference and checks the expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedContentKind {
    Inline(Vec<u8>),
    Staged {
        upload_id: Option<UploadId>,
        content_ref: ContentRef,
        content_token: Option<ContentToken>,
    },
}

impl PreparedContent {
    /// Returns the uploaded reference; inline content receives its reference at publication.
    pub fn content_ref(&self) -> Option<&ContentRef> {
        match &self.kind {
            PreparedContentKind::Inline(_) => None,
            PreparedContentKind::Staged { content_ref, .. } => Some(content_ref),
        }
    }
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
    /// Bytes the caller already has.
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
    checksum: Option<ChecksumDigest>,
}

impl DirectPutObservation {
    fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            size_bytes: 0,
            checksum: Some(ChecksumDigest::new(algorithm)),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        self.size_bytes += bytes.len() as u64;
        self.checksum
            .as_mut()
            .expect("the direct PUT checksum should be unfinished")
            .update(bytes);
    }

    fn finish(&mut self) -> UploadContentClaim {
        UploadContentClaim {
            size_bytes: self.size_bytes,
            checksum: self
                .checksum
                .take()
                .expect("the direct PUT checksum should be finished only once")
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
                        .expect("the direct PUT observation lock should not be poisoned")
                        .update(bytes);
                }
                chunk
            })
            .boxed()
    });
    (source, observation)
}

/// Builds a presigned PUT request, for a whole object or for one part.
pub(super) fn presigned_put_request(
    access: &ObjectTransferAccess,
    policy: SendPolicy,
) -> Result<WireRequest> {
    let ObjectTransferAccess::PresignedUrl {
        method,
        url,
        headers,
        ..
    } = access
    else {
        return Err(ClientError::Protocol(
            "unsupported upload access kind".to_owned(),
        ));
    };
    if method != "PUT" {
        return Err(ClientError::Protocol(format!(
            "unsupported presigned upload method `{method}`"
        )));
    }
    let mut request = WireRequest::presigned(http::Method::PUT, url, policy);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    Ok(request)
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
    ) -> Result<PreparedContent> {
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
    ) -> Result<PreparedContent> {
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
        let capabilities = self.get_capabilities().await?;
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
        let capabilities = self.get_capabilities().await?;
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
        let worth_cutting = size_bytes.is_none_or(|size| size >= MIN_STREAMING_PUT_BYTES);
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
            .get(LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES)
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
                     (`{LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES}`), `direct_put` at most {cap} \
                     (`{LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES}`), and \
                     `{FEATURE_UPLOADS_DIRECT_MULTIPART}` is not advertised"
                ),
                None => format!(
                    "the service takes at most {proxy_cap} bytes \
                     (`{LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES}`), and neither \
                     `{FEATURE_UPLOADS_DIRECT_MULTIPART}` nor `{FEATURE_UPLOADS_DIRECT_PUT}` is \
                     advertised"
                ),
            },
            direct_put_supported,
        })
    }

    /// Opens a `direct_put` session, writes its object, and completes it.
    ///
    /// Aborts the session on any failure after it opens.
    async fn direct_put_transfer(
        &self,
        namespace_id: &NamespaceId,
        body: DirectPutBody<'_>,
    ) -> Result<PreparedContent> {
        let begin = self
            .create_direct_put_upload_target(namespace_id, body.size_hint())
            .await?;
        let staged = async {
            if begin.mode != UploadMode::DirectPut {
                return Err(negotiated_a_different_upload_mode());
            }
            let UploadSessionStatus::Open {
                checksum_algorithm,
                access: Some(access),
                ..
            } = begin.status
            else {
                return Err(ClientError::Protocol(
                    "`direct_put` session is not open or lacks `checksum_algorithm` or `access`"
                        .to_owned(),
                ));
            };
            let content = match body {
                DirectPutBody::Held(bytes) => {
                    self.upload_via_presigned_url(&access, bytes).await?;
                    UploadContentClaim {
                        size_bytes: bytes.len() as u64,
                        checksum: Checksum::compute(checksum_algorithm, bytes),
                    }
                }
                DirectPutBody::Streamed(source) => {
                    let (source, observation) =
                        observed_direct_put_source(source, checksum_algorithm);
                    self.upload_stream_via_presigned_url(&access, source)
                        .await?;
                    let content = observation
                        .lock()
                        .expect("the direct PUT observation lock should not be poisoned")
                        .finish();
                    content
                }
            };
            let response = self
                .complete_upload(
                    namespace_id,
                    &begin.upload_id,
                    &CompleteUploadBody::DirectPut { content },
                )
                .await?;
            Self::staged_from_completion(response)
        }
        .await;
        self.abort_if_failed(namespace_id, &begin.upload_id, staged)
            .await
    }

    async fn stage_bytes_via_server(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        let upload = self
            .create_upload(namespace_id, &CreateUploadBody::ServiceProxied {})
            .await?;
        let staged = async {
            self.put_upload_content(namespace_id, &upload.upload_id, bytes)
                .await?;
            self.complete_staged(namespace_id, &upload.upload_id).await
        }
        .await;
        self.abort_if_failed(namespace_id, &upload.upload_id, staged)
            .await
    }

    /// Stages a streamed payload through the server, which hashes it as it
    /// forwards it on.
    ///
    /// Aborts the session on any failure after it opens.
    async fn stage_source_via_server(
        &self,
        namespace_id: &NamespaceId,
        source: PayloadSource,
    ) -> Result<PreparedContent> {
        let upload = self
            .create_upload(namespace_id, &CreateUploadBody::ServiceProxied {})
            .await?;
        let staged = async {
            self.put_upload_content_stream(namespace_id, &upload.upload_id, source)
                .await?;
            self.complete_staged(namespace_id, &upload.upload_id).await
        }
        .await;
        self.abort_if_failed(namespace_id, &upload.upload_id, staged)
            .await
    }

    /// Aborts a one-shot upload session when its staging failed.
    ///
    /// The abort is best effort. The caller gets the error that failed the upload.
    async fn abort_if_failed(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        staged: Result<PreparedContent>,
    ) -> Result<PreparedContent> {
        if staged.is_err() {
            let _ = self.abort_upload(namespace_id, upload_id).await;
        }
        staged
    }

    pub(crate) async fn complete_staged(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<PreparedContent> {
        let response = self
            .complete_upload(
                namespace_id,
                upload_id,
                &CompleteUploadBody::ServiceProxied {},
            )
            .await?;
        Self::staged_from_completion(response)
    }

    pub(crate) fn staged_from_completion(response: UploadSession) -> Result<PreparedContent> {
        let status = match response.status {
            UploadSessionStatus::Completed {
                content_ref,
                content_token,
                ..
            } => {
                return Ok(PreparedContent {
                    kind: PreparedContentKind::Staged {
                        upload_id: Some(response.upload_id),
                        content_ref,
                        content_token,
                    },
                });
            }
            UploadSessionStatus::Open { .. } => "open",
            UploadSessionStatus::Aborted { .. } => "aborted",
            UploadSessionStatus::Unknown => "unknown",
        };
        Err(ClientError::Protocol(format!(
            "completion of upload `{}` returned status `{status}`",
            response.upload_id
        )))
    }
}

mod multipart;

#[cfg(test)]
mod tests;
