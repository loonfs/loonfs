//! Upload transport selection, multipart driving, and resumable staging state.

use super::super::*;

/// Payload size from which a put stops holding its bytes whole.
///
/// It mirrors
/// `loonfs_objectstore::provider_object_store::PROVIDER_MULTIPART_PART_BYTES`,
/// and that one number answers two questions the same way: below it a direct
/// multipart upload would be a one-part upload with extra round trips and
/// nothing to gain, and a payload that fits in a single part is not worth
/// streaming either. At or above it — and for any payload whose length is not
/// known in advance — a put reads its source once, in bounded pieces.
pub const STREAMING_PUT_MIN_BYTES: u64 = 8 * 1024 * 1024;

/// Parts in flight at once. Each holds its bytes, so a one-pass upload's
/// memory is this many parts and no more.
const DIRECT_MULTIPART_PARTS_IN_FLIGHT: usize = 4;

/// Attempts one part gets before its upload gives up. A retry re-asks for
/// the part's URL, because the first thing that goes stale about a part is
/// its signature.
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
    /// The part size the session was opened with. A resumed upload must cut
    /// the payload exactly as the interrupted one did, or the parts it
    /// sends will not line up with the ones already there.
    pub part_size_bytes: u64,
    /// Checksum algorithm frozen into the upload session when it began.
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

/// How one upload survives an interruption: what an earlier run got
/// through, and where this one writes down what it gets through.
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
    /// Parts, straight to object storage, retried one at a time.
    Multipart,
    /// One whole-object request, straight to object storage, carrying the
    /// checksum this deployment's provider will enforce on it.
    DirectPut(ChecksumAlgorithm),
    /// Through the server, which writes the content object itself.
    Proxied,
}

/// A payload length this client has actually measured, as against a source's
/// file-metadata hint.
///
/// Only a measured length may end an upload. A file can change before it is
/// read, so a refusal built on metadata would turn a stale hint into a failed
/// upload. Threading the distinction through the type is what makes
/// [`ClientError::UploadTooLarge`] unconstructible from a hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MeasuredBytes(u64);

/// No transport this deployment offers can carry a payload of the length it
/// was asked about.
struct NoTransportFits {
    /// The caps it passed, named so a caller can act on them.
    reason: String,
    /// The checksum a whole-object write would use, when one is offered at
    /// all — even though this payload is past its ceiling. A length that was
    /// only a hint routes through that transport anyway: its first pass
    /// measures the payload without sending a byte, and the refusal that may
    /// follow is built on what it found.
    direct_put_algorithm: Option<ChecksumAlgorithm>,
}

/// What a whole-object write sends, and where it reads it from.
///
/// The two arms are the two ways a payload's digest can already exist when
/// the session opens: the caller was holding the bytes, or a measuring pass
/// left them somewhere they can be read again. Neither holds the payload on
/// this client's account.
enum DirectPutBody<'a> {
    /// Bytes the caller already holds.
    Held(&'a [u8]),
    /// A measured payload, re-read from disk in pieces.
    Rewound(PayloadSource),
}

/// A measuring pass wrote or found the payload, and then could not read it
/// back — the upload cannot proceed without the bytes it just measured.
fn read_back_failed(error: std::io::Error) -> ClientError {
    ClientError::Io(format!("could not re-read the measured payload: {error}"))
}

/// Builds the presigned whole-object write both upload forms send.
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

/// What a multipart upload has to work with.
///
/// The two arms exist so that neither caller pays for the other's shape: a
/// caller holding its payload should not have it copied whole to be
/// uploaded, and a caller reading a stream cannot be asked for a length it
/// does not have. Past this point the upload cannot tell them apart.
enum MultipartPayload<'a> {
    /// A payload the caller already holds.
    Held(&'a [u8]),
    /// A payload read once, in pieces, as it is uploaded.
    Streamed(PayloadStream),
}

impl<'a> MultipartPayload<'a> {
    /// Binds the payload to the geometry the server chose.
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
    /// The next part, or `None` once the payload is spent. Both arms hand
    /// out one part's worth of bytes and no more.
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

/// What one pass over a payload produced: the assembled object's length and
/// digest, and the parts it was written as.
struct UploadedObject {
    size_bytes: u64,
    checksum: Checksum,
    parts: Vec<CompletedUploadPart>,
}

/// Picks one part's authorization out of a signing response.
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

/// A begin-upload response that is not the transport the request asked for.
///
/// Which transport carries a payload is settled before the request goes
/// out, from what the deployment advertises, so a different mode coming
/// back is a broken server rather than something to fall back from.
fn negotiated_a_different_upload_mode() -> ClientError {
    ClientError::Protocol(
        "the server answered with a different upload mode than negotiated".to_owned(),
    )
}

impl Client {
    /// Makes bytes durable, choosing the transport the payload and the
    /// deployment allow.
    ///
    /// A large payload goes straight to object storage in parallel parts
    /// where the server can authorize that; everything else goes through
    /// the server. Either way the caller gets back one content reference
    /// plus the receipt that admits it at commit.
    pub(crate) async fn stage_bytes_as_content_ref(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<StagedContent> {
        // A payload already in hand has been measured by definition, so this
        // may refuse.
        match self
            .transport_for_measured(MeasuredBytes(bytes.len() as u64))
            .await?
        {
            UploadTransport::Multipart => {
                self.stage_via_multipart(
                    namespace_id,
                    MultipartPayload::Held(bytes),
                    UploadContinuity::default(),
                )
                .await
            }
            UploadTransport::DirectPut(algorithm) => {
                self.stage_via_direct_put(namespace_id, bytes, algorithm)
                    .await
            }
            UploadTransport::Proxied => self.stage_bytes_via_server(namespace_id, bytes).await,
        }
    }

    /// Makes a streamed payload durable, choosing the transport the source
    /// and the deployment allow.
    ///
    /// Either way the source is read once, forward, and never held whole.
    /// A deployment that can authorize direct part uploads gets them; one
    /// that cannot receives the same source as a streaming request body.
    pub(crate) async fn stage_source_as_content_ref(
        &self,
        namespace_id: &NamespaceId,
        source: PayloadSource,
        continuity: UploadContinuity<'_>,
    ) -> Result<StagedContent> {
        // The length a source declares is a hint, so it only routes; it
        // never refuses. Both streaming transports measure the payload as
        // they send it, and the one that cannot — a whole-object write —
        // measures it in the pass it has to make anyway.
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
            UploadTransport::DirectPut(algorithm) => {
                self.stage_source_via_direct_put(namespace_id, source, algorithm, continuity)
                    .await
            }
            // Nothing to resume off this path: a proxied upload is one
            // request with no session behind it, so there are no parts to
            // have landed.
            UploadTransport::Proxied => self.stage_source_via_server(namespace_id, source).await,
        }
    }

    /// Stages a streamed payload through one presigned whole-object write.
    ///
    /// Two passes, neither of which holds the payload. The first folds the
    /// deployment's own checksum and counts the bytes, spooling them only if
    /// the source cannot be read again; the second streams them into the
    /// signed request. Between the two, what the first pass *measured*
    /// re-decides the transport — the hint that routed here may have been
    /// wrong in either direction, and only now is there a length worth
    /// refusing on.
    async fn stage_source_via_direct_put(
        &self,
        namespace_id: &NamespaceId,
        source: PayloadSource,
        algorithm: ChecksumAlgorithm,
        continuity: UploadContinuity<'_>,
    ) -> Result<StagedContent> {
        let mut digest = StreamingChecksum::for_algorithm(algorithm);
        let measured = source
            .measure(&mut digest)
            .await
            .map_err(|error| ClientError::Io(error.to_string()))?;
        let size = MeasuredBytes(measured.size_bytes());
        let checksum = digest.finish();

        match self.transport_for_measured(size).await? {
            UploadTransport::DirectPut(_) => {
                self.direct_put_transfer(
                    namespace_id,
                    size,
                    checksum,
                    DirectPutBody::Rewound(measured.reread().await.map_err(read_back_failed)?),
                )
                .await
            }
            // Reachable only if the deployment's answer changed under us;
            // the rewound payload serves either transport unchanged.
            UploadTransport::Multipart => {
                let (stream, _) = measured
                    .reread()
                    .await
                    .map_err(read_back_failed)?
                    .into_stream();
                self.stage_via_multipart(
                    namespace_id,
                    MultipartPayload::Streamed(stream),
                    continuity,
                )
                .await
            }
            UploadTransport::Proxied => {
                self.stage_source_via_server(
                    namespace_id,
                    measured.reread().await.map_err(read_back_failed)?,
                )
                .await
            }
        }
    }

    /// Routes a payload whose length is only a hint. Never refuses.
    ///
    /// A hint that says nothing fits may simply be wrong, and only a
    /// measured length may end an upload. So where a whole-object write is
    /// on offer this takes it — that transport's first pass measures the
    /// payload without sending a byte, and any refusal comes after. Where
    /// one is not, the payload streams to the service, which measures it as
    /// it receives it and answers `content_too_large` if it must.
    async fn provisional_transport(&self, size_hint: Option<u64>) -> Result<UploadTransport> {
        let capabilities = self.capabilities().await?;
        Ok(match Self::transport_for(&capabilities, size_hint) {
            Ok(transport) => transport,
            Err(no_fit) => no_fit
                .direct_put_algorithm
                .map_or(UploadTransport::Proxied, UploadTransport::DirectPut),
        })
    }

    /// Routes a payload whose length this client measured, refusing when no
    /// transport can carry it.
    ///
    /// The refusal names the caps it passed. It is raised before any byte
    /// moves rather than after the capped proxy has read and rejected the
    /// whole payload.
    async fn transport_for_measured(&self, size: MeasuredBytes) -> Result<UploadTransport> {
        let capabilities = self.capabilities().await?;
        Self::transport_for(&capabilities, Some(size.0)).map_err(|no_fit| {
            ClientError::UploadTooLarge {
                size_bytes: size.0,
                reason: no_fit.reason,
            }
        })
    }

    /// The best transport for a payload of this length, against what the
    /// deployment actually advertises.
    ///
    /// Parts win wherever they are offered and the payload is worth cutting:
    /// a part is retried on its own, and nothing has to know the length in
    /// advance. A whole-object write is the next rung — it exists for a
    /// provider that can sign a write but has no multipart API to open — and
    /// it earns its extra pass over the payload only when the payload is
    /// large, or when the service would not take it anyway. Everything else
    /// goes through the service.
    ///
    /// Every limit here is read from the capability document. Nothing is
    /// assumed about how the deployment is configured, which is why the
    /// document is fetched even for a small payload: the fetch happens once
    /// per client and is cached, so the round trip is paid at most once.
    fn transport_for(
        capabilities: &CapabilityDocument,
        size_bytes: Option<u64>,
    ) -> std::result::Result<UploadTransport, NoTransportFits> {
        // Below one part there is nothing to cut, and a length nobody knows
        // cannot rule parts out.
        let worth_cutting = size_bytes.is_none_or(|size| size >= STREAMING_PUT_MIN_BYTES);
        if worth_cutting && capabilities.supports(FEATURE_UPLOADS_DIRECT_MULTIPART) {
            return Ok(UploadTransport::Multipart);
        }
        let direct_put_algorithm = capabilities.direct_put_checksum_algorithm();
        // A length nobody knows cannot be checked against a cap. Where a
        // whole-object write is on offer that is no reason to give up on
        // it: the transport's first pass measures the payload without
        // sending a byte, and that measurement re-decides the transport
        // before anything moves. Falling straight to the service here would
        // stream an unmeasured payload at a cap it may not fit, and where the
        // deployment signs no multipart it would be discarding the only rung
        // that could have carried it.
        let Some(size_bytes) = size_bytes else {
            return Ok(
                direct_put_algorithm.map_or(UploadTransport::Proxied, UploadTransport::DirectPut)
            );
        };
        let proxy_cap = capabilities
            .limits
            .get(LIMIT_UPLOAD_MAX_CONTENT_BYTES)
            .copied();
        let fits_proxy = proxy_cap.is_none_or(|cap| size_bytes <= cap);
        let direct_put_cap = capabilities.direct_put_max_content_bytes();
        if worth_cutting || !fits_proxy {
            if let Some(algorithm) = direct_put_algorithm {
                if direct_put_cap.is_none_or(|cap| size_bytes <= cap) {
                    return Ok(UploadTransport::DirectPut(algorithm));
                }
            }
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
                     `{FEATURE_UPLOADS_DIRECT_MULTIPART}` nor a usable `direct_put` checksum is \
                     advertised"
                ),
            },
            direct_put_algorithm,
        })
    }

    /// Uploads one whole object the caller already holds straight to object
    /// storage.
    ///
    /// The digest is signed into the write the provider will enforce, so it
    /// exists before the session does. A payload already in hand needs no
    /// second pass to produce it: it is folded here, over the bytes the
    /// caller is holding anyway.
    async fn stage_via_direct_put(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
        algorithm: ChecksumAlgorithm,
    ) -> Result<StagedContent> {
        let mut digest = StreamingChecksum::for_algorithm(algorithm);
        digest.update(bytes);
        self.direct_put_transfer(
            namespace_id,
            MeasuredBytes(bytes.len() as u64),
            digest.finish(),
            DirectPutBody::Held(bytes),
        )
        .await
    }

    /// Opens a `direct_put` session, writes its object, and completes it.
    ///
    /// The claim is the measured length and the digest folded over the same
    /// bytes; nothing here is taken from a source's declared length. A
    /// session whose transfer fails is aborted rather than left open — it
    /// owns an object nothing will finish writing, exactly as a multipart
    /// session does.
    async fn direct_put_transfer(
        &self,
        namespace_id: &NamespaceId,
        size: MeasuredBytes,
        checksum: Checksum,
        body: DirectPutBody<'_>,
    ) -> Result<StagedContent> {
        let begin = self
            .begin_direct_put(
                namespace_id,
                UploadContentClaim {
                    size_bytes: size.0,
                    checksum,
                },
            )
            .await?;
        let BeginUploadResponse::DirectPut {
            upload_id,
            direct_put,
            ..
        } = begin
        else {
            return Err(negotiated_a_different_upload_mode());
        };
        let written = match body {
            DirectPutBody::Held(bytes) => {
                self.upload_via_presigned_url(&direct_put.access, bytes)
                    .await
            }
            DirectPutBody::Rewound(source) => {
                self.upload_streamed_via_presigned_url(&direct_put.access, source)
                    .await
            }
        };
        if let Err(error) = written {
            let _ = self.abort_upload(namespace_id, &upload_id).await;
            return Err(error);
        }
        let response = self.complete_upload(namespace_id, &upload_id).await?;
        Self::staged_from_completion(response)
    }

    /// Uploads one object straight to object storage in bounded waves of
    /// parts.
    ///
    /// The whole-object checksum is folded part by part as the payload is
    /// cut, so the same pass that produces what the provider enforces on
    /// each part also produces what completion verifies the assembly
    /// against — and because the claim is only needed at completion, that
    /// one pass is the only pass over the bytes anyone has to make. Nothing
    /// here needs the payload's length in advance, which is what lets a
    /// stream with no length take this path unchanged.
    ///
    /// A session that fails partway is aborted rather than left open.
    async fn stage_via_multipart(
        &self,
        namespace_id: &NamespaceId,
        payload: MultipartPayload<'_>,
        continuity: UploadContinuity<'_>,
    ) -> Result<StagedContent> {
        // A resumed upload rejoins the session a previous run opened, at the
        // part size that run was given. Asking for a new one would open a
        // second session and orphan the parts already in object storage.
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
                    direct_multipart,
                    ..
                } = begin
                else {
                    return Err(negotiated_a_different_upload_mode());
                };
                if let Some(journal) = continuity.journal {
                    journal.began(
                        &upload_id,
                        direct_multipart.part_size_bytes,
                        direct_multipart.checksum_algorithm,
                    );
                }
                (
                    upload_id,
                    direct_multipart.part_size_bytes,
                    direct_multipart.checksum_algorithm,
                )
            }
        };
        let uploaded = self
            .upload_every_part(
                namespace_id,
                &upload_id,
                payload,
                part_size_bytes,
                checksum_algorithm,
                continuity,
            )
            .await;
        let uploaded = match uploaded {
            Ok(uploaded) => uploaded,
            Err(error) => {
                // The session owns an object this upload will never finish
                // writing. Ending it is best-effort: the original failure is
                // what the caller needs to see, and abandoned sessions are
                // collected either way.
                let _ = self.abort_upload(namespace_id, &upload_id).await;
                return Err(error);
            }
        };
        if uploaded.parts.is_empty() {
            // The source was empty, and a provider has no empty assembly to
            // make. The payload is nothing, so staging it costs nothing.
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
    /// The window is the memory bound: each in-flight part holds its bytes,
    /// and nothing outside the window does. One wave asks for its part URLs
    /// in a single request, uploads them together, and only then reads the
    /// next wave — so the payload's length never enters into how much of it
    /// is resident.
    /// A resumed upload still reads every byte: the whole-object checksum
    /// completion verifies the assembly against is folded over the payload
    /// in one forward pass, so a part already in object storage is cut,
    /// folded, and then let go rather than sent again. What resuming saves
    /// is the network, which is the part that was expensive.
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
        let response = self.complete_upload(namespace_id, upload_id).await?;
        Self::staged_from_completion(response)
    }

    pub(crate) fn staged_from_completion(response: UploadSessionResponse) -> Result<StagedContent> {
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
