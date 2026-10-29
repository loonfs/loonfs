//! Direct multipart staging: the parts of a payload, the sliding window that
//! signs and uploads them, and the retries of one part.

use crate::payload::PartReader;

use crate::{Client, ClientError, PayloadStream, Result};
use bytes::Bytes;
use futures::stream::FuturesUnordered;
use futures::StreamExt as _;
use loonfs_types::{Checksum, ChecksumAlgorithm, ChecksumDigest, NamespaceId, UploadId};
use loonfs_types::{
    CompleteUploadBody, CompletedUploadPart, ObjectTransferAccess, SignedUploadPart,
    UploadContentClaim, UploadMode, UploadPartChecksumClaim, UploadSessionStatus,
};

use crate::uploads::staging::negotiated_a_different_upload_mode;
use crate::uploads::staging::PreparedContent;
use crate::uploads::staging::UploadContinuity;

/// Most multipart parts kept in memory at once, which is also the most parts
/// uploading at once.
pub(super) const DIRECT_MULTIPART_PARTS_IN_FLIGHT: usize = 4;

/// Maximum attempts for one part, including refreshed authorization.
const DIRECT_MULTIPART_PART_ATTEMPTS: usize = 3;

/// In-memory or streaming input for a multipart upload.
pub(super) enum MultipartPayload<'a> {
    /// A payload the caller already has.
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

impl Client {
    /// Uploads one object directly in bounded batches of parts.
    ///
    /// One pass computes part checksums and the final object checksum while
    /// reading the payload. The total size does not need to be known first.
    ///
    /// A journaled upload leaves its session open on failure so the caller
    /// can resume it. Any other upload aborts its session when the transfer
    /// or completion fails.
    pub(super) async fn stage_via_multipart(
        &self,
        namespace_id: &NamespaceId,
        payload: MultipartPayload<'_>,
        continuity: UploadContinuity<'_>,
    ) -> Result<PreparedContent> {
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
                    .create_direct_multipart_upload_target(namespace_id)
                    .await?;
                if begin.mode != UploadMode::DirectMultipart {
                    return Err(negotiated_a_different_upload_mode());
                }
                let UploadSessionStatus::Open {
                    part_size_bytes: Some(part_size_bytes),
                    checksum_algorithm,
                    ..
                } = begin.status
                else {
                    return Err(ClientError::Protocol("`direct_multipart` session is not open or lacks `part_size_bytes` or `checksum_algorithm`".to_owned()));
                };
                let upload_id = begin.upload_id;
                if let Some(journal) = continuity.journal {
                    journal
                        .began(&upload_id, part_size_bytes, checksum_algorithm)
                        .map_err(|error| {
                            ClientError::Io(format!("recording upload `{upload_id}`: {error}"))
                        })?;
                }
                (upload_id, part_size_bytes, checksum_algorithm)
            }
        };
        let staged = async {
            let uploaded = self
                .upload_every_part(
                    namespace_id,
                    &upload_id,
                    payload,
                    part_size_bytes,
                    checksum_algorithm,
                    continuity,
                )
                .await?;
            if uploaded.parts.is_empty() {
                // Providers cannot assemble zero parts, so use the proxied empty
                // upload path instead.
                let _ = self.abort_upload(namespace_id, &upload_id).await;
                return self.stage_bytes_via_server(namespace_id, &[]).await;
            }
            let response = self
                .complete_upload(
                    namespace_id,
                    &upload_id,
                    &CompleteUploadBody::DirectMultipart {
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
        .await;
        if continuity.journal.is_some() {
            return staged;
        }
        self.abort_if_failed(namespace_id, &upload_id, staged).await
    }

    /// Cuts the payload into parts and uploads them, keeping at most
    /// [`DIRECT_MULTIPART_PARTS_IN_FLIGHT`] of them at a time.
    ///
    /// A new part starts uploading whenever one finishes, and one signing
    /// request authorizes all the parts that fill the free places. Each
    /// in-flight part owns one buffer, which bounds memory use. A resumed
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
        let mut whole_object = ChecksumDigest::new(checksum_algorithm);
        let mut size_bytes = 0u64;
        let mut parts = Vec::new();
        let mut next_part_number = 1u32;
        let mut source_ended = false;
        let mut in_flight = FuturesUnordered::new();

        loop {
            let mut unsigned = Vec::new();
            while !source_ended
                && in_flight.len() + unsigned.len() < DIRECT_MULTIPART_PARTS_IN_FLIGHT
            {
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
                unsigned.push(PendingPart {
                    claim: UploadPartChecksumClaim {
                        part_number,
                        checksum: Checksum::compute(checksum_algorithm, &bytes),
                    },
                    bytes,
                });
            }
            if !unsigned.is_empty() {
                for (part, access) in self.sign_parts(namespace_id, upload_id, unsigned).await? {
                    in_flight.push(self.upload_one_part(namespace_id, upload_id, part, access));
                }
            }
            let Some(uploaded) = in_flight.next().await else {
                break;
            };
            let uploaded = uploaded?;
            if let Some(journal) = continuity.journal {
                journal.part_completed(&uploaded).map_err(|error| {
                    ClientError::Io(format!("recording upload `{upload_id}`: {error}"))
                })?;
            }
            parts.push(uploaded);
        }

        parts.sort_by_key(|part| part.part_number);
        Ok(UploadedObject {
            size_bytes,
            checksum: whole_object.finish(),
            parts,
        })
    }

    /// Authorizes the upload of `unsigned` with one signing request, and
    /// returns each part with its authorization.
    async fn sign_parts(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        unsigned: Vec<PendingPart>,
    ) -> Result<Vec<(PendingPart, ObjectTransferAccess)>> {
        let claims = unsigned.iter().map(|part| part.claim.clone()).collect();
        let signed = self
            .sign_upload_parts(namespace_id, upload_id, claims)
            .await?;
        unsigned
            .into_iter()
            .map(|part| {
                let access = signed_access(&signed.parts, part.claim.part_number)?;
                Ok((part, access))
            })
            .collect()
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
}
