//! Issuer contract and values for presigned direct-put transfers.

use crate::object_store::Result;
use loonfs_api::{ContentRef, StorageChecksum};
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

/// Describes one immutable create-only write to authorize for a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedPutRequest<'a> {
    /// Logical unscoped object key that the issuer resolves beneath its configured prefix.
    pub object_key: &'a str,
    /// Expected digest and byte length the provider must enforce for the request body.
    pub content_ref: &'a ContentRef,
    /// Lifetime of the issued capability measured from the supplied signing time.
    pub expires_in: Duration,
}

/// Describes one part of an open multipart upload to authorize for a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedPartRequest<'a> {
    /// Logical unscoped object key the finished upload assembles into.
    pub object_key: &'a str,
    /// Provider-side upload the part belongs to.
    pub provider_upload_id: &'a str,
    /// One-based part number.
    pub part_number: u32,
    /// Checksum the provider must enforce on this part's bytes.
    pub part_checksum: &'a StorageChecksum,
    /// Lifetime of the issued capability measured from the supplied signing time.
    pub expires_in: Duration,
}

/// Carries a short-lived HTTP capability and every request header covered by its signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedUrl {
    /// HTTP method the client must use exactly as issued.
    pub method: String,
    /// Complete provider URL including authentication query parameters.
    pub url: String,
    /// Signed headers and values the client must send unchanged.
    pub headers: BTreeMap<String, String>,
    /// Unix-millisecond instant after which the provider rejects the capability.
    pub expires_at_ms: u64,
}

/// Issues short-lived transfer capabilities for `direct_put` uploads, and in
/// doing so carries the write-time enforcement contract the rest of the
/// system leans on:
///
/// - The signed request must make the provider verify that the uploaded
///   body hashes to the content ref's digest and reject anything else
///   (S3-family: a signed `x-amz-checksum-sha256` header).
/// - The signed request must be create-only, so an existing object is never
///   replaced through a transfer capability (S3-family: a signed
///   `if-none-match: *` header).
/// - Both requirements ride the signature: a client cannot drop or alter
///   them without invalidating the capability.
///
/// Because every issuer guarantees this, `direct_put` completion proves an
/// upload by existence and size alone — it never reads content back. A
/// provider that cannot enforce digest verification and create-only
/// preconditions in a presigned request must not implement this trait; the
/// deployment then reports `direct_put` as unsupported instead of falling
/// back to weaker verification.
pub trait ObjectTransferIssuer: Send + Sync + std::fmt::Debug {
    /// Issues a create-only write capability bound to the requested content identity.
    ///
    /// Issuance fails for invalid keys, unsupported content-reference kinds,
    /// invalid expiry policy, unusable signing time, or malformed provider configuration.
    fn presign_put(
        &self,
        request: PresignedPutRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl>;

    /// Issues a write capability for one part of an open multipart upload.
    ///
    /// A part is not the object, so this one is not create-only: re-issuing
    /// a part is how a client retries a transfer that failed halfway, and
    /// the provider takes the last write. What still rides the signature is
    /// the part's checksum, which both providers enforce on the way in — so
    /// no part of the eventual object is ever bytes the client did not
    /// declare.
    fn presign_multipart_part(
        &self,
        request: PresignedPartRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl>;
}
