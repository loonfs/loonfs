//! Issuer contract and values for presigned direct-put transfers.

use crate::object_store::Result;
use loonfs_api::ContentRef;
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
}
