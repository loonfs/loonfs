//! Presigned-URL issuing for direct_put uploads.

use crate::object_store::Result;
use loonfs_api::ContentRef;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

mod aws_sigv4;
mod s3_compatible;

pub use s3_compatible::{S3CompatiblePresigner, S3PresignerConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedPutRequest<'a> {
    pub object_key: &'a str,
    pub content_ref: &'a ContentRef,
    pub expires_in: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedUrl {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
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
    fn presign_put(
        &self,
        request: PresignedPutRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl>;
}
