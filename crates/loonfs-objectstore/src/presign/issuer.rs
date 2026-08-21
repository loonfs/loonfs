//! Interfaces for issuing presigned reads and writes.

use crate::object_store::Result;
use async_trait::async_trait;
use loonfs_api::{Checksum, ChecksumAlgorithm};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Describes one immutable create-only write to authorize for a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedPutRequest<'a> {
    /// Logical unscoped object key that the issuer resolves beneath its configured prefix.
    pub object_key: &'a str,
    /// Lifetime of the issued capability measured from the supplied signing time.
    pub expires_in: Duration,
}

/// Describes one read of an existing content object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedGetRequest<'a> {
    /// Logical unscoped object key that the issuer resolves beneath its configured prefix.
    pub object_key: &'a str,
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
    pub checksum: &'a Checksum,
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

/// Issues create-only write permissions for complete objects.
///
/// - The signed request must be create-only, so an existing object is never
///   replaced through a transfer capability (S3-family: a signed
///   `if-none-match: *` header).
/// - The issuer must report the checksum algorithm the provider stores for a
///   checksum-less single PUT. Completion verifies that stored checksum.
///
/// A provider that cannot preserve create-only writes and report a durable
/// full-object checksum must not implement this trait.
#[async_trait]
pub trait DirectPutIssuer: Send + Sync + std::fmt::Debug {
    /// The whole-object checksum this provider stores for a single PUT.
    ///
    /// The begin response returns this algorithm to the client. Completion
    /// compares the client checksum with the checksum stored by the provider.
    fn stored_checksum_algorithm(&self) -> ChecksumAlgorithm;

    /// The largest object this provider accepts in one presigned request.
    ///
    /// The server can reject a size hint above this limit at begin. Completion
    /// always checks the actual stored size.
    fn max_content_bytes(&self) -> u64;

    /// Issues a create-only write capability for the requested object key.
    ///
    /// Issuance fails for invalid keys, invalid expiry policy, unusable signing
    /// time, or malformed provider configuration.
    async fn presign_put(
        &self,
        request: PresignedPutRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl>;
}

/// Issues read capabilities for content objects a client fetches straight
/// from the provider.
#[async_trait]
pub trait DirectGetIssuer: Send + Sync + std::fmt::Debug {
    /// Issues a read capability for one content object.
    ///
    /// One capability may be used for ranged, resumed, or parallel reads.
    /// Implementations must not sign the `Range` header, because doing so
    /// would restrict the capability to one byte range.
    ///
    /// Issuance fails for invalid keys, invalid expiry policy, unusable
    /// signing time, or malformed provider configuration.
    async fn presign_get(
        &self,
        request: PresignedGetRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl>;
}

/// Issues write permissions for an open multipart upload.
#[async_trait]
pub trait DirectMultipartIssuer: Send + Sync + std::fmt::Debug {
    /// Issues a write capability for one part of an open multipart upload.
    ///
    /// Multipart parts are replaceable so clients can retry them. The signed
    /// request includes the checksum that the provider must enforce.
    async fn presign_multipart_part(
        &self,
        request: PresignedPartRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl>;
}

/// Direct transfers supported by one configured object store.
///
/// Direct reads are required whenever any direct transfer is enabled. Direct
/// PUT and multipart support are independent and may be absent.
#[derive(Debug, Clone)]
pub struct DirectTransferIssuers {
    /// Signs reads of content objects. Always present.
    pub get: Arc<dyn DirectGetIssuer>,
    /// Signs whole-object create-only writes when the provider can report a
    /// durable full-object checksum afterward.
    pub put: Option<Arc<dyn DirectPutIssuer>>,
    /// Signs the parts of a provider multipart upload, when the provider has
    /// one.
    pub multipart: Option<Arc<dyn DirectMultipartIssuer>>,
}

impl DirectTransferIssuers {
    /// Builds a read-only bundle: reads are signed, neither write direction
    /// is offered.
    pub fn read_only(get: Arc<dyn DirectGetIssuer>) -> Self {
        Self {
            get,
            put: None,
            multipart: None,
        }
    }

    /// Adds whole-object write signing to this bundle.
    #[must_use]
    pub fn with_put(mut self, put: Arc<dyn DirectPutIssuer>) -> Self {
        self.put = Some(put);
        self
    }

    /// Adds multipart part signing to this bundle.
    #[must_use]
    pub fn with_multipart(mut self, multipart: Arc<dyn DirectMultipartIssuer>) -> Self {
        self.multipart = Some(multipart);
        self
    }
}
