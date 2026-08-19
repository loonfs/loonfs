//! Issuer contracts and values for presigned direct transfers, in both
//! directions: the create-only writes `direct_put` authorizes, the parts
//! `direct_multipart` authorizes, and the reads `direct_get` authorizes.
//!
//! Each direction is its own trait, and a store offers the ones it can
//! actually sign. A provider that signs whole-object writes and reads but
//! has no S3-style multipart API is spelled by leaving
//! [`DirectTransferIssuers::multipart`] empty — absence, never a method that
//! exists and fails.

use crate::object_store::Result;
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

/// Describes one read of an existing content object to authorize for a client.
///
/// There is nothing to bind but the key. A write also carries a create-only
/// precondition because the bytes do not exist yet and the provider is the
/// only party that can prevent replacement. A read is of bytes this
/// deployment already verified, and the reader checks what arrives against
/// the reference it was handed.
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

/// Issues whole-object create-only write capabilities.
///
/// - The signed request must be create-only, so an existing object is never
///   replaced through a transfer capability (S3-family: a signed
///   `if-none-match: *` header).
/// - The issuer must report the checksum algorithm the provider stores for a
///   checksum-less single PUT. Completion verifies that stored checksum.
///
/// A provider that cannot preserve create-only writes and report a durable
/// full-object checksum must not implement this trait.
pub trait DirectPutIssuer: Send + Sync + std::fmt::Debug {
    /// The whole-object checksum this provider stores for a single PUT.
    ///
    /// The begin response dictates this algorithm to the client. Completion
    /// compares the client's claim with the provider's stored checksum.
    fn stored_checksum_algorithm(&self) -> ChecksumAlgorithm;

    /// The largest object this provider accepts in one presigned request.
    ///
    /// A payload past it has to move some other way, so the deployment
    /// advertises the number. The begin handler refuses an advisory size over
    /// it, and completion checks the stored size authoritatively.
    fn max_content_bytes(&self) -> u64;

    /// Issues a create-only write capability for the requested object key.
    ///
    /// Issuance fails for invalid keys, invalid expiry policy, unusable signing
    /// time, or malformed provider configuration.
    fn presign_put(
        &self,
        request: PresignedPutRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl>;
}

/// Issues read capabilities for content objects a client fetches straight
/// from the provider.
pub trait DirectGetIssuer: Send + Sync + std::fmt::Debug {
    /// Issues a read capability for one content object.
    ///
    /// One capability serves the whole transfer, however many requests the
    /// client makes with it. A presigned URL signs a named set of headers,
    /// and `Range` is not among them — so a reader may range, resume after
    /// a broken connection, or fetch in parallel windows on the one URL,
    /// and the signature is unaffected. Implementors must keep it that way:
    /// signing a `Range` header would bind a capability to one window and
    /// turn every resumption into another round trip to this server.
    ///
    /// Issuance fails for invalid keys, invalid expiry policy, unusable
    /// signing time, or malformed provider configuration.
    fn presign_get(
        &self,
        request: PresignedGetRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl>;
}

/// Issues write capabilities for the parts of an open provider multipart
/// upload.
///
/// Only a provider with an S3-style multipart API — one this server opens,
/// signs parts against, and completes — implements this. Whole-object
/// direct writes do not depend on it.
pub trait DirectMultipartIssuer: Send + Sync + std::fmt::Debug {
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

/// What one deployment's store can authorize a client to do directly.
///
/// A store either offers direct transfers or it does not, and a store that
/// offers them can always sign a read: a deployment that lets a client write
/// an object too large to proxy back has to be able to hand that object
/// back. That is why `get` is not optional here — the symmetry is the type,
/// not a rule someone has to remember.
///
/// The two write directions are independent of it and of each other. A
/// provider may sign whole-object writes without having an S3-style
/// multipart API, or serve reads while signing no writes at all; each
/// feature is advertised from its own field.
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
