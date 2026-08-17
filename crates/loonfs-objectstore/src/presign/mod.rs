//! Presigned-URL issuing for direct transfers: `direct_put` uploads,
//! `direct_multipart` part uploads, and `direct_get` downloads.

mod gcs;
mod issuer;
mod s3_compatible;
mod v4;

/// Maximum lifetime accepted by both supported signed-URL schemes.
const MAX_PRESIGN_EXPIRY: u64 = 7 * 24 * 60 * 60;

/// Lifetime of an internally signed checksum-readback request.
pub(crate) const CHECKSUM_HEAD_TTL: std::time::Duration = std::time::Duration::from_secs(60);

pub use gcs::{GcsPresignerConfig, GcsV4Presigner, GCP_GCS_MAX_DIRECT_PUT_BYTES};
pub use issuer::{
    DirectGetIssuer, DirectMultipartIssuer, DirectPutIssuer, DirectTransferIssuers,
    PresignedGetRequest, PresignedPartRequest, PresignedPutRequest, PresignedUrl,
};
pub use s3_compatible::{
    S3CompatiblePresigner, S3PresignerConfig, AWS_S3_MAX_DIRECT_PUT_BYTES,
    CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
};

pub(crate) use gcs::stored_crc32c;
