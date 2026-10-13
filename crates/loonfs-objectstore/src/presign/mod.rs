//! Presigned-URL issuing for direct transfers: `direct_put` uploads,
//! `direct_multipart` part uploads, and `direct_get` downloads.

mod gcs;
mod issuer;
mod limits;
mod s3_compatible;
mod v4;

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
pub(crate) use limits::CHECKSUM_HEAD_TTL;
pub(crate) use s3_compatible::base64_crc64nvme;
pub(crate) use v4::percent_encode_segment;
