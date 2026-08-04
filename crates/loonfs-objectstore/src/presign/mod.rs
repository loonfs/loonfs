//! Presigned-URL issuing for direct transfers: `direct_put` uploads,
//! `direct_multipart` part uploads, and `direct_get` downloads.

mod aws_sigv4;
mod issuer;
mod s3_compatible;

pub use issuer::{
    DirectGetIssuer, DirectMultipartIssuer, DirectPutIssuer, DirectTransferIssuers,
    PresignedGetRequest, PresignedPartRequest, PresignedPutRequest, PresignedUrl,
};
pub use s3_compatible::{
    S3CompatiblePresigner, S3PresignerConfig, AWS_S3_MAX_DIRECT_PUT_BYTES,
    CLOUDFLARE_R2_MAX_DIRECT_PUT_BYTES,
};
