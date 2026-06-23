use crate::ObjectStoreError;
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

pub trait ObjectTransferIssuer: Send + Sync + std::fmt::Debug {
    fn presign_put(
        &self,
        request: PresignedPutRequest<'_>,
        now: SystemTime,
    ) -> Result<PresignedUrl, ObjectStoreError>;
}
