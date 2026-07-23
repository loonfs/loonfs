//! Cloudflare R2 provider, over the S3-compatible transport.

use super::s3_compatible::{S3CompatibleConfig, S3CompatibleStore};
use super::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, PutMode};
use crate::object_store::Result;
use crate::secret::SecretString;
use crate::ObjectStoreError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

/// Supplies explicit S3 credentials and account addressing for Cloudflare R2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudflareR2StoreConfig {
    /// R2 bucket that acts as the LoonFS object-store root.
    pub bucket: String,
    /// Cloudflare account identity, required even though requests use the explicit endpoint.
    pub account_id: String,
    /// Account-level R2 S3 endpoint; this adapter always uses path-style bucket addressing.
    pub endpoint_url: String,
    /// S3-compatible access-key id used for request signing.
    pub access_key_id: SecretString,
    /// S3-compatible secret used for request signing.
    pub secret_access_key: SecretString,
    /// Logical prefix prepended to every key, or `None` to use the bucket root.
    pub key_prefix: Option<String>,
}

/// Implements the LoonFS storage contract on Cloudflare R2's S3-compatible API.
#[derive(Debug)]
pub struct CloudflareR2Store {
    inner: S3CompatibleStore,
}

impl CloudflareR2Store {
    /// Builds an R2 adapter with path-style addressing and checksum-compatible upload settings.
    ///
    /// Construction fails for a blank account id or any invalid shared
    /// S3-compatible configuration, including credentials, endpoint, and key prefix.
    pub fn new(config: CloudflareR2StoreConfig) -> Result<Self> {
        if config.account_id.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "account id must not be empty".to_owned(),
            ));
        }

        Ok(Self {
            inner: S3CompatibleStore::new(S3CompatibleConfig {
                provider_name: "cloudflare-r2",
                bucket: config.bucket,
                region: "auto".to_owned(),
                endpoint_url: Some(config.endpoint_url),
                access_key_id: config.access_key_id,
                secret_access_key: config.secret_access_key,
                session_token: None,
                key_prefix: config.key_prefix,
                // The configured endpoint is the bucket-less account host;
                // path style makes the client append the bucket. Virtual
                // hosting would use the endpoint verbatim and address keys
                // as buckets.
                force_path_style: true,
                // Left off pending a live verification: R2 historically
                // rejected aws-chunked checksummed uploads, yet its
                // presigner requires `x-amz-checksum-sha256` on direct
                // puts — so R2 provably accepts the checksum and this is
                // likely stale caution. Verify single-part and multipart
                // against live R2 before enabling.
                sha256_upload_checksum: false,
            })?,
        })
    }
}

#[async_trait]
impl ObjectStore for CloudflareR2Store {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        self.inner.get_with_metadata(key).await
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        self.inner.get(key, range).await
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(&self, prefix: &str) -> BoxStream<'static, Result<String>> {
        self.inner.list_prefix_stream(prefix)
    }
}
