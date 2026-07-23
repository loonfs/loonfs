//! AWS S3 provider, over the S3-compatible transport.

use super::s3_compatible::{S3CompatibleConfig, S3CompatibleStore};
use super::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, PutMode};
use crate::object_store::Result;
use crate::secret::SecretString;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

/// Supplies explicit credentials, addressing, and key scoping for AWS S3.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsS3StoreConfig {
    /// Bucket that acts as the LoonFS object-store root.
    pub bucket: String,
    /// Signing region passed to the S3 client and presigner.
    pub region: String,
    /// S3-compatible endpoint override, or `None` for the regional AWS endpoint.
    pub endpoint_url: Option<String>,
    /// Access-key id used for SigV4 request signing.
    pub access_key_id: SecretString,
    /// Secret access key used for SigV4 request signing.
    pub secret_access_key: SecretString,
    /// Temporary credential token, or `None` for long-lived credentials.
    pub session_token: Option<SecretString>,
    /// Logical prefix prepended to every key, or `None` to use the bucket root.
    pub key_prefix: Option<String>,
    /// Selects path-style bucket addressing for compatible endpoints that require it.
    pub force_path_style: bool,
}

/// Implements the LoonFS storage contract on AWS S3 or an explicitly configured endpoint.
#[derive(Debug)]
pub struct AwsS3Store {
    inner: S3CompatibleStore,
}

impl AwsS3Store {
    /// Builds an S3 adapter with bounded retries and SHA-256 upload checksums.
    ///
    /// Construction fails for invalid credentials, bucket, region, endpoint,
    /// key prefix, runtime initialization, or provider-client configuration.
    pub fn new(config: AwsS3StoreConfig) -> Result<Self> {
        Ok(Self {
            inner: S3CompatibleStore::new(S3CompatibleConfig {
                provider_name: "aws-s3",
                bucket: config.bucket,
                region: config.region,
                endpoint_url: config.endpoint_url,
                access_key_id: config.access_key_id,
                secret_access_key: config.secret_access_key,
                session_token: config.session_token,
                key_prefix: config.key_prefix,
                force_path_style: config.force_path_style,
                sha256_upload_checksum: true,
            })?,
        })
    }
}

#[async_trait]
impl ObjectStore for AwsS3Store {
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
