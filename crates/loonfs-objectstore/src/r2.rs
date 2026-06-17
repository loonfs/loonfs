use super::s3_compatible::{S3CompatibleConfig, S3CompatibleStore};
use super::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, PutMode};
use crate::ObjectStoreError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct R2StoreConfig {
    pub bucket: String,
    pub account_id: String,
    pub endpoint_url: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub key_prefix: Option<String>,
}

#[derive(Debug)]
pub struct R2Store {
    inner: S3CompatibleStore,
}

impl R2Store {
    pub fn new(config: R2StoreConfig) -> Result<Self, ObjectStoreError> {
        if config.account_id.trim().is_empty() {
            return Err(ObjectStoreError::Transport(
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
                sha256_checksum_metadata: false,
            })?,
        })
    }
}

#[async_trait]
impl ObjectStore for R2Store {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}
