use super::s3_compatible::{S3CompatibleConfig, S3CompatibleStore};
use super::{AsyncObjectStore, ByteRange, ObjectBody, ObjectMetadata, PutMode};
use crate::ObjectStoreError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsS3StoreConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub key_prefix: Option<String>,
    pub force_path_style: bool,
}

#[derive(Debug)]
pub struct AwsS3Store {
    inner: S3CompatibleStore,
}

impl AwsS3Store {
    pub fn new(config: AwsS3StoreConfig) -> Result<Self, ObjectStoreError> {
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
                sha256_checksum_metadata: true,
            })?,
        })
    }
}

#[async_trait]
impl AsyncObjectStore for AwsS3Store {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head_with_checksum(key).await
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
