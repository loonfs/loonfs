use super::s3_compatible::{S3CompatibleConfig, S3CompatibleStore};
use super::{ByteRange, ObjectMetadata, ObjectStore, PutMode};
use crate::ObjectStoreError;

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
            })?,
        })
    }
}

impl ObjectStore for AwsS3Store {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn head_with_checksum(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head_with_checksum(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}
