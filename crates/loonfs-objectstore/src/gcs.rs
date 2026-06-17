use super::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, PutMode};
use crate::{ObjectStoreError, ProviderObjectStore, ProviderObjectStoreConfig};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::gcp::GoogleCloudStorageBuilder;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsStoreConfig {
    pub bucket: String,
    /// Service account key file. When absent, falls back to
    /// `application_credentials_path`, then the ambient Google credential
    /// chain (environment, application default credentials, metadata server).
    pub service_account_key_path: Option<String>,
    pub application_credentials_path: Option<String>,
    pub key_prefix: Option<String>,
}

/// Google Cloud Storage through its native API.
///
/// GCS conditions writes on object generations, not ETags, and silently
/// ignores HTTP `If-Match`/`If-None-Match` on its S3-interoperability
/// surface — conformance proved interop overwrites instead of failing
/// preconditions. The native API is therefore the only correct backend, and
/// this adapter hands out the generation as the opaque compare token.
#[derive(Debug)]
pub struct GcsStore {
    inner: ProviderObjectStore,
}

impl GcsStore {
    pub fn new(config: GcsStoreConfig) -> Result<Self, ObjectStoreError> {
        if config.bucket.trim().is_empty() {
            return Err(ObjectStoreError::Transport(
                "bucket must not be empty".to_owned(),
            ));
        }

        let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(config.bucket);
        if let Some(path) = config.service_account_key_path {
            builder = builder.with_service_account_path(path);
        }
        if let Some(path) = config.application_credentials_path {
            builder = builder.with_application_credentials(path);
        }

        let provider = builder
            .build()
            .map_err(|err| ObjectStoreError::Transport(err.to_string()))?;
        let inner = ProviderObjectStore::new(
            Arc::new(provider),
            ProviderObjectStoreConfig {
                key_prefix: config.key_prefix,
                sha256_checksum_metadata: false,
            },
        )?;

        Ok(Self { inner })
    }

    fn generation_as_compare_token(metadata: ObjectMetadata) -> ObjectMetadata {
        ObjectMetadata {
            etag: metadata.version.clone(),
            ..metadata
        }
    }

    fn require_generation_compare_token(expected_etag: &str) -> Result<(), ObjectStoreError> {
        expected_etag
            .parse::<u64>()
            .map(|_| ())
            .map_err(|_| ObjectStoreError::PreconditionFailed)
    }
}

#[async_trait]
impl ObjectStore for GcsStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        Ok(self
            .inner
            .head(key)
            .await?
            .map(Self::generation_as_compare_token))
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        Ok(self
            .inner
            .get_with_metadata(key)
            .await?
            .map(|body| ObjectBody {
                metadata: Self::generation_as_compare_token(body.metadata),
                bytes: body.bytes,
            }))
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
        self.inner.validate_key(key)?;
        if let PutMode::CompareAndSwap { expected_etag } = &mode {
            Self::require_generation_compare_token(expected_etag)?;
        }
        Ok(Self::generation_as_compare_token(
            self.inner.put(key, bytes, mode).await?,
        ))
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

#[cfg(test)]
mod tests {
    use super::{GcsStore, GcsStoreConfig};
    use crate::{ObjectStore, ObjectStoreError};
    use bytes::Bytes;

    #[test]
    fn generation_compare_tokens_must_be_numeric() {
        assert!(GcsStore::require_generation_compare_token("1729").is_ok());
        assert!(matches!(
            GcsStore::require_generation_compare_token("not-a-generation"),
            Err(ObjectStoreError::PreconditionFailed)
        ));
    }

    #[tokio::test]
    async fn invalid_keys_are_rejected_before_generation_tokens() {
        let store = GcsStore::new(GcsStoreConfig {
            bucket: "bucket".to_owned(),
            service_account_key_path: None,
            application_credentials_path: None,
            key_prefix: None,
        })
        .expect("construct gcs store");

        assert!(matches!(
            store
                .compare_and_swap("../escape", "not-a-generation", Bytes::from_static(b"oops"))
                .await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
    }
}
