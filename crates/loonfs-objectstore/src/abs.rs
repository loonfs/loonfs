use super::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, PutMode};
use crate::{ObjectStoreError, ProviderObjectStore, ProviderObjectStoreConfig};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::azure::MicrosoftAzureBuilder;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbsStoreConfig {
    pub account_name: String,
    pub container_name: String,
    pub access_key: String,
    pub key_prefix: Option<String>,
}

/// Azure Blob Storage through its native API.
///
/// LoonFS intentionally uses exactly one Azure auth path: storage-account
/// access-key signing. This adapter does not read credential environment
/// variables, SAS tokens, managed identity, Azure CLI, or anonymous fallback.
#[derive(Debug)]
pub struct AbsStore {
    inner: ProviderObjectStore,
}

impl AbsStore {
    pub fn new(config: AbsStoreConfig) -> Result<Self, ObjectStoreError> {
        if config.account_name.trim().is_empty() {
            return Err(ObjectStoreError::Transport(
                "account name must not be empty".to_owned(),
            ));
        }
        if config.container_name.trim().is_empty() {
            return Err(ObjectStoreError::Transport(
                "container name must not be empty".to_owned(),
            ));
        }
        if config.access_key.trim().is_empty() {
            return Err(ObjectStoreError::Transport(
                "access key must not be empty".to_owned(),
            ));
        }

        let builder = MicrosoftAzureBuilder::new()
            .with_account(config.account_name)
            .with_container_name(config.container_name)
            .with_access_key(config.access_key);

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
}

#[async_trait]
impl ObjectStore for AbsStore {
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

#[cfg(test)]
mod tests {
    use super::{AbsStore, AbsStoreConfig};
    use crate::{ObjectStore, ObjectStoreError};
    use bytes::Bytes;

    const FAKE_ACCESS_KEY: &str = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    #[tokio::test]
    async fn invalid_keys_are_rejected_before_provider_conditions() {
        let store = AbsStore::new(AbsStoreConfig {
            account_name: "account".to_owned(),
            container_name: "container".to_owned(),
            access_key: FAKE_ACCESS_KEY.to_owned(),
            key_prefix: None,
        })
        .expect("construct azure abs store");

        assert!(matches!(
            store
                .compare_and_swap("../escape", "not-an-etag", Bytes::from_static(b"oops"))
                .await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
    }

    #[test]
    fn account_name_is_required() {
        assert!(matches!(
            AbsStore::new(AbsStoreConfig {
                account_name: " ".to_owned(),
                container_name: "container".to_owned(),
                access_key: FAKE_ACCESS_KEY.to_owned(),
                key_prefix: None,
            }),
            Err(ObjectStoreError::Transport(_))
        ));
    }

    #[test]
    fn container_name_is_required() {
        assert!(matches!(
            AbsStore::new(AbsStoreConfig {
                account_name: "account".to_owned(),
                container_name: " ".to_owned(),
                access_key: FAKE_ACCESS_KEY.to_owned(),
                key_prefix: None,
            }),
            Err(ObjectStoreError::Transport(_))
        ));
    }

    #[test]
    fn access_key_is_required() {
        assert!(matches!(
            AbsStore::new(AbsStoreConfig {
                account_name: "account".to_owned(),
                container_name: "container".to_owned(),
                access_key: " ".to_owned(),
                key_prefix: None,
            }),
            Err(ObjectStoreError::Transport(_))
        ));
    }
}
