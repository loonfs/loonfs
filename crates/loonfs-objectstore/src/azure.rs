use super::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, PutMode};
use crate::{ObjectStoreError, ProviderObjectStore, ProviderObjectStoreConfig};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::azure::MicrosoftAzureBuilder;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureAbsStoreConfig {
    pub account_name: String,
    pub container_name: String,
    pub access_key: String,
    pub endpoint_url: Option<String>,
    pub key_prefix: Option<String>,
}

/// Azure Blob Storage through its native API.
///
/// Authentication intentionally has one path: the caller must provide an
/// account name and account access key explicitly. The adapter does not use SAS
/// tokens, bearer tokens, managed identity, Azure CLI credentials, or ambient
/// environment fallback.
#[derive(Debug)]
pub struct AzureAbsStore {
    inner: ProviderObjectStore,
}

impl AzureAbsStore {
    pub fn new(config: AzureAbsStoreConfig) -> Result<Self, ObjectStoreError> {
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

        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(config.account_name)
            .with_container_name(config.container_name)
            .with_access_key(config.access_key);
        if let Some(endpoint_url) = config.endpoint_url {
            let endpoint_url = endpoint_url.trim();
            if endpoint_url.is_empty() {
                return Err(ObjectStoreError::Transport(
                    "endpoint url must not be empty".to_owned(),
                ));
            }
            if endpoint_url.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
            builder = builder.with_endpoint(endpoint_url.to_owned());
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
}

#[async_trait]
impl ObjectStore for AzureAbsStore {
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
    use super::{AzureAbsStore, AzureAbsStoreConfig};
    use crate::ObjectStore;
    use crate::ObjectStoreError;

    const AZURITE_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    #[test]
    fn access_key_is_required() {
        let error = AzureAbsStore::new(AzureAbsStoreConfig {
            account_name: "account".to_owned(),
            container_name: "container".to_owned(),
            access_key: " ".to_owned(),
            endpoint_url: None,
            key_prefix: None,
        })
        .expect_err("blank access key should be rejected");

        assert!(
            matches!(error, ObjectStoreError::Transport(message) if message.contains("access key"))
        );
    }

    #[test]
    fn http_endpoint_is_allowed_for_emulator() {
        AzureAbsStore::new(AzureAbsStoreConfig {
            account_name: "devstoreaccount1".to_owned(),
            container_name: "container".to_owned(),
            access_key: AZURITE_ACCOUNT_KEY.to_owned(),
            endpoint_url: Some("http://127.0.0.1:10000/devstoreaccount1".to_owned()),
            key_prefix: None,
        })
        .expect("construct azure store with HTTP endpoint");
    }

    #[tokio::test]
    async fn invalid_keys_are_rejected_before_compare_tokens() {
        let store = AzureAbsStore::new(AzureAbsStoreConfig {
            account_name: "devstoreaccount1".to_owned(),
            container_name: "container".to_owned(),
            access_key: AZURITE_ACCOUNT_KEY.to_owned(),
            endpoint_url: None,
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct azure store");

        let error = store
            .compare_and_swap(
                "../escape",
                "not-an-etag",
                bytes::Bytes::from_static(br#"{"seq":1}"#),
            )
            .await
            .expect_err("invalid key should be rejected before provider request");

        assert!(matches!(error, ObjectStoreError::InvalidKey(_)));
    }
}
