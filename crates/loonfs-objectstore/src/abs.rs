//! Azure Blob Storage provider.

use super::{ByteRange, ByteStream, ObjectBody, ObjectMetadata, ObjectStore, PutMode};
use crate::object_store::Result;
use crate::store_io_runtime::StoreIoRuntime;
use crate::{ObjectStoreError, ProviderObjectStore, ProviderObjectStoreConfig};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::SecretString;
use object_store::azure::MicrosoftAzureBuilder;
use std::sync::Arc;

/// Supplies explicit credentials and key scoping for the native Azure Blob adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureAbsStoreConfig {
    /// Storage account used for both request addressing and shared-key signing.
    pub account_name: String,
    /// Blob container that acts as the LoonFS object-store root.
    pub container_name: String,
    /// Shared account key; empty or whitespace-only credentials are rejected.
    pub access_key: SecretString,
    /// Azure-compatible service endpoint override, or `None` for the public Azure endpoint.
    pub endpoint_url: Option<String>,
    /// Logical prefix prepended to every key, or `None` to use the container root.
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
    /// Keeps the HTTP IO runtime alive for the provider client's lifetime;
    /// the connector inside the client holds only a handle onto it.
    _io_runtime: StoreIoRuntime,
}

impl AzureAbsStore {
    /// Builds a native Azure Blob adapter with its own bounded HTTP runtime.
    ///
    /// Construction fails for blank account, container, key, or endpoint
    /// values, an invalid key prefix, runtime initialization, or provider-client configuration.
    pub fn new(config: AzureAbsStoreConfig) -> Result<Self> {
        if config.account_name.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "account name must not be empty".to_owned(),
            ));
        }
        if config.container_name.trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "container name must not be empty".to_owned(),
            ));
        }
        if config.access_key.expose().trim().is_empty() {
            return Err(ObjectStoreError::Configuration(
                "access key must not be empty".to_owned(),
            ));
        }

        let io_runtime = StoreIoRuntime::new()?;
        let mut builder = MicrosoftAzureBuilder::new()
            .with_http_connector(io_runtime.connector())
            .with_client_options(crate::provider_object_store::provider_client_options())
            .with_retry(crate::provider_object_store::provider_retry_config())
            .with_account(config.account_name)
            .with_container_name(config.container_name)
            .with_access_key(config.access_key.expose());
        if let Some(endpoint_url) = config.endpoint_url {
            let endpoint_url = endpoint_url.trim();
            if endpoint_url.is_empty() {
                return Err(ObjectStoreError::Configuration(
                    "endpoint url must not be empty".to_owned(),
                ));
            }
            let endpoint_url = normalize_http_endpoint_scheme(endpoint_url);
            if endpoint_url.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
            builder = builder.with_endpoint(endpoint_url);
        }

        let provider = Arc::new(
            builder
                .build()
                .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?,
        );
        let inner = ProviderObjectStore::new(
            Arc::clone(&provider) as Arc<dyn object_store::ObjectStore>,
            Some(provider),
            ProviderObjectStoreConfig {
                key_prefix: config.key_prefix,
            },
        )?;
        Ok(Self {
            inner,
            _io_runtime: io_runtime,
        })
    }
}

fn normalize_http_endpoint_scheme(endpoint_url: &str) -> String {
    match endpoint_url.split_once("://") {
        Some((scheme, rest)) if scheme.eq_ignore_ascii_case("http") => format!("http://{rest}"),
        Some((scheme, rest)) if scheme.eq_ignore_ascii_case("https") => format!("https://{rest}"),
        _ => endpoint_url.to_owned(),
    }
}

#[async_trait]
impl ObjectStore for AzureAbsStore {
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

    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        self.inner.put_streamed(key, body, mode).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String>> {
        // Azure has no native start-after key, so the provider adapter filters
        // results while following its normal continuation pages.
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

#[cfg(test)]
mod tests {
    use super::{AzureAbsStore, AzureAbsStoreConfig};
    use crate::test_support::AZURITE_ACCOUNT_KEY;
    use crate::ObjectStore;
    use crate::ObjectStoreError;

    #[test]
    fn access_key_is_required() {
        let error = AzureAbsStore::new(AzureAbsStoreConfig {
            account_name: "account".to_owned(),
            container_name: "container".to_owned(),
            access_key: " ".into(),
            endpoint_url: None,
            key_prefix: None,
        })
        .expect_err("blank access key should be rejected");

        assert!(
            matches!(error, ObjectStoreError::Configuration(message) if message.contains("access key"))
        );
    }

    #[test]
    fn http_endpoint_is_allowed_for_emulator() {
        for endpoint_url in [
            "http://127.0.0.1:10000/devstoreaccount1",
            "HTTP://127.0.0.1:10000/devstoreaccount1",
        ] {
            let store = AzureAbsStore::new(AzureAbsStoreConfig {
                account_name: "devstoreaccount1".to_owned(),
                container_name: "container".to_owned(),
                access_key: AZURITE_ACCOUNT_KEY.into(),
                endpoint_url: Some(endpoint_url.to_owned()),
                key_prefix: None,
            });
            assert!(
                store.is_ok(),
                "an emulator endpoint spelled {endpoint_url} should build a store"
            );
        }
    }

    #[tokio::test]
    async fn invalid_keys_are_rejected_before_compare_tokens() {
        let store = AzureAbsStore::new(AzureAbsStoreConfig {
            account_name: "devstoreaccount1".to_owned(),
            container_name: "container".to_owned(),
            access_key: AZURITE_ACCOUNT_KEY.into(),
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

        assert!(matches!(
            error,
            ObjectStoreError::InvalidKey { object_key, .. } if object_key == "../escape"
        ));
    }
}
