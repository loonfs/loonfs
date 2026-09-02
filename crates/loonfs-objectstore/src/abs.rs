//! Azure Blob Storage provider.

use crate::configured::ConfiguredObjectStoreKind;
use crate::endpoint::parse_endpoint_url;
use crate::object_store::Result;
use crate::provider_object_store::CompareToken;
use crate::store_io_runtime::StoreIoRuntime;
use crate::{ObjectStoreError, ProviderObjectStore, ProviderObjectStoreConfig};
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
    /// Shared account key used for request signing.
    pub access_key: SecretString,
    /// Azure-compatible service endpoint override, or `None` for the public Azure endpoint.
    pub endpoint_url: Option<String>,
    /// Logical prefix prepended to every key, or `None` to use the container root.
    pub key_prefix: Option<String>,
}

/// Builds a native Azure Blob adapter with its own bounded HTTP runtime.
pub fn azure_abs(config: AzureAbsStoreConfig) -> Result<ProviderObjectStore> {
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
        let parsed = parse_endpoint_url(endpoint_url)?;
        if parsed.scheme == "http" {
            builder = builder.with_allow_http(true);
        }
        let endpoint_url = if parsed.path.is_empty() {
            format!("{}://{}", parsed.scheme, parsed.authority)
        } else {
            format!("{}://{}/{}", parsed.scheme, parsed.authority, parsed.path)
        };
        builder = builder.with_endpoint(endpoint_url);
    }

    let provider = Arc::new(
        builder
            .build()
            .map_err(|err| ObjectStoreError::Configuration(err.to_string()))?,
    );
    ProviderObjectStore::new(
        Arc::clone(&provider) as Arc<dyn object_store::ObjectStore>,
        provider,
        ProviderObjectStoreConfig {
            key_prefix: config.key_prefix,
        },
        ConfiguredObjectStoreKind::AzureAbs,
        io_runtime,
    )
    .map(|store| store.compare_token(CompareToken::Etag))
}

#[cfg(test)]
mod tests {
    use super::{azure_abs, AzureAbsStoreConfig};
    use crate::test_support::AZURITE_ACCOUNT_KEY;
    use crate::ObjectStore;
    use crate::ObjectStoreError;

    #[test]
    fn http_endpoint_is_allowed_for_emulator() {
        for endpoint_url in [
            "http://127.0.0.1:10000/devstoreaccount1",
            "HTTP://127.0.0.1:10000/devstoreaccount1",
        ] {
            let store = azure_abs(AzureAbsStoreConfig {
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
        let store = azure_abs(AzureAbsStoreConfig {
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
