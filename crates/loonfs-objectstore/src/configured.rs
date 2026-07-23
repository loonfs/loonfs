//! [`ConfiguredObjectStore`]: one enum over every provider, built from
//! configuration.

use super::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, PutMode};
use crate::abs::{AzureAbsStore, AzureAbsStoreConfig};
use crate::gcs::{GcpGcsStore, GcpGcsStoreConfig};
use crate::keyspace::{
    normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
};
use crate::local_fs_store::LocalFsStore;
use crate::object_store::Result;
use crate::presign::{ObjectTransferIssuer, S3CompatiblePresigner, S3PresignerConfig};
use crate::r2::{CloudflareR2Store, CloudflareR2StoreConfig};
use crate::s3::{AwsS3Store, AwsS3StoreConfig};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use std::path::PathBuf;
use std::sync::Arc;

/// Identifies the concrete provider backing a [`ConfiguredObjectStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfiguredObjectStoreKind {
    /// Stores objects beneath a local filesystem root.
    LocalFs,
    /// Uses AWS S3 through its SigV4 API.
    AwsS3,
    /// Uses Cloudflare R2 through its S3-compatible API.
    CloudflareR2,
    /// Uses Google Cloud Storage's native generation-aware API.
    GcpGcs,
    /// Uses Azure Blob Storage's native shared-key API.
    AzureAbs,
}

impl ConfiguredObjectStoreKind {
    /// Returns the stable kebab-case label, matching the `kind` tag used in
    /// config files.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalFs => "local-fs",
            Self::AwsS3 => "aws-s3",
            Self::CloudflareR2 => "cloudflare-r2",
            Self::GcpGcs => "gcp-gcs",
            Self::AzureAbs => "azure-abs",
        }
    }
}

/// Dispatches the common storage contract to one validated runtime provider.
#[derive(Debug)]
pub struct ConfiguredObjectStore {
    kind: ConfiguredObjectStoreKind,
    inner: ConfiguredObjectStoreInner,
    transfer_issuer: Option<Arc<dyn ObjectTransferIssuer>>,
}

#[derive(Debug)]
enum ConfiguredObjectStoreInner {
    LocalFs {
        store: LocalFsStore,
        key_prefix: Option<String>,
    },
    AwsS3(AwsS3Store),
    CloudflareR2(CloudflareR2Store),
    GcpGcs(GcpGcsStore),
    AzureAbs(AzureAbsStore),
}

impl ConfiguredObjectStore {
    /// Opens a local-filesystem provider with optional logical key scoping.
    ///
    /// Construction fails when the root cannot be created or `key_prefix` is invalid.
    pub fn local_fs(root: impl Into<PathBuf>, key_prefix: Option<&str>) -> Result<Self> {
        Ok(Self {
            kind: ConfiguredObjectStoreKind::LocalFs,
            inner: ConfiguredObjectStoreInner::LocalFs {
                store: LocalFsStore::new(root)?,
                key_prefix: normalize_key_prefix(key_prefix)?,
            },
            transfer_issuer: None,
        })
    }

    /// Builds an AWS S3 store and matching direct-transfer presigner.
    ///
    /// Construction fails when signing, provider, runtime, or key-prefix configuration is invalid.
    pub fn aws_s3(config: AwsS3StoreConfig) -> Result<Self> {
        let transfer_issuer = Some(Arc::new(S3CompatiblePresigner::new(S3PresignerConfig {
            bucket: config.bucket.clone(),
            region: config.region.clone(),
            endpoint_url: config.endpoint_url.clone(),
            access_key_id: config.access_key_id.clone(),
            secret_access_key: config.secret_access_key.clone(),
            session_token: config.session_token.clone(),
            key_prefix: config.key_prefix.clone(),
            force_path_style: config.force_path_style,
        })?) as Arc<dyn ObjectTransferIssuer>);
        let store = AwsS3Store::new(config)?;
        Ok(Self {
            kind: ConfiguredObjectStoreKind::AwsS3,
            inner: ConfiguredObjectStoreInner::AwsS3(store),
            transfer_issuer,
        })
    }

    /// Builds a Cloudflare R2 store and matching direct-transfer presigner.
    ///
    /// Construction fails when signing, provider, runtime, or key-prefix configuration is invalid.
    pub fn cloudflare_r2(config: CloudflareR2StoreConfig) -> Result<Self> {
        let transfer_issuer = Some(Arc::new(S3CompatiblePresigner::new(S3PresignerConfig {
            bucket: config.bucket.clone(),
            region: "auto".to_owned(),
            endpoint_url: Some(config.endpoint_url.clone()),
            access_key_id: config.access_key_id.clone(),
            secret_access_key: config.secret_access_key.clone(),
            session_token: None,
            key_prefix: config.key_prefix.clone(),
            force_path_style: true,
        })?) as Arc<dyn ObjectTransferIssuer>);
        let store = CloudflareR2Store::new(config)?;
        Ok(Self {
            kind: ConfiguredObjectStoreKind::CloudflareR2,
            inner: ConfiguredObjectStoreInner::CloudflareR2(store),
            transfer_issuer,
        })
    }

    /// Builds a native GCS store without direct-transfer issuance.
    ///
    /// Construction fails when credentials, provider runtime, or key-prefix configuration is invalid.
    pub fn gcp_gcs(config: GcpGcsStoreConfig) -> Result<Self> {
        let store = GcpGcsStore::new(config)?;
        Ok(Self {
            kind: ConfiguredObjectStoreKind::GcpGcs,
            inner: ConfiguredObjectStoreInner::GcpGcs(store),
            transfer_issuer: None,
        })
    }

    /// Builds a native Azure Blob store without direct-transfer issuance.
    ///
    /// Construction fails when credentials, provider runtime, or key-prefix configuration is invalid.
    pub fn azure_abs(config: AzureAbsStoreConfig) -> Result<Self> {
        let store = AzureAbsStore::new(config)?;
        Ok(Self {
            kind: ConfiguredObjectStoreKind::AzureAbs,
            inner: ConfiguredObjectStoreInner::AzureAbs(store),
            transfer_issuer: None,
        })
    }

    /// Returns the provider selected when this store was constructed.
    pub fn kind(&self) -> ConfiguredObjectStoreKind {
        self.kind
    }

    /// Returns a direct-upload issuer for supported S3-compatible providers.
    ///
    /// Local filesystem, GCS, and Azure stores return `None`.
    pub fn transfer_issuer(&self) -> Option<Arc<dyn ObjectTransferIssuer>> {
        self.transfer_issuer.clone()
    }
}

#[async_trait]
impl ObjectStore for ConfiguredObjectStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .head(&scope_object_key(key_prefix.as_deref(), key)?)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.head(key).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.head(key).await,
            ConfiguredObjectStoreInner::GcpGcs(store) => store.head(key).await,
            ConfiguredObjectStoreInner::AzureAbs(store) => store.head(key).await,
        }
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .get_with_metadata(&scope_object_key(key_prefix.as_deref(), key)?)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.get_with_metadata(key).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.get_with_metadata(key).await,
            ConfiguredObjectStoreInner::GcpGcs(store) => store.get_with_metadata(key).await,
            ConfiguredObjectStoreInner::AzureAbs(store) => store.get_with_metadata(key).await,
        }
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .get(&scope_object_key(key_prefix.as_deref(), key)?, range)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.get(key, range).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.get(key, range).await,
            ConfiguredObjectStoreInner::GcpGcs(store) => store.get(key, range).await,
            ConfiguredObjectStoreInner::AzureAbs(store) => store.get(key, range).await,
        }
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .put(&scope_object_key(key_prefix.as_deref(), key)?, bytes, mode)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.put(key, bytes, mode).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.put(key, bytes, mode).await,
            ConfiguredObjectStoreInner::GcpGcs(store) => store.put(key, bytes, mode).await,
            ConfiguredObjectStoreInner::AzureAbs(store) => store.put(key, bytes, mode).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .delete(&scope_object_key(key_prefix.as_deref(), key)?)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.delete(key).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.delete(key).await,
            ConfiguredObjectStoreInner::GcpGcs(store) => store.delete(key).await,
            ConfiguredObjectStoreInner::AzureAbs(store) => store.delete(key).await,
        }
    }

    fn list_prefix_stream(&self, prefix: &str) -> BoxStream<'static, Result<String>> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                let scoped_prefix = match scope_list_prefix(key_prefix.as_deref(), prefix) {
                    Ok(prefix) => prefix,
                    Err(err) => return stream::once(async { Err(err) }).boxed(),
                };
                let key_prefix = key_prefix.clone();
                store
                    .list_prefix_stream(&scoped_prefix)
                    .filter_map(move |result| {
                        let key_prefix = key_prefix.clone();
                        async move {
                            match result {
                                Ok(key) => match key_prefix.as_deref() {
                                    Some(prefix) => unscope_listed_key(Some(prefix), &key).map(Ok),
                                    None => Some(Ok(key)),
                                },
                                Err(err) => Some(Err(err)),
                            }
                        }
                    })
                    .boxed()
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.list_prefix_stream(prefix),
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.list_prefix_stream(prefix),
            ConfiguredObjectStoreInner::GcpGcs(store) => store.list_prefix_stream(prefix),
            ConfiguredObjectStoreInner::AzureAbs(store) => store.list_prefix_stream(prefix),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
    use crate::abs::AzureAbsStoreConfig;
    use crate::gcs::GcpGcsStoreConfig;
    use crate::keys::wal_head;
    use crate::local_fs_store::LocalFsStore;
    use crate::presign::PresignedPutRequest;
    use crate::r2::CloudflareR2StoreConfig;
    use crate::s3::AwsS3StoreConfig;
    use crate::ObjectStore;
    use crate::ObjectStoreError;
    use bytes::Bytes;
    use loonfs_api::ContentRef;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const AZURITE_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    const FAKE_GCS_SERVICE_ACCOUNT_KEY: &str = r#"{"private_key":"private_key","private_key_id":"private_key_id","client_email":"client_email","disable_oauth":true}"#;

    #[tokio::test]
    async fn configured_local_fs_scopes_optional_key_prefix() {
        let temp_dir = unique_temp_dir("configured-store-local");
        let store = ConfiguredObjectStore::local_fs(&temp_dir, Some("tenant-a"))
            .expect("construct configured local fs store");
        let head_key = wal_head("ns-1");

        store
            .put_overwrite(&head_key, Bytes::from_static(br#"{"ok":true}"#))
            .await
            .expect("write scoped object");

        let raw_store = LocalFsStore::new(&temp_dir).expect("open raw store");
        assert!(raw_store
            .head(&format!("tenant-a/{head_key}"))
            .await
            .expect("head raw scoped object")
            .is_some());
        assert_eq!(
            store
                .list_prefix("namespaces/ns-1/")
                .await
                .expect("list scoped prefix"),
            vec![head_key]
        );
    }

    #[test]
    fn configured_object_store_builds_provider_variants() {
        let local = ConfiguredObjectStore::local_fs(unique_temp_dir("configured-store-kind"), None)
            .expect("construct local store");
        assert_eq!(local.kind(), ConfiguredObjectStoreKind::LocalFs);
        assert!(local.transfer_issuer().is_none());

        let s3 = ConfiguredObjectStore::aws_s3(AwsS3StoreConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: Some("http://127.0.0.1:9000".to_owned()),
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
            session_token: None,
            key_prefix: Some("tenant-a".to_owned()),
            force_path_style: true,
        })
        .expect("construct s3 store");
        assert_eq!(s3.kind(), ConfiguredObjectStoreKind::AwsS3);
        assert!(s3.transfer_issuer().is_some());

        let r2 = ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
            bucket: "bucket".to_owned(),
            account_id: "account".to_owned(),
            endpoint_url: "https://example.r2.cloudflarestorage.com".to_owned(),
            access_key_id: "debug-access-key".into(),
            secret_access_key: "secret".into(),
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct r2 store");
        assert_eq!(r2.kind(), ConfiguredObjectStoreKind::CloudflareR2);
        assert!(r2.transfer_issuer().is_some());

        let gcs_service_account_key_path =
            fake_gcs_service_account_key_file("configured-store-gcs-kind");
        let gcs = ConfiguredObjectStore::gcp_gcs(GcpGcsStoreConfig {
            bucket: "bucket".to_owned(),
            service_account_key_path: gcs_service_account_key_path.display().to_string(),
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct gcs store");
        assert_eq!(gcs.kind(), ConfiguredObjectStoreKind::GcpGcs);
        assert!(gcs.transfer_issuer().is_none());

        let azure = ConfiguredObjectStore::azure_abs(AzureAbsStoreConfig {
            account_name: "devstoreaccount1".to_owned(),
            container_name: "container".to_owned(),
            access_key: AZURITE_ACCOUNT_KEY.into(),
            endpoint_url: None,
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct azure store");
        assert_eq!(azure.kind(), ConfiguredObjectStoreKind::AzureAbs);
        assert!(azure.transfer_issuer().is_none());
    }

    #[test]
    fn cloudflare_r2_presigner_uses_path_style_account_endpoint() {
        let store = ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
            bucket: "bucket".to_owned(),
            account_id: "account".to_owned(),
            endpoint_url: "https://account.r2.cloudflarestorage.com".to_owned(),
            access_key_id: "access".into(),
            secret_access_key: "secret".into(),
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct r2 store");
        let issuer = store.transfer_issuer().expect("r2 presigner");

        let signed = issuer
            .presign_put(
                PresignedPutRequest {
                    object_key: "content-stores/cs/blobs/sha256/ab/cd/digest",
                    content_ref: &ContentRef::whole_file_v0(b"hello"),
                    expires_in: Duration::from_secs(900),
                },
                UNIX_EPOCH + Duration::from_secs(1_700_000_000),
            )
            .expect("presign");

        assert!(signed.url.starts_with(
            "https://account.r2.cloudflarestorage.com/bucket/tenant-a/content-stores/"
        ));
        assert!(!signed.url.starts_with("https://bucket.account."));
    }

    #[test]
    fn configured_object_store_debug_redacts_presigner_credentials() {
        let store = ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
            bucket: "bucket".to_owned(),
            account_id: "account".to_owned(),
            endpoint_url: "https://account.r2.cloudflarestorage.com".to_owned(),
            access_key_id: "access".into(),
            secret_access_key: "debug-secret".into(),
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct r2 store");

        let rendered = format!("{store:?}");

        assert!(!rendered.contains("debug-secret"));
        assert!(!rendered.contains("debug-access-key"));
    }

    #[tokio::test]
    async fn configured_local_fs_preserves_invalid_key_errors() {
        let temp_dir = unique_temp_dir("configured-store-invalid-key");
        let store = ConfiguredObjectStore::local_fs(&temp_dir, Some("tenant-a"))
            .expect("construct configured local fs store");

        let error = store
            .put_overwrite("../head.json", Bytes::from_static(br#"{"ok":true}"#))
            .await
            .expect_err("traversal key should be rejected");
        assert!(matches!(
            error,
            ObjectStoreError::InvalidKey { object_key, .. } if object_key == "../head.json"
        ));
    }

    #[allow(clippy::disallowed_methods)]
    fn unique_temp_dir(label: &str) -> PathBuf {
        // Test-only unique paths are an entropy boundary, not protocol time.
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("loonfs-objectstore-{label}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn fake_gcs_service_account_key_file(label: &str) -> PathBuf {
        let path = unique_temp_dir(label).join("service-account.json");
        fs::write(&path, FAKE_GCS_SERVICE_ACCOUNT_KEY).expect("write fake service account key");
        path
    }
}
