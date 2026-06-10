use super::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, PutMode};
use crate::fs::LocalFsStore;
use crate::keyspace::{
    normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
};
use crate::r2::{R2Store, R2StoreConfig};
use crate::s3::{AwsS3Store, AwsS3StoreConfig};
use crate::ObjectStoreError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfiguredObjectStoreKind {
    LocalFs,
    AwsS3,
    CloudflareR2,
}

#[derive(Debug)]
pub struct ConfiguredObjectStore {
    kind: ConfiguredObjectStoreKind,
    inner: ConfiguredObjectStoreInner,
}

#[derive(Debug)]
enum ConfiguredObjectStoreInner {
    LocalFs {
        store: LocalFsStore,
        key_prefix: Option<String>,
    },
    AwsS3(AwsS3Store),
    CloudflareR2(R2Store),
}

impl ConfiguredObjectStore {
    pub fn local_fs(
        root: impl Into<PathBuf>,
        key_prefix: Option<&str>,
    ) -> Result<Self, ObjectStoreError> {
        Ok(Self {
            kind: ConfiguredObjectStoreKind::LocalFs,
            inner: ConfiguredObjectStoreInner::LocalFs {
                store: LocalFsStore::new(root)?,
                key_prefix: normalize_key_prefix(key_prefix)?,
            },
        })
    }

    pub fn aws_s3(config: AwsS3StoreConfig) -> Result<Self, ObjectStoreError> {
        let store = AwsS3Store::new(config)?;
        Ok(Self {
            kind: ConfiguredObjectStoreKind::AwsS3,
            inner: ConfiguredObjectStoreInner::AwsS3(store),
        })
    }

    pub fn cloudflare_r2(config: R2StoreConfig) -> Result<Self, ObjectStoreError> {
        let store = R2Store::new(config)?;
        Ok(Self {
            kind: ConfiguredObjectStoreKind::CloudflareR2,
            inner: ConfiguredObjectStoreInner::CloudflareR2(store),
        })
    }

    pub fn kind(&self) -> ConfiguredObjectStoreKind {
        self.kind
    }
}

#[async_trait]
impl ObjectStore for ConfiguredObjectStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .head(&scope_object_key(key_prefix.as_deref(), key)?)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.head(key).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.head(key).await,
        }
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .head_with_checksum(&scope_object_key(key_prefix.as_deref(), key)?)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.head_with_checksum(key).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.head_with_checksum(key).await,
        }
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .get_with_metadata(&scope_object_key(key_prefix.as_deref(), key)?)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.get_with_metadata(key).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.get_with_metadata(key).await,
        }
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .get(&scope_object_key(key_prefix.as_deref(), key)?, range)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.get(key, range).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.get(key, range).await,
        }
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .put(&scope_object_key(key_prefix.as_deref(), key)?, bytes, mode)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.put(key, bytes, mode).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.put(key, bytes, mode).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store
                    .delete(&scope_object_key(key_prefix.as_deref(), key)?)
                    .await
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.delete(key).await,
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.delete(key).await,
        }
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
    use crate::fs::LocalFsStore;
    use crate::keys::namespace_head;
    use crate::r2::R2StoreConfig;
    use crate::s3::AwsS3StoreConfig;
    use crate::ObjectStore;
    use crate::ObjectStoreError;
    use bytes::Bytes;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn configured_local_fs_scopes_optional_key_prefix() {
        let temp_dir = unique_temp_dir("configured-store-local");
        let store = ConfiguredObjectStore::local_fs(&temp_dir, Some("tenant-a"))
            .expect("construct configured local fs store");
        let head_key = namespace_head("ns-1");

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

        let s3 = ConfiguredObjectStore::aws_s3(AwsS3StoreConfig {
            bucket: "bucket".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint_url: Some("http://127.0.0.1:9000".to_owned()),
            access_key_id: "access".to_owned(),
            secret_access_key: "secret".to_owned(),
            session_token: None,
            key_prefix: Some("tenant-a".to_owned()),
            force_path_style: true,
        })
        .expect("construct s3 store");
        assert_eq!(s3.kind(), ConfiguredObjectStoreKind::AwsS3);

        let r2 = ConfiguredObjectStore::cloudflare_r2(R2StoreConfig {
            bucket: "bucket".to_owned(),
            account_id: "account".to_owned(),
            endpoint_url: "https://example.r2.cloudflarestorage.com".to_owned(),
            access_key_id: "access".to_owned(),
            secret_access_key: "secret".to_owned(),
            key_prefix: Some("tenant-a".to_owned()),
        })
        .expect("construct r2 store");
        assert_eq!(r2.kind(), ConfiguredObjectStoreKind::CloudflareR2);
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
        assert!(matches!(error, ObjectStoreError::InvalidKey(_)));
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
}
