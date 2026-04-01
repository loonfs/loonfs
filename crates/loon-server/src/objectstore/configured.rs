use crate::objectstore::error::ObjectStoreError;
use crate::objectstore::fs::LocalFsStore;
use crate::objectstore::keyspace::{
    normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
};
use crate::objectstore::r2::{R2Store, R2StoreConfig};
use crate::objectstore::s3::{AwsS3Store, AwsS3StoreConfig};
use super::{ByteRange, ObjectMetadata, ObjectStore, PutMode};
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
        Ok(Self {
            kind: ConfiguredObjectStoreKind::AwsS3,
            inner: ConfiguredObjectStoreInner::AwsS3(AwsS3Store::new(config)?),
        })
    }

    pub fn cloudflare_r2(config: R2StoreConfig) -> Result<Self, ObjectStoreError> {
        Ok(Self {
            kind: ConfiguredObjectStoreKind::CloudflareR2,
            inner: ConfiguredObjectStoreInner::CloudflareR2(R2Store::new(config)?),
        })
    }

    pub fn kind(&self) -> ConfiguredObjectStoreKind {
        self.kind
    }
}

impl ObjectStore for ConfiguredObjectStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store.head(&scope_object_key(key_prefix.as_deref(), key)?)
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.head(key),
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.head(key),
        }
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store.get(&scope_object_key(key_prefix.as_deref(), key)?, range)
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.get(key, range),
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.get(key, range),
        }
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store.put(&scope_object_key(key_prefix.as_deref(), key)?, bytes, mode)
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.put(key, bytes, mode),
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.put(key, bytes, mode),
        }
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                store.delete(&scope_object_key(key_prefix.as_deref(), key)?)
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.delete(key),
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.delete(key),
        }
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        match &self.inner {
            ConfiguredObjectStoreInner::LocalFs { store, key_prefix } => {
                let listed =
                    store.list_prefix(&scope_list_prefix(key_prefix.as_deref(), prefix)?)?;
                Ok(match key_prefix.as_deref() {
                    Some(scoped_prefix) => listed
                        .into_iter()
                        .filter_map(|key| unscope_listed_key(Some(scoped_prefix), &key))
                        .collect(),
                    None => listed,
                })
            }
            ConfiguredObjectStoreInner::AwsS3(store) => store.list_prefix(prefix),
            ConfiguredObjectStoreInner::CloudflareR2(store) => store.list_prefix(prefix),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfiguredObjectStore, ConfiguredObjectStoreKind};
    use crate::objectstore::error::ObjectStoreError;
    use crate::objectstore::fs::LocalFsStore;
    use crate::objectstore::r2::R2StoreConfig;
    use crate::objectstore::s3::AwsS3StoreConfig;
    use crate::objectstore::ObjectStore;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn configured_local_fs_scopes_optional_key_prefix() {
        let temp_dir = unique_temp_dir("configured-store-local");
        let store = ConfiguredObjectStore::local_fs(&temp_dir, Some("tenant-a"))
            .expect("construct configured local fs store");

        store
            .put_overwrite("namespaces/ns-1/head.json", br#"{"ok":true}"#)
            .expect("write scoped object");

        let raw_store = LocalFsStore::new(&temp_dir).expect("open raw store");
        assert!(raw_store
            .head("tenant-a/namespaces/ns-1/head.json")
            .expect("head raw scoped object")
            .is_some());
        assert_eq!(
            store
                .list_prefix("namespaces/ns-1/")
                .expect("list scoped prefix"),
            vec!["namespaces/ns-1/head.json".to_owned()]
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

    #[test]
    fn configured_local_fs_preserves_invalid_key_errors() {
        let temp_dir = unique_temp_dir("configured-store-invalid-key");
        let store = ConfiguredObjectStore::local_fs(&temp_dir, Some("tenant-a"))
            .expect("construct configured local fs store");

        let error = store
            .put_overwrite("../head.json", br#"{"ok":true}"#)
            .expect_err("traversal key should be rejected");
        assert!(matches!(error, ObjectStoreError::InvalidKey(_)));
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("loondb-objectstore-{label}-{stamp}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
