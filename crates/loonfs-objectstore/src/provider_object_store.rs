use crate::keyspace::{
    normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
};
use crate::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use loonfs_api::sha256_digest;
use object_store as provider_store;
use provider_store::path::Path;
use provider_store::{
    GetOptions, GetRange, ObjectMeta, PutOptions, PutPayload, PutResult, UpdateVersion,
};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderObjectStoreConfig {
    pub key_prefix: Option<String>,
    pub sha256_checksum_metadata: bool,
}

#[derive(Clone)]
pub struct ProviderObjectStore {
    inner: Arc<dyn provider_store::ObjectStore>,
    key_prefix: Option<String>,
    sha256_checksum_metadata: bool,
}

impl fmt::Debug for ProviderObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderObjectStore")
            .field("key_prefix", &self.key_prefix)
            .field("sha256_checksum_metadata", &self.sha256_checksum_metadata)
            .finish_non_exhaustive()
    }
}

impl ProviderObjectStore {
    pub fn new(
        inner: Arc<dyn provider_store::ObjectStore>,
        config: ProviderObjectStoreConfig,
    ) -> Result<Self, ObjectStoreError> {
        Ok(Self {
            inner,
            key_prefix: normalize_key_prefix(config.key_prefix.as_deref())?,
            sha256_checksum_metadata: config.sha256_checksum_metadata,
        })
    }

    fn to_path(&self, key: &str) -> Result<Path, ObjectStoreError> {
        let scoped = scope_object_key(self.key_prefix.as_deref(), key)?;
        Path::parse(scoped).map_err(|err| ObjectStoreError::InvalidKey(err.to_string()))
    }

    fn list_path(&self, prefix: &str) -> Result<Option<Path>, ObjectStoreError> {
        let scoped = scope_list_prefix(self.key_prefix.as_deref(), prefix)?;
        if scoped.is_empty() {
            return Ok(None);
        }
        Path::parse(scoped)
            .map(Some)
            .map_err(|err| ObjectStoreError::InvalidKey(err.to_string()))
    }

    fn from_meta(meta: ObjectMeta, checksum_sha256: Option<String>) -> ObjectMetadata {
        ObjectMetadata {
            etag: meta.e_tag,
            version: meta.version,
            size_bytes: meta.size,
            checksum_sha256,
        }
    }

    fn from_put_result(
        result: PutResult,
        size_bytes: u64,
        checksum_sha256: Option<String>,
    ) -> ObjectMetadata {
        ObjectMetadata {
            etag: result.e_tag,
            version: result.version,
            size_bytes,
            checksum_sha256,
        }
    }

    async fn provider_range(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<ProviderRange>, ObjectStoreError> {
        let Some(range) = range else {
            return Ok(None);
        };
        let metadata = match self.head(key).await? {
            Some(metadata) => metadata,
            None => return Err(ObjectStoreError::NotFound),
        };
        if range.end_exclusive < range.start_inclusive
            || range.start_inclusive > metadata.size_bytes
        {
            return Err(ObjectStoreError::InvalidRange);
        }

        let bounded_end = range.end_exclusive.min(metadata.size_bytes);
        if bounded_end == range.start_inclusive {
            return Ok(Some(ProviderRange::Empty));
        }
        Ok(Some(ProviderRange::Bounded(GetRange::Bounded(Range {
            start: range.start_inclusive,
            end: bounded_end,
        }))))
    }
}

#[async_trait]
impl ObjectStore for ProviderObjectStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let path = self.to_path(key)?;
        match self.inner.head(&path).await {
            Ok(meta) => Ok(Some(Self::from_meta(meta, None))),
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(err)),
        }
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let path = self.to_path(key)?;
        match self.inner.get(&path).await {
            Ok(result) => {
                let metadata = Self::from_meta(result.meta.clone(), None);
                let bytes = result.bytes().await.map_err(map_provider_error)?;
                Ok(Some(ObjectBody {
                    metadata,
                    bytes: bytes.to_vec(),
                }))
            }
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(err)),
        }
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let path = self.to_path(key)?;
        let Some(provider_range) = self.provider_range(key, range).await? else {
            return match self.inner.get(&path).await {
                Ok(result) => result.bytes().await.map(Some).map_err(map_provider_error),
                Err(err) if provider_not_found(&err) => Ok(None),
                Err(err) => Err(map_provider_error(err)),
            };
        };

        let ProviderRange::Bounded(provider_range) = provider_range else {
            return Ok(Some(Bytes::new()));
        };

        let options = GetOptions {
            range: Some(provider_range),
            ..Default::default()
        };
        match self.inner.get_opts(&path, options).await {
            Ok(result) => result.bytes().await.map(Some).map_err(map_provider_error),
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(err)),
        }
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let path = self.to_path(key)?;
        let checksum_sha256 = self.sha256_checksum_metadata.then(|| sha256_digest(&bytes));
        let size_bytes = bytes.len() as u64;
        let options = PutOptions {
            mode: map_put_mode(mode),
            ..Default::default()
        };

        match self
            .inner
            .put_opts(&path, PutPayload::from(bytes), options)
            .await
        {
            Ok(result) => Ok(Self::from_put_result(result, size_bytes, checksum_sha256)),
            Err(err) => Err(map_provider_error(err)),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let path = self.to_path(key)?;
        match self.inner.delete(&path).await {
            Ok(()) => Ok(()),
            Err(err) if provider_not_found(&err) => Ok(()),
            Err(err) => Err(map_provider_error(err)),
        }
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        let prefix_path = match self.list_path(prefix) {
            Ok(prefix_path) => prefix_path,
            Err(err) => return stream::once(async { Err(err) }).boxed(),
        };
        let key_prefix = self.key_prefix.clone();
        self.inner
            .list(prefix_path.as_ref())
            .filter_map(move |result| {
                let key_prefix = key_prefix.clone();
                async move {
                    match result {
                        Ok(meta) => {
                            let key = meta.location.as_ref();
                            match key_prefix.as_deref() {
                                Some(prefix) => unscope_listed_key(Some(prefix), key).map(Ok),
                                None => Some(Ok(key.to_owned())),
                            }
                        }
                        Err(err) => Some(Err(map_provider_error(err))),
                    }
                }
            })
            .boxed()
    }
}

fn map_put_mode(mode: PutMode) -> provider_store::PutMode {
    match mode {
        PutMode::Overwrite => provider_store::PutMode::Overwrite,
        PutMode::CreateIfAbsent => provider_store::PutMode::Create,
        PutMode::CompareAndSwap { expected_etag } => {
            provider_store::PutMode::Update(UpdateVersion {
                e_tag: Some(expected_etag),
                version: None,
            })
        }
    }
}

enum ProviderRange {
    Empty,
    Bounded(GetRange),
}

fn provider_not_found(err: &provider_store::Error) -> bool {
    matches!(err, provider_store::Error::NotFound { .. })
}

fn map_provider_error(err: provider_store::Error) -> ObjectStoreError {
    match err {
        provider_store::Error::NotFound { .. } => ObjectStoreError::NotFound,
        provider_store::Error::AlreadyExists { .. }
        | provider_store::Error::Precondition { .. }
        | provider_store::Error::NotModified { .. } => ObjectStoreError::PreconditionFailed,
        provider_store::Error::InvalidPath { source } => {
            ObjectStoreError::InvalidKey(source.to_string())
        }
        provider_store::Error::NotSupported { .. } | provider_store::Error::NotImplemented => {
            ObjectStoreError::Unsupported("provider object store operation")
        }
        provider_store::Error::Generic { source, .. } => {
            ObjectStoreError::Transport(source.to_string())
        }
        provider_store::Error::JoinError { source } => {
            ObjectStoreError::Transport(source.to_string())
        }
        provider_store::Error::PermissionDenied { source, .. }
        | provider_store::Error::Unauthenticated { source, .. } => {
            ObjectStoreError::Transport(source.to_string())
        }
        provider_store::Error::UnknownConfigurationKey { key, store } => {
            ObjectStoreError::Transport(format!("unknown {store} configuration key `{key}`"))
        }
        other => ObjectStoreError::Transport(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use object_store::memory::InMemory;

    fn memory_store() -> ProviderObjectStore {
        ProviderObjectStore::new(
            Arc::new(InMemory::default()),
            ProviderObjectStoreConfig {
                key_prefix: Some("tenant-a".to_owned()),
                sha256_checksum_metadata: true,
            },
        )
        .expect("provider store")
    }

    #[tokio::test]
    async fn provider_store_preserves_put_get_head_and_prefix_scoping() {
        let store = memory_store();
        let key = "namespaces/demo/control/head.json";

        let metadata = store
            .put_if_absent(key, Bytes::from_static(b"head"))
            .await
            .expect("put");
        assert_eq!(metadata.size_bytes, 4);
        assert!(metadata.etag.is_some());
        assert_eq!(metadata.checksum_sha256, Some(sha256_digest(b"head")));

        let head = store.head(key).await.expect("head").expect("head exists");
        assert_eq!(head.size_bytes, 4);
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"head"))
        );
        assert_eq!(
            store.list_prefix("namespaces/demo/").await.expect("list"),
            vec![key.to_owned()]
        );
    }

    #[tokio::test]
    async fn provider_store_enforces_create_and_cas_preconditions() {
        let store = memory_store();
        let key = "namespaces/demo/control/head.json";
        let first = store
            .put_if_absent(key, Bytes::from_static(b"one"))
            .await
            .expect("first put");

        assert!(matches!(
            store.put_if_absent(key, Bytes::from_static(b"two")).await,
            Err(ObjectStoreError::PreconditionFailed)
        ));
        assert!(matches!(
            store
                .compare_and_swap(key, "stale", Bytes::from_static(b"two"))
                .await,
            Err(ObjectStoreError::PreconditionFailed)
        ));
        let etag = first.etag.expect("etag");
        store
            .compare_and_swap(key, &etag, Bytes::from_static(b"two"))
            .await
            .expect("cas");
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"two"))
        );
    }

    #[tokio::test]
    async fn provider_store_range_semantics_match_blocking_contract() {
        let store = memory_store();
        let key = "content-stores/cs_0123456789abcdef0123456789abcdef/blobs/sha256/ab/cd/abcdef";
        store
            .put_overwrite(key, Bytes::from_static(b"abcdef"))
            .await
            .expect("put");

        assert_eq!(
            store
                .get(
                    key,
                    Some(ByteRange {
                        start_inclusive: 2,
                        end_exclusive: 4,
                    }),
                )
                .await
                .expect("range"),
            Some(Bytes::from_static(b"cd"))
        );
        assert_eq!(
            store
                .get(
                    key,
                    Some(ByteRange {
                        start_inclusive: 6,
                        end_exclusive: 10,
                    }),
                )
                .await
                .expect("empty"),
            Some(Bytes::new())
        );
        assert!(matches!(
            store
                .get(
                    key,
                    Some(ByteRange {
                        start_inclusive: 7,
                        end_exclusive: 8,
                    }),
                )
                .await,
            Err(ObjectStoreError::InvalidRange)
        ));
    }

    #[tokio::test]
    async fn provider_stream_reports_invalid_prefix() {
        let store = memory_store();
        let mut stream = store.list_prefix_stream("../");
        assert!(matches!(
            stream.next().await,
            Some(Err(ObjectStoreError::InvalidKey(_)))
        ));
    }
}
