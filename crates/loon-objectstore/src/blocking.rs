use crate::object_store::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStoreError, PutMode, SharedObjectStore,
};
use bytes::Bytes;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

/// Temporary synchronous compatibility facade for current callers.
///
/// New storage/cache/table code should depend on the async [`ObjectStore`] trait instead.
pub trait BlockingObjectStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError>;
    fn head_with_checksum(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.head(key)
    }
    fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError>;
    fn get(&self, key: &str, range: Option<ByteRange>)
        -> Result<Option<Vec<u8>>, ObjectStoreError>;
    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError>;
    fn delete(&self, key: &str) -> Result<(), ObjectStoreError>;
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError>;

    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put(key, bytes, PutMode::Overwrite)
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put(key, bytes, PutMode::CreateIfAbsent)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: &[u8],
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.put(
            key,
            bytes,
            PutMode::CompareAndSwap {
                expected_etag: expected_etag.to_owned(),
            },
        )
    }
}

impl<T> BlockingObjectStore for &T
where
    T: BlockingObjectStore + ?Sized,
{
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        (*self).head(key)
    }

    fn head_with_checksum(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        (*self).head_with_checksum(key)
    }

    fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        (*self).get_with_metadata(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        (*self).get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        (*self).put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        (*self).delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        (*self).list_prefix(prefix)
    }

    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        (*self).put_overwrite(key, bytes)
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        (*self).put_if_absent(key, bytes)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: &[u8],
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        (*self).compare_and_swap(key, expected_etag, bytes)
    }
}

impl<T> BlockingObjectStore for Arc<T>
where
    T: BlockingObjectStore + ?Sized,
{
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.as_ref().head(key)
    }

    fn head_with_checksum(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.as_ref().head_with_checksum(key)
    }

    fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.as_ref().get_with_metadata(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.as_ref().get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.as_ref().put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.as_ref().delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.as_ref().list_prefix(prefix)
    }

    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        self.as_ref().put_overwrite(key, bytes)
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        self.as_ref().put_if_absent(key, bytes)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: &[u8],
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.as_ref().compare_and_swap(key, expected_etag, bytes)
    }
}

#[derive(Debug)]
pub struct BlockingObjectStoreAdapter {
    inner: SharedObjectStore,
    runtime: Runtime,
}

impl BlockingObjectStoreAdapter {
    pub fn new(inner: SharedObjectStore) -> Result<Self, ObjectStoreError> {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| ObjectStoreError::Transport(err.to_string()))?;
        Ok(Self { inner, runtime })
    }
}

impl BlockingObjectStore for BlockingObjectStoreAdapter {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.runtime.block_on(self.inner.head(key))
    }

    fn head_with_checksum(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.runtime.block_on(self.inner.head_with_checksum(key))
    }

    fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.runtime.block_on(self.inner.get_with_metadata(key))
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.runtime
            .block_on(self.inner.get(key, range))
            .map(|maybe| maybe.map(|bytes| bytes.to_vec()))
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.runtime
            .block_on(self.inner.put(key, Bytes::copy_from_slice(bytes), mode))
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.runtime.block_on(self.inner.delete(key))
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.runtime.block_on(self.inner.list_prefix(prefix))
    }
}
