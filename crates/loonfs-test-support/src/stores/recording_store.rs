//! Exact operation logs for selected object keys.

use super::KeyPredicate;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::Checksum;
use loonfs_objectstore::{
    ByteRange, ByteStream, MultipartCompletion, MultipartPart, ObjectBody, ObjectMetadata,
    ObjectStore, ObjectStoreError, PutMode, StoredObjectChecksum,
};
use std::sync::Mutex;

/// An owned get record containing its key and optional `(start, end)` byte range.
pub type RecordedGet = (String, Option<(u64, u64)>);

/// One owned operation-log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedOperation {
    /// A metadata-only read.
    Head { key: String },
    /// A byte read.
    Get {
        key: String,
        range: Option<ByteRange>,
    },
    /// A full-object read.
    GetWithMetadata { key: String },
    /// A put.
    Put {
        key: String,
        mode: PutMode,
        bytes: usize,
    },
    /// A streamed put, recorded once its payload has been written and its
    /// length is known.
    PutStreamed {
        key: String,
        mode: PutMode,
        bytes: u64,
    },
    /// A compare-and-swap call.
    CompareAndSwap { key: String, bytes: usize },
    /// A delete.
    Delete { key: String },
    /// A prefix-list call.
    List { prefix: String },
}

impl RecordedOperation {
    /// Returns the addressed key or list prefix.
    pub fn key(&self) -> &str {
        match self {
            Self::Head { key }
            | Self::Get { key, .. }
            | Self::GetWithMetadata { key }
            | Self::Put { key, .. }
            | Self::PutStreamed { key, .. }
            | Self::CompareAndSwap { key, .. }
            | Self::Delete { key } => key,
            Self::List { prefix } => prefix,
        }
    }
}

/// Records selected object-store operations in call order.
#[derive(Debug)]
pub struct RecordingStore<S> {
    inner: S,
    keys: KeyPredicate,
    operations: Mutex<Vec<RecordedOperation>>,
}

impl<S> RecordingStore<S> {
    /// Records every operation whose key matches `keys`.
    pub fn new(inner: S, keys: KeyPredicate) -> Self {
        Self {
            inner,
            keys,
            operations: Mutex::new(Vec::new()),
        }
    }

    /// Returns a snapshot without clearing the log.
    pub fn snapshot(&self) -> Vec<RecordedOperation> {
        self.operations
            .lock()
            .expect("operation log lock poisoned")
            .clone()
    }

    /// Takes and clears the operation log.
    pub fn take(&self) -> Vec<RecordedOperation> {
        std::mem::take(&mut *self.operations.lock().expect("operation log lock poisoned"))
    }

    /// Takes all operations and returns only byte-read records.
    ///
    /// `get_with_metadata` records carry no range.
    pub fn take_gets(&self) -> Vec<RecordedGet> {
        self.take()
            .into_iter()
            .filter_map(|operation| match operation {
                RecordedOperation::Get { key, range } => Some((
                    key,
                    range.map(|range| (range.start_inclusive, range.end_exclusive)),
                )),
                RecordedOperation::GetWithMetadata { key } => Some((key, None)),
                _ => None,
            })
            .collect()
    }

    /// Takes all operations and returns only keys read through either get form.
    pub fn take_get_keys(&self) -> Vec<String> {
        self.take_gets().into_iter().map(|(key, _)| key).collect()
    }

    /// Clears the operation log.
    pub fn reset(&self) {
        self.operations
            .lock()
            .expect("operation log lock poisoned")
            .clear();
    }

    /// Returns a reference to the wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    fn record(&self, operation: RecordedOperation) {
        if self.keys.matches(operation.key()) {
            self.operations
                .lock()
                .expect("operation log lock poisoned")
                .push(operation);
        }
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for RecordingStore<S> {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.record(RecordedOperation::Head {
            key: key.to_owned(),
        });
        self.inner.head(key).await
    }

    async fn head_stored_checksum(
        &self,
        key: &str,
    ) -> Result<Option<StoredObjectChecksum>, ObjectStoreError> {
        self.record(RecordedOperation::Head {
            key: key.to_owned(),
        });
        self.inner.head_stored_checksum(key).await
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String, ObjectStoreError> {
        self.inner.create_multipart_upload(key).await
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<MultipartCompletion, ObjectStoreError> {
        self.inner
            .complete_multipart_upload(key, provider_upload_id, parts, checksum)
            .await
    }

    async fn abort_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
    ) -> Result<(), ObjectStoreError> {
        self.inner
            .abort_multipart_upload(key, provider_upload_id)
            .await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.record(RecordedOperation::GetWithMetadata {
            key: key.to_owned(),
        });
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.record(RecordedOperation::Get {
            key: key.to_owned(),
            range: range.clone(),
        });
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.record(RecordedOperation::Put {
            key: key.to_owned(),
            mode: mode.clone(),
            bytes: bytes.len(),
        });
        self.inner.put(key, bytes, mode).await
    }

    async fn put_streamed(
        &self,
        key: &str,
        body: ByteStream,
        mode: PutMode,
    ) -> Result<u64, ObjectStoreError> {
        let result = self.inner.put_streamed(key, body, mode.clone()).await;
        if let Ok(bytes) = &result {
            self.record(RecordedOperation::PutStreamed {
                key: key.to_owned(),
                mode,
                bytes: *bytes,
            });
        }
        result
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.record(RecordedOperation::CompareAndSwap {
            key: key.to_owned(),
            bytes: bytes.len(),
        });
        self.inner.compare_and_swap(key, expected_etag, bytes).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.record(RecordedOperation::Delete {
            key: key.to_owned(),
        });
        self.inner.delete(key).await
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.record(RecordedOperation::List {
            prefix: prefix.to_owned(),
        });
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}
