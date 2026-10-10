//! In-memory objects with a selectable provider checksum algorithm.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use loonfs_objectstore::{
    ByteRange, ListedObject, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    Result,
};
use loonfs_types::{Checksum, ChecksumAlgorithm, EffectiveLimit, Page};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

#[derive(Debug, Default)]
struct Objects {
    entries: BTreeMap<String, ObjectBody>,
    version: u64,
}

/// Stores the checksum computed at each write alongside its bytes.
#[derive(Debug)]
pub struct MemoryStore {
    algorithm: ChecksumAlgorithm,
    objects: Mutex<Objects>,
}

impl MemoryStore {
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            algorithm,
            objects: Mutex::default(),
        }
    }
}

#[async_trait]
impl ObjectStore for MemoryStore {
    fn checksum_algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        Ok(self
            .objects
            .lock()
            .expect("objects lock should not be poisoned")
            .entries
            .get(key)
            .map(|object| object.metadata.clone()))
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        Ok(self
            .objects
            .lock()
            .expect("objects lock should not be poisoned")
            .entries
            .get(key)
            .cloned())
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        if range
            .as_ref()
            .is_some_and(|range| range.start_inclusive > range.end_exclusive)
        {
            return Err(ObjectStoreError::InvalidRange {
                object_key: key.to_owned(),
            });
        }
        let Some(object) = self.get_with_metadata(key).await? else {
            return Ok(None);
        };
        let bytes = match range {
            Some(range) => {
                if range.start_inclusive > object.bytes.len() as u64 {
                    return Err(ObjectStoreError::InvalidRange {
                        object_key: key.to_owned(),
                    });
                }
                let end = range.end_exclusive.min(object.bytes.len() as u64) as usize;
                Bytes::from(object.bytes).slice(range.start_inclusive as usize..end)
            }
            None => Bytes::from(object.bytes),
        };
        Ok(Some(bytes))
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        let mut objects = self
            .objects
            .lock()
            .expect("objects lock should not be poisoned");
        let current = objects.entries.get(key);
        let accepts = match mode {
            PutMode::Overwrite => true,
            PutMode::CreateIfAbsent => current.is_none(),
            PutMode::CompareAndSwap { expected_etag } => {
                current.is_some_and(|object| object.metadata.etag.as_ref() == Some(&expected_etag))
            }
        };
        if !accepts {
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
        objects.version += 1;
        let metadata = ObjectMetadata {
            etag: Some(objects.version.to_string()),
            version: None,
            size_bytes: bytes.len() as u64,
            last_modified_ms: Some(0),
            checksum: Some(Checksum::compute(self.algorithm, &bytes)),
        };
        objects.entries.insert(
            key.to_owned(),
            ObjectBody {
                bytes: bytes.to_vec(),
                metadata: metadata.clone(),
            },
        );
        Ok(metadata)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.objects
            .lock()
            .expect("objects lock should not be poisoned")
            .entries
            .remove(key);
        Ok(())
    }

    fn list_entries_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ListedObject>> {
        let entries: Vec<_> = self
            .objects
            .lock()
            .expect("objects lock should not be poisoned")
            .entries
            .iter()
            .filter(|(key, _)| {
                key.starts_with(prefix) && start_after.is_none_or(|start| key.as_str() > start)
            })
            .map(|(key, object)| {
                Ok(ListedObject {
                    key: key.clone(),
                    last_modified_ms: object.metadata.last_modified_ms,
                })
            })
            .collect();
        Box::pin(stream::iter(entries))
    }

    async fn list_child_prefixes(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: EffectiveLimit,
    ) -> Result<Page<String, String>> {
        let children: BTreeSet<_> = self
            .objects
            .lock()
            .expect("objects lock should not be poisoned")
            .entries
            .keys()
            .filter_map(|key| {
                key.strip_prefix(prefix)?
                    .split_once('/')
                    .map(|(child, _)| format!("{prefix}{child}/"))
            })
            .filter(|child| start_after.is_none_or(|start| child.as_str() > start))
            .collect();
        let mut items: Vec<_> = children.into_iter().take(limit.limit_plus_one()).collect();
        let next_cursor = limit.finish_page(&mut items, Clone::clone);
        Ok(Page { items, next_cursor })
    }
}
