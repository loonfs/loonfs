//! Short-lived proofs that uploaded content blobs are durable, letting a
//! commit skip re-verifying bytes this process just wrote and verified.

use crate::time::wall_clock_now;
use crate::{ContentRef, NamespaceId, ObjectStore, ObjectStoreError};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loon_api::sha256_digest;
use loon_objectstore::{ByteRange, ObjectBody, ObjectMetadata, PutMode};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

const MAX_UPLOADED_CONTENT_PROOF_ENTRIES: usize = 16_384;
const UPLOADED_CONTENT_PROOF_TTL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UploadedContentProofKey {
    namespace_id: NamespaceId,
    digest: String,
}

#[derive(Debug, Default)]
pub(crate) struct UploadedContentProofCache {
    entries: HashMap<UploadedContentProofKey, UploadedContentProof>,
    order: VecDeque<UploadedContentProofKey>,
}

#[derive(Debug, Clone)]
struct UploadedContentProof {
    content_ref: ContentRef,
    expires_at: SystemTime,
}

impl UploadedContentProofCache {
    fn insert(&mut self, namespace_id: &NamespaceId, content_ref: ContentRef) {
        let key = UploadedContentProofKey {
            namespace_id: namespace_id.clone(),
            digest: content_ref.digest.clone(),
        };
        let proof = UploadedContentProof {
            content_ref,
            expires_at: wall_clock_now() + UPLOADED_CONTENT_PROOF_TTL,
        };
        self.entries.insert(key.clone(), proof);
        self.order.retain(|existing| existing != &key);
        self.order.push_back(key);
        while self.entries.len() > MAX_UPLOADED_CONTENT_PROOF_ENTRIES {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&evicted);
        }
    }

    fn get(&mut self, namespace_id: &NamespaceId, digest: &str) -> Option<ContentRef> {
        let key = UploadedContentProofKey {
            namespace_id: namespace_id.clone(),
            digest: digest.to_owned(),
        };
        let proof = self.entries.get(&key)?;
        if wall_clock_now() > proof.expires_at {
            self.entries.remove(&key);
            self.order.retain(|existing| existing != &key);
            return None;
        }
        Some(proof.content_ref.clone())
    }
}

#[cfg(test)]
mod proof_cache_tests {
    use super::{wall_clock_now, UploadedContentProofCache, UploadedContentProofKey};
    use loon_api::{ContentRef, NamespaceId};
    use std::time::Duration;

    #[test]
    fn uploaded_content_proof_expires_without_refresh_on_lookup() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let content_ref = ContentRef::whole_file_v0(b"bytes");
        let mut cache = UploadedContentProofCache::default();
        cache.insert(&namespace_id, content_ref.clone());

        assert_eq!(
            cache.get(&namespace_id, &content_ref.digest),
            Some(content_ref.clone())
        );

        let key = UploadedContentProofKey {
            namespace_id: namespace_id.clone(),
            digest: content_ref.digest.clone(),
        };
        cache
            .entries
            .get_mut(&key)
            .expect("proof exists")
            .expires_at = wall_clock_now() - Duration::from_secs(1);

        assert_eq!(cache.get(&namespace_id, &content_ref.digest), None);
        assert!(!cache.entries.contains_key(&key));
    }
}

#[derive(Debug)]
pub(crate) struct UploadedContentProofStore<'a> {
    pub(crate) inner: &'a (dyn ObjectStore + Send + Sync),
    pub(crate) namespace_id: &'a NamespaceId,
    pub(crate) proofs: &'a Mutex<UploadedContentProofCache>,
}

impl UploadedContentProofStore<'_> {
    fn proof_metadata(&self, key: &str) -> Option<ObjectMetadata> {
        let digest = content_blob_digest_from_key(key)?;
        let content_ref = self
            .proofs
            .lock()
            .expect("uploaded content proof cache lock poisoned")
            .get(self.namespace_id, &digest)?;
        Some(ObjectMetadata {
            etag: None,
            version: None,
            size_bytes: content_ref.size_bytes,
            checksum_sha256: Some(content_ref.digest),
        })
    }

    fn record_write_proof(&self, key: &str, bytes: &[u8]) {
        let Some(digest) = content_blob_digest_from_key(key) else {
            return;
        };
        if sha256_digest(bytes) != digest {
            return;
        }
        self.proofs
            .lock()
            .expect("uploaded content proof cache lock poisoned")
            .insert(
                self.namespace_id,
                ContentRef {
                    kind: loon_api::ContentRefKind::WholeFileV0,
                    digest,
                    size_bytes: bytes.len() as u64,
                },
            );
    }
}

#[async_trait]
impl ObjectStore for UploadedContentProofStore<'_> {
    async fn head(
        &self,
        key: &str,
    ) -> std::result::Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> std::result::Result<Option<ObjectMetadata>, ObjectStoreError> {
        if let Some(metadata) = self.proof_metadata(key) {
            return Ok(Some(metadata));
        }
        self.inner.head_with_checksum(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> std::result::Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(
        &self,
        key: &str,
    ) -> std::result::Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> std::result::Result<ObjectMetadata, ObjectStoreError> {
        let proof_bytes = bytes.clone();
        let metadata = self.inner.put(key, bytes, mode).await?;
        self.record_write_proof(key, &proof_bytes);
        Ok(metadata)
    }

    async fn delete(&self, key: &str) -> std::result::Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, std::result::Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

fn content_blob_digest_from_key(key: &str) -> Option<String> {
    if !key.contains("/blobs/sha256/") {
        return None;
    }
    let hex = key.rsplit('/').next()?;
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!("sha256:{}", hex.to_ascii_lowercase()))
}
