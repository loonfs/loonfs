//! An in-memory stand-in for a provider's multipart upload API.
//!
//! It reproduces the provider behaviours the completion path is built
//! around, taken from the live conformance results rather than from
//! documentation:
//!
//! - a part upload is not create-only, so re-uploading one replaces it and
//!   the assembled object follows the last write;
//! - a completed upload is *consumed*, so replaying its completion reports
//!   an upload the provider has never heard of while the object it produced
//!   sits there correct — the lost-completion case;
//! - the whole-object checksum supplied at completion is not necessarily
//!   enforced (Cloudflare R2 accepts a wrong one and stores the true value),
//!   which is why LoonFS reads the object back;
//! - aborting a completed or unknown upload succeeds and destroys nothing.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::Checksum;
use loonfs_objectstore::{
    ByteRange, ByteStream, MultipartCompletion, MultipartPart, ObjectBody, ObjectMetadata,
    ObjectStore, ObjectStoreError, PutMode, Result, StoredObjectChecksum,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Whether the stand-in refuses a completion whose whole-object checksum
/// disagrees with the parts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MultipartChecksumEnforcement {
    /// Accept the completion and store the true checksum, the way Cloudflare
    /// R2 does. This is the default because it is the case the read-back
    /// exists for.
    #[default]
    Witness,
    /// Refuse the completion, the way AWS S3 does.
    Precondition,
}

#[derive(Debug)]
struct OpenUpload {
    object_key: String,
    parts: BTreeMap<u32, StoredPart>,
}

#[derive(Debug)]
struct StoredPart {
    bytes: Vec<u8>,
    etag: String,
}

/// Wraps a store with a working multipart upload surface.
#[derive(Debug)]
pub struct FakeMultipartStore<S> {
    inner: S,
    enforcement: MultipartChecksumEnforcement,
    open: Mutex<BTreeMap<String, OpenUpload>>,
    stored_checksums: Mutex<BTreeMap<String, Checksum>>,
    next_id: AtomicUsize,
    aborts: AtomicUsize,
}

impl<S> FakeMultipartStore<S> {
    /// Wraps `inner`, witnessing rather than enforcing the whole-object
    /// checksum at completion.
    pub fn new(inner: S) -> Self {
        Self::with_enforcement(inner, MultipartChecksumEnforcement::Witness)
    }

    /// Wraps `inner` with an explicit completion-checksum behaviour.
    pub fn with_enforcement(inner: S, enforcement: MultipartChecksumEnforcement) -> Self {
        Self {
            inner,
            enforcement,
            open: Mutex::new(BTreeMap::new()),
            stored_checksums: Mutex::new(BTreeMap::new()),
            next_id: AtomicUsize::new(1),
            aborts: AtomicUsize::new(0),
        }
    }

    /// How many aborts this store has been asked for, including aborts of
    /// uploads it no longer knows about.
    pub fn aborts(&self) -> usize {
        self.aborts.load(Ordering::SeqCst)
    }

    /// How many uploads are still open.
    pub fn open_uploads(&self) -> usize {
        self.lock().len()
    }

    /// Uploads one part, standing in for the client's presigned PUT, and
    /// returns the etag the provider would have reported.
    ///
    /// Repeating a part replaces it, which is what makes retrying one part
    /// of a large upload cheap.
    pub fn upload_part(
        &self,
        provider_upload_id: &str,
        part_number: u32,
        bytes: &[u8],
    ) -> Result<String> {
        let mut open = self.lock();
        let upload =
            open.get_mut(provider_upload_id)
                .ok_or_else(|| ObjectStoreError::NotFound {
                    object_key: provider_upload_id.to_owned(),
                })?;
        let etag = format!("\"{}\"", Checksum::crc64nvme(bytes).value);
        upload.parts.insert(
            part_number,
            StoredPart {
                bytes: bytes.to_vec(),
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, OpenUpload>> {
        self.open.lock().unwrap_or_else(|err| err.into_inner())
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for FakeMultipartStore<S> {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        self.inner.head(key).await
    }

    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>> {
        let checksum = self
            .stored_checksums
            .lock()
            .expect("stored checksum lock should not be poisoned")
            .get(key)
            .cloned();
        if let Some(checksum) = checksum {
            let size_bytes = self
                .inner
                .head(key)
                .await?
                .ok_or_else(|| ObjectStoreError::NotFound {
                    object_key: key.to_owned(),
                })?
                .size_bytes;
            return Ok(Some(StoredObjectChecksum {
                size_bytes,
                checksum,
            }));
        }
        self.inner.head_stored_checksum(key).await
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        let provider_upload_id = format!(
            "mpu-{}-{}",
            self.next_id.fetch_add(1, Ordering::SeqCst),
            key.len()
        );
        self.lock().insert(
            provider_upload_id.clone(),
            OpenUpload {
                object_key: key.to_owned(),
                parts: BTreeMap::new(),
            },
        );
        Ok(provider_upload_id)
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        full_object_checksum: &Checksum,
    ) -> Result<MultipartCompletion> {
        let Some(upload) = self.lock().remove(provider_upload_id) else {
            // Consumed already. The object, if any, is the only evidence
            // left — exactly what the caller reconciles from.
            return Ok(MultipartCompletion::UnknownUpload);
        };
        if upload.object_key != key {
            return Err(ObjectStoreError::transport(
                key,
                "multipart completion named a different object",
            ));
        }

        let mut assembled = Vec::new();
        for part in parts {
            let Some(stored) = upload.parts.get(&part.part_number) else {
                return Err(ObjectStoreError::transport(
                    key,
                    format!("multipart complete named unknown part {}", part.part_number),
                ));
            };
            // Both providers verify a part's checksum on the way in, so a
            // part whose declared checksum does not match its bytes could
            // never have landed.
            if part.etag != stored.etag
                || part.checksum != Checksum::compute(part.checksum.algorithm, &stored.bytes)
            {
                return Err(ObjectStoreError::transport(
                    key,
                    format!(
                        "part {} checksum does not match its bytes",
                        part.part_number
                    ),
                ));
            }
            assembled.extend_from_slice(&stored.bytes);
        }

        let stored_checksum = Checksum::compute(full_object_checksum.algorithm, &assembled);
        if self.enforcement == MultipartChecksumEnforcement::Precondition
            && stored_checksum != *full_object_checksum
        {
            return Err(ObjectStoreError::transport(
                key,
                "the specified checksum did not match the calculated checksum",
            ));
        }

        self.inner
            .put(key, Bytes::from(assembled), PutMode::Overwrite)
            .await?;
        self.stored_checksums
            .lock()
            .expect("stored checksum lock should not be poisoned")
            .insert(key.to_owned(), stored_checksum);
        Ok(MultipartCompletion::Assembled)
    }

    async fn abort_multipart_upload(&self, _key: &str, provider_upload_id: &str) -> Result<()> {
        self.aborts.fetch_add(1, Ordering::SeqCst);
        // An upload the provider no longer has is already in the state an
        // abort is trying to reach, and an assembled object is untouched.
        self.lock().remove(provider_upload_id);
        Ok(())
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
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}
