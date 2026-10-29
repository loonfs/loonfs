//! An in-memory stand-in for a provider's multipart upload API.
//!
//! It reproduces the provider behaviours the completion path is built
//! around, taken from the live conformance results rather than from
//! documentation:
//!
//! - a part upload is not create-only, so re-uploading one replaces it and
//!   the assembled object follows the last write;
//! - a completed upload is *consumed*, so replaying its completion succeeds
//!   and changes nothing, and the object it produced is the only evidence of
//!   what happened: the lost-completion case;
//! - a completion is create-only, so completing over an existing object
//!   leaves that object in place, and the adapter reports success so that
//!   the read-back decides;
//! - the whole-object checksum supplied at completion is not necessarily
//!   enforced (Cloudflare R2 accepts a wrong one and stores the true value),
//!   which is why LoonFS reads the object back;
//! - aborting a completed or unknown upload succeeds and destroys nothing;
//! - a new object can start with a copy of another object's prefix, taken
//!   only while that object is at the version named. The copy reports the
//!   copied part's checksum, and a claim is held to the parts' checksums
//!   before completion, which accepts any claim, as Cloudflare R2's does.

use async_trait::async_trait;
use bytes::Bytes;
use loonfs_objectstore::{
    AssemblySource, MultipartPart, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode, Result,
};
use loonfs_types::Checksum;
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
    aborts: Mutex<Vec<(String, String)>>,
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
            aborts: Mutex::new(Vec::new()),
        }
    }

    /// How many aborts this store has been asked for, including aborts of
    /// uploads it no longer knows about.
    pub fn aborts(&self) -> usize {
        self.abort_records().len()
    }

    /// Keys and provider IDs supplied to abort requests, in request order.
    pub fn abort_records(&self) -> Vec<(String, String)> {
        self.aborts
            .lock()
            .expect("abort records lock should not be poisoned")
            .clone()
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
        let etag = format!("\"{}\"", Checksum::crc64nvme(bytes).value());
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
        self.open
            .lock()
            .expect("the open upload lock should not be poisoned")
    }
}

#[async_trait]
impl<S: ObjectStore> ObjectStore for FakeMultipartStore<S> {
    crate::delegate_object_store!(self => self.inner;
        checksum_algorithm,
        identity,
        get_with_metadata,
        get,
        put_streamed,
        list_entries_from_stream,
        list_child_prefixes,
    );

    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        let mut metadata = self.inner.head(key).await?;
        if let Some(metadata) = &mut metadata {
            if let Some(checksum) = self.stored_checksums.lock().expect("checksums").get(key) {
                metadata.checksum = Some(checksum.clone());
            }
        }
        Ok(metadata)
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
    ) -> Result<()> {
        let Some(upload) = self.lock().remove(provider_upload_id) else {
            return Ok(());
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
                || part.checksum != Checksum::compute(part.checksum.algorithm(), &stored.bytes)
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

        let stored_checksum = Checksum::compute(full_object_checksum.algorithm(), &assembled);
        if self.enforcement == MultipartChecksumEnforcement::Precondition
            && stored_checksum != *full_object_checksum
        {
            return Err(ObjectStoreError::transport(
                key,
                "the specified checksum did not match the calculated checksum",
            ));
        }

        match self
            .inner
            .put(key, Bytes::from(assembled), PutMode::CreateIfAbsent)
            .await
        {
            Ok(_) => {}
            Err(ObjectStoreError::PreconditionFailed { .. }) => return Ok(()),
            Err(error) => return Err(error),
        }
        self.stored_checksums
            .lock()
            .expect("stored checksum lock should not be poisoned")
            .insert(key.to_owned(), stored_checksum);
        Ok(())
    }

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        self.aborts
            .lock()
            .expect("abort records lock should not be poisoned")
            .push((key.to_owned(), provider_upload_id.to_owned()));
        let mut open = self.lock();
        if open
            .get(provider_upload_id)
            .is_some_and(|upload| upload.object_key == key)
        {
            open.remove(provider_upload_id);
        }
        Ok(())
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        let metadata = self.inner.put(key, bytes, mode).await?;
        self.stored_checksums
            .lock()
            .expect("stored checksum lock should not be poisoned")
            .remove(key);
        Ok(metadata)
    }

    async fn assemble(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        if let Some(metadata) = self.head(key).await? {
            return match &metadata.checksum {
                Some(actual) if actual == expected => Ok(metadata),
                Some(_) => Err(ObjectStoreError::DifferentObject {
                    object_key: key.to_owned(),
                }),
                None => Err(ObjectStoreError::StoredChecksumMissing {
                    object_key: key.to_owned(),
                }),
            };
        }
        let mut bytes = Vec::new();
        for source in sources {
            let selected = self
                .inner
                .get(&source.key, source.range.clone())
                .await?
                .ok_or_else(|| ObjectStoreError::PreconditionFailed {
                    object_key: source.key.clone(),
                })?;
            if source.range.as_ref().is_some_and(|range| {
                selected.len() as u64 != range.end_exclusive - range.start_inclusive
            }) {
                return Err(ObjectStoreError::PreconditionFailed {
                    object_key: source.key.clone(),
                });
            }
            if !source.checksum.matches(&selected) {
                return Err(ObjectStoreError::ChecksumMismatch {
                    object_key: source.key.clone(),
                });
            }
            bytes.extend_from_slice(&selected);
        }
        for piece in tail {
            bytes.extend_from_slice(&piece);
        }
        if !expected.matches(&bytes) {
            return Err(ObjectStoreError::ChecksumMismatch {
                object_key: key.to_owned(),
            });
        }
        let mut metadata = self.inner.put_immutable_verified(key, bytes.into()).await?;
        self.stored_checksums
            .lock()
            .expect("checksums")
            .insert(key.to_owned(), expected.clone());
        metadata.checksum = Some(expected.clone());
        Ok(metadata)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await?;
        self.stored_checksums
            .lock()
            .expect("stored checksum lock should not be poisoned")
            .remove(key);
        Ok(())
    }
}
