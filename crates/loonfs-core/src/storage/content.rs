//! Content object reads and writes: minting immutable identities,
//! validating references, and verified read-back.

use crate::error::CoreError;
#[cfg(any(test, feature = "test-support"))]
use crate::namespace::catalog::load_namespace_content_store_id;
#[cfg(any(test, feature = "test-support"))]
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
#[cfg(any(test, feature = "test-support"))]
use crate::storage::content_admission::{ContentAdmission, PreparedContent};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
#[cfg(any(test, feature = "test-support"))]
use loonfs_api::NamespaceId;
use loonfs_api::{
    Checksum, ContentId, ContentRef, ContentRefValidationError, ContentStoreId, PathEntry, Sha256,
    StreamingChecksum,
};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{ByteRange, ByteStream, ObjectStore, ObjectStoreError, PutMode};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// Confirms that LoonFS durably stored the content described by this reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoredContent {
    content_store_id: ContentStoreId,
    #[cfg(any(test, feature = "test-support"))]
    object_key: String,
    content_ref: ContentRef,
    #[cfg(any(test, feature = "test-support"))]
    #[serde(skip)]
    _write_acknowledged: StoredContentWriteAcknowledgement,
}

impl StoredContent {
    pub fn content_ref(&self) -> &ContentRef {
        &self.content_ref
    }

    pub fn into_content_ref(self) -> ContentRef {
        self.content_ref
    }

    #[cfg(test)]
    pub(crate) fn content_store_id(&self) -> &ContentStoreId {
        &self.content_store_id
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn object_key(&self) -> &str {
        &self.object_key
    }
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredContentWriteAcknowledgement;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum DurableContentValidationError {
    #[error("invalid content reference: {0}")]
    InvalidContentRef(ContentRefValidationError),
    #[error("missing content object `{object_key}`")]
    MissingContentObject { object_key: String },
    #[error("content length mismatch for `{object_key}`: expected {expected}, actual {actual}")]
    ContentLengthMismatch {
        object_key: String,
        expected: u64,
        actual: u64,
    },
    #[error(
        "content checksum mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ContentChecksumMismatch {
        object_key: String,
        expected: String,
        actual: String,
    },
    #[error(
        "stored content belongs to content store `{actual}`, not namespace-bound store `{expected}`"
    )]
    ContentStoreMismatch {
        expected: ContentStoreId,
        actual: ContentStoreId,
    },
    #[error("object store error for `{object_key}`: {message}")]
    Store { object_key: String, message: String },
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) async fn validate_durable_content_reference<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<(), DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    validate_content_size(store, &object_key, content_ref).await?;
    let bytes = load_required_object(store, &object_key).await?;
    validate_loaded_content_bytes(object_key, content_ref, &bytes)
}

/// Opens a chunked reader over an existing object for import: one HEAD, then
/// ranged reads pumped through a one-chunk channel, so nothing holds the
/// whole object. Drive the pump and the staging consumer together.
pub(crate) async fn open_content_import_reader<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<
    (
        String,
        impl std::future::Future<Output = ()> + 'a,
        ByteStream,
    ),
    DurableContentValidationError,
> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    validate_content_size(store, &object_key, content_ref).await?;
    let size_bytes = content_ref.size_bytes;
    let (mut sender, receiver) = futures::channel::mpsc::channel(1);
    let pump_key = object_key.clone();
    let pump = async move {
        let mut offset = 0u64;
        while offset < size_bytes {
            let end = offset
                .saturating_add(CONTENT_READ_CHUNK_BYTES)
                .min(size_bytes);
            let range = ByteRange {
                start_inclusive: offset,
                end_exclusive: end,
            };
            let chunk = match store.get(&pump_key, Some(range)).await {
                Ok(Some(bytes)) => Ok(bytes),
                Ok(None) => Err(ObjectStoreError::transport(
                    &pump_key,
                    "content object disappeared during import",
                )),
                Err(error) => Err(error),
            };
            let failed = chunk.is_err();
            if sender.send(chunk).await.is_err() || failed {
                return;
            }
            offset = end;
        }
    };
    Ok((object_key, pump, receiver.boxed()))
}

/// Checks that a staged import carries the bytes the source ref claimed.
pub(crate) fn ensure_imported_ref_matches(
    object_key: String,
    expected: &ContentRef,
    staged: &ContentRef,
) -> Result<(), DurableContentValidationError> {
    if staged.size_bytes != expected.size_bytes {
        return Err(DurableContentValidationError::ContentLengthMismatch {
            object_key,
            expected: expected.size_bytes,
            actual: staged.size_bytes,
        });
    }
    if staged.checksum != expected.checksum {
        return Err(DurableContentValidationError::ContentChecksumMismatch {
            object_key,
            expected: describe_checksum(&expected.checksum),
            actual: describe_checksum(&staged.checksum),
        });
    }
    Ok(())
}

/// Prepares content from an acknowledged LoonFS-managed durable write.
///
/// Consuming [`StoredContent`] ties the proof to the successful return from
/// [`store_bytes_as_content`] or [`store_bytes_as_content_with_store_id`]. The
/// verified catalog prevents pairing that acknowledgement with an unrelated
/// namespace binding.
#[cfg(any(test, feature = "test-support"))]
pub fn prepare_stored_content(
    catalog: &VerifiedNamespaceCatalogEntry,
    stored_content: StoredContent,
) -> Result<PreparedContent, DurableContentValidationError> {
    if stored_content.content_store_id != *catalog.content_store_id() {
        return Err(DurableContentValidationError::ContentStoreMismatch {
            expected: catalog.content_store_id().clone(),
            actual: stored_content.content_store_id,
        });
    }
    let content_store_id = stored_content.content_store_id;
    let content_ref = stored_content.content_ref;
    let admission = ContentAdmission::for_durable_content_write(
        catalog.namespace_id().clone(),
        content_store_id,
        content_ref,
    );
    Ok(PreparedContent::from_admission(admission))
}

/// Fully validates an existing durable content reference for publication.
///
/// The verified catalog selects the store to validate. This performs one
/// object HEAD followed by one full GET and checksum check.
#[cfg(any(test, feature = "test-support"))]
pub async fn prepare_existing_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    content_ref: ContentRef,
) -> Result<PreparedContent, DurableContentValidationError> {
    let content_store_id = catalog.content_store_id();
    validate_durable_content_reference(store, content_store_id, &content_ref).await?;
    let admission = ContentAdmission::for_durable_content_write(
        catalog.namespace_id().clone(),
        content_store_id.clone(),
        content_ref,
    );
    Ok(PreparedContent::from_admission(admission))
}

/// Compares a content reference with the size and checksum stored by the provider.
///
/// This verifies direct uploads without downloading the object. Callers must
/// delete the unpublished object when the values do not match.
pub(crate) async fn verify_durable_content_checksum<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<(), DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    let stored = match store.head_stored_checksum(&object_key).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return Err(DurableContentValidationError::MissingContentObject { object_key }),
        Err(err) => {
            return Err(DurableContentValidationError::Store {
                object_key,
                message: err.public_message().into_owned(),
            })
        }
    };

    if stored.size_bytes != content_ref.size_bytes {
        return Err(DurableContentValidationError::ContentLengthMismatch {
            object_key,
            expected: content_ref.size_bytes,
            actual: stored.size_bytes,
        });
    }
    if stored.checksum != content_ref.checksum {
        return Err(DurableContentValidationError::ContentChecksumMismatch {
            object_key,
            expected: describe_checksum(&content_ref.checksum),
            actual: describe_checksum(&stored.checksum),
        });
    }
    Ok(())
}

/// Removes the content object an upload session owned but never published.
///
/// The id is random and an upload session is the only thing that can name
/// one before publication, so exactly one session is ever talking about this
/// object and no metadata can reference it. Deleting is therefore safe and
/// keeping it would leak bytes nobody can name again. This runs strictly
/// after the durable transition that made the session terminal, and it is
/// idempotent, so a cleanup lost to a crash is simply repeated by the next
/// garbage-collection pass — which is why a failure here is logged rather
/// than propagated.
pub(crate) async fn delete_unpublished_content_object<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_id: &ContentId,
) {
    let object_key = content_blob(content_store_id, content_id);
    if let Err(error) = store.delete(&object_key).await {
        tracing::warn!(
            content_id = %content_id,
            error_class = ?error.class(),
            "failed to remove the content object of a terminated upload session"
        );
    }
}

/// Abandons the provider multipart upload a terminated session opened.
///
/// Aborting is safe whatever the upload's real state: an upload that already
/// assembled its object survives the abort untouched, and one the provider
/// has never heard of succeeds anyway. So this runs strictly after the
/// durable transition without first proving what it is cleaning up, and a
/// failure is logged rather than propagated — the next garbage-collection
/// pass repeats it from the terminal record.
pub(crate) async fn abort_unpublished_multipart_upload<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_id: &ContentId,
    provider_upload_id: &str,
) {
    let object_key = content_blob(content_store_id, content_id);
    if let Err(error) = store
        .abort_multipart_upload(&object_key, provider_upload_id)
        .await
    {
        tracing::warn!(
            content_id = %content_id,
            error_class = ?error.class(),
            "failed to abandon the multipart upload of a terminated upload session"
        );
    }
}

/// Bytes fetched by each ranged content read.
pub const CONTENT_READ_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

/// Reads file content in fixed-size chunks while verifying its size and
/// checksum.
///
/// Verification completes when [`Self::next_chunk`] returns `None`. A caller
/// that stops earlier has not verified the complete object.
pub struct FileContentStream<S> {
    store: S,
    entry: PathEntry,
    object_key: String,
    content_ref: ContentRef,
    chunk_bytes: NonZeroU64,
    /// Start offset of the next ranged read.
    next_offset: u64,
    /// Offset where this stream started.
    resumed_from: u64,
    /// Number of bytes before `resumed_from` included in the checksum.
    prefix_folded: u64,
    /// Checksum state for bytes processed so far.
    digest: StreamingChecksum,
    /// Expected checksum.
    expected: Checksum,
    /// Cached result because the checksum can only be finalized once.
    completion: Option<Result<(), DurableContentValidationError>>,
}

impl<S: ObjectStore> FileContentStream<S> {
    /// Opens a streaming read of the object `content_ref` names.
    ///
    /// The object size is validated before content is returned. For resumed
    /// reads, `start_offset` skips bytes that the caller supplies through
    /// [`Self::fold_resumed_prefix`].
    pub(crate) async fn open(
        store: S,
        content_store_id: &ContentStoreId,
        entry: PathEntry,
        content_ref: ContentRef,
        chunk_bytes: NonZeroU64,
        start_offset: u64,
    ) -> Result<Self, DurableContentValidationError> {
        let object_key = content_object_key_for_ref(content_store_id, &content_ref)?;
        validate_content_size(&store, &object_key, &content_ref).await?;
        let expected = content_ref.checksum.clone();
        let digest = StreamingChecksum::for_algorithm(expected.algorithm);
        Ok(Self {
            store,
            entry,
            object_key,
            content_ref,
            chunk_bytes,
            next_offset: start_offset,
            resumed_from: start_offset,
            prefix_folded: 0,
            digest,
            expected,
            completion: None,
        })
    }

    /// Adds already-downloaded prefix bytes to a resumed read's checksum.
    ///
    /// Bytes must be supplied in order before fetching new chunks. Incorrect
    /// bytes cause checksum verification to fail at the end of the stream.
    pub fn fold_resumed_prefix(&mut self, bytes: &[u8]) -> Result<(), CoreError> {
        if self.completion.is_some() {
            return Err(CoreError::Internal(format!(
                "content stream for `{}` was handed a resumed prefix after its read had \
                 already reached the end",
                self.object_key
            )));
        }
        if self.next_offset != self.resumed_from {
            return Err(CoreError::Internal(format!(
                "content stream for `{}` was handed a resumed prefix after it had already \
                 fetched content",
                self.object_key
            )));
        }
        let folded = self.prefix_folded.saturating_add(bytes.len() as u64);
        if folded > self.content_ref.size_bytes {
            return Err(CoreError::ResumeOffsetOutOfRange {
                start_offset: folded,
                size_bytes: self.content_ref.size_bytes,
            });
        }
        self.digest.update(bytes);
        self.prefix_folded = folded;
        Ok(())
    }

    /// The authoritative metadata entry the path resolved to.
    pub fn entry(&self) -> &PathEntry {
        &self.entry
    }

    /// Complete length of the content this stream reads.
    pub fn size_bytes(&self) -> u64 {
        self.content_ref.size_bytes
    }

    /// Fetches the next chunk, or reports the end of a verified read.
    ///
    /// `Ok(None)` is returned only after the folded digest and the byte count
    /// agree with the reference; a mismatch fails this call instead. Chunks
    /// arrive in order from wherever the stream started, and every one but
    /// the last is exactly the chunk size this stream was opened with.
    ///
    /// A resumed stream must receive its complete prefix before this method is
    /// called.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, CoreError> {
        if self.prefix_folded != self.resumed_from {
            return Err(CoreError::ResumePrefixIncomplete {
                start_offset: self.resumed_from,
                folded: self.prefix_folded,
            });
        }
        Ok(self.next_verified_chunk().await?)
    }

    async fn next_verified_chunk(
        &mut self,
    ) -> Result<Option<Bytes>, DurableContentValidationError> {
        if self.next_offset == self.content_ref.size_bytes {
            return self.completion().map(|()| None);
        }
        let end_exclusive = self
            .next_offset
            .saturating_add(self.chunk_bytes.get())
            .min(self.content_ref.size_bytes);
        let bytes = match self
            .store
            .get(
                &self.object_key,
                Some(ByteRange {
                    start_inclusive: self.next_offset,
                    end_exclusive,
                }),
            )
            .await
        {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                return Err(DurableContentValidationError::MissingContentObject {
                    object_key: self.object_key.clone(),
                })
            }
            Err(err) => {
                return Err(DurableContentValidationError::Store {
                    object_key: self.object_key.clone(),
                    message: err.public_message().into_owned(),
                })
            }
        };
        // A range ending past the object is truncated, so a short answer is
        // an object that ended earlier than its reference says it does.
        if bytes.len() as u64 != end_exclusive - self.next_offset {
            return Err(DurableContentValidationError::ContentLengthMismatch {
                object_key: self.object_key.clone(),
                expected: self.content_ref.size_bytes,
                actual: self.next_offset + bytes.len() as u64,
            });
        }
        self.digest.update(&bytes);
        self.next_offset += bytes.len() as u64;
        Ok(Some(bytes))
    }

    /// The verdict on the complete object: computed the first time the end is
    /// reached, and repeated on every later ask.
    ///
    /// The byte count needs no check of its own here. Every chunk is required
    /// to arrive exactly as long as it was asked for, and no chunk is asked
    /// for past the declared size, so reaching this point *is* having folded
    /// exactly `size_bytes` — the resumed head start, which the first
    /// [`Self::next_chunk`] refuses to start without, plus everything
    /// fetched from it to the end. The object's own length was checked
    /// against the reference by the head request [`Self::open`] made.
    fn completion(&mut self) -> Result<(), DurableContentValidationError> {
        let verdict = match self.completion.take() {
            Some(verdict) => verdict,
            None => self.verify_complete(),
        };
        self.completion = Some(verdict.clone());
        verdict
    }

    /// Closes the digest over everything read and holds it to the reference.
    fn verify_complete(&mut self) -> Result<(), DurableContentValidationError> {
        // Closing consumes the digest, which is why this runs exactly once.
        let digest = std::mem::replace(
            &mut self.digest,
            StreamingChecksum::for_algorithm(self.expected.algorithm),
        );
        let actual = digest.finish();
        if actual != self.expected {
            return Err(DurableContentValidationError::ContentChecksumMismatch {
                object_key: self.object_key.clone(),
                expected: describe_checksum(&self.expected),
                actual: describe_checksum(&actual),
            });
        }
        Ok(())
    }
}

impl<S> std::fmt::Debug for FileContentStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileContentStream")
            .field("object_key", &self.object_key)
            .field("size_bytes", &self.content_ref.size_bytes)
            .field("next_offset", &self.next_offset)
            .finish_non_exhaustive()
    }
}

pub(crate) async fn get_durable_content_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<Vec<u8>, DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    let bytes = load_required_object(store, &object_key).await?;
    validate_loaded_content_bytes(object_key, content_ref, &bytes)?;
    Ok(bytes)
}

pub(crate) fn content_object_key_for_ref(
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<String, DurableContentValidationError> {
    content_ref
        .validate()
        .map_err(DurableContentValidationError::InvalidContentRef)?;
    Ok(content_blob(content_store_id, &content_ref.content_id))
}

/// Checks fetched bytes against everything the reference claims about them.
///
/// The reference's checksum is recomputed over the complete payload for every
/// supported algorithm.
fn validate_loaded_content_bytes(
    object_key: String,
    content_ref: &ContentRef,
    bytes: &[u8],
) -> Result<(), DurableContentValidationError> {
    let actual_size = bytes.len() as u64;
    if actual_size != content_ref.size_bytes {
        return Err(DurableContentValidationError::ContentLengthMismatch {
            object_key,
            expected: content_ref.size_bytes,
            actual: actual_size,
        });
    }

    let expected = &content_ref.checksum;
    if !expected.matches(bytes) {
        let actual = Checksum::compute(expected.algorithm, bytes);
        return Err(DurableContentValidationError::ContentChecksumMismatch {
            object_key,
            expected: describe_checksum(expected),
            actual: describe_checksum(&actual),
        });
    }

    Ok(())
}

fn describe_checksum(checksum: &Checksum) -> String {
    format!("{}:{}", checksum.algorithm, checksum.value)
}

/// Existence and size from one HEAD, used as cheap prevalidation before the
/// authoritative read-and-hash: a wrong-sized object fails without being
/// downloaded.
async fn validate_content_size<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    content_ref: &ContentRef,
) -> Result<(), DurableContentValidationError> {
    let metadata = match store.head(object_key).await {
        Ok(Some(metadata)) => metadata,
        Ok(None) => {
            return Err(DurableContentValidationError::MissingContentObject {
                object_key: object_key.to_owned(),
            })
        }
        Err(err) => {
            return Err(DurableContentValidationError::Store {
                object_key: object_key.to_owned(),
                message: err.public_message().into_owned(),
            })
        }
    };

    if metadata.size_bytes != content_ref.size_bytes {
        return Err(DurableContentValidationError::ContentLengthMismatch {
            object_key: object_key.to_owned(),
            expected: content_ref.size_bytes,
            actual: metadata.size_bytes,
        });
    }
    Ok(())
}

/// Plants durable content under a fresh identity, resolving the namespace's
/// content store first. See [`store_bytes_as_content_with_store_id`] for
/// what this is for and what it is not.
#[tracing::instrument(
    level = "debug",
    name = "loonfs.phase",
    err(level = "warn"),
    skip_all,
    fields(phase = "write_content_blob", key_class = "content")
)]
#[cfg(any(test, feature = "test-support"))]
pub async fn store_bytes_as_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    store_bytes_as_content_with_store_id(store, content_store_id, bytes).await
}

/// Test fixture for planting durable content without an upload session.
///
/// Every call mints a fresh identity. Production staging uses
/// [`crate::protocol::stage_owned_bytes`] so the object has a durable owner
/// before it is written and can later become eligible for reclamation.
#[cfg(any(test, feature = "test-support"))]
pub(crate) async fn store_bytes_as_content_with_store_id<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: ContentStoreId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    stage_bytes_under_content_id(store, content_store_id, ContentId::generate(), bytes).await
}

/// Result of staging streamed content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedStream {
    /// Identity, length, and checksum of the complete payload.
    pub content_ref: ContentRef,
    /// Whether the object existed before this write.
    pub already_present: bool,
}

/// Stages and hashes a payload without buffering the complete stream.
///
/// The write is create-only. Multipart providers may check for an existing
/// object immediately before assembly rather than atomically with it. Upload
/// session claims prevent concurrent writes to the same random content ID.
pub(crate) async fn stage_streamed_under_content_id<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: ContentStoreId,
    content_id: ContentId,
    body: ByteStream,
) -> Result<StagedStream, CoreError> {
    let object_key = content_blob(&content_store_id, &content_id);
    let observed = Arc::new(Mutex::new(StreamedPayload::default()));
    let hashed = {
        let observed = Arc::clone(&observed);
        body.map(move |chunk| {
            let chunk = chunk?;
            let mut observed = observed.lock().unwrap_or_else(|err| err.into_inner());
            observed.digest.update(&chunk);
            observed.size_bytes += chunk.len() as u64;
            Ok(chunk)
        })
        .boxed()
    };

    let stored = store
        .put_streamed(&object_key, hashed, PutMode::CreateIfAbsent)
        .await;
    let observed = std::mem::take(&mut *observed.lock().unwrap_or_else(|err| err.into_inner()));
    let already_present = match stored {
        Ok(stored_bytes) if stored_bytes != observed.size_bytes => {
            return Err(CoreError::Internal(format!(
                "streamed write of `{object_key}` stored {stored_bytes} bytes, \
                 but {} passed through this writer",
                observed.size_bytes
            )))
        }
        Ok(_) => false,
        // The caller compares the checksum with the session's recorded value.
        Err(ObjectStoreError::PreconditionFailed { .. }) => true,
        Err(err) => return Err(CoreError::store(&object_key, &err)),
    };

    Ok(StagedStream {
        content_ref: ContentRef::blob_v1_streamed(content_id, observed.size_bytes, observed.digest),
        already_present,
    })
}

/// Reads and hashes a stream without storing it.
pub(crate) async fn identify_streamed_payload(
    content_id: ContentId,
    mut body: ByteStream,
) -> Result<ContentRef, CoreError> {
    let mut observed = StreamedPayload::default();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|err| CoreError::store("upload body", &err))?;
        observed.digest.update(&chunk);
        observed.size_bytes += chunk.len() as u64;
    }
    Ok(ContentRef::blob_v1_streamed(
        content_id,
        observed.size_bytes,
        observed.digest,
    ))
}

/// Size and checksum state for a streamed payload.
#[derive(Debug, Default)]
struct StreamedPayload {
    digest: Sha256,
    size_bytes: u64,
}

/// Stages bytes under an identity the caller already allocated, for writers
/// that minted the id earlier (an upload session allocates at `begin`).
pub(crate) async fn stage_bytes_under_content_id<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: ContentStoreId,
    content_id: ContentId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    let content_ref = ContentRef::blob_v1(content_id, bytes);
    let object_key = content_blob(&content_store_id, &content_ref.content_id);
    // Create-only plus the byte check stay on this write even though a
    // random id cannot collide: if this key is ever occupied by different
    // bytes, that is corruption, and it must fail loudly rather than be
    // overwritten.
    store
        .put_immutable_verified(&object_key, Bytes::copy_from_slice(bytes))
        .await?;

    Ok(StoredContent {
        content_store_id,
        #[cfg(any(test, feature = "test-support"))]
        object_key,
        content_ref,
        #[cfg(any(test, feature = "test-support"))]
        _write_acknowledged: StoredContentWriteAcknowledgement,
    })
}

async fn load_required_object<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
) -> Result<Vec<u8>, DurableContentValidationError> {
    match store.get(object_key, None).await {
        Ok(Some(bytes)) => Ok(bytes.to_vec()),
        Ok(None) => Err(DurableContentValidationError::MissingContentObject {
            object_key: object_key.to_owned(),
        }),
        Err(err) => Err(DurableContentValidationError::Store {
            object_key: object_key.to_owned(),
            message: err.public_message().into_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        get_durable_content_bytes, store_bytes_as_content_with_store_id,
        validate_durable_content_reference, verify_durable_content_checksum, CoreError,
        DurableContentValidationError, FileContentStream, NonZeroU64,
    };
    use bytes::Bytes;
    use loonfs_api::{Checksum, ContentId, ContentRef, ContentRefKind, ContentStoreId, PathEntry};
    use loonfs_objectstore::keys::content_blob;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_test_support::ids::content_ref;
    use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass};
    use tempfile::tempdir;

    #[tokio::test]
    async fn validate_content_ref_success() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"whole file bytes";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect("validate content ref");
    }

    #[tokio::test]
    async fn validate_content_ref_reads_and_hashes_the_bytes() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = b"whole file bytes";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        store.reset();
        validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect("validate content ref");
        assert_eq!(store.count(OperationClass::Read), 1);
    }

    #[tokio::test]
    async fn get_durable_content_bytes_accepts_empty_files() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let bytes = get_durable_content_bytes(&store, &content_store_id, &content_ref)
            .await
            .expect("read empty content ref");
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn validate_content_ref_rejects_missing_object() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = content_ref(b"missing");

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("missing object");
        assert!(matches!(
            err,
            DurableContentValidationError::MissingContentObject { .. }
        ));
    }

    #[tokio::test]
    async fn validate_content_ref_rejects_size_mismatch() {
        let (_temp_dir, store, content_store_id) = test_store();
        let mut content_ref = content_ref(b"abc");
        put_content_object(&store, &content_store_id, &content_ref, b"abc").await;
        content_ref.size_bytes += 1;

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("size mismatch");
        assert!(matches!(
            err,
            DurableContentValidationError::ContentLengthMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn validate_content_ref_rejects_checksum_mismatch() {
        let (_temp_dir, store, content_store_id) = test_store();
        let expected = content_ref(b"expected");
        // Same id, different bytes: identity alone can no longer prove
        // content, so the checksum has to.
        let planted = ContentRef::blob_v1(expected.content_id.clone(), b"mismatch");
        put_content_object(&store, &content_store_id, &planted, b"mismatch").await;

        let err = validate_durable_content_reference(&store, &content_store_id, &expected)
            .await
            .expect_err("checksum mismatch");
        assert!(matches!(
            err,
            DurableContentValidationError::ContentChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn read_verifies_a_reference_whose_only_evidence_is_a_crc32c() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"transferred straight to the provider";
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            checksum: Checksum::crc32c(bytes),
        };
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let read = get_durable_content_bytes(&store, &content_store_id, &content_ref)
            .await
            .expect("a crc32c-only reference verifies by its crc");
        assert_eq!(read, bytes);

        // Same length, different bytes: only the checksum can tell.
        let (_temp_dir, store, content_store_id) = test_store();
        let planted = ContentRef {
            checksum: Checksum::crc32c(b"transferred straight to the PROVIDER"),
            ..content_ref
        };
        put_content_object(&store, &content_store_id, &planted, bytes).await;
        assert!(matches!(
            get_durable_content_bytes(&store, &content_store_id, &planted)
                .await
                .expect_err("crc mismatch"),
            DurableContentValidationError::ContentChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn read_verifies_a_reference_whose_only_evidence_is_a_crc64nvme() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"provider-assembled bytes";
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            checksum: Checksum::crc64nvme(bytes),
        };
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let read = get_durable_content_bytes(&store, &content_store_id, &content_ref)
            .await
            .expect("a crc-only reference verifies by its crc");
        assert_eq!(read, bytes);

        // Same length, different bytes: only the checksum can tell.
        let (_temp_dir, store, content_store_id) = test_store();
        let planted = ContentRef {
            checksum: Checksum::crc64nvme(b"provider-assembled BYTES"),
            ..content_ref.clone()
        };
        put_content_object(&store, &content_store_id, &planted, bytes).await;
        assert!(matches!(
            get_durable_content_bytes(&store, &content_store_id, &planted)
                .await
                .expect_err("crc mismatch"),
            DurableContentValidationError::ContentChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn checksum_verification_proves_the_object_without_reading_it() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = b"provider-verified bytes";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        store.reset();
        verify_durable_content_checksum(&store, &content_store_id, &content_ref)
            .await
            .expect("verify content ref");
        assert_eq!(
            store.count(OperationClass::Read),
            0,
            "verification reads provider metadata, never the payload"
        );
    }

    #[tokio::test]
    async fn checksum_verification_rejects_missing_size_and_checksum_drift() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"abc";
        let content_ref = content_ref(bytes);

        let err = verify_durable_content_checksum(&store, &content_store_id, &content_ref)
            .await
            .expect_err("missing object");
        assert!(matches!(
            err,
            DurableContentValidationError::MissingContentObject { .. }
        ));

        put_content_object(&store, &content_store_id, &content_ref, bytes).await;
        let mut wrong_size = content_ref.clone();
        wrong_size.size_bytes += 1;
        assert!(matches!(
            verify_durable_content_checksum(&store, &content_store_id, &wrong_size)
                .await
                .expect_err("size mismatch"),
            DurableContentValidationError::ContentLengthMismatch { .. }
        ));

        // The bytes at the key hash to something else: exactly the case the
        // completion check exists to catch on a provider that accepts a
        // wrong claim.
        let mut wrong_checksum = content_ref.clone();
        wrong_checksum.checksum = Checksum::sha256(b"other bytes");
        assert!(matches!(
            verify_durable_content_checksum(&store, &content_store_id, &wrong_checksum)
                .await
                .expect_err("checksum mismatch"),
            DurableContentValidationError::ContentChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn validate_content_ref_rejects_unsupported_kind() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = ContentRef {
            kind: ContentRefKind::Unsupported("kind_from_the_future".to_owned()),
            ..content_ref(b"bytes")
        };

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("unsupported content ref kind");
        assert!(matches!(
            err,
            DurableContentValidationError::InvalidContentRef(_)
        ));
    }

    #[tokio::test]
    async fn staging_identical_bytes_twice_mints_two_distinct_objects() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"identical payload";

        let first = store_bytes_as_content_with_store_id(&store, content_store_id.clone(), bytes)
            .await
            .expect("first stage");
        let second = store_bytes_as_content_with_store_id(&store, content_store_id, bytes)
            .await
            .expect("second stage");

        assert_ne!(
            first.content_ref().content_id,
            second.content_ref().content_id,
            "each staging write owns its own content object"
        );
        assert_ne!(first.object_key(), second.object_key());
        assert_eq!(
            first.content_ref().checksum,
            second.content_ref().checksum,
            "identical bytes still carry identical evidence"
        );
        for stored in [&first, &second] {
            assert_eq!(
                store
                    .get(stored.object_key(), None)
                    .await
                    .expect("read staged object")
                    .expect("staged object exists"),
                Bytes::from_static(b"identical payload")
            );
        }
    }

    /// Chunk size the streaming tests read in, small enough that a
    /// many-chunk object is a few kilobytes rather than tens of megabytes.
    const TEST_CHUNK_BYTES: u64 = 1024;

    fn test_chunk_bytes() -> NonZeroU64 {
        NonZeroU64::new(TEST_CHUNK_BYTES).expect("non-zero test chunk size")
    }

    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|offset| (offset % 251) as u8).collect()
    }

    fn test_entry() -> PathEntry {
        PathEntry {
            namespace_id: loonfs_api::NamespaceId::parse("demo").expect("namespace id"),
            path: loonfs_api::AbsolutePath::parse("/file.bin").expect("absolute path"),
            inode_id: loonfs_api::InodeId(1),
            created_by: loonfs_api::ActorRef::loonfs_system(),
            created_at_ms: 1,
            kind: loonfs_api::PathEntryKind::File {
                revision_no: loonfs_api::RevisionNo(1),
                size_bytes: 0,
                content_ref: ContentRef::blob_v1(loonfs_api::ContentId::generate(), b""),
                revision_committed_by: loonfs_api::ActorRef::loonfs_system(),
                revision_committed_at_ms: 1,
            },
            head_seq: loonfs_api::ChangeSeq(1),
            parent_inode_id: None,
            display_name: None,
            attributes: None,
        }
    }

    async fn open_stream<S: ObjectStore>(
        store: S,
        content_store_id: &ContentStoreId,
        content_ref: &ContentRef,
    ) -> Result<FileContentStream<S>, DurableContentValidationError> {
        open_stream_at(store, content_store_id, content_ref, 0).await
    }

    async fn open_stream_at<S: ObjectStore>(
        store: S,
        content_store_id: &ContentStoreId,
        content_ref: &ContentRef,
        start_offset: u64,
    ) -> Result<FileContentStream<S>, DurableContentValidationError> {
        FileContentStream::open(
            store,
            content_store_id,
            test_entry(),
            content_ref.clone(),
            test_chunk_bytes(),
            start_offset,
        )
        .await
    }

    #[tokio::test]
    async fn a_streamed_read_returns_the_object_one_chunk_at_a_time() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = payload(3 * TEST_CHUNK_BYTES as usize + 7);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;

        let mut stream = open_stream(&store, &content_store_id, &content_ref)
            .await
            .expect("open stream");
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            chunks.push(chunk);
        }

        assert_eq!(chunks.len(), 4, "three full chunks and the remainder");
        for chunk in &chunks[..3] {
            assert_eq!(chunk.len() as u64, TEST_CHUNK_BYTES);
        }
        assert_eq!(chunks[3].len(), 7);
        assert_eq!(chunks.concat(), bytes, "the object arrives byte-identical");
    }

    #[tokio::test]
    async fn a_finished_stream_repeats_its_verdict() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = payload(TEST_CHUNK_BYTES as usize + 3);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;

        let mut stream = open_stream(&store, &content_store_id, &content_ref)
            .await
            .expect("open stream");
        while stream.next_chunk().await.expect("chunk").is_some() {}
        assert!(stream.next_chunk().await.expect("verified end").is_none());
        assert!(stream.next_chunk().await.expect("verified end").is_none());
    }

    #[tokio::test]
    async fn a_resumed_read_fetches_only_the_rest_and_verifies_all_of_it() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = payload(3 * TEST_CHUNK_BYTES as usize);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;

        let held = 2 * TEST_CHUNK_BYTES as usize;
        store.reset();
        let mut stream = open_stream_at(&store, &content_store_id, &content_ref, held as u64)
            .await
            .expect("open stream");
        stream
            .fold_resumed_prefix(&bytes[..held])
            .expect("fold the held prefix");
        let mut fetched = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            fetched.extend_from_slice(&chunk);
        }
        assert_eq!(
            fetched,
            bytes[held..],
            "a resumed read hands back only what it fetched"
        );
        assert_eq!(
            store.count(OperationClass::Read),
            1,
            "one chunk was left to fetch, so one ranged read happened"
        );
    }

    #[tokio::test]
    async fn a_resumed_read_holds_the_prefix_to_the_same_verdict() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = payload(2 * TEST_CHUNK_BYTES as usize);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;
        let held = TEST_CHUNK_BYTES as usize;

        store.reset();
        let mut unfed = open_stream_at(&store, &content_store_id, &content_ref, held as u64)
            .await
            .expect("open stream");
        let err = unfed.next_chunk().await.expect_err("prefix still owed");
        assert!(
            matches!(
                err,
                CoreError::ResumePrefixIncomplete {
                    start_offset,
                    folded: 0
                } if start_offset == held as u64
            ),
            "unexpected error: {err}"
        );
        assert_eq!(
            store.count(OperationClass::Read),
            0,
            "nothing is fetched until the stream has what it skipped"
        );

        let mut wrong = open_stream_at(&store, &content_store_id, &content_ref, held as u64)
            .await
            .expect("open stream");
        wrong
            .fold_resumed_prefix(&vec![0u8; held])
            .expect("a prefix of the right length is accepted");
        let verdict = loop {
            match wrong.next_chunk().await {
                Ok(Some(_)) => continue,
                // A verified end is the only thing that reports `None`, so
                // this arm would mean bad bytes had been accepted.
                #[allow(clippy::panic, reason = "the failure this test exists to catch")]
                Ok(None) => panic!("a prefix that is not the object's verified"),
                Err(error) => break error,
            }
        };
        assert!(
            matches!(
                verdict,
                CoreError::DurableContent(
                    DurableContentValidationError::ContentChecksumMismatch { .. }
                )
            ),
            "a prefix that is not the object's fails the whole read: {verdict}"
        );
    }

    #[tokio::test]
    async fn a_resumed_prefix_may_be_folded_in_pieces() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = payload(2 * TEST_CHUNK_BYTES as usize);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;
        let held = TEST_CHUNK_BYTES as usize;

        let mut stream = open_stream_at(&store, &content_store_id, &content_ref, held as u64)
            .await
            .expect("open stream");
        for piece in bytes[..held].chunks(64) {
            stream.fold_resumed_prefix(piece).expect("fold a piece");
        }
        let mut fetched = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            fetched.extend_from_slice(&chunk);
        }
        assert_eq!(fetched, bytes[held..]);
    }

    #[tokio::test]
    async fn a_prefix_longer_than_the_content_is_refused() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = payload(2 * TEST_CHUNK_BYTES as usize);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;
        let held = TEST_CHUNK_BYTES as usize;

        store.reset();
        let mut stream = open_stream_at(&store, &content_store_id, &content_ref, held as u64)
            .await
            .expect("open stream");
        let overlong = payload(bytes.len() + 1);
        let err = stream
            .fold_resumed_prefix(&overlong)
            .expect_err("a prefix past the end of the content");
        assert!(
            matches!(
                err,
                CoreError::ResumeOffsetOutOfRange {
                    start_offset,
                    size_bytes,
                } if start_offset == overlong.len() as u64 && size_bytes == bytes.len() as u64
            ),
            "unexpected error: {err}"
        );
        assert_eq!(store.count(OperationClass::Read), 0, "nothing was fetched");
    }

    #[tokio::test]
    async fn a_prefix_offered_after_a_fetch_is_refused() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = payload(2 * TEST_CHUNK_BYTES as usize);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;

        let mut stream = open_stream(&store, &content_store_id, &content_ref)
            .await
            .expect("open stream");
        stream.next_chunk().await.expect("chunk").expect("a chunk");
        let err = stream
            .fold_resumed_prefix(&bytes[..8])
            .expect_err("a prefix after a fetch");
        assert!(
            matches!(err, CoreError::Internal(_)),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn a_prefix_offered_after_the_end_is_refused() {
        let (_temp_dir, store, content_store_id) = test_store();
        // Empty content reaches its verdict without fetching, which leaves
        // only the finished-stream guard between this fold and the digest.
        let content_ref = content_ref(b"");
        put_content_object(&store, &content_store_id, &content_ref, b"").await;

        let mut stream = open_stream(&store, &content_store_id, &content_ref)
            .await
            .expect("open stream");
        assert!(stream.next_chunk().await.expect("verified end").is_none());
        let err = stream
            .fold_resumed_prefix(b"")
            .expect_err("a prefix after the end");
        assert!(
            matches!(err, CoreError::Internal(_)),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn a_streamed_read_of_an_empty_object_verifies_without_fetching() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let content_ref = content_ref(b"");
        put_content_object(&store, &content_store_id, &content_ref, b"").await;

        store.reset();
        let mut stream = open_stream(&store, &content_store_id, &content_ref)
            .await
            .expect("open stream");
        assert!(stream.next_chunk().await.expect("verified end").is_none());
        assert_eq!(
            store.count(OperationClass::Read),
            0,
            "an empty object needs no ranged read"
        );
    }

    #[tokio::test]
    async fn a_streamed_read_verifies_a_reference_whose_only_evidence_is_a_crc32c() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = payload(2 * TEST_CHUNK_BYTES as usize + 5);
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            checksum: Checksum::crc32c(&bytes),
        };
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;

        let mut stream = open_stream(&store, &content_store_id, &content_ref)
            .await
            .expect("open stream");
        let mut fetched = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            fetched.extend_from_slice(&chunk);
        }
        assert_eq!(fetched, bytes);

        let (_temp_dir, store, content_store_id) = test_store();
        let planted = ContentRef {
            checksum: Checksum::crc32c(&payload(bytes.len() + 1)),
            ..content_ref
        };
        put_content_object(&store, &content_store_id, &planted, &bytes).await;
        let mut stream = open_stream(&store, &content_store_id, &planted)
            .await
            .expect("open stream");
        let verdict = loop {
            match stream.next_chunk().await {
                Ok(Some(_)) => continue,
                #[allow(clippy::panic, reason = "the failure this test exists to catch")]
                Ok(None) => panic!("an object that is not the reference's verified"),
                Err(error) => break error,
            }
        };
        assert!(
            matches!(
                verdict,
                CoreError::DurableContent(
                    DurableContentValidationError::ContentChecksumMismatch { .. }
                )
            ),
            "unexpected verdict: {verdict}"
        );
    }

    #[tokio::test]
    async fn a_resumed_crc32c_read_folds_the_prefix_into_the_same_verdict() {
        assert_resumed_checksum_verification(Checksum::crc32c).await;
    }

    #[tokio::test]
    async fn a_resumed_crc64nvme_read_folds_the_prefix_into_the_same_verdict() {
        assert_resumed_checksum_verification(Checksum::crc64nvme).await;
    }

    async fn assert_resumed_checksum_verification(checksum: fn(&[u8]) -> Checksum) {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = payload(2 * TEST_CHUNK_BYTES as usize);
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            checksum: checksum(&bytes),
        };
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;
        let held = TEST_CHUNK_BYTES as usize;

        store.reset();
        let mut stream = open_stream_at(&store, &content_store_id, &content_ref, held as u64)
            .await
            .expect("open stream");
        stream
            .fold_resumed_prefix(&bytes[..held])
            .expect("fold the held prefix");
        let mut fetched = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            fetched.extend_from_slice(&chunk);
        }
        assert_eq!(fetched, bytes[held..]);
        assert_eq!(
            store.count(OperationClass::Read),
            1,
            "one chunk was left to fetch, so one ranged read happened"
        );

        // The prefix is part of the verdict here too: wrong bytes below the
        // resume point fail the whole read.
        let mut wrong = open_stream_at(&store, &content_store_id, &content_ref, held as u64)
            .await
            .expect("open stream");
        wrong
            .fold_resumed_prefix(&vec![0u8; held])
            .expect("a prefix of the right length is accepted");
        let verdict = loop {
            match wrong.next_chunk().await {
                Ok(Some(_)) => continue,
                #[allow(clippy::panic, reason = "the failure this test exists to catch")]
                Ok(None) => panic!("a prefix that is not the object's verified"),
                Err(error) => break error,
            }
        };
        assert!(
            matches!(
                verdict,
                CoreError::DurableContent(
                    DurableContentValidationError::ContentChecksumMismatch { .. }
                )
            ),
            "unexpected verdict: {verdict}"
        );
    }

    #[tokio::test]
    async fn a_streamed_read_rejects_an_object_that_does_not_match_its_reference() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = payload(2 * TEST_CHUNK_BYTES as usize);
        let expected = content_ref(&bytes);
        // Same id and same length, different bytes: only the digest can tell.
        let mut planted = bytes.clone();
        planted[0] ^= 0xff;
        let planted_ref = ContentRef::blob_v1(expected.content_id.clone(), &planted);
        put_content_object(&store, &content_store_id, &planted_ref, &planted).await;

        let mut stream = open_stream(&store, &content_store_id, &expected)
            .await
            .expect("open stream");
        let mut chunks = 0;
        let err = loop {
            match stream.next_chunk().await {
                Ok(Some(_)) => chunks += 1,
                Ok(None) => break None,
                Err(err) => break Some(err),
            }
        }
        .expect("a mismatched object must not report a verified end");
        assert_eq!(chunks, 2, "the mismatch is reported after the last chunk");
        assert!(matches!(
            err,
            CoreError::DurableContent(
                DurableContentValidationError::ContentChecksumMismatch { .. }
            )
        ));
    }

    #[tokio::test]
    async fn a_streamed_read_reports_a_missing_object_when_it_opens() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = content_ref(b"never stored");

        let err = open_stream(&store, &content_store_id, &content_ref)
            .await
            .expect_err("missing object");
        assert!(matches!(
            err,
            DurableContentValidationError::MissingContentObject { .. }
        ));
    }

    #[tokio::test]
    async fn a_streamed_read_rejects_an_object_of_the_wrong_length() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = payload(TEST_CHUNK_BYTES as usize + 1);
        let mut content_ref = content_ref(&bytes);
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;
        content_ref.size_bytes += 1;

        let err = open_stream(&store, &content_store_id, &content_ref)
            .await
            .expect_err("length mismatch");
        assert!(matches!(
            err,
            DurableContentValidationError::ContentLengthMismatch { .. }
        ));
    }

    fn test_store() -> (tempfile::TempDir, LocalFsStore, ContentStoreId) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let content_store_id = ContentStoreId::parse("cs_00000000000000000000000000000001")
            .expect("valid content store id");
        (temp_dir, store, content_store_id)
    }

    async fn put_content_object(
        store: &impl ObjectStore,
        content_store_id: &ContentStoreId,
        content_ref: &ContentRef,
        bytes: &[u8],
    ) {
        let key = content_blob(content_store_id, &content_ref.content_id);
        store
            .put_if_absent(&key, Bytes::copy_from_slice(bytes))
            .await
            .expect("put content");
    }
}
