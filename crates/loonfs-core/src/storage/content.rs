//! Content object reads and writes: minting immutable identities,
//! validating references, and verified read-back.

use crate::error::CoreError;
use crate::namespace::catalog::{load_namespace_content_store_id, VerifiedNamespaceCatalogEntry};
use crate::storage::content_admission::{ContentAdmission, PreparedContent};
use bytes::Bytes;
use futures::StreamExt;
use loonfs_api::{
    AuthoritativePathEntry, ContentId, ContentRef, ContentRefValidationError, ContentStoreId,
    NamespaceId, Sha256, StorageChecksum, StreamingChecksum,
};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{ByteRange, ByteStream, ObjectStore, ObjectStoreError, PutMode};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValidatedDurableContent {
    pub content_ref: ContentRef,
    pub object_key: String,
    pub file_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReadDurableContent {
    pub validated: ValidatedDurableContent,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoredContent {
    pub content_store_id: ContentStoreId,
    pub object_key: String,
    pub content_ref: ContentRef,
    pub file_size_bytes: u64,
    #[serde(skip)]
    _write_acknowledged: StoredContentWriteAcknowledgement,
}

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

pub(crate) async fn validate_durable_content_reference<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<ValidatedDurableContent, DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    validate_content_size(store, &object_key, content_ref).await?;

    let bytes = load_required_object(store, &object_key).await?;
    validate_loaded_content_bytes(object_key, content_ref, &bytes)
}

/// Prepares content from an acknowledged LoonFS-managed durable write.
///
/// Consuming [`StoredContent`] ties the proof to the successful return from
/// [`store_bytes_as_content`] or [`store_bytes_as_content_with_store_id`]. The
/// verified catalog prevents pairing that acknowledgement with an unrelated
/// namespace binding.
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
    let admission = ContentAdmission::for_durable_content_write(content_store_id, content_ref);
    Ok(PreparedContent::from_admission(admission))
}

/// Fully validates an existing durable content reference for publication.
///
/// The verified catalog selects the store to validate. This performs one
/// object HEAD followed by one full GET and checksum check.
pub async fn prepare_existing_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    content_ref: ContentRef,
) -> Result<PreparedContent, DurableContentValidationError> {
    let content_store_id = catalog.content_store_id();
    validate_durable_content_reference(store, content_store_id, &content_ref).await?;
    let admission =
        ContentAdmission::for_durable_content_write(content_store_id.clone(), content_ref);
    Ok(PreparedContent::from_admission(admission))
}

/// Verifies the object a reference names against that reference, from the
/// provider's own stored checksum and size.
///
/// This is the completion check for bytes that never passed through the
/// LoonFS server. It verifies rather than trusts: the presigned write is
/// checksum-bound, but a provider that accepts a wrong claim at assembly
/// time (Cloudflare R2 does, at multipart completion) would otherwise leave
/// a corrupt object publishable. One `HeadObject` with checksum mode enabled
/// answers both questions and moves no payload.
///
/// A caller that gets a mismatch owns the repair: the object sits at a
/// random id nothing references yet, so deleting it costs nothing and
/// leaving it would leak.
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
                message: err.message(),
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
    if stored.storage_checksum != content_ref.storage_checksum {
        return Err(DurableContentValidationError::ContentChecksumMismatch {
            object_key,
            expected: describe_checksum(&content_ref.storage_checksum),
            actual: describe_checksum(&stored.storage_checksum),
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
    let object_key = content_blob(content_store_id.as_str(), content_id);
    if let Err(error) = store.delete(&object_key).await {
        tracing::warn!(
            content_id = %content_id,
            error = %error,
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
    let object_key = content_blob(content_store_id.as_str(), content_id);
    if let Err(error) = store
        .abort_multipart_upload(&object_key, provider_upload_id)
        .await
    {
        tracing::warn!(
            content_id = %content_id,
            error = %error,
            "failed to abandon the multipart upload of a terminated upload session"
        );
    }
}

/// Bytes one ranged read of a content object fetches, and therefore the most
/// of that object a streaming read holds at once.
///
/// The same 8 MiB the write path moves a large payload in
/// ([`loonfs_objectstore::PROVIDER_MULTIPART_PART_BYTES`]): one transfer unit
/// for both directions, large enough that per-request overhead disappears
/// against the payload on a large object, small enough that a read's memory
/// is a fixed few megabytes whatever the object's size.
pub const CONTENT_READ_CHUNK_BYTES: u64 = 8 * 1024 * 1024;

/// One file's current content, read as fixed-size ranged chunks.
///
/// This is the streaming twin of the buffered content read, for a reader that
/// must not hold what it reads: chunks are fetched one range at a time and
/// the verifying digest is folded as they go, so a 50 GiB object costs one
/// chunk of memory rather than 50 GiB. It verifies exactly what the buffered
/// read verifies — the declared size, and the reference's trusted whole-file
/// SHA-256 when it has one, otherwise its own storage checksum, which for an
/// object this deployment did not hash itself is the only full-object
/// evidence there is.
///
/// The object is immutable and named by a random content id, so nothing can
/// rewrite it under a reader: chunk *n* and chunk *n+1* are always from the
/// same object, and no revalidation between them is needed or done.
///
/// Verification lands on the final [`Self::next_chunk`] call — the one that
/// reports the end of the content. A caller that stops early stops with
/// unverified bytes, which is what streaming means and why the buffered read
/// stays for callers that want the whole answer or none of it.
pub struct FileContentStream<S> {
    store: S,
    entry: AuthoritativePathEntry,
    object_key: String,
    content_ref: ContentRef,
    chunk_bytes: NonZeroU64,
    /// Offset the next ranged read starts at; also how much has been read.
    next_offset: u64,
    /// Offset this stream was opened at. Zero for a read of the whole
    /// object, and the length of what the caller already holds for a
    /// resumed one.
    resumed_from: u64,
    /// How much of that head start the caller has folded in so far. The
    /// stream fetches nothing until this reaches `resumed_from`, because
    /// the verdict is over the whole object either way.
    prefix_folded: u64,
    /// The checksum the complete object must produce, folded so far.
    digest: StreamingChecksum,
    /// The value `digest` is closed against.
    expected: StorageChecksum,
    /// The verdict on the complete object, once there is one. Kept because a
    /// digest can only be closed once: without it, asking again after the end
    /// would fold a second, empty digest and report a mismatch that is not
    /// one.
    completion: Option<Result<(), DurableContentValidationError>>,
}

impl<S: ObjectStore> FileContentStream<S> {
    /// Opens a streaming read of the object `content_ref` names.
    ///
    /// One `HeadObject` proves the object exists and is exactly as long as
    /// the reference claims before any payload moves, which is what lets a
    /// wrong-sized object fail without a partial answer having been handed
    /// out.
    /// `start_offset` is where the caller already is: bytes below it are
    /// never fetched, and [`Self::fold_resumed_prefix`] is how they still
    /// reach the digest.
    pub(crate) async fn open(
        store: S,
        content_store_id: &ContentStoreId,
        entry: AuthoritativePathEntry,
        content_ref: ContentRef,
        chunk_bytes: NonZeroU64,
        start_offset: u64,
    ) -> Result<Self, DurableContentValidationError> {
        let object_key = content_object_key_for_ref(content_store_id, &content_ref)?;
        validate_content_size(&store, &object_key, &content_ref).await?;
        let expected = content_ref.verifiable_checksum();
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

    /// Hands the stream part of what the caller already holds, in order,
    /// from the object's first byte.
    ///
    /// A resumed read still reports on the whole object, so the bytes it
    /// will never fetch have to be folded into the same digest that closes
    /// over the ones it does. Feeding the wrong bytes fails verification at
    /// the end, which is exactly right: the reference is the authority on
    /// what the object holds, not the partial copy on the caller's disk.
    ///
    /// Feeding the wrong *number* of bytes, or feeding them at the wrong
    /// time, is refused here instead. A digest is order-dependent and closes
    /// once, so those could only ever surface as a corruption verdict at the
    /// end — a report about the object, for a mistake that was never the
    /// object's.
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
    pub fn entry(&self) -> &AuthoritativePathEntry {
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
    /// This is the method callers outside this crate hold, so it speaks the
    /// crate's error type: a content object that disagrees with its reference
    /// is namespace corruption, and it is classified as such here rather than
    /// at every call site. A resumed stream that has not been told what it
    /// skipped is the caller's own mistake instead, and says so before
    /// anything is fetched.
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
                    message: err.message(),
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

pub(crate) async fn read_durable_content_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<ReadDurableContent, DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    let bytes = load_required_object(store, &object_key).await?;
    let validated = validate_loaded_content_bytes(object_key, content_ref, &bytes)?;

    Ok(ReadDurableContent { validated, bytes })
}

pub(crate) fn content_object_key_for_ref(
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<String, DurableContentValidationError> {
    content_ref
        .validate()
        .map_err(DurableContentValidationError::InvalidContentRef)?;
    Ok(content_blob(
        content_store_id.as_str(),
        &content_ref.content_id,
    ))
}

/// Checks fetched bytes against everything the reference claims about them.
///
/// The whole-file SHA-256 is the check whenever it is present; a reference
/// that carries only a CRC — which a direct transfer produces, because an
/// object assembled by the provider or written by the client is never hashed
/// by us — is verified by that CRC instead.
fn validate_loaded_content_bytes(
    object_key: String,
    content_ref: &ContentRef,
    bytes: &[u8],
) -> Result<ValidatedDurableContent, DurableContentValidationError> {
    let actual_size = bytes.len() as u64;
    if actual_size != content_ref.size_bytes {
        return Err(DurableContentValidationError::ContentLengthMismatch {
            object_key,
            expected: content_ref.size_bytes,
            actual: actual_size,
        });
    }

    let expected = content_ref.verifiable_checksum();
    if !expected.matches(bytes) {
        let actual = StorageChecksum::compute(expected.algorithm, bytes);
        return Err(DurableContentValidationError::ContentChecksumMismatch {
            object_key,
            expected: describe_checksum(&expected),
            actual: describe_checksum(&actual),
        });
    }

    Ok(ValidatedDurableContent {
        content_ref: content_ref.clone(),
        object_key,
        file_size_bytes: actual_size,
    })
}

fn describe_checksum(checksum: &StorageChecksum) -> String {
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
                message: err.message(),
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
    fields(phase = "write_content_blob", key_class = "content_blob")
)]
pub async fn store_bytes_as_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    store_bytes_as_content_with_store_id(store, content_store_id, bytes).await
}

/// Plants durable content for a caller that already knows the namespace's
/// content-store binding.
///
/// Every call mints its own content identity, so two writers staging the
/// same bytes produce two objects rather than racing for one key. Sharing a
/// key was free deduplication and also a free existence oracle: anyone
/// allowed to upload could learn whether specific known bytes were already
/// in a shared content store. Retry idempotency, the thing that dedup was
/// quietly providing, belongs to the upload session instead.
///
/// So does reclamation, which is why this is a fixture rather than a write
/// path. The object it writes belongs to no session, and a session record is
/// the only handle anything has on a content object before metadata names
/// one — so nothing will ever collect it. Production staging opens a session
/// ([`crate::protocol::stage_owned_bytes`]); immortal bytes are what a test
/// wants and what a namespace does not.
pub(crate) async fn store_bytes_as_content_with_store_id<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: ContentStoreId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    stage_bytes_under_content_id(store, content_store_id, ContentId::generate(), bytes).await
}

/// What a streamed staging write established about the content object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedStream {
    /// Identity, length, and the digest folded over the payload on its way
    /// through. The digest is always over the complete stream: the store
    /// consumes the body before it evaluates any precondition.
    pub content_ref: ContentRef,
    /// Whether the key was already occupied when the write tried to create
    /// it. Only this session can have written there — the identity is
    /// random and belongs to one session — so the caller decides whether
    /// this is its own earlier attempt replayed or a conflicting one, by
    /// comparing `content_ref` against what the session recorded.
    pub already_present: bool,
}

/// Stages a payload that arrives as a stream, hashing it on the way through.
///
/// The bytes are never held whole: the digest is folded chunk by chunk as
/// they are forwarded to the store, and the reference is built from that
/// digest and the length the store reports back. The result carries a
/// trusted `whole_file_sha256` for the same reason the buffered path's does
/// — the LoonFS write path hashed the complete payload itself — and it is
/// the constructor, not a convention, that guarantees it.
///
/// The write is create-only, exactly like the buffered staging write. Past
/// the store's multipart threshold the store cannot make that condition part
/// of the write — a provider assembles a multipart object unconditionally —
/// so it reads the key instead, immediately before the assembly. That read
/// is not atomic with the assembly, so `already_present` answers for a key
/// that was occupied before this write started and not for one occupied
/// during it.
///
/// The only writer that could occupy the key during the write is another
/// request against the same upload session, because the key is named by 128
/// random bits one session owns. The staging claim in
/// [`crate::protocol::upload_streamed_content`] is what keeps that writer
/// away: exactly one request holds the claim, so `already_present` is exact
/// for every caller that takes it. A caller that stages under a freshly
/// minted identity nobody else holds — [`crate::protocol::stage_owned_stream`]
/// — needs no claim for the same reason.
pub(crate) async fn stage_streamed_under_content_id<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: ContentStoreId,
    content_id: ContentId,
    body: ByteStream,
) -> Result<StagedStream, CoreError> {
    let object_key = content_blob(content_store_id.as_str(), &content_id);
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
        // The key is occupied. Only this session can name it, so the caller
        // decides from the digest whether that was its own earlier attempt.
        Err(ObjectStoreError::PreconditionFailed { .. }) => true,
        Err(err) => return Err(CoreError::store(&object_key, &err)),
    };

    Ok(StagedStream {
        content_ref: ContentRef::blob_v1_streamed(content_id, observed.size_bytes, observed.digest),
        already_present,
    })
}

/// Reads a payload without writing it anywhere, and reports what it was.
///
/// This is how a session that has already staged content answers a repeated
/// upload: the only way to tell "the same bytes again" from "different
/// bytes" is to hash them, and the object it already owns must not be
/// touched while that is decided.
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

/// What a streamed payload amounted to, folded as it passed through.
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
    let object_key = content_blob(content_store_id.as_str(), &content_ref.content_id);
    // Create-only plus the byte check stay on this write even though a
    // random id cannot collide: if this key is ever occupied by different
    // bytes, that is corruption, and it must fail loudly rather than be
    // overwritten.
    store
        .put_immutable_verified(&object_key, Bytes::copy_from_slice(bytes))
        .await?;

    Ok(StoredContent {
        content_store_id,
        object_key,
        file_size_bytes: content_ref.size_bytes,
        content_ref,
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
            message: err.message(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        read_durable_content_bytes, store_bytes_as_content_with_store_id,
        validate_durable_content_reference, verify_durable_content_checksum, CoreError,
        DurableContentValidationError, FileContentStream, NonZeroU64,
    };
    use bytes::Bytes;
    use loonfs_api::{
        AuthoritativePathEntry, ContentId, ContentRef, ContentRefKind, ContentStoreId,
        StorageChecksum,
    };
    use loonfs_objectstore::keys::content_blob;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass};
    use tempfile::tempdir;

    fn content_ref(bytes: &[u8]) -> ContentRef {
        ContentRef::blob_v1(ContentId::generate(), bytes)
    }

    #[tokio::test]
    async fn validate_content_ref_success() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"whole file bytes";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let validated = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect("validate content ref");
        assert_eq!(validated.content_ref, content_ref);
        assert_eq!(validated.file_size_bytes, bytes.len() as u64);
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
    async fn validate_content_ref_accepts_empty_files() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let read = read_durable_content_bytes(&store, &content_store_id, &content_ref)
            .await
            .expect("read empty content ref");
        assert_eq!(read.bytes, bytes);
        assert_eq!(read.validated.file_size_bytes, 0);
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

    /// A direct transfer to Google Cloud Storage produces a reference whose
    /// only evidence is the CRC-32C that provider computed. Reads verify it
    /// like any other full-object evidence, and a wrong object fails on it.
    #[tokio::test]
    async fn read_verifies_a_reference_whose_only_evidence_is_a_crc32c() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"transferred straight to the provider";
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum::crc32c(bytes),
            whole_file_sha256: None,
        };
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let read = read_durable_content_bytes(&store, &content_store_id, &content_ref)
            .await
            .expect("a crc32c-only reference verifies by its crc");
        assert_eq!(read.bytes, bytes);

        // Same length, different bytes: only the checksum can tell.
        let (_temp_dir, store, content_store_id) = test_store();
        let planted = ContentRef {
            storage_checksum: StorageChecksum::crc32c(b"transferred straight to the PROVIDER"),
            ..content_ref
        };
        put_content_object(&store, &content_store_id, &planted, bytes).await;
        assert!(matches!(
            read_durable_content_bytes(&store, &content_store_id, &planted)
                .await
                .expect_err("crc mismatch"),
            DurableContentValidationError::ContentChecksumMismatch { .. }
        ));
    }

    /// A direct multipart upload produces a reference whose only evidence is
    /// the CRC-64/NVME the provider computed over the assembly. Reads must
    /// verify it — the alternative is a whole write path whose bytes are
    /// never checked on the way back out.
    #[tokio::test]
    async fn read_verifies_a_reference_whose_only_evidence_is_a_crc64nvme() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"provider-assembled bytes";
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum::crc64nvme(bytes),
            whole_file_sha256: None,
        };
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let read = read_durable_content_bytes(&store, &content_store_id, &content_ref)
            .await
            .expect("a crc-only reference verifies by its crc");
        assert_eq!(read.bytes, bytes);

        // Same length, different bytes: only the checksum can tell.
        let (_temp_dir, store, content_store_id) = test_store();
        let planted = ContentRef {
            storage_checksum: StorageChecksum::crc64nvme(b"provider-assembled BYTES"),
            ..content_ref.clone()
        };
        put_content_object(&store, &content_store_id, &planted, bytes).await;
        assert!(matches!(
            read_durable_content_bytes(&store, &content_store_id, &planted)
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
        wrong_checksum.storage_checksum = StorageChecksum::sha256(b"other bytes");
        wrong_checksum.whole_file_sha256 = Some(wrong_checksum.storage_checksum.value.clone());
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

    /// Two writers staging the same bytes get two objects. There is no
    /// shared key to coalesce on, so neither can observe the other.
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
            first.content_ref.content_id, second.content_ref.content_id,
            "each staging write owns its own content object"
        );
        assert_ne!(first.object_key, second.object_key);
        assert_eq!(
            first.content_ref.storage_checksum, second.content_ref.storage_checksum,
            "identical bytes still carry identical evidence"
        );
        for stored in [&first, &second] {
            assert_eq!(
                store
                    .get(&stored.object_key, None)
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

    fn test_entry() -> AuthoritativePathEntry {
        AuthoritativePathEntry {
            namespace_id: loonfs_api::NamespaceId::parse("demo").expect("namespace id"),
            absolute_path: loonfs_api::AbsolutePath::parse("/file.bin").expect("absolute path"),
            inode_id: loonfs_api::InodeId(1),
            kind: loonfs_api::AuthoritativePathEntryKind::File {
                revision_no: loonfs_api::RevisionNo(1),
                size_bytes: 0,
                content_ref: ContentRef::blob_v1(loonfs_api::ContentId::generate(), b""),
                committed_at_ms: 1,
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

    /// A streamed read hands back the object in chunks of the size it was
    /// opened with, in order, and ends only after verifying the whole thing.
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

    /// The end is an answer, not an event: a caller that asks again after it
    /// gets the same verdict rather than a digest closed a second time over
    /// nothing.
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

    /// A resumed read fetches only what it does not already have, and still
    /// closes its verdict over the whole object.
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

    /// The prefix is part of the verdict, not a formality: bytes that are
    /// not the object's fail the read at its end, and a stream driven before
    /// it has them refuses to fetch anything at all.
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

    /// A prefix handed over in pieces is the ordinary case, and the fold
    /// accepts every one of them: the digest only cares that the bytes
    /// arrive in order.
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

    /// A partial longer than the content it claims to resume is the caller's
    /// own file being wrong, and it is refused where that is still visible
    /// — not folded in to reappear as a corruption verdict about the object.
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

    /// The digest folds in one direction, so a prefix offered after the
    /// stream has already fetched content could only ever land in the wrong
    /// place. Saying so beats folding it and blaming the object.
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

    /// The verdict is closed once. A prefix offered after it exists cannot
    /// change it, so the fold refuses rather than pretending to.
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

    /// An empty file has nothing to fetch and still verifies: the digest of
    /// no bytes is the digest its reference carries.
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

    /// A CRC-32C-only reference is what a direct transfer to Google Cloud
    /// Storage leaves behind, and a streamed read of one is verified like any
    /// other: folded chunk by chunk, closed at the end, and failed when the
    /// object is not what the reference says.
    #[tokio::test]
    async fn a_streamed_read_verifies_a_reference_whose_only_evidence_is_a_crc32c() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = payload(2 * TEST_CHUNK_BYTES as usize + 5);
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum::crc32c(&bytes),
            whole_file_sha256: None,
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
            storage_checksum: StorageChecksum::crc32c(&payload(bytes.len() + 1)),
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

    /// A CRC folds forward from a running value, so a resumed read of a
    /// CRC-32C-only reference closes over the prefix it never fetched
    /// exactly as a hashed one does.
    #[tokio::test]
    async fn a_resumed_crc32c_read_folds_the_prefix_into_the_same_verdict() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = payload(2 * TEST_CHUNK_BYTES as usize);
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum::crc32c(&bytes),
            whole_file_sha256: None,
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

    /// The same evidence the buffered read holds bytes to, folded chunk by
    /// chunk: a provider-assembled object carries only a CRC, and a streamed
    /// read verifies it rather than waving it through.
    #[tokio::test]
    async fn a_streamed_read_verifies_a_reference_whose_only_evidence_is_a_crc64nvme() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = payload(2 * TEST_CHUNK_BYTES as usize);
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum::crc64nvme(&bytes),
            whole_file_sha256: None,
        };
        put_content_object(&store, &content_store_id, &content_ref, &bytes).await;

        let mut stream = open_stream(&store, &content_store_id, &content_ref)
            .await
            .expect("open stream");
        let mut read = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            read.extend_from_slice(&chunk);
        }
        assert_eq!(read, bytes);
    }

    /// Bytes that disagree with the reference fail the read at the call that
    /// reports the end, after the chunks have been handed out. That is what
    /// streaming costs, and why a caller that installs a file installs it
    /// only once this call has returned.
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

    /// An object that is not there fails when the stream is opened, so a
    /// caller learns it before it has written anything anywhere.
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

    /// An object longer or shorter than its reference claims is caught
    /// before any of it is handed out, by the same size check the buffered
    /// read makes over the bytes it downloaded.
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
        let key = content_blob(content_store_id.as_str(), &content_ref.content_id);
        store
            .put_if_absent(&key, Bytes::copy_from_slice(bytes))
            .await
            .expect("put content");
    }
}
