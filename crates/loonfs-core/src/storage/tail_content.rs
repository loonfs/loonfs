//! Writes the pieces the WAL tail holds for one content id into its content
//! object: the fold's materialization, which a direct download runs early.

use super::content::{
    content_object_key_for_ref, validate_loaded_content_bytes, FileContentStream,
    CONTENT_READ_CHUNK_BYTES,
};
use super::content_location::ContentLocation;
use crate::error::CoreError;
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::wal::{ProjectedContent, ProjectedWalTail};
use bytes::Bytes;
use futures::stream::{self, StreamExt, TryStreamExt};
use loonfs_objectstore::{
    required_etag, ByteRange, ExtendBase, ExtendedObject, ImmutableWriteError, ObjectMetadata,
    ObjectStore, ObjectStoreError, PROVIDER_MIN_COPIED_PART_BYTES,
};
use loonfs_types::{ChecksumAlgorithm, ContentRef, Sha256State};
use std::num::NonZeroU64;
use std::sync::Mutex;

/// Joins one content id's pieces from its first offset to its newest
/// reference's end. A chain that starts at offset 0 is the whole value, so
/// it is checked against the reference here, before anything is written.
pub(crate) fn assemble_tail_content(content: &ProjectedContent) -> Result<Bytes, CoreError> {
    let newest = &content.content_ref;
    let mut end = content.pieces[0].offset;
    for piece in &content.pieces {
        if piece.offset != end {
            break;
        }
        end = piece.end();
    }
    if end != newest.size_bytes {
        return Err(CoreError::NamespaceCorrupt(format!(
            "content `{}` has unfolded pieces that do not reach its newest reference",
            newest.content_id
        )));
    }
    let bytes = match content.pieces.as_slice() {
        [piece] => piece.bytes.clone(),
        pieces => pieces
            .iter()
            .map(|piece| piece.bytes.as_ref())
            .collect::<Vec<_>>()
            .concat()
            .into(),
    };
    if content.pieces[0].offset == 0 {
        validate_loaded_content_bytes(content_object_key_for_ref(newest)?, newest, &bytes)?;
    }
    Ok(bytes)
}

/// How one attempt to write a chain ended.
enum Attempt {
    /// The object holds the chain. The state is the SHA-256 state of the
    /// newest reference's bytes, when that reference is not a SHA-256 and
    /// the attempt computed it.
    Written(Option<Sha256State>),
    /// The object changed after the attempt measured it.
    Changed,
}

/// Writes `pieces`, the bytes from the first unfolded offset of `content`
/// to the end of its newest reference. Bytes that start at 0 are the whole
/// value. A chain that starts from a base gets the base's prefix and the
/// pieces at the id's own key, copied inside the provider where the store
/// can and streamed otherwise, so the base is never held whole. A key that
/// already holds part of the chain is extended from the length it holds.
///
/// Returns the SHA-256 state of the newest reference's bytes when that
/// reference is not a SHA-256 and this call computed the state: to attest
/// an extension, or alongside the check of a chain from a base.
pub(crate) async fn write_tail_content<S: ObjectStore + ?Sized>(
    store: &S,
    tail: &ProjectedWalTail,
    content: &ProjectedContent,
    pieces: Bytes,
) -> Result<Option<Sha256State>, CoreError> {
    let newest = &content.content_ref;
    let object_key = content_object_key_for_ref(newest)?;
    let first = &content.pieces[0];
    if first.offset == 0 {
        let written = store
            .put_immutable_verified(&object_key, pieces.clone())
            .await;
        if let Attempt::Written(state) = created(&object_key, written, None)? {
            return Ok(state);
        }
    }
    for _ in 0..CONTENTION_RETRY_LIMIT {
        let metadata = store
            .head(&object_key)
            .await
            .map_err(|error| CoreError::store(&object_key, &error))?;
        let Some(metadata) = metadata else {
            if first.base.is_none() {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "content object `{object_key}` is missing below its first unfolded offset {}",
                    first.offset
                )));
            }
            if let Attempt::Written(state) = create_from_base(store, tail, content).await? {
                return Ok(state);
            }
            continue;
        };
        if let Attempt::Written(state) =
            extend(store, &object_key, content, &pieces, metadata).await?
        {
            return Ok(state);
        }
    }
    Err(CoreError::contention_exhausted(&object_key))
}

/// Extends the object from the length it holds, or finds that another fold
/// already did.
async fn extend<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    content: &ProjectedContent,
    pieces: &Bytes,
    metadata: ObjectMetadata,
) -> Result<Attempt, CoreError> {
    let newest = &content.content_ref;
    let first = content.pieces[0].offset;
    let length = metadata.size_bytes;
    if length < first {
        return Err(CoreError::NamespaceCorrupt(format!(
            "content object `{object_key}` holds {length} bytes, fewer than the {first} its unfolded pieces follow"
        )));
    }
    let attested =
        (newest.checksum.algorithm == ChecksumAlgorithm::Sha256).then(|| newest.checksum.clone());
    if length >= newest.size_bytes {
        // Another fold wrote the pieces. An attestation at exactly the end
        // proves the whole value. Otherwise, and for an object that carries
        // no attestation, the bytes at the pieces' offsets are read back and
        // compared, so an object that holds other bytes under this reference
        // never lets a manifest publish.
        return match (&attested, &metadata.sha256) {
            (Some(attested), Some(held)) if length == newest.size_bytes => {
                if held != attested {
                    return Err(CoreError::NamespaceCorrupt(format!(
                        "content object `{object_key}` does not hold the bytes its newest reference names"
                    )));
                }
                Ok(Attempt::Written(None))
            }
            _ => Ok(if holds_pieces(store, object_key, first, pieces).await? {
                Attempt::Written(None)
            } else {
                Attempt::Changed
            }),
        };
    }
    let store_error = |error: ObjectStoreError| CoreError::store(object_key, &error);
    let remaining = pieces.slice((length - first) as usize..);
    let (sha256, computed) = match attested {
        Some(sha256) => (sha256, None),
        // A chain without a SHA-256 started from a client's direct upload,
        // which recorded no hash state, so the attestation has to read the
        // bytes the object holds. The fold records the state it computes,
        // so the chain's next append continues a SHA-256.
        None => {
            let mut state = Sha256State::new();
            while state.length() < length {
                let range = ByteRange {
                    start_inclusive: state.length(),
                    end_exclusive: length.min(state.length() + CONTENT_READ_CHUNK_BYTES),
                };
                let Some(held) = store
                    .get(object_key, Some(range))
                    .await
                    .map_err(store_error)?
                    .filter(|held| !held.is_empty())
                else {
                    return Ok(Attempt::Changed);
                };
                state.update(&held);
            }
            state.update(&remaining);
            (state.finish(), Some(state))
        }
    };
    let base = ExtendBase {
        length,
        etag: required_etag(object_key, metadata.etag).map_err(store_error)?,
    };
    let result = ExtendedObject {
        sha256,
        crc: content.crc64nvme.clone(),
    };
    match store
        .extend_object(object_key, &base, remaining, &result)
        .await
    {
        Ok(_) => Ok(Attempt::Written(computed)),
        Err(error) => refused(object_key, error),
    }
}

/// Whether the object holds `pieces` from `first`, read back in chunks.
/// Returns `false` when the object ends before them, so it changed after it
/// was measured. Other bytes there are corruption.
async fn holds_pieces<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    first: u64,
    pieces: &Bytes,
) -> Result<bool, CoreError> {
    let end = first + pieces.len() as u64;
    let mut offset = first;
    while offset < end {
        let chunk_end = end.min(offset + CONTENT_READ_CHUNK_BYTES);
        let range = ByteRange {
            start_inclusive: offset,
            end_exclusive: chunk_end,
        };
        let held = store
            .get(object_key, Some(range))
            .await
            .map_err(|error| CoreError::store(object_key, &error))?;
        let Some(held) = held.filter(|held| held.len() as u64 == chunk_end - offset) else {
            return Ok(false);
        };
        if held != pieces.slice((offset - first) as usize..(chunk_end - first) as usize) {
            return Err(CoreError::NamespaceCorrupt(format!(
                "content object `{object_key}` holds other bytes where its unfolded pieces belong"
            )));
        }
        offset = chunk_end;
    }
    Ok(true)
}

/// Creates the chain's own object from its base: the base's prefix copied
/// inside the provider where the store can, and streamed through this
/// process where it cannot. Returns `Attempt::Changed` when the key already
/// holds another fold's object, or the base moved before the copy.
async fn create_from_base<S: ObjectStore + ?Sized>(
    store: &S,
    tail: &ProjectedWalTail,
    content: &ProjectedContent,
) -> Result<Attempt, CoreError> {
    let newest = &content.content_ref;
    let location = ContentLocation::resolve(Some(tail), newest)?;
    if let Some(held) = location.check_prefix(store).await? {
        if let Some(attempt) = copy_from_base(store, &location, held, content).await? {
            return Ok(attempt);
        }
    }
    stream_from_base(store, location, newest).await
}

/// Asks the store to copy the base's prefix, which `held` describes, into
/// the chain's own key and add the pieces after it. The store records the
/// newest reference's checksum as the attestation, so only a SHA-256 chain
/// asks, and only for a prefix a provider copies. Returns `None` when the
/// chain does not ask or the store cannot copy, and otherwise how the copy
/// attempt ended.
async fn copy_from_base<S: ObjectStore + ?Sized>(
    store: &S,
    location: &ContentLocation,
    held: ObjectMetadata,
    content: &ProjectedContent,
) -> Result<Option<Attempt>, CoreError> {
    let newest = &content.content_ref;
    let Some((base_key, length)) = location.object_prefix().filter(|(_, length)| {
        *length >= PROVIDER_MIN_COPIED_PART_BYTES
            && newest.checksum.algorithm == ChecksumAlgorithm::Sha256
    }) else {
        return Ok(None);
    };
    let object_key = location.object_key();
    let base = ExtendBase {
        length,
        etag: required_etag(base_key, held.etag)
            .map_err(|error| CoreError::store(base_key, &error))?,
    };
    let result = ExtendedObject {
        sha256: newest.checksum.clone(),
        crc: content.crc64nvme.clone(),
    };
    match store
        .put_immutable_extended(
            object_key,
            base_key,
            &base,
            location.joined_pieces(),
            &result,
        )
        .await
    {
        Ok(created) => Ok(created.map(|_| Attempt::Written(None))),
        Err(error) => refused(object_key, error).map(Some),
    }
}

/// Streams the value a chain from a base names, the base's prefix and then
/// the pieces, into the chain's own key, `CONTENT_READ_CHUNK_BYTES` at a
/// time. The stream checks the whole value against the newest reference as
/// it passes and ends in an error when they differ, so the store creates
/// nothing. When the newest reference is not a SHA-256, the same bytes give
/// its SHA-256 state. `Attempt::Changed` is a key that already holds
/// another fold's object. The caller has checked the base's prefix.
async fn stream_from_base<S: ObjectStore + ?Sized>(
    store: &S,
    location: ContentLocation,
    newest: &ContentRef,
) -> Result<Attempt, CoreError> {
    let object_key = location.object_key().to_owned();
    let chunk_bytes = NonZeroU64::new(CONTENT_READ_CHUNK_BYTES)
        .expect("content read chunk size should be nonzero");
    let source = FileContentStream::open_with_checked_prefix(
        store,
        location,
        None,
        newest.clone(),
        chunk_bytes,
        0,
    )
    .await?;
    let sha256 =
        (newest.checksum.algorithm == ChecksumAlgorithm::Sha256).then_some(&newest.checksum);
    let mut computed = sha256.is_none().then(Sha256State::new);
    // The store sees only that the stream failed; the reason stays here.
    let failure = Mutex::new(None);
    let failed = &failure;
    let key = object_key.as_str();
    let body = stream::try_unfold(source, move |mut source| async move {
        match source.next_verified_chunk().await {
            Ok(chunk) => Ok(chunk.map(|chunk| (chunk, source))),
            Err(error) => {
                *failed
                    .lock()
                    .expect("stream failure lock should not be poisoned") = Some(error);
                Err(ObjectStoreError::transport(
                    key,
                    "the streamed value failed its check",
                ))
            }
        }
    })
    .inspect_ok(|chunk| {
        if let Some(state) = &mut computed {
            state.update(chunk);
        }
    })
    .boxed();
    let written = store
        .put_immutable_verified_stream(&object_key, newest.size_bytes, sha256, body)
        .await;
    if let Some(error) = failure
        .into_inner()
        .expect("stream failure lock should not be poisoned")
    {
        return Err(error.into());
    }
    created(&object_key, written, computed)
}

/// How a create ended. A landed create carries `state`. `Attempt::Changed`
/// is a key that already holds another fold's object, with a different
/// attestation or none, which the length table decides.
fn created(
    object_key: &str,
    written: Result<ObjectMetadata, ImmutableWriteError>,
    state: Option<Sha256State>,
) -> Result<Attempt, CoreError> {
    let error = match written {
        Ok(_) => return Ok(Attempt::Written(state)),
        Err(
            ImmutableWriteError::DifferentObject { .. } | ImmutableWriteError::Unattested { .. },
        ) => return Ok(Attempt::Changed),
        Err(error) => error,
    };
    tracing::error!(object_key, %error, "content materialization failed");
    Err(match error {
        ImmutableWriteError::Transport {
            object_key,
            source: ObjectStoreError::Transport { message, .. },
        } => CoreError::store(
            &object_key,
            &ObjectStoreError::retryable_transport(&object_key, message),
        ),
        error => CoreError::from(error),
    })
}

/// How an extension or a copy that the store refused ended. A base that
/// moved, or a key another fold created, is a change; a checksum mismatch
/// is corruption.
fn refused(object_key: &str, error: ObjectStoreError) -> Result<Attempt, CoreError> {
    match error {
        ObjectStoreError::PreconditionFailed { .. } => Ok(Attempt::Changed),
        ObjectStoreError::ChecksumMismatch { .. } => Err(CoreError::NamespaceCorrupt(format!(
            "content object `{object_key}` does not match the checksum its pieces record"
        ))),
        error => Err(CoreError::store(object_key, &error)),
    }
}
