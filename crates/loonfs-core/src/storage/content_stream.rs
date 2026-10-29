//! The verified streaming read of a content object, whole or in a range, and
//! the import reader built on it.

use crate::error::{CoreError, Result};
use crate::limits::CONTENT_READ_CHUNK_BYTES;
use crate::object_cache::ObjectCache;
use crate::storage::content::{
    describe_checksum, validate_loaded_content_bytes, DurableContentValidationError,
};
use crate::storage::content_location::ContentLocation;
use bytes::Bytes;
use futures::StreamExt;
use loonfs_objectstore::{ByteStream, ObjectStore, ObjectStoreError};
use loonfs_types::options::ReadRange;
use loonfs_types::{ContentRef, ContentVerifier, PathEntry};
use std::num::NonZeroU64;
use std::sync::Arc;

/// Opens a verified chunked reader over existing content for import.
pub(crate) async fn open_content_import_reader<S: ObjectStore + 'static>(
    store: S,
    location: ContentLocation,
    content_ref: &ContentRef,
) -> std::result::Result<ByteStream, DurableContentValidationError> {
    let stream_key = location.object_key().to_owned();
    let mut source = FileContentStream::open_inner(
        store,
        location,
        None,
        content_ref.clone(),
        CONTENT_READ_CHUNK_BYTES,
        StreamRange::default(),
    )
    .await?;
    let first = source.next_verified_chunk().await?;
    if source.next_offset == content_ref.size_bytes {
        source.completion()?;
    }
    let remaining = futures::stream::try_unfold(source, move |mut source| {
        let stream_key = stream_key.clone();
        async move {
            match source.next_verified_chunk().await {
                Ok(chunk) => Ok(chunk.map(|bytes| (bytes, source))),
                Err(error @ DurableContentValidationError::ContentChecksumMismatch { .. }) => {
                    Err(ObjectStoreError::BodyAborted(error.to_string()))
                }
                Err(error) => Err(ObjectStoreError::transport(stream_key, error.to_string())),
            }
        }
    })
    .boxed();
    Ok(futures::stream::iter(first.into_iter().map(Ok))
        .chain(remaining)
        .boxed())
}

/// The bytes of a file that a [`FileContentStream`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRange {
    /// The file from `start_offset` to its end. The stream verifies the whole
    /// file's size and checksum when it ends. A nonzero `start_offset`
    /// resumes an interrupted read, so the caller must first pass the bytes
    /// below `start_offset` to [`FileContentStream::fold_resumed_prefix`].
    Whole { start_offset: u64 },
    /// One byte range of the file. The stream doesn't verify the bytes,
    /// because the file's checksum covers the whole file.
    Range(ReadRange),
}

impl Default for StreamRange {
    fn default() -> Self {
        Self::Whole { start_offset: 0 }
    }
}

impl StreamRange {
    /// Returns an error if a file of `size_bytes` can't satisfy this range.
    ///
    /// Callers run this check before they read any content.
    pub(crate) fn ensure_within(self, size_bytes: u64) -> Result<()> {
        match self {
            Self::Whole { start_offset } if start_offset > size_bytes => {
                Err(CoreError::ResumeOffsetOutOfRange {
                    start_offset,
                    size_bytes,
                })
            }
            Self::Whole { .. } => Ok(()),
            Self::Range(range) => {
                range
                    .end_offset(size_bytes)
                    .map(drop)
                    .ok_or(CoreError::RangeNotSatisfiable {
                        start_offset: range.start_offset,
                        size_bytes,
                    })
            }
        }
    }
}

/// How a [`FileContentStream`] checks the bytes it returns.
enum StreamCheck {
    /// The stream reads to the end of the file and verifies the whole file.
    Whole(ContentVerifier),
    /// The stream reads a range up to `end_offset`, the offset after its last
    /// byte, and it checks the length of each store read but not the file's
    /// checksum.
    Range { end_offset: u64 },
}

/// Reads file content in fixed-size chunks, and verifies its size and
/// checksum unless the read returns a range of the file.
///
/// Verification completes when [`Self::next_chunk`] returns `None`. A caller
/// that stops earlier has not verified the complete object.
pub struct FileContentStream<S> {
    store: S,
    entry: Option<PathEntry>,
    location: ContentLocation,
    content_ref: ContentRef,
    chunk_bytes: NonZeroU64,
    /// Offset of the first byte the stream returns.
    start_offset: u64,
    /// Start offset of the next ranged read.
    next_offset: u64,
    check: StreamCheck,
    /// Where ranged reads look for content objects before the store.
    object_cache: Option<Arc<ObjectCache>>,
    /// A whole reference read in one chunk, kept until the stream verifies
    /// it so that `object_cache` can keep the verified bytes.
    unverified_object: Option<Bytes>,
}

impl<S: ObjectStore> FileContentStream<S> {
    /// Opens a stream over `range` of the content, which the caller has
    /// checked with [`StreamRange::ensure_within`].
    pub(crate) async fn open_inner(
        store: S,
        location: ContentLocation,
        entry: Option<PathEntry>,
        content_ref: ContentRef,
        chunk_bytes: NonZeroU64,
        range: StreamRange,
    ) -> std::result::Result<Self, DurableContentValidationError> {
        let (start_offset, check) = match range {
            StreamRange::Whole { start_offset } => {
                if location.is_resident() {
                    let bytes = location
                        .read_range(&store, None, 0, content_ref.size_bytes)
                        .await?;
                    validate_loaded_content_bytes(
                        location.object_key().to_owned(),
                        &content_ref,
                        &bytes,
                    )?;
                }
                (
                    start_offset,
                    StreamCheck::Whole(ContentVerifier::new(&content_ref, start_offset)),
                )
            }
            StreamRange::Range(range) => {
                let end_offset = range
                    .end_offset(content_ref.size_bytes)
                    .expect("the caller checks that a range starts inside the content");
                (range.start_offset, StreamCheck::Range { end_offset })
            }
        };
        Ok(Self {
            store,
            entry,
            location,
            content_ref,
            chunk_bytes,
            start_offset,
            next_offset: start_offset,
            check,
            object_cache: None,
            unverified_object: None,
        })
    }

    /// Reads content objects from `object_cache` before the store, and keeps
    /// a small object there once the stream verifies all of its bytes.
    pub(crate) fn with_object_cache(mut self, object_cache: Arc<ObjectCache>) -> Self {
        self.object_cache = Some(object_cache);
        self
    }

    /// Adds already-downloaded prefix bytes to a resumed read's checksum.
    ///
    /// Bytes must be supplied in order before fetching new chunks. A prefix
    /// that runs past the start offset, or that arrives after the stream has
    /// fetched content or reached its end, is refused with an
    /// `invalid_request` error, and so is a prefix for a ranged read.
    /// Incorrect bytes cause checksum verification to fail at the end of the
    /// stream.
    pub fn fold_resumed_prefix(&mut self, bytes: &[u8]) -> Result<()> {
        match &mut self.check {
            StreamCheck::Whole(verifier) => Ok(verifier.fold_resumed_prefix(bytes)?),
            StreamCheck::Range { .. } => Err(CoreError::PrefixForRangedRead),
        }
    }

    /// The authoritative metadata entry for a path read. Inode-only reads
    /// have no path entry, including when the file has been deleted.
    pub fn entry(&self) -> Option<&PathEntry> {
        self.entry.as_ref()
    }

    /// The immutable content claim this stream verifies.
    pub fn content_ref(&self) -> &ContentRef {
        &self.content_ref
    }

    /// Complete length of the content this stream reads.
    pub fn size_bytes(&self) -> u64 {
        self.content_ref.size_bytes
    }

    /// Returns the offset of the first byte that this stream returns and the
    /// offset after its last byte.
    pub fn offsets(&self) -> std::ops::Range<u64> {
        self.start_offset..self.end_offset()
    }

    fn end_offset(&self) -> u64 {
        match self.check {
            StreamCheck::Whole(_) => self.content_ref.size_bytes,
            StreamCheck::Range { end_offset } => end_offset,
        }
    }

    /// Fetches the next chunk, or reports the end of the read.
    ///
    /// For a read of the whole file, `Ok(None)` is returned only after the
    /// folded digest and the byte count agree with the reference, and a
    /// mismatch fails this call instead. Chunks arrive in order from wherever
    /// the stream started, and every one but the last is exactly the chunk
    /// size this stream was opened with.
    ///
    /// A resumed stream must receive its complete prefix before this method is
    /// called.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if let StreamCheck::Whole(verifier) = &self.check {
            verifier
                .ensure_resumed_prefix_complete()
                .map_err(|incomplete| CoreError::ResumePrefixIncomplete {
                    start_offset: incomplete.start_offset,
                    folded: incomplete.prefix_bytes,
                })?;
        }
        Ok(self.next_verified_chunk().await?)
    }

    pub(super) async fn next_verified_chunk(
        &mut self,
    ) -> std::result::Result<Option<Bytes>, DurableContentValidationError> {
        let end_offset = self.end_offset();
        if self.next_offset == end_offset {
            if let StreamCheck::Range { .. } = self.check {
                return Ok(None);
            }
            self.completion()?;
            if let Some(((object_cache, object_key), bytes)) = self
                .object_cache
                .as_deref()
                .zip(self.location.single_object_key())
                .zip(self.unverified_object.take())
            {
                object_cache.insert_content(object_key, &bytes);
            }
            return Ok(None);
        }
        let end_exclusive = self
            .next_offset
            .saturating_add(self.chunk_bytes.get())
            .min(end_offset);
        let bytes = self
            .location
            .read_range(
                &self.store,
                self.object_cache.as_deref(),
                self.next_offset,
                end_exclusive,
            )
            .await?;
        // A range ending past the object is truncated, so a short response is
        // an object that ended earlier than its reference says it does.
        if bytes.len() as u64 != end_exclusive - self.next_offset {
            return Err(DurableContentValidationError::ContentLengthMismatch {
                object_key: self.location.object_key().to_owned(),
                expected: self.content_ref.size_bytes,
                actual: self.next_offset + bytes.len() as u64,
            });
        }
        if let StreamCheck::Whole(verifier) = &mut self.check {
            verifier.update(&bytes).expect(
                "a chunk below the content size arrives before `completion` finishes the verifier",
            );
            if self.next_offset == 0 && end_exclusive == self.content_ref.size_bytes {
                self.unverified_object = self.object_cache.as_ref().map(|_| bytes.clone());
            }
        }
        self.next_offset += bytes.len() as u64;
        Ok(Some(bytes))
    }

    /// The verdict on the complete object: computed the first time the end is
    /// reached, and repeated on every later ask.
    fn completion(&mut self) -> std::result::Result<(), DurableContentValidationError> {
        let StreamCheck::Whole(verifier) = &mut self.check else {
            return Ok(());
        };
        let mismatch = match verifier.finish() {
            Ok(()) => return Ok(()),
            Err(mismatch) => mismatch,
        };
        let object_key = self.location.object_key().to_owned();
        if mismatch.actual_bytes != mismatch.expected_bytes {
            return Err(DurableContentValidationError::ContentLengthMismatch {
                object_key,
                expected: mismatch.expected_bytes,
                actual: mismatch.actual_bytes,
            });
        }
        Err(DurableContentValidationError::ContentChecksumMismatch {
            object_key,
            expected: describe_checksum(&mismatch.expected),
            actual: describe_checksum(&mismatch.actual),
        })
    }
}

impl<S> std::fmt::Debug for FileContentStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileContentStream")
            .field("object_key", &self.location.object_key())
            .field("size_bytes", &self.content_ref.size_bytes)
            .field("next_offset", &self.next_offset)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::content::tests::{put_content_object, test_store};
    use super::*;

    use loonfs_objectstore::keys::content_blob;

    use loonfs_objectstore::ObjectStore;
    use loonfs_test_support::ids::content_ref;
    use loonfs_test_support::stores::{KeyPredicate, OperationSelector, RecordingStore};
    use loonfs_types::{Checksum, ContentId, ContentRef, ContentRefKind, PathEntry};
    use std::assert_matches;

    /// Chunk size the streaming tests read in, small enough that a
    /// many-chunk object is a few kilobytes rather than tens of megabytes.
    const TEST_CHUNK_BYTES: usize = 1024;

    fn test_chunk_bytes() -> NonZeroU64 {
        NonZeroU64::new(TEST_CHUNK_BYTES as u64).expect("non-zero test chunk size")
    }

    fn payload(len: usize) -> Vec<u8> {
        (0..251_u8).cycle().take(len).collect()
    }

    fn test_entry() -> PathEntry {
        PathEntry {
            namespace_id: loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
            path: loonfs_types::AbsolutePath::parse("/file.bin").expect("absolute path"),
            inode_id: loonfs_types::InodeId(1),
            created_by: loonfs_types::ActorId::loonfs(),
            created_at_ms: 1,
            kind: loonfs_types::PathEntryKind::File {
                revision_no: loonfs_types::RevisionNo(1),
                size_bytes: 0,
                content_ref: ContentRef::blob_v1(
                    loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
                    loonfs_types::ContentId::generate(),
                    b"",
                    loonfs_types::ChecksumAlgorithm::Crc64nvme,
                ),
                revision_committed_by: loonfs_types::ActorId::loonfs(),
                revision_committed_at_ms: 1,
            },
            head_seq: loonfs_types::ChangeSeq(1),
            parent_inode_id: None,
            display_name: None,
            binding_version: None,
            attributes: None,
        }
    }

    async fn open_stream<S: ObjectStore>(
        store: S,
        content_ref: &ContentRef,
    ) -> std::result::Result<FileContentStream<S>, DurableContentValidationError> {
        open_stream_at(store, content_ref, 0).await
    }

    async fn open_stream_at<S: ObjectStore>(
        store: S,
        content_ref: &ContentRef,
        start_offset: u64,
    ) -> std::result::Result<FileContentStream<S>, DurableContentValidationError> {
        open_stream_over(store, content_ref, StreamRange::Whole { start_offset }).await
    }

    async fn open_stream_over<S: ObjectStore>(
        store: S,
        content_ref: &ContentRef,
        range: StreamRange,
    ) -> std::result::Result<FileContentStream<S>, DurableContentValidationError> {
        range
            .ensure_within(content_ref.size_bytes)
            .expect("the test opens a range inside the content");
        FileContentStream::open_inner(
            store,
            super::ContentLocation::whole(content_ref),
            Some(test_entry()),
            content_ref.clone(),
            test_chunk_bytes(),
            range,
        )
        .await
    }

    fn stream_range(start_offset: u64, length: u64) -> StreamRange {
        StreamRange::Range(ReadRange {
            start_offset,
            length: NonZeroU64::new(length),
        })
    }

    #[tokio::test]
    async fn a_ranged_read_fetches_only_its_range_one_chunk_at_a_time() {
        let (_temp_dir, inner) = test_store();
        let store = RecordingStore::new(inner, KeyPredicate::content_blob());
        let bytes = payload(3 * TEST_CHUNK_BYTES + 7);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_ref, &bytes).await;
        let key = content_blob(&content_ref.owner_namespace_id, &content_ref.content_id);
        let start = TEST_CHUNK_BYTES / 2;
        let end = start + TEST_CHUNK_BYTES + 10;

        store.reset();
        let mut stream = open_stream_over(
            &store,
            &content_ref,
            stream_range(start as u64, (end - start) as u64),
        )
        .await
        .expect("open stream");
        assert_eq!(stream.offsets(), start as u64..end as u64);
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            chunks.push(chunk);
        }

        assert_eq!(chunks.concat(), bytes[start..end]);
        assert_eq!(
            store.take_gets(),
            [
                (
                    key.clone(),
                    Some((start as u64, (start + TEST_CHUNK_BYTES) as u64))
                ),
                (key, Some(((start + TEST_CHUNK_BYTES) as u64, end as u64))),
            ],
            "each ranged read stays inside the requested range"
        );
    }

    #[tokio::test]
    async fn a_ranged_read_stops_at_the_end_and_skips_verification() {
        let (_temp_dir, store) = test_store();
        let bytes = payload(TEST_CHUNK_BYTES + 3);
        let expected = content_ref(&bytes);
        // Same ID and length, different bytes. A whole read would fail its
        // checksum check, but a ranged read doesn't check the checksum.
        let mut planted = bytes.clone();
        planted[0] ^= 0xff;
        let planted_ref = ContentRef::blob_v1(
            expected.owner_namespace_id.clone(),
            expected.content_id.clone(),
            &planted,
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        put_content_object(&store, &planted_ref, &planted).await;

        let mut stream = open_stream_over(&store, &expected, stream_range(10, u64::MAX))
            .await
            .expect("open stream");
        assert_eq!(stream.offsets(), 10..bytes.len() as u64);
        let err = stream
            .fold_resumed_prefix(&bytes[..10])
            .expect_err("a ranged read takes no prefix");
        assert_matches!(err, CoreError::PrefixForRangedRead);
        let mut fetched = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            fetched.extend_from_slice(&chunk);
        }
        assert_eq!(fetched, planted[10..]);
        assert!(stream.next_chunk().await.expect("end").is_none());
    }

    #[test]
    fn a_stream_range_outside_the_content_is_refused_before_any_read() {
        assert_matches!(
            stream_range(8, 1).ensure_within(8),
            Err(CoreError::RangeNotSatisfiable {
                start_offset: 8,
                size_bytes: 8
            })
        );
        assert_matches!(
            stream_range(0, 1).ensure_within(0),
            Err(CoreError::RangeNotSatisfiable {
                start_offset: 0,
                size_bytes: 0
            })
        );
        assert_matches!(stream_range(7, 100).ensure_within(8), Ok(()));
        assert_matches!(
            StreamRange::Whole { start_offset: 9 }.ensure_within(8),
            Err(CoreError::ResumeOffsetOutOfRange { .. })
        );
        assert_matches!(
            StreamRange::Whole { start_offset: 8 }.ensure_within(8),
            Ok(())
        );
    }

    #[tokio::test]
    async fn a_streamed_read_returns_the_object_one_chunk_at_a_time() {
        let (_temp_dir, store) = test_store();
        let bytes = payload(3 * TEST_CHUNK_BYTES + 7);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_ref, &bytes).await;

        let mut stream = open_stream(&store, &content_ref)
            .await
            .expect("open stream");
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            chunks.push(chunk);
        }

        assert_eq!(chunks.len(), 4, "three full chunks and the remainder");
        for chunk in &chunks[..3] {
            assert_eq!(chunk.len(), TEST_CHUNK_BYTES);
        }
        assert_eq!(chunks[3].len(), 7);
        assert_eq!(chunks.concat(), bytes, "the object arrives byte-identical");
    }

    #[tokio::test]
    async fn a_finished_stream_repeats_its_verdict() {
        let (_temp_dir, store) = test_store();
        let bytes = payload(TEST_CHUNK_BYTES + 3);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_ref, &bytes).await;

        let mut stream = open_stream(&store, &content_ref)
            .await
            .expect("open stream");
        while stream.next_chunk().await.expect("chunk").is_some() {}
        assert!(stream.next_chunk().await.expect("verified end").is_none());
        assert!(stream.next_chunk().await.expect("verified end").is_none());
    }

    #[tokio::test]
    async fn a_resumed_read_fetches_only_the_rest_and_verifies_all_of_it() {
        let (_temp_dir, inner) = test_store();
        let store = RecordingStore::new(inner, KeyPredicate::content_blob());
        let bytes = payload(3 * TEST_CHUNK_BYTES);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_ref, &bytes).await;

        let held = 2 * TEST_CHUNK_BYTES;
        store.reset();
        let mut stream = open_stream_at(&store, &content_ref, held as u64)
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
            store.count(OperationSelector::Read),
            1,
            "one chunk was left to fetch, so one ranged read happened"
        );
    }

    #[tokio::test]
    async fn a_resumed_read_holds_the_prefix_to_the_same_verdict() {
        let (_temp_dir, inner) = test_store();
        let store = RecordingStore::new(inner, KeyPredicate::content_blob());
        let bytes = payload(2 * TEST_CHUNK_BYTES);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_ref, &bytes).await;
        let held = TEST_CHUNK_BYTES;

        store.reset();
        let mut unfed = open_stream_at(&store, &content_ref, held as u64)
            .await
            .expect("open stream");
        let err = unfed.next_chunk().await.expect_err("prefix still owed");
        assert_matches!(
            err,
            CoreError::ResumePrefixIncomplete {
                start_offset,
                folded: 0
            } if start_offset == held as u64,
            "unexpected error: {err}"
        );
        assert_eq!(
            store.count(OperationSelector::Read),
            0,
            "nothing is fetched until the stream has what it skipped"
        );

        let mut wrong = open_stream_at(&store, &content_ref, held as u64)
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
                // This is the failure this test exists to catch.
                Ok(None) => panic!("a prefix that is not the object's verified"),
                Err(error) => break error,
            }
        };
        assert_matches!(
            verdict,
            CoreError::DurableContent(
                DurableContentValidationError::ContentChecksumMismatch { .. }
            ),
            "a prefix that is not the object's fails the whole read: {verdict}"
        );
    }

    #[tokio::test]
    async fn a_resumed_prefix_may_be_folded_in_pieces() {
        let (_temp_dir, store) = test_store();
        let bytes = payload(2 * TEST_CHUNK_BYTES);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_ref, &bytes).await;
        let held = TEST_CHUNK_BYTES;

        let mut stream = open_stream_at(&store, &content_ref, held as u64)
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
    async fn a_prefix_past_the_start_offset_is_refused() {
        let (_temp_dir, inner) = test_store();
        let store = RecordingStore::new(inner, KeyPredicate::content_blob());
        let bytes = payload(2 * TEST_CHUNK_BYTES);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_ref, &bytes).await;
        let held = TEST_CHUNK_BYTES;

        store.reset();
        let mut stream = open_stream_at(&store, &content_ref, held as u64)
            .await
            .expect("open stream");
        // Shorter than the content, but one byte past where the read resumes.
        let overlong = &bytes[..held + 1];
        let err = stream
            .fold_resumed_prefix(overlong)
            .expect_err("a prefix past the start offset");
        assert_eq!(err.code(), loonfs_types::ErrorCode::InvalidRequest);
        assert_matches!(
            err,
            CoreError::ResumedPrefix(loonfs_types::ResumedPrefixError::PastStartOffset {
                prefix_bytes,
                start_offset,
            }) if prefix_bytes == overlong.len() as u64 && start_offset == held as u64,
            "unexpected error: {err}"
        );
        assert_eq!(
            store.count(OperationSelector::Read),
            0,
            "nothing was fetched"
        );
    }

    #[tokio::test]
    async fn a_prefix_offered_after_a_fetch_is_refused() {
        let (_temp_dir, store) = test_store();
        let bytes = payload(2 * TEST_CHUNK_BYTES);
        let content_ref = content_ref(&bytes);
        put_content_object(&store, &content_ref, &bytes).await;

        let mut stream = open_stream(&store, &content_ref)
            .await
            .expect("open stream");
        stream.next_chunk().await.expect("chunk").expect("a chunk");
        let err = stream
            .fold_resumed_prefix(&bytes[..8])
            .expect_err("a prefix after a fetch");
        assert_matches!(
            err,
            CoreError::ResumedPrefix(loonfs_types::ResumedPrefixError::AfterFetch),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn a_prefix_offered_after_the_end_is_refused() {
        let (_temp_dir, store) = test_store();
        // Empty content reaches its verdict without fetching, which leaves
        // only the finished-stream guard between this fold and the digest.
        let content_ref = content_ref(b"");
        put_content_object(&store, &content_ref, b"").await;

        let mut stream = open_stream(&store, &content_ref)
            .await
            .expect("open stream");
        assert!(stream.next_chunk().await.expect("verified end").is_none());
        let err = stream
            .fold_resumed_prefix(b"")
            .expect_err("a prefix after the end");
        assert_matches!(
            err,
            CoreError::ResumedPrefix(loonfs_types::ResumedPrefixError::AfterEnd),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn a_streamed_read_of_an_empty_object_verifies_without_fetching() {
        let (_temp_dir, inner) = test_store();
        let store = RecordingStore::new(inner, KeyPredicate::content_blob());
        let content_ref = content_ref(b"");
        put_content_object(&store, &content_ref, b"").await;

        store.reset();
        let mut stream = open_stream(&store, &content_ref)
            .await
            .expect("open stream");
        assert!(stream.next_chunk().await.expect("verified end").is_none());
        assert_eq!(
            store.count(OperationSelector::Read),
            0,
            "an empty object needs no ranged read"
        );
    }

    #[tokio::test]
    async fn a_streamed_read_verifies_a_reference_whose_only_evidence_is_a_crc32c() {
        let (_temp_dir, store) = test_store();
        let bytes = payload(2 * TEST_CHUNK_BYTES + 5);
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id: loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            checksum: Checksum::crc32c(&bytes),
        };
        put_content_object(&store, &content_ref, &bytes).await;

        let mut stream = open_stream(&store, &content_ref)
            .await
            .expect("open stream");
        let mut fetched = Vec::new();
        while let Some(chunk) = stream.next_chunk().await.expect("chunk") {
            fetched.extend_from_slice(&chunk);
        }
        assert_eq!(fetched, bytes);

        let (_temp_dir, store) = test_store();
        let planted = ContentRef {
            checksum: Checksum::crc32c(&payload(bytes.len() + 1)),
            ..content_ref
        };
        put_content_object(&store, &planted, &bytes).await;
        let mut stream = open_stream(&store, &planted).await.expect("open stream");
        let verdict = loop {
            match stream.next_chunk().await {
                Ok(Some(_)) => continue,
                // This is the failure this test exists to catch.
                Ok(None) => panic!("an object that is not the reference's verified"),
                Err(error) => break error,
            }
        };
        assert_matches!(
            verdict,
            CoreError::DurableContent(
                DurableContentValidationError::ContentChecksumMismatch { .. }
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
        let (_temp_dir, inner) = test_store();
        let store = RecordingStore::new(inner, KeyPredicate::content_blob());
        let bytes = payload(2 * TEST_CHUNK_BYTES);
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id: loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            checksum: checksum(&bytes),
        };
        put_content_object(&store, &content_ref, &bytes).await;
        let held = TEST_CHUNK_BYTES;

        store.reset();
        let mut stream = open_stream_at(&store, &content_ref, held as u64)
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
            store.count(OperationSelector::Read),
            1,
            "one chunk was left to fetch, so one ranged read happened"
        );

        // The prefix is part of the verdict here too: wrong bytes below the
        // resume point fail the whole read.
        let mut wrong = open_stream_at(&store, &content_ref, held as u64)
            .await
            .expect("open stream");
        wrong
            .fold_resumed_prefix(&vec![0u8; held])
            .expect("a prefix of the right length is accepted");
        let verdict = loop {
            match wrong.next_chunk().await {
                Ok(Some(_)) => continue,
                // This is the failure this test exists to catch.
                Ok(None) => panic!("a prefix that is not the object's verified"),
                Err(error) => break error,
            }
        };
        assert_matches!(
            verdict,
            CoreError::DurableContent(
                DurableContentValidationError::ContentChecksumMismatch { .. }
            ),
            "unexpected verdict: {verdict}"
        );
    }

    #[tokio::test]
    async fn a_streamed_read_rejects_an_object_that_does_not_match_its_reference() {
        let (_temp_dir, store) = test_store();
        let bytes = payload(2 * TEST_CHUNK_BYTES);
        let expected = content_ref(&bytes);
        // Same id and same length, different bytes: only the digest can tell.
        let mut planted = bytes.clone();
        planted[0] ^= 0xff;
        let planted_ref = ContentRef::blob_v1(
            expected.owner_namespace_id.clone(),
            expected.content_id.clone(),
            &planted,
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        put_content_object(&store, &planted_ref, &planted).await;

        let mut stream = open_stream(&store, &expected).await.expect("open stream");
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
        assert_matches!(
            err,
            CoreError::DurableContent(
                DurableContentValidationError::ContentChecksumMismatch { .. }
            )
        );
    }

    #[tokio::test]
    async fn a_streamed_read_reports_a_missing_object_on_its_first_range() {
        let (_temp_dir, store) = test_store();
        let content_ref = content_ref(b"never stored");

        let mut stream = open_stream(&store, &content_ref)
            .await
            .expect("open stream");
        let err = stream.next_chunk().await.expect_err("missing object");
        assert_matches!(
            err,
            CoreError::DurableContent(DurableContentValidationError::MissingContentObject { .. })
        );
    }

    #[tokio::test]
    async fn a_streamed_read_rejects_an_object_of_the_wrong_length() {
        let (_temp_dir, store) = test_store();
        let bytes = payload(TEST_CHUNK_BYTES + 1);
        let mut content_ref = content_ref(&bytes);
        put_content_object(&store, &content_ref, &bytes).await;
        content_ref.size_bytes += 1;

        let mut stream = open_stream(&store, &content_ref)
            .await
            .expect("open stream");
        stream.next_chunk().await.expect("first range");
        let err = stream.next_chunk().await.expect_err("length mismatch");
        assert_matches!(
            err,
            CoreError::DurableContent(DurableContentValidationError::ContentLengthMismatch { .. })
        );
    }
}
