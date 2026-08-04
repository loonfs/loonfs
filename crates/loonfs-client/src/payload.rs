//! Where a streaming put reads its bytes, and how it cuts them into parts.
//!
//! A [`PayloadSource`] is read exactly once, forward, in pieces. That is the
//! whole contract: it is what lets one put serve a file on disk and a pipe
//! with no length equally well, and what keeps the memory a large put costs
//! independent of how large it is.

use bytes::{Bytes, BytesMut};
use futures::stream::{BoxStream, StreamExt};
use loonfs_api::StreamingChecksum;
use std::io;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt as _};

/// Bytes read from a source in one go before they are handed on.
///
/// Sized like an HTTP body's chunk rather than a transfer part: this is the
/// slack a bounded uploader carries on top of the parts it holds, so it
/// should stay small next to a part.
pub(crate) const SOURCE_CHUNK_BYTES: usize = 64 * 1024;

/// A payload delivered in pieces, for a put that must not hold it whole.
///
/// Chunk boundaries carry no meaning — an uploader regroups them into
/// whatever units its transport wants. A chunk error ends the upload.
pub type PayloadStream = BoxStream<'static, io::Result<Bytes>>;

/// Where one put reads its payload, and what it knows about it up front.
///
/// The length is a hint and never a promise: a source that knows it lets the
/// put pick the cheaper transport and declare a `Content-Length`, and a
/// source that does not know it — a pipe, a socket, standard input — takes
/// exactly the same path and discovers the length as it goes.
pub struct PayloadSource {
    stream: PayloadStream,
    size_bytes: Option<u64>,
    /// A file these same bytes can be read from again, when this source was
    /// opened from one. A transport that has to state the payload's digest
    /// before it may write reads twice, and re-opening a file costs nothing;
    /// every other source pays for its second pass with a spool.
    rewind: Option<PathBuf>,
}

impl std::fmt::Debug for PayloadSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadSource")
            .field("size_bytes", &self.size_bytes)
            .finish_non_exhaustive()
    }
}

impl PayloadSource {
    /// A source of unknown length.
    pub fn stream(stream: PayloadStream) -> Self {
        Self {
            stream,
            size_bytes: None,
            rewind: None,
        }
    }

    /// A source whose complete length the caller already knows.
    ///
    /// The length is used to choose a transport and to frame the request; a
    /// source that delivers a different number of bytes is still uploaded
    /// faithfully, and it is the bytes that decide what is stored.
    pub fn sized_stream(stream: PayloadStream, size_bytes: u64) -> Self {
        Self {
            stream,
            size_bytes: Some(size_bytes),
            rewind: None,
        }
    }

    /// A source of unknown length reading from anything asynchronous —
    /// standard input, a socket, a decompressor.
    pub fn reader<R>(reader: R) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        Self::stream(read_in_chunks(reader))
    }

    /// A source reading one local file, whose length is known before the
    /// first byte moves.
    pub async fn open_file(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = tokio::fs::File::open(path.as_ref()).await?;
        let size_bytes = file.metadata().await?.len();
        Ok(Self {
            stream: read_in_chunks(file),
            size_bytes: Some(size_bytes),
            rewind: Some(path.as_ref().to_path_buf()),
        })
    }

    /// Complete length when the source knows it.
    pub fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }

    /// Splits the source into the stream to read and the length it
    /// declared, for a caller that has to restate it in another crate's
    /// terms.
    pub fn into_stream(self) -> (PayloadStream, Option<u64>) {
        (self.stream, self.size_bytes)
    }

    /// Wraps the stream while keeping everything the source knows about
    /// itself.
    ///
    /// A wrapper that only watches bytes go past — counting them for a
    /// progress bar, say — has not changed which payload this is or where it
    /// came from. Rebuilding the source instead would throw that away, and
    /// with it the free second pass a file-backed payload gets: an upload
    /// that has to read the bytes twice would copy them to a spool rather
    /// than re-open a file that was there all along.
    pub fn map_stream(self, wrap: impl FnOnce(PayloadStream) -> PayloadStream) -> Self {
        Self {
            stream: wrap(self.stream),
            size_bytes: self.size_bytes,
            rewind: self.rewind,
        }
    }

    /// Reads the source to its end, folding `digest` and counting bytes, and
    /// hands back a payload that can be read again.
    ///
    /// This is the price of a transport that states the payload's digest
    /// before it may write: the digest must be complete before the first
    /// body byte, and one forward pass cannot do both. What it must not cost
    /// is memory. A source opened from a file is not copied at all — the
    /// second pass re-opens it. Any other source is spooled to a temporary
    /// file as it is read, chunk by chunk, and the spool is deleted when the
    /// result is dropped. Nothing here ever holds the payload whole.
    ///
    /// The length it returns is measured, not the hint the source declared:
    /// a source that delivers a different number of bytes is measured as it
    /// actually is, and that is what may be claimed and what may refuse.
    pub(crate) async fn measure(
        self,
        digest: &mut StreamingChecksum,
    ) -> io::Result<MeasuredPayload> {
        let Self {
            mut stream, rewind, ..
        } = self;
        let mut size_bytes = 0u64;
        match rewind {
            // The bytes are already on disk under a name that outlives this
            // upload, so the pass only has to read and fold them.
            Some(path) => {
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    size_bytes += chunk.len() as u64;
                    digest.update(&chunk);
                }
                Ok(MeasuredPayload {
                    path,
                    spool: None,
                    size_bytes,
                })
            }
            None => {
                let spool = tempfile::NamedTempFile::new()?;
                let path = spool.path().to_path_buf();
                let mut writer = tokio::fs::File::create(&path).await?;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    size_bytes += chunk.len() as u64;
                    digest.update(&chunk);
                    writer.write_all(&chunk).await?;
                }
                writer.flush().await?;
                Ok(MeasuredPayload {
                    path,
                    spool: Some(spool),
                    size_bytes,
                })
            }
        }
    }
}

/// A payload that has been read once and can be read again, plus what that
/// first pass measured.
///
/// Dropping this deletes the spool, if one was written, so it must outlive
/// every read taken from it.
pub(crate) struct MeasuredPayload {
    path: PathBuf,
    /// Holds the spool's name for as long as this value lives. `None` when
    /// the bytes were already a file the caller named.
    ///
    /// Never read: it is held for its `Drop`, which is what deletes the
    /// spool once the upload that reads it is done.
    #[allow(dead_code, reason = "owns the spool's lifetime, not its contents")]
    spool: Option<tempfile::NamedTempFile>,
    size_bytes: u64,
}

impl MeasuredPayload {
    /// The payload's measured length.
    pub(crate) fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Opens a fresh forward read of the measured bytes, in pieces.
    ///
    /// A spool cannot change under this, and a caller's own file that does
    /// is caught rather than stored: the digest the first pass measured is
    /// what the write was signed against, so a provider refuses bytes that
    /// no longer hash to it.
    pub(crate) async fn reread(&self) -> io::Result<PayloadSource> {
        PayloadSource::open_file(&self.path).await
    }
}

/// Reads an asynchronous source into modest chunks until it ends.
fn read_in_chunks<R>(reader: R) -> PayloadStream
where
    R: AsyncRead + Send + Unpin + 'static,
{
    futures::stream::unfold(Some(reader), |reader| async move {
        let mut reader = reader?;
        let mut buffer = BytesMut::with_capacity(SOURCE_CHUNK_BYTES);
        match reader.read_buf(&mut buffer).await {
            // A read of zero is the end of the source, and the only thing
            // that ends it: a short read is just a short read.
            Ok(0) => None,
            Ok(_) => Some((Ok(buffer.freeze()), Some(reader))),
            Err(error) => Some((Err(error), None)),
        }
    })
    .boxed()
}

/// Cuts a source into fixed-size parts, holding one at a time.
///
/// Chunk boundaries in the source carry no meaning, so a chunk that overruns
/// the part being cut is split and its tail carried into the next one. The
/// last part is whatever is left when the source ends, and a source that
/// ends exactly on a boundary produces no final part.
pub(crate) struct PartReader {
    stream: PayloadStream,
    /// The tail of a chunk that overran the part being cut.
    carry: Option<Bytes>,
    part_bytes: usize,
    exhausted: bool,
}

impl PartReader {
    pub(crate) fn new(stream: PayloadStream, part_bytes: usize) -> Self {
        Self {
            stream,
            carry: None,
            part_bytes: part_bytes.max(1),
            exhausted: false,
        }
    }

    /// Cuts the next part: exactly `part_bytes`, or whatever is left when
    /// the source ends. `None` once nothing is left.
    ///
    /// A full part is returned without reading further, so a caller cannot
    /// conclude from a full part that more is coming — only a short part
    /// proves the source ended.
    pub(crate) async fn next_part(&mut self) -> io::Result<Option<Bytes>> {
        let mut buffer: Option<BytesMut> = None;
        loop {
            let filled = buffer.as_ref().map_or(0, BytesMut::len);
            if filled >= self.part_bytes {
                break;
            }
            let mut chunk = match self.carry.take() {
                Some(chunk) => chunk,
                None if self.exhausted => break,
                None => match self.stream.next().await {
                    Some(chunk) => chunk?,
                    None => {
                        self.exhausted = true;
                        break;
                    }
                },
            };
            let take = (self.part_bytes - filled).min(chunk.len());
            let taken = chunk.split_to(take);
            if !chunk.is_empty() {
                self.carry = Some(chunk);
            }
            match &mut buffer {
                Some(buffer) => buffer.extend_from_slice(&taken),
                // A chunk that fills a part on its own is handed straight
                // through. Nothing is copied, and the part is a view of the
                // source's own buffer rather than a second copy of it.
                None if taken.len() == self.part_bytes => return Ok(Some(taken)),
                None => {
                    let mut fresh = BytesMut::with_capacity(self.part_bytes);
                    fresh.extend_from_slice(&taken);
                    buffer = Some(fresh);
                }
            }
        }
        Ok(buffer
            .filter(|buffer| !buffer.is_empty())
            .map(BytesMut::freeze))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loonfs_api::StorageChecksum;

    fn source(chunks: Vec<&'static [u8]>) -> PayloadStream {
        futures::stream::iter(chunks.into_iter().map(|chunk| Ok(Bytes::from(chunk)))).boxed()
    }

    async fn cut(reader: &mut PartReader) -> Vec<Vec<u8>> {
        let mut parts = Vec::new();
        while let Some(part) = reader.next_part().await.expect("cut a part") {
            parts.push(part.to_vec());
        }
        parts
    }

    /// Source chunks and part boundaries are unrelated: a chunk that
    /// straddles a boundary is split, and its tail opens the next part.
    #[tokio::test]
    async fn parts_are_cut_regardless_of_how_the_source_chunked_them() {
        let mut reader = PartReader::new(source(vec![b"abcde", b"fg", b"hijkl"]), 4);
        assert_eq!(
            cut(&mut reader).await,
            vec![b"abcd".to_vec(), b"efgh".to_vec(), b"ijkl".to_vec()]
        );
    }

    /// A source that ends exactly on a boundary produces no trailing empty
    /// part, and one that does not ends with a short one.
    #[tokio::test]
    async fn the_last_part_is_whatever_is_left() {
        let mut reader = PartReader::new(source(vec![b"abcdef"]), 4);
        assert_eq!(
            cut(&mut reader).await,
            vec![b"abcd".to_vec(), b"ef".to_vec()]
        );

        let mut reader = PartReader::new(source(vec![b"abcd"]), 4);
        assert_eq!(cut(&mut reader).await, vec![b"abcd".to_vec()]);
    }

    /// An empty source produces no parts at all, which is what tells a
    /// one-pass uploader that it has nothing to assemble.
    #[tokio::test]
    async fn an_empty_source_produces_no_parts() {
        let mut reader = PartReader::new(source(vec![]), 4);
        assert!(reader.next_part().await.expect("cut a part").is_none());
    }

    #[tokio::test]
    async fn a_file_source_knows_its_length() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("payload.bin");
        std::fs::write(&path, vec![7u8; 5_000]).expect("write payload");

        let source = PayloadSource::open_file(&path).await.expect("open payload");
        assert_eq!(source.size_bytes(), Some(5_000));

        let (stream, _) = source.into_stream();
        let mut reader = PartReader::new(stream, 4_096);
        let first = reader.next_part().await.expect("cut").expect("first part");
        let second = reader.next_part().await.expect("cut").expect("second part");
        assert_eq!(first.len(), 4_096);
        assert_eq!(second.len(), 904);
        assert!(reader.next_part().await.expect("cut").is_none());
    }

    /// The measuring pass counts and folds what it actually read, and a
    /// file-backed payload is not copied to do it: the second pass re-opens
    /// the caller's own file.
    ///
    /// Wrapping the stream must not cost that. The CLI wraps every source it
    /// opens to count progress, so losing the rewind here would copy every
    /// uploaded file to a spool for nothing.
    #[tokio::test]
    async fn a_wrapped_file_source_is_re_read_rather_than_spooled() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("payload.bin");
        let payload = vec![3u8; 5_000];
        std::fs::write(&path, &payload).expect("write payload");

        let source = PayloadSource::open_file(&path)
            .await
            .expect("open payload")
            .map_stream(|stream| stream.boxed());

        let mut digest = sha256();
        let measured = source.measure(&mut digest).await.expect("measure payload");

        assert_eq!(measured.size_bytes(), 5_000);
        assert!(
            measured.spool.is_none(),
            "a file-backed payload must not be copied to be read twice"
        );
        assert_eq!(digest.finish(), StorageChecksum::sha256(&payload));
    }

    /// A source that cannot be read again is spooled so the second pass has
    /// something to read, and the spool goes away with the payload.
    #[tokio::test]
    async fn a_pipe_source_is_spooled_and_the_spool_is_reclaimed() {
        let payload = vec![9u8; 5_000];
        let source = PayloadSource::reader(std::io::Cursor::new(payload.clone()));

        let mut digest = sha256();
        let measured = source.measure(&mut digest).await.expect("measure payload");

        assert_eq!(measured.size_bytes(), 5_000);
        assert_eq!(digest.finish(), StorageChecksum::sha256(&payload));
        let spool_path = measured.path.clone();
        assert!(spool_path.exists(), "the second pass has nothing to read");

        let mut reread = Vec::new();
        let (mut stream, size_bytes) = measured.reread().await.expect("re-read").into_stream();
        while let Some(chunk) = stream.next().await {
            reread.extend_from_slice(&chunk.expect("read a chunk"));
        }
        assert_eq!(size_bytes, Some(5_000));
        assert_eq!(reread, payload);

        drop(stream);
        drop(measured);
        assert!(!spool_path.exists(), "the spool outlived the upload");
    }

    fn sha256() -> StreamingChecksum {
        StreamingChecksum::for_algorithm(loonfs_api::ChecksumAlgorithm::Sha256)
    }

    /// A reader-backed source declares no length, which is exactly the
    /// case a pipe presents.
    #[tokio::test]
    async fn a_reader_source_declares_no_length() {
        let source = PayloadSource::reader(std::io::Cursor::new(vec![1u8; 100]));
        assert_eq!(source.size_bytes(), None);
        let (stream, _) = source.into_stream();
        let mut reader = PartReader::new(stream, 64);
        assert_eq!(
            reader.next_part().await.expect("cut").expect("part").len(),
            64
        );
        assert_eq!(
            reader.next_part().await.expect("cut").expect("part").len(),
            36
        );
        assert!(reader.next_part().await.expect("cut").is_none());
    }
}
