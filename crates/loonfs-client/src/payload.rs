//! Streaming payload sources and multipart chunking.
//!
//! [`PayloadSource`] yields bytes once, in order, without requiring a known
//! length. This supports files, pipes, and sockets while keeping memory use
//! independent of total payload size.

use bytes::{Bytes, BytesMut};
use futures::stream::{BoxStream, StreamExt};
use std::io;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt};

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
        })
    }

    /// Complete length when the source knows it.
    pub fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }

    /// Returns the byte stream and its optional size hint.
    pub fn into_stream(self) -> (PayloadStream, Option<u64>) {
        (self.stream, self.size_bytes)
    }

    /// Wraps the byte stream without changing its size hint.
    pub fn map_stream(self, wrap: impl FnOnce(PayloadStream) -> PayloadStream) -> Self {
        Self {
            stream: wrap(self.stream),
            size_bytes: self.size_bytes,
        }
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

    #[tokio::test]
    async fn parts_are_cut_regardless_of_how_the_source_chunked_them() {
        let mut reader = PartReader::new(source(vec![b"abcde", b"fg", b"hijkl"]), 4);
        assert_eq!(
            cut(&mut reader).await,
            vec![b"abcd".to_vec(), b"efgh".to_vec(), b"ijkl".to_vec()]
        );
    }

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
