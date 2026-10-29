//! The requests that a provider adapter signs and sends itself, and the part
//! reader and the upload guard that those requests share.

use crate::object_store::Result;
use crate::{AssemblySource, MultipartPart, ObjectMetadata};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use loonfs_types::Checksum;

/// The requests a provider adapter signs and sends itself: checksum reads,
/// checked creates, conditional multipart writes, and assemblies.
#[async_trait]
pub(crate) trait ProviderSigner: Send + Sync {
    /// Reads `key`'s metadata with its stored checksum, or `None` when the
    /// object is absent.
    async fn head_metadata(&self, key: &str) -> Result<Option<ObjectMetadata>>;

    /// Creates `key` from `bytes` in one request while `key` is absent, and
    /// sends `checksum` for the provider to verify and store.
    async fn create_checked(
        &self,
        key: &str,
        bytes: Bytes,
        checksum: &Checksum,
    ) -> Result<ObjectMetadata>;

    /// Writes `head` and then every part of `rest` to `key` only while `key`
    /// is absent, with at most `part_window` parts in flight. A provider whose
    /// upload takes its parts in order sends them one at a time.
    async fn put_if_absent(
        &self,
        key: &str,
        head: Bytes,
        rest: PartReader<'_>,
        part_window: usize,
    ) -> Result<ObjectMetadata>;

    /// Creates `key` from `sources` followed by `tail`, as
    /// [`crate::ObjectStore::assemble`] describes.
    async fn assemble(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata>;
}

/// The control requests of a client-driven multipart upload, as
/// [`crate::ObjectStore::create_multipart_upload`] and its siblings describe them.
#[async_trait]
pub(crate) trait ClientMultipart: Send + Sync {
    async fn create_multipart_upload(&self, key: &str) -> Result<String>;

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<()>;

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()>;
}

/// Cuts a byte stream into fixed-size parts, holding one at a time.
///
/// Chunk boundaries in the source stream carry no meaning, so a chunk that
/// straddles a part boundary is split and its tail carried into the next
/// part. A stream that ends exactly on a boundary produces no final part.
pub(crate) struct PartReader<'a> {
    body: BoxStream<'a, Result<Bytes>>,
    /// The tail of a chunk that overran the part being cut.
    carry: Option<Bytes>,
    part_bytes: usize,
    exhausted: bool,
}

impl<'a> PartReader<'a> {
    pub(crate) fn new(body: BoxStream<'a, Result<Bytes>>, part_bytes: usize) -> Self {
        Self {
            body,
            carry: None,
            part_bytes,
            exhausted: false,
        }
    }

    /// Cuts the next part: exactly `part_bytes`, or whatever is left when
    /// the stream ends. `None` once nothing is left.
    ///
    /// A full part is returned without polling the stream again, so a
    /// caller cannot conclude from a full part that more is coming — only
    /// a short part proves the stream ended.
    pub(crate) async fn next_part(&mut self) -> Result<Option<Bytes>> {
        let mut buffer = bytes::BytesMut::with_capacity(self.part_bytes);
        while buffer.len() < self.part_bytes {
            let mut chunk = match self.carry.take() {
                Some(chunk) => chunk,
                None if self.exhausted => break,
                None => match self.body.next().await {
                    Some(chunk) => chunk?,
                    None => {
                        self.exhausted = true;
                        break;
                    }
                },
            };
            let take = (self.part_bytes - buffer.len()).min(chunk.len());
            buffer.extend_from_slice(&chunk.split_to(take));
            if !chunk.is_empty() {
                self.carry = Some(chunk);
            }
        }
        Ok((!buffer.is_empty()).then(|| buffer.freeze()))
    }

    /// Whether the stream has already reported its end.
    pub(crate) fn exhausted(&self) -> bool {
        self.exhausted && self.carry.is_none()
    }
}

/// Aborts a provider upload when its write is abandoned.
///
/// A write that finishes, or that aborts explicitly, disarms this guard. A
/// failed or cancelled write that does neither has no cleanup `await` point,
/// so `Drop` starts `abort` on the store's IO runtime, which keeps running
/// when the caller's runtime shuts down and when the drop happens outside any
/// runtime. The abort is best effort: if it fails, or the store's runtime has
/// already shut down, the provider's cleanup of incomplete uploads removes
/// what was sent.
pub(crate) struct AbortUploadOnDrop {
    object_key: String,
    io_runtime: tokio::runtime::Handle,
    abort: Option<BoxFuture<'static, Result<()>>>,
}

impl AbortUploadOnDrop {
    pub(crate) fn new(
        object_key: &str,
        io_runtime: tokio::runtime::Handle,
        abort: BoxFuture<'static, Result<()>>,
    ) -> Self {
        Self {
            object_key: object_key.to_owned(),
            io_runtime,
            abort: Some(abort),
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.abort = None;
    }
}

impl Drop for AbortUploadOnDrop {
    fn drop(&mut self) {
        let Some(abort) = self.abort.take() else {
            return;
        };
        let object_key = std::mem::take(&mut self.object_key);
        self.io_runtime.spawn(async move {
            if abort.await.is_err() {
                tracing::warn!(
                    object_key,
                    operation = "abort_multipart",
                    "failed to abort the upload of an abandoned write",
                );
            }
        });
    }
}
