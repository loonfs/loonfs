//! Embedded content writes, where this process writes a content object itself
//! and returns the proof that a commit of the namespace admits.

use crate::engine::RuntimeReadContext;
use crate::engine::{NamespaceEngine, Writable};
use crate::error::Result;
use crate::limits::OWNED_STAGING_ADMISSION_WINDOW_MS;
use crate::storage::content_admission::PreparedContent;
use crate::storage::content_staging::{
    stage_bytes_under_content_id, stage_streamed_under_content_id, ContentKeyOwner,
};
use loonfs_objectstore::{ByteStream, ObjectStore};
use loonfs_types::{ContentId, ContentRef, NamespaceId};

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Writes in-process bytes as a content object under a fresh content ID,
    /// and returns a proof that expires [`OWNED_STAGING_ADMISSION_WINDOW_MS`]
    /// after the write started.
    ///
    /// The write sends one put and writes no record. Until a commit
    /// references the object, pool collection keeps the object only while
    /// it's younger than the grace window.
    pub async fn stage_owned_bytes(
        &self,
        context: &RuntimeReadContext,
        bytes: &[u8],
    ) -> Result<PreparedContent> {
        let _permit = self.namespace.content_writes.acquire().await;
        let head = self.namespace.context_read_basis(context).head();
        let started_at_ms = self.namespace.start_clock().request_at_ms();
        let stored = stage_bytes_under_content_id(
            &self.namespace.store,
            &head.pool_id,
            ContentId::generate(),
            bytes,
            ContentKeyOwner::FreshIdentity,
        )
        .await?;
        Ok(owned_content(
            &head.namespace_id,
            stored.content_ref,
            started_at_ms,
        ))
    }

    /// Writes a streamed payload as a content object, hashing it on the way
    /// through instead of keeping it in memory.
    ///
    /// The streaming form of [`Self::stage_owned_bytes`], with the same proof
    /// expiry.
    pub async fn stage_owned_stream(
        &self,
        context: &RuntimeReadContext,
        body: ByteStream,
    ) -> Result<PreparedContent> {
        let _permit = self.namespace.content_writes.acquire().await;
        let head = self.namespace.context_read_basis(context).head();
        let started_at_ms = self.namespace.start_clock().request_at_ms();
        let content_ref = stage_streamed_under_content_id(
            &self.namespace.store,
            &head.pool_id,
            ContentId::generate(),
            body,
            ContentKeyOwner::FreshIdentity,
        )
        .await?;
        Ok(owned_content(
            &head.namespace_id,
            content_ref,
            started_at_ms,
        ))
    }
}

/// Returns the proof for an object of `namespace_id` that a write which
/// started at `started_at_ms` stored.
///
/// The start is read before the put is sent, so the provider stamps the
/// object no earlier than the moment the write started. A commit checks the
/// expiry by the
/// request clock plus elapsed monotonic time immediately before its WAL put.
fn owned_content(
    namespace_id: &NamespaceId,
    content_ref: ContentRef,
    started_at_ms: u64,
) -> PreparedContent {
    PreparedContent::for_staged_content(
        namespace_id.clone(),
        content_ref,
        started_at_ms.saturating_add(OWNED_STAGING_ADMISSION_WINDOW_MS),
    )
}
