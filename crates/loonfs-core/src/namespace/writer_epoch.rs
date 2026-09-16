//! Writer acquisition through a manifest epoch and a numbered fence segment.

use crate::checkpoint::publish::{
    encode_manifest, publish_manifest_against, ManifestPublicationOutcome,
};
use crate::context::MutationContext;
use crate::error::{CoreError, Result, WriterFence};
use crate::namespace::control::{load_current_manifest, LoadedManifest};
use crate::namespace::read_anchor::{
    load_read_anchor, load_read_anchor_from_manifest, LoadedNamespaceBasis,
};
use crate::namespace::state::NamespaceReadState;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use crate::wal::{prepare_fence_segment, publish_segment, resulting_head_after};
use loonfs_api::wire::control::{AcquiredWriter, WriterBlock};
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;

pub(crate) async fn acquire_writer_epoch_with_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<(AcquiredWriter, LoadedNamespaceBasis)> {
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let (acquired, published_manifest) = loop {
        let current = load_current_manifest(store, namespace_id).await?;
        let mut payload = current.envelope.payload().clone();
        super::control::ensure_namespace_live(&NamespaceReadState::from(&payload))?;
        payload.manifest_no = payload
            .manifest_no
            .successor()
            .map_err(|error| CoreError::Internal(format!("manifest number {error}")))?;
        payload.writer_epoch = payload
            .writer_epoch
            .successor()
            .map_err(|error| CoreError::Internal(format!("writer epoch {error}")))?;
        payload.writer = Some(WriterBlock {
            writer_id: context.writer_id.clone(),
            acquired_at_ms: context.now_ms,
        });
        let acquired = AcquiredWriter {
            writer_id: context.writer_id.clone(),
            writer_epoch: payload.writer_epoch,
        };
        let manifest = encode_manifest(payload)?;
        let mut published = LoadedManifest::from_envelope(manifest.envelope().clone());
        // Preserve the previously observed hint as a lower bound. Discovery
        // still validates its WAL chain and rechecks the manifest successor.
        published.discovery_start_manifest_no = current.discovery_start_manifest_no;
        published.hinted_wal_no = current.hinted_wal_no;
        if matches!(
            publish_manifest_against(
                store,
                namespace_id,
                manifest,
                Some(current.state.manifest.manifest_no),
                Some(current),
                &timer,
                started_ms
            )
            .await?,
            ManifestPublicationOutcome::Published(_)
        ) {
            break (acquired, published);
        }
    };
    let mut anchor =
        load_read_anchor_from_manifest(store, namespace_id, published_manifest).await?;
    loop {
        let mut basis = anchor.into_loaded_basis();
        let head = &basis.head;
        ensure_writer_not_fenced(head, &acquired)?;
        super::control::ensure_namespace_live(head)?;
        let fence = prepare_fence_segment(namespace_id.clone(), acquired.writer_epoch, head)
            .map_err(|error| CoreError::Internal(format!("WAL fence build failed: {error}")))?;
        crate::checkpoint::ensure_metadata_publication_budget(&timer, started_ms, namespace_id)?;
        match publish_segment(store, &fence).await {
            Ok(()) => {
                // The fence adds no rows. Carry the already validated anchor
                // into this session's first publication, advancing only its WAL
                // position. A later competing fence still collides at the next
                // immutable WAL number; retries rediscover normally.
                basis.head = resulting_head_after(&fence, head, head.head_commit_id.clone());
                return Ok((acquired, basis));
            }
            Err(CoreError::WalPublish(crate::commit::WalPublishError::StaleHead)) => {
                anchor = load_read_anchor(store, namespace_id).await?;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
pub(crate) async fn acquire_writer_epoch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AcquiredWriter> {
    acquire_writer_epoch_with_basis(store, namespace_id, context)
        .await
        .map(|(writer, _)| writer)
}

pub(crate) fn ensure_writer_not_fenced(
    head: &NamespaceReadState,
    acquired_writer: &AcquiredWriter,
) -> Result<()> {
    if head.writer_epoch == acquired_writer.writer_epoch {
        return Ok(());
    }
    Err(CoreError::WriterFenced(WriterFence {
        fenced_epoch: acquired_writer.writer_epoch,
        active_epoch: head.writer_epoch,
        active_writer: head.writer.as_ref().map(|writer| writer.writer_id.clone()),
        active_acquired_at_ms: head.writer.as_ref().map(|writer| writer.acquired_at_ms),
    }))
}
