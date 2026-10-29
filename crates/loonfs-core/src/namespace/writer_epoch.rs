//! Writer acquisition through a manifest epoch and a numbered fence WAL object.

use crate::context::MutationContext;
use crate::error::{CoreError, Result, WriterFence};
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::manifest::publish::{update_manifest, ManifestChange};
use crate::namespace::control::LoadedManifest;
use crate::namespace::read_anchor::{
    load_read_anchor, load_read_anchor_from_manifest, NamespaceReadAnchor,
};
use crate::namespace::state::NamespaceReadState;
use crate::time::{Deadline, MonotonicTimer};
use crate::wal::{prepare_wal_object, publish_wal_object};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::WriterBlock;
use loonfs_types::{ManifestNo, NamespaceId, WriterEpoch, WriterId};
use std::sync::Arc;

/// Captures the writer identity and fencing epoch a session must retain while publishing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquiredWriter {
    /// Stable writer label copied into the manifest's writer block.
    pub writer_id: WriterId,
    /// Fencing epoch every commit publication from this session must match.
    pub writer_epoch: WriterEpoch,
}

/// A writer epoch that a session acquired: the epoch, the number of its
/// claim manifest, and the read anchor whose tip is the session's fence.
#[derive(Debug)]
pub(crate) struct WriterAcquisition {
    pub(crate) writer: AcquiredWriter,
    pub(crate) claim_manifest_no: ManifestNo,
    pub(crate) anchor: NamespaceReadAnchor,
}

#[cfg(test)]
pub(crate) async fn acquire_writer_epoch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AcquiredWriter> {
    let timer = Arc::new(crate::time::StdMonotonicTimer::default());
    Ok(acquire_writer(store, namespace_id, context, timer)
        .await?
        .writer)
}

pub(crate) async fn acquire_writer<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    timer: Arc<dyn MonotonicTimer>,
) -> Result<WriterAcquisition> {
    let deadline = Deadline::start(timer);
    let (acquired, manifest, hint) = update_manifest(
        store,
        namespace_id,
        &deadline,
        MAX_CONTENTION_ATTEMPTS,
        |current: LoadedManifest| async move {
            let mut payload = current.state.envelope.payload().clone();
            super::control::ensure_namespace_live(&NamespaceReadState::from(&payload))?;
            payload.writer_epoch = payload.writer_epoch.successor().map_err(|_| {
                CoreError::NamespaceCorrupt(format!(
                    "manifest {} records writer epoch {}, the largest writer epoch, so no \
                     writer can acquire the namespace",
                    payload.manifest_no, payload.writer_epoch
                ))
            })?;
            payload.writer = Some(WriterBlock {
                writer_id: context.writer_id.clone(),
                acquired_at_ms: context.now_ms,
            });
            let acquired = AcquiredWriter {
                writer_id: context.writer_id.clone(),
                writer_epoch: payload.writer_epoch,
            };
            Ok(ManifestChange::Next(Box::new(payload), acquired))
        },
    )
    .await?;
    let claim_manifest_no = manifest.state.manifest().manifest_no;
    let mut tip = deadline.observe();
    let mut anchor =
        load_read_anchor_from_manifest(store, namespace_id, manifest, hint, deadline.origin())
            .await?;
    // Only the deadline bounds this loop, because a lost fence race means the
    // old writer wrote a WAL object, and stopping after a count would make the
    // caller publish another epoch.
    loop {
        let head = &anchor.read_state;
        ensure_writer_not_fenced(head, &acquired)?;
        super::control::ensure_namespace_live(head)?;
        let fence = prepare_wal_object(
            namespace_id.clone(),
            acquired.writer_epoch,
            head,
            anchor.retention_floor_seq(),
            &[],
        )?;
        match publish_wal_object(store, &fence, &tip).await {
            Ok(()) => {
                anchor.read_state = head.after_wal_object(fence.envelope().payload());
                anchor.tail.push_published(fence.envelope().clone());
                return Ok(WriterAcquisition {
                    writer: acquired,
                    claim_manifest_no,
                    anchor,
                });
            }
            Err(CoreError::WalPublish(
                crate::wal::WalPublishError::NumberTaken
                | crate::wal::WalPublishError::PublishBudgetExceeded { .. },
            )) => {}
            Err(error) => return Err(error),
        }
        deadline.ensure_metadata_publication_budget(namespace_id)?;
        tip = deadline.observe();
        anchor = load_read_anchor(store, namespace_id, deadline.observe()).await?;
    }
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
        active_writer_id: head.writer.as_ref().map(|writer| writer.writer_id.clone()),
        active_acquired_at_ms: head.writer.as_ref().map(|writer| writer.acquired_at_ms),
    }))
}
