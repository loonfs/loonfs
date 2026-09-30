//! Writer acquisition through a manifest epoch and a numbered fence segment.

use crate::context::MutationContext;
use crate::error::{CoreError, Result, WriterFence};
use crate::manifest::publish::{update_manifest, ManifestChange};
use crate::namespace::read_anchor::{
    load_read_anchor, load_read_anchor_from_manifest, NamespaceReadAnchor,
};
use crate::namespace::state::NamespaceReadState;
use crate::time::{Deadline, MonotonicTimer};
use crate::wal::{prepare_segment, publish_segment};
use loonfs_api::wire::control::{AcquiredWriter, WriterBlock};
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

#[cfg(test)]
pub(crate) async fn acquire_writer_epoch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AcquiredWriter> {
    let timer = Arc::new(crate::time::StdMonotonicTimer::default());
    Ok(acquire_writer(store, namespace_id, context, timer).await?.0)
}

pub(crate) async fn acquire_writer<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    timer: Arc<dyn MonotonicTimer>,
) -> Result<(AcquiredWriter, NamespaceReadAnchor)> {
    let deadline = Deadline::start(timer);
    let (acquired, manifest, hint) =
        update_manifest(store, namespace_id, &deadline, |mut payload| async move {
            super::control::ensure_namespace_live(&NamespaceReadState::from(&payload))?;
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
            Ok(ManifestChange::Next(Box::new(payload), acquired))
        })
        .await?;
    let mut tip = deadline.observe();
    let mut anchor =
        load_read_anchor_from_manifest(store, namespace_id, manifest, hint, deadline.origin())
            .await?;
    loop {
        let head = &anchor.read_state;
        ensure_writer_not_fenced(head, &acquired)?;
        super::control::ensure_namespace_live(head)?;
        let fence = prepare_segment(namespace_id.clone(), acquired.writer_epoch, head, &[])
            .map_err(|error| CoreError::Internal(format!("WAL fence build failed: {error}")))?;
        match publish_segment(store, &fence, &tip).await {
            Ok(()) => {
                anchor.read_state = head.after_segment(fence.envelope().payload());
                anchor.tail.push_published(fence.envelope().clone());
                return Ok((acquired, anchor));
            }
            Err(CoreError::WalPublish(
                crate::commit::WalPublishError::NumberTaken
                | crate::commit::WalPublishError::PublishBudgetExceeded { .. },
            )) => {}
            Err(error) => return Err(error),
        }
        deadline.ensure_metadata_publication_budget(namespace_id)?;
        tip = deadline.observe();
        anchor = load_read_anchor(store, namespace_id).await?;
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
