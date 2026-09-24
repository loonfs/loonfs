//! Writer acquisition through a manifest epoch and a numbered fence segment.

use crate::checkpoint::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use crate::context::MutationContext;
use crate::error::{CoreError, Result, WriterFence};
use crate::namespace::control::{load_current_manifest, load_namespace_read_state};
use crate::namespace::state::NamespaceReadState;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use crate::wal::{prepare_fence_segment, publish_segment};
use loonfs_api::wire::control::{AcquiredWriter, WriterBlock};
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;

pub(crate) async fn acquire_writer_epoch<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<AcquiredWriter> {
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let acquired = loop {
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
        if matches!(
            publish_manifest(store, manifest, &timer, started_ms).await?,
            ManifestPublicationOutcome::Published(_)
        ) {
            break acquired;
        }
    };
    loop {
        let head = load_namespace_read_state(store, namespace_id).await?;
        ensure_writer_not_fenced(&head, &acquired)?;
        super::control::ensure_namespace_live(&head)?;
        let fence = prepare_fence_segment(namespace_id.clone(), acquired.writer_epoch, &head)
            .map_err(|error| CoreError::Internal(format!("WAL fence build failed: {error}")))?;
        crate::checkpoint::ensure_metadata_publication_budget(&timer, started_ms, namespace_id)?;
        match publish_segment(store, &fence).await {
            Ok(()) => return Ok(acquired),
            Err(CoreError::WalPublish(crate::commit::WalPublishError::StaleHead)) => {}
            Err(error) => return Err(error),
        }
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
        active_writer: head.writer.as_ref().map(|writer| writer.writer_id.clone()),
        active_acquired_at_ms: head.writer.as_ref().map(|writer| writer.acquired_at_ms),
    }))
}
