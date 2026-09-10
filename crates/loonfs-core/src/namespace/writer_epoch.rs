//! Writer acquisition through a manifest epoch and a numbered fence segment.

use crate::checkpoint::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use crate::context::MutationContext;
use crate::error::{CoreError, Result, WriterFence};
use crate::namespace::control::{load_current_manifest, load_namespace_read_state};
use crate::namespace::state::NamespaceReadState;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use loonfs_api::wire::control::{AcquiredWriter, WriterBlock};
use loonfs_api::wire::wal::{encode_wal_segment_envelope_zstd, WalSegmentPayload};
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
            publish_manifest(
                store,
                namespace_id,
                &manifest,
                Some(current.state.manifest.manifest_no),
                &timer,
                started_ms
            )
            .await?,
            ManifestPublicationOutcome::Published(_)
        ) {
            break acquired;
        }
    };
    loop {
        let head = load_namespace_read_state(store, namespace_id).await?;
        ensure_writer_not_fenced(&head, &acquired)?;
        super::control::ensure_namespace_live(&head)?;
        let wal_no = head
            .wal_no
            .successor()
            .map_err(|error| CoreError::Internal(format!("WAL number {error}")))?;
        let fence = encode_wal_segment_envelope_zstd(WalSegmentPayload {
            namespace_id: namespace_id.clone(),
            wal_no,
            writer_epoch: acquired.writer_epoch,
            next_inode_id: head.next_inode_id,
            base_head_seq: head.seq,
            start_seq: head.seq,
            end_seq: head.seq,
            records: Vec::new(),
        })
        .map_err(|error| CoreError::Codec {
            object_key: loonfs_objectstore::keys::wal_segment(namespace_id, &wal_no),
            message: error.to_string(),
        })?;
        crate::checkpoint::ensure_metadata_publication_budget(&timer, started_ms, namespace_id)?;
        match crate::commit::publish_wal(store, &fence).await {
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
