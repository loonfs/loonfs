//! Writer acquisition through a manifest epoch and a numbered fence WAL object.

use crate::context::MutationContext;
use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result, WriterFence};
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::manifest::publish::ManifestChange;
use crate::manifest::{PublicationReport, PublishedManifest};
use crate::namespace::control::LoadedManifest;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::time::Deadline;
use crate::wal::{prepare_wal_object, publish_wal_object};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::WriterBlock;
use loonfs_types::{WriterEpoch, WriterId};
use std::sync::Arc;

/// Captures the writer identity and fencing epoch a session must retain while publishing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquiredWriter {
    /// Stable writer label copied into the manifest's writer block.
    pub writer_id: WriterId,
    /// Fencing epoch every commit publication from this session must match.
    pub writer_epoch: WriterEpoch,
}

/// A writer epoch that a session or a maintainer acquired: the epoch, its
/// claim manifest, and the read anchor whose tip is the fence.
#[derive(Debug)]
pub(crate) struct WriterAcquisition {
    pub(crate) writer: AcquiredWriter,
    pub(crate) claim: PublishedManifest,
    pub(crate) anchor: NamespaceReadAnchor,
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Acquires the namespace's writer epoch without a writer session, and
    /// commits nothing under it (format spec, "Writer ownership").
    ///
    /// The acquisition publishes a claim manifest with the next epoch and
    /// this engine's writer ID, then writes a fence at the next WAL number.
    /// The fence records the retention floor of the manifest that the
    /// acquisition discovered, so a fold of the fence acknowledges that
    /// floor. A writer session with an older epoch returns `writer_fenced`
    /// to its next publication. The report includes the claim manifest.
    pub async fn acquire_writer_epoch(&self) -> Result<PublicationReport<WriterEpoch>> {
        let acquisition = self
            .namespace
            .acquire_writer(&self.mutation_context())
            .await?;
        Ok(PublicationReport {
            response: acquisition.writer.writer_epoch,
            published: Some(acquisition.claim),
        })
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Acquires the namespace's writer epoch for `context.writer_id`, as a
    /// test that needs a fence without a writer session does.
    #[cfg(test)]
    pub(crate) async fn acquire_writer_epoch(
        &self,
        context: &MutationContext,
    ) -> Result<AcquiredWriter> {
        Ok(self.acquire_writer(context).await?.writer)
    }

    /// Acquires the namespace's writer epoch for `context.writer_id`: publishes
    /// a claim manifest with the next epoch, then writes a fence at the next WAL
    /// number (format spec, "Writer ownership").
    ///
    /// A writer session acquires its epoch here before its first publication,
    /// and a maintainer that acknowledges the retention floor acquires one here
    /// and commits nothing after the fence.
    pub(crate) async fn acquire_writer(
        &self,
        context: &MutationContext,
    ) -> Result<WriterAcquisition> {
        let deadline = Deadline::start(Arc::clone(&self.timer));
        let (acquired, manifest, hint) = self
            .update_manifest(
                None,
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
        let hint = hint.expect("a claim starts from no basis, so it loads the hint");
        let claim = PublishedManifest::new(&manifest.state.envelope, Some(deadline.origin()));
        let mut tip = deadline.observe();
        let mut anchor = self
            .load_read_anchor_from_manifest(manifest, hint, deadline.origin())
            .await?;
        // Only the deadline bounds this loop, because a lost fence race means the
        // old writer wrote a WAL object, and stopping after a count would make the
        // caller publish another epoch.
        loop {
            let head = &anchor.read_state;
            ensure_writer_not_fenced(head, &acquired)?;
            super::control::ensure_namespace_live(head)?;
            let fence = prepare_wal_object(
                self.namespace_id.clone(),
                acquired.writer_epoch,
                head,
                anchor.retention_floor_seq(),
                &[],
            )?;
            match publish_wal_object(&self.store, &fence, &tip).await {
                Ok(()) => {
                    anchor.read_state = head.after_wal_object(fence.envelope().payload());
                    anchor.tail.push_published(fence.envelope().clone());
                    return Ok(WriterAcquisition {
                        writer: acquired,
                        claim,
                        anchor,
                    });
                }
                Err(CoreError::WalPublish(
                    crate::wal::WalPublishError::NumberTaken
                    | crate::wal::WalPublishError::PublishBudgetExceeded { .. },
                )) => {}
                Err(error) => return Err(error),
            }
            deadline.ensure_metadata_publication_budget(&self.namespace_id)?;
            tip = deadline.observe();
            anchor = self.load_read_anchor(deadline.observe()).await?;
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
        active_writer: head.writer.clone(),
    }))
}
