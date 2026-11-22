//! Writer acquisition through a manifest epoch and a numbered fence WAL object.

use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result, WriterFence};
use crate::manifest::publish::ManifestChange;
use crate::manifest::{PublicationReport, PublishedManifest};
use crate::namespace::basis::PublicationBasis;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use crate::wal::{prepare_fence, publish_wal_object, wal_object_is_live};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::WriterBlock;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{WriterEpoch, WriterId};

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
    /// A writer session with an older epoch returns `writer_fenced` to its
    /// next publication. The report includes the claim manifest.
    pub async fn acquire_writer_epoch(&self) -> Result<PublicationReport<WriterEpoch>> {
        let acquisition = self.namespace.acquire_writer(self.writer_id()).await?;
        Ok(PublicationReport {
            response: acquisition.writer.writer_epoch,
            published: Some(acquisition.claim),
        })
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Acquires the namespace's writer epoch for `writer_id`: publishes
    /// a claim manifest with the next epoch, then writes a fence at the next WAL
    /// number (format spec, "Writer ownership").
    ///
    /// After the fence put succeeds, the call checks whether the fence is
    /// live (see [`wal_object_is_live`]), and puts a dead fence again at the
    /// new tip. A newest manifest with a higher writer epoch returns
    /// `writer_fenced`.
    ///
    /// A writer session acquires its epoch here before its first publication.
    /// The claim builds on the manifest of the namespace's cached head when no
    /// writer has claimed the namespace at that head (see
    /// [`Self::unclaimed_head_basis`]), and otherwise finds the newest
    /// manifest first.
    pub(crate) async fn acquire_writer(&self, writer_id: &WriterId) -> Result<WriterAcquisition> {
        let clock = self.start_clock();
        let acquired_at_ms = clock.request_at_ms();
        let basis = self.unclaimed_head_basis().await?;
        let update = self
            .update_live_manifest(
                basis,
                &clock,
                |mut payload: NamespaceManifestPayload| async move {
                    payload.writer_epoch = payload.writer_epoch.successor().map_err(|_| {
                        CoreError::NamespaceCorrupt(format!(
                            "manifest {} records writer epoch {}, the largest writer epoch, so no \
                         writer can acquire the namespace",
                            payload.manifest_no, payload.writer_epoch
                        ))
                    })?;
                    payload.writer = Some(WriterBlock {
                        writer_id: writer_id.clone(),
                        acquired_at_ms,
                    });
                    let acquired = AcquiredWriter {
                        writer_id: writer_id.clone(),
                        writer_epoch: payload.writer_epoch,
                    };
                    Ok(ManifestChange::Next(Box::new(payload), acquired))
                },
            )
            .await?;
        let claim = update.current_manifest();
        let acquired = update.response;
        // The tail read before the fence put doesn't check the manifest: the
        // check after the put does, and it also tells whether the fence is
        // live.
        let anchor = self
            .load_unchecked_read_anchor(
                update.current.manifest,
                update.current.boundary,
                clock.origin(),
                Some(&acquired),
            )
            .await?;
        let anchor = self.put_fence(&acquired, anchor, &clock).await?;
        Ok(WriterAcquisition {
            writer: acquired,
            claim,
            anchor,
        })
    }

    /// Writes a fence with `held`, the writer epoch that a writer session
    /// already holds, at the next WAL number, as the second step of an
    /// acquisition does, and returns the read anchor whose tip is the fence
    /// (format spec, "Failed and unknown outcomes"). A session owes such a
    /// fence after a WAL put of its own whose outcome it doesn't know. The
    /// tail read before the put returns `writer_fenced` when the manifest
    /// has a higher epoch, as every tail read of a session does.
    pub(crate) async fn fence_writer(&self, held: &AcquiredWriter) -> Result<NamespaceReadAnchor> {
        let clock = self.start_clock();
        let anchor = self.load_writer_read_anchor(held, clock.observe()).await?;
        self.put_fence(held, anchor, &clock).await
    }

    /// Puts a fence with `acquired`'s epoch at the tip of `anchor`, checks
    /// it after the put, and puts it again at the new tip while it's dead or
    /// its number is taken (format spec, "Writer ownership"). Returns the
    /// read anchor whose tip is the live fence.
    async fn put_fence(
        &self,
        acquired: &AcquiredWriter,
        mut anchor: NamespaceReadAnchor,
        clock: &OperationClock,
    ) -> Result<NamespaceReadAnchor> {
        // Bounded by both the publication time budget and
        // `MAX_UNFOLDED_WAL_OBJECTS`: an old writer can never race more than
        // the write-stop ceiling of WAL objects without folding, while the
        // attempt cap prevents an unbounded spin against a frozen clock or
        // broken store.
        for _attempt in 0..crate::limits::MAX_UNFOLDED_WAL_OBJECTS {
            let head = &anchor.read_state;
            super::control::ensure_namespace_live(&head.namespace_id, head.status)?;
            let fence = prepare_fence(head, acquired.writer_epoch)?;
            match publish_wal_object(&self.store, &fence).await {
                Ok(()) => {
                    let fenced_head = head.after_wal_object(fence.envelope().payload());
                    if self.check_anchor_manifest(&mut anchor).await? {
                        anchor.read_state = fenced_head;
                        anchor
                            .tail
                            .push_published(fence.envelope().clone(), fence.as_bytes().len());
                        return Ok(anchor);
                    }
                    // A newer manifest. The writer read anchor at that
                    // manifest returns `writer_fenced` for a higher epoch,
                    // and replays a live fence.
                    anchor = self
                        .load_writer_read_anchor(acquired, clock.observe())
                        .await?;
                    if wal_object_is_live(fenced_head.wal_no, anchor.current_manifest().payload()) {
                        super::control::ensure_namespace_live(
                            &anchor.read_state.namespace_id,
                            anchor.read_state.status,
                        )?;
                        return Ok(anchor);
                    }
                    clock.ensure_metadata_publication_budget(&self.namespace_id)?;
                }
                Err(CoreError::WalPublish(crate::wal::WalPublishError::NumberTaken)) => {
                    clock.ensure_metadata_publication_budget(&self.namespace_id)?;
                    anchor = self
                        .load_writer_read_anchor(acquired, clock.observe())
                        .await?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(CoreError::contention_exhausted(
            &anchor.current_manifest().object_key,
        ))
    }

    /// Returns the manifest of the namespace's cached head, with the manifest
    /// boundary that the head keeps, when the head is active and no writer has
    /// claimed the namespace at that head, such as the head that a create or a
    /// fork installs (see [`Self::install_active_head`]). Returns `None` when
    /// there's no such head, or when the manifest is at or below the boundary.
    ///
    /// A claim builds on this manifest without a request first. If another
    /// process published past the manifest, the claim's put is lost and the
    /// claim builds on the manifest that won (format spec, "Publishing a
    /// manifest").
    async fn unclaimed_head_basis(&self) -> Result<Option<PublicationBasis>> {
        let Some(head) = self.head_and_wal_tail_cache.get_head(&self.namespace_id) else {
            return Ok(None);
        };
        let state = &head.read_basis.head;
        if state.writer.is_some() || !state.status.is_active() {
            return Ok(None);
        }
        let segments = self
            .load_held_manifest(head.read_basis.basis.manifest(), head.boundary.clone())
            .await?;
        Ok(segments.map(|segments| PublicationBasis {
            manifest: segments.loaded_manifest(),
            boundary: head.boundary.clone(),
        }))
    }
}

/// Fails with `writer_fenced` when `active_epoch`, the writer epoch of a
/// manifest or a head, isn't the epoch of `acquired_writer`. `active_writer`
/// is the writer block that the manifest or head records.
pub(crate) fn ensure_writer_not_fenced(
    active_epoch: WriterEpoch,
    active_writer: Option<&WriterBlock>,
    acquired_writer: &AcquiredWriter,
) -> Result<()> {
    if active_epoch == acquired_writer.writer_epoch {
        return Ok(());
    }
    Err(CoreError::WriterFenced(WriterFence {
        fenced_epoch: acquired_writer.writer_epoch,
        active_epoch,
        active_writer: active_writer.cloned(),
    }))
}
