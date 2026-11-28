//! Writer acquisition through a manifest epoch and a numbered fence WAL object.

use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::manifest::publish::ManifestPublicationOutcome;
use crate::manifest::PublicationReport;
use crate::namespace::control::{ensure_namespace_live, LoadedManifest};
use crate::namespace::held::Checked;
use crate::namespace::held::Held;
use crate::namespace::read_anchor::WriterAnchor;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use crate::wal::{prepare_wal_object, publish_wal_object, wal_no_is_unfolded};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::WriterBlock;
use loonfs_types::format::wal::WalObjectBody;
use loonfs_types::{WriterEpoch, WriterId};

/// Captures the writer identity and fencing epoch a session must retain while publishing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquiredWriter {
    /// Stable writer label copied into the manifest's writer block.
    pub writer_id: WriterId,
    /// Fencing epoch every commit publication from this session must match.
    pub writer_epoch: WriterEpoch,
}

/// A writer epoch that a session acquired: the epoch, its
/// claim manifest, and the read anchor whose tip is the fence.
#[derive(Debug)]
pub(crate) struct WriterAcquisition {
    pub(crate) writer: AcquiredWriter,
    pub(crate) claim: Held<LoadedManifest>,
    pub(crate) anchor: WriterAnchor,
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
    /// The claim builds on the manifest of the namespace's cached head when
    /// no writer has claimed the namespace at that head (see
    /// [`Self::unclaimed_head_basis`]), and otherwise on the newest manifest,
    /// which it finds from the manifest boundary. A claim is the one
    /// publication of an active namespace that builds again after a lost
    /// put: it builds its claim on the manifest that won, for at most
    /// [`MAX_CONTENTION_ATTEMPTS`] puts, after which it returns
    /// `store_contention`. A tombstone returns `namespace_deleted`. After the
    /// fence's put succeeds, the call checks whether the fence is live (see
    /// [`Self::put_fence`]). A newest manifest with a higher writer epoch
    /// returns `writer_fenced`.
    pub(crate) async fn acquire_writer(&self, writer_id: &WriterId) -> Result<WriterAcquisition> {
        let clock = self.start_clock();
        let acquired_at_ms = clock.request_at_ms();
        let mut basis = match self.unclaimed_head_basis().await? {
            Some(basis) => basis,
            None => self.load_current_basis(clock.origin()).await?,
        };
        for _ in 0..MAX_CONTENTION_ATTEMPTS {
            let mut payload = basis.manifest().payload().clone();
            ensure_namespace_live(&payload.namespace_id, payload.status)?;
            payload.writer_epoch = payload.writer_epoch.successor().map_err(|_| {
                CoreError::NamespaceCorrupt(format!(
                    "manifest {} records writer epoch {}, the largest writer epoch, so no writer \
                     can acquire the namespace",
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
            let claim = match self
                .publish_manifest_from(payload, &clock, Some(&basis))
                .await?
            {
                ManifestPublicationOutcome::Published(claim) => claim,
                ManifestPublicationOutcome::Lost(winner) => {
                    basis = winner;
                    continue;
                }
            };
            // The tail read before the fence put doesn't check the manifest,
            // because the check after the put does, and that check also
            // tells whether the fence is live.
            let anchor = self
                .load_unchecked_writer_anchor(claim.clone(), &acquired)
                .await?;
            let anchor = self.put_fence(&acquired, anchor, &clock).await?;
            return Ok(WriterAcquisition {
                writer: acquired,
                claim,
                anchor,
            });
        }
        Err(CoreError::contention_exhausted(
            &basis.manifest().object_key,
        ))
    }

    /// Puts a fence with `acquired`'s epoch at the tip of `anchor`, and
    /// checks after the put whether it's live (format spec, "Writer
    /// ownership"). Returns the writer anchor whose view includes the live
    /// fence.
    ///
    /// The fence is live when the anchor's manifest is still the newest, and
    /// otherwise when its number is above the `folded_wal_no` of the newest
    /// manifest (see [`wal_no_is_unfolded`]). The newest manifest is the one
    /// that the writer anchor's load finds (see
    /// [`Self::load_writer_read_anchor`]), which returns `writer_fenced` for
    /// another writer's epoch. A fence that the check finds folded returns
    /// `namespace_corrupt`: only a fold between the claim and the fence
    /// could fold it, and the claim's epoch is the namespace's, so no other
    /// process publishes in between. A put that finds its number taken, by
    /// a commit of the writer that the claim displaced, puts the fence again
    /// at the tip of that writer anchor. Every put after the first checks
    /// the publication budget first, and the call returns
    /// `store_contention` when the budget or the attempts run out.
    async fn put_fence(
        &self,
        acquired: &AcquiredWriter,
        anchor: WriterAnchor,
        clock: &OperationClock,
    ) -> Result<WriterAnchor> {
        let mut anchor = anchor.0;
        // Bounded by both the publication time budget and
        // `MAX_UNFOLDED_WAL_OBJECTS`: a writer that commits ahead of the put
        // stops at `MAX_UNFOLDED_WAL_OBJECTS` unfolded WAL objects until a
        // fold, and the attempt cap prevents an unbounded spin against a
        // frozen clock or broken store.
        for attempt in 0..crate::limits::MAX_UNFOLDED_WAL_OBJECTS {
            let head = &anchor.read_state;
            let prepared = prepare_wal_object(head, acquired.writer_epoch, WalObjectBody::Fence)?;
            if attempt > 0 {
                clock.ensure_metadata_publication_budget(&self.namespace_id)?;
            }
            let put_wal_no = match publish_wal_object(&self.store, &prepared).await {
                Ok(()) => {
                    let put_head = head.after_wal_object(prepared.envelope().payload());
                    if self.check_newest(&mut anchor.held, clock.origin()).await? == Checked::Newest
                    {
                        anchor.read_state = put_head;
                        anchor
                            .tail
                            .push_published(prepared.envelope().clone(), prepared.as_bytes().len());
                        return Ok(WriterAnchor(anchor));
                    }
                    Some(put_head.wal_no)
                }
                Err(CoreError::WalPublish(crate::wal::WalPublishError::NumberTaken)) => None,
                Err(error) => return Err(error),
            };
            // The old anchor's decoded WAL tail is dropped before the next
            // anchor's load, so the retry keeps one decoded tail in memory.
            drop(anchor);
            anchor = self
                .load_writer_read_anchor(acquired, clock.observe())
                .await?
                .0;
            if let Some(wal_no) = put_wal_no {
                let folded_wal_no = anchor.current_manifest().folded_wal_no();
                if wal_no_is_unfolded(wal_no, folded_wal_no) {
                    return Ok(WriterAnchor(anchor));
                }
                return Err(CoreError::NamespaceCorrupt(format!(
                    "the fence of writer epoch {} at WAL number {wal_no} is folded into manifest \
                     {}, which was published after the claim of that epoch",
                    acquired.writer_epoch,
                    anchor.current_manifest().payload().manifest_no
                )));
            }
        }
        Err(CoreError::contention_exhausted(
            &anchor.current_manifest().object_key,
        ))
    }

    /// Returns the manifest of the namespace's cached head, with the manifest
    /// boundary that the head keeps, when the head is active and no writer has
    /// claimed the namespace at that head, such as the head that a create or a
    /// fork installs when it publishes the active manifest (see
    /// [`Self::publish_active_manifest`]). Returns `None` when
    /// there's no such head, or when the manifest is missing from the store
    /// and at or below the boundary.
    ///
    /// A claim builds on this manifest without a request first. If another
    /// process published past the manifest, the claim's put is lost and the
    /// claim builds on the manifest that won (see [`Self::acquire_writer`]).
    async fn unclaimed_head_basis(&self) -> Result<Option<Held<LoadedManifest>>> {
        let Some(head) = self.head_and_wal_tail_cache.get_head(&self.namespace_id) else {
            return Ok(None);
        };
        let state = &head.read_basis().head;
        if state.writer.is_some() || !state.status.is_active() {
            return Ok(None);
        }
        let decoded = self
            .load_held_manifest(
                head.read_basis().basis.manifest(),
                head.basis().boundary().clone(),
            )
            .await?;
        Ok(decoded.map(|decoded| head.basis().decoded(decoded.manifest)))
    }
}
