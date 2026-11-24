//! Writer acquisition through a manifest epoch and a numbered fence WAL object,
//! and the retry loop that fences and floor objects share.

use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::manifest::publish::ManifestChange;
use crate::manifest::PublicationReport;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Checked;
use crate::namespace::held::Held;
use crate::namespace::read_anchor::{NamespaceReadAnchor, WriterAnchor};
use crate::namespace::store::NamespaceStore;
use crate::time::{Observation, OperationClock};
use crate::wal::{prepare_wal_object, publish_wal_object, wal_no_is_unfolded};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::WriterBlock;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::format::wal::WalObjectBody;
use loonfs_types::{CommitSeq, WriterEpoch, WriterId};

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
    pub(crate) claim: Held<LoadedManifest>,
    pub(crate) anchor: WriterAnchor,
}

/// A fence or floor object that [`NamespaceStore::put_fence_or_floor_object`]
/// puts at the WAL tip. The two kinds differ in where each attempt's writer
/// epoch comes from.
#[derive(Debug, Clone, Copy)]
enum FenceOrFloorObject<'a> {
    /// A fence with the epoch of this acquired writer. Loading a writer
    /// anchor whose manifest has a higher epoch returns `writer_fenced`.
    Fence(&'a AcquiredWriter),
    /// A floor object for this floor, put by a process without the writer
    /// epoch. Each attempt copies the epoch of the manifest that the
    /// attempt's anchor loaded (format spec, "Advancing the retention
    /// floor").
    Floor(CommitSeq),
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
    /// live (see [`wal_no_is_unfolded`]), and puts a dead fence again at the
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
        let claim = update.current.clone();
        let acquired = update.response;
        // The tail read before the fence put doesn't check the manifest,
        // because the check after the put does, and that check also tells
        // whether the fence is live.
        let anchor = self
            .load_unchecked_writer_anchor(update.current, &acquired)
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
    /// load before the put checks the writer epoch and the namespace's
    /// status, as every load of a writer anchor does (see [`WriterAnchor`]).
    pub(crate) async fn fence_writer(&self, held: &AcquiredWriter) -> Result<WriterAnchor> {
        let clock = self.start_clock();
        let anchor = self.load_writer_read_anchor(held, clock.observe()).await?;
        self.put_fence(held, anchor, &clock).await
    }

    /// Puts a fence with `acquired`'s epoch at the tip of `anchor`, as
    /// [`Self::put_fence_or_floor_object`] describes. Every anchor that the
    /// put starts from or loads is a writer anchor, so the anchor it returns
    /// is one too.
    async fn put_fence(
        &self,
        acquired: &AcquiredWriter,
        anchor: WriterAnchor,
        clock: &OperationClock,
    ) -> Result<WriterAnchor> {
        self.put_fence_or_floor_object(FenceOrFloorObject::Fence(acquired), anchor.0, clock)
            .await
            .map(WriterAnchor)
    }

    /// Creates a floor object for `target` at the WAL tip of the live read
    /// anchor, as [`Self::put_fence_or_floor_object`] describes, and returns
    /// the anchor whose view includes the floor object, or the first anchor
    /// whose floor is already at or above `target`.
    pub(crate) async fn put_floor_object(&self, target: CommitSeq) -> Result<NamespaceReadAnchor> {
        let clock = self.start_clock();
        let anchor = self.load_live_read_anchor(clock.observe()).await?;
        self.put_fence_or_floor_object(FenceOrFloorObject::Floor(target), anchor, &clock)
            .await
    }

    /// Puts `object` at the tip of `anchor`, checks after the put whether it's
    /// live, and puts it again at the new tip while it's dead or its number is
    /// taken (format spec, "Writer ownership" and "Advancing the retention
    /// floor"). Returns the read anchor whose view includes the live object.
    /// For a floor object, the call returns without a put at the first anchor
    /// whose floor is already at or above the target.
    ///
    /// The object is live when the anchor's manifest is still the newest, and
    /// otherwise when its number is above the `folded_wal_no` of the newest
    /// manifest (see [`wal_no_is_unfolded`]). The newest manifest is the one
    /// that the next anchor's load finds, so a dead object's next attempt
    /// starts from that anchor without another load. Every put after the
    /// first checks the publication budget first, and the call returns
    /// `store_contention` when the budget or the attempts run out.
    async fn put_fence_or_floor_object(
        &self,
        object: FenceOrFloorObject<'_>,
        mut anchor: NamespaceReadAnchor,
        clock: &OperationClock,
    ) -> Result<NamespaceReadAnchor> {
        // Bounded by both the publication time budget and
        // `MAX_UNFOLDED_WAL_OBJECTS`: a writer that commits ahead of the put
        // stops at `MAX_UNFOLDED_WAL_OBJECTS` unfolded WAL objects until a
        // fold, and the attempt cap prevents an unbounded spin against a
        // frozen clock or broken store.
        for attempt in 0..crate::limits::MAX_UNFOLDED_WAL_OBJECTS {
            let head = &anchor.read_state;
            let (writer_epoch, body) = match object {
                FenceOrFloorObject::Fence(acquired) => {
                    (acquired.writer_epoch, WalObjectBody::Fence)
                }
                FenceOrFloorObject::Floor(target) if head.retention_floor_seq >= target => {
                    return Ok(anchor)
                }
                FenceOrFloorObject::Floor(target) => (
                    head.writer_epoch,
                    WalObjectBody::Floor {
                        retention_floor_seq: target,
                    },
                ),
            };
            let prepared = prepare_wal_object(head, writer_epoch, body)?;
            if attempt > 0 {
                clock.ensure_metadata_publication_budget(&self.namespace_id)?;
            }
            match publish_wal_object(&self.store, &prepared).await {
                Ok(()) => {
                    let put_head = head.after_wal_object(prepared.envelope().payload());
                    if self.check_newest(&mut anchor.held, clock.origin()).await? == Checked::Newest
                    {
                        anchor.read_state = put_head;
                        anchor
                            .tail
                            .push_published(prepared.envelope().clone(), prepared.as_bytes().len());
                        return Ok(anchor);
                    }
                    anchor = self.load_next_anchor(object, clock.observe()).await?;
                    if wal_no_is_unfolded(
                        put_head.wal_no,
                        anchor.current_manifest().folded_wal_no(),
                    ) {
                        return Ok(anchor);
                    }
                }
                Err(CoreError::WalPublish(crate::wal::WalPublishError::NumberTaken)) => {
                    anchor = self.load_next_anchor(object, clock.observe()).await?;
                }
                Err(error) => return Err(error),
            }
        }
        Err(CoreError::contention_exhausted(
            &anchor.current_manifest().object_key,
        ))
    }

    /// Loads the anchor that the next put of `object` starts from: the writer
    /// anchor for a fence (see [`Self::load_writer_read_anchor`]), or the
    /// live read anchor for a floor object, whose manifest's epoch the floor
    /// object copies. Either load returns `namespace_deleted` for a
    /// tombstone.
    async fn load_next_anchor(
        &self,
        object: FenceOrFloorObject<'_>,
        observed: Observation,
    ) -> Result<NamespaceReadAnchor> {
        match object {
            FenceOrFloorObject::Fence(acquired) => {
                Ok(self.load_writer_read_anchor(acquired, observed).await?.0)
            }
            FenceOrFloorObject::Floor(_) => self.load_live_read_anchor(observed).await,
        }
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
    /// claim builds on the manifest that won (format spec, "Publishing a
    /// manifest").
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
