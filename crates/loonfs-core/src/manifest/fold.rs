//! Folds the visible WAL tail and publishes the next manifest number.

use crate::commit::publication_lane::LaneTurn;
use crate::commit::publish_view::WriterPosition;
use crate::error::CoreError;
use crate::error::Result;
use crate::in_flight::STORE_WRITES_IN_FLIGHT;
use crate::manifest::load::position_manifest_gone;
use crate::metadata::MetadataView;
use crate::namespace::basis::ReadBasis;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::segment::runs::flatten_manifest_segments;
use crate::segment::scan::VerifiedMetadataSegments;
use crate::segment::KeptBy;
use crate::storage::tail_content::{
    assemble_tail_content, write_tail_content, CombinePolicy, ContentWrites,
};
use crate::time::{Observation, OperationClock};
use crate::wal::ProjectedWalTail;
use futures::{stream, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::format::manifest::{
    ManifestActivity, MetadataRunRef, MetadataSegmentRef, NamespaceManifestPayload, RunTier,
};
use loonfs_types::{CommitSeq, ContentId, ManifestNo, NamespaceId, RunNo, WalNo};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::Instrument;

/// How one WAL fold ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldWalOutcome {
    /// A current manifest already folded the WAL tail, so nothing was
    /// published.
    AlreadyCurrent,
    /// This call published the next current manifest.
    Published,
}

/// The current manifest state after one WAL fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldWalResponse {
    /// Namespace whose WAL tail was folded.
    pub namespace_id: NamespaceId,
    /// Head sequence the fold attempted to cover.
    pub target_head_seq: CommitSeq,
    /// Current manifest number after the operation.
    pub manifest_no: ManifestNo,
    /// Sequence covered by that manifest.
    pub manifest_head_seq: CommitSeq,
    /// Whether this call published the current manifest.
    pub outcome: FoldWalOutcome,
}

impl<S: ObjectStore> NamespaceStore<S> {
    async fn load_fold_projection(
        &self,
        observed: Observation,
    ) -> Result<ManifestProjection<'_, S>> {
        self.load_manifest_projection(observed)
            .instrument(crate::phase_span!(crate::Phase::ScanNamespaceState))
            .await
    }

    /// Writes the pending pieces of each chain in the projection's tail to
    /// content objects under the pool's prefix, and returns each chain's
    /// layout, which records the chain's creator.
    pub(super) async fn materialize_tail_content(
        &self,
        projection: &ManifestProjection<'_, S>,
    ) -> Result<HashMap<ContentId, ContentLayoutRecord>> {
        let tail = projection.tail_state.as_ref();
        // Each layout records the chain's creator, which is this namespace
        // for every chain in its own tail, and the source for a chain in the
        // source's tail that a fork folds.
        let head = projection.head();
        let assembled = tail
            .contents()
            .into_iter()
            .map(|content| {
                let pieces = assemble_tail_content(&head.pool_id, &content)?;
                Ok((content, pieces))
            })
            .collect::<Result<Vec<_>>>()?;
        let view = projection.metadata_view();
        let layouts = Mutex::new(HashMap::new());
        // In a live namespace, failed folds leave committed content for the next fold.
        stream::iter(assembled.into_iter().map(Ok))
            .try_for_each_concurrent(STORE_WRITES_IN_FLIGHT, |(content, pieces)| {
                let layouts = &layouts;
                async move {
                    let layout = write_tail_content(
                        ContentWrites::new(&self.store, &self.content_writes, head),
                        &view,
                        &content,
                        pieces,
                        CombinePolicy::OwnAndSharedBaseExtents,
                    )
                    .await?;
                    {
                        layouts
                            .lock()
                            .expect("layout lock should not be poisoned")
                            .insert(content.content_ref.content_id.clone(), layout);
                    }
                    Result::Ok(())
                }
            })
            .await?;
        Ok(layouts
            .into_inner()
            .expect("layout lock should not be poisoned"))
    }

    /// Writes the content objects and segments of the WAL tail of
    /// `position`, a writer session's position, from the position's
    /// manifest, before the fold takes its turn in the session's publication
    /// lane, where [`Self::publish_fold_in_turn`] publishes them.
    ///
    /// The fold writes each chain's pending pieces as content objects and a
    /// metadata segment for each family that the tail's commits touched, as
    /// one new run: the base run when the manifest has no runs, and a delta
    /// run otherwise. A tail of floor objects and no commits writes no run.
    /// A position whose manifest already folds its tail writes nothing and
    /// returns [`FoldWriteOutcome::AlreadyFolded`]. A position whose manifest
    /// is missing from the store and at or below the manifest boundary
    /// returns `head_moved`, so the session drops the position and loads it
    /// again from the store under its writer epoch (see `load_held_manifest`). A missing segment
    /// of the position's manifest returns `manifest_replaced` when a newer
    /// manifest replaced it, and `namespace_corrupt` otherwise (see
    /// [`SegmentKeeper::load_error`](crate::segment::SegmentKeeper::load_error)).
    pub async fn write_fold_segments(
        &self,
        position: &WriterPosition,
        clock: &OperationClock,
    ) -> Result<FoldWriteOutcome> {
        let Some(projection) = self.load_position_projection(position.clone()).await? else {
            return Err(position_manifest_gone(position));
        };
        let manifest = projection.manifest_segments.manifest();
        let started_folded_wal_no = manifest.payload().folded_wal_no;
        if started_folded_wal_no == projection.head().wal_no {
            // The manifest that the tail was read above already folds it, and
            // keeps the check that confirmed it.
            let basis = projection
                .basis
                .decoded(projection.manifest_segments.loaded_manifest());
            return Ok(FoldWriteOutcome::AlreadyFolded(FoldedWalTail::new(
                &self.namespace_id,
                projection.head().seq,
                basis,
                FoldWalOutcome::AlreadyCurrent,
            )));
        }
        let layouts = self.materialize_tail_content(&projection).await?;
        clock.ensure_metadata_publication_budget(&self.namespace_id)?;
        Ok(FoldWriteOutcome::Written(Box::new(FoldWrites {
            started_folded_wal_no,
            head: projection.head().clone(),
            tail_activity: projection.tail_state.activity(),
            run: self.write_fold_run(&projection, &layouts).await?,
        })))
    }

    /// Publishes the manifest that lists `writes`, a fold's segments, as a
    /// publication of a writer session's publication lane, whose turn is
    /// `turn` (see [`Self::publish_manifest_in_turn`]), and returns the
    /// manifest that covers the tail.
    ///
    /// The manifest is the turn's basis, the session's newest published
    /// manifest, with the fold's run listed after whatever runs the basis
    /// lists, the fold's head, and the higher of the basis's floor and the
    /// floors of the floor objects that the fold covers (format spec,
    /// "Publishing a manifest"). The session's folds run one at a time from
    /// the session's position, and a publication of the session in between,
    /// such as a compaction unit, a window change or the session's own
    /// claim, doesn't change the head or the folded WAL number, so the basis
    /// has the `folded_wal_no` that the fold started from. When another
    /// writer folded the tail before the session's claim, the basis covers
    /// the fold's tail, and the call publishes nothing. A basis that covers
    /// only part of the fold's tail is bad stored state, and the call returns
    /// `namespace_corrupt`.
    ///
    /// # Panics
    ///
    /// Panics if `writes` are another namespace's, which is a bug in the
    /// calling process.
    pub async fn publish_fold_in_turn(
        &self,
        turn: LaneTurn<'_>,
        writes: Box<FoldWrites>,
        clock: &OperationClock,
    ) -> Result<FoldedWalTail> {
        assert_eq!(
            writes.head.namespace_id, self.namespace_id,
            "a fold publishes the segments that it wrote for its own namespace"
        );
        let previous = turn.basis().manifest().payload();
        if previous.folded_wal_no >= writes.head.wal_no && previous.head_seq >= writes.head.seq {
            return Ok(FoldedWalTail::new(
                &self.namespace_id,
                writes.head.seq,
                turn.basis().clone(),
                FoldWalOutcome::AlreadyCurrent,
            ));
        }
        // No state that this process writes reaches this branch. The session's
        // folds run one at a time under the namespace's fold lock, so no fold of
        // the session publishes between this fold's writes and its turn, and a
        // fold of another writer that the session's claim builds on covers the
        // whole tail below that writer's fence.
        if previous.folded_wal_no != writes.started_folded_wal_no {
            return Err(CoreError::NamespaceCorrupt(format!(
                "manifest {} folds WAL number {}, but the fold that builds on it in a writer \
                 session's lane started from folded WAL number {} and folds through {}",
                previous.manifest_no,
                previous.folded_wal_no,
                writes.started_folded_wal_no,
                writes.head.wal_no
            )));
        }
        let payload = fold_payload(previous, &writes)?;
        let published = self.publish_manifest_in_turn(turn, payload, clock).await?;
        Ok(FoldedWalTail::new(
            &self.namespace_id,
            writes.head.seq,
            published,
            FoldWalOutcome::Published,
        ))
    }

    /// Returns the manifest and the WAL tail of this namespace's head:
    /// `held`, a writer session's position, whatever its age, or what a
    /// discovery finds when `held` is `None` or its manifest is missing from
    /// the store and at or below the manifest boundary (see
    /// `load_held_manifest`). A fork reads its source's head here, and
    /// publishes nothing on the source.
    pub(crate) async fn load_head_projection(
        &self,
        held: Option<WriterPosition>,
        clock: &OperationClock,
    ) -> Result<ManifestProjection<'_, S>> {
        let held = match held {
            Some(held) => self.load_position_projection(held).await?,
            None => None,
        };
        match held {
            Some(projection) => Ok(projection),
            None => self.load_fold_projection(clock.observe()).await,
        }
    }

    /// Returns the manifest and the WAL tail of `held`, a writer session's
    /// position, or `None` when its manifest is missing from the store and
    /// at or below the manifest boundary (see `load_held_manifest`).
    async fn load_position_projection(
        &self,
        held: WriterPosition,
    ) -> Result<Option<ManifestProjection<'_, S>>> {
        let decoded = self
            .load_held_manifest(
                held.read_basis().basis.manifest(),
                held.basis.boundary().clone(),
            )
            .await?;
        Ok(decoded.map(|decoded| {
            let segments = self.segments(decoded, self.keeper(KeptBy::Current));
            ManifestProjection {
                basis: held.basis,
                manifest_segments: segments.segments,
                tail_state: held.tail,
            }
        }))
    }

    pub(super) async fn load_manifest_projection(
        &self,
        observed: Observation,
    ) -> Result<ManifestProjection<'_, S>> {
        let anchor = self.load_live_read_anchor(observed).await?;
        let loaded_basis = self.current_segments(anchor.held.manifest());
        let tail_state = loaded_basis.project_tail(&anchor)?;
        Ok(ManifestProjection {
            basis: anchor.held_basis(),
            manifest_segments: loaded_basis.segments,
            tail_state,
        })
    }

    /// Returns the manifest after `projection`'s manifest that folds the
    /// projection's tail, whose content objects `layouts` describe, after it
    /// writes the tail's segments.
    pub(super) async fn build_namespace_manifest_for_projection(
        &self,
        projection: &ManifestProjection<'_, S>,
        layouts: &HashMap<ContentId, ContentLayoutRecord>,
    ) -> Result<NamespaceManifestPayload> {
        let writes = FoldWrites {
            started_folded_wal_no: projection
                .manifest_segments
                .manifest()
                .payload()
                .folded_wal_no,
            head: projection.head().clone(),
            tail_activity: projection.tail_state.activity(),
            run: self.write_fold_run(projection, layouts).await?,
        };
        fold_payload(projection.manifest_segments.manifest().payload(), &writes)
    }

    /// Writes the segments of `projection`'s tail as the fold's new run: the
    /// base run when the projection's manifest has no runs, a delta run of
    /// the rows above the manifest's head otherwise, and no run when the head
    /// is the manifest's.
    async fn write_fold_run(
        &self,
        projection: &ManifestProjection<'_, S>,
        layouts: &HashMap<ContentId, ContentLayoutRecord>,
    ) -> Result<Option<FoldRun>> {
        let previous = projection.manifest_segments.manifest().payload();
        let tail_state = projection.tail_state.combined_rows();
        // A WAL fold keeps existing runs and writes the WAL delta as one new delta
        // run. Compaction rewrites delta runs into the base separately.
        let run = if previous.runs.is_empty() {
            FoldRun {
                tier: RunTier::Base,
                segments: flatten_manifest_segments(
                    self.build_manifest_segments(&tail_state, layouts).await?,
                ),
            }
        } else if previous.head_seq < projection.head().seq {
            FoldRun {
                tier: RunTier::Delta,
                segments: flatten_manifest_segments(
                    self.build_manifest_delta_run_segments(previous.head_seq, &tail_state, layouts)
                        .await?,
                ),
            }
        } else {
            return Ok(None);
        };
        Ok(Some(run))
    }
}

/// How the writes of a fold before its turn in the publication lane ended
/// (see [`NamespaceStore::write_fold_segments`]).
pub enum FoldWriteOutcome {
    /// The position's manifest already folds the position's tail, so the
    /// fold writes and publishes nothing.
    AlreadyFolded(FoldedWalTail),
    /// The fold wrote the tail's content objects and segments.
    Written(Box<FoldWrites>),
}

/// The content objects and segments that a fold wrote for a writer
/// position's tail, which the fold's publication lists in its turn (see
/// [`NamespaceStore::publish_fold_in_turn`]).
pub struct FoldWrites {
    /// The `folded_wal_no` of the manifest that the fold started from.
    started_folded_wal_no: WalNo,
    /// The head that replaying the tail reached.
    head: NamespaceReadState,
    /// The activity of the tail's commits.
    tail_activity: ManifestActivity,
    /// The run that lists the tail's segments, or `None` when the tail has
    /// no commit above the manifest's head.
    run: Option<FoldRun>,
}

/// The run that a fold writes, which takes its number from the manifest
/// that lists it.
struct FoldRun {
    tier: RunTier,
    segments: Vec<MetadataSegmentRef>,
}

/// Returns the manifest after `previous` that lists `writes`, a fold's run,
/// with the fold's head. The manifest that first lists a run allocates its
/// number, and an extension's publication can allocate numbers before the
/// first fold.
fn fold_payload(
    previous: &NamespaceManifestPayload,
    writes: &FoldWrites,
) -> Result<NamespaceManifestPayload> {
    let activity = previous
        .activity
        .checked_add(writes.tail_activity)
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(
                "activity counter cannot exceed 9007199254740991".to_owned(),
            )
        })?;
    let mut runs = previous.runs.clone();
    let mut next_run_no = previous.next_run_no;
    if let Some(run) = &writes.run {
        runs.push(MetadataRunRef {
            run_no: next_run_no,
            run_seq: writes.head.seq,
            tier: run.tier,
            segments: run.segments.clone(),
        });
        next_run_no = next_run_no_after(next_run_no)?;
    }
    // The head's floor includes every floor object in the tail that the
    // fold covers (format spec, "Publishing a manifest").
    Ok(NamespaceManifestPayload {
        activity,
        head_seq: writes.head.seq,
        next_run_no,
        folded_wal_no: writes.head.wal_no,
        retention_floor_seq: previous
            .retention_floor_seq
            .max(writes.head.retention_floor_seq),
        ..previous
            .successor(runs)
            .map_err(|_| no_manifest_after(previous.manifest_no))?
    })
}

#[derive(Clone)]
pub struct FoldedWalTail {
    pub response: FoldWalResponse,
    /// The manifest that covers the tail after the call: the one it
    /// published, or the current one that already covered it, with the
    /// manifest boundary that the call read. A snapshot of the manifest is
    /// verified from it.
    pub current: Held<LoadedManifest>,
}

impl FoldedWalTail {
    fn new(
        namespace_id: &NamespaceId,
        target_head_seq: CommitSeq,
        current: Held<LoadedManifest>,
        outcome: FoldWalOutcome,
    ) -> Self {
        let manifest = current.manifest().payload();
        Self {
            response: FoldWalResponse {
                namespace_id: namespace_id.clone(),
                target_head_seq,
                manifest_no: manifest.manifest_no,
                manifest_head_seq: manifest.head_seq,
                outcome,
            },
            current,
        }
    }

    /// Returns the fold of `anchor`'s tail as already current, or `None`
    /// when the anchor has a WAL object above its manifest's
    /// `folded_wal_no`.
    ///
    /// A fold whose tail has no WAL object publishes nothing, so a process
    /// that reads such a tail returns this without claiming the namespace.
    pub fn current_when_no_wal_object(anchor: &NamespaceReadAnchor) -> Option<Self> {
        let manifest = anchor.current_manifest();
        (manifest.folded_wal_no() == anchor.read_state.wal_no).then(|| {
            Self::new(
                &manifest.payload().namespace_id,
                anchor.read_state.seq,
                anchor.held().clone(),
                FoldWalOutcome::AlreadyCurrent,
            )
        })
    }

    /// The manifest this fold published, or `None` when it published none.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn published(&self) -> Option<Held<LoadedManifest>> {
        (self.response.outcome == FoldWalOutcome::Published).then(|| self.current.clone())
    }
}

/// A namespace's head: a manifest, its WAL tail, and the head that replaying
/// the tail reached.
pub(crate) struct ManifestProjection<'a, S: ObjectStore + ?Sized> {
    /// The head, and the manifest that the tail was read above, with the
    /// check that confirmed that manifest.
    pub(crate) basis: Held<ReadBasis>,
    /// The segments of the same manifest, which the projection's reads go
    /// through, and which include the decoded manifest.
    pub(crate) manifest_segments: VerifiedMetadataSegments<'a, S>,
    /// Rows that are not in any segment yet: the genesis root inode when the
    /// basis is genesis, plus the replayed WAL tail.
    pub(crate) tail_state: Arc<ProjectedWalTail>,
}

impl<S: ObjectStore + ?Sized> ManifestProjection<'_, S> {
    pub(crate) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(self.head(), &self.manifest_segments, &self.tail_state)
    }

    /// Returns the head that replaying the tail reached.
    pub(crate) fn head(&self) -> &NamespaceReadState {
        self.basis.head()
    }
}

pub(super) fn next_manifest_no_after(current: ManifestNo) -> Result<ManifestNo> {
    current.successor().map_err(|_| no_manifest_after(current))
}

/// Returns the error for a manifest numbered `current`, the largest manifest
/// number, which no manifest can follow.
pub(crate) fn no_manifest_after(current: ManifestNo) -> CoreError {
    CoreError::NamespaceCorrupt(format!(
        "the current manifest is number {current}, the largest manifest number, so no \
         manifest can follow it"
    ))
}

/// Advances the manifest's run allocator after a producer has taken
/// `current`.
pub(crate) fn next_run_no_after(current: RunNo) -> Result<RunNo> {
    current.successor().map_err(|_| {
        CoreError::NamespaceCorrupt(format!(
            "the manifest's next run number is {current}, the largest run number, so no \
             run can follow it"
        ))
    })
}

#[cfg(test)]
mod ordinal_tests {
    use super::*;
    use loonfs_types::MAX_PUBLIC_INTEGER;
    use std::assert_matches;

    #[test]
    fn manifest_no_advancement_accepts_the_maximum_and_rejects_the_next_value() {
        assert_eq!(
            next_manifest_no_after(ManifestNo(MAX_PUBLIC_INTEGER - 1))
                .expect("advance to public maximum"),
            ManifestNo(MAX_PUBLIC_INTEGER)
        );

        let error = next_manifest_no_after(ManifestNo(MAX_PUBLIC_INTEGER))
            .expect_err("manifest number must not exceed the public maximum");
        assert_matches!(
            error,
            CoreError::NamespaceCorrupt(message) if message.contains("largest manifest number")
        );
    }
}

#[cfg(test)]
#[path = "tests/segment_puts.rs"]
mod segment_puts;
