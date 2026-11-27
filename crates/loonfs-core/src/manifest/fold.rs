//! Folds the visible WAL tail and publishes the next manifest number.

use super::publish::ManifestChange;
use super::published::same_head;
use crate::commit::publish_view::WriterPosition;
use crate::error::CoreError;
use crate::error::Result;
use crate::in_flight::STORE_WRITES_IN_FLIGHT;
use crate::metadata::MetadataView;
use crate::namespace::basis::{MetadataBasis, ReadBasis};
use crate::namespace::control::{ensure_namespace_live, LoadedManifest};
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
use crate::wal::WalPublishError;
use futures::{stream, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::format::manifest::{MetadataRunRef, NamespaceManifestPayload, RunTier};
use loonfs_types::{CommitSeq, ContentId, ManifestNo, NamespaceId, RunNo};
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

    /// Folds the WAL tail from `position`, a writer session's position, and
    /// publishes the next manifest.
    ///
    /// The manifest copies the writer epoch from its predecessor, and its
    /// retention floor is the higher of its predecessor's floor and the
    /// floors of the floor objects that the fold covers (format spec,
    /// "Publishing a manifest"). A tail of floor objects and no commits is
    /// folded too, into a manifest with no new run. The position is the
    /// session's, whatever its age, and only a writer session has one (see
    /// [`NamespaceCommitEngine`](crate::publish::NamespaceCommitEngine)), so
    /// every fold is a writer session's. A position whose manifest is
    /// missing from the store and at or below the manifest boundary returns
    /// `head_moved`, so the session drops the position and its next fold
    /// loads the tail with the writer's load (see `load_held_manifest`). The
    /// publication's first attempt starts from the position's manifest, and
    /// each later attempt starts from the manifest that the attempt before
    /// it found (see `update_manifest`).
    /// A current manifest that folds the tail finishes the fold, and any other
    /// manifest after the fold's own predecessor makes the fold rebuild against
    /// it. The rebuild keeps the tail it read when the manifest is at the same
    /// head (see `same_head`), and loads the tail again otherwise, which
    /// folds a new writer's log after the writer's claim. When an attempt
    /// finds a segment of its manifest missing and a newer manifest replaced
    /// that manifest, the next attempt rebuilds against the newest manifest
    /// the same way (see [`SegmentKeeper::load_error`](crate::segment::SegmentKeeper::load_error)). A missing
    /// segment of the current manifest returns `namespace_corrupt`. A
    /// namespace tombstone stops the fold.
    pub async fn fold_wal_tail_from(
        &self,
        position: &WriterPosition,
        clock: &OperationClock,
    ) -> Result<FoldedWalTail> {
        let manifest_no = position.read_basis().basis.manifest().manifest_no;
        let Some(projection) = self.load_position_projection(position.clone()).await? else {
            return Err(CoreError::HeadMoved(format!(
                "manifest `{manifest_no}` of the writer position is missing and at or below the \
                 manifest boundary"
            )));
        };
        let basis = projection
            .basis
            .decoded(projection.manifest_segments.loaded_manifest());
        if basis.manifest().folded_wal_no() == projection.head().wal_no {
            // The manifest that the tail was read above already folds it, and
            // keeps the check that confirmed it.
            return Ok(FoldedWalTail::new(
                &self.namespace_id,
                projection.head().seq,
                basis,
                FoldWalOutcome::AlreadyCurrent,
            ));
        }
        // Each attempt's future borrows the fold through this lock, because a
        // future that a closure returns can't borrow the closure's own state.
        let fold = tokio::sync::Mutex::new(FoldAttempts {
            namespace: self,
            clock,
            projection,
            layouts: None,
        });
        let folded = self
            .update_manifest(Some(basis), clock, |current: LoadedManifest| {
                let fold = &fold;
                async move { fold.lock().await.attempt(current).await }
            })
            .await;
        let update = match folded {
            Ok(update) => update,
            // Every attempt lost its put to another publication or found its
            // manifest replaced. The fold reports a lost race, so a runtime
            // that keeps a view of the namespace drops it (see
            // `CoreError::is_stale_basis`).
            Err(CoreError::StoreContention { .. }) => {
                return Err(CoreError::WalPublish(WalPublishError::NumberTaken))
            }
            Err(error) => return Err(error),
        };
        Ok(FoldedWalTail::new(
            &self.namespace_id,
            fold.into_inner().projection.head().seq,
            update.current,
            update.response,
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

    pub(super) async fn build_namespace_manifest_for_projection(
        &self,
        projection: &ManifestProjection<'_, S>,
        layouts: &HashMap<ContentId, ContentLayoutRecord>,
    ) -> Result<NamespaceManifestPayload> {
        let activity = projection
            .manifest_segments
            .manifest()
            .payload()
            .activity
            .checked_add(projection.tail_state.activity())
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(
                    "activity counter cannot exceed 9007199254740991".to_owned(),
                )
            })?;
        let head_seq = projection.head().seq;
        let tail_state = projection.tail_state.combined_rows();
        // A WAL fold keeps existing runs and writes the WAL delta as one new delta
        // run. Compaction rewrites delta runs into the base separately. The
        // manifest that first lists a run allocates its number, and an
        // extension's publication can allocate numbers before the first fold.
        let run_no = projection
            .manifest_segments
            .manifest()
            .payload()
            .next_run_no;
        let (runs, next_run_no) = if projection
            .manifest_segments
            .manifest()
            .payload()
            .runs
            .is_empty()
        {
            (
                vec![MetadataRunRef {
                    run_no,
                    run_seq: head_seq,
                    tier: RunTier::Base,
                    segments: flatten_manifest_segments(
                        self.build_manifest_segments(&tail_state, layouts).await?,
                    ),
                }],
                next_run_no_after(run_no)?,
            )
        } else {
            let previous_manifest = projection.manifest_segments.manifest();
            // A fold takes one number for its delta, or none when the head is
            // unchanged.
            let mut runs = previous_manifest.payload().runs.clone();
            let mut next_run_no = run_no;
            if previous_manifest.payload().head_seq < head_seq {
                runs.push(MetadataRunRef {
                    run_no,
                    run_seq: head_seq,
                    tier: RunTier::Delta,
                    segments: flatten_manifest_segments(
                        self.build_manifest_delta_run_segments(
                            previous_manifest.payload().head_seq,
                            &tail_state,
                            layouts,
                        )
                        .await?,
                    ),
                });
                next_run_no = next_run_no_after(run_no)?;
            }
            (runs, next_run_no)
        };

        let previous = projection.manifest_segments.manifest().payload();
        // The head's floor includes every floor object in the tail that the
        // fold covers (format spec, "Publishing a manifest").
        Ok(NamespaceManifestPayload {
            activity,
            head_seq,
            writer_epoch: projection.head().writer_epoch,
            next_run_no,
            folded_wal_no: projection.head().wal_no,
            retention_floor_seq: previous
                .retention_floor_seq
                .max(projection.head().retention_floor_seq),
            ..previous
                .successor(runs)
                .map_err(|_| no_manifest_after(previous.manifest_no))?
        })
    }
}

/// Whether `current` already folds the WAL tail that ends at `head`.
fn covers_tail(current: &NamespaceManifestPayload, head: &NamespaceReadState) -> bool {
    current.folded_wal_no >= head.wal_no && current.head_seq >= head.seq
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

/// One fold's state across its publication attempts.
struct FoldAttempts<'a, S: ObjectStore> {
    namespace: &'a NamespaceStore<S>,
    clock: &'a OperationClock,
    projection: ManifestProjection<'a, S>,
    /// The content objects written for the projection's tail, which a
    /// rebuild over a manifest at the same head uses again.
    layouts: Option<HashMap<ContentId, ContentLayoutRecord>>,
}

impl<S: ObjectStore> FoldAttempts<'_, S> {
    /// Returns the next manifest over `current`, or `Finished` when `current`
    /// already folds the tail.
    async fn attempt(&mut self, current: LoadedManifest) -> Result<ManifestChange<FoldWalOutcome>> {
        let payload = current.payload();
        ensure_namespace_live(&self.namespace.namespace_id, payload.status)?;
        if covers_tail(payload, self.projection.head()) {
            return Ok(ManifestChange::Finished(FoldWalOutcome::AlreadyCurrent));
        }
        // Another manifest followed the fold's own predecessor without
        // folding its tail, such as a compaction or a new writer's claim.
        // Over a manifest at the same head, everything the fold has is
        // still right:
        // - Tail rows: see `same_head`.
        // - Activity: a successor at the same `head_seq` keeps it, so the
        //   basis activity plus the tail's is still the total.
        // - Run numbers: the retry takes its runs and run number from the
        //   winner.
        // - Content: the first attempt's extents and layouts still describe
        //   this tail.
        if payload.manifest_no != self.projection.basis.manifest_no() {
            let read_basis = self.projection.basis.read_basis();
            if same_head(&read_basis.basis, &read_basis.head, &current) {
                // A check that found no manifest after the projection's
                // manifest also found none after `current`, a later manifest,
                // so the projection keeps that check.
                self.projection.basis.read_basis_mut().basis =
                    MetadataBasis::new(current.manifest());
                self.projection.manifest_segments =
                    self.namespace.current_segments(&current).segments;
            } else {
                self.projection = self
                    .namespace
                    .load_fold_projection(self.clock.observe())
                    .await?;
                self.layouts = None;
                // The tail was read above a manifest newer than `current`.
                if self.projection.basis.manifest_no() != payload.manifest_no {
                    return Ok(ManifestChange::Again);
                }
                if covers_tail(payload, self.projection.head()) {
                    return Ok(ManifestChange::Finished(FoldWalOutcome::AlreadyCurrent));
                }
            }
        }
        // The projection reads the rows of `current`, which can be a writer
        // position's manifest kept in memory. A newer manifest may have
        // replaced that manifest, and a collection may then have deleted its
        // segments. A read that finds a segment missing then returns
        // `ManifestReplaced` (see `SegmentKeeper::load_error`), the attempt
        // returns `Again`, and the next attempt builds on the newest manifest
        // through the rebuild above, as an attempt after a lost put does. A
        // missing segment of the current manifest stays `namespace_corrupt`.
        let built = async {
            let layouts = match self.layouts.take() {
                Some(layouts) => layouts,
                None => {
                    self.namespace
                        .materialize_tail_content(&self.projection)
                        .await?
                }
            };
            self.clock
                .ensure_metadata_publication_budget(&self.namespace.namespace_id)?;
            let manifest = self
                .namespace
                .build_namespace_manifest_for_projection(&self.projection, &layouts)
                .await?;
            Result::Ok((manifest, layouts))
        }
        .await;
        match built {
            Ok((manifest, layouts)) => {
                self.layouts = Some(layouts);
                Ok(ManifestChange::Next(
                    Box::new(manifest),
                    FoldWalOutcome::Published,
                ))
            }
            Err(CoreError::ManifestReplaced { .. }) => Ok(ManifestChange::Again),
            Err(error) => Err(error),
        }
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
