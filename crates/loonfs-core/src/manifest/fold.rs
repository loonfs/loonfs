//! Folds the visible WAL tail and publishes the next manifest number.

use super::build::{build_manifest_delta_run_segments, build_manifest_segments};
use super::cache::MetadataSegmentCache;
use super::load::load_basis_metadata_segments;
use super::publish::{
    encode_manifest, manifest_ref_for, publish_manifest, ManifestPublicationOutcome,
};
use super::published::PublishedManifest;
use super::runs::{flatten_manifest_segments, MetadataRunPolicy};
use super::scan::VerifiedMetadataSegments;
use crate::commit_engine::WalFoldInput;
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::error::Result;
use crate::in_flight::STORE_WRITES_IN_FLIGHT;
use crate::limits::MAX_CONTENTION_ATTEMPTS;
use crate::metadata::{MetadataState, MetadataView};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::read_anchor::load_read_anchor;
use crate::namespace::state::NamespaceReadState;
use crate::storage::tail_content::{assemble_tail_content, write_tail_content};
use crate::time::{Deadline, Observation};
use crate::wal::replay_discovered_tail;
use crate::wal::ProjectedWalTail;
use crate::wal::WalPublishError;
use futures::{stream, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::ContentLayoutRecord;
use loonfs_types::format::manifest::{
    MetadataRunRef, NamespaceManifestEnvelope, NamespaceManifestPayload, RunTier,
};
use loonfs_types::{ChangeSeq, ContentId, ManifestNo, NamespaceId, RunNo, WalNo, WriterEpoch};
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
    pub target_head_seq: ChangeSeq,
    /// Current manifest number after the operation.
    pub manifest_no: ManifestNo,
    /// Sequence covered by that manifest.
    pub manifest_head_seq: ChangeSeq,
    /// Whether this call published the current manifest.
    pub outcome: FoldWalOutcome,
}

/// Folds the visible WAL tail into segments and publishes the next manifest.
#[cfg(test)]
pub(crate) async fn fold_wal<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<FoldWalResponse> {
    let deadline = Deadline::start(Arc::new(crate::time::StdMonotonicTimer::default()));
    fold_wal_tail(
        store,
        None,
        namespace_id,
        None,
        &deadline,
        MetadataRunPolicy::default(),
        &tokio::sync::Semaphore::new(32),
    )
    .await
    .map(|folded| folded.response)
}

async fn load_fold_projection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    segment_cache: Option<&'a MetadataSegmentCache>,
    observed: Observation,
) -> Result<ManifestProjection<'a, S>> {
    load_manifest_projection(store, namespace_id, segment_cache, observed)
        .instrument(tracing::debug_span!(
            "loonfs.phase",
            phase = "scan_namespace_state"
        ))
        .await
}

/// Whether a tail replayed above `basis` is also the tail above `winner`, a
/// later manifest that took the fold's number.
fn keeps_held_tail(basis: &NamespaceManifestPayload, winner: &NamespaceManifestPayload) -> bool {
    // The winner may differ from the basis only where a compactor claim, a
    // compaction, or a retention advance writes. Everything the fold holds
    // is then still right over the winner:
    // - Tail rows: replay reads the WAL above `folded_wal_no`, starts from
    //   `head_seq` and `next_inode_id`, and adds the root inode only when
    //   there are no runs. All of these are equal.
    // - Deletion roots: the fold reads each one through the winner's runs,
    //   which hold the same view at `head_seq`. A compaction keeps every
    //   view at or above the floor, and the floor is at most `head_seq`.
    // - Activity: a successor at the same `head_seq` keeps it, so the
    //   basis activity plus the tail's is still the total.
    // - Run numbers: the retry takes its runs and run number from the winner.
    // - Content: the first attempt's extents and layouts still describe this tail.
    //   A deleted winner differs in `status`.
    winner.manifest_no > basis.manifest_no
        && winner.runs.is_empty() == basis.runs.is_empty()
        && *winner
            == NamespaceManifestPayload {
                manifest_no: winner.manifest_no,
                compactor_epoch: winner.compactor_epoch,
                retention_floor_seq: winner.retention_floor_seq,
                next_run_no: winner.next_run_no,
                runs: winner.runs.clone(),
                ..basis.clone()
            }
}

async fn publish_fold<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &ManifestProjection<'_, S>,
    layouts: &HashMap<ContentId, ContentLayoutRecord>,
    deadline: &Deadline,
    policy: MetadataRunPolicy,
) -> Result<ManifestPublicationOutcome> {
    deadline.ensure_metadata_publication_budget(namespace_id)?;
    let manifest_no = next_manifest_no_after(projection.basis.manifest_no())?;
    let manifest = build_namespace_manifest_for_projection(
        store,
        namespace_id,
        projection,
        layouts,
        manifest_no,
        policy,
    )
    .await?;
    // Written segments may outlive the GC grace if publication exceeds its budget.
    deadline.ensure_metadata_publication_budget(namespace_id)?;
    publish_manifest(store, encode_manifest(manifest), deadline).await
}

async fn materialize_tail_content<S: ObjectStore + ?Sized>(
    store: &S,
    projection: &ManifestProjection<'_, S>,
    content_writes: &tokio::sync::Semaphore,
) -> Result<HashMap<ContentId, ContentLayoutRecord>> {
    let tail = projection.tail_state.as_ref();
    let assembled = tail
        .contents()
        .into_iter()
        .map(|content| {
            let pieces = assemble_tail_content(&content)?;
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
                let layout =
                    write_tail_content(store, &view, &content, pieces, content_writes).await?;
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

pub struct FoldedWalTail {
    pub response: FoldWalResponse,
    /// The manifest that covers the tail after the call: the one it published,
    /// or the current one that already covered it.
    pub basis: MetadataBasis,
    /// The writer epoch of `basis`.
    pub writer_epoch: WriterEpoch,
    /// The WAL number that `basis` folds through.
    pub folded_wal_no: WalNo,
}

impl FoldedWalTail {
    fn new(
        namespace_id: &NamespaceId,
        target_head_seq: ChangeSeq,
        manifest: &NamespaceManifestEnvelope,
        outcome: FoldWalOutcome,
    ) -> Self {
        let basis = manifest_ref_for(namespace_id, manifest);
        Self {
            response: FoldWalResponse {
                namespace_id: namespace_id.clone(),
                target_head_seq,
                manifest_no: basis.manifest_no,
                manifest_head_seq: basis.head_seq,
                outcome,
            },
            basis: MetadataBasis(basis),
            writer_epoch: manifest.payload().writer_epoch,
            folded_wal_no: manifest.payload().folded_wal_no,
        }
    }

    /// The manifest this fold published, or `None` when it published none.
    pub(crate) fn published(&self) -> Option<PublishedManifest> {
        (self.response.outcome == FoldWalOutcome::Published).then(|| PublishedManifest::Fold {
            manifest: self.basis.0.clone(),
            folded_wal_no: self.folded_wal_no,
        })
    }
}

/// Folds the WAL tail and publishes the next manifest as the maintainer.
///
/// The manifest copies both epochs from its predecessor (format spec,
/// "Publishing a materialized file set"). A writer session passes the tail
/// it holds as `held`, and every other caller passes `None`, which loads the
/// tail from the store. A fold whose manifest put loses to a manifest that
/// doesn't cover its tail rebuilds against that manifest. It keeps the tail
/// it read when the manifest is at the same head (see `keeps_held_tail`),
/// and loads the tail again otherwise, which folds a new writer's log after
/// the writer's claim. A namespace tombstone stops the fold.
pub async fn fold_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    held: Option<WalFoldInput>,
    deadline: &Deadline,
    policy: MetadataRunPolicy,
    content_writes: &tokio::sync::Semaphore,
) -> Result<FoldedWalTail> {
    let mut projection = match held {
        Some(held) => ManifestProjection {
            manifest_segments: load_basis_metadata_segments(store, segment_cache, &held.basis)
                .await?
                .segments,
            head: held.head,
            basis: held.basis,
            tail_state: held.tail_state,
        },
        None => {
            load_fold_projection(store, namespace_id, segment_cache, deadline.observe()).await?
        }
    };
    let mut layouts = None;
    for _attempt in 0..MAX_CONTENTION_ATTEMPTS {
        let basis = projection.manifest_segments.manifest();
        if basis.payload().folded_wal_no == projection.head.wal_no {
            return Ok(FoldedWalTail::new(
                namespace_id,
                projection.head.seq,
                basis,
                FoldWalOutcome::AlreadyCurrent,
            ));
        }
        let tail_layouts = match layouts.take() {
            Some(layouts) => layouts,
            None => materialize_tail_content(store, &projection, content_writes).await?,
        };
        let (current, outcome) = match publish_fold(
            store,
            namespace_id,
            &projection,
            &tail_layouts,
            deadline,
            policy,
        )
        .await?
        {
            ManifestPublicationOutcome::Published(current) => (current, FoldWalOutcome::Published),
            ManifestPublicationOutcome::CoveredByCurrent(current) => {
                (current, FoldWalOutcome::AlreadyCurrent)
            }
            // Another manifest took the fold's number without folding its
            // tail, such as a compaction or a new writer's claim.
            ManifestPublicationOutcome::PredecessorChanged(winner) => {
                if keeps_held_tail(
                    projection.manifest_segments.manifest().payload(),
                    winner.envelope.payload(),
                ) {
                    projection.basis = MetadataBasis(winner.manifest());
                    projection.manifest_segments =
                        load_basis_metadata_segments(store, segment_cache, &projection.basis)
                            .await?
                            .segments;
                    layouts = Some(tail_layouts);
                } else {
                    projection = load_fold_projection(
                        store,
                        namespace_id,
                        segment_cache,
                        deadline.observe(),
                    )
                    .await?;
                }
                continue;
            }
        };
        return Ok(FoldedWalTail::new(
            namespace_id,
            projection.head.seq,
            &current.envelope,
            outcome,
        ));
    }
    Err(CoreError::WalPublish(WalPublishError::NumberTaken))
}

pub(super) struct ManifestProjection<'a, S: ObjectStore + ?Sized> {
    pub(super) head: NamespaceReadState,
    pub(super) basis: MetadataBasis,
    pub(super) manifest_segments: VerifiedMetadataSegments<'a, S>,
    /// Rows that are not in any segment yet: the genesis root inode when the
    /// basis is genesis, plus the replayed WAL tail.
    pub(super) tail_state: Arc<ProjectedWalTail>,
}

impl<S: ObjectStore + ?Sized> ManifestProjection<'_, S> {
    pub(super) fn metadata_view(&self) -> MetadataView<'_, '_, S> {
        MetadataView::from_loaded_head(&self.head, &self.manifest_segments, &self.tail_state)
    }

    pub(super) async fn tail_with_deletion_inodes(&self) -> Result<MetadataState> {
        let mut tail = self.tail_state.merged_rows();
        let view = self.metadata_view();
        for tombstone in self.tail_state.subtree_tombstones() {
            if tail
                .inode_at_seq(tombstone.root_inode_id, self.head.seq)
                .is_some()
            {
                continue;
            }
            let inode = view
                .inode_at_seq(tombstone.root_inode_id)
                .await?
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "deletion root inode `{}` is missing",
                        tombstone.root_inode_id
                    ))
                })?;
            tail.push_inode_record(inode);
        }
        Ok(tail)
    }
}

pub(super) async fn load_manifest_projection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    segment_cache: Option<&'a MetadataSegmentCache>,
    observed: Observation,
) -> Result<ManifestProjection<'a, S>> {
    let anchor = load_read_anchor(store, namespace_id, observed)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let basis = anchor.basis();
    let head = anchor.read_state;
    if head.status.is_deleted() {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    let loaded_basis =
        super::load::metadata_basis_from_manifest(store, segment_cache, &anchor.manifest);
    let manifest_head = loaded_basis.replay_head(&head);
    let manifest_segments = loaded_basis.segments;
    let replayed = replay_discovered_tail(&manifest_head, &loaded_basis.base_state, &anchor.tail)
        .map_err(CoreError::MetadataProjection)?;
    Ok(ManifestProjection {
        head,
        basis,
        manifest_segments,
        tail_state: Arc::new(replayed.projected_tail),
    })
}

pub(super) fn next_manifest_no_after(current: ManifestNo) -> Result<ManifestNo> {
    current.successor().map_err(|_| {
        CoreError::NamespaceCorrupt(format!(
            "the current manifest is number {current}, the largest manifest number, so no \
             manifest can follow it"
        ))
    })
}

/// Advances the manifest's run allocator after a producer has taken
/// `current`.
pub fn next_run_no_after(current: RunNo) -> Result<RunNo> {
    current.successor().map_err(|_| {
        CoreError::NamespaceCorrupt(format!(
            "the manifest's next run number is {current}, the largest run number, so no \
             run can follow it"
        ))
    })
}

async fn build_namespace_manifest_for_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &ManifestProjection<'_, S>,
    layouts: &HashMap<ContentId, ContentLayoutRecord>,
    manifest_no: ManifestNo,
    policy: MetadataRunPolicy,
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
    let head_seq = projection.head.seq;
    let tail_state = projection.tail_with_deletion_inodes().await?;
    // The fold inserts the segments it writes into the cache it read through.
    let segment_cache = projection.manifest_segments.segment_cache;
    // A WAL fold keeps existing runs and writes the WAL delta as one new delta
    // run. Compaction merges delta runs into the base separately.
    //
    let (runs, next_run_no) = if projection
        .manifest_segments
        .manifest()
        .payload()
        .runs
        .is_empty()
    {
        let run_no = RunNo(0);
        (
            vec![MetadataRunRef {
                run_no,
                run_seq: head_seq,
                tier: RunTier::Base,
                segments: flatten_manifest_segments(
                    build_manifest_segments(
                        store,
                        segment_cache,
                        namespace_id,
                        &tail_state,
                        layouts,
                        policy,
                    )
                    .await?,
                ),
            }],
            next_run_no_after(run_no)?,
        )
    } else {
        let previous_manifest = projection.manifest_segments.manifest();
        // The manifest that first names a run allocates its number. A fold
        // takes one number for its delta, or none when the head is unchanged.
        let run_no = previous_manifest.payload().next_run_no;
        let mut runs = previous_manifest.payload().runs.clone();
        let mut next_run_no = run_no;
        if previous_manifest.payload().head_seq < head_seq {
            runs.push(MetadataRunRef {
                run_no,
                run_seq: head_seq,
                tier: RunTier::Delta,
                segments: flatten_manifest_segments(
                    build_manifest_delta_run_segments(
                        store,
                        segment_cache,
                        namespace_id,
                        previous_manifest.payload().head_seq,
                        &tail_state,
                        layouts,
                        policy,
                    )
                    .await?,
                ),
            });
            next_run_no = next_run_no_after(run_no)?;
        }
        (runs, next_run_no)
    };

    Ok(NamespaceManifestPayload {
        activity,
        manifest_no,
        head_seq,
        writer_epoch: projection.head.writer_epoch,
        next_inode_id: projection.head.next_inode_id,
        next_run_no,
        folded_wal_no: projection.head.wal_no,
        runs,
        ..projection.manifest_segments.manifest().payload().clone()
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
