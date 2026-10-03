//! Folds the visible WAL tail and publishes the next manifest number.

use super::block_load::SessionBlockMemo;
use super::build::{build_manifest_delta_run_segments, build_manifest_segments};
use super::cache::{read_working_memory, MetadataSegmentCache};
use super::load::load_basis_metadata_segments;
use super::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use super::runs::{flatten_manifest_segments, MetadataLsmPolicy};
use super::scan::VerifiedMetadataSegments;
use crate::commit::WalPublishError;
use crate::commit_engine::WalFoldInput;
use crate::control_update::{retry_while_contended, CasAttempt};
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::error::Result;
use crate::limits::CONTENTION_RETRY_LIMIT;
use crate::metadata::{MetadataState, MetadataView};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control::CurrentManifest;
use crate::namespace::read_anchor::load_read_anchor;
use crate::namespace::state::NamespaceReadState;
use crate::read_working_memory::ReadWorkingMemory;
use crate::storage::content::{
    content_object_key_for_ref, materialize_content, validate_loaded_content_bytes,
};
use crate::store_waves::STORE_WRITE_WAVE;
use crate::time::Deadline;
use crate::wal::replay_discovered_tail;
use crate::wal::ProjectedWalTail;
use futures::{stream, TryStreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::ManifestRef;
use loonfs_types::format::manifest::{MetadataRunRef, NamespaceManifestPayload, RunTier};
use loonfs_types::{
    ChangeSeq, FoldWalOutcome, FoldWalResponse, ManifestNo, NamespaceId, RunNo, MAX_PUBLIC_INTEGER,
};
use std::sync::Arc;
use tracing::Instrument;

/// Manifest that covers the head after a fold attempt.
///
/// This may be a newly published manifest or one that was already current.
pub(crate) struct FoldedBasis {
    /// Reference to the manifest that covers the head.
    pub(crate) manifest: ManifestRef,
    /// Head sequence the attempt targeted.
    pub(super) target_head_seq: ChangeSeq,
    /// Current manifest after the attempt.
    pub(super) current_manifest_no: ManifestNo,
    /// Sequence covered by `current_manifest_no`.
    pub(super) current_manifest_head_seq: ChangeSeq,
    pub(super) outcome: FoldWalOutcome,
}

pub(crate) enum TryFoldWal {
    /// The attempt finished with a valid basis, whether or not it published
    /// that basis itself.
    Settled(Box<FoldedBasis>),
    /// A concurrent manifest publication does not cover this attempt's
    /// target. It carries the current manifest the attempt lost to.
    RaceLost(CurrentManifest),
}

/// Folds the visible WAL tail into segments and publishes the next manifest.
#[cfg(test)]
pub(crate) async fn fold_wal<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Result<FoldWalResponse> {
    let deadline = Deadline::start(Arc::new(crate::time::StdMonotonicTimer::default()));
    fold_wal_with_deadline(
        store,
        namespace_id,
        &deadline,
        MetadataLsmPolicy::default(),
        Arc::default(),
    )
    .await
}

pub(crate) async fn fold_wal_with_deadline<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    deadline: &Deadline,
    policy: MetadataLsmPolicy,
    pool: Arc<ReadWorkingMemory>,
) -> Result<FoldWalResponse> {
    let basis = fold_wal_basis_with_deadline(store, namespace_id, deadline, policy, &pool).await?;
    Ok(fold_wal_response(namespace_id, basis))
}

async fn fold_wal_basis_with_deadline<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    deadline: &Deadline,
    policy: MetadataLsmPolicy,
    pool: &Arc<ReadWorkingMemory>,
) -> Result<FoldedBasis> {
    retry_while_contended(|| async move {
        // The fallback reloads after every lost race, so it never relies on
        // the rule that keeps a held tail.
        let mut projection = load_fold_projection(store, namespace_id, None).await?;
        projection.manifest_segments.block_memo = SessionBlockMemo::new(Arc::clone(pool));
        Result::Ok(
            match try_fold_wal_projection(store, namespace_id, &projection, deadline, policy)
                .await?
            {
                TryFoldWal::Settled(basis) => CasAttempt::Settled(*basis),
                TryFoldWal::RaceLost(_) => {
                    CasAttempt::Contended(CoreError::WalPublish(WalPublishError::NumberTaken))
                }
            },
        )
    })
    .await?
}

/// Loads the namespace and folds its tail, keeping that tail across
/// manifests that fold no WAL (see `fold_held_projection`).
pub(crate) async fn try_fold_wal<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    deadline: &Deadline,
    policy: MetadataLsmPolicy,
    segment_cache: Option<&MetadataSegmentCache>,
) -> Result<TryFoldWal> {
    let projection = load_fold_projection(store, namespace_id, segment_cache).await?;
    fold_held_projection(
        store,
        segment_cache,
        namespace_id,
        projection,
        deadline,
        policy,
    )
    .await
}

async fn load_fold_projection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    segment_cache: Option<&'a MetadataSegmentCache>,
) -> Result<ManifestProjection<'a, S>> {
    load_manifest_projection(store, namespace_id, segment_cache)
        .instrument(tracing::debug_span!(
            "loonfs.phase",
            phase = "scan_namespace_state"
        ))
        .await
}

/// Folds a projection the caller already holds. When a manifest that folded
/// no WAL past the projection's basis takes the fold's number, the fold
/// takes that manifest as its basis and publishes again from the same tail,
/// up to the contention limit. Any other lost race is returned.
async fn fold_held_projection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    segment_cache: Option<&'a MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    mut projection: ManifestProjection<'a, S>,
    deadline: &Deadline,
    policy: MetadataLsmPolicy,
) -> Result<TryFoldWal> {
    let mut attempt =
        try_fold_wal_projection(store, namespace_id, &projection, deadline, policy).await?;
    for _retry in 0..CONTENTION_RETRY_LIMIT {
        let TryFoldWal::RaceLost(winner) = &attempt else {
            break;
        };
        if !keeps_held_tail(
            projection.manifest_segments.manifest().payload(),
            winner.envelope.payload(),
        ) {
            break;
        }
        projection.basis = MetadataBasis(winner.manifest());
        projection.manifest_segments =
            load_basis_metadata_segments(store, segment_cache, &projection.basis)
                .await?
                .segments;
        attempt = publish_fold(store, namespace_id, &projection, deadline, policy).await?;
    }
    Ok(attempt)
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
    // - Inline content: the first attempt wrote it, and nothing deletes the
    //   content of a live namespace. A deleted winner differs in `status`.
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

async fn try_fold_wal_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &ManifestProjection<'_, S>,
    deadline: &Deadline,
    policy: MetadataLsmPolicy,
) -> Result<TryFoldWal> {
    let head_seq = projection.head.seq;
    let basis_manifest = projection.basis.manifest();
    if projection
        .manifest_segments
        .manifest()
        .payload()
        .folded_wal_no
        == projection.head.wal_no
    {
        return Ok(TryFoldWal::Settled(Box::new(FoldedBasis {
            manifest: basis_manifest.clone(),
            target_head_seq: head_seq,
            current_manifest_no: basis_manifest.manifest_no,
            current_manifest_head_seq: head_seq,
            outcome: FoldWalOutcome::AlreadyCurrent,
        })));
    }

    materialize_inline_content(store, projection).await?;
    publish_fold(store, namespace_id, projection, deadline, policy).await
}

async fn publish_fold<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &ManifestProjection<'_, S>,
    deadline: &Deadline,
    policy: MetadataLsmPolicy,
) -> Result<TryFoldWal> {
    deadline.ensure_metadata_publication_budget(namespace_id)?;
    let manifest_no = next_manifest_no_after(projection.basis.manifest_no())?;
    let manifest = build_namespace_manifest_for_projection(
        store,
        namespace_id,
        projection,
        manifest_no,
        policy,
    )
    .await?;
    let manifest = encode_manifest(manifest)?;
    // Written segments may outlive the GC grace if publication exceeds its budget.
    deadline.ensure_metadata_publication_budget(namespace_id)?;
    let (outcome, current) = match publish_manifest(store, manifest, deadline).await? {
        ManifestPublicationOutcome::Published(current) => (FoldWalOutcome::Published, current),
        ManifestPublicationOutcome::CoveredByCurrent(current) => {
            (FoldWalOutcome::ManifestAdvanced, current)
        }
        // A same-sequence compaction can replace the predecessor without
        // covering the newer WAL head. That manifest wins, but it has not
        // satisfied the fold.
        ManifestPublicationOutcome::PredecessorChanged(current) => {
            return Ok(TryFoldWal::RaceLost(current));
        }
    };
    Ok(TryFoldWal::Settled(Box::new(FoldedBasis {
        current_manifest_no: current.manifest().manifest_no,
        current_manifest_head_seq: current.manifest().head_seq,
        manifest: current.manifest(),
        target_head_seq: projection.head.seq,
        outcome,
    })))
}

async fn materialize_inline_content<S: ObjectStore + ?Sized>(
    store: &S,
    projection: &ManifestProjection<'_, S>,
) -> Result<()> {
    let values = projection
        .tail_state
        .inline_values()
        .map(|value| {
            let key = content_object_key_for_ref(&value.content_ref)?;
            validate_loaded_content_bytes(key.clone(), &value.content_ref, &value.bytes)?;
            Ok((key, value))
        })
        .collect::<Result<Vec<_>>>()?;
    // In a live namespace, failed folds leave committed content for the next fold.
    stream::iter(values.into_iter().map(Ok))
        .try_for_each_concurrent(STORE_WRITE_WAVE, |(object_key, value)| async move {
            materialize_content(store, &object_key, &value.content_ref, value.bytes.clone()).await
        })
        .await
}

pub struct FoldedWalTail {
    pub response: FoldWalResponse,
    /// The manifest that covers the tail after the call: the one it published,
    /// or the current one that already covered it.
    pub basis: MetadataBasis,
}

pub async fn fold_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    input: Option<WalFoldInput>,
    deadline: &Deadline,
) -> Result<FoldedWalTail> {
    let policy = MetadataLsmPolicy::default();
    let pool = read_working_memory(segment_cache);
    let folded = if let Some(input) = input {
        let loaded_basis = load_basis_metadata_segments(store, segment_cache, &input.basis).await?;
        let manifest_projection = ManifestProjection {
            head: input.head,
            basis: input.basis,
            manifest_segments: loaded_basis.segments,
            tail_state: input.tail_state,
        };
        // A fold publishes metadata without updating the namespace head.
        match fold_held_projection(
            store,
            segment_cache,
            namespace_id,
            manifest_projection,
            deadline,
            policy,
        )
        .await?
        {
            TryFoldWal::Settled(basis) => *basis,
            TryFoldWal::RaceLost(_) => {
                fold_wal_basis_with_deadline(store, namespace_id, deadline, policy, &pool).await?
            }
        }
    } else {
        fold_wal_basis_with_deadline(store, namespace_id, deadline, policy, &pool).await?
    };
    Ok(FoldedWalTail {
        basis: MetadataBasis(folded.manifest.clone()),
        response: fold_wal_response(namespace_id, folded),
    })
}

fn fold_wal_response(namespace_id: &NamespaceId, basis: FoldedBasis) -> FoldWalResponse {
    FoldWalResponse {
        namespace_id: namespace_id.clone(),
        target_head_seq: basis.target_head_seq,
        manifest_no: basis.current_manifest_no,
        manifest_head_seq: basis.current_manifest_head_seq,
        outcome: basis.outcome,
    }
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
    pub(super) async fn tail_with_deletion_inodes(
        &self,
    ) -> Result<std::borrow::Cow<'_, MetadataState>> {
        let mut tail = std::borrow::Cow::Borrowed(&self.tail_state.rows);
        let view = MetadataView::from_loaded_head(
            &self.head,
            &self.manifest_segments,
            &self.tail_state.rows,
        );
        for tombstone in self.tail_state.rows.subtree_tombstones() {
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
            tail.to_mut().push_inode_record(inode);
        }
        Ok(tail)
    }
}

pub(super) async fn load_manifest_projection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
    segment_cache: Option<&'a MetadataSegmentCache>,
) -> Result<ManifestProjection<'a, S>> {
    let anchor = load_read_anchor(store, namespace_id)
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
        CoreError::Internal(format!(
            "manifest number cannot exceed {MAX_PUBLIC_INTEGER}"
        ))
    })
}

/// Advances the manifest's run allocator after a producer has taken
/// `current`.
pub fn next_run_no_after(current: RunNo) -> Result<RunNo> {
    current
        .successor()
        .map_err(|error| CoreError::Internal(format!("run number {error}")))
}

async fn build_namespace_manifest_for_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &ManifestProjection<'_, S>,
    manifest_no: ManifestNo,
    policy: MetadataLsmPolicy,
) -> Result<NamespaceManifestPayload> {
    let activity = projection
        .manifest_segments
        .manifest()
        .payload()
        .activity
        .checked_add(projection.tail_state.activity)
        .ok_or_else(|| {
            CoreError::NamespaceCorrupt(
                "activity counter cannot exceed 9007199254740991".to_owned(),
            )
        })?;
    let head_seq = projection.head.seq;
    let tail_state = projection.tail_with_deletion_inodes().await?;
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
                    build_manifest_segments(store, namespace_id, &tail_state, policy).await?,
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
                        namespace_id,
                        previous_manifest.payload().head_seq,
                        &tail_state,
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

    #[test]
    fn manifest_no_advancement_accepts_the_maximum_and_rejects_the_next_value() {
        assert_eq!(
            next_manifest_no_after(ManifestNo(MAX_PUBLIC_INTEGER - 1))
                .expect("advance to public maximum"),
            ManifestNo(MAX_PUBLIC_INTEGER)
        );

        let error = next_manifest_no_after(ManifestNo(MAX_PUBLIC_INTEGER))
            .expect_err("manifest number must not exceed the public maximum");
        assert!(matches!(
            error,
            CoreError::Internal(message) if message.contains("cannot exceed")
        ));
    }
}

#[cfg(test)]
#[path = "tests/segment_puts.rs"]
mod segment_puts;
