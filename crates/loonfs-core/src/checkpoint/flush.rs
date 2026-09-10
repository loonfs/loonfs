//! Flushes the visible WAL tail and publishes the next manifest number.

use super::build::{build_manifest_delta_run_segments, build_manifest_segments};
use super::cache::MetadataSegmentCache;
use super::load::load_basis_metadata_segments;
use super::publish::{
    encode_manifest, manifest_ref_for, publish_manifest, ManifestPublicationOutcome,
};
use super::runs::{flatten_manifest_segments, MetadataLsmPolicy};
use super::scan::VerifiedMetadataSegments;
use crate::commit::WalPublishError;
use crate::commit_engine::WalFoldInput;
use crate::context::MutationContext;
use crate::control_update::{retry_while_contended, CasAttempt, WriteEvidence};
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::error::Result;
use crate::limits::METADATA_PUBLICATION_BUDGET_MS;
use crate::metadata::MetadataState;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control::load_current_manifest;
use crate::namespace::read_anchor::{load_read_anchor, resolve_retention_floor_seq};
use crate::namespace::state::NamespaceReadState;
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use crate::wal::{
    ensure_replayed_head_matches, load_wal_tail, project_validated_wal_tail, WalTailLoadRequest,
};
use loonfs_api::wire::control::ManifestRef;
use loonfs_api::wire::manifest::{MetadataRunRef, NamespaceManifestPayload, RunTier};
use loonfs_api::{
    ChangeSeq, CommitId, FlushWalOutcome, FlushWalResponse, ManifestNo, NamespaceId, RunNo,
    MAX_PUBLIC_INTEGER,
};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;
use tracing::Instrument;

/// Manifest that covers the head after a flush attempt.
///
/// This may be a newly published manifest or one that was already current.
pub(super) struct FlushedBasis {
    /// Reference to the manifest that covers the head.
    pub(super) manifest: ManifestRef,
    /// Head commit the basis covers.
    pub(super) head_commit_id: CommitId,
    /// Head sequence the attempt targeted.
    pub(super) target_head_seq: ChangeSeq,
    /// Current manifest after the attempt.
    pub(super) current_manifest_no: ManifestNo,
    /// Sequence covered by `current_manifest_no`.
    pub(super) current_manifest_head_seq: ChangeSeq,
    pub(super) outcome: FlushWalOutcome,
}

pub(super) enum TryFlushWal {
    /// The attempt finished with a valid basis, whether or not it published
    /// that basis itself.
    Settled(Box<FlushedBasis>),
    /// A concurrent manifest publication does not cover this attempt's
    /// target; retry against a fresh projection.
    RaceLost,
}

/// Flushes the visible WAL tail into segments and publishes the next manifest.
pub(crate) async fn flush_wal<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<FlushWalResponse> {
    let timer = StdMonotonicTimer::default();
    flush_wal_with_timer(store, namespace_id, context, &timer).await
}

pub(super) async fn flush_wal_with_timer<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    timer: &dyn MonotonicTimer,
) -> Result<FlushWalResponse> {
    retry_while_contended(
        || async move {
            Result::Ok(
                match try_flush_wal(store, namespace_id, context, timer).await? {
                    TryFlushWal::Settled(basis) => {
                        CasAttempt::Settled(flush_wal_response(namespace_id, *basis))
                    }
                    TryFlushWal::RaceLost => {
                        CasAttempt::Contended(CoreError::WalPublish(WalPublishError::StaleHead))
                    }
                },
            )
        },
        |_, ()| async { Ok(WriteEvidence::Unknown) },
    )
    .await?
}

/// One flush attempt against one fresh projection.
///
/// The metadata publication budget covers this attempt end to end: the
/// measurement starts before any segment object is written and gates the manifest
/// put-if-absent, so an over-budget build aborts with only unreachable
/// immutable outputs behind it.
pub(super) async fn try_flush_wal<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    timer: &dyn MonotonicTimer,
) -> Result<TryFlushWal> {
    let projection = load_manifest_projection(store, namespace_id)
        .instrument(tracing::debug_span!(
            "loonfs.phase",
            phase = "scan_namespace_state"
        ))
        .await?;
    try_flush_wal_projection(store, namespace_id, &projection, context, timer).await
}

async fn try_flush_wal_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &ManifestProjection<'_, S>,
    _context: &MutationContext,
    timer: &dyn MonotonicTimer,
) -> Result<TryFlushWal> {
    let publication_started_ms = timer.monotonic_now_ms();
    let head_seq = projection.head.seq;
    let basis_manifest_no = projection.basis.manifest_no();
    let basis_manifest = projection.basis.manifest();
    if projection
        .manifest_segments
        .manifest()
        .payload()
        .last_folded_wal_no
        == projection.head.wal_no
    {
        return Ok(TryFlushWal::Settled(Box::new(FlushedBasis {
            manifest: basis_manifest.clone(),
            head_commit_id: projection.head.head_commit_id.clone(),
            target_head_seq: head_seq,
            current_manifest_no: basis_manifest.manifest_no,
            current_manifest_head_seq: head_seq,
            outcome: FlushWalOutcome::AlreadyCurrent,
        })));
    }

    let manifest_no = next_manifest_no_after(basis_manifest_no)?;
    let manifest =
        build_namespace_manifest_for_projection(store, namespace_id, projection, manifest_no)
            .await?;
    let manifest = encode_manifest(manifest)?;
    // Written segments may outlive the GC grace if publication exceeds its budget.
    ensure_metadata_publication_budget(timer, publication_started_ms, namespace_id)?;
    let (outcome, current) = match publish_manifest(
        store,
        namespace_id,
        &manifest,
        Some(basis_manifest_no),
        timer,
        publication_started_ms,
    )
    .await?
    {
        ManifestPublicationOutcome::Published(current) => (FlushWalOutcome::Published, current),
        ManifestPublicationOutcome::CoveredByCurrent(current) => {
            (FlushWalOutcome::RootAdvanced, current)
        }
        // A same-sequence reorganization can replace the predecessor without
        // covering the newer WAL head. That manifest wins, but it has not
        // satisfied the flush: reload its runs, replay the tail, try again.
        ManifestPublicationOutcome::PredecessorChanged(_) => {
            return Ok(TryFlushWal::RaceLost);
        }
        ManifestPublicationOutcome::Installable => return Ok(TryFlushWal::RaceLost),
    };
    let head_commit_id = if current.manifest == manifest_ref_for(namespace_id, &manifest) {
        manifest.payload().head_commit_id.clone()
    } else {
        let winner = super::load::load_namespace_manifest_envelope(
            store,
            namespace_id,
            &current.manifest.manifest_no,
        )
        .await
        .map_err(MetadataProjectionLoadError::ManifestLoad)?;
        super::load::ensure_manifest_reference_matches(
            "published manifest",
            &current.manifest,
            &winner,
        )?;
        winner.payload().head_commit_id.clone()
    };
    Ok(TryFlushWal::Settled(Box::new(FlushedBasis {
        current_manifest_no: current.manifest.manifest_no,
        current_manifest_head_seq: current.manifest.manifest_head_seq,
        manifest: current.manifest,
        head_commit_id,
        target_head_seq: head_seq,
        outcome,
    })))
}

pub async fn fold_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    input: Option<WalFoldInput>,
    context: &MutationContext,
    timer: &dyn MonotonicTimer,
) -> Result<FlushWalResponse> {
    let Some(input) = input else {
        return flush_wal(store, namespace_id, context).await;
    };
    let loaded_basis = load_basis_metadata_segments(store, segment_cache, &input.basis).await?;
    let current_manifest = load_current_manifest(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let current_basis = MetadataBasis(current_manifest.state.manifest);
    if current_basis != input.basis {
        return flush_wal(store, namespace_id, context).await;
    }
    let floor_seq = match input.retention_floor_seq {
        Some(floor_seq) => floor_seq,
        None => resolve_retention_floor_seq(store, &input.head.namespace_id)
            .await
            .map_err(CoreError::ControlObjectLoad)?,
    };
    let manifest_projection = ManifestProjection {
        head: input.head,
        basis: input.basis,
        floor_seq,
        manifest_segments: loaded_basis.segments,
        tail_state: input.tail_state,
    };
    // A fold publishes metadata without updating the namespace head.
    match try_flush_wal_projection(store, namespace_id, &manifest_projection, context, timer)
        .await?
    {
        TryFlushWal::Settled(basis) => Ok(flush_wal_response(namespace_id, *basis)),
        TryFlushWal::RaceLost => flush_wal(store, namespace_id, context).await,
    }
}

fn flush_wal_response(namespace_id: &NamespaceId, basis: FlushedBasis) -> FlushWalResponse {
    FlushWalResponse {
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
    pub(super) floor_seq: ChangeSeq,
    pub(super) manifest_segments: VerifiedMetadataSegments<'a, S>,
    /// Rows that are not in any segment yet: the genesis root inode when the
    /// basis is genesis, plus the replayed WAL tail.
    pub(super) tail_state: Arc<MetadataState>,
}

pub(super) async fn load_manifest_projection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
) -> Result<ManifestProjection<'a, S>> {
    let anchor = load_read_anchor(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let basis = anchor.basis();
    let floor_seq = anchor.retention_floor_seq;
    let head = anchor.read_state;
    if head.status.is_deleted() {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    let loaded_basis = load_basis_metadata_segments(store, None, &basis).await?;
    let manifest_head = loaded_basis.replay_head(&head);
    let manifest_segments = loaded_basis.segments;
    let wal_tail = load_wal_tail(
        store,
        WalTailLoadRequest {
            namespace_id,
            base_seq: manifest_head.seq,
            head_seq: head.seq,
            base_wal_no: head.last_folded_wal_no,
            tip_wal_no: head.wal_no,
            writer_epoch: head.writer_epoch,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalTailLoad(error))
    })?;
    let replayed = {
        let _span =
            tracing::debug_span!("loonfs.phase", phase = "project_metadata_state").entered();
        project_validated_wal_tail(
            &manifest_head,
            &loaded_basis.base_state,
            Some(head.writer_epoch),
            &wal_tail,
        )
        .map_err(MetadataProjectionLoadError::WalReplay)
        .map_err(CoreError::MetadataProjection)?
    };
    ensure_replayed_head_matches(&head, &replayed.resulting_head)?;
    Ok(ManifestProjection {
        head,
        basis,
        floor_seq,
        manifest_segments,
        tail_state: Arc::new(replayed.resulting_metadata_state),
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

/// Refuses to initiate a manifest put-if-absent once the publication budget
/// is spent (format spec, Appendix C).
pub fn ensure_metadata_publication_budget(
    timer: &dyn MonotonicTimer,
    publication_started_ms: u64,
    namespace_id: &NamespaceId,
) -> Result<()> {
    let elapsed_ms = timer
        .monotonic_now_ms()
        .saturating_sub(publication_started_ms);
    if elapsed_ms <= METADATA_PUBLICATION_BUDGET_MS {
        return Ok(());
    }
    tracing::error!(
        namespace_id = namespace_id.as_str(),
        elapsed_ms,
        budget_ms = METADATA_PUBLICATION_BUDGET_MS,
        "metadata publication overran its budget; aborting before the manifest put-if-absent",
    );
    Err(CoreError::MetadataPublicationBudgetExceeded {
        elapsed_ms,
        budget_ms: METADATA_PUBLICATION_BUDGET_MS,
    })
}

async fn build_namespace_manifest_for_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &ManifestProjection<'_, S>,
    manifest_no: ManifestNo,
) -> Result<NamespaceManifestPayload> {
    let head_seq = projection.head.seq;
    // A WAL flush keeps existing runs and writes the WAL delta as one new delta
    // run. Reorganization merges delta runs into the base separately.
    //
    let (base_seq, runs, next_run_no) = if projection
        .manifest_segments
        .manifest()
        .payload()
        .runs
        .is_empty()
    {
        let run_no = RunNo(0);
        (
            head_seq,
            vec![MetadataRunRef {
                run_no,
                run_seq: head_seq,
                tier: RunTier::Base,
                segments: flatten_manifest_segments(
                    build_manifest_segments(
                        store,
                        namespace_id,
                        &projection.tail_state,
                        MetadataLsmPolicy::default(),
                    )
                    .await?,
                ),
            }],
            next_run_no_after(run_no)?,
        )
    } else {
        let previous_manifest = projection.manifest_segments.manifest();
        // The manifest that first names a run allocates its number. A flush
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
                        &projection.tail_state,
                        MetadataLsmPolicy::default(),
                    )
                    .await?,
                ),
            });
            next_run_no = next_run_no_after(run_no)?;
        }
        (previous_manifest.payload().base_seq, runs, next_run_no)
    };

    Ok(NamespaceManifestPayload {
        manifest_no,
        head_seq,
        head_commit_id: projection.head.head_commit_id.clone(),
        base_seq,
        writer_epoch: projection.head.writer_epoch,
        next_inode_id: projection.head.next_inode_id,
        next_run_no,
        retention_floor_seq: projection.floor_seq,
        last_folded_wal_no: projection.head.wal_no,
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
