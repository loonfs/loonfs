//! Flushes the visible WAL tail into metadata segments, publishes a manifest
//! for the current head, and advances `metadata/root.json` by compare-and-swap.
//! This does not create a checkpoint record.
//!
//! This is the latest-state maintenance path: superseded manifests become
//! garbage-collection candidates once nothing pins them. Pinning a manifest
//! version for retention is a separate concern layered on top by
//! [`create`](super::create).

use super::build::{build_manifest_delta_run_segments, build_manifest_segments};
use super::load::{head_from_manifest, load_basis_metadata_segments};
use super::publish::{
    manifest_ref_for, manifest_write_failure, publish_metadata_root, write_namespace_manifest,
    ManifestPublicationOutcome,
};
use super::runs::{flatten_manifest_segments, MetadataLsmPolicy, CHECKPOINT_BASE_RUN_LEVEL};
use super::scan::VerifiedMetadataSegments;
use crate::commit::CommitHeadPublishError;
use crate::context::MutationContext;
use crate::control_update::{retry_while_contended, CasAttempt};
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::error::Result;
use crate::limits::METADATA_PUBLICATION_BUDGET_MS;
use crate::metadata::MetadataState;
use crate::namespace::basis::{
    advanced_floor_without_root, load_head_and_metadata_basis, namespace_birth_seq,
    resolve_retention_floor_seq, MetadataBasis,
};
use crate::time::{MonotonicTimer, StdMonotonicTimer};
use crate::wal::{
    ensure_replayed_head_matches, load_validated_wal_chain, project_validated_wal_tail,
    WalChainLoadRequest,
};
use loonfs_api::wire::control::{HeadState, ManifestRef, NamespaceStatus};
use loonfs_api::wire::manifest::{NamespaceManifestEnvelope, NamespaceManifestPayload};
use loonfs_api::{
    next_public_ordinal, ChangeSeq, CommitId, FlushWalOutcome, FlushWalResponse, ManifestNo,
    ManifestObjectId, NamespaceId, RunNo, MAX_PUBLIC_INTEGER,
};
use loonfs_objectstore::ObjectStore;
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
    /// Manifest referenced by `metadata/root.json` after the attempt.
    pub(super) root_after_manifest_no: ManifestNo,
    /// Sequence covered by `root_after_manifest_no`.
    pub(super) root_after_head_seq: ChangeSeq,
    pub(super) outcome: FlushWalOutcome,
}

pub(super) enum TryFlushWal {
    /// The attempt finished with a valid basis, whether or not it published
    /// that basis itself.
    Settled(Box<FlushedBasis>),
    /// The root compare-and-swap raced another publisher to exhaustion;
    /// retry against a fresh projection.
    RaceLost,
}

/// Flushes the visible WAL tail into metadata segments and advances
/// `metadata/root.json` to a manifest covering the current head.
///
/// The WAL delta lands as one new delta run when the root lags the head; the
/// manifest publishes and the root advances. No checkpoint record is
/// created. Returns `StaleHead` when every attempt lost the root race.
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
    let flushed = retry_while_contended(|| async move {
        Result::Ok(
            match try_flush_wal(store, namespace_id, context, timer).await? {
                TryFlushWal::Settled(basis) => CasAttempt::Settled(FlushWalResponse {
                    namespace_id: namespace_id.clone(),
                    target_head_seq: basis.target_head_seq,
                    manifest_no: basis.root_after_manifest_no,
                    manifest_head_seq: basis.root_after_head_seq,
                    outcome: basis.outcome,
                }),
                TryFlushWal::RaceLost => CasAttempt::Contended,
            },
        )
    })
    .await?;
    flushed.ok_or(CoreError::HeadPublish(CommitHeadPublishError::StaleHead))
}

/// One flush attempt against one fresh projection.
///
/// The metadata publication budget covers this attempt end to end: the
/// measurement starts before any segment object is written and gates the root
/// compare-and-swap, so an over-budget build aborts with only unreachable
/// immutable outputs behind it.
pub(super) async fn try_flush_wal<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
    timer: &dyn MonotonicTimer,
) -> Result<TryFlushWal> {
    let publication_started_ms = timer.monotonic_now_ms();
    let projection = load_root_projection(store, namespace_id)
        .instrument(tracing::debug_span!(
            "loonfs.phase",
            phase = "scan_namespace_state"
        ))
        .await?;
    let head_seq = projection.head.seq;
    let basis_manifest_no = projection.basis.manifest_no();
    // Only a manifest this namespace published can already cover the head.
    // A genesis or fork basis must be materialized here even at an
    // unchanged head, because the namespace owns no manifest yet and a
    // checkpoint record can pin only its own.
    let basis_manifest = projection.basis.manifest();
    if projection.basis.is_owned_by(namespace_id)
        && projection.manifest_segments.manifest().payload.head_seq == head_seq
    {
        let basis_manifest = basis_manifest.expect("an owned basis names a manifest");
        return Ok(TryFlushWal::Settled(Box::new(FlushedBasis {
            manifest: basis_manifest.clone(),
            head_commit_id: projection.head.head_commit_id.clone(),
            target_head_seq: head_seq,
            root_after_manifest_no: basis_manifest.manifest_no,
            root_after_head_seq: head_seq,
            outcome: FlushWalOutcome::AlreadyCurrent,
        })));
    }

    // One generated object id, one write. The generated id ends in 16 random
    // hex characters, so the key is this attempt's alone and a conflict under
    // it is corruption rather than contention.
    let manifest_no = next_manifest_no_after(basis_manifest_no)?;
    let manifest = build_namespace_manifest_for_projection(
        store,
        namespace_id,
        &projection,
        manifest_no,
        ManifestObjectId::generate(manifest_no),
    )
    .await?;
    write_namespace_manifest(store, &manifest)
        .await
        .map_err(manifest_write_failure)?;
    // The publication budget gates the root compare-and-swap: past it, the
    // written segments and manifest may have aged into the GC grace window,
    // so this attempt must abort without publishing (format spec, "Garbage
    // collection", rule 1). The orphans are reclaimed by a later pass.
    ensure_metadata_publication_budget(timer, publication_started_ms, namespace_id)?;
    // Advance the root. If another publisher updates it first, this attempt's
    // manifest remains valid but unreferenced.
    let (outcome, root_after_manifest_no, root_after_head_seq) = match publish_metadata_root(
        store,
        namespace_id,
        &manifest,
        projection.basis.is_owned_by(namespace_id).then(|| {
            projection
                .manifest_segments
                .manifest()
                .payload
                .manifest_object_id
                .clone()
        }),
        context.now_ms,
    )
    .await?
    {
        ManifestPublicationOutcome::Published(_) => (
            FlushWalOutcome::Published,
            manifest.payload.manifest_no,
            manifest.payload.head_seq,
        ),
        ManifestPublicationOutcome::Superseded(current) => (
            FlushWalOutcome::RootAdvanced,
            current.manifest.manifest_no,
            current.manifest.manifest_head_seq,
        ),
        ManifestPublicationOutcome::RootCasRaceLost => {
            return Ok(TryFlushWal::RaceLost);
        }
    };
    Ok(TryFlushWal::Settled(Box::new(FlushedBasis {
        manifest: manifest_ref_for(namespace_id, &manifest),
        head_commit_id: projection.head.head_commit_id.clone(),
        target_head_seq: head_seq,
        root_after_manifest_no,
        root_after_head_seq,
        outcome,
    })))
}

pub(super) struct RootProjection<'a, S: ObjectStore + ?Sized> {
    pub(super) head: HeadState,
    pub(super) basis: MetadataBasis,
    pub(super) floor_seq: ChangeSeq,
    pub(super) manifest_segments: VerifiedMetadataSegments<'a, S>,
    /// Rows that are not in any segment yet: the genesis root inode when the
    /// basis is genesis, plus the replayed WAL tail.
    pub(super) tail_state: MetadataState,
}

pub(super) async fn load_root_projection<'a, S: ObjectStore + ?Sized>(
    store: &'a S,
    namespace_id: &NamespaceId,
) -> Result<RootProjection<'a, S>> {
    let loaded = load_head_and_metadata_basis(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    let head = loaded.head.state;
    if head.status == (NamespaceStatus::Deleted {}) {
        return Err(CoreError::MetadataProjection(
            MetadataProjectionLoadError::NamespaceDeleted {
                namespace_id: namespace_id.clone(),
            },
        ));
    }
    let floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    // The floor never advances past the materialized root, so a floor above
    // the namespace's birth sequence with no root of its own means the root
    // was lost.
    if !loaded.basis.is_owned_by(namespace_id) && floor_seq > namespace_birth_seq(&head) {
        return Err(advanced_floor_without_root(namespace_id, floor_seq));
    }
    let basis =
        load_basis_metadata_segments(store, None, namespace_id, &loaded.basis, head.created_at_ms)
            .await?;
    let manifest_segments = basis.segments;
    let manifest_head = head_from_manifest(&head, manifest_segments.manifest());
    let wal_chain = load_validated_wal_chain(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: manifest_head.seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: None,
            recent_segments: &head.recent_segments,
        },
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    let replayed = {
        let _span =
            tracing::debug_span!("loonfs.phase", phase = "project_metadata_state").entered();
        project_validated_wal_tail(
            &manifest_head,
            &basis.base_state,
            Some(head.writer_epoch),
            &wal_chain,
        )
        .map_err(MetadataProjectionLoadError::WalReplay)
        .map_err(CoreError::MetadataProjection)?
    };
    ensure_replayed_head_matches(&head, &replayed.resulting_head)?;
    Ok(RootProjection {
        head,
        basis: loaded.basis,
        floor_seq,
        manifest_segments,
        tail_state: replayed.resulting_metadata_state,
    })
}

pub(super) fn next_manifest_no_after(current: ManifestNo) -> Result<ManifestNo> {
    next_public_ordinal(current.0)
        .map(ManifestNo)
        .ok_or_else(|| {
            CoreError::Internal(format!(
                "manifest number cannot exceed {MAX_PUBLIC_INTEGER}"
            ))
        })
}

/// Advances the manifest's run allocator after a producer has taken
/// `current`.
pub(super) fn next_run_no_after(current: RunNo) -> Result<RunNo> {
    next_public_ordinal(current.0).map(RunNo).ok_or_else(|| {
        CoreError::Internal(format!("run number cannot exceed {MAX_PUBLIC_INTEGER}"))
    })
}

/// Refuses to initiate a root compare-and-swap once the publication budget
/// is spent (format spec, "Garbage collection", rule 1).
pub(super) fn ensure_metadata_publication_budget(
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
        "metadata publication overran its budget; aborting before the root compare-and-swap",
    );
    Err(CoreError::MetadataPublicationBudgetExceeded {
        elapsed_ms,
        budget_ms: METADATA_PUBLICATION_BUDGET_MS,
    })
}

async fn build_namespace_manifest_for_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &RootProjection<'_, S>,
    manifest_no: ManifestNo,
    manifest_object_id: ManifestObjectId,
) -> Result<NamespaceManifestEnvelope> {
    let head_seq = projection.head.seq;
    let previous_manifest = projection.manifest_segments.manifest();

    // The manifest that first names a run allocates its number. A flush writes
    // at most one run, so it takes at most one number, and a flush that writes
    // none leaves the allocator where the previous manifest left it.
    let run_no = previous_manifest.payload.next_run_no;

    // A WAL flush keeps existing runs and writes the WAL delta as one new delta
    // run. Reorganization merges delta runs into the base separately.
    //
    // The genesis basis is the exception: it has no run to extend and its
    // one root-inode row sits at sequence zero, which no delta run above
    // that sequence would carry. The namespace's first manifest is
    // therefore one complete base run over the whole projected state.
    let (base_seq, segments, next_run_no) = if matches!(projection.basis, MetadataBasis::Genesis) {
        (
            head_seq,
            flatten_manifest_segments(
                build_manifest_segments(
                    store,
                    namespace_id,
                    run_no,
                    head_seq,
                    CHECKPOINT_BASE_RUN_LEVEL,
                    &projection.tail_state,
                    MetadataLsmPolicy::default().max_rows_per_segment,
                )
                .await?,
            ),
            next_run_no_after(run_no)?,
        )
    } else {
        let mut segments = previous_manifest.payload.segments.clone();
        let mut next_run_no = run_no;
        if previous_manifest.payload.head_seq < head_seq {
            segments.extend(flatten_manifest_segments(
                build_manifest_delta_run_segments(
                    store,
                    namespace_id,
                    run_no,
                    head_seq,
                    previous_manifest.payload.head_seq,
                    &projection.tail_state,
                )
                .await?,
            ));
            next_run_no = next_run_no_after(run_no)?;
        }
        (previous_manifest.payload.base_seq, segments, next_run_no)
    };

    NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
        namespace_id: namespace_id.clone(),
        manifest_no,
        manifest_object_id,
        head_seq,
        head_commit_id: projection.head.head_commit_id.clone(),
        base_seq,
        writer_epoch: projection.head.writer_epoch,
        next_inode_id: projection.head.next_inode_id,
        next_run_no,
        retention_floor_seq: projection.floor_seq,
        segments,
    })
    .map_err(|err| {
        CoreError::Internal(format!(
            "failed to build namespace manifest envelope: {err}"
        ))
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
