//! Flushes the visible WAL tail and publishes the next manifest number.

use super::build::{build_manifest_delta_run_segments, build_manifest_segments};
use super::cache::MetadataSegmentCache;
use super::load::load_basis_metadata_segments;
use super::publish::{encode_manifest, publish_manifest, ManifestPublicationOutcome};
use super::runs::{flatten_manifest_segments, MetadataLsmPolicy};
use super::scan::VerifiedMetadataSegments;
use crate::commit::WalPublishError;
use crate::commit_engine::WalFoldInput;
use crate::control_update::{retry_while_contended, CasAttempt, WriteEvidence};
use crate::error::CoreError;
use crate::error::MetadataProjectionLoadError;
use crate::error::Result;
use crate::metadata::{MetadataState, MetadataView};
use crate::namespace::basis::MetadataBasis;
use crate::namespace::read_anchor::load_read_anchor;
use crate::namespace::state::NamespaceReadState;
use crate::storage::content::{
    content_object_key_for_ref, materialize_content, validate_loaded_content_bytes,
};
use crate::time::{Deadline, StdMonotonicTimer};
use crate::wal::load_replayed_wal_tail;
use crate::wal::ProjectedWalTail;
use futures::{stream, TryStreamExt};
use loonfs_api::wire::control::ManifestRef;
use loonfs_api::wire::manifest::{MetadataRunRef, NamespaceManifestPayload, RunTier};
use loonfs_api::{
    ChangeSeq, FlushWalOutcome, FlushWalResponse, ManifestNo, NamespaceId, RunNo,
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
) -> Result<FlushWalResponse> {
    let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
    flush_wal_with_deadline(store, namespace_id, &deadline).await
}

pub(crate) async fn flush_wal_with_deadline<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    deadline: &Deadline,
) -> Result<FlushWalResponse> {
    retry_while_contended(
        || async move {
            Result::Ok(match try_flush_wal(store, namespace_id, deadline).await? {
                TryFlushWal::Settled(basis) => {
                    CasAttempt::Settled(flush_wal_response(namespace_id, *basis))
                }
                TryFlushWal::RaceLost => {
                    CasAttempt::Contended(CoreError::WalPublish(WalPublishError::StaleHead))
                }
            })
        },
        |_, ()| async { Ok(WriteEvidence::Unknown) },
    )
    .await?
}

pub(super) async fn try_flush_wal<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    deadline: &Deadline,
) -> Result<TryFlushWal> {
    let projection = load_manifest_projection(store, namespace_id)
        .instrument(tracing::debug_span!(
            "loonfs.phase",
            phase = "scan_namespace_state"
        ))
        .await?;
    try_flush_wal_projection(store, namespace_id, &projection, deadline).await
}

async fn try_flush_wal_projection<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    projection: &ManifestProjection<'_, S>,
    deadline: &Deadline,
) -> Result<TryFlushWal> {
    let head_seq = projection.head.seq;
    let basis_manifest_no = projection.basis.manifest_no();
    let basis_manifest = projection.basis.manifest();
    if projection
        .manifest_segments
        .manifest()
        .payload()
        .folded_wal_no
        == projection.head.wal_no
    {
        return Ok(TryFlushWal::Settled(Box::new(FlushedBasis {
            manifest: basis_manifest.clone(),
            target_head_seq: head_seq,
            current_manifest_no: basis_manifest.manifest_no,
            current_manifest_head_seq: head_seq,
            outcome: FlushWalOutcome::AlreadyCurrent,
        })));
    }

    materialize_inline_content(store, projection).await?;
    deadline.ensure_metadata_publication_budget(namespace_id)?;
    let manifest_no = next_manifest_no_after(basis_manifest_no)?;
    let manifest =
        build_namespace_manifest_for_projection(store, namespace_id, projection, manifest_no)
            .await?;
    let manifest = encode_manifest(manifest)?;
    // Written segments may outlive the GC grace if publication exceeds its budget.
    deadline.ensure_metadata_publication_budget(namespace_id)?;
    let (outcome, current) = match publish_manifest(store, manifest, deadline).await? {
        ManifestPublicationOutcome::Published(current) => (FlushWalOutcome::Published, current),
        ManifestPublicationOutcome::CoveredByCurrent(current) => {
            (FlushWalOutcome::ManifestAdvanced, current)
        }
        // A same-sequence reorganization can replace the predecessor without
        // covering the newer WAL head. That manifest wins, but it has not
        // satisfied the flush: reload its runs, replay the tail, try again.
        ManifestPublicationOutcome::PredecessorChanged(_) => {
            return Ok(TryFlushWal::RaceLost);
        }
    };
    Ok(TryFlushWal::Settled(Box::new(FlushedBasis {
        current_manifest_no: current.manifest().manifest_no,
        current_manifest_head_seq: current.manifest().head_seq,
        manifest: current.manifest(),
        target_head_seq: head_seq,
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
    // In a live namespace, failed flushes leave committed content for the next flush.
    stream::iter(values.into_iter().map(Ok))
        .try_for_each_concurrent(32, |(object_key, value)| async move {
            materialize_content(store, &object_key, &value.content_ref, value.bytes.clone()).await
        })
        .await
}

pub async fn fold_wal_tail<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    input: Option<WalFoldInput>,
    deadline: &Deadline,
) -> Result<FlushWalResponse> {
    let Some(input) = input else {
        return flush_wal_with_deadline(store, namespace_id, deadline).await;
    };
    let loaded_basis = load_basis_metadata_segments(store, segment_cache, &input.basis).await?;
    let manifest_projection = ManifestProjection {
        head: input.head,
        basis: input.basis,
        manifest_segments: loaded_basis.segments,
        tail_state: input.tail_state,
    };
    // A fold publishes metadata without updating the namespace head.
    match try_flush_wal_projection(store, namespace_id, &manifest_projection, deadline).await? {
        TryFlushWal::Settled(basis) => Ok(flush_wal_response(namespace_id, *basis)),
        TryFlushWal::RaceLost => flush_wal_with_deadline(store, namespace_id, deadline).await,
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
    let loaded_basis = load_basis_metadata_segments(store, None, &basis).await?;
    let manifest_head = loaded_basis.replay_head(&head);
    let manifest_segments = loaded_basis.segments;
    let replayed = load_replayed_wal_tail(store, &manifest_head, &head, &loaded_basis.base_state)
        .await
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
    // A WAL flush keeps existing runs and writes the WAL delta as one new delta
    // run. Reorganization merges delta runs into the base separately.
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
                        namespace_id,
                        &tail_state,
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
                        &tail_state,
                        MetadataLsmPolicy::default(),
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
