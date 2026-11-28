//! Folds a source's WAL tail into a fork's own manifest 2, so the fork has a
//! manifest at its fork point and the source publishes nothing
//! ([format spec](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#92-forking-a-namespace)).

use super::fold::ManifestProjection;
use super::load::{load_referenced_manifest, manifest_base_state, DecodedManifest};
use super::publish::ManifestPublicationOutcome;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::control::LoadedManifest;
use crate::namespace::fork::ForkDeadline;
use crate::namespace::held::Held;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::segment::runs::runs_newest_first;
use crate::segment::{KeptBy, SegmentKeeper};
use crate::time::OperationClock;
use crate::wal::{load_replayed_wal_tail_through_seq, ProjectedWalTail, WalTailLoadError};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::ForkBasis;
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{CommitSeq, WriterEpoch};
use std::sync::Arc;

/// The source's WAL tail above the source manifest that a fork is built
/// from, through the fork point.
pub(crate) struct ForkPointTail {
    /// The fork point, the head that replaying the tail reached.
    pub(crate) seq: CommitSeq,
    pub(crate) tail: Arc<ProjectedWalTail>,
}

impl ForkPointTail {
    /// Returns the tail of `projection`, the source's head view.
    pub(crate) fn from_projection<S: ObjectStore + ?Sized>(
        projection: &ManifestProjection<'_, S>,
    ) -> Self {
        Self {
            seq: projection.head().seq,
            tail: Arc::clone(&projection.tail_state),
        }
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads the WAL tail of this namespace, a fork's source, at the fork
    /// point that `fork_basis` records, for a call that finishes the fork
    /// from its manifest 1.
    ///
    /// The tail is the WAL objects above the `folded_wal_no` of the source
    /// manifest that `fork_basis` refers to, through the first whose head is
    /// the fork point, read whether or not this namespace has folded them
    /// since, with writer epochs at most `epoch_bound`. A WAL object that's
    /// gone returns `snapshot_not_found` for the snapshot that the fork was
    /// built from, as a finishing call does after that snapshot ended.
    pub(crate) async fn load_fork_point_tail(
        &self,
        fork_basis: &ForkBasis,
        epoch_bound: WriterEpoch,
    ) -> Result<ForkPointTail> {
        let decoded =
            load_referenced_manifest(&self.store, Some(&self.segment_cache), &fork_basis.manifest)
                .await?;
        let base_head = NamespaceReadState::from(decoded.manifest.payload());
        let replayed = load_replayed_wal_tail_through_seq(
            &self.store,
            &base_head,
            &manifest_base_state(&decoded.manifest.envelope),
            fork_basis.fork_point_seq,
            epoch_bound,
        )
        .await
        .map_err(|error| match error {
            MetadataProjectionLoadError::WalTailLoad(WalTailLoadError::MissingWalObject {
                ..
            }) => CoreError::SnapshotNotFound {
                snapshot_id: fork_basis.forked_from_snapshot_id.clone(),
            },
            error => CoreError::MetadataProjection(error),
        })?;
        Ok(ForkPointTail {
            seq: replayed.resulting_head.seq,
            tail: Arc::new(replayed.projected_tail),
        })
    }

    /// Publishes this fork's manifest 2, which folds `tail` (the source's WAL
    /// tail through the fork point) on top of `manifest_1`, and returns the
    /// fork's newest manifest after the publication.
    ///
    /// The fold writes each chain's pending pieces as one extent under the
    /// pool's prefix, with the source as the chain's creator, and a metadata
    /// segment under this namespace's prefix for each family that the tail's
    /// commits touched. Manifest 2 is still initializing. It lists manifest
    /// 1's runs and the new run, and its head is the fork point. Its
    /// retention floor, writer epoch, folded WAL number, and activity counters
    /// stay manifest 1's. The activity counters stay because the source's
    /// commits are the source's activity. The tail has at least one commit
    /// above manifest 1's head.
    ///
    /// The manifest is put once. If another call's manifest 2 won the put, or
    /// another call already made the fork active, this function returns the
    /// manifest that won. If that manifest is a tombstone, it returns
    /// `namespace_deleted`. The
    /// call checks `deadline` with the publication budget, after the
    /// materialization and before the manifest, and writes no manifest past
    /// either.
    pub(crate) async fn publish_fork_point_manifest(
        &self,
        manifest_1: Held<LoadedManifest>,
        tail: &ForkPointTail,
        deadline: &ForkDeadline,
        clock: &OperationClock,
    ) -> Result<Held<LoadedManifest>> {
        let payload = manifest_1.manifest().payload();
        assert!(
            tail.seq > payload.head_seq,
            "a fork publishes manifest 2 only for a tail with commits"
        );
        // Manifest 1 lists the source's runs, and the snapshot of the source
        // that the fork was built from keeps those runs' segments until the
        // fork's own fork point snapshot keeps them. So a segment that the fold finds missing is a
        // corrupt store while that snapshot admits a read, whether or not
        // another call published the fork's manifest 2 since.
        let fork_basis = payload.fork_basis.as_ref().ok_or_else(|| {
            CoreError::NamespaceCorrupt(format!(
                "manifest 1 of fork `{}` records no fork basis",
                self.namespace_id
            ))
        })?;
        let source = self.with_namespace(fork_basis.manifest.owner_namespace_id.clone());
        let keeper = SegmentKeeper::new(
            source,
            KeptBy::Snapshot(fork_basis.forked_from_snapshot_id.clone()),
        );
        let decoded = DecodedManifest {
            manifest: manifest_1.manifest().clone(),
            scan_runs: Arc::new(runs_newest_first(payload)),
        };
        let projection = ManifestProjection {
            basis: manifest_1.with_head(NamespaceReadState {
                seq: tail.seq,
                ..NamespaceReadState::from(payload)
            }),
            manifest_segments: self.segments(decoded, keeper).segments,
            tail_state: Arc::clone(&tail.tail),
        };
        let layouts = self.materialize_tail_content(&projection).await?;
        clock.ensure_metadata_publication_budget(&self.namespace_id)?;
        deadline.ensure_before(clock)?;
        let manifest_2 = NamespaceManifestPayload {
            activity: payload.activity,
            ..self
                .build_namespace_manifest_for_projection(&projection, &layouts)
                .await?
        };
        match self
            .publish_manifest_from(manifest_2, clock, Some(&manifest_1))
            .await?
        {
            ManifestPublicationOutcome::Published(published) => Ok(published),
            ManifestPublicationOutcome::Lost(winner)
                if winner.manifest().payload().status.is_deleted() =>
            {
                Err(self.namespace_deleted())
            }
            ManifestPublicationOutcome::Lost(winner) => Ok(winner),
        }
    }
}
