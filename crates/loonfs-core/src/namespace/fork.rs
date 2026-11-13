//! Fork installation takes a snapshot of a source manifest and lists its run references in
//! a new namespace's first manifest; no segment or content object is copied.

use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::manifest::load_namespace_manifest_envelope;
use crate::merge::merge_start;
use crate::metadata::MetadataView;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::snapshot::record::write_snapshot_record;
use crate::snapshot::{ensure_live, load_snapshot_basis};
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{
    ForkBasis, ManifestRef, RebasedFrom, SnapshotOwner, SnapshotPayload,
};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{CommitSeq, NamespaceId, NamespaceMetadata, SnapshotId};

/// The fork whose commits a rebase replays into the new fork that it
/// creates, and the sequence of the forked namespace that the fork was made
/// at.
struct Rebase<'a> {
    source_namespace_id: &'a NamespaceId,
    fork_point_seq: CommitSeq,
}

/// The state that a fork keeps.
enum ForkAt<'a> {
    /// The manifest that the caller's fold of the WAL tail published or
    /// found current.
    Head(ManifestRef),
    /// An unexpired snapshot of this namespace.
    Snapshot(&'a SnapshotId),
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates a new namespace at `manifest`, the manifest that the caller's
    /// fold of this namespace's WAL tail published or found current.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    /// Returns the target's status at the fork point. A manifest that stopped
    /// being current before the fork snapshot's verification returns
    /// `snapshot_unavailable`, so the caller folds and forks again.
    pub async fn fork_namespace(
        &self,
        new_namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        manifest: ManifestRef,
    ) -> Result<NamespaceMetadata> {
        self.install_fork(new_namespace_id, actor_id, ForkAt::Head(manifest), None)
            .await
    }

    /// Creates a new namespace at the unexpired snapshot `snapshot_id`, as
    /// [`Self::fork_namespace`] does at a manifest.
    pub async fn fork_namespace_at_snapshot(
        &self,
        new_namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        snapshot_id: &SnapshotId,
    ) -> Result<NamespaceMetadata> {
        self.install_fork(
            new_namespace_id,
            actor_id,
            ForkAt::Snapshot(snapshot_id),
            None,
        )
        .await
    }

    /// Creates a new namespace at `manifest` for a rebase of
    /// `source_namespace_id`, a fork of this namespace made at
    /// `fork_point_seq`, as [`Self::fork_namespace`] does
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/merge.md#rebasing)).
    ///
    /// The new fork's fork basis records `rebased_from`, with the fork
    /// sequence that the fork's next merge into this namespace would start
    /// after, read through the new fork's snapshot. A fork point below the retention
    /// floor of the snapshot's manifest returns `below_retention_floor`, and the
    /// call then deletes the snapshot and creates no namespace.
    pub async fn fork_namespace_for_rebase(
        &self,
        new_namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        source_namespace_id: &NamespaceId,
        fork_point_seq: CommitSeq,
        manifest: ManifestRef,
    ) -> Result<NamespaceMetadata> {
        let rebase = Rebase {
            source_namespace_id,
            fork_point_seq,
        };
        self.install_fork(
            new_namespace_id,
            actor_id,
            ForkAt::Head(manifest),
            Some(rebase),
        )
        .await
    }

    async fn install_fork(
        &self,
        new_namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        at: ForkAt<'_>,
        rebase: Option<Rebase<'_>>,
    ) -> Result<NamespaceMetadata> {
        let clock = self.namespace.start_clock();
        let target = self.namespace.with_namespace(new_namespace_id.clone());
        let owner = SnapshotOwner::Fork {
            target_namespace_id: new_namespace_id.clone(),
        };
        let source_record = match at {
            ForkAt::Head(manifest) => {
                self.namespace
                    .create_snapshot_record(owner, manifest, &clock)
                    .await?
            }
            ForkAt::Snapshot(snapshot_id) => {
                self.namespace
                    .create_fork_snapshot_at_snapshot(snapshot_id, owner, &clock)
                    .await?
            }
        };
        let source_manifest = load_namespace_manifest_envelope(
            &self.namespace.store,
            &self.namespace.namespace_id,
            &source_record.snapshot_id.manifest_no(),
        )
        .await?;
        crate::manifest::ensure_manifest_reference_matches(
            "fork snapshot",
            &source_record.manifest(),
            &source_manifest,
        )?;
        let rebased_from = match rebase {
            Some(rebase) => match self.namespace.rebased_from(&source_record, rebase).await {
                Ok(rebased_from) => Some(rebased_from),
                Err(error) => {
                    self.namespace
                        .delete_failed_snapshot_record(&source_record.snapshot_id, &error)
                        .await;
                    return Err(error);
                }
            },
            None => None,
        };
        let fork_basis = ForkBasis {
            manifest: source_record.manifest(),
            source_snapshot_id: source_record.snapshot_id.clone(),
            rebased_from,
        };
        let manifest = NamespaceManifestPayload::initial_fork(
            new_namespace_id.clone(),
            clock.request_at_ms(),
            actor_id.clone(),
            fork_basis,
            source_manifest.payload(),
        );
        let installed = async {
            target.publish_first_hint().await?;
            target
                .publish_first_manifest(manifest.clone(), &clock)
                .await
        }
        .await;
        if let Err(error) = installed {
            if matches!(
                error,
                CoreError::NamespaceExists { .. } | CoreError::NamespaceDeleted { .. }
            ) {
                match target.target_retains_snapshot(&source_record).await {
                    Ok(true) => {}
                    Ok(false) => {
                        self.namespace
                            .delete_failed_snapshot_record(&source_record.snapshot_id, &error)
                            .await;
                    }
                    Err(check_error) => tracing::warn!(
                        namespace_id = %self.namespace.namespace_id,
                        snapshot_id = %source_record.snapshot_id,
                        original_error = %error,
                        check_error = %check_error,
                        "kept a fork snapshot whose target could not be read after installation failed"
                    ),
                }
            }
            return Err(error);
        }
        Ok(crate::namespace::status::created_namespace(&manifest))
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Returns the `rebased_from` of a rebase's new fork whose fork snapshot is
    /// `record`, with the fork sequence that the next merge of `rebase`'s
    /// fork into this namespace starts after in the snapshot's manifest (see
    /// [`merge_start`]), or `below_retention_floor` when the fork point is
    /// below the retention floor of the snapshot's manifest.
    ///
    /// The snapshot keeps the manifest that the new fork starts from, so the
    /// floor check and the merge index read describe exactly the history that
    /// the new fork inherits.
    async fn rebased_from(
        &self,
        record: &SnapshotPayload,
        rebase: Rebase<'_>,
    ) -> Result<RebasedFrom> {
        let basis = load_snapshot_basis(&self.store, &self.segment_cache, record.clone()).await?;
        let head = NamespaceReadState::from(basis.segments.manifest().payload());
        let view = MetadataView::over_manifest_segments(&basis.segments, basis.manifest.head_seq);
        let resume_after_seq = merge_start(
            &view,
            &head,
            rebase.source_namespace_id,
            rebase.fork_point_seq,
        )
        .await??
        .after_seq;
        Ok(RebasedFrom {
            source_namespace_id: rebase.source_namespace_id.clone(),
            resume_after_seq,
        })
    }

    async fn create_fork_snapshot_at_snapshot(
        &self,
        snapshot_id: &SnapshotId,
        owner: SnapshotOwner,
        clock: &OperationClock,
    ) -> Result<SnapshotPayload> {
        let snapshot = self.load_snapshot_fork_basis(snapshot_id, clock).await?;
        let record = SnapshotPayload {
            snapshot_id: SnapshotId::generate(snapshot.snapshot_id.manifest_no()),
            created_at_ms: clock.request_at_ms(),
            owner,
            ..snapshot
        };
        let verification = async {
            write_snapshot_record(&self.store, &record).await?;
            self.load_snapshot_fork_basis(snapshot_id, clock).await
        }
        .await;
        if let Err(error) = verification {
            self.delete_failed_snapshot_record(&record.snapshot_id, &error)
                .await;
            return Err(error);
        }
        Ok(record)
    }

    /// Loads an unexpired snapshot of a live source namespace.
    async fn load_snapshot_fork_basis(
        &self,
        snapshot_id: &SnapshotId,
        clock: &OperationClock,
    ) -> Result<SnapshotPayload> {
        let source_head = self.load_read_anchor(clock.observe()).await?.read_state;
        crate::namespace::control::ensure_namespace_live(
            &source_head.namespace_id,
            source_head.status,
        )?;
        let snapshot = self.load_user_snapshot(snapshot_id).await?;
        Ok(ensure_live(snapshot, clock.now_ms())?.state)
    }

    pub(crate) async fn target_retains_snapshot(&self, record: &SnapshotPayload) -> Result<bool> {
        let Some(target) = self.load_current_manifest_if_present().await? else {
            return Ok(false);
        };
        let Some(basis) = target.payload().fork_basis.as_ref() else {
            return Ok(false);
        };
        if basis.source_snapshot_id != record.snapshot_id {
            return Ok(false);
        }
        if basis.manifest != record.manifest() {
            return Err(CoreError::NamespaceCorrupt(format!(
                "fork target `{}` refers to snapshot `{}` with a different manifest reference",
                self.namespace_id, record.snapshot_id
            )));
        }
        Ok(true)
    }
}
