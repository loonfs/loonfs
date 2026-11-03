//! Fork installation pins a source manifest and lists its run references in
//! a new namespace's first manifest; no segment or content object is copied.

use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::manifest::{load_namespace_manifest_envelope, PublicationReport};
use crate::merge::merge_start;
use crate::metadata::MetadataView;
use crate::namespace::state::NamespaceReadState;
use crate::namespace::store::NamespaceStore;
use crate::pin::record::write_pin;
use crate::pin::{ensure_unexpired, load_pin_basis};
use crate::time::OperationClock;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ForkBasis, PinOwner, PinPayload, RebasedFrom};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{ChangeSeq, NamespaceId, NamespaceMetadata, PinId};

/// The fork whose commits a rebase replays into the new fork that it
/// creates, and the sequence of the forked namespace that the fork was made
/// at.
struct Rebase<'a> {
    source_namespace_id: &'a NamespaceId,
    fork_point_seq: ChangeSeq,
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Creates a new namespace at this namespace's current head or an unexpired snapshot.
    ///
    /// The fork shares immutable file bytes but gets its own metadata history.
    /// Returns the target's status at the fork point. A fork of the current
    /// head first folds this namespace's WAL tail, starting from the writer
    /// position that [`Self::with_writer_position`] set, if the caller set a
    /// position. The report
    /// includes the manifest of this namespace that the fork's fold
    /// published, if it published one.
    pub async fn fork_namespace(
        &self,
        new_namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        snapshot_id: Option<&PinId>,
    ) -> Result<PublicationReport<NamespaceMetadata>> {
        self.install_fork(new_namespace_id, actor_id, snapshot_id, None)
            .await
    }

    /// Creates a new namespace at this namespace's current head for a rebase
    /// of `source_namespace_id`, a fork of this namespace made at
    /// `fork_point_seq`, as [`Self::fork_namespace`] does
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/merge.md#rebasing)).
    ///
    /// The new fork's fork basis records `rebased_from`, with the fork
    /// sequence that the fork's next merge into this namespace would start
    /// after, read through the new fork's pin. A fork point below the pinned
    /// manifest's retention floor returns `rebootstrap_required`, and the
    /// call then deletes the pin and creates no namespace.
    pub async fn fork_namespace_for_rebase(
        &self,
        new_namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        source_namespace_id: &NamespaceId,
        fork_point_seq: ChangeSeq,
    ) -> Result<PublicationReport<NamespaceMetadata>> {
        let rebase = Rebase {
            source_namespace_id,
            fork_point_seq,
        };
        self.install_fork(new_namespace_id, actor_id, None, Some(rebase))
            .await
    }

    async fn install_fork(
        &self,
        new_namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        snapshot_id: Option<&PinId>,
        rebase: Option<Rebase<'_>>,
    ) -> Result<PublicationReport<NamespaceMetadata>> {
        let clock = self.namespace.start_clock();
        let target = self.namespace.with_namespace(new_namespace_id.clone());
        target.ensure_absent().await?;
        let owner = PinOwner::Fork {
            target_namespace_id: new_namespace_id.clone(),
        };
        let PublicationReport {
            response: source_record,
            published,
        } = if let Some(snapshot_id) = snapshot_id {
            PublicationReport {
                response: self
                    .namespace
                    .create_snapshot_fork_pin(snapshot_id, owner, &clock)
                    .await?,
                published: None,
            }
        } else {
            self.namespace
                .create_pin(owner, self.writer_position.clone(), &clock)
                .await?
        };
        let source_manifest = load_namespace_manifest_envelope(
            &self.namespace.store,
            &self.namespace.namespace_id,
            &source_record.pin_id.manifest_no(),
        )
        .await?;
        crate::manifest::ensure_manifest_reference_matches(
            "fork pin",
            &source_record.manifest(),
            &source_manifest,
        )?;
        let rebased_from = match rebase {
            Some(rebase) => match self.namespace.rebased_from(&source_record, rebase).await {
                Ok(rebased_from) => Some(rebased_from),
                Err(error) => {
                    self.namespace
                        .delete_failed_pin(&source_record.pin_id, &error)
                        .await;
                    return Err(error);
                }
            },
            None => None,
        };
        let fork_basis = ForkBasis {
            manifest: source_record.manifest(),
            source_pin_id: source_record.pin_id.clone(),
            rebased_from,
        };
        let manifest = NamespaceManifestPayload::initial_fork(
            new_namespace_id.clone(),
            clock.request_at_ms(),
            actor_id.clone(),
            fork_basis,
            source_manifest.payload(),
        );
        if let Err(error) = target.create_manifest(manifest.clone(), &clock).await {
            if matches!(
                error,
                CoreError::NamespaceExists { .. } | CoreError::NamespaceDeleted { .. }
            ) {
                match target.target_retains_pin(&source_record).await {
                    Ok(true) => {}
                    Ok(false) => {
                        self.namespace
                            .delete_failed_pin(&source_record.pin_id, &error)
                            .await;
                    }
                    Err(check_error) => tracing::warn!(
                        namespace_id = %self.namespace.namespace_id,
                        pin_id = %source_record.pin_id,
                        original_error = %error,
                        check_error = %check_error,
                        "kept a fork pin whose target could not be read after installation failed"
                    ),
                }
            }
            return Err(error);
        }
        Ok(PublicationReport {
            response: crate::namespace::status::created_namespace(&manifest),
            published,
        })
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Returns the `rebased_from` of a rebase's new fork whose fork pin is
    /// `record`, with the fork sequence that the next merge of `rebase`'s
    /// fork into this namespace starts after in the pinned manifest (see
    /// [`merge_start`]), or `rebootstrap_required` when the fork point is
    /// below the pinned manifest's retention floor.
    ///
    /// The pin keeps the manifest that the new fork starts from, so the
    /// floor check and the merge index read describe exactly the history that
    /// the new fork inherits.
    async fn rebased_from(&self, record: &PinPayload, rebase: Rebase<'_>) -> Result<RebasedFrom> {
        let pinned = load_pin_basis(&self.store, &self.segment_cache, record.clone()).await?;
        let head = NamespaceReadState::from(pinned.segments.manifest().payload());
        let view = MetadataView::over_manifest_segments(&pinned.segments, pinned.manifest.head_seq);
        let resume_after_seq = merge_start(
            &view,
            &head,
            rebase.source_namespace_id,
            rebase.fork_point_seq,
        )
        .await?
        .after_seq;
        Ok(RebasedFrom {
            source_namespace_id: rebase.source_namespace_id.clone(),
            resume_after_seq,
        })
    }

    async fn create_snapshot_fork_pin(
        &self,
        snapshot_id: &PinId,
        owner: PinOwner,
        clock: &OperationClock,
    ) -> Result<PinPayload> {
        let snapshot = self.load_snapshot_fork_basis(snapshot_id, clock).await?;
        let record = PinPayload {
            pin_id: PinId::generate(snapshot.pin_id.manifest_no()),
            created_at_ms: clock.request_at_ms(),
            owner,
            ..snapshot
        };
        let verification = async {
            write_pin(&self.store, &record).await?;
            self.load_snapshot_fork_basis(snapshot_id, clock).await
        }
        .await;
        if let Err(error) = verification {
            self.delete_failed_pin(&record.pin_id, &error).await;
            return Err(error);
        }
        Ok(record)
    }

    /// Loads an unexpired snapshot of a live source namespace.
    async fn load_snapshot_fork_basis(
        &self,
        snapshot_id: &PinId,
        clock: &OperationClock,
    ) -> Result<PinPayload> {
        let source_head = self.load_read_anchor(clock.observe()).await?.read_state;
        crate::namespace::control::ensure_namespace_live(
            &source_head.namespace_id,
            source_head.status,
        )?;
        let snapshot = self.load_user_pin(snapshot_id).await?;
        Ok(ensure_unexpired(snapshot, clock.now_ms())?.state)
    }

    pub(crate) async fn target_retains_pin(&self, record: &PinPayload) -> Result<bool> {
        let Some(target) = self.load_current_manifest_if_present().await? else {
            return Ok(false);
        };
        let Some(basis) = target.payload().fork_basis.as_ref() else {
            return Ok(false);
        };
        if basis.source_pin_id != record.pin_id {
            return Ok(false);
        }
        if basis.manifest != record.manifest() {
            return Err(CoreError::NamespaceCorrupt(format!(
                "fork target `{}` names pin `{}` with a different manifest reference",
                self.namespace_id, record.pin_id
            )));
        }
        Ok(true)
    }
}
