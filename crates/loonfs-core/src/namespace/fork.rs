//! Forks a namespace: the fork publishes manifest 1 with the status
//! `initializing`, built from a snapshot of the source, before it writes its
//! member entry and its fork snapshot on the source, and then publishes
//! manifest 2 with the status `active`. A later fork call with the same
//! request finishes a fork that stopped before manifest 2, and no segment or
//! content object is copied.

use crate::engine::{NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::limits::OPERATION_SNAPSHOT_TTL_MS;
use crate::manifest::load_namespace_manifest_envelope;
use crate::manifest::publish::ExistingNamespace;
use crate::merge::merge_start;
use crate::metadata::MetadataView;
use crate::namespace::basis::PublicationBasis;
use crate::namespace::control::NewestManifest;
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
use std::sync::Arc;

/// The fork whose commits a rebase replays into the new fork that it
/// creates, and the sequence of the forked namespace that the fork was made
/// at.
#[derive(Clone, Copy)]
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
    /// Returns the target's state after manifest 2. A manifest that stopped
    /// being current before the temporary snapshot's verification returns
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
    /// after, read through the temporary snapshot. A fork point below the
    /// retention floor of the snapshot's manifest returns
    /// `below_retention_floor`, and the call then creates no namespace.
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

    /// Forks this namespace into `new_namespace_id`, or finishes a fork into
    /// it that stopped before manifest 2
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/namespace-creation.md#forking-a-namespace)).
    ///
    /// 1. Finds the newest manifest of `new_namespace_id`. An active namespace
    ///    returns `namespace_exists`, and a tombstone or a retired ID returns
    ///    `namespace_deleted`. An initializing namespace is finished from step
    ///    5 when its manifest 1 matches the request, and returns
    ///    `namespace_exists` otherwise.
    /// 2. Takes the snapshot that the fork is built from: the user snapshot
    ///    that `at` identifies, or a temporary snapshot of the head, which
    ///    expires `OPERATION_SNAPSHOT_TTL_MS` after the call starts.
    /// 3. For a rebase, checks the fork point against the retention floor of
    ///    that snapshot's manifest.
    /// 4. Publishes manifest 1 with the status `initializing`, whose fork
    ///    basis records the snapshot of step 2 and a generated fork snapshot
    ///    ID. When another call's manifest 1 won, the call continues as in
    ///    step 1.
    /// 5. Writes the fork's member entry in the pool that manifest 1 records.
    /// 6. Checks that this namespace is active and that the snapshot of step
    ///    2 hasn't ended, writes the fork snapshot, and checks again. Each
    ///    check finds this namespace's newest manifest from the manifest that
    ///    the call holds, and reads the snapshot record.
    /// 7. Publishes manifest 2 with the status `active`.
    ///
    /// Manifest 1 records the fork snapshot's ID before the snapshot exists,
    /// so every fork snapshot has a namespace whose manifest records it, and
    /// the source's snapshot cleanup needs no creation grace. The call never
    /// deletes a snapshot.
    async fn install_fork(
        &self,
        new_namespace_id: &NamespaceId,
        actor_id: &loonfs_types::ActorId,
        at: ForkAt<'_>,
        rebase: Option<Rebase<'_>>,
    ) -> Result<NamespaceMetadata> {
        let clock = self.namespace.start_clock();
        let target = self.namespace.with_namespace(new_namespace_id.clone());
        let existing = match target.find_existing_namespace().await? {
            Some(existing) => existing,
            None => match self
                .publish_fork_manifest_1(&target, actor_id, &at, rebase, &clock)
                .await
            {
                Ok((manifest_1, source)) => {
                    return self.finish_fork(&target, manifest_1, source, &clock).await
                }
                Err(CoreError::NamespaceExists { .. }) => {
                    target.find_existing_namespace().await?.ok_or_else(|| {
                        CoreError::NamespaceCorrupt(format!(
                            "the manifest 1 put of namespace `{new_namespace_id}` lost to a manifest that isn't there"
                        ))
                    })?
                }
                Err(error) => return Err(error),
            },
        };
        let manifest_1 = match existing {
            ExistingNamespace::Initializing(manifest_1) => manifest_1,
            ExistingNamespace::Active => {
                return Err(CoreError::NamespaceExists {
                    namespace_id: new_namespace_id.clone(),
                })
            }
        };
        self.ensure_fork_request_matches(manifest_1.manifest.payload(), &at, rebase)?;
        let source = self.namespace.load_current_basis().await?;
        self.finish_fork(&target, manifest_1, source, &clock).await
    }

    /// Writes the member entry and the fork snapshot that `manifest_1`
    /// records, and publishes manifest 2 (steps 5 to 7 of
    /// [`Self::install_fork`]). `source` is a manifest of this namespace that
    /// the call read, from which the checks of step 6 start.
    async fn finish_fork(
        &self,
        target: &NamespaceStore<&S>,
        manifest_1: PublicationBasis,
        source: PublicationBasis,
        clock: &OperationClock,
    ) -> Result<NamespaceMetadata> {
        target
            .write_member_entry(&manifest_1.manifest.payload().pool_id)
            .await?;
        if let Err(error) = self
            .write_fork_snapshot(manifest_1.manifest.payload(), source, clock)
            .await
        {
            // Another call that finished the fork may have used the same fork
            // snapshot, so the call deletes nothing.
            return match target.find_newest_manifest().await {
                Ok(NewestManifest::Manifest(current))
                    if current.manifest.payload().status.is_active() =>
                {
                    Ok(crate::namespace::status::created_namespace(
                        current.manifest.payload(),
                    ))
                }
                _ => Err(error),
            };
        }
        let active = target.publish_active_manifest(manifest_1, clock).await?;
        Ok(crate::namespace::status::created_namespace(
            active.manifest.payload(),
        ))
    }

    /// Takes the snapshot that a new fork is built from, and publishes the
    /// fork's manifest 1 (steps 2 to 4 of [`Self::install_fork`]).
    ///
    /// Returns manifest 1 with this namespace's newest manifest as the call
    /// read it, or `namespace_exists` when another call's manifest 1 won the
    /// put.
    async fn publish_fork_manifest_1(
        &self,
        target: &NamespaceStore<&S>,
        actor_id: &loonfs_types::ActorId,
        at: &ForkAt<'_>,
        rebase: Option<Rebase<'_>>,
        clock: &OperationClock,
    ) -> Result<(PublicationBasis, PublicationBasis)> {
        let (built_from, source) = match at {
            ForkAt::Head(manifest) => {
                let owner = SnapshotOwner::User {
                    name: None,
                    expires_at_ms: Some(
                        clock
                            .request_at_ms()
                            .saturating_add(OPERATION_SNAPSHOT_TTL_MS),
                    ),
                    deleted_at_ms: None,
                };
                self.namespace
                    .create_snapshot_record(owner, manifest.clone(), clock)
                    .await?
            }
            ForkAt::Snapshot(snapshot_id) => {
                let source = self.namespace.load_current_basis().await?;
                let built_from = self
                    .namespace
                    .load_snapshot_fork_basis(snapshot_id, &source, clock)
                    .await?;
                (built_from, source)
            }
        };
        let rebased_from = match rebase {
            Some(rebase) => Some(self.namespace.rebased_from(&built_from, rebase).await?),
            None => None,
        };
        // A fork of the head, or of a user snapshot of the newest manifest,
        // copies the manifest that the call holds. A fork of an older user
        // snapshot reads the snapshot's manifest.
        let source_manifest =
            if source.manifest.payload().manifest_no == built_from.snapshot_id.manifest_no() {
                source.manifest.envelope().clone()
            } else {
                Arc::new(
                    load_namespace_manifest_envelope(
                        &self.namespace.store,
                        &self.namespace.namespace_id,
                        &built_from.snapshot_id.manifest_no(),
                    )
                    .await?,
                )
            };
        crate::manifest::ensure_manifest_reference_matches(
            "snapshot",
            &built_from.manifest(),
            &source_manifest,
        )?;
        let fork_basis = ForkBasis {
            manifest: built_from.manifest(),
            source_snapshot_id: SnapshotId::generate(built_from.snapshot_id.manifest_no()),
            forked_from_snapshot_id: built_from.snapshot_id.clone(),
            rebased_from,
        };
        let manifest_1 = NamespaceManifestPayload::initial_fork(
            target.namespace_id.clone(),
            clock.request_at_ms(),
            actor_id.clone(),
            fork_basis,
            source_manifest.payload(),
        );
        let manifest_1 = target.publish_first_manifest(manifest_1, clock).await?;
        Ok((manifest_1, source))
    }

    /// Checks that `manifest_1`, the manifest 1 of an initializing namespace,
    /// is a fork that a call with this request finishes, and returns
    /// `namespace_exists` otherwise.
    ///
    /// The request matches when manifest 1 records this namespace as the
    /// source, the snapshot that `at` identifies (any snapshot for a fork of
    /// the head), and the same rebased fork. The creator and the creation
    /// time aren't compared.
    fn ensure_fork_request_matches(
        &self,
        manifest_1: &NamespaceManifestPayload,
        at: &ForkAt<'_>,
        rebase: Option<Rebase<'_>>,
    ) -> Result<()> {
        let matches = manifest_1.fork_basis.as_ref().is_some_and(|fork_basis| {
            let same_snapshot = match at {
                ForkAt::Head(_) => true,
                ForkAt::Snapshot(snapshot_id) => {
                    fork_basis.forked_from_snapshot_id == **snapshot_id
                }
            };
            let rebased_fork = fork_basis
                .rebased_from
                .as_ref()
                .map(|rebased_from| &rebased_from.source_namespace_id);
            fork_basis.manifest.owner_namespace_id == self.namespace.namespace_id
                && same_snapshot
                && rebased_fork == rebase.map(|rebase| rebase.source_namespace_id)
        });
        if !matches {
            return Err(CoreError::NamespaceExists {
                namespace_id: manifest_1.namespace_id.clone(),
            });
        }
        Ok(())
    }

    /// Writes the fork snapshot that `manifest_1`, a fork's manifest 1,
    /// records, between two checks that this namespace is active and that the
    /// snapshot the fork was built from hasn't ended (step 6 of
    /// [`Self::install_fork`]).
    ///
    /// Each check finds this namespace's newest manifest from the manifest
    /// that the previous read found, starting at `source`, with a HEAD
    /// request for the successor and a read of the manifest boundary (see
    /// [`NamespaceStore::find_current_above`]). A fork snapshot that another
    /// call wrote counts as written.
    async fn write_fork_snapshot(
        &self,
        manifest_1: &NamespaceManifestPayload,
        source: PublicationBasis,
        clock: &OperationClock,
    ) -> Result<()> {
        let fork_basis = manifest_1
            .fork_basis
            .as_ref()
            .expect("a fork's manifest 1 records its fork basis");
        let source = self.namespace.find_current_above(&source).await?;
        self.namespace
            .load_snapshot_fork_basis(&fork_basis.forked_from_snapshot_id, &source, clock)
            .await?;
        write_snapshot_record(
            &self.namespace.store,
            &SnapshotPayload {
                snapshot_id: fork_basis.source_snapshot_id.clone(),
                namespace_id: self.namespace.namespace_id.clone(),
                head_seq: fork_basis.manifest.head_seq,
                payload_checksum: fork_basis.manifest.payload_checksum.clone(),
                created_at_ms: clock.request_at_ms(),
                owner: SnapshotOwner::Fork {
                    target_namespace_id: manifest_1.namespace_id.clone(),
                },
            },
        )
        .await?;
        let source = self.namespace.find_current_above(&source).await?;
        self.namespace
            .load_snapshot_fork_basis(&fork_basis.forked_from_snapshot_id, &source, clock)
            .await?;
        Ok(())
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Returns the `rebased_from` of a rebase's new fork built from the
    /// snapshot `record`, with the fork sequence that the next merge of `rebase`'s
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

    /// Loads the unexpired snapshot `snapshot_id` of this namespace, and
    /// checks that `source`, a manifest of this namespace that the caller
    /// read, is live. The caller decides when to read `source` again.
    async fn load_snapshot_fork_basis(
        &self,
        snapshot_id: &SnapshotId,
        source: &PublicationBasis,
        clock: &OperationClock,
    ) -> Result<SnapshotPayload> {
        let source = source.manifest.payload();
        crate::namespace::control::ensure_namespace_live(&source.namespace_id, source.status)?;
        let snapshot = self.load_user_snapshot(snapshot_id).await?;
        Ok(ensure_live(snapshot, clock.now_ms())?.state)
    }

    /// Returns whether this namespace, the target of the fork snapshot
    /// `record`, keeps the snapshot: its newest manifest is a fork of that
    /// snapshot. A target without a manifest, or a retired target, has no
    /// roots and keeps nothing.
    pub(crate) async fn target_retains_snapshot(&self, record: &SnapshotPayload) -> Result<bool> {
        let NewestManifest::Manifest(target) = self.find_newest_manifest().await? else {
            return Ok(false);
        };
        let Some(basis) = target.manifest.payload().fork_basis.as_ref() else {
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
