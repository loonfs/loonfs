//! Forks a namespace: the fork reads the source's head and publishes its
//! manifest 1 with the status `initializing`, built from the source's
//! manifest, folds the source's WAL tail into its own manifest 2, writes its
//! fork point snapshot, its member entry, and its fork snapshot on the
//! source, and then publishes its active manifest. The source publishes
//! nothing. A later fork call with the same request finishes a fork that
//! stopped before its active manifest, and no segment or content object of
//! the source is copied.

use crate::commit::publish_view::WriterPosition;
use crate::engine::{Administrator, NamespaceEngine, Writable};
use crate::error::{CoreError, Result};
use crate::limits::{MAX_CONTENTION_ATTEMPTS, OPERATION_SNAPSHOT_TTL_MS};
use crate::manifest::fold::ManifestProjection;
use crate::manifest::load_namespace_manifest_envelope;
use crate::manifest::publish::ExistingNamespace;
use crate::manifest::ForkPointTail;
use crate::merge::merge_start;
use crate::namespace::control::LoadedManifest;
use crate::namespace::control::NewestManifest;
use crate::namespace::held::Held;
use crate::namespace::store::NamespaceStore;
use crate::snapshot::ensure_live;
use crate::snapshot::record::write_snapshot_record;
use crate::time::{Observation, OperationClock};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ForkBasis, RebasedFrom, SnapshotOwner, SnapshotPayload};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{CommitSeq, ManifestNo, NamespaceId, NamespaceMetadata, SnapshotId};
use std::sync::Arc;

/// The fork whose commits a rebase replays into the new fork that it
/// creates, and the sequence of the forked namespace that the fork was made
/// at.
#[derive(Clone, Copy)]
struct Rebase<'a> {
    source_namespace_id: &'a NamespaceId,
    fork_point_seq: CommitSeq,
}

/// The state that a fork starts from.
#[allow(
    clippy::large_enum_variant,
    reason = "one value per fork call, borrowed until the call ends: indirection would buy an allocation and save nothing"
)]
enum ForkAt<'a> {
    /// This namespace's head. `held` is the position of this namespace's
    /// writer session that the caller has, which the first read of the
    /// head starts from, and without one the read is a discovery. A rebase's
    /// new fork records `rebase`.
    Head {
        held: Option<WriterPosition>,
        rebase: Option<Rebase<'a>>,
    },
    /// An unexpired snapshot of this namespace.
    Snapshot(&'a SnapshotId),
}

impl ForkAt<'_> {
    fn rebase(&self) -> Option<Rebase<'_>> {
        match self {
            Self::Head { rebase, .. } => *rebase,
            Self::Snapshot(_) => None,
        }
    }
}

/// How a fork of a namespace's head ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(
    clippy::large_enum_variant,
    reason = "one value per fork call, which the caller matches right away: a box would buy an allocation and save nothing"
)]
pub enum ForkOutcome {
    /// The fork is active, with this state.
    Created(NamespaceMetadata),
    /// A commit in the WAL tail that the fork read is older than
    /// [`MAX_TAIL_COMMIT_AGE_MS`](crate::limits::MAX_TAIL_COMMIT_AGE_MS), so
    /// the fork wrote nothing. The caller folds the source and forks again
    /// ([format spec](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#92-forking-a-namespace)).
    SourceTailTooOld,
}

impl ForkOutcome {
    /// Returns the fork's state, or `None` when the source's tail was too old
    /// for the fork to fold.
    pub fn created(self) -> Option<NamespaceMetadata> {
        match self {
            Self::Created(created) => Some(created),
            Self::SourceTailTooOld => None,
        }
    }
}

/// The time by which a fork that folds the source's WAL tail has to have
/// written everything, which the garbage collection timing argument reserves
/// for the content that the fork takes from the tail
/// ([format spec](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#92-forking-a-namespace)).
pub(crate) enum ForkDeadline {
    /// The fork folds a tail, and writes nothing at or after `ends_at_ms`,
    /// which is `OPERATION_SNAPSHOT_TTL_MS` after the temporary snapshot
    /// `snapshot_id` was created. An extension of the snapshot doesn't move
    /// the deadline, because the snapshot's creation time can't change.
    Tail {
        snapshot_id: SnapshotId,
        ends_at_ms: u64,
    },
    /// The fork folds no tail, so only the publication budget bounds it.
    NoTail,
}

impl ForkDeadline {
    /// Returns the deadline of the fork whose fork basis is `fork_basis`, and
    /// which is built from `snapshot`, the snapshot that the fork basis
    /// records.
    ///
    /// The fork folds a tail when its fork point is above the head of the
    /// source manifest. Only a fork of the head can, so `snapshot` is then
    /// the temporary snapshot, whose creation time is the first call's start.
    fn new(fork_basis: &ForkBasis, snapshot: &SnapshotPayload) -> Self {
        assert_eq!(
            fork_basis.forked_from_snapshot_id, snapshot.snapshot_id,
            "a fork's deadline runs from the snapshot that its fork basis records"
        );
        if fork_basis.fork_point_seq > fork_basis.manifest.head_seq {
            Self::Tail {
                snapshot_id: snapshot.snapshot_id.clone(),
                ends_at_ms: snapshot
                    .created_at_ms
                    .saturating_add(OPERATION_SNAPSHOT_TTL_MS),
            }
        } else {
            Self::NoTail
        }
    }

    /// Returns `snapshot_not_found` for the temporary snapshot once `clock`
    /// reads the deadline of a fork that folds a tail, or later, the same
    /// outcome as after the temporary snapshot ended.
    pub(crate) fn ensure_before(&self, clock: &OperationClock) -> Result<()> {
        match self {
            Self::Tail {
                snapshot_id,
                ends_at_ms,
            } if clock.now_ms() >= *ends_at_ms => Err(CoreError::SnapshotNotFound {
                snapshot_id: snapshot_id.clone(),
            }),
            Self::Tail { .. } | Self::NoTail => Ok(()),
        }
    }
}

/// The namespace ID that a fork creates, and the state that the fork found
/// at that ID before its first write
/// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/namespace-creation.md#forking-a-namespace)).
///
/// A caller finds the target once with
/// [`NamespaceEngine::find_fork_target`], before it reads the source, and
/// passes it to the fork. If another call creates the target after the caller
/// found it, the fork's manifest 1 put fails, and the fork then finds the
/// target again. A fork that finishes an initializing target finds the target
/// again after its publication budget starts, because the budget that protects
/// its fork snapshot has to start before the fork reads the target.
#[derive(Clone, Debug)]
pub struct ForkTarget {
    namespace_id: NamespaceId,
    /// Whether the target was initializing when the caller found it, rather
    /// than free.
    initializing: bool,
}

/// A fork whose manifest 1 this call published.
struct StartedFork<'a, S: ObjectStore> {
    manifest_1: Held<LoadedManifest>,
    /// The source's newest manifest as the call read it.
    source: Held<LoadedManifest>,
    /// The source's head, when the fork folds its WAL tail into manifest 2.
    head: Option<ManifestProjection<'a, S>>,
    deadline: ForkDeadline,
}

impl<S: ObjectStore> NamespaceEngine<S, Writable> {
    /// Finds the newest manifest of `new_namespace_id` for a fork of this
    /// namespace, the first step of a fork.
    ///
    /// Returns `namespace_exists` for an active namespace, and
    /// `namespace_deleted` for a tombstone or a retired ID, so a caller that
    /// finds the target first reads nothing of the source for a taken ID.
    /// For an initializing namespace, whose earlier fork stopped before its
    /// active manifest, it returns a target that the fork finishes.
    pub async fn find_fork_target(&self, new_namespace_id: &NamespaceId) -> Result<ForkTarget> {
        let target = self.namespace.with_namespace(new_namespace_id.clone());
        let observed = Observation::now(Arc::clone(&self.namespace.timer));
        let initializing = match target.find_existing_namespace(observed).await? {
            None => false,
            Some(ExistingNamespace::Initializing(_)) => true,
            Some(ExistingNamespace::Active) => {
                return Err(CoreError::NamespaceExists {
                    namespace_id: new_namespace_id.clone(),
                })
            }
        };
        Ok(ForkTarget {
            namespace_id: new_namespace_id.clone(),
            initializing,
        })
    }

    /// Creates a new namespace at this namespace's head, whose first read
    /// starts from `held`, the position of this namespace's writer session,
    /// or from a discovery when `held` is `None`.
    ///
    /// The fork shares immutable file bytes but gets its own metadata
    /// history, and this namespace publishes nothing. Returns the target's
    /// state after its active manifest, or
    /// [`ForkOutcome::SourceTailTooOld`] when the fork has to wait for a fold
    /// of this namespace. `administrator` proves the caller administers this
    /// namespace, because forking it is only for the administrator.
    pub async fn fork_namespace(
        &self,
        administrator: &Administrator,
        target: ForkTarget,
        actor_id: &loonfs_types::ActorId,
        held: Option<WriterPosition>,
    ) -> Result<ForkOutcome> {
        administrator.assert_for(&self.namespace.namespace_id);
        self.install_fork(target, actor_id, ForkAt::Head { held, rebase: None })
            .await
    }

    /// Creates a new namespace at the unexpired snapshot `snapshot_id`, as
    /// [`Self::fork_namespace`] does at the head, with the same proof.
    pub async fn fork_namespace_at_snapshot(
        &self,
        administrator: &Administrator,
        target: ForkTarget,
        actor_id: &loonfs_types::ActorId,
        snapshot_id: &SnapshotId,
    ) -> Result<NamespaceMetadata> {
        administrator.assert_for(&self.namespace.namespace_id);
        Ok(self
            .install_fork(target, actor_id, ForkAt::Snapshot(snapshot_id))
            .await?
            .created()
            .expect("a fork of a user snapshot reads no WAL tail, so no tail is too old"))
    }

    /// Creates a new namespace at this namespace's head for a rebase of
    /// `source_namespace_id`, a fork of this namespace made at
    /// `fork_point_seq`, as [`Self::fork_namespace`] does
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/merge.md#rebasing)).
    ///
    /// The new fork's fork basis records `rebased_from`, with the fork
    /// sequence that the fork's next merge into this namespace would start
    /// after, read from the head that the new fork starts from. A fork point
    /// below the retention floor of that head returns
    /// `below_retention_floor`, and the call then creates no namespace. It
    /// takes the proof that [`Self::fork_namespace`] takes.
    pub async fn fork_namespace_for_rebase(
        &self,
        administrator: &Administrator,
        target: ForkTarget,
        actor_id: &loonfs_types::ActorId,
        source_namespace_id: &NamespaceId,
        fork_point_seq: CommitSeq,
        held: Option<WriterPosition>,
    ) -> Result<ForkOutcome> {
        administrator.assert_for(&self.namespace.namespace_id);
        let rebase = Rebase {
            source_namespace_id,
            fork_point_seq,
        };
        let at = ForkAt::Head {
            held,
            rebase: Some(rebase),
        };
        self.install_fork(target, actor_id, at).await
    }

    /// Forks this namespace into `target`, or finishes a fork into it that
    /// stopped before its active manifest
    /// ([format spec](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#92-forking-a-namespace)).
    ///
    /// 1. The caller finds the newest manifest of the target's ID with
    ///    [`Self::find_fork_target`], which returns `namespace_exists` for an
    ///    active namespace, and `namespace_deleted` for a tombstone or a
    ///    retired ID. For an initializing namespace, the call finds the
    ///    target's newest manifest again after its publication budget starts,
    ///    and finishes the fork (see [`Self::finish_initializing_fork`]).
    /// 2. Takes the snapshot that the fork is built from: the user snapshot
    ///    that `at` identifies, or a temporary snapshot of the manifest of
    ///    this namespace's head, without a fold, which expires
    ///    `OPERATION_SNAPSHOT_TTL_MS` after the call starts. The fork point
    ///    is the head. When a commit in the WAL tail that the call read is
    ///    older than `MAX_TAIL_COMMIT_AGE_MS`, the call writes nothing and
    ///    returns [`ForkOutcome::SourceTailTooOld`] (see
    ///    [`Self::snapshot_head`]).
    /// 3. For a rebase, checks the fork point against the retention floor of
    ///    the head.
    /// 4. Publishes manifest 1 with the status `initializing`, whose fork
    ///    basis records the fork point, the snapshot of step 2, and generated
    ///    IDs for the fork snapshot and the fork point snapshot. When another
    ///    call's manifest 1 put succeeded first, the call finds the target
    ///    again and continues as step 1 describes.
    /// 5. When the head's WAL tail has commits, publishes manifest 2, which
    ///    folds the tail (see [`NamespaceStore::publish_fork_point_manifest`]).
    /// 6. Writes the fork point snapshot, the fork's member entry, and the
    ///    fork snapshot on this namespace, checks this namespace and the
    ///    snapshot of step 2, and publishes the active manifest (see
    ///    [`Self::finish_fork`]).
    ///
    /// For a fork that folds the source's WAL tail, each write after step 2
    /// first checks that `OPERATION_SNAPSHOT_TTL_MS` hasn't passed since the
    /// temporary snapshot was created, and a call that's later writes nothing
    /// more and returns `snapshot_not_found` (see [`ForkDeadline`]).
    ///
    /// The source's collection keeps every fork snapshot, the target's
    /// collection keeps its fork point snapshot, and the target's retirement
    /// deletes both. The call never deletes a snapshot.
    async fn install_fork(
        &self,
        target: ForkTarget,
        actor_id: &loonfs_types::ActorId,
        at: ForkAt<'_>,
    ) -> Result<ForkOutcome> {
        let clock = self.namespace.start_clock();
        let new_namespace_id = &target.namespace_id;
        let target_store = self.namespace.with_namespace(new_namespace_id.clone());
        let newest = if target.initializing {
            load_initializing_target(&target_store, &clock).await?
        } else {
            match self
                .publish_fork_manifest_1(&target_store, actor_id, &at, &clock)
                .await
            {
                Ok(None) => return Ok(ForkOutcome::SourceTailTooOld),
                Ok(Some(started)) => {
                    let newest = match &started.head {
                        Some(head) => {
                            target_store
                                .publish_fork_point_manifest(
                                    started.manifest_1,
                                    &ForkPointTail::from_projection(head),
                                    &started.deadline,
                                    &clock,
                                )
                                .await?
                        }
                        None => started.manifest_1,
                    };
                    return self
                        .finish_fork(
                            &target_store,
                            newest,
                            started.source,
                            &started.deadline,
                            &clock,
                        )
                        .await
                        .map(ForkOutcome::Created);
                }
                Err(CoreError::NamespaceExists { .. }) => {
                    load_initializing_target(&target_store, &clock).await?
                }
                Err(error) => return Err(error),
            }
        };
        self.finish_initializing_fork(&target_store, newest, &at, &clock)
            .await
            .map(ForkOutcome::Created)
    }

    /// Finishes the fork into `target`, whose newest manifest `newest` is
    /// initializing, from the step after that manifest
    /// ([format spec](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#finishing-an-initializing-namespace)).
    ///
    /// The call returns `namespace_exists` when the fork basis doesn't match
    /// the request. It then checks that this namespace is active and that the
    /// snapshot that the fork was built from hasn't ended, and, for a fork
    /// that folds the source's WAL tail, that `OPERATION_SNAPSHOT_TTL_MS`
    /// hasn't passed since that snapshot was created. It writes nothing for a deleted source or a
    /// snapshot that ended. When `newest` is
    /// manifest 1 and the fork point is above its head, the fork point
    /// snapshot is on manifest 2, so the call folds the source's WAL tail
    /// again, from the source's WAL objects, and publishes manifest 2. That
    /// manifest 2 is the one that the first call published or would have
    /// published.
    async fn finish_initializing_fork(
        &self,
        target: &NamespaceStore<&S>,
        newest: Held<LoadedManifest>,
        at: &ForkAt<'_>,
        clock: &OperationClock,
    ) -> Result<NamespaceMetadata> {
        let fork_basis = self
            .ensure_fork_request_matches(newest.manifest().payload(), at)?
            .clone();
        let source = self.namespace.load_current_basis(clock.origin()).await?;
        // The call that wrote manifest 1 found the snapshot of step 2 live
        // after it started, and the budget of the fork snapshot write runs
        // from that observation. This call made no such observation, so it
        // checks that this namespace is active and reads the snapshot before
        // its writes, and writes nothing for a deleted source or a snapshot
        // that ended. The snapshot's record stores its creation time, from
        // which the deadline of a fork that folds a tail runs.
        let snapshot = self
            .namespace
            .load_snapshot_fork_basis(&fork_basis.forked_from_snapshot_id, &source, clock)
            .await?;
        let deadline = ForkDeadline::new(&fork_basis, &snapshot);
        let newest = if newest.manifest().payload().manifest_no == ManifestNo(1)
            && fork_basis.fork_point_seq > newest.manifest().payload().head_seq
        {
            let tail = self
                .namespace
                .load_fork_point_tail(&fork_basis, source.manifest().payload().writer_epoch)
                .await?;
            target
                .publish_fork_point_manifest(newest, &tail, &deadline, clock)
                .await?
        } else {
            newest
        };
        self.finish_fork(target, newest, source, &deadline, clock)
            .await
    }

    /// Writes the fork point snapshot on `newest`, the fork's manifest at its
    /// fork point, the member entry, and the fork snapshot on this namespace
    /// that `newest` records, and publishes the active manifest. `source` is
    /// a manifest of this namespace that the call read, from which the checks
    /// after the fork snapshot start. Each write first checks `deadline`.
    ///
    /// When another call already made the fork active, the function returns
    /// the state of the newest manifest that this call found, and installs no
    /// cached head (see
    /// [`NamespaceStore::publish_active_manifest`]).
    async fn finish_fork(
        &self,
        target: &NamespaceStore<&S>,
        newest: Held<LoadedManifest>,
        source: Held<LoadedManifest>,
        deadline: &ForkDeadline,
        clock: &OperationClock,
    ) -> Result<NamespaceMetadata> {
        let payload = newest.manifest().payload();
        if payload.status.is_active() {
            return Ok(crate::namespace::status::created_namespace(payload));
        }
        deadline.ensure_before(clock)?;
        target.write_fork_point_snapshot(&newest, clock).await?;
        deadline.ensure_before(clock)?;
        target.write_member_entry(&payload.pool_id, clock).await?;
        // The check after the fork snapshot's put checks `deadline` last, so it
        // is also the check before the active manifest.
        if let Err(error) = self
            .write_fork_snapshot(payload, source, deadline, clock)
            .await
        {
            // Another call that finished the fork may have used the same fork
            // snapshot, so the call deletes nothing.
            return match target.find_newest_manifest(clock.origin()).await {
                Ok(NewestManifest::Manifest(current))
                    if current.manifest().payload().status.is_active() =>
                {
                    Ok(crate::namespace::status::created_namespace(
                        current.manifest().payload(),
                    ))
                }
                _ => Err(error),
            };
        }
        let active = target.publish_active_manifest(newest, clock).await?;
        Ok(crate::namespace::status::created_namespace(
            active.manifest().payload(),
        ))
    }

    /// Takes the snapshot that a new fork is built from, and publishes the
    /// fork's manifest 1 (steps 2 to 4 of [`Self::install_fork`]).
    ///
    /// Returns manifest 1, this namespace's newest manifest as the call read
    /// it, the head whose WAL tail the fork folds, and the fork's deadline, or
    /// `namespace_exists` when another call's manifest 1 won the put. Returns
    /// `None`, and writes nothing, when the head's WAL tail has a commit older
    /// than `MAX_TAIL_COMMIT_AGE_MS`.
    async fn publish_fork_manifest_1(
        &self,
        target: &NamespaceStore<&S>,
        actor_id: &loonfs_types::ActorId,
        at: &ForkAt<'_>,
        clock: &OperationClock,
    ) -> Result<Option<StartedFork<'_, S>>> {
        let (built_from, source, head) = match at {
            ForkAt::Head { held, .. } => {
                let Some((built_from, source, head)) =
                    self.snapshot_head(held.clone(), clock).await?
                else {
                    return Ok(None);
                };
                (built_from, source, Some(head))
            }
            ForkAt::Snapshot(snapshot_id) => {
                let source = self.namespace.load_current_basis(clock.origin()).await?;
                let built_from = self
                    .namespace
                    .load_snapshot_fork_basis(snapshot_id, &source, clock)
                    .await?;
                (built_from, source, None)
            }
        };
        let rebased_from = match (at.rebase(), &head) {
            (Some(rebase), Some(head)) => Some(rebased_from(head, rebase).await?),
            _ => None,
        };
        // A fork of the head, or of a user snapshot of the newest manifest,
        // copies the manifest that the call holds. A fork of an older user
        // snapshot reads the snapshot's manifest.
        let source_manifest =
            if source.manifest().payload().manifest_no == built_from.snapshot_id.manifest_no() {
                source.manifest().envelope().clone()
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
        // Only a head whose tail has commits has a manifest 2, which folds the
        // tail. A tail of fences changes nothing that the fork keeps, so the
        // fork point is the source manifest's head.
        let head = head.filter(|head| head.head().seq > built_from.head_seq);
        let fork_basis = ForkBasis {
            manifest: built_from.manifest(),
            fork_point_seq: head
                .as_ref()
                .map_or(built_from.head_seq, |head| head.head().seq),
            source_snapshot_id: SnapshotId::generate(built_from.snapshot_id.manifest_no()),
            forked_from_snapshot_id: built_from.snapshot_id.clone(),
            fork_point_snapshot_id: SnapshotId::generate(ManifestNo(if head.is_some() {
                2
            } else {
                1
            })),
            rebased_from,
        };
        let deadline = ForkDeadline::new(&fork_basis, &built_from);
        let manifest_1 = NamespaceManifestPayload::initial_fork(
            target.namespace_id.clone(),
            clock.request_at_ms(),
            actor_id.clone(),
            fork_basis,
            source_manifest.payload(),
        );
        deadline.ensure_before(clock)?;
        let manifest_1 = target.publish_first_manifest(manifest_1, clock).await?;
        Ok(Some(StartedFork {
            manifest_1,
            source,
            head,
            deadline,
        }))
    }

    /// Reads this namespace's head, starting from `held`, and takes the
    /// temporary snapshot that a fork of the head is built from on the
    /// manifest that the head was read above, without a fold (format spec,
    /// "Creating and verifying a snapshot").
    ///
    /// Returns the snapshot record, this namespace's newest manifest with the
    /// manifest boundary that the snapshot's verification read, and the head.
    /// When a newer manifest became current before the verification, the call
    /// reads the head again with a discovery, up to
    /// `MAX_CONTENTION_ATTEMPTS` times, and then returns `store_contention`.
    ///
    /// Each read of the head is checked before the snapshot is written: when
    /// a commit in its WAL tail is older than `MAX_TAIL_COMMIT_AGE_MS` at the
    /// time that the call started, the call writes nothing and returns `None`,
    /// and the caller folds this namespace first. The check runs on the tail
    /// that the fork folds, whichever read found it, and not on the position
    /// `held` that the read started from.
    async fn snapshot_head(
        &self,
        mut held: Option<WriterPosition>,
        clock: &OperationClock,
    ) -> Result<
        Option<(
            SnapshotPayload,
            Held<LoadedManifest>,
            ManifestProjection<'_, S>,
        )>,
    > {
        let expires_at_ms = clock
            .request_at_ms()
            .saturating_add(OPERATION_SNAPSHOT_TTL_MS);
        for _ in 0..MAX_CONTENTION_ATTEMPTS {
            let head = self
                .namespace
                .load_head_projection(held.take(), clock)
                .await?;
            if head
                .tail_state
                .has_commit_older_than_max_age(head.head().seq, clock.request_at_ms())
            {
                return Ok(None);
            }
            match self
                .namespace
                .create_verified_snapshot_record(
                    None,
                    Some(expires_at_ms),
                    &head.manifest_segments.loaded_manifest(),
                    head.basis.boundary().clone(),
                    clock,
                )
                .await
            {
                Ok((record, basis)) => return Ok(Some((record, basis, head))),
                Err(CoreError::HeadMoved(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Err(CoreError::HeadMoved(format!(
            "a newer manifest of namespace `{}` became current before each of \
             {MAX_CONTENTION_ATTEMPTS} temporary snapshots was verified",
            self.namespace.namespace_id
        )))
    }

    /// Checks that `manifest`, the newest manifest of an initializing
    /// namespace, is a fork that a call with this request finishes, and
    /// returns its fork basis, or `namespace_exists` otherwise.
    ///
    /// The request matches when the manifest records this namespace as the
    /// source, the snapshot that `at` identifies (any snapshot for a fork of
    /// the head), and the same rebased fork. The creator and the creation
    /// time aren't compared.
    fn ensure_fork_request_matches<'m>(
        &self,
        manifest: &'m NamespaceManifestPayload,
        at: &ForkAt<'_>,
    ) -> Result<&'m ForkBasis> {
        let fork_basis = manifest.fork_basis.as_ref().filter(|fork_basis| {
            let same_snapshot = match at {
                ForkAt::Head { .. } => true,
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
                && rebased_fork == at.rebase().map(|rebase| rebase.source_namespace_id)
        });
        fork_basis.ok_or_else(|| CoreError::NamespaceExists {
            namespace_id: manifest.namespace_id.clone(),
        })
    }

    /// Writes the fork snapshot on this namespace that `fork`, a fork's
    /// manifest, records, and then checks that this namespace is active and
    /// that the snapshot the fork was built from hasn't ended.
    ///
    /// The write checks the call's publication budget first, as a manifest
    /// put does, so the fork snapshot is written while the record of the
    /// snapshot the fork was built from still keeps the source manifest: that
    /// record is deleted no earlier than the grace window after the snapshot
    /// ends, and the grace window is at least the publication completion
    /// bound. A call that stalled past its budget writes nothing. The write
    /// and the check after it also check `deadline`.
    ///
    /// The check finds this namespace's newest manifest from `source`, the
    /// manifest that the call holds, with a HEAD request for the successor
    /// and a read of the manifest boundary (see
    /// [`NamespaceStore::find_current_above`]). A fork snapshot that another
    /// call wrote counts as written.
    async fn write_fork_snapshot(
        &self,
        fork: &NamespaceManifestPayload,
        source: Held<LoadedManifest>,
        deadline: &ForkDeadline,
        clock: &OperationClock,
    ) -> Result<()> {
        let fork_basis = fork
            .fork_basis
            .as_ref()
            .expect("a fork's manifest records its fork basis");
        deadline.ensure_before(clock)?;
        write_snapshot_record(
            &self.namespace.store,
            &SnapshotPayload {
                snapshot_id: fork_basis.source_snapshot_id.clone(),
                namespace_id: self.namespace.namespace_id.clone(),
                head_seq: fork_basis.manifest.head_seq,
                payload_checksum: fork_basis.manifest.payload_checksum.clone(),
                created_at_ms: clock.request_at_ms(),
                owner: SnapshotOwner::Fork {
                    target_namespace_id: fork.namespace_id.clone(),
                },
            },
            clock,
        )
        .await?;
        let source = self
            .namespace
            .find_current_above(source.manifest(), source.boundary().clone(), clock.origin())
            .await?;
        self.namespace
            .load_snapshot_fork_basis(&fork_basis.forked_from_snapshot_id, &source, clock)
            .await?;
        deadline.ensure_before(clock)
    }
}

/// Loads the newest manifest of `target`, and returns that manifest while the
/// target is still initializing. A fork calls this when its caller found the
/// target initializing, or when another call's manifest 1 put succeeded before
/// the fork's put.
///
/// Returns `namespace_exists` for an active namespace, and
/// `namespace_deleted` for a tombstone or a retired ID.
async fn load_initializing_target<S: ObjectStore>(
    target: &NamespaceStore<S>,
    clock: &OperationClock,
) -> Result<Held<LoadedManifest>> {
    match target.find_existing_namespace(clock.origin()).await? {
        Some(ExistingNamespace::Initializing(newest)) => Ok(newest),
        Some(ExistingNamespace::Active) => Err(CoreError::NamespaceExists {
            namespace_id: target.namespace_id().clone(),
        }),
        None => Err(CoreError::NamespaceCorrupt(format!(
            "namespace `{}` has no manifest after a call found its manifest 1",
            target.namespace_id()
        ))),
    }
}

/// Returns the `rebased_from` of a rebase's new fork that starts from `head`,
/// with the fork sequence that the next merge of `rebase`'s fork into the
/// namespace of `head` starts after (see [`merge_start`]), or
/// `below_retention_floor` when the fork point is below the retention floor
/// of `head`, or `invalid_request` when a rebase's new fork already merged
/// the fork's work into that namespace.
///
/// The new fork's state at its fork point is `head`, so the floor check and
/// the merge index read describe exactly the history that the new fork
/// inherits.
async fn rebased_from<S: ObjectStore + ?Sized>(
    head: &ManifestProjection<'_, S>,
    rebase: Rebase<'_>,
) -> Result<RebasedFrom> {
    let resume_after_seq = merge_start(
        &head.metadata_view(),
        head.head(),
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

impl<S: ObjectStore> NamespaceStore<S> {
    /// Writes the fork point snapshot on `newest`, this namespace's manifest
    /// at its fork point, with the ID that its fork basis records and this
    /// namespace as the target. A fork point snapshot that another call wrote
    /// counts as written.
    ///
    /// The write checks the call's publication budget first, as the fork
    /// snapshot's write does, so a call that stalled past its budget writes
    /// nothing. Within the budget, a manifest that this call published can't
    /// have been deleted: a collection deletes a manifest of an initializing
    /// namespace only after a tombstone replaced it and the manifest is older
    /// than the grace window, and the grace window is longer than the budget.
    /// A call that finishes a fork can find a manifest that's already that
    /// old, and write this snapshot on a manifest that a delete and a
    /// collection removed during the call. The namespace is deleted then, and
    /// its collection leaves the snapshot to retirement
    /// (`NamespaceRoots::missing_manifest` in `gc/roots.rs`).
    pub(crate) async fn write_fork_point_snapshot(
        &self,
        newest: &Held<LoadedManifest>,
        clock: &OperationClock,
    ) -> Result<()> {
        let payload = newest.manifest().payload();
        let fork_basis = payload
            .fork_basis
            .as_ref()
            .expect("a fork's manifest records its fork basis");
        assert_eq!(
            fork_basis.fork_point_snapshot_id.manifest_no(),
            payload.manifest_no,
            "the fork point snapshot is on the fork's manifest at the fork point"
        );
        let manifest = newest.manifest().manifest();
        write_snapshot_record(
            &self.store,
            &SnapshotPayload {
                snapshot_id: fork_basis.fork_point_snapshot_id.clone(),
                namespace_id: self.namespace_id.clone(),
                head_seq: manifest.head_seq,
                payload_checksum: manifest.payload_checksum,
                created_at_ms: clock.request_at_ms(),
                owner: SnapshotOwner::Fork {
                    target_namespace_id: self.namespace_id.clone(),
                },
            },
            clock,
        )
        .await?;
        Ok(())
    }

    /// Loads the unexpired snapshot `snapshot_id` of this namespace, and
    /// checks that `source`, a manifest of this namespace that the caller
    /// read, is live. The caller decides when to read `source` again.
    async fn load_snapshot_fork_basis(
        &self,
        snapshot_id: &SnapshotId,
        source: &Held<LoadedManifest>,
        clock: &OperationClock,
    ) -> Result<SnapshotPayload> {
        let source = source.manifest().payload();
        crate::namespace::control::ensure_namespace_live(&source.namespace_id, source.status)?;
        let snapshot = self.load_user_snapshot(snapshot_id).await?;
        Ok(ensure_live(snapshot, clock.now_ms())?.state)
    }
}
