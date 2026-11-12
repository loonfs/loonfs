//! The roots of a pool's members, which protect the pool's content: the
//! upload sessions, and each member's current manifest, WAL tail, and the
//! manifests that its snapshots keep, read in the walk over fork snapshots that finds the members.

use super::uploads::{ListedUpload, ListedUploadSession};
use crate::error::{CoreError, Result};
use crate::gc::charged_set::{ChargedMap, ChargedQueue, ChargedSet, CollectionMemory};
use crate::gc::live_set::RetirementStatus;
use crate::gc::session_cache::{CompletedUploadSession, CompletedUploadSessionCache};
use crate::gc::snapshot_manifests::{missing_root_manifest, ListedSnapshotKey, SnapshotManifest};
use crate::limits::{NAMESPACE_RETIREMENT_GRACE_WINDOW_MS, POOL_ROOTS_READ_BUDGET_MS};
use crate::namespace::control::{LoadedHint, LoadedManifest};
use crate::namespace::read_anchor::project_anchor_tail;
use crate::namespace::store::NamespaceStore;
use crate::pool_store::PoolStore;
use crate::segment::cache::read_working_memory;
use crate::time::{Observation, OperationClock};
use futures::TryStreamExt;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{SnapshotOwner, SnapshotPayload};
use loonfs_types::format::manifest::{MetadataRowFamily, MetadataSegmentRef};
use loonfs_types::{ContentId, NamespaceId, PoolId, UploadId};
use std::sync::Arc;

/// How far the collection has read one member of the pool.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberState {
    /// The member is the pool's first member, or the target of a fork snapshot
    /// that the walk listed, and the collection hasn't read its roots.
    Found,
    /// The collection has read the member's roots, and its retirement status by the
    /// collection's clock.
    Read { status: RetirementStatus },
    /// The member is the target of a fork snapshot, and it has no current
    /// manifest, because its installation hasn't finished, or it failed. It
    /// has no roots, and no commit can be published in it.
    Absent,
}

/// How the collection's walk over the pool's members ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MemberWalkOutcome {
    /// The collection read the roots of every member that the walk found.
    Complete,
    /// [`POOL_ROOTS_READ_BUDGET_MS`] passed before the collection read every
    /// member's roots.
    BudgetExceeded,
    /// The roots of one member couldn't be read.
    UnreadableMember,
}

/// What the collection read of the pool's members.
#[derive(Clone, Copy, Debug)]
pub(super) struct MemberWalkReport {
    pub(super) outcome: MemberWalkOutcome,
    /// The pool's first member, and every fork snapshot target that the walk
    /// found, including a target whose installation hasn't finished.
    pub(super) members: usize,
    /// The members whose roots the collection read. A fork snapshot target without a
    /// current manifest has no roots, and it counts as read.
    pub(super) members_read: usize,
}

/// The roots of a pool's members, charged to the read working memory.
pub(super) struct PoolRoots {
    pub(super) content_ids: ChargedSet<ContentId>,
    /// Each upload session that was completed when the collection listed the
    /// sessions, keyed by upload ID. An outcome record can't change, so the
    /// session sweep uses these facts instead of reading the records again.
    pub(super) completed_sessions: ChargedMap<UploadId, CompletedUploadSession>,
    /// The entries of the uploads prefix that the collection listed to root the
    /// sessions' content, which the session sweep decides from instead of
    /// listing the prefix again.
    pub(super) uploads: ChargedQueue<ListedUpload>,
    /// The layout segments that the members' rooted manifests list, keyed by
    /// object key, whose extent filters root the content that every rooted
    /// view refers to.
    pub(super) layout_segments: ChargedMap<String, MetadataSegmentRef>,
    pub(super) memory: CollectionMemory,
    pub(super) walk: MemberWalkReport,
    /// Every member that the walk has found.
    members: ChargedMap<NamespaceId, MemberState>,
    /// The members whose roots the collection hasn't read yet, in the order that
    /// the fork snapshot listings found them.
    member_queue: ChargedQueue<NamespaceId>,
    /// The configured grace, or the retirement grace when that's longer.
    retirement_grace_window_ms: u64,
    now_ms: u64,
}

/// What the collection read of one member.
struct MemberRead {
    status: RetirementStatus,
    /// The member's current manifest, whose layout segments the caller
    /// protects.
    current: LoadedManifest,
}

impl PoolRoots {
    /// Lists the pool's upload sessions, and then reads the roots of every
    /// member, starting from `first_member_id`, whose current manifest is
    /// `first_current`.
    ///
    /// The sessions, content IDs, layout segments, and members that the
    /// collection keeps are charged to the read working memory of the pool's
    /// segment cache as they arrive. The collection doesn't read the record of
    /// a completed session that `session_cache` has.
    pub(super) async fn load<S: ObjectStore>(
        pool: &PoolStore<S>,
        first_member_id: &NamespaceId,
        first_current: Result<(LoadedManifest, LoadedHint)>,
        observed: Observation,
        session_cache: Option<&CompletedUploadSessionCache>,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> Result<Self> {
        let memory = CollectionMemory::new(
            pool.pool_id.to_string(),
            read_working_memory(Some(pool.segment_cache.as_ref())),
        );
        let mut roots = Self {
            content_ids: ChargedSet::new(&memory),
            completed_sessions: ChargedMap::new(&memory),
            uploads: ChargedQueue::new(&memory),
            layout_segments: ChargedMap::new(&memory),
            walk: MemberWalkReport {
                outcome: MemberWalkOutcome::Complete,
                members: 0,
                members_read: 0,
            },
            members: ChargedMap::new(&memory),
            member_queue: ChargedQueue::new(&memory),
            memory,
            retirement_grace_window_ms: grace_window_ms.max(NAMESPACE_RETIREMENT_GRACE_WINDOW_MS),
            now_ms: clock.request_at_ms(),
        };
        let outcome = async {
            // Sessions are listed before any WAL tail is read. A session that
            // this collection misses is newer than the collection, or it ended
            // before the tail read and the tail or a rooted manifest names
            // every commit of its content.
            pool.load_upload_sessions(session_cache, |listed, session| {
                match session {
                    Some(ListedUploadSession::Completed { upload_id, session }) => {
                        roots.content_ids.insert(session.content_id.clone())?;
                        roots.completed_sessions.insert(upload_id, session)?;
                    }
                    Some(ListedUploadSession::Incomplete { content_id }) => {
                        roots.content_ids.insert(content_id)?;
                    }
                    None => {}
                }
                roots.uploads.push_back(listed)
            })
            .await?;
            roots
                .load_members(pool, first_member_id, first_current, observed, clock)
                .await
        }
        .await;
        tracing::info!(pool_id = %pool.pool_id,
            content_id_bytes = roots.content_ids.reserved_bytes(),
            completed_session_bytes = roots.completed_sessions.reserved_bytes(),
            layout_segment_bytes = roots.layout_segments.reserved_bytes(),
            member_bytes = roots.members.reserved_bytes() + roots.member_queue.reserved_bytes(),
            live_set_bytes = roots.memory.held_bytes(),
            read_memory_limit_bytes = roots.memory.limit(),
            complete = outcome.is_ok(),
            "read the pool's roots");
        outcome?;
        Ok(roots)
    }

    /// Reads the roots of the first member, and then of every other member in
    /// the order that the walk over fork snapshots finds them.
    ///
    /// The walk reads each member once, one member at a time. It stops at the
    /// first member whose roots can't be read, and before a member that it
    /// would start after [`POOL_ROOTS_READ_BUDGET_MS`] has passed since the
    /// collection's clock started. Either way the collection skips the content
    /// sweep and continues. A charge to the read working memory that fails
    /// ends the collection.
    ///
    /// A first member that's eligible for retirement has no forks, and its
    /// tombstone protects nothing, so the pool then has no member roots.
    async fn load_members<S: ObjectStore>(
        &mut self,
        pool: &PoolStore<S>,
        first_member_id: &NamespaceId,
        first_current: Result<(LoadedManifest, LoadedHint)>,
        observed: Observation,
        clock: &OperationClock,
    ) -> Result<()> {
        // The first member is found before its snapshot listing, so that a fork snapshot
        // can't add it to the walk's queue.
        self.members
            .insert(first_member_id.clone(), MemberState::Found)?;
        self.walk.members = 1;
        let walked = async {
            let first = pool.member(first_member_id.clone());
            let read = self.read_member(&first, first_current?, observed).await?;
            if read.status != RetirementStatus::Eligible {
                self.protect_layout_segments(&read.current)?;
            }
            self.members.insert(
                first_member_id.clone(),
                MemberState::Read {
                    status: read.status,
                },
            )?;
            self.walk.members_read += 1;
            while let Some(member_id) = self.member_queue.pop_front() {
                if clock.elapsed_ms() > POOL_ROOTS_READ_BUDGET_MS {
                    return Ok(MemberWalkOutcome::BudgetExceeded);
                }
                let member = pool.member(member_id.clone());
                let state = self.load_member(&member, &pool.pool_id).await?;
                self.members.insert(member_id, state)?;
                self.walk.members_read += 1;
            }
            // A member read that ends after the budget is as late as one that
            // starts after it.
            Ok::<_, CoreError>(
                if self.walk.members > 1 && clock.elapsed_ms() > POOL_ROOTS_READ_BUDGET_MS {
                    MemberWalkOutcome::BudgetExceeded
                } else {
                    MemberWalkOutcome::Complete
                },
            )
        }
        .await;
        self.walk.outcome = match walked {
            Ok(outcome) => outcome,
            Err(error @ CoreError::LiveSetExceedsReadMemory { .. }) => return Err(error),
            Err(error) => {
                tracing::warn!(
                    pool_id = %pool.pool_id,
                    error = %error,
                    "a pool member's roots couldn't be read; skipping the pool's content sweep"
                );
                MemberWalkOutcome::UnreadableMember
            }
        };
        Ok(())
    }

    /// Reads the roots of `member`, a fork snapshot's target, and returns the
    /// member's state.
    ///
    /// A fork snapshot's target has no current manifest until its installation
    /// finishes, and it has no roots and no forks until then.
    async fn load_member<S: ObjectStore>(
        &mut self,
        member: &NamespaceStore<S>,
        pool_id: &PoolId,
    ) -> Result<MemberState> {
        let observed = Observation::now(Arc::clone(&member.timer));
        let current = match member.load_current_manifest_with_hint().await {
            Ok(current) => current,
            Err(CoreError::NamespaceNotFound { .. }) => return Ok(MemberState::Absent),
            Err(error) => return Err(error),
        };
        let recorded = &current.0.payload().pool_id;
        if recorded != pool_id {
            return Err(CoreError::NamespaceCorrupt(format!(
                "fork `{}` of pool `{pool_id}` records pool `{recorded}`",
                member.namespace_id
            )));
        }
        let read = self.read_member(member, current, observed).await?;
        self.protect_layout_segments(&read.current)?;
        Ok(MemberState::Read {
            status: read.status,
        })
    }

    /// Adds the content IDs of the WAL tail above `current`, the layout
    /// segments of the manifests that the member's snapshots keep, and the targets
    /// of its fork snapshots, and returns the member's retirement status and
    /// current manifest.
    ///
    /// A snapshot whose manifest is missing makes the member's roots unreadable,
    /// because only the member's own collection decides about its snapshots.
    async fn read_member<S: ObjectStore>(
        &mut self,
        member: &NamespaceStore<S>,
        (manifest, hint): (LoadedManifest, LoadedHint),
        observed: Observation,
    ) -> Result<MemberRead> {
        let anchor = member
            .load_read_anchor_from_manifest(manifest, hint, observed)
            .await?;
        let head = anchor.manifest.payload();
        // A tombstone's runs cover its whole WAL, so it has no tail.
        if !head.status.is_deleted() {
            let tail = project_anchor_tail(&member.store, &member.segment_cache, &anchor)?;
            for revision in tail.revisions() {
                self.content_ids
                    .insert(revision.content_ref.content_id.clone())?;
            }
            for row in tail.content_layouts() {
                for extent in &row.layout.extents {
                    self.content_ids.insert(extent.content_id.clone())?;
                }
            }
        }
        let mut has_snapshots = false;
        let mut snapshots = member.list_snapshot_manifests(head.manifest_no);
        while let Some(listed) = snapshots.try_next().await? {
            has_snapshots = true;
            let ListedSnapshotKey::Snapshot(snapshot) = listed else {
                continue;
            };
            if let Some(SnapshotPayload {
                owner:
                    SnapshotOwner::Fork {
                        target_namespace_id,
                    },
                ..
            }) = &snapshot.record
            {
                self.add_fork_target(target_namespace_id)?;
            }
            match &snapshot.manifest {
                SnapshotManifest::Loaded(manifest) => self.protect_layout_segments(manifest)?,
                SnapshotManifest::Rooted => {}
                SnapshotManifest::Missing(manifest_no) => {
                    return Err(missing_root_manifest(
                        &member.namespace_id,
                        *manifest_no,
                        &snapshot.key,
                    ));
                }
            }
        }
        drop(snapshots);
        let status = RetirementStatus::of(
            head.status.is_deleted().then_some(head),
            has_snapshots,
            self.retirement_grace_window_ms,
            self.now_ms,
        );
        Ok(MemberRead {
            status,
            current: anchor.manifest,
        })
    }

    /// Keeps the layout segments that `manifest` lists.
    ///
    /// A tombstone's layout segments are kept too, because a deleted member's
    /// references protect content until the member is retired.
    fn protect_layout_segments(&mut self, manifest: &LoadedManifest) -> Result<()> {
        for segment in manifest
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
            .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
        {
            let key = metadata_segment_object_key(segment);
            if self.layout_segments.get(&key).is_none() {
                self.layout_segments.insert(key, segment.clone())?;
            }
        }
        Ok(())
    }

    /// Adds `target_namespace_id`, the target of a fork snapshot, to the members
    /// that the walk has found and to the back of the walk's queue, unless the
    /// walk has found it already.
    fn add_fork_target(&mut self, target_namespace_id: &NamespaceId) -> Result<()> {
        if self.members.get(target_namespace_id).is_some() {
            return Ok(());
        }
        self.members
            .insert(target_namespace_id.clone(), MemberState::Found)?;
        self.member_queue.push_back(target_namespace_id.clone())?;
        self.walk.members += 1;
        Ok(())
    }

    /// Returns the retirement status of `namespace_id`, the namespace of an
    /// upload session that the collection sweeps, or `None` when the collection can't
    /// decide it.
    ///
    /// The collection can't decide the status of a member whose roots it hasn't
    /// read, or of a namespace that the walk didn't find when the walk
    /// stopped early. A member without a current manifest, and a namespace
    /// that a finished walk didn't find, are eligible, because no commit can
    /// be published in the first, and only retirement deletes the fork snapshot
    /// that would find the second. That holds for a namespace with a session that
    /// the collection listed before the walk, because a namespace has a current
    /// manifest before it can create a session. A fork installed after the
    /// walk read its source's snapshots isn't found either, so a session that the
    /// collection didn't list as completed before the walk can't rely on this status
    /// to skip the completed-content grace.
    pub(super) fn find_retirement_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<RetirementStatus> {
        match self.members.get(namespace_id) {
            Some(MemberState::Read { status }) => Some(*status),
            Some(MemberState::Absent) => Some(RetirementStatus::Eligible),
            Some(MemberState::Found) => None,
            None => match self.walk.outcome {
                MemberWalkOutcome::Complete => Some(RetirementStatus::Eligible),
                MemberWalkOutcome::BudgetExceeded | MemberWalkOutcome::UnreadableMember => None,
            },
        }
    }
}
