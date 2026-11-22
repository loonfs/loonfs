//! The roots of a pool's members, found through the pool's member list: the
//! upload sessions, and each member's current manifest (until a deleted
//! member's retirement deadline), WAL tail, and the manifests that its
//! snapshots keep.

use super::uploads::{ListedUpload, ListedUploadSession};
use crate::error::{CoreError, Result};
use crate::gc::charged_set::{ChargedMap, ChargedQueue, ChargedSet, CollectionMemory};
use crate::gc::live_set::{retirement_deadline_ms, RetirementStatus};
use crate::gc::session_cache::{CompletedUploadSession, CompletedUploadSessionCache};
use crate::gc::snapshot_manifests::{missing_root_manifest, ListedSnapshotKey, SnapshotManifest};
use crate::in_flight::POOL_MEMBER_READS_IN_FLIGHT;
use crate::limits::POOL_ROOTS_READ_BUDGET_MS;
use crate::namespace::basis::PublicationBasis;
use crate::namespace::control::{LoadedManifest, NewestManifest};
use crate::namespace::read_anchor::project_anchor_tail;
use crate::namespace::store::NamespaceStore;
use crate::pool_store::PoolStore;
use crate::time::{Observation, OperationClock};
use futures::stream::FuturesOrdered;
use futures::{StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{metadata_segment_object_key, pool_member, pool_members_prefix};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{NamespaceStatus, SnapshotOwner};
use loonfs_types::format::manifest::{MetadataRowFamily, MetadataSegmentRef};
use loonfs_types::{ContentId, NamespaceId, PoolId, UploadId};
use std::sync::{Arc, Mutex};

/// What the collection read of one member of the pool.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberState {
    /// The collection has read the member's roots, and its retirement status by the
    /// collection's clock.
    Read { status: RetirementStatus },
    /// The listed member is initializing, so it has no roots: LoonFS refuses
    /// commits to it, so no commit can be published in it that refers to the
    /// pool's content.
    NoRoots,
}

/// How the collection's read of the pool's members ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MemberReadOutcome {
    /// The collection read the roots of every member that the member list
    /// listed.
    Complete,
    /// [`POOL_ROOTS_READ_BUDGET_MS`] passed before the collection read every
    /// member's roots.
    BudgetExceeded,
    /// The roots of one member couldn't be read.
    UnreadableMember,
}

/// What the collection read of the pool's members.
#[derive(Clone, Copy, Debug)]
pub(super) struct MemberReadReport {
    pub(super) outcome: MemberReadOutcome,
    /// The member entries that the listing of the member list returned
    /// before the collection stopped reading.
    pub(super) members: usize,
    /// The listed entries that the collection read. An initializing member
    /// and a deleted stale entry count as read.
    pub(super) members_read: usize,
}

/// The roots of a pool's members, charged to the collection's budget.
pub(super) struct PoolRoots {
    pub(super) content_ids: ChargedSet<ContentId>,
    /// Each upload session that was completed when the collection listed the
    /// sessions, keyed by upload ID. An outcome record can't change, so the
    /// session sweep uses these facts instead of reading the records again.
    pub(super) completed_sessions: ChargedMap<UploadId, CompletedUploadSession>,
    /// The entries of the uploads prefix that the collection listed to root the
    /// sessions' content, which the session sweep decides from instead of
    /// listing the prefix again. A completed session isn't in the queue,
    /// because the session sweep decides it from `completed_sessions`.
    pub(super) uploads: ChargedQueue<ListedUpload>,
    /// The layout segments that the members' rooted manifests list, keyed by
    /// object key, whose extent filters root the content that every rooted
    /// view refers to.
    pub(super) layout_segments: ChargedMap<String, MetadataSegmentRef>,
    pub(super) memory: CollectionMemory,
    pub(super) member_read: MemberReadReport,
    /// The entries that the listings of the upload sessions and of the
    /// member list returned.
    pub(super) listed_objects: u64,
    /// The bytes of the entries that the listing of the member list returned,
    /// as the listing reported them.
    pub(super) member_list_bytes: u64,
    /// The bytes of the upload records that the listing of the uploads
    /// prefix returned, as the listing reported them.
    pub(super) upload_record_bytes: u64,
    /// Every listed member that the collection has read.
    members: ChargedMap<NamespaceId, MemberState>,
    now_ms: u64,
}

impl PoolRoots {
    /// Lists the pool's upload sessions and then the pool's member list, and
    /// reads the roots of every listed member.
    ///
    /// The sessions, content IDs, layout segments, and members that the
    /// collection keeps are charged to `memory` as they arrive. The
    /// collection doesn't read the record of a completed session that
    /// `session_cache` has.
    pub(super) async fn load<S: ObjectStore>(
        pool: &PoolStore<S>,
        session_cache: Option<&CompletedUploadSessionCache>,
        clock: &OperationClock,
        memory: CollectionMemory,
    ) -> Result<Self> {
        let mut roots = Self {
            content_ids: ChargedSet::new(&memory),
            completed_sessions: ChargedMap::new(&memory),
            uploads: ChargedQueue::new(&memory),
            layout_segments: ChargedMap::new(&memory),
            member_read: MemberReadReport {
                outcome: MemberReadOutcome::Complete,
                members: 0,
                members_read: 0,
            },
            listed_objects: 0,
            member_list_bytes: 0,
            upload_record_bytes: 0,
            members: ChargedMap::new(&memory),
            memory,
            now_ms: clock.request_at_ms(),
        };
        let outcome = async {
            // Sessions are listed before any WAL tail is read. A session that
            // this collection misses is newer than the collection, or it ended
            // before the tail read and the tail or a rooted manifest refers to
            // every commit of its content.
            roots.listed_objects += pool
                .load_upload_sessions(session_cache, |listed, session| {
                    roots.upload_record_bytes += listed.size_bytes;
                    match session {
                        Some(ListedUploadSession::Completed { upload_id, session }) => {
                            roots.content_ids.insert(session.content_id.clone())?;
                            roots.completed_sessions.insert(upload_id, session)
                        }
                        Some(ListedUploadSession::Incomplete { content_id }) => {
                            roots.content_ids.insert(content_id)?;
                            roots.uploads.push_back(listed)
                        }
                        None => roots.uploads.push_back(listed),
                    }
                })
                .await?;
            roots.load_members(pool, clock).await
        }
        .await;
        tracing::info!(pool_id = %pool.pool_id,
            content_id_bytes = roots.content_ids.reserved_bytes(),
            completed_session_bytes = roots.completed_sessions.reserved_bytes(),
            layout_segment_bytes = roots.layout_segments.reserved_bytes(),
            member_bytes = roots.members.reserved_bytes(),
            live_set_bytes = roots.memory.held_bytes(),
            live_set_limit_bytes = roots.memory.limit(),
            complete = outcome.is_ok(),
            "read the pool's roots");
        outcome?;
        Ok(roots)
    }

    /// Lists the pool's member list once, and reads the roots of up to
    /// [`POOL_MEMBER_READS_IN_FLIGHT`] listed members at once as the listing
    /// yields their entries.
    ///
    /// The collection reads each member once, and charges each root to its
    /// budget as the root arrives, whichever member's read it arrives from.
    /// It stops at a member whose roots can't be read, and before a member
    /// that it would start after [`POOL_ROOTS_READ_BUDGET_MS`] has passed
    /// since the collection's clock started. Either way the collection skips
    /// the content sweep and continues. A charge to the collection's budget
    /// that fails ends the collection, and so does a listing that fails. A
    /// key under the member list's prefix that isn't a member entry is logged
    /// and left alone.
    async fn load_members<S: ObjectStore>(
        &mut self,
        pool: &PoolStore<S>,
        clock: &OperationClock,
    ) -> Result<()> {
        let prefix = pool_members_prefix(&pool.pool_id);
        let mut entries = std::pin::pin!(pool.store.list_entries_from_stream(&prefix, None));
        let now_ms = self.now_ms;
        let roots = Mutex::new(SharedRoots {
            content_ids: &mut self.content_ids,
            layout_segments: &mut self.layout_segments,
        });
        let mut in_flight = FuturesOrdered::new();
        let mut listing_done = false;
        loop {
            while !listing_done && in_flight.len() < POOL_MEMBER_READS_IN_FLIGHT {
                let Some(entry) = entries
                    .try_next()
                    .await
                    .map_err(|error| CoreError::store(&prefix, &error))?
                else {
                    listing_done = true;
                    break;
                };
                self.listed_objects += 1;
                self.member_list_bytes += entry.size_bytes;
                let namespace_id = parse_object_key(&entry.key)
                    .filter(|parsed| parsed.family() == DurableObjectFamily::PoolMember)
                    .and_then(|parsed| parsed.identifier())
                    .and_then(|identifier| NamespaceId::parse(identifier).ok());
                let Some(member_id) = namespace_id else {
                    tracing::warn!(pool_id = %pool.pool_id, key = %entry.key,
                        "left alone a key under the pool's member list that isn't a member entry");
                    continue;
                };
                self.member_read.members += 1;
                if clock.elapsed_ms() > POOL_ROOTS_READ_BUDGET_MS {
                    self.member_read.outcome = MemberReadOutcome::BudgetExceeded;
                    return Ok(());
                }
                let member = pool.member(member_id.clone());
                let pool_id = pool.pool_id.clone();
                let roots = &roots;
                in_flight.push_back(async move {
                    let result = load_member_roots(&member, &pool_id, now_ms, roots).await;
                    (member_id, result)
                });
            }
            let Some((member_id, result)) = in_flight.next().await else {
                break;
            };
            match result {
                Ok(Some(state)) => self.members.insert(member_id, state)?,
                Ok(None) => {}
                Err(error @ CoreError::LiveSetExceedsCollectionMemory { .. }) => return Err(error),
                Err(error) => {
                    tracing::warn!(
                        pool_id = %pool.pool_id,
                        error = %error,
                        "a pool member's roots couldn't be read; skipping the pool's content sweep"
                    );
                    self.member_read.outcome = MemberReadOutcome::UnreadableMember;
                    return Ok(());
                }
            }
            self.member_read.members_read += 1;
            // A member read that ends after the budget is as late as one that
            // starts after it.
            if clock.elapsed_ms() > POOL_ROOTS_READ_BUDGET_MS {
                self.member_read.outcome = MemberReadOutcome::BudgetExceeded;
                return Ok(());
            }
        }
        Ok(())
    }

    /// Returns the retirement status of `namespace_id`, the namespace of an
    /// upload session that the collection sweeps, or `None` when the collection can't
    /// decide it.
    ///
    /// The collection can't decide the status of a listed member whose roots
    /// it hasn't read, so a namespace that it hasn't read has no status when
    /// the collection stopped before reading every listed member. A listed
    /// namespace without roots is eligible, because no commit that refers to
    /// the pool's content can be published in it. A namespace that isn't in
    /// the map has no member entry, because the listing didn't include it or
    /// the collection deleted its entry: it was retired, it isn't in the pool,
    /// or its entry was written after the listing, and it's eligible too. A session
    /// that the collection listed before the member listing can't belong to a
    /// namespace whose entry was written after, because a namespace has its
    /// entry before it's active. A session that the collection didn't list as
    /// completed before the member listing can't rely on this status to skip
    /// the completed-content grace.
    pub(super) fn find_retirement_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Option<RetirementStatus> {
        match self.members.get(namespace_id) {
            Some(MemberState::Read { status }) => Some(*status),
            Some(MemberState::NoRoots) => Some(RetirementStatus::Eligible),
            None => match self.member_read.outcome {
                MemberReadOutcome::Complete => Some(RetirementStatus::Eligible),
                MemberReadOutcome::BudgetExceeded | MemberReadOutcome::UnreadableMember => None,
            },
        }
    }
}

/// The pool's content IDs and layout segments while the member reads run.
/// The reads share them, so each root is deduplicated and charged to the
/// collection's budget as it arrives, as a session's content ID is. One task
/// polls the reads, so the lock is never contended.
struct SharedRoots<'roots> {
    content_ids: &'roots mut ChargedSet<ContentId>,
    layout_segments: &'roots mut ChargedMap<String, MetadataSegmentRef>,
}

fn lock<'guard, 'roots>(
    roots: &'guard Mutex<SharedRoots<'roots>>,
) -> std::sync::MutexGuard<'guard, SharedRoots<'roots>> {
    roots
        .lock()
        .expect("the pool roots lock should not be poisoned")
}

/// Reads the roots of `member`, a namespace that the pool's member list lists,
/// into `roots`, and returns the member's state, or `None` for a namespace
/// that isn't a member.
///
/// An initializing namespace has no roots, and the collection reads nothing of
/// it beyond its newest manifest: LoonFS refuses commits to it, and a fork's
/// manifest lists the source's segments, which the source's snapshot keeps. A
/// retired namespace, an ID without a manifest, and a namespace whose manifest
/// records another pool aren't members. The collection deletes their entries
/// as the listing yields them and keeps nothing of them, so stale entries take
/// none of the collection's budget.
async fn load_member_roots<S: ObjectStore>(
    member: &NamespaceStore<S>,
    pool_id: &PoolId,
    now_ms: u64,
    roots: &Mutex<SharedRoots<'_>>,
) -> Result<Option<MemberState>> {
    let observed = Observation::now(Arc::clone(&member.timer));
    let reason = match member.find_newest_manifest().await? {
        NewestManifest::Manifest(current) if current.manifest.payload().pool_id == *pool_id => {
            if let NamespaceStatus::Initializing {} = current.manifest.payload().status {
                return Ok(Some(MemberState::NoRoots));
            }
            let status = read_member_roots(member, current, observed, now_ms, roots).await?;
            return Ok(Some(MemberState::Read { status }));
        }
        NewestManifest::Manifest(_) => "other_pool",
        NewestManifest::Retired => "retired",
        NewestManifest::Absent => "absent",
    };
    let key = pool_member(pool_id, &member.namespace_id);
    member
        .store
        .delete(&key)
        .await
        .map_err(|error| CoreError::store(&key, &error))?;
    tracing::info!(pool_id = %pool_id, namespace_id = %member.namespace_id, reason,
        "deleted the member entry of a namespace that isn't a member");
    Ok(None)
}

/// Adds to `roots` the content IDs of the WAL tail above `current`, the
/// layout segments of the current manifest while its view is a root, and the
/// layout segments of the manifests that the member's snapshots keep, and
/// returns the member's retirement status.
///
/// A deleted member stays a member until its boundary is retired, so its
/// status is retained, with its retirement deadline while the deadline is
/// in the future. The tombstone's own view stops being a root at the
/// deadline, and the manifests that the member's snapshots keep stay roots
/// until the snapshots are deleted.
///
/// A snapshot whose manifest is missing makes the member's roots unreadable,
/// unless the snapshot's record is gone or the snapshot is a user snapshot.
/// (A user snapshot whose manifest is missing is a candidate that was never
/// verified, so it protects nothing.) Pool collection doesn't delete a
/// member's snapshots, because the member's own collection deletes them.
async fn read_member_roots<S: ObjectStore>(
    member: &NamespaceStore<S>,
    current: PublicationBasis,
    observed: Observation,
    now_ms: u64,
    roots: &Mutex<SharedRoots<'_>>,
) -> Result<RetirementStatus> {
    let anchor = member
        .load_read_anchor_from_manifest(current.manifest, current.boundary, observed, None)
        .await?;
    let head = anchor.manifest.payload();
    // A tombstone's runs cover its whole WAL, so it has no tail.
    if head.status.is_active() {
        let tail = project_anchor_tail(&member.store, &member.segment_cache, &anchor)?;
        let mut roots = lock(roots);
        for revision in tail.revisions() {
            roots
                .content_ids
                .insert(revision.content_ref.content_id.clone())?;
        }
        for row in tail.content_layouts() {
            for extent in &row.layout.extents {
                roots.content_ids.insert(extent.content_id.clone())?;
            }
        }
    }
    let mut snapshots = member.list_snapshot_manifests(head.manifest_no);
    while let Some(listed) = snapshots.try_next().await? {
        let ListedSnapshotKey::Snapshot(snapshot, ()) = listed else {
            continue;
        };
        match &snapshot.manifest {
            SnapshotManifest::Loaded(manifest) => protect_layout_segments(manifest, roots)?,
            SnapshotManifest::Rooted => {}
            SnapshotManifest::Missing(manifest_no) => {
                // A verified user snapshot protects its manifest from the
                // moment it's acknowledged, so a user snapshot whose manifest
                // is missing was never verified (format spec, "Snapshot
                // cleanup"). The listing reads no record, so the collection
                // reads this one record to find the snapshot's owner.
                let record = member.load_listed_snapshot_record(&snapshot.key).await?;
                let keeps_root = record
                    .is_some_and(|record| !matches!(record.owner, SnapshotOwner::User { .. }));
                if keeps_root {
                    return Err(missing_root_manifest(
                        &member.namespace_id,
                        *manifest_no,
                        &snapshot.key,
                    ));
                }
            }
        }
    }
    drop(snapshots);
    let status = if head.status.is_deleted() {
        RetirementStatus::Retained {
            retires_at_ms: Some(retirement_deadline_ms(head))
                .filter(|deadline_ms| now_ms < *deadline_ms),
        }
    } else {
        RetirementStatus::Active
    };
    if status.current_view_is_root() {
        protect_layout_segments(&anchor.manifest, roots)?;
    }
    Ok(status)
}

/// Keeps the layout segments that `manifest` lists, whose extent filters
/// root the content that the manifest's layout view refers to.
fn protect_layout_segments(
    manifest: &LoadedManifest,
    roots: &Mutex<SharedRoots<'_>>,
) -> Result<()> {
    let mut roots = lock(roots);
    for segment in manifest
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
    {
        let key = metadata_segment_object_key(segment);
        if roots.layout_segments.get(&key).is_none() {
            roots.layout_segments.insert(key, segment.clone())?;
        }
    }
    Ok(())
}
