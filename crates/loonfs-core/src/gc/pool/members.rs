//! The roots of a pool's members, found through the pool's member list: the
//! upload sessions, and each member's current manifest (until a deleted
//! member's retirement deadline), WAL tail, and the manifests that its
//! snapshots keep.

use super::uploads::{ListedUpload, ListedUploadSession};
use crate::error::{CoreError, Result};
use crate::gc::charged_set::{ChargedMap, ChargedQueue, ChargedSet, CollectionMemory};
use crate::gc::roots::{
    ListedSnapshot, ListedSnapshotKey, NamespaceRoots, RetirementStatus, SnapshotManifest,
};
use crate::gc::session_cache::{CompletedUploadSession, CompletedUploadSessionCache};
use crate::in_flight::POOL_MEMBER_READS_IN_FLIGHT;
use crate::limits::POOL_ROOTS_READ_BUDGET_MS;
use crate::namespace::control::{LoadedManifest, NewestManifest};
use crate::namespace::held::Held;
use crate::namespace::read_anchor::project_anchor_tail;
use crate::namespace::store::NamespaceStore;
use crate::pool_store::PoolStore;
use crate::time::{Observation, OperationClock};
use futures::stream::FuturesOrdered;
use futures::{StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{metadata_segment_object_key, pool_members_prefix};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRowFamily, MetadataSegmentRef};
use loonfs_types::{ContentId, NamespaceId, PoolId, UploadId};
use std::sync::{Arc, Mutex};

/// What a pool's collection knows of the namespace that created an upload
/// session, in the terms of the format spec's upload-session cleanup rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SessionNamespace {
    /// The collection keeps the namespace's sessions whatever their records
    /// say, until `reclaimable_at_ms`, or with no date when it's `None`. The
    /// collection keeps a session with no date when it stopped before it read
    /// every listed member, so it can't decide the namespace's status. It
    /// keeps a session of a deleted namespace whose retirement deadline
    /// hasn't passed until that deadline.
    KeepsSessions { reclaimable_at_ms: Option<u64> },
    /// A member that is being created or is active.
    Live,
    /// A namespace that the collection treats as retired, even while a fork's
    /// snapshot keeps it from retirement: a deleted member at or past its
    /// retirement deadline, or a namespace with no member entry in the
    /// collection's listing, or whose entry the collection deleted, after the
    /// collection read every listed member. A namespace without an entry was
    /// retired, isn't in the pool, or had its entry written after the
    /// listing.
    Retired,
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
    /// The retirement status of every listed member that the collection has
    /// read.
    members: ChargedMap<NamespaceId, RetirementStatus>,
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
                Ok(Some(status)) => self.members.insert(member_id, status)?,
                Ok(None) => {}
                Err(error @ CoreError::LiveSetExceedsCollectionMemory { .. }) => return Err(error),
                // Another process can collect a member while this collection
                // reads it, so an object that the read needs can be gone.
                Err(error) => {
                    tracing::warn!(
                        pool_id = %pool.pool_id,
                        namespace_id = %member_id,
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

    /// Returns what the collection knows of `namespace_id`, the namespace of
    /// an upload session that the collection sweeps.
    ///
    /// The collection can't decide the status of a listed member whose roots
    /// it hasn't read, so a namespace that it hasn't read has no status when
    /// the collection stopped before reading every listed member. A namespace
    /// that isn't in the map has no member entry, because the listing didn't
    /// include it or the collection deleted its entry. A session that the
    /// collection listed before the member listing can't belong to a
    /// namespace whose entry was written after, because a namespace has its
    /// entry before it's active. A session that the collection didn't list as
    /// completed before the member listing can't rely on this status to skip
    /// the completed-content grace.
    pub(super) fn find_session_namespace(&self, namespace_id: &NamespaceId) -> SessionNamespace {
        let Some(status) = self.members.get(namespace_id) else {
            return match self.member_read.outcome {
                MemberReadOutcome::Complete => SessionNamespace::Retired,
                MemberReadOutcome::BudgetExceeded | MemberReadOutcome::UnreadableMember => {
                    SessionNamespace::KeepsSessions {
                        reclaimable_at_ms: None,
                    }
                }
            };
        };
        match *status {
            RetirementStatus::Initializing { .. } | RetirementStatus::Active => {
                SessionNamespace::Live
            }
            RetirementStatus::BeforeDeadline { retires_at_ms } => SessionNamespace::KeepsSessions {
                reclaimable_at_ms: Some(retires_at_ms),
            },
            RetirementStatus::PastDeadline => SessionNamespace::Retired,
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
/// into `roots`, and returns the member's retirement status, or `None` for a
/// namespace that isn't a member.
///
/// An initializing namespace has no roots, and the collection reads nothing of
/// it beyond its newest manifest: LoonFS refuses commits to it, so no commit
/// that refers to the pool's content can be published in it, and a fork's
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
) -> Result<Option<RetirementStatus>> {
    let observed = Observation::now(Arc::clone(&member.timer));
    let reason = match member.find_newest_manifest(observed).await? {
        NewestManifest::Manifest(current) if current.manifest().payload().pool_id == *pool_id => {
            let found = NamespaceRoots::new(member, current.manifest().payload(), now_ms).status;
            let status = if matches!(found, RetirementStatus::Initializing { .. }) {
                found
            } else {
                read_member_roots(member, current, now_ms, roots).await?
            };
            return Ok(Some(status));
        }
        NewestManifest::Manifest(_) => "other_pool",
        NewestManifest::Retired => "retired",
        NewestManifest::Absent => "absent",
    };
    member.delete_member_entry(pool_id).await?;
    tracing::info!(pool_id = %pool_id, namespace_id = %member.namespace_id, reason,
        "deleted the member entry of a namespace that isn't a member");
    Ok(None)
}

/// Adds to `roots` the content IDs of the WAL tail above `current` (the
/// manifest that the member's discovery found), the layout segments of the
/// current manifest while its view is a root, and the layout segments of the
/// manifests that the member's snapshots keep, and returns the member's
/// retirement status.
///
/// A deleted member stays a member until its boundary is retired. The
/// tombstone's own view stops being a root at the retirement deadline, and
/// the manifests that the member's snapshots keep stay roots until the
/// snapshots are deleted.
///
/// A snapshot whose manifest is missing protects nothing, or makes the
/// member's roots unreadable (see [`NamespaceRoots::snapshots`]). Pool
/// collection doesn't delete a member's snapshots, because the member's own
/// collection deletes them.
async fn read_member_roots<S: ObjectStore>(
    member: &NamespaceStore<S>,
    current: Held<LoadedManifest>,
    now_ms: u64,
    roots: &Mutex<SharedRoots<'_>>,
) -> Result<RetirementStatus> {
    let anchor = member.load_read_anchor_from_manifest(current).await?;
    let member_roots = NamespaceRoots::new(member, anchor.current_manifest().payload(), now_ms);
    // A tombstone's runs cover its whole WAL, so it has no tail.
    if member_roots.status == RetirementStatus::Active {
        let tail = project_anchor_tail(&anchor)?;
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
    let mut snapshots = member_roots.snapshots();
    while let Some(listed) = snapshots.try_next().await? {
        if let ListedSnapshotKey::Snapshot(ListedSnapshot {
            manifest: SnapshotManifest::Loaded(manifest),
            ..
        }) = &listed
        {
            protect_layout_segments(manifest, roots)?;
        }
    }
    drop(snapshots);
    if member_roots.status.current_view_is_root() {
        protect_layout_segments(anchor.current_manifest(), roots)?;
    }
    Ok(member_roots.status)
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
