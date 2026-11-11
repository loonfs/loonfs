//! Manifests, runs, WAL objects, and content protected by the current manifest,
//! pins, and upload sessions, and on a family root by the roots of every other
//! member of its fork family.

use super::charged_set::{ChargedMap, ChargedQueue, ChargedSet, CollectionMemory};
use super::families::CandidateFamily;
use super::fork_family::{FamilyWalkReport, MemberState};
use super::pins::PinSweep;
use super::session_cache::{CompletedUploadSession, CompletedUploadSessionCache};
use super::uploads::{ListedUpload, ListedUploadSession};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::NAMESPACE_RETIREMENT_GRACE_WINDOW_MS;
use crate::manifest::load_namespace_manifest_envelope_if_present;
use crate::namespace::control::LoadedManifest;
use crate::namespace::read_anchor::{project_anchor_tail, NamespaceReadAnchor};
use crate::namespace::store::NamespaceStore;
use crate::segment::cache::read_working_memory;
use crate::snapshot::record::pin_key_ids;
use crate::time::Observation;
use crate::time::OperationClock;
use crate::wal::{live_folded_wal_no, object_is_required};
use futures::{future, TryStreamExt};
use loonfs_objectstore::keys::{
    extension_segment_object_key, metadata_manifest_object, metadata_segment_object_key, pin_prefix,
};
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_types::format::control::{PinOwner, PinPayload};
use loonfs_types::format::manifest::{
    MetadataRowFamily, MetadataSegmentRef, NamespaceManifestPayload,
};
use loonfs_types::{ContentId, ManifestNo, NamespaceId, PoolId, UploadId, WalNo};
use std::collections::BTreeSet;
use std::pin::pin;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetirementStatus {
    Active,
    Retained { reclaimable_at_ms: Option<u64> },
    Eligible,
}

impl RetirementStatus {
    /// Returns the [retirement](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#95-retirement)
    /// status at `now_ms` of a namespace whose current manifest is `tombstone`
    /// when the namespace is deleted, and whose complete pin listing returned
    /// a key when `pinned`.
    pub(super) fn of(
        tombstone: Option<&NamespaceManifestPayload>,
        pinned: bool,
        retirement_grace_window_ms: u64,
        now_ms: u64,
    ) -> Self {
        let Some(tombstone) = tombstone else {
            return Self::Active;
        };
        let deadline_ms = retirement_deadline_ms(tombstone, retirement_grace_window_ms);
        if now_ms < deadline_ms {
            Self::Retained {
                reclaimable_at_ms: Some(deadline_ms),
            }
        } else if pinned {
            Self::Retained {
                reclaimable_at_ms: None,
            }
        } else {
            Self::Eligible
        }
    }
}

/// Whose roots one read adds to the live set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Roots {
    /// The namespace that the collection collects, when it was forked from another
    /// namespace.
    CollectedFork,
    /// The namespace that the collection collects, when it's the root of its fork
    /// family.
    CollectedFamilyRoot,
    /// Another member of the fork family whose root the collection collects.
    Member,
}

impl Roots {
    /// Returns whether the roots protect the namespace's own manifests and
    /// segments, which only the namespace's own collection collects.
    fn protects_own_objects(self) -> bool {
        self != Self::Member
    }

    /// Returns whether the read finds the namespace's forks through its pin
    /// records, which only the collection on a family root needs.
    fn finds_forks(self) -> bool {
        self != Self::CollectedFork
    }
}

/// The pins that one namespace's pin listing returned.
pub(super) struct ListedPins {
    /// Whether the listing returned any key, recognized or not, which keeps a
    /// tombstone from retirement.
    pub(super) any_key: bool,
}

pub(super) struct LiveSet {
    /// Whether the collected namespace is a fork or the root of its fork
    /// family. Only the collection on a family root collects the content and
    /// upload records that the pool's prefix stores for the whole family.
    pub(super) collected: Roots,
    /// The pool that the collected namespace's current manifest records, whose
    /// prefix stores the content and upload records that the collection on a
    /// family root collects.
    pub(super) pool_id: PoolId,
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: ChargedSet<String>,
    pub(super) content_ids: ChargedSet<ContentId>,
    /// Each upload session that was completed when the collection listed the
    /// sessions, keyed by upload ID. An outcome record can't change, so the
    /// session sweep uses these facts instead of reading the records again.
    pub(super) completed_sessions: ChargedMap<UploadId, CompletedUploadSession>,
    /// The entries of the uploads prefix that the collection listed to root the
    /// sessions' content, which the session sweep decides from instead of
    /// listing the prefix again. Only the collection on a family root lists them.
    pub(super) uploads: ChargedQueue<ListedUpload>,
    /// The records of the namespace's own pins that the collection read while it
    /// loaded its roots, keyed by object key, which the pin sweep uses
    /// instead of reading each record again. The collection on a family root reads
    /// every record of its own pins to find its forks.
    pub(super) pin_records: ChargedMap<String, PinPayload>,
    /// The layout segments that the rooted manifests list, keyed by object
    /// key, whose extent filters root the content that every rooted view
    /// refers to.
    pub(super) layout_segments: ChargedMap<String, MetadataSegmentRef>,
    pub(super) memory: CollectionMemory,
    /// What the collection read of its fork family's roots.
    pub(super) family: FamilyWalkReport,
    /// The collected namespace, and on a family root's collection every other
    /// member of the fork family that the walk has found.
    pub(super) members: ChargedMap<NamespaceId, MemberState>,
    /// The members whose roots the collection hasn't read yet, in the order that
    /// the fork pin listings found them.
    pub(super) member_queue: ChargedQueue<NamespaceId>,
    has_pins: bool,
    /// The configured grace, or the retirement grace when that's longer.
    pub(super) retirement_grace_window_ms: u64,
    pub(super) now_ms: u64,
    live_folded_wal_no: Option<WalNo>,
}

impl LiveSet {
    /// Loads roots, their segments, and the content they refer to: the
    /// current manifest, the WAL tail above it, and the manifests that pins
    /// keep. On a family root, it then loads the same roots of every other
    /// member of the fork family. Reads go through the namespace's segment
    /// cache, with up to `STORE_READS_IN_FLIGHT` root manifests loading at
    /// once, and the object keys, content IDs, listed upload records,
    /// completed sessions, layout segments, and fork family members that the
    /// collection keeps are charged to its read working memory as they
    /// arrive. The collection doesn't read the record of a completed session that
    /// `session_cache` has.
    pub(super) async fn load<S: ObjectStore>(
        namespace: &NamespaceStore<S>,
        session_cache: Option<&CompletedUploadSessionCache>,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> Result<Self> {
        let namespace_id = &namespace.namespace_id;
        let observed = Observation::now(Arc::clone(&namespace.timer));
        let (manifest, hint) = namespace.load_current_manifest_with_hint().await?;
        let roots = if manifest.payload().fork_basis.is_none() {
            Roots::CollectedFamilyRoot
        } else {
            Roots::CollectedFork
        };
        let pool_id = manifest.payload().pool_id.clone();
        // Sessions are listed before the WAL tail is read. A session this collection
        // misses is newer than the collection, or it ended before the tail read and
        // the tail or a rooted manifest names every commit of its content. Only
        // a family root's collection sweeps its pool's upload sessions, and a
        // deleted family root's collection still sweeps content, so its sessions
        // are still roots.
        let memory = CollectionMemory::new(
            namespace_id,
            read_working_memory(Some(namespace.segment_cache.as_ref())),
        );
        let mut content_ids = ChargedSet::new(&memory);
        let mut completed_sessions = ChargedMap::new(&memory);
        let mut uploads = ChargedQueue::new(&memory);
        if roots == Roots::CollectedFamilyRoot {
            namespace
                .load_upload_sessions(&pool_id, session_cache, |listed, session| {
                    match session {
                        Some(ListedUploadSession::Completed { upload_id, session }) => {
                            content_ids.insert(session.content_id.clone())?;
                            completed_sessions.insert(upload_id, session)?;
                        }
                        Some(ListedUploadSession::Incomplete { content_id }) => {
                            content_ids.insert(content_id)?;
                        }
                        None => {}
                    }
                    uploads.push_back(listed)
                })
                .await?;
        }
        let anchor = namespace
            .load_read_anchor_from_manifest(manifest, hint, observed)
            .await?;
        let head = anchor.manifest.payload();
        let mut live = Self {
            collected: roots,
            pool_id,
            current_tombstone: head.status.is_deleted().then(|| head.clone()),
            discovery_start_manifest_no: anchor.hint.state.manifest_no,
            objects: ChargedSet::new(&memory),
            content_ids,
            completed_sessions,
            uploads,
            pin_records: ChargedMap::new(&memory),
            family: FamilyWalkReport::own_roots(),
            members: ChargedMap::new(&memory),
            member_queue: ChargedQueue::new(&memory),
            layout_segments: ChargedMap::new(&memory),
            memory,
            has_pins: false,
            retirement_grace_window_ms: grace_window_ms.max(NAMESPACE_RETIREMENT_GRACE_WINDOW_MS),
            now_ms: clock.request_at_ms(),
            live_folded_wal_no: live_folded_wal_no(&anchor.read_state),
        };
        let outcome = live
            .load_roots(namespace, &anchor, roots, grace_window_ms, clock)
            .await;
        tracing::info!(namespace_id = %namespace_id,
            object_key_bytes = live.objects.reserved_bytes(),
            content_id_bytes = live.content_ids.reserved_bytes(),
            completed_session_bytes = live.completed_sessions.reserved_bytes(),
            pin_record_bytes = live.pin_records.reserved_bytes(),
            layout_segment_bytes = live.layout_segments.reserved_bytes(),
            member_bytes = live.members.reserved_bytes() + live.member_queue.reserved_bytes(),
            live_set_bytes = live.memory.held_bytes(),
            read_memory_limit_bytes = live.memory.limit(),
            complete = outcome.is_ok(),
            "read the live set's roots");
        outcome?;
        Ok(live)
    }

    /// Adds the roots of the collected namespace, and on a family root the
    /// roots of every other member of the fork family.
    async fn load_roots<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        anchor: &NamespaceReadAnchor,
        roots: Roots,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> Result<()> {
        // The collected namespace is a member before its pin listing, so that
        // a fork pin can't add it to the walk's queue.
        self.members
            .insert(namespace.namespace_id.clone(), MemberState::Collected)?;
        let pins = self
            .load_namespace_roots(namespace, anchor, roots, grace_window_ms, clock)
            .await?;
        self.has_pins = pins.any_key;
        if roots == Roots::CollectedFamilyRoot {
            self.load_family_roots(namespace, grace_window_ms, clock)
                .await?;
        }
        Ok(())
    }

    /// Adds the roots of one namespace whose current manifest `anchor`
    /// loaded: that manifest, the WAL tail above it, and the manifests that
    /// the namespace's pins keep. It returns what the pin listing found.
    pub(super) async fn load_namespace_roots<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        anchor: &NamespaceReadAnchor,
        roots: Roots,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> Result<ListedPins> {
        let head = anchor.manifest.payload();
        // A tombstone roots its runs like any current manifest, so a deleted
        // namespace's references protect its content until retirement.
        self.protect_manifest(&anchor.manifest, roots)?;
        // A tombstone's runs cover its whole WAL, so it has no tail.
        if !head.status.is_deleted() {
            let tail = project_anchor_tail(&namespace.store, &namespace.segment_cache, anchor)?;
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
        let mut rooted = BTreeSet::from([head.manifest_no]);
        self.load_pinned_manifests(namespace, roots, grace_window_ms, clock, &mut rooted)
            .await
    }

    /// Protects the manifest of each listed pin, loading up to
    /// `STORE_READS_IN_FLIGHT` manifests at once, and adds each loaded number
    /// to `rooted`. When `roots` finds forks, it also reads each pin record
    /// and adds each fork pin's target to the walk as the listing streams,
    /// and it keeps the records of the collected namespace's own pins for the
    /// pin sweep.
    ///
    /// The first listed pin for a manifest number loads that manifest, and
    /// later pins for the number use the result. A pin whose manifest is
    /// missing fails the collection unless pin cleanup would delete it. Another
    /// member's collection decides about that member's pins, so there a missing
    /// manifest makes the member's roots unreadable.
    async fn load_pinned_manifests<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        roots: Roots,
        grace_window_ms: u64,
        clock: &OperationClock,
        rooted: &mut BTreeSet<ManifestNo>,
    ) -> Result<ListedPins> {
        let prefix = pin_prefix(&namespace.namespace_id);
        let mut any_key = false;
        let mut requested = rooted.clone();
        {
            // `try_buffered` keeps listing order, so a pin that reuses an
            // earlier pin's load arrives after that load's result.
            let mut pins = pin!(namespace
                .store
                .list_prefix_stream(&prefix)
                .map_err(|error| CoreError::store(&prefix, &error))
                .try_filter_map(|key| {
                    any_key = true;
                    future::ready(
                        pinned_manifest_no(&key)
                            .map(|manifest_no| manifest_no.map(|manifest_no| (key, manifest_no))),
                    )
                })
                .map_ok(|(key, manifest_no)| {
                    let first = requested.insert(manifest_no);
                    async move {
                        let manifest = async {
                            if first {
                                namespace.load_root_manifest(manifest_no).await.map(Some)
                            } else {
                                Ok(None)
                            }
                        };
                        let record = async {
                            if roots.finds_forks() {
                                namespace.load_listed_pin(&key).await
                            } else {
                                Ok(None)
                            }
                        };
                        let (loaded, record) = futures::try_join!(manifest, record)?;
                        Ok((key, manifest_no, loaded, record))
                    }
                })
                .try_buffered(STORE_READS_IN_FLIGHT));
            while let Some((key, manifest_no, loaded, record)) = pins.try_next().await? {
                if let Some(record) = record {
                    if let PinOwner::Fork {
                        target_namespace_id,
                    } = &record.owner
                    {
                        self.add_fork_target(target_namespace_id)?;
                    }
                    if roots == Roots::CollectedFamilyRoot {
                        self.pin_records.insert(key.clone(), record)?;
                    }
                }
                if let Some(Some(manifest)) = loaded {
                    self.protect_manifest(&manifest, roots)?;
                    rooted.insert(manifest_no);
                }
                if rooted.contains(&manifest_no) {
                    continue;
                }
                if roots == Roots::Member {
                    return Err(missing_root_manifest(
                        &namespace.namespace_id,
                        manifest_no,
                        &key,
                    ));
                }
                // A failed pin installation can outlive both its basis and its
                // cleanup attempt. Another collector can also remove a released
                // pin's basis after this collection listed it. Every pin for a missing
                // manifest is checked on its own, because another pin for the
                // number can still require retention when an earlier pin was
                // deletable.
                match namespace
                    .sweep_pin(&key, grace_window_ms, self, clock)
                    .await?
                {
                    PinSweep::Retain { .. } => {
                        return Err(missing_root_manifest(
                            &namespace.namespace_id,
                            manifest_no,
                            &key,
                        ));
                    }
                    PinSweep::Gone | PinSweep::DeleteUser | PinSweep::DeleteFork => {}
                }
            }
        }
        Ok(ListedPins { any_key })
    }

    /// Keeps the layout segments that `manifest` lists, and keeps the
    /// manifest and its metadata and extension segments themselves when
    /// `roots` protects the namespace's own objects.
    ///
    /// Every root manifest contributes its layout segments, a tombstone
    /// included, because a deleted member's references protect content until
    /// the member is retired.
    fn protect_manifest(&mut self, manifest: &LoadedManifest, roots: Roots) -> Result<()> {
        if roots.protects_own_objects() {
            self.objects.insert(manifest.object_key.clone())?;
            for segment in manifest.payload().runs.iter().flat_map(|run| &run.segments) {
                self.objects.insert(metadata_segment_object_key(segment))?;
            }
            for (name, entry) in &manifest.payload().extensions {
                for segment in entry.segments() {
                    self.objects
                        .insert(extension_segment_object_key(name, segment))?;
                }
            }
        }
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

    /// Returns whether the collection keeps the object of `family` at `key`, because
    /// a live root references the object, discovery can still read it, or a
    /// live head hasn't folded it.
    pub(super) fn protects(&self, family: CandidateFamily, key: &str) -> bool {
        self.objects.contains(key)
            || match family {
                CandidateFamily::Manifests => manifest_no_of(key)
                    .is_some_and(|number| number >= self.discovery_start_manifest_no),
                CandidateFamily::WalObjects => self
                    .live_folded_wal_no
                    .is_some_and(|folded_wal_no| object_is_required(key, folded_wal_no)),
                CandidateFamily::MetadataSegments
                | CandidateFamily::ExtensionSegments
                | CandidateFamily::Pins
                | CandidateFamily::Content
                | CandidateFamily::Temporary => false,
            }
    }

    /// Returns whether the collection collects the content, upload records, and
    /// temporary objects of its pool, which only the collection on a family
    /// root does.
    pub(super) fn collects_family_content(&self) -> bool {
        self.collected == Roots::CollectedFamilyRoot
    }

    pub(super) fn deadline(&self, tombstone: &NamespaceManifestPayload) -> u64 {
        retirement_deadline_ms(tombstone, self.retirement_grace_window_ms)
    }

    /// Returns the retirement status of the collected namespace.
    pub(super) fn retirement_status(&self) -> RetirementStatus {
        RetirementStatus::of(
            self.current_tombstone.as_ref(),
            self.has_pins,
            self.retirement_grace_window_ms,
            self.now_ms,
        )
    }
}

/// Returns the earliest time at which the namespace of `tombstone` can be
/// retired.
fn retirement_deadline_ms(
    tombstone: &NamespaceManifestPayload,
    retirement_grace_window_ms: u64,
) -> u64 {
    tombstone
        .status
        .deleted_at_ms()
        .expect("a tombstone should carry its deletion stamp")
        .saturating_add(retirement_grace_window_ms)
}

/// Returns the manifest number in a pin key, or `None` for a key that isn't
/// a pin record.
fn pinned_manifest_no(key: &str) -> Result<Option<ManifestNo>> {
    if !super::families::CandidateFamily::Pins.recognizes(key) {
        return Ok(None);
    }
    let (_, pin_id) = pin_key_ids(key).map_err(CoreError::ControlObjectLoad)?;
    Ok(Some(pin_id.manifest_no()))
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Loads one root manifest, or returns `None` when the manifest is missing.
    async fn load_root_manifest(&self, manifest_no: ManifestNo) -> Result<Option<LoadedManifest>> {
        let envelope = load_namespace_manifest_envelope_if_present(
            &self.store,
            &self.namespace_id,
            &manifest_no,
        )
        .await
        .map_err(MetadataProjectionLoadError::from)?;
        Ok(envelope.map(|(envelope, manifest_bytes)| LoadedManifest {
            object_key: metadata_manifest_object(&self.namespace_id, &manifest_no),
            manifest_bytes,
            envelope: Arc::new(envelope),
        }))
    }
}

fn missing_root_manifest(
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
    root_key: &str,
) -> CoreError {
    let key = metadata_manifest_object(namespace_id, &manifest_no);
    CoreError::NamespaceCorrupt(format!("root `{root_key}` pins missing manifest `{key}`"))
}
