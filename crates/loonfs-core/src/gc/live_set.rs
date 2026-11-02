//! Manifests, runs, WAL objects, and content protected by namespace retention,
//! pins, and upload sessions, and on a family root by the roots of every other
//! member of its fork family. Superseded manifests and their runs remain
//! rooted while their successor is younger than the pass's grace window or has
//! no provider timestamp.

use super::charged_set::{Charge, ChargedMap, ChargedQueue, ChargedSet, PassMemory};
use super::families::CandidateFamily;
use super::fork_family::{FamilyWalkReport, MemberState};
use super::pins::{GraceAge, PinSweep};
use super::session_cache::{CompletedUploadSession, CompletedUploadSessionCache};
use super::uploads::ListedUploadSession;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::heap_bytes::manifest_heap_bytes;
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::NAMESPACE_RETIREMENT_GRACE_WINDOW_MS;
use crate::manifest::load_namespace_manifest_envelope_if_present;
use crate::namespace::control::LoadedManifest;
use crate::namespace::read_anchor::{project_anchor_tail, NamespaceReadAnchor};
use crate::namespace::store::NamespaceStore;
use crate::pin::record::pin_key_ids;
use crate::segment::cache::read_working_memory;
use crate::time::Observation;
use crate::time::OperationClock;
use crate::wal::{live_folded_wal_no, object_is_required};
use futures::{future, stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{
    metadata_manifest_object, metadata_manifest_prefix, metadata_segment_object_key, pin_prefix,
};
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_types::format::control::{PinOwner, PinPayload};
use loonfs_types::format::manifest::{MetadataRowFamily, NamespaceManifestPayload};
use loonfs_types::{ContentId, ManifestNo, NamespaceId, UploadId, WalNo};
use std::collections::{BTreeMap, BTreeSet};
use std::pin::pin;
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RetirementStatus {
    Active,
    Retained { reclaimable_at_ms: Option<u64> },
    Eligible,
}

/// Whose roots one read adds to the live set.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Roots {
    /// The namespace that the pass collects, when it was forked from another
    /// namespace.
    CollectedFork,
    /// The namespace that the pass collects, when it's the root of its fork
    /// family.
    CollectedFamilyRoot,
    /// Another member of the fork family whose root the pass collects.
    Member,
}

impl Roots {
    /// Returns whether the roots protect the namespace's own manifests and
    /// segments, which only the namespace's own pass collects.
    fn protects_own_objects(self) -> bool {
        self != Self::Member
    }

    /// Returns whether the read finds the namespace's forks through its pin
    /// records, which only the pass on a family root needs.
    fn finds_forks(self) -> bool {
        self != Self::CollectedFork
    }
}

/// The pins that one namespace's pin listing returned.
pub(super) struct ListedPins {
    /// Whether the listing returned any key, recognized or not, which keeps a
    /// tombstone from retirement.
    any_key: bool,
}

pub(super) struct LiveSet {
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: ChargedSet<String>,
    pub(super) content_ids: ChargedSet<ContentId>,
    /// Each upload session that was completed when the pass listed the
    /// sessions, keyed by upload ID. An outcome record can't change, so the
    /// session sweep uses these facts instead of reading the records again.
    pub(super) completed_sessions: ChargedMap<UploadId, CompletedUploadSession>,
    /// The records of the namespace's own pins that the pass read while it
    /// loaded its roots, keyed by object key, which the pin sweep uses
    /// instead of reading each record again. The pass on a family root reads
    /// every record of its own pins to find its forks.
    pub(super) pin_records: ChargedMap<String, PinPayload>,
    /// The layout views of every namespace whose roots the pass read.
    pub(super) manifests: Vec<LoadedManifest>,
    manifest_charge: Charge,
    pub(super) memory: PassMemory,
    /// The layout rows of the current manifests of every namespace whose
    /// roots the pass read, which size the shared base filter.
    pub(super) content_layout_rows: u64,
    /// What the pass read of its fork family's roots.
    pub(super) family: FamilyWalkReport,
    /// Every member of the fork family that the walk has found, the family
    /// root included. The pass on a fork finds no members.
    pub(super) members: ChargedMap<NamespaceId, MemberState>,
    /// The members whose roots the pass hasn't read yet, in the order that
    /// the fork pin listings found them.
    pub(super) member_queue: ChargedQueue<NamespaceId>,
    has_pins: bool,
    grace_window_ms: u64,
    now_ms: u64,
    live_folded_wal_no: Option<WalNo>,
}

impl LiveSet {
    /// Loads roots, their segments, and the content they refer to, including
    /// superseded manifests with a young or undated successor regardless of
    /// the discovery hint. On a family root, it then loads the same roots of
    /// every other member of the fork family. Reads go through the
    /// namespace's segment cache, with up to `STORE_READS_IN_FLIGHT` root
    /// manifests loading at once, and the object keys, content IDs, completed
    /// sessions, manifests, and fork family members that the pass keeps are
    /// charged to its read working memory as they arrive. The pass doesn't
    /// read the record of a completed session that `session_cache` has.
    pub(super) async fn load<S: ObjectStore>(
        namespace: &NamespaceStore<S>,
        session_cache: Option<&CompletedUploadSessionCache>,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> Result<Self> {
        let namespace_id = &namespace.namespace_id;
        let observed = Observation::now(Arc::clone(&namespace.timer));
        let (manifest, hint) = namespace.load_current_manifest_with_hint().await?;
        let roots = if manifest.payload().is_family_root() {
            Roots::CollectedFamilyRoot
        } else {
            Roots::CollectedFork
        };
        // Sessions are listed before the WAL tail is read. A session this pass
        // misses is newer than the pass, or it ended before the tail read and
        // the tail or a rooted manifest names every commit of its content. A
        // deleted family root's pass still sweeps content, so its sessions are
        // still roots.
        let memory = PassMemory::new(
            namespace_id,
            read_working_memory(Some(namespace.segment_cache.as_ref())),
        );
        let mut content_ids = ChargedSet::new(&memory);
        let mut completed_sessions = ChargedMap::new(&memory);
        if roots == Roots::CollectedFamilyRoot || !manifest.payload().status.is_deleted() {
            namespace
                .load_upload_sessions(session_cache, |listed| match listed {
                    ListedUploadSession::Completed { upload_id, session } => {
                        content_ids.insert(session.content_id.clone())?;
                        completed_sessions.insert(upload_id, session)
                    }
                    ListedUploadSession::Incomplete { content_id } => {
                        content_ids.insert(content_id)
                    }
                })
                .await?;
        }
        let anchor = namespace
            .load_read_anchor_from_manifest(manifest, hint, observed)
            .await?;
        let head = anchor.manifest.payload();
        let mut live = Self {
            current_tombstone: head.status.is_deleted().then(|| head.clone()),
            discovery_start_manifest_no: anchor.hint.state.manifest_no,
            objects: ChargedSet::new(&memory),
            content_ids,
            completed_sessions,
            pin_records: ChargedMap::new(&memory),
            content_layout_rows: 0,
            family: FamilyWalkReport::own_roots(),
            members: ChargedMap::new(&memory),
            member_queue: ChargedQueue::new(&memory),
            manifests: Vec::new(),
            manifest_charge: Charge::new(&memory),
            memory,
            has_pins: false,
            grace_window_ms: grace_window_ms.max(NAMESPACE_RETIREMENT_GRACE_WINDOW_MS),
            now_ms: clock.request_at_ms(),
            live_folded_wal_no: live_folded_wal_no(&anchor.read_state),
        };
        // The family root is a member before its pin listing, so that a fork
        // pin can't add it to the walk's queue.
        if roots == Roots::CollectedFamilyRoot {
            live.members
                .insert(namespace_id.clone(), MemberState::Read)?;
        }
        let pins = live
            .load_namespace_roots(namespace, &anchor, roots, grace_window_ms, clock)
            .await?;
        live.has_pins = pins.any_key;
        if roots == Roots::CollectedFamilyRoot {
            live.load_family_roots(namespace, grace_window_ms, clock)
                .await?;
        }
        Ok(live)
    }

    /// Adds the roots of one namespace whose current manifest `anchor`
    /// loaded: that manifest, the WAL tail above it, the manifests that the
    /// namespace's pins keep, and its manifests whose successor is younger
    /// than the grace window. It also adds the current manifest's layout rows
    /// to the count that sizes the shared base filter, and returns what the
    /// pin listing found.
    pub(super) async fn load_namespace_roots<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        anchor: &NamespaceReadAnchor,
        roots: Roots,
        grace_window_ms: u64,
        clock: &OperationClock,
    ) -> Result<ListedPins> {
        let head = anchor.manifest.payload();
        self.content_layout_rows = self
            .content_layout_rows
            .saturating_add(content_layout_rows(head));
        // A tombstone roots its runs like any current manifest: an import from
        // a deleted owner is still authorized against its final access state.
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
        let pins = self
            .load_pinned_manifests(namespace, roots, grace_window_ms, clock, &mut rooted)
            .await?;
        self.load_superseded_manifests(
            namespace,
            head.manifest_no,
            roots,
            grace_window_ms,
            clock,
            &rooted,
        )
        .await?;
        Ok(pins)
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
    /// missing fails the pass unless pin cleanup would delete it. Another
    /// member's pass decides about that member's pins, so there a missing
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
                // pin's basis after this pass listed it. Every pin for a missing
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

    /// Protects each manifest below the current one, `current_manifest_no`,
    /// that `rooted` doesn't list and whose immediate successor is younger
    /// than the grace window or has no provider timestamp.
    async fn load_superseded_manifests<S: ObjectStore>(
        &mut self,
        namespace: &NamespaceStore<S>,
        current_manifest_no: ManifestNo,
        roots: Roots,
        grace_window_ms: u64,
        clock: &OperationClock,
        rooted: &BTreeSet<ManifestNo>,
    ) -> Result<()> {
        let prefix = metadata_manifest_prefix(&namespace.namespace_id);
        let mut listing = namespace.store.list_entries_from_stream(&prefix, None);
        let mut listed_manifests = BTreeMap::new();
        while let Some(entry) = listing
            .next()
            .await
            .transpose()
            .map_err(|error| CoreError::store(&prefix, &error))?
        {
            if let Some(manifest_no) = manifest_no_of(&entry.key) {
                listed_manifests.insert(manifest_no, entry.last_modified_at_ms);
            }
        }
        let superseded = listed_manifests.keys().copied().filter(|&manifest_no| {
            manifest_no < current_manifest_no
                && !rooted.contains(&manifest_no)
                && listed_manifests
                    .get(&ManifestNo(manifest_no.0 + 1))
                    .is_some_and(|&successor_time| {
                        GraceAge::of(successor_time, grace_window_ms, clock.request_at_ms())
                            != GraceAge::Aged
                    })
        });
        // Another collector can delete a listed manifest after the listing, and
        // a manifest that's gone needs no protection.
        let mut loads = pin!(stream::iter(superseded)
            .map(|manifest_no| namespace.load_root_manifest(manifest_no))
            .buffered(STORE_READS_IN_FLIGHT));
        while let Some(manifest) = loads.try_next().await? {
            if let Some(manifest) = manifest {
                self.protect_manifest(&manifest, roots)?;
            }
        }
        Ok(())
    }

    /// Keeps `manifest` as a layout view, and keeps the manifest and its
    /// segments themselves when `roots` protects the namespace's own objects.
    ///
    /// Every root manifest is a layout view, a tombstone included, because a
    /// deleted member's references protect content until the member is
    /// retired.
    fn protect_manifest(&mut self, manifest: &LoadedManifest, roots: Roots) -> Result<()> {
        if roots.protects_own_objects() {
            self.objects.insert(manifest.object_key.clone())?;
            for segment in manifest.payload().runs.iter().flat_map(|run| &run.segments) {
                self.objects.insert(metadata_segment_object_key(segment))?;
            }
        }
        self.manifest_charge
            .add(manifest_heap_bytes(&manifest.envelope))?;
        self.manifests.push(manifest.clone());
        Ok(())
    }

    /// Returns whether the pass keeps the object of `family` at `key`, because
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
                | CandidateFamily::Pins
                | CandidateFamily::Content
                | CandidateFamily::Temporary => false,
            }
    }

    pub(super) fn deadline(&self, tombstone: &NamespaceManifestPayload) -> u64 {
        tombstone
            .status
            .deleted_at_ms()
            .expect("a tombstone should carry its deletion stamp")
            .saturating_add(self.grace_window_ms)
    }

    pub(super) fn retirement_status(&self) -> RetirementStatus {
        let Some(tombstone) = &self.current_tombstone else {
            return RetirementStatus::Active;
        };
        let deadline_ms = self.deadline(tombstone);
        if self.now_ms < deadline_ms {
            RetirementStatus::Retained {
                reclaimable_at_ms: Some(deadline_ms),
            }
        } else if self.has_pins {
            RetirementStatus::Retained {
                reclaimable_at_ms: None,
            }
        } else {
            RetirementStatus::Eligible
        }
    }
}

/// Returns the number of layout rows that `manifest` lists.
fn content_layout_rows(manifest: &NamespaceManifestPayload) -> u64 {
    manifest
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
        .map(|segment| segment.row_count)
        .sum()
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
