//! Manifests, runs, WAL objects, and content protected by namespace retention,
//! pins, and upload sessions. Superseded manifests and their runs remain rooted
//! while their successor is younger than the pass's grace window or has no
//! provider timestamp.

use super::charged_set::{Charge, ChargedMap, ChargedSet, PassMemory};
use super::reap::GraceAge;
use super::session_cache::{CompletedUploadSession, CompletedUploadSessionCache};
use super::uploads::{load_upload_sessions, ListedUploadSession};
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::heap_bytes::manifest_heap_bytes;
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::NAMESPACE_RETIREMENT_GRACE_WINDOW_MS;
use crate::manifest::cache::read_working_memory;
use crate::manifest::{load_namespace_manifest_envelope_if_present, MetadataSegmentCache};
use crate::namespace::control::{load_current_manifest_with_hint, CurrentManifest, LoadedManifest};
use crate::namespace::read_anchor::{load_read_anchor_from_manifest, project_anchor_tail};
use crate::pin::record::pin_key_ids;
use crate::time::{MonotonicTimer, Observation};
use crate::wal::{live_folded_wal_no, object_is_required};
use futures::{future, stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{
    metadata_manifest_object, metadata_manifest_prefix, metadata_segment_object_key, pin_prefix,
};
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_objectstore::ObjectStore;
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

pub(super) struct LiveSet {
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: ChargedSet<String>,
    pub(super) content_ids: ChargedSet<ContentId>,
    /// Each upload session that was completed when the pass listed the
    /// sessions, keyed by upload ID. An outcome record can't change, so the
    /// session sweep uses these facts instead of reading the records again.
    pub(super) completed_sessions: ChargedMap<UploadId, CompletedUploadSession>,
    pub(super) manifests: Vec<LoadedManifest>,
    manifest_charge: Charge,
    pub(super) memory: PassMemory,
    pub(super) content_layout_rows: u64,
    has_pins: bool,
    grace_window_ms: u64,
    now_ms: u64,
    live_folded_wal_no: Option<WalNo>,
}

impl LiveSet {
    /// Loads roots, their segments, and in an active namespace the content they
    /// refer to, including superseded manifests with a young or undated successor
    /// regardless of the discovery hint. Reads go through `segment_cache`, with
    /// up to `STORE_READS_IN_FLIGHT` root manifests loading at once, and the
    /// object keys, content IDs, completed sessions, and manifests that the
    /// pass keeps are charged to its read working memory as they arrive. The
    /// pass doesn't read the record of a completed session that
    /// `session_cache` has.
    pub(super) async fn load<S: ObjectStore + ?Sized>(
        store: &S,
        segment_cache: Option<&MetadataSegmentCache>,
        session_cache: Option<&CompletedUploadSessionCache>,
        namespace_id: &NamespaceId,
        grace_window_ms: u64,
        context: &MutationContext,
        timer: Arc<dyn MonotonicTimer>,
    ) -> Result<Self> {
        let observed = Observation::now(timer);
        let (manifest, hint) = load_current_manifest_with_hint(store, namespace_id).await?;
        // Sessions are listed before the WAL tail is read. A session this pass
        // misses is newer than the pass, or it ended before the tail read and
        // the tail or a rooted manifest names every commit of its content.
        let memory = PassMemory::new(namespace_id, read_working_memory(segment_cache));
        let mut content_ids = ChargedSet::new(&memory);
        let mut completed_sessions = ChargedMap::new(&memory);
        if !manifest.state.envelope.payload().status.is_deleted() {
            load_upload_sessions(store, namespace_id, session_cache, |listed| match listed {
                ListedUploadSession::Completed { upload_id, session } => {
                    content_ids.insert(session.content_id.clone())?;
                    completed_sessions.insert(upload_id, session)
                }
                ListedUploadSession::Incomplete { content_id } => content_ids.insert(content_id),
            })
            .await?;
        }
        let anchor =
            load_read_anchor_from_manifest(store, namespace_id, manifest, hint, observed).await?;
        let head = anchor.manifest.state.envelope.payload();
        let mut live = Self {
            current_tombstone: head.status.is_deleted().then(|| head.clone()),
            discovery_start_manifest_no: anchor.hint.state.manifest_no,
            objects: ChargedSet::new(&memory),
            content_ids,
            completed_sessions,
            content_layout_rows: head
                .runs
                .iter()
                .flat_map(|run| &run.segments)
                .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
                .map(|segment| segment.row_count)
                .sum(),
            manifests: Vec::new(),
            manifest_charge: Charge::new(&memory),
            memory,
            has_pins: false,
            grace_window_ms: grace_window_ms.max(NAMESPACE_RETIREMENT_GRACE_WINDOW_MS),
            now_ms: context.now_ms,
            live_folded_wal_no: live_folded_wal_no(&anchor.read_state),
        };
        // A tombstone roots its runs like any current manifest: an import from
        // a deleted owner is still authorized against its final access state.
        live.protect_manifest(&anchor.manifest)?;
        if live.current_tombstone.is_none() {
            let tail = project_anchor_tail(store, segment_cache, &anchor).await?;
            for revision in tail.revisions() {
                live.content_ids
                    .insert(revision.content_ref.content_id.clone())?;
            }
            for row in tail.content_layouts() {
                for extent in &row.layout.extents {
                    if extent.owner_namespace_id == *namespace_id {
                        live.content_ids.insert(extent.content_id.clone())?;
                    }
                }
            }
        }
        let mut rooted = BTreeSet::from([head.manifest_no]);
        live.load_pinned_manifests(store, namespace_id, grace_window_ms, context, &mut rooted)
            .await?;
        let prefix = metadata_manifest_prefix(namespace_id);
        let mut listing = store.list_entries_from_stream(&prefix, None);
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
            manifest_no < head.manifest_no
                && !rooted.contains(&manifest_no)
                && listed_manifests
                    .get(&ManifestNo(manifest_no.0 + 1))
                    .is_some_and(|&successor_time| {
                        GraceAge::of(successor_time, grace_window_ms, context.now_ms)
                            != GraceAge::Aged
                    })
        });
        // Another collector can delete a listed manifest after the listing, and
        // a manifest that's gone needs no protection.
        let mut loads = pin!(stream::iter(superseded)
            .map(|manifest_no| load_root_manifest(store, namespace_id, manifest_no))
            .buffered(STORE_READS_IN_FLIGHT));
        while let Some(manifest) = loads.try_next().await? {
            if let Some(manifest) = manifest {
                live.protect_manifest(&manifest)?;
            }
        }
        Ok(live)
    }

    /// Protects the manifest of each listed pin, loading up to
    /// `STORE_READS_IN_FLIGHT` manifests at once, and adds each loaded number
    /// to `rooted`.
    ///
    /// The first listed pin for a manifest number loads that manifest, and
    /// later pins for the number use the result. A pin whose manifest is
    /// missing fails the pass unless pin cleanup would delete it.
    async fn load_pinned_manifests<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        grace_window_ms: u64,
        context: &MutationContext,
        rooted: &mut BTreeSet<ManifestNo>,
    ) -> Result<()> {
        let prefix = pin_prefix(namespace_id);
        let mut has_pins = false;
        let mut requested = rooted.clone();
        {
            // `try_buffered` keeps listing order, so a pin that reuses an
            // earlier pin's load arrives after that load's result.
            let mut pins = pin!(store
                .list_prefix_stream(&prefix)
                .map_err(|error| CoreError::store(&prefix, &error))
                .try_filter_map(|key| {
                    has_pins = true;
                    future::ready(
                        pinned_manifest_no(&key)
                            .map(|manifest_no| manifest_no.map(|manifest_no| (key, manifest_no))),
                    )
                })
                .map_ok(|(key, manifest_no)| {
                    let first = requested.insert(manifest_no);
                    async move {
                        let loaded = if first {
                            Some(load_root_manifest(store, namespace_id, manifest_no).await?)
                        } else {
                            None
                        };
                        Ok((key, manifest_no, loaded))
                    }
                })
                .try_buffered(STORE_READS_IN_FLIGHT));
            while let Some((key, manifest_no, loaded)) = pins.try_next().await? {
                if let Some(Some(manifest)) = loaded {
                    self.protect_manifest(&manifest)?;
                    rooted.insert(manifest_no);
                }
                if rooted.contains(&manifest_no) {
                    continue;
                }
                // A failed pin installation can outlive both its basis and its
                // cleanup attempt. Another collector can also remove a released
                // pin's basis after this pass listed it. Every pin for a missing
                // manifest is checked on its own, because another pin for the
                // number can still require retention when an earlier pin was
                // deletable.
                match super::reap::sweep_pin(store, &key, grace_window_ms, self, context).await? {
                    super::reap::PinSweep::Retain { .. } => {
                        return Err(missing_root_manifest(namespace_id, manifest_no, &key));
                    }
                    super::reap::PinSweep::Gone
                    | super::reap::PinSweep::DeleteUser
                    | super::reap::PinSweep::DeleteFork => {}
                }
            }
        }
        self.has_pins = has_pins;
        Ok(())
    }

    fn protect_manifest(&mut self, manifest: &LoadedManifest) -> Result<()> {
        self.objects.insert(manifest.object_key.clone())?;
        for segment in manifest
            .state
            .envelope
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
        {
            self.objects.insert(metadata_segment_object_key(segment))?;
        }
        if self.current_tombstone.is_none() {
            self.manifest_charge
                .add(manifest_heap_bytes(&manifest.state.envelope))?;
            self.manifests.push(manifest.clone());
        }
        Ok(())
    }

    pub(super) fn protects_wal(&self, key: &str) -> bool {
        self.live_folded_wal_no
            .is_some_and(|folded_wal_no| object_is_required(key, folded_wal_no))
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

/// Returns the manifest number in a pin key, or `None` for a key that isn't
/// a pin record.
fn pinned_manifest_no(key: &str) -> Result<Option<ManifestNo>> {
    if !super::families::CandidateFamily::Pins.recognizes(key) {
        return Ok(None);
    }
    let (_, pin_id) = pin_key_ids(key).map_err(CoreError::ControlObjectLoad)?;
    Ok(Some(pin_id.manifest_no()))
}

/// Loads one root manifest, or returns `None` when the manifest is missing.
async fn load_root_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
) -> Result<Option<LoadedManifest>> {
    let envelope = load_namespace_manifest_envelope_if_present(store, namespace_id, &manifest_no)
        .await
        .map_err(MetadataProjectionLoadError::from)?;
    Ok(envelope.map(|(envelope, manifest_bytes)| LoadedManifest {
        object_key: metadata_manifest_object(namespace_id, &manifest_no),
        manifest_bytes,
        state: CurrentManifest {
            envelope: Arc::new(envelope),
        },
    }))
}

fn missing_root_manifest(
    namespace_id: &NamespaceId,
    manifest_no: ManifestNo,
    root_key: &str,
) -> CoreError {
    let key = metadata_manifest_object(namespace_id, &manifest_no);
    CoreError::NamespaceCorrupt(format!("root `{root_key}` pins missing manifest `{key}`"))
}
