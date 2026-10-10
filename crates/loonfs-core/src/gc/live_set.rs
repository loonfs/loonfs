//! Manifests, runs, WAL objects, and content protected by namespace retention,
//! pins, and upload sessions. Superseded manifests and their runs remain rooted
//! while their successor is younger than the pass's grace window or has no
//! provider timestamp.

use super::charged_set::ChargedSet;
use super::reap::GraceAge;
use super::uploads::protect_session_content;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::limits::NAMESPACE_RETIREMENT_GRACE_MS;
use crate::manifest::cache::read_working_memory;
use crate::manifest::{load_namespace_manifest_envelope_if_present, MetadataSegmentCache};
use crate::namespace::control::{load_current_manifest_with_hint, CurrentManifest, LoadedManifest};
use crate::namespace::read_anchor::{load_read_anchor_from_manifest, project_anchor_tail};
use crate::pin::record::pin_key_ids;
use crate::time::{Observation, StdMonotonicTimer};
use crate::wal::{live_folded_wal_no, object_is_required};
use futures::StreamExt;
use loonfs_objectstore::keys::{
    metadata_manifest_object, metadata_manifest_prefix, metadata_segment_object_key, pin_prefix,
};
use loonfs_objectstore::layout::manifest_no_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRowFamily, NamespaceManifestPayload};
use loonfs_types::{ContentId, ManifestNo, NamespaceId, WalNo};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RetirementState {
    Active,
    Retained { until_ms: Option<u64> },
    Eligible,
}

pub(super) struct LiveSet {
    pub(super) namespace_deleted: bool,
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: BTreeSet<String>,
    pub(super) content_ids: ChargedSet<ContentId>,
    pub(super) manifests: Vec<LoadedManifest>,
    pub(super) content_layout_rows: u64,
    has_pins: bool,
    grace_window_ms: u64,
    now_ms: u64,
    live_folded_wal_no: Option<WalNo>,
}

impl LiveSet {
    /// Loads roots, their segments, and in an active namespace the content they
    /// name, including superseded manifests with a young or undated successor
    /// regardless of the discovery hint. Reads go through `segment_cache`, and
    /// the content ids are charged to its read working memory.
    pub(super) async fn load<S: ObjectStore + ?Sized>(
        store: &S,
        segment_cache: Option<&MetadataSegmentCache>,
        namespace_id: &NamespaceId,
        grace_window_ms: u64,
        context: &MutationContext,
    ) -> Result<Self> {
        let observed = Observation::now(Arc::new(StdMonotonicTimer::default()));
        let (manifest, hint) = load_current_manifest_with_hint(store, namespace_id).await?;
        // Sessions are listed before the WAL tail is read. A session this pass
        // misses is newer than the pass, or it ended before the tail read and
        // the tail or a rooted manifest names every commit of its content.
        let mut content_ids = ChargedSet::new(namespace_id, read_working_memory(segment_cache));
        if !manifest.state.envelope.payload().status.is_deleted() {
            protect_session_content(store, namespace_id, |content_id| {
                content_ids.insert(content_id)
            })
            .await?;
        }
        let anchor =
            load_read_anchor_from_manifest(store, namespace_id, manifest, hint, observed).await?;
        let head = anchor.manifest.state.envelope.payload();
        let mut live = Self {
            namespace_deleted: head.status.is_deleted(),
            current_tombstone: head.status.is_deleted().then(|| head.clone()),
            discovery_start_manifest_no: anchor.hint.state.manifest_no,
            objects: BTreeSet::new(),
            content_ids,
            content_layout_rows: head
                .runs
                .iter()
                .flat_map(|run| &run.segments)
                .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
                .map(|segment| segment.row_count)
                .sum(),
            manifests: Vec::new(),
            has_pins: false,
            grace_window_ms: grace_window_ms.max(NAMESPACE_RETIREMENT_GRACE_MS),
            now_ms: context.now_ms,
            live_folded_wal_no: live_folded_wal_no(&anchor.read_state),
        };
        // A tombstone roots its runs like any current manifest: an import from
        // a deleted owner is still authorized against its final access state.
        live.protect_manifest(&anchor.manifest);
        if !live.namespace_deleted {
            let tail = project_anchor_tail(store, segment_cache, &anchor).await?;
            for revision in tail.rows.revisions() {
                live.content_ids
                    .insert(revision.content_ref.content_id.clone())?;
            }
            for row in tail.rows.content_layouts() {
                for extent in &row.layout.extents {
                    if extent.owner_namespace_id == *namespace_id {
                        live.content_ids.insert(extent.content_id.clone())?;
                    }
                }
            }
        }
        let mut manifests = BTreeSet::from([head.manifest_no]);
        let prefix = pin_prefix(namespace_id);
        let mut listing = store.list_prefix_stream(&prefix);
        while let Some(key) = listing
            .next()
            .await
            .transpose()
            .map_err(|error| CoreError::store(&prefix, &error))?
        {
            live.has_pins = true;
            if !super::families::CandidateFamily::Pins.recognizes(&key) {
                continue;
            }
            let (_, pin_id) = pin_key_ids(&key).map_err(CoreError::ControlObjectLoad)?;
            if live
                .load_manifest(store, namespace_id, pin_id.manifest_no(), &mut manifests)
                .await?
            {
                continue;
            }
            // A failed pin installation can outlive both its basis and its
            // cleanup attempt. Another collector can also remove a released
            // pin's basis after this pass listed it. Only tolerate absence
            // when the same owner/grace rules used by the sweep allow it.
            match super::reap::sweep_pin(store, &key, grace_window_ms, &live, context).await? {
                super::reap::PinSweep::Retain { .. } => {
                    return Err(missing_root_manifest(
                        namespace_id,
                        pin_id.manifest_no(),
                        &key,
                    ));
                }
                super::reap::PinSweep::Gone
                | super::reap::PinSweep::DeleteUser
                | super::reap::PinSweep::DeleteSnapshot
                | super::reap::PinSweep::DeleteFork => {}
            }
        }
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
                listed_manifests.insert(manifest_no, entry.last_modified_ms);
            }
        }
        for manifest_no in listed_manifests.keys().copied() {
            if manifest_no >= head.manifest_no || manifests.contains(&manifest_no) {
                continue;
            }
            let Some(&successor_time) = listed_manifests.get(&ManifestNo(manifest_no.0 + 1)) else {
                continue;
            };
            if GraceAge::of(successor_time, grace_window_ms, context.now_ms) != GraceAge::Aged {
                live.load_manifest(store, namespace_id, manifest_no, &mut manifests)
                    .await?;
            }
        }
        Ok(live)
    }

    async fn load_manifest<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        manifest_no: ManifestNo,
        manifests: &mut BTreeSet<ManifestNo>,
    ) -> Result<bool> {
        if manifests.contains(&manifest_no) {
            return Ok(true);
        }
        let envelope =
            load_namespace_manifest_envelope_if_present(store, namespace_id, &manifest_no)
                .await
                .map_err(MetadataProjectionLoadError::from)?;
        let Some((envelope, manifest_bytes)) = envelope else {
            // Do not cache absence: another pin for this number may still
            // require retention even when an earlier pin was deletable.
            return Ok(false);
        };
        let manifest = LoadedManifest {
            object_key: metadata_manifest_object(namespace_id, &manifest_no),
            manifest_bytes,
            state: CurrentManifest {
                envelope: Arc::new(envelope),
            },
        };
        self.protect_manifest(&manifest);
        manifests.insert(manifest_no);
        Ok(true)
    }

    fn protect_manifest(&mut self, manifest: &LoadedManifest) {
        self.objects.insert(manifest.object_key.clone());
        self.objects.extend(
            manifest
                .state
                .envelope
                .payload()
                .runs
                .iter()
                .flat_map(|run| &run.segments)
                .map(metadata_segment_object_key),
        );
        if !self.namespace_deleted {
            self.manifests.push(manifest.clone());
        }
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

    pub(super) fn retirement_state(&self) -> RetirementState {
        let Some(tombstone) = &self.current_tombstone else {
            return RetirementState::Active;
        };
        let deadline_ms = self.deadline(tombstone);
        if self.now_ms < deadline_ms {
            RetirementState::Retained {
                until_ms: Some(deadline_ms),
            }
        } else if self.has_pins {
            RetirementState::Retained { until_ms: None }
        } else {
            RetirementState::Eligible
        }
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
