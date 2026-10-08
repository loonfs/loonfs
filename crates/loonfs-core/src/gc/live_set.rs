//! Manifests, runs, WAL objects, and content protected by namespace retention,
//! pins, and upload sessions. Superseded manifests and their runs remain rooted
//! while their successor is younger than the pass's grace window or has no
//! provider timestamp.

use super::reap::{grace_age, GraceAge};
use super::uploads::protect_session_content;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::heap_bytes::{hash_set_table_bytes, HeapBytes};
use crate::limits::NAMESPACE_RETIREMENT_GRACE_MS;
use crate::manifest::cache::read_working_memory;
use crate::manifest::{
    load_namespace_manifest_envelope_if_present, metadata_basis_from_manifest, MetadataSegmentCache,
};
use crate::metadata::row_decode::revision_from_manifest_row;
use crate::namespace::control::{load_current_manifest_with_hint, CurrentManifest, LoadedManifest};
use crate::namespace::read_anchor::{load_read_anchor_from_manifest, project_anchor_tail};
use crate::pin::record::pin_key_ids;
use crate::read_working_memory::ReadWorkingMemory;
use crate::time::{Observation, StdMonotonicTimer};
use crate::wal::{live_folded_wal_no, object_is_required};
use futures::StreamExt;
use loonfs_objectstore::keys::{
    metadata_manifest_object, metadata_manifest_prefix, metadata_segment_object_key, pin_prefix,
};
use loonfs_objectstore::layout::{content_id_of, manifest_no_of};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{lookup_keys, MetadataRowFamily, NamespaceManifestPayload};
use loonfs_types::format::sst_blocks::string_prefix_upper_bound;
use loonfs_types::{ContentId, ContentRef, ManifestNo, NamespaceId, WalNo};
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

const REVISION_PAGE_ROWS: usize = 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RetirementState {
    Active,
    Retained { until_ms: Option<u64> },
    Eligible,
}

/// Protects current and pinned manifests, plus superseded manifests whose
/// successor is within the pass's grace window or has an unknown age. In an
/// active namespace it also holds the owned content IDs those manifests, the
/// WAL tail, and the upload sessions name.
pub(super) struct LiveSet {
    pub(super) namespace_deleted: bool,
    pub(super) current_tombstone: Option<NamespaceManifestPayload>,
    pub(super) discovery_start_manifest_no: ManifestNo,
    pub(super) objects: BTreeSet<String>,
    content_roots: ContentRoots,
    has_pins: bool,
    grace_window_ms: u64,
    now_ms: u64,
    live_folded_wal_no: Option<WalNo>,
}

/// The content ids the roots name. Their table is charged to the read
/// working memory as it grows, beside the scan's own blocks, so a pass over
/// a namespace with more live content than the budget holds fails instead
/// of growing past it.
struct ContentRoots {
    namespace_id: NamespaceId,
    ids: HashSet<ContentId>,
    id_heap_bytes: usize,
    memory: Arc<ReadWorkingMemory>,
    reserved_bytes: usize,
}

impl ContentRoots {
    fn new(namespace_id: &NamespaceId, memory: Arc<ReadWorkingMemory>) -> Self {
        Self {
            namespace_id: namespace_id.clone(),
            ids: HashSet::new(),
            id_heap_bytes: 0,
            memory,
            reserved_bytes: 0,
        }
    }

    fn insert(&mut self, content_id: &ContentId) -> Result<()> {
        if !self.ids.insert(content_id.clone()) {
            return Ok(());
        }
        self.id_heap_bytes += content_id.heap_bytes();
        let bytes = hash_set_table_bytes(&self.ids) + self.id_heap_bytes;
        if bytes > self.reserved_bytes {
            if !self.memory.try_reserve(bytes - self.reserved_bytes) {
                return Err(CoreError::ContentRootsExceedReadMemory {
                    namespace_id: self.namespace_id.clone(),
                    bytes,
                    limit: self.memory.limit(),
                });
            }
            self.reserved_bytes = bytes;
        }
        Ok(())
    }
}

impl Drop for ContentRoots {
    fn drop(&mut self) {
        self.memory.release(self.reserved_bytes);
    }
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
        let mut content_roots = ContentRoots::new(namespace_id, read_working_memory(segment_cache));
        if !manifest.state.envelope.payload().status.is_deleted() {
            protect_session_content(store, namespace_id, |content_id| {
                content_roots.insert(&content_id)
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
            content_roots,
            has_pins: false,
            grace_window_ms: grace_window_ms.max(NAMESPACE_RETIREMENT_GRACE_MS),
            now_ms: context.now_ms,
            live_folded_wal_no: live_folded_wal_no(&anchor.read_state),
        };
        // A tombstone roots its runs like any current manifest: an import from
        // a deleted owner is still authorized against its final access state.
        live.protect_manifest(store, segment_cache, namespace_id, &anchor.manifest)
            .await?;
        if !live.namespace_deleted {
            for revision in project_anchor_tail(store, segment_cache, &anchor)
                .await?
                .rows
                .revisions()
            {
                live.protect_content(namespace_id, &revision.content_ref)?;
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
                .load_manifest(
                    store,
                    segment_cache,
                    namespace_id,
                    pin_id.manifest_no(),
                    &mut manifests,
                )
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
        let mut listing = store.list_prefix_stream(&prefix);
        while let Some(key) = listing
            .next()
            .await
            .transpose()
            .map_err(|error| CoreError::store(&prefix, &error))?
        {
            let Some(manifest_no) = manifest_no_of(&key) else {
                continue;
            };
            if manifest_no >= head.manifest_no || manifests.contains(&manifest_no) {
                continue;
            }
            let successor = metadata_manifest_object(namespace_id, &ManifestNo(manifest_no.0 + 1));
            let age = grace_age(store, &successor, grace_window_ms, context.now_ms)
                .await
                .map_err(|error| CoreError::store(&successor, &error))?;
            if matches!(age, GraceAge::Young | GraceAge::Unknown) {
                live.load_manifest(
                    store,
                    segment_cache,
                    namespace_id,
                    manifest_no,
                    &mut manifests,
                )
                .await?;
            }
        }
        Ok(live)
    }

    async fn load_manifest<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        segment_cache: Option<&MetadataSegmentCache>,
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
        self.protect_manifest(store, segment_cache, namespace_id, &manifest)
            .await?;
        manifests.insert(manifest_no);
        Ok(true)
    }

    async fn protect_manifest<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        segment_cache: Option<&MetadataSegmentCache>,
        namespace_id: &NamespaceId,
        manifest: &LoadedManifest,
    ) -> Result<()> {
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
        if self.namespace_deleted {
            return Ok(());
        }
        let segments = metadata_basis_from_manifest(store, segment_cache, manifest).segments;
        let upper_bound = string_prefix_upper_bound(lookup_keys::REVISION_ROW_PREFIX);
        let mut lower_bound = lookup_keys::REVISION_ROW_PREFIX.to_owned();
        loop {
            let rows = segments
                .scan_range_page_with_keys(
                    MetadataRowFamily::Revisions,
                    &lower_bound,
                    upper_bound.as_deref(),
                    REVISION_PAGE_ROWS,
                )
                .await
                .map_err(MetadataProjectionLoadError::from)?;
            let Some((last_key, _)) = rows.last() else {
                return Ok(());
            };
            lower_bound = lookup_keys::after_row_key(last_key);
            let exhausted = rows.len() < REVISION_PAGE_ROWS;
            for (_, row) in rows {
                self.protect_content(namespace_id, &revision_from_manifest_row(row)?.content_ref)?;
            }
            if exhausted {
                return Ok(());
            }
        }
    }

    fn protect_content(
        &mut self,
        namespace_id: &NamespaceId,
        content_ref: &ContentRef,
    ) -> Result<()> {
        if content_ref.owner_namespace_id == *namespace_id {
            self.content_roots.insert(&content_ref.content_id)?;
        }
        Ok(())
    }

    pub(super) fn protects_wal(&self, key: &str) -> bool {
        self.live_folded_wal_no
            .is_some_and(|folded_wal_no| object_is_required(key, folded_wal_no))
    }

    pub(super) fn protects_content(&self, key: &str) -> bool {
        content_id_of(key).is_some_and(|content_id| self.content_roots.ids.contains(&content_id))
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
