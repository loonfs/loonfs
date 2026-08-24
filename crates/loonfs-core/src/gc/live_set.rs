//! Collects objects that remain reachable from a namespace.
//!
//! Garbage collection refreshes this set in bounded chunks while sweeping.

use super::budget::PassBudget;
use super::fork_checkpoints::fork_target_proven_gone;
use super::reap::{lease_expired, manifest_object_id_of};
use crate::checkpoint::record::load_checkpoint_record_at_key;
use crate::checkpoint::{load_namespace_manifest_envelope_if_present, ManifestLoadFailureClass};
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::basis::{
    load_head_and_metadata_basis, resolve_retention_floor_seq, LoadedNamespaceBasis,
};
use crate::wal::{load_wal_chain_within, WalChainLoad, WalChainLoadRequest};
use futures::StreamExt;
use loonfs_api::wire::control::{
    CheckpointOwner, CheckpointRecordState, CheckpointStatus, HeadState, NamespaceStatus,
};
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{wal_segment_id_start_seq, ChangeSeq, ContentId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_manifest_prefix,
    metadata_segment_object_key, wal_segment_id_from_key,
};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Objects reachable from the most recently collected roots.
pub(super) struct LiveSet {
    pub(super) manifests: BTreeSet<ManifestObjectId>,
    pub(super) segments: BTreeSet<String>,
    pub(super) wal_segments: BTreeSet<String>,
    /// Content referenced by retained WAL commits but not yet materialized in
    /// a manifest. These references are collected while validating the WAL
    /// chain, avoiding a second read of each segment.
    pub(super) wal_content_ids: BTreeSet<ContentId>,
    pub(super) checkpoint_keys: BTreeSet<String>,
    /// Active checkpoint records whose basis manifest is missing. These can
    /// result from a crash between writing and verifying the record.
    pub(super) missing_basis_records: BTreeSet<String>,
    /// Whether a root manifest was missing or could not be read. The pass must
    /// not delete manifests or segments when this is true. Corruption fails the
    /// pass instead.
    pub(super) degraded: bool,
    /// Whether the namespace has been deleted.
    pub(super) namespace_deleted: bool,
    /// References held when the grace window began.
    pub(super) anchor: ReferenceAnchor,
}

impl LiveSet {
    fn collecting(namespace_deleted: bool) -> Self {
        Self {
            manifests: BTreeSet::new(),
            segments: BTreeSet::new(),
            wal_segments: BTreeSet::new(),
            wal_content_ids: BTreeSet::new(),
            checkpoint_keys: BTreeSet::new(),
            missing_basis_records: BTreeSet::new(),
            degraded: false,
            namespace_deleted,
            anchor: ReferenceAnchor::NotNeeded,
        }
    }

    /// Returns whether the current state or grace-window anchor needs a WAL
    /// segment.
    pub(super) fn protects_wal_segment(&self, key: &str) -> bool {
        self.wal_segments.contains(key) || self.anchor.replays_wal_segment(key)
    }
}

/// References recorded at the start of the grace window.
///
/// Object age alone is not enough to protect active readers. An old segment may
/// have stopped being referenced only moments ago. The collector therefore
/// uses the newest manifest that is at least one grace window old as an
/// anchor. An object can be deleted only when it is absent from the current
/// roots and the anchor, and its own write time is also old enough.
///
/// This protects readers pinned during the grace window. Newer objects are
/// protected by their write time. Older objects still needed during the
/// window appear in the anchor because manifest references form one
/// continuous interval: manifests build on their predecessor and object ids
/// are never reused. The anchor also protects its own manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ReferenceAnchor {
    /// The selected manifest and its referenced objects.
    Manifest(Box<AnchoredManifest>),
    /// No historical manifest is required. This applies to namespaces that
    /// never published a manifest and to deleted namespaces with no readers.
    NotNeeded,
    /// No manifest is old enough to prove when references ended. The pass
    /// keeps aged candidates until an anchor becomes available.
    Missing,
}

/// Information from one manifest needed during sweeping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AnchoredManifest {
    manifest_object_id: ManifestObjectId,
    /// Highest sequence materialized by the anchor. Readers pinned to it may
    /// still replay later WAL segments.
    head_seq: ChangeSeq,
    /// Object keys of the metadata segments the anchor named.
    segments: BTreeSet<String>,
}

impl ReferenceAnchor {
    /// Whether the pass may reap an aged, unreferenced candidate at all.
    pub(super) fn proves_unreferencing(&self) -> bool {
        !matches!(self, Self::Missing)
    }

    /// Whether a read pinned on the anchor still replays this segment.
    fn replays_wal_segment(&self, key: &str) -> bool {
        let Self::Manifest(anchor) = self else {
            return false;
        };
        wal_segment_id_from_key(key)
            .and_then(wal_segment_id_start_seq)
            .is_some_and(|start_seq| start_seq > anchor.head_seq)
    }
}

/// Result of collecting a complete live set within the pass budget.
///
/// A partial set is discarded because it cannot safely authorize deletion.
pub(super) enum LiveSetCollection {
    Complete(LiveSet),
    BudgetExhausted,
}

/// Reason a live-set collector stopped.
#[derive(Debug)]
pub(super) enum CollectStop {
    Budget,
    Core(CoreError),
}

impl From<CoreError> for CollectStop {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}

type CollectResult<T> = std::result::Result<T, CollectStop>;

fn charge(budget: &mut PassBudget) -> CollectResult<()> {
    if budget.try_charge() {
        Ok(())
    } else {
        Err(CollectStop::Budget)
    }
}

impl LiveSetCollection {
    /// Returns the roots only when collection completed.
    #[cfg(test)]
    pub(super) fn complete(self) -> Option<LiveSet> {
        match self {
            Self::Complete(live) => Some(live),
            Self::BudgetExhausted => None,
        }
    }
}

/// Whether sweeping can continue within the current budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SweepStep {
    Continue,
    BudgetExhausted,
}

/// Refreshes the live set after each `reverify_chunk` deletion decisions.
/// Once a refresh reports degraded retention, the flag remains set for the
/// rest of the pass.
pub(super) struct SweepVerifier {
    pub(super) live: Arc<LiveSet>,
    pub(super) degraded: bool,
    pub(super) reverify_chunk: usize,
    pub(super) decided_since_collect: usize,
}

impl SweepVerifier {
    pub(super) fn seeded(live: Arc<LiveSet>, reverify_chunk: usize) -> Self {
        Self {
            degraded: live.degraded,
            live,
            reverify_chunk,
            decided_since_collect: 0,
        }
    }

    pub(super) async fn refresh_if_due<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        grace_window_ms: u64,
        budget: &mut PassBudget,
        context: &MutationContext,
    ) -> Result<SweepStep> {
        if self.decided_since_collect >= self.reverify_chunk {
            // Reuse the original anchor. The pass uses a fixed clock, and any
            // manifest published during the pass is too new to replace it.
            let anchor = self.live.anchor.clone();
            match recollect_live_set(
                store,
                namespace_id,
                grace_window_ms,
                Some(anchor),
                budget,
                context,
            )
            .await?
            {
                LiveSetCollection::Complete(live) => {
                    self.live = Arc::new(live);
                    self.degraded |= self.live.degraded;
                    self.decided_since_collect = 0;
                }
                // Stop if the pass cannot afford the required refresh. A stale
                // live set cannot safely authorize more deletions.
                LiveSetCollection::BudgetExhausted => return Ok(SweepStep::BudgetExhausted),
            }
        }
        self.decided_since_collect += 1;
        Ok(SweepStep::Continue)
    }
}

/// Reloads the namespace head and basis before collecting a new live set.
pub(super) async fn recollect_live_set<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    grace_window_ms: u64,
    reused_anchor: Option<ReferenceAnchor>,
    budget: &mut PassBudget,
    context: &MutationContext,
) -> Result<LiveSetCollection> {
    if !budget.try_charge() {
        return Ok(LiveSetCollection::BudgetExhausted);
    }
    let loaded = load_head_and_metadata_basis(store, namespace_id)
        .await
        .map_err(CoreError::ControlObjectLoad)?;
    collect_live_set(
        store,
        namespace_id,
        &loaded,
        grace_window_ms,
        reused_anchor,
        budget,
        context,
    )
    .await
}

/// Collects from the head and basis the pass already read and charged for.
pub(super) async fn collect_live_set<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    loaded: &LoadedNamespaceBasis,
    grace_window_ms: u64,
    reused_anchor: Option<ReferenceAnchor>,
    budget: &mut PassBudget,
    context: &MutationContext,
) -> Result<LiveSetCollection> {
    let head = &loaded.head.state;
    // A namespace with no root of its own roots no manifest here: the
    // genesis basis has none, and a fork target's basis is a source-prefix
    // object that the source's own pass protects through the fork-owned
    // checkpoint record. Neither is ever a candidate of this pass.
    let root_manifest_object_id = loaded.basis.is_owned_by(namespace_id).then(|| {
        loaded
            .basis
            .manifest()
            .expect("owned basis")
            .manifest_object_id
            .clone()
    });
    let namespace_deleted = head.status == NamespaceStatus::Deleted {};
    let collected: CollectResult<LiveSet> = async {
        // Without a stored floor, retain WAL from the namespace's first
        // sequence.
        charge(budget)?;
        let floor_seq = resolve_retention_floor_seq(store, head)
            .await
            .map_err(CoreError::ControlObjectLoad)?;
        let mut live = LiveSet::collecting(namespace_deleted);
        let mut manifest_object_ids = BTreeSet::new();
        if !namespace_deleted {
            manifest_object_ids.extend(root_manifest_object_id.clone());
        }
        let active_record_bases = collect_checkpoint_records(
            store,
            namespace_id,
            namespace_deleted,
            &mut manifest_object_ids,
            &mut live,
            budget,
            context,
        )
        .await?;
        collect_manifest_segments(
            store,
            namespace_id,
            root_manifest_object_id.as_ref(),
            manifest_object_ids,
            &active_record_bases,
            &mut live,
            budget,
        )
        .await?;
        collect_reference_anchor(
            store,
            namespace_id,
            grace_window_ms,
            reused_anchor,
            &mut live,
            budget,
            context,
        )
        .await?;
        collect_retained_wal(store, namespace_id, head, floor_seq, &mut live, budget).await?;
        Ok(live)
    }
    .await;
    match collected {
        Ok(live) => Ok(LiveSetCollection::Complete(live)),
        Err(CollectStop::Budget) => Ok(LiveSetCollection::BudgetExhausted),
        Err(CollectStop::Core(error)) => Err(error),
    }
}

type ActiveRecordBases = BTreeMap<ManifestObjectId, Vec<String>>;

/// Collects checkpoint roots and record keys that must be retained.
///
/// In a live namespace, every readable record protects its basis even if the
/// record itself is eligible for deletion.
async fn collect_checkpoint_records<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    namespace_deleted: bool,
    manifest_object_ids: &mut BTreeSet<ManifestObjectId>,
    live: &mut LiveSet,
    budget: &mut PassBudget,
    context: &MutationContext,
) -> CollectResult<ActiveRecordBases> {
    let prefix = checkpoint_prefix(namespace_id);
    let mut keys = store.list_prefix_stream(&prefix);
    let mut active_record_bases: ActiveRecordBases = BTreeMap::new();
    while let Some(item) = keys.next().await {
        let key = item.map_err(|error| CoreError::store(&prefix, &error))?;
        charge(budget)?;
        let loaded = load_checkpoint_record_at_key(store, &key).await;
        let record = match loaded {
            Ok(loaded) => loaded.state,
            Err(ControlObjectLoadError::MissingObject { .. }) => continue,
            Err(error) => return Err(CoreError::ControlObjectLoad(error).into()),
        };
        let candidate = checkpoint_is_candidate(store, &record, budget, context).await?;
        // A deleted namespace has no readers. Only an active fork checkpoint
        // for an existing target still protects the source basis.
        if namespace_deleted && (candidate || matches!(record.owner, CheckpointOwner::User { .. }))
        {
            continue;
        }
        manifest_object_ids.insert(record.manifest.manifest_object_id.clone());
        // In a live namespace, a candidate still protects its basis but does
        // not protect its own record key.
        if !candidate {
            active_record_bases
                .entry(record.manifest.manifest_object_id)
                .or_default()
                .push(key.clone());
            live.checkpoint_keys.insert(key);
        }
    }
    Ok(active_record_bases)
}

async fn checkpoint_is_candidate<S: ObjectStore + ?Sized>(
    store: &S,
    record: &CheckpointRecordState,
    budget: &mut PassBudget,
    context: &MutationContext,
) -> CollectResult<bool> {
    // User checkpoints expire by time. A fork checkpoint remains a root while
    // its target namespace exists even if a racing pass already moved the
    // record to `released`: the target head can land between that pass's head
    // read and checkpoint CAS, and the forker can then crash before its guard.
    match &record.owner {
        CheckpointOwner::User { .. } => {
            Ok(record.status != (CheckpointStatus::Active {})
                || lease_expired(record, context.now_ms))
        }
        CheckpointOwner::Fork {
            target_namespace_id,
            expires_at_ms,
        } => {
            charge(budget)?;
            Ok(
                fork_target_proven_gone(store, target_namespace_id, *expires_at_ms, context)
                    .await?,
            )
        }
    }
}

async fn collect_manifest_segments<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    root_manifest_object_id: Option<&ManifestObjectId>,
    manifest_object_ids: BTreeSet<ManifestObjectId>,
    active_record_bases: &ActiveRecordBases,
    live: &mut LiveSet,
    budget: &mut PassBudget,
) -> CollectResult<()> {
    // Only a validated manifest can protect its segment objects.
    for manifest_object_id in &manifest_object_ids {
        charge(budget)?;
        let manifest_key = metadata_manifest_object(namespace_id, manifest_object_id);
        match load_namespace_manifest_envelope_if_present(
            store,
            namespace_id,
            manifest_object_id,
            &manifest_key,
        )
        .await
        {
            Ok(Some(manifest)) => live.segments.extend(
                manifest
                    .payload
                    .segments
                    .iter()
                    .map(metadata_segment_object_key),
            ),
            Ok(None) if Some(manifest_object_id) == root_manifest_object_id => {
                live.degraded = true;
            }
            Ok(None) => {
                if let Some(record_keys) = active_record_bases.get(manifest_object_id) {
                    live.missing_basis_records
                        .extend(record_keys.iter().cloned());
                }
            }
            Err(error) => match error.failure_class() {
                ManifestLoadFailureClass::Store => {
                    live.degraded = true;
                    tracing::warn!(
                        namespace_id = %namespace_id,
                        object_key = manifest_key,
                        error = %error,
                        "a root manifest did not read; this pass reclaims no manifests or segments"
                    );
                }
                ManifestLoadFailureClass::Corrupt => {
                    return Err(CoreError::NamespaceCorrupt(format!(
                        "a manifest this namespace still references does not load: {error}"
                    ))
                    .into());
                }
            },
        }
    }
    live.manifests = manifest_object_ids;
    Ok(())
}

async fn collect_reference_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    grace_window_ms: u64,
    reused_anchor: Option<ReferenceAnchor>,
    live: &mut LiveSet,
    budget: &mut PassBudget,
    context: &MutationContext,
) -> CollectResult<()> {
    // Deleted namespaces need no historical root. Refreshes reuse the anchor
    // selected at the start of the pass.
    live.anchor = match (live.namespace_deleted, reused_anchor) {
        (true, _) => ReferenceAnchor::NotNeeded,
        (false, Some(anchor)) => anchor,
        (false, None) => {
            select_reference_anchor(store, namespace_id, grace_window_ms, budget, context).await?
        }
    };
    // Protect the anchor's segments and manifest so later passes can use the
    // same evidence.
    if let ReferenceAnchor::Manifest(anchor) = &live.anchor {
        live.manifests.insert(anchor.manifest_object_id.clone());
        live.segments.extend(anchor.segments.iter().cloned());
    }
    Ok(())
}

async fn collect_retained_wal<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    head: &HeadState,
    floor_seq: ChangeSeq,
    live: &mut LiveSet,
    budget: &mut PassBudget,
) -> CollectResult<()> {
    // Live namespaces retain the complete WAL chain from floor to head.
    // Deleted namespaces do not need a replay chain.
    if live.namespace_deleted || head.seq <= floor_seq {
        return Ok(());
    }
    let remaining = budget.remaining();
    if remaining == 0 {
        return Err(CollectStop::Budget);
    }
    let load = load_wal_chain_within(
        store,
        WalChainLoadRequest {
            namespace_id,
            chain_base_seq: floor_seq,
            head_seq: head.seq,
            visible_tip: head.visible_wal_tip.clone(),
            stop_after_seq: None,
            recent_segments: &head.recent_segments,
        },
        usize::try_from(remaining).unwrap_or(usize::MAX),
    )
    .await
    .map_err(|error| {
        CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
    })?;
    let chain = match load {
        WalChainLoad::Complete {
            chain,
            requests_issued,
        } => {
            budget.charge_block(u64::try_from(requests_issued).unwrap_or(u64::MAX));
            chain
        }
        WalChainLoad::LimitReached { requests_issued } => {
            budget.charge_block(u64::try_from(requests_issued).unwrap_or(u64::MAX));
            // Discard the partial chain and the incomplete live set.
            return Err(CollectStop::Budget);
        }
    };
    for segment in chain.segments() {
        live.wal_segments.insert(segment.object_key().to_owned());
        for record in segment.records() {
            for delta in &record.deltas {
                if let WalDelta::AppendFileRevision { content_ref, .. } = &delta.delta {
                    live.wal_content_ids.insert(content_ref.content_id.clone());
                }
            }
        }
    }
    Ok(())
}

/// Finds the reference manifest R and reads what it named.
///
/// Manifest keys sort by logical manifest position, which is publication
/// order, so the scan walks the listing from the oldest manifest and stops at
/// the first one the window still covers. The last aged manifest before that
/// is R. Stopping at the first young one rather than taking the newest aged
/// timestamp is the conservative reading of a provider clock: one manifest
/// reporting an early stamp cannot pull the anchor forward past a manifest
/// that reads as young.
///
/// The age test is the sweep's own (`gc/reap.rs`), on the same provider
/// timestamp, so a manifest with no timestamp reads as young here exactly as
/// a candidate without one does there.
///
pub(super) async fn select_reference_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    grace_window_ms: u64,
    budget: &mut PassBudget,
    context: &MutationContext,
) -> CollectResult<ReferenceAnchor> {
    let prefix = metadata_manifest_prefix(namespace_id);
    let mut keys = store.list_prefix_stream(&prefix);
    let mut published_any = false;
    let mut aged = None;
    while let Some(item) = keys.next().await {
        let key = item.map_err(|error| CoreError::store(&prefix, &error))?;
        // Keys this collector does not recognize are never manifests of
        // its own, so they neither anchor the pass nor end the scan.
        let Some(Ok(manifest_object_id)) = manifest_object_id_of(&key) else {
            continue;
        };
        published_any = true;
        charge(budget)?;
        let Some(metadata) = store
            .head(&key)
            .await
            .map_err(|error| CoreError::store(&key, &error))?
        else {
            continue;
        };
        let aged_out = metadata.last_modified_ms.is_some_and(|written_at_ms| {
            context.now_ms.saturating_sub(written_at_ms) >= grace_window_ms
        });
        if !aged_out {
            break;
        }
        aged = Some((manifest_object_id, key));
    }

    let Some((manifest_object_id, key)) = aged else {
        // Nothing published, nothing unreferenced: a namespace with no
        // manifest of its own has never taken an object out of a file set,
        // and its floor cannot have advanced past its birth without a root.
        return Ok(match published_any {
            true => ReferenceAnchor::Missing,
            false => ReferenceAnchor::NotNeeded,
        });
    };
    charge(budget)?;
    let manifest = match load_namespace_manifest_envelope_if_present(
        store,
        namespace_id,
        &manifest_object_id,
        &key,
    )
    .await
    {
        Ok(Some(manifest)) => manifest,
        Ok(None) => return Ok(ReferenceAnchor::Missing),
        Err(error) => match error.failure_class() {
            ManifestLoadFailureClass::Store => {
                tracing::warn!(
                    namespace_id = %namespace_id,
                    object_key = key,
                    error = %error,
                    "the reference manifest did not read; retaining every aged candidate"
                );
                return Ok(ReferenceAnchor::Missing);
            }
            ManifestLoadFailureClass::Corrupt => {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "the reference manifest does not load: {error}"
                ))
                .into());
            }
        },
    };
    Ok(ReferenceAnchor::Manifest(Box::new(AnchoredManifest {
        manifest_object_id,
        head_seq: manifest.payload.head_seq,
        segments: manifest
            .payload
            .segments
            .iter()
            .map(metadata_segment_object_key)
            .collect(),
    })))
}
