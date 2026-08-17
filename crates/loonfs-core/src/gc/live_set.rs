//! Collects the live set: every object the current namespace state
//! can still reach, re-verified in chunks as the sweep advances.

use super::budget::PassBudget;
use super::fork_checkpoints::fork_target_proven_gone;
use super::reap::{lease_expired, manifest_object_id_of};
use crate::checkpoint::record::load_checkpoint_record_at_key;
use crate::checkpoint::{load_namespace_manifest_envelope_if_present, ManifestLoadFailureClass};
use crate::context::MutationContext;
use crate::control_object::{core_control_load_error, ControlObjectLoadError};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::basis::{
    load_head_and_metadata_basis, resolve_retention_floor_seq, LoadedNamespaceBasis,
};
use crate::wal::{load_wal_chain_within, WalChainLoad, WalChainLoadRequest};
use futures::StreamExt;
use loonfs_api::wire::control::{
    CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState, HeadState, NamespaceState,
};
use loonfs_api::wire::wal::WalDelta;
use loonfs_api::{wal_segment_id_start_seq, ChangeSeq, ContentId, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, metadata_manifest_prefix, wal_segment_id_from_key,
};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Everything reachable from the fresh root set (rule 4).
pub(super) struct LiveSet {
    pub(super) manifests: BTreeSet<ManifestObjectId>,
    pub(super) tables: BTreeSet<String>,
    pub(super) wal_segments: BTreeSet<String>,
    /// Content the retained chain's commits append, harvested from the same
    /// decoded bodies the chain walk validated. These are the references
    /// that protect bytes a durable commit named but no manifest has
    /// materialized yet, and reading them here is why no later scan fetches
    /// a segment a second time.
    pub(super) wal_content_ids: BTreeSet<ContentId>,
    pub(super) checkpoint_keys: BTreeSet<String>,
    /// Still-active records whose basis manifest is verifiably absent —
    /// the crash window between record write and verification. The pass
    /// releases them; they never degrade sweeping.
    pub(super) missing_basis_records: BTreeSet<String>,
    /// A root manifest did not resolve: it read as absent, or the read
    /// failed. Manifest and table deletion must not proceed on this pass.
    /// A corrupt root never lands here, because it fails the pass instead.
    pub(super) degraded: bool,
    /// The inspected namespace head is the terminal, absorbing tombstone.
    pub(super) namespace_deleted: bool,
    /// What this pass knows about the references this namespace held when
    /// the grace window opened.
    pub(super) anchor: ReferenceAnchor,
}

impl LiveSet {
    fn collecting(namespace_deleted: bool) -> Self {
        Self {
            manifests: BTreeSet::new(),
            tables: BTreeSet::new(),
            wal_segments: BTreeSet::new(),
            wal_content_ids: BTreeSet::new(),
            checkpoint_keys: BTreeSet::new(),
            missing_basis_records: BTreeSet::new(),
            degraded: false,
            namespace_deleted,
            anchor: ReferenceAnchor::NotNeeded,
        }
    }

    /// Whether anything still protects one WAL segment key.
    ///
    /// The current chain protects what a read at this instant replays; the
    /// reference anchor protects what a read pinned earlier in the grace
    /// window replays, which is every segment above the anchor's own head.
    pub(super) fn protects_wal_segment(&self, key: &str) -> bool {
        self.wal_segments.contains(key) || self.anchor.replays_wal_segment(key)
    }
}

/// The reference manifest and what it named — the pass's evidence about
/// which objects this namespace referenced a grace window ago.
///
/// The grace window exists to protect a reader that pinned an anchor (a head
/// and the basis manifest under it) and is still reading through it. Aging an
/// unreferenced object by its own write time protects the wrong thing: a
/// table written yesterday and superseded by a fold one second ago has
/// already outlived any window, so it is reaped while the reader that pinned
/// the anchor just before the fold is still reading it. The window has to run
/// from the moment an object stopped being referenced, and nothing durable
/// records that moment.
///
/// Manifests record it collectively. Each one is a timestamped snapshot of
/// what the namespace referenced when it was published, so the newest
/// manifest published at least a grace window ago says what was referenced
/// when the window opened. Call it R. An object is reaped only when the
/// current live set, R, and the object's own write time all agree it is
/// unreferenced:
///
/// 1. it is not reachable now,
/// 2. R did not name it,
/// 3. its provider write time is a grace window old.
///
/// **The theorem.** Take a reader that pinned a head at any instant T inside
/// the last grace window. It can only reference what the namespace
/// referenced at T. If the object was written after the window opened, arm 3
/// keeps it. Otherwise it was written before, so the publication that first
/// referenced it had already landed by T — publications self-enforce a
/// budget below the grace floor (`limits::GC_MIN_GRACE_WINDOW_MS`) — and
/// references only start at a publication. Its reference span is one
/// interval: manifests are built on their immediate predecessor
/// (`checkpoint/publish.rs`) and every id is freshly generated, so an object
/// that leaves a file set never re-enters one. R's publication falls inside
/// that span, because it is at or before the window's opening and the object
/// was still referenced at T. So R named it, and arm 2 keeps it. Either way
/// the reader's object survives the pass.
///
/// R protects itself, so the anchor cannot be swept out from under the
/// namespace: it is in its own reference set, and it stops being the anchor
/// only once a newer manifest has aged past the window in its place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ReferenceAnchor {
    /// R, with everything it named.
    Manifest(Box<AnchoredManifest>),
    /// This namespace has never published a manifest, so no publication has
    /// ever stopped referencing anything: everything a reader could hold is
    /// the WAL chain from the namespace's birth, which the floor cannot have
    /// advanced past without a root. Or the namespace is the terminal
    /// tombstone, which no read can reach at all. Aged candidates are debris
    /// either way, and the pass reaps them.
    NotNeeded,
    /// Manifests exist but none has aged past the window, so nothing proves
    /// an unreferenced object was already unreferenced when the window
    /// opened. The pass keeps every aged candidate instead of guessing.
    Missing,
}

/// One reference manifest, reduced to what a sweep decision asks of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AnchoredManifest {
    manifest_object_id: ManifestObjectId,
    /// Greatest sequence the anchor materialized. A read pinned on it
    /// replays every segment above this, and a manifest always materializes
    /// through the end of a committed segment, so nothing above it is reaped.
    head_seq: ChangeSeq,
    /// Object keys of the metadata tables the anchor named.
    tables: BTreeSet<String>,
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

/// What one collection produced. Marking is all or nothing: a partial root
/// set is not a smaller answer, it is no answer, and deciding a deletion
/// against one would delete something live.
pub(super) enum LiveSetCollection {
    Complete(LiveSet),
    BudgetExhausted,
}

/// Why a collection stopped. `?` carries both out of a family collector.
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
    /// The roots, when the collection got all of them.
    #[cfg(test)]
    pub(super) fn complete(self) -> Option<LiveSet> {
        match self {
            Self::Complete(live) => Some(live),
            Self::BudgetExhausted => None,
        }
    }
}

/// Whether the pass may go on from here, or stopped because its budget did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SweepStep {
    Continue,
    BudgetExhausted,
}

/// Delete-time re-verification state (rule 3): deletion decisions consult a
/// live set no staler than `reverify_chunk` candidates. Rule 5 degradation
/// is sticky for the pass once any collection observes it.
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
            // The anchor is a fact about the past, so a re-collection reuses
            // the one the pass opened with rather than paying for it again.
            // Nothing inside a pass can change it: the pass's clock is
            // fixed, a manifest published while it runs is young, and the
            // anchor is protected from the pass's own sweep.
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
                // Rule 3 does not bend for a budget: a decision needs a
                // live set no staler than the chunk, so a pass that cannot
                // pay for a fresh one stops sweeping rather than deciding
                // against the stale one.
                LiveSetCollection::BudgetExhausted => return Ok(SweepStep::BudgetExhausted),
            }
        }
        self.decided_since_collect += 1;
        Ok(SweepStep::Continue)
    }
}

/// Collects against a freshly read head and basis. Re-collecting is what a
/// mid-sweep refresh is for, so it pays for the pair like the pass's own
/// first read did.
pub(super) async fn recollect_live_set<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    grace_window_ms: u64,
    anchor: Option<ReferenceAnchor>,
    budget: &mut PassBudget,
    context: &MutationContext,
) -> Result<LiveSetCollection> {
    if !budget.try_charge() {
        return Ok(LiveSetCollection::BudgetExhausted);
    }
    let loaded = load_head_and_metadata_basis(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?;
    collect_live_set(
        store,
        namespace_id,
        &loaded,
        grace_window_ms,
        anchor,
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
    let namespace_deleted = head.state == NamespaceState::Deleted;
    let collected: CollectResult<LiveSet> = async {
        // A missing floor means retain from the namespace's birth sequence
        // (format spec, "WAL floor").
        charge(budget)?;
        let floor_seq = resolve_retention_floor_seq(store, head)
            .await
            .map_err(CoreError::load_head)?;
        let mut live = LiveSet::collecting(namespace_deleted);
        let mut manifest_ids = BTreeSet::new();
        if !namespace_deleted {
            manifest_ids.extend(root_manifest_object_id.clone());
        }
        let active_record_bases = collect_checkpoint_records(
            store,
            namespace_id,
            namespace_deleted,
            &mut manifest_ids,
            &mut live,
            budget,
            context,
        )
        .await?;
        collect_manifest_tables(
            store,
            namespace_id,
            root_manifest_object_id.as_ref(),
            manifest_ids,
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

/// Collects checkpoint roots and the protected record keys that named them.
/// Every readable record roots its basis in a live namespace, even when the
/// record itself is a sweep candidate.
async fn collect_checkpoint_records<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    namespace_deleted: bool,
    manifest_ids: &mut BTreeSet<ManifestObjectId>,
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
            Err(error) => return Err(core_control_load_error(error).into()),
        };
        let candidate = checkpoint_is_candidate(store, &record, budget, context).await?;
        // A tombstone has no readers; only a fork record whose target still
        // lives continues to root its source basis.
        if namespace_deleted && (candidate || matches!(record.owner, CheckpointOwner::User { .. }))
        {
            continue;
        }
        manifest_ids.insert(record.manifest_object_id.clone());
        // A candidate still roots its basis in a live namespace; it is only
        // kept out of the protected key set so the sweep can act on it.
        if !candidate {
            active_record_bases
                .entry(record.manifest_object_id)
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
    // User pins answer to expiry. Fork pins answer only to target fate; the
    // lease alone never drops a pin whose target is alive.
    match &record.owner {
        _ if record.state != (CheckpointRecordLifecycle::Active {}) => Ok(true),
        CheckpointOwner::User { .. } => Ok(lease_expired(record, context.now_ms)),
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

async fn collect_manifest_tables<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    root_manifest_object_id: Option<&ManifestObjectId>,
    manifest_ids: BTreeSet<ManifestObjectId>,
    active_record_bases: &ActiveRecordBases,
    live: &mut LiveSet,
    budget: &mut PassBudget,
) -> CollectResult<()> {
    // Only validated manifest envelopes may protect their table objects.
    for manifest_object_id in &manifest_ids {
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
            Ok(Some(manifest)) => live.tables.extend(
                manifest
                    .payload
                    .metadata_files
                    .iter()
                    .map(|file| file.object_key.clone()),
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
                        "a root manifest did not read; this pass reclaims no manifests or tables"
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
    live.manifests = manifest_ids;
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
    // Tombstones need no historical root, and refreshes reuse the pass's
    // original anchor because it is a fixed fact about the past.
    live.anchor = match (live.namespace_deleted, reused_anchor) {
        (true, _) => ReferenceAnchor::NotNeeded,
        (false, Some(anchor)) => anchor,
        (false, None) => {
            select_reference_anchor(store, namespace_id, grace_window_ms, budget, context).await?
        }
    };
    // The anchor roots what it named exactly as the current root does. Its
    // own key goes in too, so the pass cannot sweep away the evidence the
    // next pass needs.
    if let ReferenceAnchor::Manifest(anchor) = &live.anchor {
        live.manifests.insert(anchor.manifest_object_id.clone());
        live.tables.extend(anchor.tables.iter().cloned());
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
    // A live namespace keeps the complete replay chain from floor to head.
    // A terminal namespace has no replay future.
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
            // The incomplete chain is not inspected, so it contributes no
            // roots before the typed budget stop discards the whole set.
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
        tables: manifest
            .payload
            .metadata_files
            .iter()
            .map(|file| file.object_key.clone())
            .collect(),
    })))
}
