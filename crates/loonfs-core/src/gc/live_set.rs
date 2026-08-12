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
    read_head_and_metadata_basis, resolve_retention_floor_seq, LoadedNamespaceBasis,
};
use crate::wal::{load_wal_chain_within, WalChainLoad, WalChainLoadRequest};
use futures::StreamExt;
use loonfs_api::wire::control::{CheckpointOwner, CheckpointRecordLifecycle, NamespaceState};
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
    let loaded = read_head_and_metadata_basis(store, namespace_id)
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
    let now_ms = context.now_ms;
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
    // A missing floor means retain from the namespace's birth sequence
    // (format spec, "WAL floor").
    if !budget.try_charge() {
        return Ok(LiveSetCollection::BudgetExhausted);
    }
    let floor_seq = resolve_retention_floor_seq(store, head)
        .await
        .map_err(CoreError::load_head)?;

    let namespace_deleted = head.state == NamespaceState::Deleted;
    let mut live = LiveSet {
        manifests: BTreeSet::new(),
        tables: BTreeSet::new(),
        wal_segments: BTreeSet::new(),
        wal_content_ids: BTreeSet::new(),
        checkpoint_keys: BTreeSet::new(),
        missing_basis_records: BTreeSet::new(),
        degraded: false,
        namespace_deleted,
        anchor: ReferenceAnchor::NotNeeded,
    };
    // Terminal namespaces forget (format spec, rule 4): the tombstone pair
    // and the root/floor pointers survive as non-candidates, but nothing
    // else is a root except fork-owned records protecting a live target —
    // reads are impossible (`namespace_deleted` at every surface, and epoch
    // acquire refuses the tombstone), so user pins and the final replay
    // chain protect nothing.
    if !namespace_deleted {
        live.manifests.extend(root_manifest_object_id.clone());
    }
    let mut active_record_bases: BTreeMap<ManifestObjectId, Vec<String>> = BTreeMap::new();

    // Every readable checkpoint record roots its basis, no matter its
    // lifecycle, expiry, or owner — no exceptions. An active record roots it
    // because it still serves reads, expiry or not: turning a passed expiry
    // into a release is the compare-and-swap below, and until that lands the
    // record is a pin. A released record roots it because deletion runs data
    // first and records last, so a record still on the store never has its
    // basis pulled out from under it inside a pass. State, expiry, and owner
    // fate gate only whether the record itself is a candidate.
    let checkpoints_prefix = checkpoint_prefix(namespace_id.as_str());
    let mut checkpoint_keys = store.list_prefix_stream(&checkpoints_prefix);
    while let Some(item) = checkpoint_keys.next().await {
        let key = item.map_err(|error| CoreError::store(&checkpoints_prefix, &error))?;
        if !budget.try_charge() {
            return Ok(LiveSetCollection::BudgetExhausted);
        }
        let loaded = load_checkpoint_record_at_key(store, &key).await;
        let record = match loaded {
            Ok(loaded) => loaded.state,
            Err(ControlObjectLoadError::MissingObject { .. }) => continue,
            Err(error) => return Err(core_control_load_error(error)),
        };
        // What makes a record a candidate depends on who owns it.
        // A user pin answers to its own expiry. A fork pin answers
        // to its target's fate, and only to that: the lease is one
        // input to proving an attempt abandoned, never a reason to
        // drop a pin whose target is alive and reading through it.
        // Every check here is repeated at decision time; this only
        // selects candidates.
        let candidate = match &record.owner {
            _ if record.state != (CheckpointRecordLifecycle::Active {}) => true,
            CheckpointOwner::User { .. } => lease_expired(&record, now_ms),
            CheckpointOwner::Fork {
                target_namespace_id,
            } => {
                if !budget.try_charge() {
                    return Ok(LiveSetCollection::BudgetExhausted);
                }
                fork_target_proven_gone(store, target_namespace_id, &record, context).await?
            }
        };
        if namespace_deleted {
            // On a terminal namespace only a fork record with a live
            // target roots anything; every other record is an
            // ordinary candidate, and its basis is not rooted (no
            // reader can reach a tombstone).
            if candidate || matches!(record.owner, CheckpointOwner::User { .. }) {
                continue;
            }
            live.manifests.insert(record.manifest_object_id.clone());
            active_record_bases
                .entry(record.manifest_object_id)
                .or_default()
                .push(key.clone());
            live.checkpoint_keys.insert(key);
            continue;
        }
        live.manifests.insert(record.manifest_object_id.clone());
        // A candidate still roots its basis above; it is only kept
        // out of the protected key set so the sweep can act on it.
        if candidate {
            continue;
        }
        active_record_bases
            .entry(record.manifest_object_id)
            .or_default()
            .push(key.clone());
        live.checkpoint_keys.insert(key);
    }

    // Live manifests protect their tables (rule 6: only validated manifests
    // are trusted to protect data — the envelope loader checks the payload
    // checksum).
    for manifest_object_id in live.manifests.clone() {
        if !budget.try_charge() {
            return Ok(LiveSetCollection::BudgetExhausted);
        }
        let manifest_key = metadata_manifest_object(namespace_id.as_str(), &manifest_object_id);
        match load_namespace_manifest_envelope_if_present(
            store,
            namespace_id,
            &manifest_object_id,
            &manifest_key,
        )
        .await
        {
            Ok(Some(manifest)) => {
                for file in &manifest.payload.metadata_files {
                    live.tables.insert(file.object_key.clone());
                }
            }
            // Absent is not ambiguous. The root's manifest missing is real
            // corruption and degrades the pass; a record-rooted basis that
            // is verifiably gone marks the still-active records above it as
            // zombies — the crash window between record write and verify —
            // and the pass releases them below instead of degrading forever.
            Ok(None) => {
                if Some(&manifest_object_id) == root_manifest_object_id.as_ref() {
                    live.degraded = true;
                } else if let Some(record_keys) = active_record_bases.get(&manifest_object_id) {
                    live.missing_basis_records
                        .extend(record_keys.iter().cloned());
                }
            }
            // A manifest the store could not hand over is a transient
            // obstacle: keep the pass conservative and let the next one try
            // again. A manifest that was read but does not validate is
            // corruption, and the pass reports it instead of degrading every
            // future pass over this namespace. `failure_class` is the same
            // split every other surface uses (`error.rs`), so this arm
            // cannot drift from them.
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
                    )));
                }
            },
        }
    }

    // The second anchor: what this namespace referenced a grace window ago.
    // A terminal namespace needs none — no read can reach a tombstone, and
    // its whole tree is meant to age out.
    let selected = match (namespace_deleted, reused_anchor) {
        (true, _) => Some(ReferenceAnchor::NotNeeded),
        (false, Some(anchor)) => Some(anchor),
        (false, None) => {
            select_reference_anchor(store, namespace_id, grace_window_ms, budget, context).await?
        }
    };
    let Some(anchor) = selected else {
        return Ok(LiveSetCollection::BudgetExhausted);
    };
    live.anchor = anchor;
    // The anchor roots what it named exactly as the current root does. Its
    // own key goes in too, so the pass cannot sweep away the evidence the
    // next pass needs.
    if let ReferenceAnchor::Manifest(anchor) = &live.anchor {
        live.manifests.insert(anchor.manifest_object_id.clone());
        live.tables.extend(anchor.tables.iter().cloned());
    }

    // Keep every WAL segment needed to replay from the floor through the
    // head. The floor never passes the root's basis, so this also covers
    // root-to-head replay (rule 7). A terminal namespace has no replay
    // future: its chain ages out.
    if !namespace_deleted && head.seq > floor_seq {
        // The loader keeps no budget of its own, because foreground reads
        // and the changefeed share it. The pass caps the load at what it
        // has left to spend instead. The loader then issues no more
        // requests than the pass can pay for, and the charge below can
        // never go past the bound.
        let remaining = budget.remaining();
        if remaining == 0 {
            return Ok(LiveSetCollection::BudgetExhausted);
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
        // Either way the pass pays for the requests the loader issued, not
        // for the segments it came back with. The two differ when a request
        // fails or a hint names an object the chain does not use, and the
        // store was asked either way.
        let chain = match load {
            WalChainLoad::Complete {
                chain,
                requests_issued,
            } => {
                budget.charge_block(u64::try_from(requests_issued).unwrap_or(u64::MAX));
                chain
            }
            // The load stopped where the budget did. The pass pays for what
            // it spent getting there and then stops, because a partial
            // chain roots nothing.
            WalChainLoad::LimitReached { requests_issued } => {
                budget.charge_block(u64::try_from(requests_issued).unwrap_or(u64::MAX));
                return Ok(LiveSetCollection::BudgetExhausted);
            }
        };
        for segment in chain.segments() {
            live.wal_segments.insert(segment.object_key().to_owned());
            // The bodies are decoded and validated right here, so the
            // content these commits name is read off them now rather than
            // by a second pass over the same objects.
            for record in segment.records() {
                for delta in &record.deltas {
                    if let WalDelta::AppendFileRevision { content_ref, .. } = &delta.delta {
                        live.wal_content_ids.insert(content_ref.content_id.clone());
                    }
                }
            }
        }
    }

    Ok(LiveSetCollection::Complete(live))
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
/// `None` reports that the budget ran out. A pass that cannot pay for this
/// has no root set at all, the same as any other partial collection.
async fn select_reference_anchor<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    grace_window_ms: u64,
    budget: &mut PassBudget,
    context: &MutationContext,
) -> Result<Option<ReferenceAnchor>> {
    let prefix = metadata_manifest_prefix(namespace_id.as_str());
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
        if !budget.try_charge() {
            return Ok(None);
        }
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
        return Ok(Some(match published_any {
            true => ReferenceAnchor::Missing,
            false => ReferenceAnchor::NotNeeded,
        }));
    };
    if !budget.try_charge() {
        return Ok(None);
    }
    // A manifest that will not load is no evidence. Retaining is what the
    // pass does with every root it cannot resolve.
    let Ok(Some(manifest)) =
        load_namespace_manifest_envelope_if_present(store, namespace_id, &manifest_object_id, &key)
            .await
    else {
        tracing::debug!(
            namespace_id = %namespace_id,
            object_key = key,
            "the reference manifest did not load; retaining every aged candidate"
        );
        return Ok(Some(ReferenceAnchor::Missing));
    };
    Ok(Some(ReferenceAnchor::Manifest(Box::new(
        AnchoredManifest {
            manifest_object_id,
            head_seq: manifest.payload.head_seq,
            tables: manifest
                .payload
                .metadata_files
                .iter()
                .map(|file| file.object_key.clone())
                .collect(),
        },
    ))))
}
