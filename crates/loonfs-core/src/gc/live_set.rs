//! Collects the live set: every object the current namespace state
//! can still reach, re-verified in chunks as the sweep advances.

use super::fork_checkpoints::fork_target_proven_gone;
use super::reap::lease_expired;
use crate::checkpoint::load_namespace_manifest_envelope_if_present;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::basis::{read_head_and_metadata_basis, resolve_retention_floor_seq};
use crate::wal::{load_validated_wal_chain, WalChainLoadRequest};
use futures::StreamExt;
use loonfs_api::wire::control::{
    decode_control_object, CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState,
    ControlObjectKind, NamespaceState,
};
use loonfs_api::{ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{checkpoint_prefix, metadata_manifest_object};
use loonfs_objectstore::ObjectStore;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Everything reachable from the fresh root set (rule 4).
pub(super) struct LiveSet {
    pub(super) manifests: BTreeSet<ManifestObjectId>,
    pub(super) tables: BTreeSet<String>,
    pub(super) wal_segments: BTreeSet<String>,
    pub(super) checkpoint_keys: BTreeSet<String>,
    /// Still-active records whose basis manifest is verifiably absent —
    /// the crash window between record write and verification. The pass
    /// releases them; they never degrade sweeping.
    pub(super) missing_basis_records: BTreeSet<String>,
    /// Record resolution failed somewhere: manifest/table deletion must not
    /// proceed on this pass.
    pub(super) degraded: bool,
    /// The inspected namespace head is the terminal, absorbing tombstone.
    pub(super) namespace_deleted: bool,
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
        context: &MutationContext,
    ) -> Result<()> {
        if self.decided_since_collect >= self.reverify_chunk {
            self.live = Arc::new(collect_live_set(store, namespace_id, context).await?);
            self.degraded |= self.live.degraded;
            self.decided_since_collect = 0;
        }
        self.decided_since_collect += 1;
        Ok(())
    }
}

pub(super) async fn collect_live_set<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) -> Result<LiveSet> {
    let now_ms = context.now_ms;
    let loaded = read_head_and_metadata_basis(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?;
    let head = loaded.head.envelope.state;
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
    let floor_seq = resolve_retention_floor_seq(store, &head)
        .await
        .map_err(CoreError::load_head)?;

    let namespace_deleted = head.state == NamespaceState::Deleted;
    let mut live = LiveSet {
        manifests: BTreeSet::new(),
        tables: BTreeSet::new(),
        wal_segments: BTreeSet::new(),
        checkpoint_keys: BTreeSet::new(),
        missing_basis_records: BTreeSet::new(),
        degraded: false,
        namespace_deleted,
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
        let Some(body) = store
            .get_with_metadata(&key)
            .await
            .map_err(|error| CoreError::store(&key, &error))?
        else {
            continue;
        };
        match decode_control_object::<CheckpointRecordState>(
            &body.bytes,
            ControlObjectKind::CheckpointRecord,
        ) {
            Ok(envelope) => {
                let record = envelope.state;
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
                        fork_target_proven_gone(store, target_namespace_id, &record, context)
                            .await?
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
            // Unreadable records are ambiguous roots: retain them and keep
            // sweeping conservative for manifests/tables.
            Err(_) => {
                live.checkpoint_keys.insert(key);
                live.degraded = true;
            }
        }
    }

    // Live manifests protect their tables (rule 6: only validated manifests
    // are trusted to protect data — the envelope loader checks the payload
    // checksum).
    for manifest_object_id in live.manifests.clone() {
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
            Err(_) => {
                live.degraded = true;
            }
        }
    }

    // Keep every WAL segment needed to replay from the floor through the
    // head. The floor never passes the root's basis, so this also covers
    // root-to-head replay (rule 7). A terminal namespace has no replay
    // future: its chain ages out.
    if !namespace_deleted && head.seq > floor_seq {
        let chain = load_validated_wal_chain(
            store,
            WalChainLoadRequest {
                namespace_id,
                chain_base_seq: floor_seq,
                head_seq: head.seq,
                visible_tip: head.visible_wal_tip.clone(),
                stop_after_seq: None,
                recent_segments: &head.recent_segments,
            },
        )
        .await
        .map_err(|error| {
            CoreError::MetadataProjection(MetadataProjectionLoadError::WalChainLoad(error))
        })?;
        for segment in chain.segments() {
            live.wal_segments.insert(segment.object_key().to_owned());
        }
    }

    Ok(live)
}
