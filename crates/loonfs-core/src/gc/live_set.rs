//! Collects the live set: every object the current namespace state
//! can still reach, re-verified in chunks as the sweep advances.

use super::config::GcConfig;
use super::fork_checkpoints::fork_target_proven_gone;
use crate::checkpoint::load_namespace_manifest_envelope_if_present;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::namespace::control::{
    read_head_object, read_metadata_root_object, read_wal_floor_object, ControlObjectLoadError,
};
use crate::wal::{load_validated_wal_chain, WalChainLoadRequest};
use futures::StreamExt;
use loonfs_api::wire::control::{
    decode_control_object, CheckpointOwner, CheckpointRecordLifecycle, CheckpointRecordState,
    ControlObjectKind, NamespaceState,
};
use loonfs_api::{ChangeSeq, ManifestObjectId, NamespaceId};
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
        config: &GcConfig,
        context: &MutationContext,
    ) -> Result<()> {
        if self.decided_since_collect >= self.reverify_chunk {
            self.live = Arc::new(collect_live_set(store, namespace_id, config, context).await?);
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
    config: &GcConfig,
    context: &MutationContext,
) -> Result<LiveSet> {
    let now_ms = context.now_ms;
    let loaded_head = read_head_object(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?;
    let head = loaded_head.envelope.state;
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(CoreError::load_head)?
        .envelope
        .state;
    let floor_seq = match read_wal_floor_object(store, namespace_id).await {
        Ok(loaded) => loaded.envelope.state.floor_seq,
        // A missing floor means retain everything (format spec, "WAL floor").
        Err(ControlObjectLoadError::MissingObject { .. }) => ChangeSeq(0),
        Err(error) => return Err(CoreError::load_head(error)),
    };

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
        live.manifests.insert(root.manifest_object_id.clone());
    }
    let mut active_record_bases: BTreeMap<ManifestObjectId, Vec<String>> = BTreeMap::new();

    // Every readable non-condemned checkpoint record roots its basis, no
    // matter its lifecycle, expiry, or owner: a revivable record must never
    // outlive its manifest. Released records can come back through
    // deterministic create or fork freshen. Condemned is the sole exception:
    // it is absorbing, so a crash between condemn and delete leaves no future
    // revival to protect. The basis becomes collectable after condemnation,
    // while records-last ordering keeps a newly condemned record's basis
    // alive through the pass that performed the CAS.
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
                if record.state == CheckpointRecordLifecycle::Condemned {
                    continue;
                }
                let expired = record
                    .expires_at_ms
                    .is_some_and(|expires_at_ms| expires_at_ms <= now_ms);
                if namespace_deleted {
                    // On a terminal namespace only a still-active fork
                    // record with a live target roots anything; every
                    // other record is an ordinary age-gated candidate,
                    // and its basis is not rooted (revival is impossible
                    // once epoch acquire refuses the tombstone).
                    if record.state != CheckpointRecordLifecycle::Active || expired {
                        continue;
                    }
                    let CheckpointOwner::Fork {
                        target_namespace_id,
                    } = &record.owner
                    else {
                        continue;
                    };
                    if let Some(last_modified_ms) = body.metadata.last_modified_ms {
                        if fork_target_proven_gone(
                            store,
                            target_namespace_id,
                            last_modified_ms,
                            config,
                            context,
                        )
                        .await?
                        {
                            continue;
                        }
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
                if record.state != CheckpointRecordLifecycle::Active || expired {
                    continue;
                }
                // An active fork-owned record whose target is provably gone
                // is no longer a root: it becomes a sweep candidate for the
                // compare-and-swap release. Every check here is repeated at
                // decision time; this only selects candidates.
                if let CheckpointOwner::Fork {
                    target_namespace_id,
                } = &record.owner
                {
                    if let Some(last_modified_ms) = body.metadata.last_modified_ms {
                        if fork_target_proven_gone(
                            store,
                            target_namespace_id,
                            last_modified_ms,
                            config,
                            context,
                        )
                        .await?
                        {
                            continue;
                        }
                    }
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
                if manifest_object_id == root.manifest_object_id {
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
