//! Mark-and-sweep garbage collection (format spec, "Garbage collection").
//!
//! GC and floor advancement are the only code paths that list the store.
//! Nothing sweeps by default: callers opt in through the admin endpoint or
//! an explicit maintenance-tick option. Writers never coordinate with GC,
//! so a sweep can race an in-flight publish or checkpoint creation. The
//! grace window, delete-time re-verification, and retain-on-ambiguity
//! defaults close those races. When in doubt, this module retains.

use crate::checkpoint::load_namespace_manifest_envelope;
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::limits::GC_MIN_GRACE_WINDOW_MS;
use crate::namespace::control::{
    read_head_object, read_metadata_root_object, read_wal_floor_object, ControlObjectLoadError,
};
use crate::wal::{load_validated_wal_chain, WalChainLoadRequest};
use bytes::Bytes;
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, CheckpointOwner, CheckpointRecordEnvelope,
    CheckpointRecordLifecycle, CheckpointRecordState, ControlObjectKind, NamespaceState,
};
use loonfs_api::{ChangeSeq, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{
    checkpoint_prefix, index_segment_prefix, metadata_manifest_prefix, metadata_table_prefix,
    namespace_config, upload_session_prefix, wal_segment_prefix,
};
use loonfs_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Grace and reap windows for the sweep (format spec, "Garbage collection",
/// rules 1 and 9). Both are wall-clock cleanup policy, never validity
/// inputs. The defaults are conservative: every object gets one hour of
/// unconditional protection, and an abandoned bootstrap tree must sit
/// untouched for seven days before it may be reaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcConfig {
    pub grace_window_ms: u64,
    pub reap_window_ms: u64,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            grace_window_ms: 60 * 60 * 1000,
            reap_window_ms: 7 * 24 * 60 * 60 * 1000,
        }
    }
}

impl GcConfig {
    fn validate(&self) -> Result<(), CoreError> {
        // The minimum grace window is derived from the publication budgets
        // and provider deadlines in `limits`. Below it, a publish still in
        // flight could have written objects that already look old enough to
        // delete, so the configuration is rejected outright.
        if self.grace_window_ms < GC_MIN_GRACE_WINDOW_MS {
            return Err(CoreError::InvalidGcConfig(format!(
                "grace_window_ms {} is below the derived safety minimum {}",
                self.grace_window_ms, GC_MIN_GRACE_WINDOW_MS
            )));
        }
        if self.reap_window_ms < self.grace_window_ms {
            return Err(CoreError::InvalidGcConfig(
                "reap_window_ms must be at least the grace window".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    pub deleted_wal_segments: u64,
    pub deleted_metadata_tables: u64,
    /// Unreferenced derived-index segments deleted.
    #[serde(default)]
    pub deleted_index_segments: u64,
    pub deleted_manifests: u64,
    pub deleted_checkpoint_records: u64,
    /// Fork-owned checkpoint records flipped to released because their
    /// target namespace is provably gone (terminally deleted, or an
    /// abandoned bootstrap past the reap window).
    pub released_fork_checkpoints: u64,
    /// Objects removed while reaping an abandoned bootstrap tree (rule 9).
    pub reaped_abandoned_objects: u64,
    /// Upload-session control objects deleted after the reap window.
    #[serde(default)]
    pub deleted_upload_sessions: u64,
    /// Candidates dropped at delete time: still inside the grace window,
    /// missing a provider timestamp, or reachable from the fresh root set.
    pub retained_candidates: u64,
    /// True when a checkpoint record could not be read or validated, which
    /// suppresses manifest and table deletion for the whole pass (rule 5:
    /// ambiguous roots cause retention).
    pub degraded_retention: bool,
}

/// Everything reachable from the fresh root set (rule 4).
struct LiveSet {
    manifests: BTreeSet<ManifestObjectId>,
    tables: BTreeSet<String>,
    /// Derived-index segments named by any live manifest's `index_files`,
    /// protected whatever their family (format spec, section 4.2.2).
    index_segments: BTreeSet<String>,
    wal_segments: BTreeSet<String>,
    checkpoint_keys: BTreeSet<String>,
    /// Record resolution failed somewhere: manifest/table deletion must not
    /// proceed on this pass.
    degraded: bool,
}

pub async fn gc_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<GcReport, CoreError> {
    gc_namespace_with_reverify_chunk(store, namespace_id, config, context, SWEEP_REVERIFY_CHUNK)
        .await
}

/// How many sweep candidates may be decided against one live set before the
/// set is re-collected (rule 3: candidate selection may be stale, deletion
/// may not). On a large sweep, deciding every candidate against a single
/// snapshot would leave the last deletions working from an arbitrarily old
/// view; re-collecting every chunk bounds that staleness.
const SWEEP_REVERIFY_CHUNK: usize = 1024;

async fn gc_namespace_with_reverify_chunk<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
    reverify_chunk: usize,
) -> Result<GcReport, CoreError> {
    config.validate()?;
    let config_key = namespace_config(namespace_id.as_str());
    let namespace_complete = store
        .head(&config_key)
        .await
        .map_err(|error| CoreError::store(&config_key, &error))?
        .is_some();
    if !namespace_complete {
        return reap_abandoned_bootstrap(store, namespace_id, config, context).await;
    }

    // Mark: select sweep candidates against one live-set snapshot. The
    // selection may be stale (rule 3); the delete-time checks below decide.
    let mark = collect_live_set(store, namespace_id, config, context).await?;
    let mut report = GcReport::default();

    let candidate_segments = list_prefix(store, &wal_segment_prefix(namespace_id.as_str())).await?;
    let candidate_tables =
        list_prefix(store, &metadata_table_prefix(namespace_id.as_str())).await?;
    let candidate_index_segments =
        list_prefix(store, &index_segment_prefix(namespace_id.as_str())).await?;
    let candidate_manifests =
        list_prefix(store, &metadata_manifest_prefix(namespace_id.as_str())).await?;
    let candidate_checkpoints =
        list_prefix(store, &checkpoint_prefix(namespace_id.as_str())).await?;
    let candidate_upload_sessions =
        list_prefix(store, &upload_session_prefix(namespace_id.as_str())).await?;

    let segment_candidates: Vec<String> = candidate_segments
        .into_iter()
        .filter(|key| !mark.wal_segments.contains(key))
        .collect();
    let table_candidates: Vec<String> = candidate_tables
        .into_iter()
        .filter(|key| !mark.tables.contains(key))
        .collect();
    let index_segment_candidates: Vec<String> = candidate_index_segments
        .into_iter()
        .filter(|key| !mark.index_segments.contains(key))
        .collect();
    let manifest_candidates: Vec<String> = candidate_manifests
        .into_iter()
        .filter(|key| manifest_object_id_of(key).is_none_or(|id| !mark.manifests.contains(&id)))
        .collect();
    let checkpoint_candidates: Vec<String> = candidate_checkpoints
        .into_iter()
        .filter(|key| !mark.checkpoint_keys.contains(key))
        .collect();

    // Sweep: every deletion is decided against a fresh live set (rule 3:
    // candidate selection may be stale, deletion may not). The set is
    // re-collected every `reverify_chunk` candidates so a large sweep never
    // runs far ahead of its verification snapshot.
    let mut sweep =
        SweepVerifier::collect(store, namespace_id, config, context, reverify_chunk).await?;

    // Delete data before records. Every readable checkpoint record roots
    // its basis in the live set, so the pass that deletes a record can
    // never also delete the data that record was protecting. A crash
    // mid-sweep leaves orphaned data for the next pass — never a record
    // whose data vanished underneath it.
    for key in segment_candidates {
        sweep
            .refresh_if_due(store, namespace_id, config, context)
            .await?;
        if sweep.live.wal_segments.contains(&key) {
            report.retained_candidates += 1;
            continue;
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.deleted_wal_segments += 1;
        }
    }
    for key in table_candidates {
        sweep
            .refresh_if_due(store, namespace_id, config, context)
            .await?;
        // Rule 5: ambiguous roots suppress manifest and table deletion for
        // the whole pass; degradation seen by any re-collection is sticky.
        if sweep.degraded {
            report.retained_candidates += 1;
            continue;
        }
        if sweep.live.tables.contains(&key) {
            report.retained_candidates += 1;
            continue;
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.deleted_metadata_tables += 1;
        }
    }
    for key in index_segment_candidates {
        sweep
            .refresh_if_due(store, namespace_id, config, context)
            .await?;
        // Index segments follow the table rules: ambiguous roots suppress
        // deletion, live manifests protect every listed key.
        if sweep.degraded {
            report.retained_candidates += 1;
            continue;
        }
        if sweep.live.index_segments.contains(&key) {
            report.retained_candidates += 1;
            continue;
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.deleted_index_segments += 1;
        }
    }
    for key in manifest_candidates {
        sweep
            .refresh_if_due(store, namespace_id, config, context)
            .await?;
        if sweep.degraded {
            report.retained_candidates += 1;
            continue;
        }
        if manifest_object_id_of(&key).is_some_and(|id| sweep.live.manifests.contains(&id)) {
            report.retained_candidates += 1;
            continue;
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.deleted_manifests += 1;
        }
    }
    for key in checkpoint_candidates {
        sweep
            .refresh_if_due(store, namespace_id, config, context)
            .await?;
        if sweep.live.checkpoint_keys.contains(&key) {
            report.retained_candidates += 1;
            continue;
        }
        // Active fork-owned records are released by compare-and-swap,
        // never deleted outright, so a fork retrying at the same moment
        // finds a released record it can revive instead of a vanished one.
        // Everything else (released records, expired user records) goes
        // through the normal age-gated delete.
        match maybe_release_fork_checkpoint(store, &key, config, context).await? {
            ForkCheckpointSweep::Released => {
                report.released_fork_checkpoints += 1;
                continue;
            }
            ForkCheckpointSweep::Retained => {
                report.retained_candidates += 1;
                continue;
            }
            ForkCheckpointSweep::NotAnActiveFork => {}
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.deleted_checkpoint_records += 1;
        }
    }
    // Upload sessions root nothing and nothing durable references them, so
    // provider age past the reap window is the whole decision — the
    // AbortIncompleteMultipartUpload convention. A session that old is dead
    // whatever its state: presigned grants expired long ago, and a
    // `complete` after the window answers session-not-found by design.
    // Writer clocks are never consulted (rule 1: provider timestamps only).
    for key in candidate_upload_sessions {
        if delete_if_aged(store, &key, config.reap_window_ms, context, &mut report).await? {
            report.deleted_upload_sessions += 1;
        }
    }
    report.degraded_retention = sweep.degraded;

    Ok(report)
}

enum ForkCheckpointSweep {
    /// The record was flipped `active -> released` under its etag.
    Released,
    /// The record must survive this pass (young, target ambiguous, or the
    /// release compare-and-swap lost a race).
    Retained,
    /// Not an active fork-owned record; the normal delete path decides.
    NotAnActiveFork,
}

/// Decides one active fork-owned sweep candidate immediately before acting
/// (rule 3): re-reads the record, re-proves the target namespace is gone,
/// and releases the record by compare-and-swap on the just-observed etag.
/// The etag check means a concurrent fork freshen either wins the swap
/// outright or observes the release — the two can never both succeed.
async fn maybe_release_fork_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<ForkCheckpointSweep, CoreError> {
    let Some(body) = store
        .get_with_metadata(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?
    else {
        return Ok(ForkCheckpointSweep::NotAnActiveFork);
    };
    let Ok(envelope) = decode_control_object::<CheckpointRecordState>(
        &body.bytes,
        ControlObjectKind::CheckpointRecord,
    ) else {
        // Unreadable records are ambiguous roots; never delete one here.
        return Ok(ForkCheckpointSweep::Retained);
    };
    let record = envelope.state;
    if record.state != CheckpointRecordLifecycle::Active {
        return Ok(ForkCheckpointSweep::NotAnActiveFork);
    }
    let CheckpointOwner::Fork {
        target_namespace_id,
    } = &record.owner
    else {
        return Ok(ForkCheckpointSweep::NotAnActiveFork);
    };
    // Rule 1 applies to state changes too: GC leaves objects younger than
    // the grace window alone.
    let Some(last_modified_ms) = body.metadata.last_modified_ms else {
        return Ok(ForkCheckpointSweep::Retained);
    };
    if context.now_ms.saturating_sub(last_modified_ms) < config.grace_window_ms {
        return Ok(ForkCheckpointSweep::Retained);
    }
    if !fork_target_proven_gone(
        store,
        target_namespace_id,
        last_modified_ms,
        config,
        context,
    )
    .await?
    {
        return Ok(ForkCheckpointSweep::Retained);
    }
    let Some(etag) = body.metadata.etag.as_deref() else {
        return Ok(ForkCheckpointSweep::Retained);
    };
    let mut released = record;
    released.state = CheckpointRecordLifecycle::Released;
    let envelope = CheckpointRecordEnvelope::from_state(
        ControlObjectKind::CheckpointRecord,
        &context.writer_version,
        released,
    )
    .map_err(|err| {
        CoreError::Internal(format!("failed to build checkpoint record envelope: {err}"))
    })?;
    let encoded = encode_control_object(&envelope).map_err(|err| {
        CoreError::Internal(format!("failed to encode checkpoint record object: {err}"))
    })?;
    match store
        .compare_and_swap(key, etag, Bytes::from(encoded))
        .await
    {
        Ok(_) => Ok(ForkCheckpointSweep::Released),
        // A fork freshen (or another pass) won the record; retain and let a
        // later pass re-decide against the fresh state.
        Err(loonfs_objectstore::ObjectStoreError::PreconditionFailed { .. })
        | Err(loonfs_objectstore::ObjectStoreError::Conflict { .. }) => {
            Ok(ForkCheckpointSweep::Retained)
        }
        Err(error) => Err(CoreError::store(key, &error)),
    }
}

/// Proves a fork target namespace is gone (rule 9's fork arm). Either its
/// head object says the namespace is terminally deleted, or nothing exists
/// under the target prefix at all and the record has aged past the reap
/// window. The age matters because a live fork retry would have freshened
/// the record; an old record over an empty tree means the fork was
/// abandoned or its partial bootstrap was already reaped.
async fn fork_target_proven_gone<S: ObjectStore + ?Sized>(
    store: &S,
    target_namespace_id: &NamespaceId,
    record_last_modified_ms: u64,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<bool, CoreError> {
    match read_head_object(store, target_namespace_id).await {
        Ok(loaded) => Ok(loaded.envelope.state.state == NamespaceState::Deleted),
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            let target_prefix = format!("namespaces/{}/", target_namespace_id.as_str());
            let tree = list_prefix(store, &target_prefix).await?;
            if !tree.is_empty() {
                // Either a bootstrap is in progress or rule 9 has not
                // reaped the partial tree yet; ambiguity retains.
                return Ok(false);
            }
            Ok(context.now_ms.saturating_sub(record_last_modified_ms) >= config.reap_window_ms)
        }
        // An unreadable target head is not verifiably deleted; retain.
        Err(_) => Ok(false),
    }
}

/// Delete-time re-verification state (rule 3): deletion decisions consult a
/// live set no staler than `reverify_chunk` candidates. Rule 5 degradation
/// is sticky for the pass once any collection observes it.
struct SweepVerifier {
    live: LiveSet,
    degraded: bool,
    reverify_chunk: usize,
    decided_since_collect: usize,
}

impl SweepVerifier {
    async fn collect<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        config: &GcConfig,
        context: &MutationContext,
        reverify_chunk: usize,
    ) -> Result<Self, CoreError> {
        let live = collect_live_set(store, namespace_id, config, context).await?;
        Ok(Self {
            degraded: live.degraded,
            live,
            reverify_chunk,
            decided_since_collect: 0,
        })
    }

    async fn refresh_if_due<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        config: &GcConfig,
        context: &MutationContext,
    ) -> Result<(), CoreError> {
        if self.decided_since_collect >= self.reverify_chunk {
            self.live = collect_live_set(store, namespace_id, config, context).await?;
            self.degraded |= self.live.degraded;
            self.decided_since_collect = 0;
        }
        self.decided_since_collect += 1;
        Ok(())
    }
}

async fn collect_live_set<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<LiveSet, CoreError> {
    let now_ms = context.now_ms;
    let loaded_head = read_head_object(store, namespace_id)
        .await
        .map_err(load_error)?;
    let head = loaded_head.envelope.state;
    let root = read_metadata_root_object(store, namespace_id)
        .await
        .map_err(load_error)?
        .envelope
        .state;
    let floor_seq = match read_wal_floor_object(store, namespace_id).await {
        Ok(loaded) => loaded.envelope.state.floor_seq,
        // A missing floor means retain everything (format spec, "WAL floor").
        Err(ControlObjectLoadError::MissingObject { .. }) => ChangeSeq(0),
        Err(error) => return Err(load_error(error)),
    };

    let mut live = LiveSet {
        manifests: BTreeSet::new(),
        tables: BTreeSet::new(),
        index_segments: BTreeSet::new(),
        wal_segments: BTreeSet::new(),
        checkpoint_keys: BTreeSet::new(),
        degraded: false,
    };
    live.manifests.insert(root.manifest_object_id);

    // Every readable checkpoint record roots its basis, no matter its
    // lifecycle, expiry, or owner: a record must never outlive its
    // manifest. This matters because released records can come back — the
    // grace window keeps young ones around, and both `create_checkpoint`
    // (via deterministic ids) and the fork freshen can revive one — and a
    // revival is only safe while the basis still exists. Lifecycle,
    // expiry, and the fork target's fate decide only whether the record
    // itself may be released or deleted; the basis becomes collectable on
    // the pass after the record is gone.
    for key in list_prefix(store, &checkpoint_prefix(namespace_id.as_str())).await? {
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
                live.manifests.insert(record.manifest_object_id.clone());
                let expired = record
                    .expires_at_ms
                    .is_some_and(|expires_at_ms| expires_at_ms <= now_ms);
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
        match load_namespace_manifest_envelope(store, namespace_id, &manifest_object_id).await {
            Ok(manifest) => {
                for file in &manifest.payload.metadata_files {
                    live.tables.insert(file.object_key.clone());
                }
                for file in &manifest.payload.index_files {
                    live.index_segments.insert(file.object_key.clone());
                }
            }
            Err(_) => {
                live.degraded = true;
            }
        }
    }

    // Keep every WAL segment needed to replay from the floor through the
    // head. The floor never passes the root's basis, so this also covers
    // root-to-head replay (rule 7).
    if head.seq > floor_seq {
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

/// Rule 9: a namespace tree with no `namespace.json` whose newest object is
/// older than the reap window may be reaped, re-checking the completion
/// marker's absence immediately before deleting.
async fn reap_abandoned_bootstrap<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<GcReport, CoreError> {
    let namespace_prefix = format!("namespaces/{}/", namespace_id.as_str());
    let keys = list_prefix(store, &namespace_prefix).await?;
    let mut report = GcReport::default();
    if keys.is_empty() {
        return Ok(report);
    }

    // The newest object bounds the tree's age; a missing timestamp reads as
    // "young" and blocks the reap.
    for key in &keys {
        let Some(metadata) = store
            .head(key)
            .await
            .map_err(|error| CoreError::store(key, &error))?
        else {
            continue;
        };
        let Some(last_modified_ms) = metadata.last_modified_ms else {
            report.retained_candidates += u64::try_from(keys.len()).unwrap_or(u64::MAX);
            return Ok(report);
        };
        if context.now_ms.saturating_sub(last_modified_ms) < config.reap_window_ms {
            report.retained_candidates += u64::try_from(keys.len()).unwrap_or(u64::MAX);
            return Ok(report);
        }
    }

    // Re-check the absence of the completion marker immediately before
    // deleting (rule 9).
    let complete_now = store
        .head(&namespace_config(namespace_id.as_str()))
        .await
        .map_err(|error| CoreError::store(namespace_config(namespace_id.as_str()), &error))?
        .is_some();
    if complete_now {
        report.retained_candidates = u64::try_from(keys.len()).unwrap_or(u64::MAX);
        return Ok(report);
    }
    for key in keys {
        store
            .delete(&key)
            .await
            .map_err(|error| CoreError::store(&key, &error))?;
        report.reaped_abandoned_objects += 1;
    }
    Ok(report)
}

async fn delete_if_aged<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    context: &MutationContext,
    report: &mut GcReport,
) -> Result<bool, CoreError> {
    let Some(metadata) = store
        .head(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?
    else {
        // Already gone; nothing to count.
        return Ok(false);
    };
    let Some(last_modified_ms) = metadata.last_modified_ms else {
        // No provider timestamp: treat as young, retain (rule 1).
        report.retained_candidates += 1;
        return Ok(false);
    };
    if context.now_ms.saturating_sub(last_modified_ms) < grace_window_ms {
        report.retained_candidates += 1;
        return Ok(false);
    }
    store
        .delete(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?;
    Ok(true)
}

async fn list_prefix<S: ObjectStore + ?Sized>(
    store: &S,
    prefix: &str,
) -> Result<Vec<String>, CoreError> {
    store
        .list_prefix(prefix)
        .await
        .map_err(|error| CoreError::store(prefix, &error))
}

fn manifest_object_id_of(key: &str) -> Option<ManifestObjectId> {
    let name = key.rsplit('/').next()?;
    let object_id = name.strip_suffix(".manifest.json")?;
    ManifestObjectId::parse(object_id).ok()
}

fn load_error(error: ControlObjectLoadError) -> CoreError {
    CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::advance_retention_floor;
    use crate::checkpoint::record::set_checkpoint_record_state;
    use loonfs_api::wire::control::CheckpointOwner;

    /// GC lifecycle tests pin as one user owner; owner-specific release
    /// rules are exercised in the fork and release suites.
    async fn create_checkpoint<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        context: &MutationContext,
    ) -> Result<loonfs_api::CreateCheckpointResponse, CoreError> {
        crate::checkpoint::create_checkpoint(
            store,
            namespace_id,
            CheckpointOwner::User {
                name: "test-pin".to_owned(),
            },
            None,
            context,
        )
        .await
    }
    use crate::commit_engine::{NamespaceCommitEngine, NamespaceMutationCandidate};
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::delete::delete_namespace;
    use crate::namespace::fork::fork_namespace;
    use crate::options::DeleteNamespaceOptions;
    use crate::path::read::{load_metadata_view, ReadLoadContext};
    use crate::publish::PathMutationIntent;
    use crate::storage::content::store_bytes_as_content;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_api::{AbsolutePath, CommitId, PutBehavior};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::{ByteRange, ObjectBody, ObjectMetadata, ObjectStoreError, PutMode};
    use tempfile::tempdir;

    const GRACE_MS: u64 = 60 * 60 * 1000;
    const REAP_MS: u64 = 7 * 24 * 60 * 60 * 1000;

    fn config() -> GcConfig {
        GcConfig {
            grace_window_ms: GRACE_MS,
            reap_window_ms: REAP_MS,
        }
    }

    fn context(now_ms: u64) -> MutationContext {
        MutationContext {
            writer_id: "gc-test".to_owned(),
            writer_session_id: "wrs_gc_test".to_owned(),
            writer_version: "gc-test/0.1.0".to_owned(),
            now_ms,
        }
    }

    /// Derives "now" from durable object ages so the tests never touch a
    /// wall clock: `offset_ms` past the newest object under the namespace.
    async fn now_after_newest_object(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        offset_ms: u64,
    ) -> u64 {
        let prefix = format!("namespaces/{}/", namespace_id.as_str());
        let mut newest = 0;
        for key in store.list_prefix(&prefix).await.expect("list namespace") {
            let modified = store
                .head(&key)
                .await
                .expect("head object")
                .expect("object exists")
                .last_modified_ms
                .expect("local fs provides timestamps");
            newest = newest.max(modified);
        }
        assert!(newest > 0, "namespace tree must not be empty");
        newest + offset_ms
    }

    async fn write_file<S: ObjectStore>(
        store: &S,
        namespace_id: &NamespaceId,
        path: &str,
        commit_id: &str,
        context: &MutationContext,
    ) {
        let content_ref = store_bytes_as_content(store, namespace_id, b"body\n")
            .await
            .expect("store content")
            .content_ref;
        NamespaceCommitEngine::new(namespace_id.clone())
            .publish_batch(
                store,
                vec![NamespaceMutationCandidate::Path(
                    PathMutationIntent::PutFile {
                        commit_id: CommitId::parse(commit_id).expect("commit id"),
                        absolute_path: AbsolutePath::parse(path).expect("path"),
                        content_ref,
                        behavior: PutBehavior::NoReplace,
                    },
                )],
                context,
            )
            .await
            .results
            .pop()
            .expect("one result")
            .expect("write file");
    }

    async fn stat_root<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) {
        load_metadata_view(store, namespace_id, ReadLoadContext::latest())
            .await
            .expect("load latest view")
            .resolve_path("/")
            .await
            .expect("resolve root");
    }

    /// `LocalFsStore` with provider timestamps stripped: rule 1 reads an
    /// object without a timestamp as young, so nothing ever ages out.
    #[derive(Debug)]
    struct TimestamplessStore(LocalFsStore);

    fn strip_timestamp(mut metadata: ObjectMetadata) -> ObjectMetadata {
        metadata.last_modified_ms = None;
        metadata
    }

    #[async_trait::async_trait]
    impl ObjectStore for TimestamplessStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            Ok(self.0.head(key).await?.map(strip_timestamp))
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            Ok(self.0.get_with_metadata(key).await?.map(|mut body| {
                body.metadata = strip_timestamp(body.metadata);
                body
            }))
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.0.get(key, range).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            Ok(strip_timestamp(self.0.put(key, bytes, mode).await?))
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.0.delete(key).await
        }

        fn list_prefix_stream(
            &self,
            prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            self.0.list_prefix_stream(prefix)
        }
    }

    /// The derived floor is enforced at validation: a pass configured below
    /// it is rejected as an invalid request before touching the store.
    #[tokio::test]
    async fn gc_rejects_grace_windows_below_the_derived_minimum() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");

        let too_small = GcConfig {
            grace_window_ms: GC_MIN_GRACE_WINDOW_MS - 1,
            reap_window_ms: REAP_MS,
        };
        let error = gc_namespace(&store, &namespace_id, &too_small, &context(1_000))
            .await
            .expect_err("sub-minimum grace window must be rejected");
        assert!(
            matches!(&error, CoreError::InvalidGcConfig(message)
                if message.contains("below the derived safety minimum")),
            "expected invalid gc config, got {error:?}"
        );
        assert_eq!(
            error.code(),
            crate::error::ErrorCode::InvalidRequest,
            "the rejection surfaces as invalid_request"
        );

        let inverted = GcConfig {
            grace_window_ms: GRACE_MS,
            reap_window_ms: GRACE_MS - 1,
        };
        let error = gc_namespace(&store, &namespace_id, &inverted, &context(1_000))
            .await
            .expect_err("reap below grace must be rejected");
        assert!(matches!(error, CoreError::InvalidGcConfig(_)));
    }

    #[tokio::test]
    async fn gc_reaps_below_floor_segments_after_the_grace_window() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("checkpoint");
        advance_retention_floor(&store, &namespace_id, &setup)
            .await
            .expect("advance floor");

        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("gc pass");

        // The only segment sits at the floor with no replay gap above it.
        assert_eq!(report.deleted_wal_segments, 1);
        assert!(!report.degraded_retention);
        stat_root(&store, &namespace_id).await;
    }

    async fn write_upload_session(store: &LocalFsStore, namespace_id: &NamespaceId) -> String {
        let upload_id = loonfs_api::UploadId::parse("upl_0123456789abcdef0123456789abcdef")
            .expect("valid upload id");
        let state = loonfs_api::wire::control::UploadSessionState {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            mode: loonfs_api::v0::UploadMode::ServiceProxied,
            direct_put_content_ref: None,
            staged_content_ref: None,
            completed: None,
            created_at_ms: 1_000,
        };
        let envelope = loonfs_api::wire::control::UploadSessionEnvelope::from_state(
            loonfs_api::wire::control::ControlObjectKind::UploadSession,
            "gc-test/0.1.0",
            state,
        )
        .expect("session envelope");
        let bytes =
            loonfs_api::wire::control::encode_control_object(&envelope).expect("encode session");
        let key =
            loonfs_objectstore::keys::upload_session(namespace_id.as_str(), upload_id.as_str());
        store
            .put_if_absent(&key, bytes::Bytes::from(bytes))
            .await
            .expect("write session");
        key
    }

    #[tokio::test]
    async fn upload_sessions_reap_after_the_window_and_survive_inside_it() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        let session_key = write_upload_session(&store, &namespace_id).await;

        // Past the grace window but inside the reap window: sessions are
        // aged on the reap window, so this pass retains the session.
        let inside = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &inside)
            .await
            .expect("gc pass inside the reap window");
        assert_eq!(report.deleted_upload_sessions, 0);
        assert!(store
            .head(&session_key)
            .await
            .expect("head session")
            .is_some());

        // Past the reap window the session is dead whatever its state:
        // age is the whole decision, and the pass counts the deletion.
        let aged = context(now_after_newest_object(&store, &namespace_id, REAP_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("gc pass past the reap window");
        assert_eq!(report.deleted_upload_sessions, 1);
        assert!(store
            .head(&session_key)
            .await
            .expect("head session")
            .is_none());

        // The pass is idempotent: nothing left to count.
        let again = context(now_after_newest_object(&store, &namespace_id, REAP_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &again)
            .await
            .expect("gc pass after the sweep");
        assert_eq!(report.deleted_upload_sessions, 0);
    }

    #[tokio::test]
    async fn gc_retains_everything_inside_the_grace_window() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("checkpoint");
        advance_retention_floor(&store, &namespace_id, &setup)
            .await
            .expect("advance floor");

        let young = context(now_after_newest_object(&store, &namespace_id, 0).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &young)
            .await
            .expect("gc pass");

        assert_eq!(report.deleted_wal_segments, 0);
        assert_eq!(report.deleted_metadata_tables, 0);
        assert_eq!(report.deleted_manifests, 0);
        assert!(report.retained_candidates > 0);
        stat_root(&store, &namespace_id).await;
    }

    #[tokio::test]
    async fn gc_never_deletes_the_live_replay_chain() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("checkpoint");
        advance_retention_floor(&store, &namespace_id, &setup)
            .await
            .expect("advance floor");
        // A commit past the floor: its segment is the live replay gap.
        write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;

        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("gc pass");

        assert_eq!(report.deleted_wal_segments, 1);
        // Latest reads replay the retained tail over the root basis.
        let view = load_metadata_view(&store, &namespace_id, ReadLoadContext::latest())
            .await
            .expect("load view");
        view.resolve_path("/docs/two.txt")
            .await
            .expect("tail commit stays readable");
    }

    /// A released record whose basis has aged out loses the record first,
    /// and the basis only on the following pass — never the other way
    /// around.
    #[tokio::test]
    async fn gc_reaps_dead_checkpoints_before_their_basis_across_passes() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        let first = create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("first checkpoint");
        write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("second checkpoint");
        let first_record =
            crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
                .await
                .expect("read first record")
                .expect("first record exists")
                .state;
        set_checkpoint_record_state(
            &store,
            &namespace_id,
            &first.checkpoint_id,
            loonfs_api::wire::control::CheckpointRecordLifecycle::Released,
            &setup.writer_version,
        )
        .await
        .expect("mark first dead");

        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let first_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("first gc pass");

        // Pass one deletes the dead record but the record still rooted its
        // basis, so the referenced manifest and tables survive the pass.
        assert_eq!(first_pass.deleted_checkpoint_records, 1);
        assert!(!first_pass.degraded_retention);
        assert!(crate::checkpoint::read_checkpoint_record(
            &store,
            &namespace_id,
            &first.checkpoint_id
        )
        .await
        .expect("read record")
        .is_none());
        let basis = crate::checkpoint::load_namespace_manifest_envelope(
            &store,
            &namespace_id,
            &first_record.manifest_object_id,
        )
        .await
        .expect("dead basis manifest survives its record");
        for file in &basis.payload.metadata_files {
            assert!(
                store
                    .head(&file.object_key)
                    .await
                    .expect("head table")
                    .is_some(),
                "dead basis table survives its record"
            );
        }

        // Pass two finds the basis unreferenced and aged, and reaps it.
        let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("second gc pass");
        assert!(
            second_pass.deleted_manifests >= 1,
            "dead basis manifest reaped once its record is gone"
        );
        assert!(crate::checkpoint::load_namespace_manifest_envelope(
            &store,
            &namespace_id,
            &first_record.manifest_object_id,
        )
        .await
        .is_err());
        stat_root(&store, &namespace_id).await;
    }

    /// Repeated WAL flushes leave superseded manifests unpinned, so GC
    /// reclaims them once aged. This is what keeps retained metadata
    /// bounded when maintenance runs continuously.
    #[tokio::test]
    async fn gc_reclaims_manifests_superseded_by_wal_flushes() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        for round in 0..3 {
            write_file(
                &store,
                &namespace_id,
                &format!("/docs/file-{round}.txt"),
                &format!("gc-adv-{round}"),
                &setup,
            )
            .await;
            crate::checkpoint::flush_wal(&store, &namespace_id, &setup)
                .await
                .expect("flush wal");
        }

        // Record-less maintenance: nothing accumulates under `checkpoints/`.
        assert!(
            store
                .list_prefix(&checkpoint_prefix(namespace_id.as_str()))
                .await
                .expect("list checkpoint records")
                .is_empty(),
            "a wal flush must not create checkpoint records"
        );

        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("gc pass");

        // Three flushes superseded the bootstrap manifest and two
        // intermediates; only the root's manifest is reachable. Its tables
        // are all still referenced (a flush only appends L0 runs).
        assert_eq!(report.deleted_manifests, 3);
        assert!(!report.degraded_retention);
        let manifests_left = list_prefix(&store, &metadata_manifest_prefix(namespace_id.as_str()))
            .await
            .expect("list manifests");
        assert_eq!(manifests_left.len(), 1, "only the live root manifest stays");

        // Reorganization folds the L0 runs into fresh base segments; the
        // superseded run tables then age out on the next pass.
        let fold_policy = crate::checkpoint::MetadataLsmPolicy {
            max_l0_runs: 1,
            ..Default::default()
        };
        for _ in 0..16 {
            let report = crate::checkpoint::reorganize_metadata_step(
                &store,
                &namespace_id,
                &setup,
                fold_policy,
            )
            .await
            .expect("reorganize step");
            if matches!(
                report.outcome,
                crate::checkpoint::MetadataReorganizeOutcome::NotNeeded { .. }
            ) {
                break;
            }
        }
        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let after_fold = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("gc pass after reorganization");
        assert!(
            after_fold.deleted_metadata_tables > 0,
            "folded-away run tables become collectable"
        );
        assert!(!after_fold.degraded_retention);

        stat_root(&store, &namespace_id).await;
        let view = load_metadata_view(&store, &namespace_id, ReadLoadContext::latest())
            .await
            .expect("load view");
        for round in 0..3 {
            view.resolve_path(&format!("/docs/file-{round}.txt"))
                .await
                .expect("file readable after sweep");
        }
    }

    /// The user release lifecycle end to end: release flips the record,
    /// the record reaps first, and the basis follows one pass later.
    #[tokio::test]
    async fn gc_reaps_released_checkpoints_before_their_basis_across_passes() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        let pinned = create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("pin checkpoint");
        write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("advance past the pinned basis");

        let first_release = crate::checkpoint::release_checkpoint(
            &store,
            &namespace_id,
            &pinned.checkpoint_id,
            &setup,
        )
        .await
        .expect("release");
        assert!(first_release.was_active);
        let repeat_release = crate::checkpoint::release_checkpoint(
            &store,
            &namespace_id,
            &pinned.checkpoint_id,
            &setup,
        )
        .await
        .expect("repeat release");
        assert!(!repeat_release.was_active);

        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let first_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("first gc pass");
        assert_eq!(first_pass.deleted_checkpoint_records, 1);

        let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("second gc pass");
        assert!(second_pass.deleted_manifests >= 1);
        // Releasing an already-reaped record stays idempotent success.
        let after_reap = crate::checkpoint::release_checkpoint(
            &store,
            &namespace_id,
            &pinned.checkpoint_id,
            &setup,
        )
        .await
        .expect("release after reap");
        assert!(!after_reap.was_active);
        stat_root(&store, &namespace_id).await;
    }

    /// An expiring pin protects until its expiry, then follows the same
    /// records-first cascade with no explicit release.
    #[tokio::test]
    async fn gc_reaps_expired_checkpoints_before_their_basis_across_passes() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        // Expiry compares the caller's `now_ms` against the record's stamp;
        // object ages come from provider timestamps. Pin one record already
        // expired at any provider-derived "now" and one that never expires.
        let expiring = crate::checkpoint::create_checkpoint(
            &store,
            &namespace_id,
            CheckpointOwner::User {
                name: "short-lived".to_owned(),
            },
            Some(setup.now_ms + GRACE_MS),
            &setup,
        )
        .await
        .expect("expiring checkpoint");
        let lasting = crate::checkpoint::create_checkpoint(
            &store,
            &namespace_id,
            CheckpointOwner::User {
                name: "long-lived".to_owned(),
            },
            Some(u64::MAX),
            &setup,
        )
        .await
        .expect("lasting checkpoint");
        write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("advance past the expiring basis");

        // Past expiry: record first, basis on the following pass.
        let expired = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        assert!(
            expired.now_ms > 1_000 + GRACE_MS,
            "provider clock sits past the expiry"
        );
        let first_pass = gc_namespace(&store, &namespace_id, &config(), &expired)
            .await
            .expect("post-expiry pass");
        assert_eq!(first_pass.deleted_checkpoint_records, 1);
        let second_pass = gc_namespace(&store, &namespace_id, &config(), &expired)
            .await
            .expect("second post-expiry pass");
        let _ = second_pass;
        assert!(crate::checkpoint::read_checkpoint_record(
            &store,
            &namespace_id,
            &expiring.checkpoint_id
        )
        .await
        .expect("read record")
        .is_none());
        // The unexpired pin — same basis, different owner — still roots it.
        let survivor = crate::checkpoint::read_checkpoint_record(
            &store,
            &namespace_id,
            &lasting.checkpoint_id,
        )
        .await
        .expect("read lasting record")
        .expect("lasting record survives")
        .state;
        assert!(crate::checkpoint::load_namespace_manifest_envelope(
            &store,
            &namespace_id,
            &survivor.manifest_object_id,
        )
        .await
        .is_ok());
        stat_root(&store, &namespace_id).await;
    }

    /// Two owners of one basis hold two records: releasing one leaves the
    /// other rooting the shared basis.
    #[tokio::test]
    async fn gc_keeps_a_basis_pinned_by_another_owner_after_one_release() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        let first = crate::checkpoint::create_checkpoint(
            &store,
            &namespace_id,
            CheckpointOwner::User {
                name: "keeper".to_owned(),
            },
            None,
            &setup,
        )
        .await
        .expect("first owner");
        let second = crate::checkpoint::create_checkpoint(
            &store,
            &namespace_id,
            CheckpointOwner::User {
                name: "releaser".to_owned(),
            },
            None,
            &setup,
        )
        .await
        .expect("second owner");
        assert_ne!(first.checkpoint_id, second.checkpoint_id);
        assert_eq!(first.manifest_id, second.manifest_id);
        write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("advance past the shared basis");

        crate::checkpoint::release_checkpoint(&store, &namespace_id, &second.checkpoint_id, &setup)
            .await
            .expect("release one owner");
        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let first_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("first gc pass");
        assert_eq!(first_pass.deleted_checkpoint_records, 1);
        let second_pass = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("second gc pass");
        let _ = second_pass;
        assert!(
            crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
                .await
                .expect("read keeper record")
                .is_some(),
            "the surviving owner's record stays"
        );
        let keeper =
            crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
                .await
                .expect("read keeper record")
                .expect("keeper record exists")
                .state;
        assert!(
            crate::checkpoint::load_namespace_manifest_envelope(
                &store,
                &namespace_id,
                &keeper.manifest_object_id,
            )
            .await
            .is_ok(),
            "shared basis survives while any owner remains"
        );
    }

    /// Fork-owned records refuse the user release operation: their release
    /// is decided by garbage collection from the fork target's fate.
    #[tokio::test]
    async fn fork_owned_checkpoints_reject_user_release() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let source = NamespaceId::parse("source").expect("namespace id");
        let clone = NamespaceId::parse("clone").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &source, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
        fork_namespace(&store, &source, &clone, &setup)
            .await
            .expect("fork");

        let fork_record = read_fork_record(&store, &source).await;

        let error = crate::checkpoint::release_checkpoint(
            &store,
            &source,
            &fork_record.checkpoint_id,
            &setup,
        )
        .await
        .expect_err("fork-owned release must fail");
        assert!(
            matches!(
                &error,
                CoreError::InvalidCheckpointRequest(message)
                    if message.contains("owned by fork target")
            ),
            "expected invalid checkpoint request, got {error:?}"
        );
    }

    #[tokio::test]
    async fn gc_retains_active_checkpoint_bases() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        let first = create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("first checkpoint");
        write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("second checkpoint");

        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("gc pass");

        // Only the unpinned bootstrap manifest is collectable; both active
        // checkpoint bases stay.
        assert!(report.deleted_manifests <= 1);
        assert_eq!(report.deleted_checkpoint_records, 0);
        let first_record =
            crate::checkpoint::read_checkpoint_record(&store, &namespace_id, &first.checkpoint_id)
                .await
                .expect("read first checkpoint")
                .expect("first checkpoint exists")
                .state;
        assert!(crate::checkpoint::load_namespace_manifest_envelope(
            &store,
            &namespace_id,
            &first_record.manifest_object_id,
        )
        .await
        .is_ok());
    }

    /// Reads the single fork-owned record a fork left under the source.
    async fn read_fork_record(store: &LocalFsStore, source: &NamespaceId) -> CheckpointRecordState {
        for key in store
            .list_prefix(&checkpoint_prefix(source.as_str()))
            .await
            .expect("list checkpoints")
        {
            let bytes = store
                .get(&key, None)
                .await
                .expect("get record")
                .expect("record exists");
            let record = decode_control_object::<CheckpointRecordState>(
                &bytes,
                ControlObjectKind::CheckpointRecord,
            )
            .expect("decode record")
            .state;
            if matches!(record.owner, CheckpointOwner::Fork { .. }) {
                return record;
            }
        }
        unreachable!("fork leaves one fork-owned record");
    }

    /// The fork-record cascade: a live target keeps the record a root; a
    /// terminal target delete releases the record by compare-and-swap; the
    /// record reaps on the next pass and its basis on the pass after that.
    #[tokio::test]
    async fn gc_releases_fork_checkpoints_of_terminally_deleted_targets_across_passes() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let source = NamespaceId::parse("source").expect("namespace id");
        let clone = NamespaceId::parse("clone").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &source, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
        fork_namespace(&store, &source, &clone, &setup)
            .await
            .expect("fork");
        let fork_record = read_fork_record(&store, &source).await;
        // Advance the source root past the fork basis so the basis is
        // reachable only through the fork-owned record.
        write_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
        create_checkpoint(&store, &source, &setup)
            .await
            .expect("advance root past the fork basis");

        let before = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &source, &config(), &before)
            .await
            .expect("gc with live target");
        assert_eq!(report.released_fork_checkpoints, 0);

        delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
            .await
            .expect("terminal delete of the fork target");
        let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);

        // Pass one flips the record; the record still roots its basis.
        let first_pass = gc_namespace(&store, &source, &config(), &aged)
            .await
            .expect("first gc pass");
        assert_eq!(first_pass.released_fork_checkpoints, 1);
        assert_eq!(first_pass.deleted_checkpoint_records, 0);
        let flipped =
            crate::checkpoint::read_checkpoint_record(&store, &source, &fork_record.checkpoint_id)
                .await
                .expect("read record")
                .expect("record survives the pass that released it")
                .state;
        assert_eq!(flipped.state, CheckpointRecordLifecycle::Released);

        // The flip refreshed the record's timestamp; age it out again.
        let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
        let second_pass = gc_namespace(&store, &source, &config(), &aged)
            .await
            .expect("second gc pass");
        assert_eq!(second_pass.deleted_checkpoint_records, 1);
        assert!(
            crate::checkpoint::load_namespace_manifest_envelope(
                &store,
                &source,
                &fork_record.manifest_object_id,
            )
            .await
            .is_ok(),
            "basis survives the pass that deletes its record"
        );

        // Pass three reaps the unreferenced basis.
        let third_pass = gc_namespace(&store, &source, &config(), &aged)
            .await
            .expect("third gc pass");
        assert!(third_pass.deleted_manifests >= 1);
        assert!(crate::checkpoint::load_namespace_manifest_envelope(
            &store,
            &source,
            &fork_record.manifest_object_id,
        )
        .await
        .is_err());
        stat_root(&store, &source).await;
    }

    /// Rule 1 for fork records: a record younger than the grace window is
    /// never released, even when its target is already terminally deleted.
    #[tokio::test]
    async fn gc_retains_young_fork_checkpoints_of_terminally_deleted_targets() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let source = NamespaceId::parse("source").expect("namespace id");
        let clone = NamespaceId::parse("clone").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &source, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
        fork_namespace(&store, &source, &clone, &setup)
            .await
            .expect("fork");
        let fork_record = read_fork_record(&store, &source).await;
        delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
            .await
            .expect("terminal delete of the fork target");

        let young = context(now_after_newest_object(&store, &source, 0).await);
        let report = gc_namespace(&store, &source, &config(), &young)
            .await
            .expect("gc pass");

        assert_eq!(report.released_fork_checkpoints, 0);
        let record =
            crate::checkpoint::read_checkpoint_record(&store, &source, &fork_record.checkpoint_id)
                .await
                .expect("read record")
                .expect("young record survives")
                .state;
        assert_eq!(
            record.state,
            CheckpointRecordLifecycle::Active,
            "young fork record stays active even though it is releasable"
        );
        stat_root(&store, &source).await;
    }

    /// The abandoned-fork arm: once the target tree is completely gone and
    /// the record has aged past the reap window, the record is released —
    /// but never before that window, and never while any target object
    /// remains.
    #[tokio::test]
    async fn gc_releases_abandoned_fork_checkpoints_after_the_reap_window() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let source = NamespaceId::parse("source").expect("namespace id");
        let clone = NamespaceId::parse("clone").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &source, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
        fork_namespace(&store, &source, &clone, &setup)
            .await
            .expect("fork");
        let fork_record = read_fork_record(&store, &source).await;

        // Simulate rule 9 having reaped the abandoned target bootstrap.
        for key in store
            .list_prefix(&format!("namespaces/{}/", clone.as_str()))
            .await
            .expect("list target tree")
        {
            store.delete(&key).await.expect("reap target object");
        }

        // Aged past grace but inside the reap window: ambiguity retains.
        let inside_reap = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
        let early = gc_namespace(&store, &source, &config(), &inside_reap)
            .await
            .expect("gc inside the reap window");
        assert_eq!(early.released_fork_checkpoints, 0);

        // Past the reap window: the record is provably abandoned.
        let past_reap = context(now_after_newest_object(&store, &source, REAP_MS + 1).await);
        let report = gc_namespace(&store, &source, &config(), &past_reap)
            .await
            .expect("gc past the reap window");
        assert_eq!(report.released_fork_checkpoints, 1);
        let record =
            crate::checkpoint::read_checkpoint_record(&store, &source, &fork_record.checkpoint_id)
                .await
                .expect("read record")
                .expect("record survives the releasing pass")
                .state;
        assert_eq!(record.state, CheckpointRecordLifecycle::Released);
        stat_root(&store, &source).await;
    }

    /// A fork retry after abandonment revives the released record through
    /// the freshen compare-and-swap and completes the target.
    #[tokio::test]
    async fn abandoned_fork_retry_revives_and_freshens_the_source_record() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let source = NamespaceId::parse("source").expect("namespace id");
        let clone = NamespaceId::parse("clone").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &source, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
        fork_namespace(&store, &source, &clone, &setup)
            .await
            .expect("fork");
        let fork_record = read_fork_record(&store, &source).await;

        // Abandonment: the target tree is reaped and a GC pass has already
        // released the source record.
        for key in store
            .list_prefix(&format!("namespaces/{}/", clone.as_str()))
            .await
            .expect("list target tree")
        {
            store.delete(&key).await.expect("reap target object");
        }
        set_checkpoint_record_state(
            &store,
            &source,
            &fork_record.checkpoint_id,
            CheckpointRecordLifecycle::Released,
            &setup.writer_version,
        )
        .await
        .expect("release the fork record");

        fork_namespace(&store, &source, &clone, &setup)
            .await
            .expect("fork retry after abandonment");
        let revived =
            crate::checkpoint::read_checkpoint_record(&store, &source, &fork_record.checkpoint_id)
                .await
                .expect("read record")
                .expect("record revived")
                .state;
        assert_eq!(revived.state, CheckpointRecordLifecycle::Active);
        crate::path::read::load_metadata_view(
            &store,
            &clone,
            crate::path::read::ReadLoadContext::latest(),
        )
        .await
        .expect("target readable after retry")
        .resolve_path("/docs/one.txt")
        .await
        .expect("forked file readable");
    }

    /// Unreadable checkpoint records are ambiguous roots on their own,
    /// without any pin involved: the record is retained and the pass
    /// degrades.
    #[tokio::test]
    async fn gc_retains_unreadable_checkpoint_records_and_degrades() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("checkpoint");

        for key in store
            .list_prefix(&checkpoint_prefix(namespace_id.as_str()))
            .await
            .expect("list checkpoints")
        {
            store
                .put_overwrite(&key, bytes::Bytes::from_static(b"not json"))
                .await
                .expect("corrupt record");
        }

        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("gc pass");
        assert!(report.degraded_retention);
        assert_eq!(report.deleted_checkpoint_records, 0);
        assert_eq!(report.deleted_manifests, 0);
        assert_eq!(report.deleted_metadata_tables, 0);
        assert!(
            !store
                .list_prefix(&checkpoint_prefix(namespace_id.as_str()))
                .await
                .expect("list checkpoints")
                .is_empty(),
            "unreadable record retained"
        );
    }

    /// Rule 1's timestamp arm: an object without a provider timestamp reads
    /// as young, so a store that reports none never deletes anything.
    #[tokio::test]
    async fn gc_retains_everything_without_provider_timestamps() {
        let temp_dir = tempdir().expect("tempdir");
        let store = TimestamplessStore(LocalFsStore::new(temp_dir.path()).expect("store"));
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("checkpoint");
        advance_retention_floor(&store, &namespace_id, &setup)
            .await
            .expect("advance floor");

        // Far past any window by wall clock, but no object carries a
        // provider timestamp.
        let aged = context(now_after_newest_object(&store.0, &namespace_id, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("gc pass");

        assert_eq!(report.deleted_wal_segments, 0);
        assert_eq!(report.deleted_metadata_tables, 0);
        assert_eq!(report.deleted_manifests, 0);
        assert_eq!(report.deleted_checkpoint_records, 0);
        assert_eq!(report.released_fork_checkpoints, 0);
        assert!(report.retained_candidates > 0);
        stat_root(&store, &namespace_id).await;
    }

    /// The chunked delete-time re-verification path (rule 3) must reach the
    /// same outcomes as a whole-batch sweep; chunk size one re-collects the
    /// live set before every candidate.
    #[tokio::test]
    async fn gc_sweep_reverification_chunks_preserve_outcomes() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "gc-one", &setup).await;
        let first = create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("first checkpoint");
        write_file(&store, &namespace_id, "/docs/two.txt", "gc-two", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("second checkpoint");
        set_checkpoint_record_state(
            &store,
            &namespace_id,
            &first.checkpoint_id,
            loonfs_api::wire::control::CheckpointRecordLifecycle::Released,
            &setup.writer_version,
        )
        .await
        .expect("mark first dead");

        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let first_pass =
            gc_namespace_with_reverify_chunk(&store, &namespace_id, &config(), &aged, 1)
                .await
                .expect("first gc pass");
        assert_eq!(first_pass.deleted_checkpoint_records, 1);
        assert!(!first_pass.degraded_retention);

        let second_pass =
            gc_namespace_with_reverify_chunk(&store, &namespace_id, &config(), &aged, 1)
                .await
                .expect("second gc pass");
        assert!(second_pass.deleted_manifests >= 1);
        stat_root(&store, &namespace_id).await;
    }

    #[tokio::test]
    async fn gc_reaps_an_abandoned_bootstrap_tree_after_the_reap_window() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("orphan").expect("namespace id");
        // A partial tree: a head object but no namespace.json completion
        // marker.
        store
            .put_if_absent(
                &format!("namespaces/{}/wal/head.json", namespace_id.as_str()),
                bytes::Bytes::from_static(b"{}"),
            )
            .await
            .expect("write partial head");

        let young = context(now_after_newest_object(&store, &namespace_id, GRACE_MS).await);
        let retained = gc_namespace(&store, &namespace_id, &config(), &young)
            .await
            .expect("gc young tree");
        assert_eq!(retained.reaped_abandoned_objects, 0);
        assert!(retained.retained_candidates > 0);

        let aged = context(now_after_newest_object(&store, &namespace_id, REAP_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("gc aged tree");
        assert_eq!(report.reaped_abandoned_objects, 1);
        assert!(store
            .list_prefix(&format!("namespaces/{}/", namespace_id.as_str()))
            .await
            .expect("list")
            .is_empty());
    }

    #[tokio::test]
    async fn gc_degrades_to_retention_when_a_pin_checkpoint_is_unreadable() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let source = NamespaceId::parse("source").expect("namespace id");
        let clone = NamespaceId::parse("clone").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &source, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &source, "/docs/one.txt", "gc-one", &setup).await;
        fork_namespace(&store, &source, &clone, &setup)
            .await
            .expect("fork");

        // Corrupt the pinned checkpoint: ambiguous roots must retain.
        for key in store
            .list_prefix(&loonfs_objectstore::keys::checkpoint_prefix(
                source.as_str(),
            ))
            .await
            .expect("list checkpoints")
        {
            store
                .put_overwrite(&key, bytes::Bytes::from_static(b"not json"))
                .await
                .expect("corrupt record");
        }

        let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &source, &config(), &aged)
            .await
            .expect("gc pass");
        assert!(report.degraded_retention);
        assert_eq!(report.deleted_manifests, 0);
        assert_eq!(report.deleted_metadata_tables, 0);
    }

    async fn drain_grams_index(
        store: &LocalFsStore,
        namespace_id: &NamespaceId,
        context: &MutationContext,
    ) {
        crate::checkpoint::enable_grams_index(store, namespace_id, context)
            .await
            .expect("enable grams index");
        loop {
            let report = crate::checkpoint::build_grams_index_step(
                store,
                None,
                namespace_id,
                context,
                crate::GramIndexBuildPolicy::default(),
            )
            .await
            .expect("build grams index step");
            if matches!(
                report.outcome,
                crate::GramIndexBuildOutcome::UpToDate { .. }
            ) {
                return;
            }
            assert!(
                matches!(
                    report.outcome,
                    crate::GramIndexBuildOutcome::Published { .. }
                ),
                "unexpected build outcome: {:?}",
                report.outcome
            );
        }
    }

    async fn index_segment_keys(store: &LocalFsStore, namespace_id: &NamespaceId) -> Vec<String> {
        list_prefix(store, &index_segment_prefix(namespace_id.as_str()))
            .await
            .expect("list index segments")
    }

    #[tokio::test]
    async fn gc_retains_live_index_segments() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "searchable", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("checkpoint");
        drain_grams_index(&store, &namespace_id, &setup).await;
        let segments = index_segment_keys(&store, &namespace_id).await;
        assert!(!segments.is_empty());

        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("gc pass");

        assert_eq!(report.deleted_index_segments, 0);
        assert_eq!(
            index_segment_keys(&store, &namespace_id).await,
            segments,
            "live index segments must survive the sweep"
        );
        assert!(!report.degraded_retention);
    }

    #[tokio::test]
    async fn gc_reclaims_index_segments_after_disable() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let setup = context(1_000);
        bootstrap_namespace(&store, &namespace_id, &setup, false)
            .await
            .expect("bootstrap");
        write_file(&store, &namespace_id, "/docs/one.txt", "searchable", &setup).await;
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("checkpoint");
        drain_grams_index(&store, &namespace_id, &setup).await;
        let segments = index_segment_keys(&store, &namespace_id).await;
        assert!(!segments.is_empty());

        let disabled = crate::checkpoint::disable_grams_index(&store, &namespace_id, &setup)
            .await
            .expect("disable");
        assert_eq!(disabled, crate::GramIndexDisableOutcome::Disabled);

        // Superseded manifests still reference the segments until they age
        // out; sweeping data and manifests in one aged pass clears both.
        let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
        let first = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("first gc pass");
        let second = gc_namespace(&store, &namespace_id, &config(), &aged)
            .await
            .expect("second gc pass");

        assert_eq!(
            first.deleted_index_segments + second.deleted_index_segments,
            segments.len() as u64,
            "disable makes every index segment collectable"
        );
        assert!(index_segment_keys(&store, &namespace_id).await.is_empty());
    }
}
