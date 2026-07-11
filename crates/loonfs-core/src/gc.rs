//! v1 listing mark-and-sweep garbage collection (format spec, "Garbage
//! collection").
//!
//! GC and floor advancement are the only consumers of listing. Nothing
//! sweeps by default: callers opt in through the admin endpoint or an
//! explicit maintenance-tick option. The safety rules close the two races
//! the un-serialized layout admits — create-vs-collect and
//! publish-in-flight — via the grace window, delete-time re-verification,
//! and retention-wins defaults. When in doubt, this module retains.

use crate::checkpoint::{load_namespace_manifest_envelope, read_checkpoint_record};
use crate::context::MutationContext;
use crate::error::{CoreError, MetadataProjectionLoadError};
use crate::namespace::control::{
    read_head_object, read_metadata_root_object, read_wal_floor_object, ControlObjectLoadError,
};
use crate::wal::{load_validated_wal_chain, WalChainLoadRequest};
use loonfs_api::wire::control::{
    decode_control_object, CheckpointRecordLifecycle, CheckpointRecordState, ControlObjectKind,
    NamespaceGcPinState, NamespaceState,
};
use loonfs_api::{ChangeSeq, ManifestObjectId, NamespaceId};
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_prefix, metadata_table_prefix, namespace_config,
    pin_prefix, wal_segment_prefix,
};
use loonfs_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Grace and reap windows for the sweep (format spec, "Garbage collection",
/// rules 1 and 9). Both are wall-clock cleanup policy, never validity
/// inputs. The defaults are deliberately conservative: one hour of
/// unconditional protection for every object, seven days before an
/// abandoned bootstrap tree may be reaped.
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
        if self.grace_window_ms == 0 {
            return Err(CoreError::Internal(
                "gc grace_window_ms must be greater than zero".to_owned(),
            ));
        }
        if self.reap_window_ms < self.grace_window_ms {
            return Err(CoreError::Internal(
                "gc reap_window_ms must be at least the grace window".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcReport {
    pub deleted_wal_segments: u64,
    pub deleted_metadata_tables: u64,
    pub deleted_manifests: u64,
    pub deleted_checkpoint_records: u64,
    pub released_pins: u64,
    /// Objects removed while reaping an abandoned bootstrap tree (rule 9).
    pub reaped_abandoned_objects: u64,
    /// Candidates dropped at delete time: still inside the grace window,
    /// missing a provider timestamp, or reachable from the fresh root set.
    pub retained_candidates: u64,
    /// True when a pin's checkpoint could not be read or validated, which
    /// suppresses manifest and table deletion for the whole pass (rule 5:
    /// ambiguous roots cause retention).
    pub degraded_retention: bool,
}

/// Everything reachable from the fresh root set (rule 4).
struct LiveSet {
    manifests: BTreeSet<ManifestObjectId>,
    tables: BTreeSet<String>,
    wal_segments: BTreeSet<String>,
    checkpoint_keys: BTreeSet<String>,
    pin_keys: BTreeSet<String>,
    /// Pin resolution failed somewhere: manifest/table deletion must not
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

/// How many sweep candidates one delete-time live set may decide before the
/// set must be re-collected (rule 3: candidate selection may be stale,
/// deletion may not). The bound keeps "immediately before deleting" honest
/// on large sweeps, where deciding the whole batch against one snapshot
/// would leave the last deletions arbitrarily far behind it.
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

    // Mark: candidate selection may be arbitrarily stale (rule 3), so one
    // live-set pass selects candidates...
    let mark = collect_live_set(store, namespace_id, context.now_ms).await?;
    let mut report = GcReport::default();

    let candidate_segments = list_prefix(store, &wal_segment_prefix(namespace_id.as_str())).await?;
    let candidate_tables =
        list_prefix(store, &metadata_table_prefix(namespace_id.as_str())).await?;
    let candidate_manifests =
        list_prefix(store, &metadata_manifest_prefix(namespace_id.as_str())).await?;
    let candidate_checkpoints =
        list_prefix(store, &checkpoint_prefix(namespace_id.as_str())).await?;
    let candidate_pins = list_prefix(store, &pin_prefix(namespace_id.as_str())).await?;

    let segment_candidates: Vec<String> = candidate_segments
        .into_iter()
        .filter(|key| !mark.wal_segments.contains(key))
        .collect();
    let table_candidates: Vec<String> = candidate_tables
        .into_iter()
        .filter(|key| !mark.tables.contains(key))
        .collect();
    let manifest_candidates: Vec<String> = candidate_manifests
        .into_iter()
        .filter(|key| manifest_object_id_of(key).is_none_or(|id| !mark.manifests.contains(&id)))
        .collect();
    let checkpoint_candidates: Vec<String> = candidate_checkpoints
        .into_iter()
        .filter(|key| !mark.checkpoint_keys.contains(key))
        .collect();
    let pin_candidates: Vec<String> = candidate_pins
        .into_iter()
        .filter(|key| !mark.pin_keys.contains(key))
        .collect();

    // ...and fresh passes immediately before deletion decide (rule 3:
    // candidate selection may be stale; deletion may not). The live set is
    // re-collected every `reverify_chunk` candidates so a large sweep never
    // runs far ahead of its verification snapshot.
    let mut sweep =
        SweepVerifier::collect(store, namespace_id, context.now_ms, reverify_chunk).await?;

    // Data first, records last: every readable checkpoint or pin record
    // roots its basis in the live set, so data referenced by a surviving
    // record is never deleted in the pass that could delete the record. A
    // crash mid-sweep therefore leaves orphaned data for the next pass,
    // never a record whose data vanished under it.
    for key in segment_candidates {
        sweep
            .refresh_if_due(store, namespace_id, context.now_ms)
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
            .refresh_if_due(store, namespace_id, context.now_ms)
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
    for key in manifest_candidates {
        sweep
            .refresh_if_due(store, namespace_id, context.now_ms)
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
            .refresh_if_due(store, namespace_id, context.now_ms)
            .await?;
        if sweep.live.checkpoint_keys.contains(&key) {
            report.retained_candidates += 1;
            continue;
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.deleted_checkpoint_records += 1;
        }
    }
    for key in pin_candidates {
        sweep
            .refresh_if_due(store, namespace_id, context.now_ms)
            .await?;
        if sweep.live.pin_keys.contains(&key) {
            report.retained_candidates += 1;
            continue;
        }
        if delete_if_aged(store, &key, config.grace_window_ms, context, &mut report).await? {
            report.released_pins += 1;
        }
    }
    report.degraded_retention = sweep.degraded;

    Ok(report)
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
        now_ms: u64,
        reverify_chunk: usize,
    ) -> Result<Self, CoreError> {
        let live = collect_live_set(store, namespace_id, now_ms).await?;
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
        now_ms: u64,
    ) -> Result<(), CoreError> {
        if self.decided_since_collect >= self.reverify_chunk {
            self.live = collect_live_set(store, namespace_id, now_ms).await?;
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
    now_ms: u64,
) -> Result<LiveSet, CoreError> {
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
        wal_segments: BTreeSet::new(),
        checkpoint_keys: BTreeSet::new(),
        pin_keys: BTreeSet::new(),
        degraded: false,
    };
    live.manifests.insert(root.manifest_object_id);

    // Records last: every readable checkpoint record roots its basis while
    // the record exists, regardless of lifecycle or expiry, so a record can
    // never outlive its manifest. This covers the young dead record whose
    // deletion the grace window refuses, and the dead-record revival in
    // `create_checkpoint` (deterministic ids), which must never flip a
    // record Active after its basis was swept. Lifecycle and expiry decide
    // only whether the record itself may be deleted; a dead or expired
    // record's basis becomes collectable on the pass after the record is
    // gone.
    for key in list_prefix(store, &checkpoint_prefix(namespace_id.as_str())).await? {
        let Some(bytes) = store
            .get(&key, None)
            .await
            .map_err(|error| CoreError::store(&key, &error))?
        else {
            continue;
        };
        match decode_control_object::<CheckpointRecordState>(
            &bytes,
            ControlObjectKind::CheckpointRecord,
        ) {
            Ok(envelope) => {
                let record = envelope.state;
                live.manifests.insert(record.manifest_object_id);
                let expired = record
                    .expires_at_ms
                    .is_some_and(|expires_at_ms| expires_at_ms <= now_ms);
                if record.state == CheckpointRecordLifecycle::Active && !expired {
                    live.checkpoint_keys.insert(key);
                }
            }
            // Unreadable records are ambiguous roots: retain them and keep
            // sweeping conservative for manifests/tables.
            Err(_) => {
                live.checkpoint_keys.insert(key);
                live.degraded = true;
            }
        }
    }

    for key in list_prefix(store, &pin_prefix(namespace_id.as_str())).await? {
        let Some(bytes) = store
            .get(&key, None)
            .await
            .map_err(|error| CoreError::store(&key, &error))?
        else {
            continue;
        };
        let Ok(envelope) = decode_control_object::<NamespaceGcPinState>(
            &bytes,
            ControlObjectKind::NamespaceGcPinState,
        ) else {
            live.pin_keys.insert(key);
            live.degraded = true;
            continue;
        };
        let pin = envelope.state;
        // A pin whose target is verifiably terminally deleted is releasable,
        // but only the pin record itself: while the record exists it keeps
        // rooting its checkpoint chain (records last), and the chain becomes
        // collectable on the pass after the pin is gone.
        if !pin_target_terminally_deleted(store, &pin.target_namespace_id).await? {
            live.pin_keys.insert(key);
        }
        match read_checkpoint_record(store, namespace_id, &pin.source_checkpoint_id).await {
            Ok(Some(record)) => {
                // Pinned checkpoints protect their basis regardless of the
                // record's lifecycle: the pin is the reachability fact.
                live.manifests.insert(record.state.manifest_object_id);
                live.checkpoint_keys
                    .insert(loonfs_objectstore::keys::checkpoint_record(
                        namespace_id.as_str(),
                        pin.source_checkpoint_id.as_str(),
                    ));
            }
            // "If GC cannot read or validate a pin's checkpoint, it
            // retains": suppress manifest/table deletion for the pass.
            Ok(None) | Err(_) => {
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
            }
            Err(_) => {
                live.degraded = true;
            }
        }
    }

    // WAL needed to replay from the floor through the head stays (rule 7 is
    // implied: floor <= root basis, so root-to-head replay is covered).
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

async fn pin_target_terminally_deleted<S: ObjectStore + ?Sized>(
    store: &S,
    target_namespace_id: &NamespaceId,
) -> Result<bool, CoreError> {
    match read_head_object(store, target_namespace_id).await {
        Ok(loaded) => Ok(loaded.envelope.state.state == NamespaceState::Deleted),
        // A target that never completed bootstrap (or whose head is
        // unreadable) is NOT verifiably deleted; rule 9 handles abandoned
        // targets by age, and ambiguity retains.
        Err(_) => Ok(false),
    }
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
    use crate::checkpoint::record::set_checkpoint_record_state;
    use crate::checkpoint::{advance_retention_floor, create_checkpoint};
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
    use loonfs_api::{CommitId, PutBehavior};
    use loonfs_objectstore::fs::LocalFsStore;
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
                        absolute_path: path.to_owned(),
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

    /// The reviewer scenario for rule 1 and "records last": a dead record
    /// referencing an aged basis loses the record first, and the basis only
    /// on the following pass — never the other way around.
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
            loonfs_api::wire::control::CheckpointRecordLifecycle::Dead,
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

    #[tokio::test]
    async fn gc_releases_pins_of_terminally_deleted_targets() {
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

        let before = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &source, &config(), &before)
            .await
            .expect("gc with live target");
        assert_eq!(report.released_pins, 0);

        delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
            .await
            .expect("terminal delete of the fork target");
        let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &source, &config(), &aged)
            .await
            .expect("gc after target delete");
        assert_eq!(report.released_pins, 1);
        stat_root(&store, &source).await;
    }

    /// Rule 1 for pins: a pin younger than the grace window stays a root
    /// even when its target namespace is already terminally deleted.
    #[tokio::test]
    async fn gc_retains_young_pins_of_terminally_deleted_targets() {
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
        delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
            .await
            .expect("terminal delete of the fork target");

        let young = context(now_after_newest_object(&store, &source, 0).await);
        let report = gc_namespace(&store, &source, &config(), &young)
            .await
            .expect("gc pass");

        assert_eq!(report.released_pins, 0);
        assert!(report.retained_candidates > 0);
        assert!(
            !store
                .list_prefix(&pin_prefix(source.as_str()))
                .await
                .expect("list pins")
                .is_empty(),
            "young pin survives even though it is releasable"
        );
        stat_root(&store, &source).await;
    }

    /// A released pin follows the same records-last cascade as checkpoint
    /// records: the pin is deleted first, the checkpoint record it rooted on
    /// the next pass, and the checkpoint's basis on the pass after that.
    #[tokio::test]
    async fn gc_releases_pins_before_their_checkpoint_chain_across_passes() {
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
        // Advance the source root past the pinned basis so the basis is
        // reachable only through the pin -> record chain.
        write_file(&store, &source, "/docs/two.txt", "gc-two", &setup).await;
        create_checkpoint(&store, &source, &setup)
            .await
            .expect("advance root past the pinned basis");

        let pin_keys = store
            .list_prefix(&pin_prefix(source.as_str()))
            .await
            .expect("list pins");
        assert_eq!(pin_keys.len(), 1, "fork leaves one pin");
        let pin_bytes = store
            .get(&pin_keys[0], None)
            .await
            .expect("get pin")
            .expect("pin exists");
        let pin = decode_control_object::<NamespaceGcPinState>(
            &pin_bytes,
            ControlObjectKind::NamespaceGcPinState,
        )
        .expect("decode pin")
        .state;
        let pinned_record =
            crate::checkpoint::read_checkpoint_record(&store, &source, &pin.source_checkpoint_id)
                .await
                .expect("read pinned record")
                .expect("pinned record exists")
                .state;

        delete_namespace(&store, &clone, DeleteNamespaceOptions::default(), &setup)
            .await
            .expect("terminal delete of the fork target");
        set_checkpoint_record_state(
            &store,
            &source,
            &pin.source_checkpoint_id,
            loonfs_api::wire::control::CheckpointRecordLifecycle::Dead,
            &setup.writer_version,
        )
        .await
        .expect("mark pinned checkpoint dead");
        let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);

        // Pass one releases the pin; the pin still rooted its chain.
        let first_pass = gc_namespace(&store, &source, &config(), &aged)
            .await
            .expect("first gc pass");
        assert_eq!(first_pass.released_pins, 1);
        assert_eq!(first_pass.deleted_checkpoint_records, 0);
        assert!(
            crate::checkpoint::read_checkpoint_record(&store, &source, &pin.source_checkpoint_id)
                .await
                .expect("read record")
                .is_some(),
            "pinned checkpoint record survives its pin"
        );

        // Pass two reaps the now-unpinned dead record; its basis stands.
        let second_pass = gc_namespace(&store, &source, &config(), &aged)
            .await
            .expect("second gc pass");
        assert_eq!(second_pass.deleted_checkpoint_records, 1);
        assert!(
            crate::checkpoint::load_namespace_manifest_envelope(
                &store,
                &source,
                &pinned_record.manifest_object_id,
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
            &pinned_record.manifest_object_id,
        )
        .await
        .is_err());
        stat_root(&store, &source).await;
    }

    /// Rule 5's missing-root arm: a pin whose checkpoint record is gone (not
    /// merely unreadable) is ambiguous and suppresses manifest and table
    /// deletion for the pass.
    #[tokio::test]
    async fn gc_degrades_to_retention_when_a_pin_checkpoint_is_missing() {
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

        for key in store
            .list_prefix(&checkpoint_prefix(source.as_str()))
            .await
            .expect("list checkpoints")
        {
            store.delete(&key).await.expect("delete pinned record");
        }

        let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
        let report = gc_namespace(&store, &source, &config(), &aged)
            .await
            .expect("gc pass");
        assert!(report.degraded_retention);
        assert_eq!(report.deleted_manifests, 0);
        assert_eq!(report.deleted_metadata_tables, 0);
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
        assert_eq!(report.released_pins, 0);
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
            loonfs_api::wire::control::CheckpointRecordLifecycle::Dead,
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
}
