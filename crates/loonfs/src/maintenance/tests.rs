//! The compaction loop, `Maintenance::compact_metadata`, and the store
//! requests of other maintenance calls, over a live runtime.

use crate::metrics::DefaultMetricsRecorder;
use crate::test_support::{counter, maintenance_past_the_grace_window, writer};
use crate::{
    LoonFs, Maintenance, MetadataCompactionOutcome, NamespaceId, SharedObjectStore, Writable,
};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    Intercept, InterceptStore, Interceptor, OperationContext, OperationSelector,
};
use loonfs_types::format::manifest::{
    decode_namespace_manifest_json, MetadataRowFamily, NamespaceManifestPayload, RunTier,
};
use loonfs_types::RunNo;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tempfile::tempdir;

/// A writer and a maintenance of a second runtime over one store, the
/// maintenance with a metrics recorder of its own.
async fn manual_deployment(
    root: &std::path::Path,
) -> (LoonFs<Writable>, Maintenance, Arc<DefaultMetricsRecorder>) {
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(root).expect("create local-fs store"));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = LoonFs::builder_with_store(Arc::clone(&store))
        .writer_id(loonfs_test_support::ids::writer_id("manual-writer"))
        .build()
        .await
        .expect("build the writer");
    let standalone = LoonFs::builder_with_store(Arc::clone(&store))
        .writer_id(loonfs_test_support::ids::writer_id(
            "standalone-maintenance",
        ))
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("build the standalone maintenance")
        .maintenance(loonfs_test_support::ids::writer_id(
            "standalone-maintenance",
        ));
    (writer, standalone, recorder)
}

#[tokio::test]
async fn a_maintenance_gc_step_records_the_collection_counters_once() {
    let temp_dir = tempdir().expect("tempdir");
    let (writer, maintenance, recorder) = manual_deployment(temp_dir.path()).await;
    let namespace = namespace_id("maintenance-gc-metrics");
    let pool_id = writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace")
        .pool_id;
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    namespace_writer
        .put_file("/live.txt", b"live", &loonfs_test_support::test_actor())
        .await
        .expect("write a live GC candidate");

    assert_eq!(counter(&recorder.snapshot(), "loonfs.gc.retained", &[]), 0);
    let gc = maintenance
        .gc(&namespace)
        .await
        .expect("run the maintenance GC step");
    assert!(
        gc.retained.total() > 0,
        "the live namespace leaves the collection candidates to retain"
    );

    let snapshot = recorder.snapshot();
    assert_eq!(
        counter(&snapshot, "loonfs.gc.retained", &[]),
        gc.retained.total(),
        "the collection records its retained count exactly once"
    );

    let pool_gc = maintenance
        .gc_pool(&pool_id)
        .await
        .expect("run the pool's collection");
    assert_eq!(pool_gc.pool_id, pool_id);
    let snapshot = recorder.snapshot();
    assert_eq!(
        counter(&snapshot, "loonfs.gc.retained", &[]),
        gc.retained.total() + pool_gc.retained.total(),
        "the pool's collection records its retained count exactly once"
    );
    let missing = maintenance
        .gc_pool(&crate::PoolId::generate())
        .await
        .expect_err("a pool without a record");
    assert_eq!(missing.code(), crate::ErrorCode::PoolNotFound);
}

/// Writes one file and folds the tail, so each call leaves one more delta run.
async fn write_and_fold(
    writer: &LoonFs<Writable>,
    maintenance: &Maintenance,
    namespace_id: &NamespaceId,
    path: &str,
) {
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    namespace
        .put_file(path, path.as_bytes(), &loonfs_test_support::test_actor())
        .await
        .expect("put a file");
    maintenance
        .fold_wal(namespace_id)
        .await
        .expect("fold the tail");
}

async fn current_manifest_payload<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> NamespaceManifestPayload {
    let root = loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("read the metadata root");
    let key = metadata_manifest_object(namespace_id, &root.manifest().manifest_no);
    let bytes = store
        .get(&key, None)
        .await
        .expect("read the manifest")
        .expect("the manifest exists");
    decode_namespace_manifest_json(&bytes)
        .expect("decode the manifest")
        .into_payload()
}

/// Writes 320 revisions of fifteen large attributes of `/file` and folds
/// them, so the attributes group holds one base run of more than 8 MiB.
/// Returns the attribute keys.
async fn write_a_large_attribute_base(
    writer: &LoonFs<Writable>,
    maintenance: &Maintenance,
    namespace_id: &NamespaceId,
) -> Vec<loonfs_types::AttributeKey> {
    use crate::publish::{parse_mutation_path, CommitCandidate, CommitIntent, FilesystemOperation};
    use crate::CommitId;
    use loonfs_test_support::ids::{attribute_key, attribute_text};

    let namespace_writer = writer.open_namespace(namespace_id).expect("open namespace");
    let keys: Vec<_> = (0..15)
        .map(|index| attribute_key(&format!("key-{index}")))
        .collect();
    namespace_writer
        .put_file("/file", b"content", &loonfs_test_support::test_actor())
        .await
        .expect("file");
    // The base's WAL bytes pass the fold trigger, so the session's own fold
    // waits for a fold permit until every batch is published, and the whole
    // tail folds as one run.
    let mut permits = Vec::new();
    for _ in 0..crate::DEFAULT_MAX_CONCURRENT_FOLDS.get() {
        permits.push(writer.execution_budget().fold_permit().await);
    }
    for batch in 0..10 {
        let operations = (batch * 32..(batch + 1) * 32)
            .map(|revision| FilesystemOperation::UpdateAttributes {
                path: parse_mutation_path("/file").expect("file path"),
                set: keys
                    .iter()
                    .enumerate()
                    .map(|(index, key)| {
                        let value: String = (0..64)
                            .map(|part| {
                                loonfs_types::sha256_hex(
                                    format!("{revision}/{index}/{part}").as_bytes(),
                                )
                            })
                            .collect();
                        (key.clone(), attribute_text(&value))
                    })
                    .collect(),
                remove: Vec::new(),
                expected_inode_id: None,
                expected_attributes_revision_no: None,
            })
            .collect();
        namespace_writer
            .commit_candidate(CommitCandidate::new(CommitIntent {
                commit_id: CommitId::generate(),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                preconditions: Vec::new(),
                operations,
            }))
            .await
            .expect("write the base attribute history");
    }
    drop(permits);
    maintenance.fold_wal(namespace_id).await.expect("fold base");
    keys
}

/// The run numbers and segment keys of the base runs that hold
/// attributes segments.
fn attribute_base(manifest: &NamespaceManifestPayload) -> (Vec<RunNo>, BTreeSet<String>) {
    let base_runs = manifest
        .runs
        .iter()
        .filter(|run| run.tier == RunTier::Base)
        .filter(|run| {
            run.segments
                .iter()
                .any(|segment| segment.family == MetadataRowFamily::Attributes)
        });
    let run_nos = base_runs.clone().map(|run| run.run_no).collect();
    let keys = base_runs
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == MetadataRowFamily::Attributes)
        .map(|segment| {
            loonfs_objectstore::keys::metadata_segment(
                &segment.owner_namespace_id,
                &segment.segment_id,
            )
        })
        .collect();
    (run_nos, keys)
}

#[tokio::test]
async fn the_compaction_loop_compacts_a_tier_of_deltas_without_reading_a_larger_base() {
    use crate::AttributeChanges;
    use loonfs_test_support::ids::{attribute_key, attribute_text};
    use loonfs_test_support::stores::RecordingStore;

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::metadata_segments(
        LocalFsStore::new(directory.path()).expect("store"),
    ));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    let namespace = namespace_id("tier-above-a-large-base");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    write_a_large_attribute_base(&writer, &maintenance, &namespace).await;
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    for revision in 0..6 {
        let mut changes = AttributeChanges::default();
        changes.set.insert(
            attribute_key("delta"),
            attribute_text(&revision.to_string()),
        );
        namespace_writer
            .update_attributes("/file", &loonfs_test_support::test_actor(), changes)
            .await
            .expect("write delta");
        maintenance.fold_wal(&namespace).await.expect("fold delta");
    }
    let attribute_runs = |manifest: &NamespaceManifestPayload| {
        manifest
            .runs
            .iter()
            .filter(|run| {
                run.segments
                    .iter()
                    .any(|segment| segment.family == MetadataRowFamily::Attributes)
            })
            .count()
    };
    let before = current_manifest_payload(store.as_ref(), &namespace).await;
    assert_eq!(attribute_runs(&before), 7);
    let (base_runs, base_keys) = attribute_base(&before);
    assert_eq!(base_runs.len(), 1, "the attributes group has one base run");

    store.reset();
    let report = maintenance
        .maintain_metadata_while_due(&namespace, &crate::MaintenanceCancellation::new())
        .await
        .expect("run the compaction loop");
    assert!(
        report.response.compaction_units > 0,
        "the tier of delta runs is due, got {report:?}"
    );
    assert!(
        !store
            .take_get_keys()
            .iter()
            .any(|key| base_keys.contains(key)),
        "the loop reads no segment of the attributes base run"
    );
    let after = current_manifest_payload(store.as_ref(), &namespace).await;
    assert_eq!(
        attribute_runs(&after),
        2,
        "one delta run replaces the six above the base"
    );
    assert_eq!(
        attribute_base(&after),
        (base_runs, base_keys),
        "the attributes base run keeps its run number and its segments"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn maintenance_clones_share_one_claim_and_never_reclaim_after_fencing() {
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
    let directory = tempdir().expect("tempdir");
    let namespace = namespace_id("shared-claim");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::metadata_manifest_prefix(
            &namespace,
        )),
    ));
    let shared: SharedObjectStore = store.clone();
    let writer = LoonFs::builder_with_store(shared.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let maintenance_runtime = LoonFs::builder_with_store(shared.clone())
        .writer_id(loonfs_test_support::ids::writer_id("maintenance"))
        .build()
        .await
        .expect("maintenance");
    let maintenance =
        maintenance_runtime.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    maintenance_runtime
        .open_namespace(&namespace)
        .expect("open namespace")
        .create_snapshot_with_options(&crate::CreateSnapshotOptions {
            name: Some("basis".to_owned()),
            ..crate::CreateSnapshotOptions::default()
        })
        .await
        .expect("snapshot");
    let before = current_manifest_payload(store.as_ref(), &namespace).await;
    store.reset();
    assert_eq!(
        maintenance
            .compact_once(
                &namespace,
                None,
                &loonfs_core::MetadataCompactionCancellation::default(),
            )
            .await
            .expect("no work"),
        MetadataCompactionOutcome::NotNeeded
    );
    assert_eq!(store.counts().puts, 0);
    let cloned = maintenance.clone();
    let (first, second) = tokio::join!(
        maintenance.compactor_epoch(&namespace),
        cloned.compactor_epoch(&namespace)
    );
    let epoch = first.expect("first claim");
    assert_eq!(second.expect("shared claim"), epoch);
    assert_eq!(store.counts().create_if_absent_puts, 1);
    let mut expected = before.clone();
    expected.manifest_no = before.manifest_no.successor().expect("next manifest");
    expected.compactor_epoch = loonfs_types::CompactorEpoch(before.compactor_epoch.0 + 1);
    assert_eq!(
        current_manifest_payload(store.as_ref(), &namespace).await,
        expected
    );
    let other = LoonFs::builder_with_store(shared)
        .writer_id(loonfs_test_support::ids::writer_id("other"))
        .build()
        .await
        .expect("other process")
        .maintenance(loonfs_test_support::ids::writer_id("other"));
    assert_eq!(
        other.compactor_epoch(&namespace).await.expect("new claim"),
        loonfs_types::CompactorEpoch(epoch.0 + 1)
    );
    store.reset();
    assert_eq!(
        maintenance
            .compactor_epoch(&namespace)
            .await
            .expect("remembered claim"),
        epoch
    );
    assert_eq!(
        maintenance
            .compact_once(
                &namespace,
                None,
                &loonfs_core::MetadataCompactionCancellation::default(),
            )
            .await
            .expect("fenced compaction"),
        MetadataCompactionOutcome::Fenced
    );
    assert_eq!(store.counts().puts, 0);
}

/// Lets a rival write one commit and fold it before each manifest write it
/// sees while it has races left, so the compaction unit that reaches its
/// publication loses that race.
#[derive(Debug)]
struct RivalFold {
    rival: LocalFsStore,
    namespace_id: NamespaceId,
    races_left: std::sync::atomic::AtomicUsize,
    races_run: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl Interceptor for RivalFold {
    type Guard = ();

    async fn before(&self, context: &OperationContext<'_>) -> (Intercept, ()) {
        use std::sync::atomic::Ordering::SeqCst;
        if OperationSelector::Put.matches(context.kind())
            && loonfs_objectstore::layout::manifest_no_of(context.key()).is_some()
            && self
                .races_left
                .try_update(SeqCst, SeqCst, |left| left.checked_sub(1))
                .is_ok()
        {
            self.races_run.fetch_add(1, SeqCst);
            loonfs_core::test_support::NamespaceFixture::new(
                &self.rival,
                &self.namespace_id,
                &loonfs_core::test_support::WriterAt {
                    writer_id: loonfs_test_support::ids::writer_id("rival"),
                    now_ms: 1_000,
                },
            )
            .append_wal_objects(1)
            .await
            .expect("the rival writes");
            loonfs_core::test_support::namespace_store(&self.rival, &self.namespace_id)
                .fold_wal_tail(
                    None,
                    &loonfs_core::time::OperationClock::start(
                        0,
                        Arc::new(loonfs_types::StdMonotonicTimer::default()),
                    ),
                )
                .await
                .expect("the rival folds");
        }
        (Intercept::Continue, ())
    }
}

/// Checks that a unit whose put loses to a fold publishes its output over the
/// fold's manifest, that the metadata loop counts only abandoned units as
/// lost races and stops after three, and that a fenced loop stops without
/// claiming the namespace back.
#[tokio::test]
async fn a_unit_retries_over_a_fold_and_the_loop_stops_after_three_abandoned_units() {
    use std::sync::atomic::Ordering::SeqCst;
    let temp_dir = tempdir().expect("tempdir");
    let namespace = namespace_id("losing-loop");
    let store = Arc::new(InterceptStore::with_interceptor(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        RivalFold {
            rival: LocalFsStore::new(temp_dir.path()).expect("store"),
            namespace_id: namespace.clone(),
            races_left: 0.into(),
            races_run: 0.into(),
        },
    ));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    for index in 0..loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS.get() + 2 {
        write_and_fold(&writer, &maintenance, &namespace, &format!("/file-{index}")).await;
    }
    maintenance
        .compactor_epoch(&namespace)
        .await
        .expect("claim the namespace before the races start");

    // One race: the unit's put loses to the rival's fold, and the next
    // attempt publishes over the fold's manifest.
    let before = current_manifest_payload(store.as_ref(), &namespace)
        .await
        .manifest_no;
    store.interceptor().races_left.store(1, SeqCst);
    let outcome = maintenance
        .compact_metadata(&namespace, &crate::MaintenanceCancellation::new())
        .await
        .expect("a lost put is retried, not an error");
    let MetadataCompactionOutcome::Published { manifest_no, .. } = outcome.compaction else {
        panic!("a unit that loses one put publishes, got {outcome:?}");
    };
    assert_eq!(store.interceptor().races_run.load(SeqCst), 1);
    // The rival's claim and fold took the numbers after `before`.
    assert!(
        manifest_no.0 > before.0 + 1,
        "the unit publishes over the rival's fold, {manifest_no} after {before}"
    );
    assert_eq!(
        current_manifest_payload(store.as_ref(), &namespace)
            .await
            .manifest_no,
        manifest_no
    );

    // Every put loses: each unit abandons after its attempts run out, and
    // the loop stops after three abandoned units in a row.
    store.interceptor().races_run.store(0, SeqCst);
    store.interceptor().races_left.store(64, SeqCst);
    assert_eq!(
        maintenance
            .maintain_metadata_while_due(&namespace, &crate::MaintenanceCancellation::new())
            .await
            .expect("a lost race is an outcome, not an error")
            .response
            .outcome,
        crate::MetadataMaintenanceOutcome::Due
    );
    assert_eq!(
        store.interceptor().races_run.load(SeqCst),
        3 * loonfs_core::limits::MAX_CONTENTION_ATTEMPTS,
        "the loop stops after three abandoned units"
    );

    store.interceptor().races_left.store(0, SeqCst);
    LoonFs::builder_with_store(Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")))
        .writer_id(loonfs_test_support::ids::writer_id("other"))
        .build()
        .await
        .expect("another process")
        .maintenance(loonfs_test_support::ids::writer_id("other"))
        .compactor_epoch(&namespace)
        .await
        .expect("another process claims the namespace");
    let fenced = current_manifest_payload(store.as_ref(), &namespace).await;
    assert_eq!(
        maintenance
            .maintain_metadata_while_due(&namespace, &crate::MaintenanceCancellation::new())
            .await
            .expect("a fenced unit is an outcome, not an error")
            .response
            .outcome,
        crate::MetadataMaintenanceOutcome::Due
    );
    assert_eq!(
        current_manifest_payload(store.as_ref(), &namespace).await,
        fenced,
        "a fenced loop stops instead of claiming the namespace back"
    );
}

#[tokio::test]
async fn a_metadata_loop_call_stops_after_its_unit_cap() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = writer(store.clone(), "writer").await;
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    let namespace = namespace_id("backlog");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    // One unit compacts at most eight runs of one family group, and each
    // folded write adds a run to several groups, so 32 folded writes need at
    // least four units in each of those groups.
    for index in 0..4 * loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS.get() {
        write_and_fold(&writer, &maintenance, &namespace, &format!("/file-{index}")).await;
    }
    maintenance
        .compactor_epoch(&namespace)
        .await
        .expect("claim the namespace first, so each later manifest is one unit");

    let mut published = Vec::new();
    for _ in 0..2 {
        let before = current_manifest_payload(store.as_ref(), &namespace)
            .await
            .manifest_no;
        let response = maintenance
            .maintain_metadata_while_due(&namespace, &crate::MaintenanceCancellation::new())
            .await
            .expect("run the metadata loop");
        if published.is_empty() {
            assert_eq!(
                response.response.outcome,
                crate::MetadataMaintenanceOutcome::Due,
                "the unit cap cannot establish completion"
            );
        }
        let after = current_manifest_payload(store.as_ref(), &namespace)
            .await
            .manifest_no;
        assert_eq!(
            response.response.compaction_units,
            after.0 - before.0,
            "the response counts every unit the call published"
        );
        published.push(after.0 - before.0);
    }
    assert_eq!(
        published[0],
        loonfs_types::u64_from_usize(super::compaction::MAX_COMPACTION_UNITS_PER_CALL),
        "one call publishes exactly the cap"
    );
    assert!(
        published[1] > 0,
        "a second call continues where the first stopped"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn an_idle_namespace_visit_costs_a_fixed_number_of_requests() {
    use loonfs_core::time::WallClock;
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore, StoreCounts};

    /// GET, HEAD, and LIST requests, then every write.
    fn requests(counts: StoreCounts) -> [usize; 4] {
        [
            counts.gets + counts.gets_with_metadata,
            counts.heads,
            counts.lists,
            counts.puts + counts.deletes,
        ]
    }

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let actor = loonfs_test_support::test_actor();
    let [idle, claimed, retired, missing] =
        ["idle", "claimed", "retired", "missing"].map(namespace_id);
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    for namespace in [&idle, &claimed, &retired] {
        writer
            .create_namespace(namespace, &actor)
            .await
            .expect("create the namespace");
        writer
            .open_namespace(namespace)
            .expect("open the namespace")
            .put_file("/file", b"body", &actor)
            .await
            .expect("put a file");
    }
    writer
        .open_namespace(&retired)
        .expect("open the namespace")
        .delete()
        .await
        .expect("delete the namespace");
    writer.shutdown().await.expect("shut down the writer");

    // A week ahead, a collection finds everything the test wrote past
    // every grace window.
    let week_ahead_ms = loonfs_core::time::SystemWallClock.now_ms() + 7 * 24 * 60 * 60 * 1000;
    let maintenance = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("maintenance"))
        .wall_clock(Arc::new(ManualClock::new(week_ahead_ms)))
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    for namespace in [&idle, &claimed] {
        maintenance
            .fold_wal(namespace)
            .await
            .expect("fold the tail");
    }
    maintenance
        .compactor_epoch(&claimed)
        .await
        .expect("claim the namespace");
    // The first collection of `retired` retires the deleted namespace, and
    // the second deletes what is left under its prefix.
    for namespace in [&idle, &claimed, &retired, &retired] {
        maintenance
            .gc(namespace)
            .await
            .expect("collect what the setup left behind");
    }

    store.reset();
    let cancelled = crate::MaintenanceCancellation::new();
    cancelled.cancel();
    assert_eq!(
        maintenance
            .maintain_metadata_while_due(&idle, &cancelled)
            .await
            .expect("cancellation is not completion")
            .response
            .outcome,
        crate::MetadataMaintenanceOutcome::Due
    );
    assert_eq!(requests(store.counts()), [0; 4]);

    // A load of a namespace lists its manifests once, and a load of a live
    // namespace sends a HEAD request for the missing manifest after the
    // current one and a read of the manifest boundary in its check of the
    // manifest. A load of a retired namespace reads its boundary and lists
    // nothing. A collection of a live namespace lists each of its five
    // families once, the snapshots included, and a collection of a retired
    // namespace lists the namespace's prefix once. Neither lists anything
    // under its pool's prefix.
    let heads = 1;
    for (namespace, metadata_requests, metadata_error, gc_requests, gc_error) in [
        (&idle, [5, heads, 1, 0], None, [5, heads, 6, 0], None),
        (&claimed, [5, heads, 1, 0], None, [5, heads, 6, 0], None),
        (
            &retired,
            [1, 0, 0, 0],
            Some(crate::ErrorCode::NamespaceDeleted),
            [1, 0, 1, 0],
            None,
        ),
        (
            &missing,
            [2, 0, 1, 0],
            Some(crate::ErrorCode::NamespaceNotFound),
            [2, 0, 1, 0],
            Some(crate::ErrorCode::NamespaceNotFound),
        ),
    ] {
        store.reset();
        let metadata = maintenance
            .maintain_metadata_while_due(namespace, &crate::MaintenanceCancellation::new())
            .await;
        if metadata_error.is_none() {
            assert_eq!(
                metadata
                    .as_ref()
                    .expect("idle metadata is caught up")
                    .response
                    .outcome,
                crate::MetadataMaintenanceOutcome::CaughtUp
            );
        }
        assert_eq!(metadata.err().map(|error| error.code()), metadata_error);
        assert_eq!(
            requests(store.counts()),
            metadata_requests,
            "the metadata loop on `{namespace}`"
        );
        store.reset();
        let gc = maintenance.gc(namespace).await;
        assert_eq!(
            requests(store.counts()),
            gc_requests,
            "collection of `{namespace}`"
        );
        assert_eq!(gc.as_ref().err().map(|error| error.code()), gc_error);
        if let Ok(report) = gc {
            assert_eq!(report.deleted, loonfs_types::DeletedObjectCounts::default());
        }
    }

    let other = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("other"))
        .build()
        .await
        .expect("another process");
    other
        .maintenance(loonfs_test_support::ids::writer_id("other"))
        .compactor_epoch(&claimed)
        .await
        .expect("another process claims the namespace");
    store.reset();
    let fenced = maintenance
        .maintain_metadata(&claimed)
        .await
        .expect("a fenced step is an outcome, not an error");
    assert_eq!(fenced.compaction, MetadataCompactionOutcome::Fenced);
    assert_eq!(
        requests(store.counts()),
        [5, heads, 1, 0],
        "the observed anchor already reports the fence"
    );
}

#[tokio::test]
async fn an_advance_that_moves_no_floor_loads_the_manifest_once_and_writes_nothing() {
    use crate::{AdvanceRetentionFloorOptions, CommitSeq, RetentionTarget};
    use loonfs_test_support::stores::{KeyPredicate, RecordedOperation, RecordingStore};

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let actor = loonfs_test_support::test_actor();
    let namespace = namespace_id("idle");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace, &actor)
        .await
        .expect("create the namespace");
    for path in ["/a", "/b"] {
        writer
            .open_namespace(&namespace)
            .expect("open the namespace")
            .put_file(path, b"body", &actor)
            .await
            .expect("put a file");
    }
    writer
        .maintenance(loonfs_test_support::ids::writer_id("writer"))
        .fold_wal(&namespace)
        .await
        .expect("fold commits 1 and 2");
    writer.shutdown().await.expect("shut down the writer");

    // Each advance runs in a new runtime, so no cache has the segments that
    // the fold wrote.
    let maintenance = || maintenance_past_the_grace_window(store.clone());
    for (floor, advances) in [
        (
            CommitSeq(1),
            [
                (RetentionTarget::Seq(CommitSeq(0)), 6),
                (RetentionTarget::Seq(CommitSeq(1)), 6),
                // The scan reads commit 2 from the commits segment, and
                // commit 2 is later than the cutoff.
                (RetentionTarget::Cutoff { cutoff_at_ms: 0 }, 7),
            ],
        ),
        (
            CommitSeq(2),
            [
                (RetentionTarget::Head, 6),
                (RetentionTarget::Seq(CommitSeq(u64::MAX)), 6),
                // No folded commit is above the head, so the scan reads no
                // segment.
                (
                    RetentionTarget::Cutoff {
                        cutoff_at_ms: u64::MAX,
                    },
                    6,
                ),
            ],
        ),
    ] {
        let advancing = maintenance().await;
        advancing
            .advance_retention_floor_with_options(
                &namespace,
                &AdvanceRetentionFloorOptions {
                    target: RetentionTarget::Seq(floor),
                },
            )
            .await
            .expect("advance the floor");
        let manifest = metadata_manifest_object(
            &namespace,
            &current_manifest_payload(store.as_ref(), &namespace)
                .await
                .manifest_no,
        );
        for (target, gets) in advances {
            let maintenance = maintenance().await;
            store.reset();
            let advanced = maintenance
                .advance_retention_floor_with_options(
                    &namespace,
                    &AdvanceRetentionFloorOptions { target },
                )
                .await
                .expect("an advance that moves nothing");
            assert_eq!(advanced.retention_floor_seq, floor, "{target:?}");
            let operations = store.take();
            let heads = operations
                .iter()
                .filter(|operation| matches!(operation, RecordedOperation::Head { .. }))
                .count();
            assert!(
                operations.iter().all(|operation| matches!(
                    operation,
                    RecordedOperation::Get { .. }
                        | RecordedOperation::GetWithMetadata { .. }
                        | RecordedOperation::Head { .. }
                        | RecordedOperation::List { .. }
                )),
                "{target:?} sends only reads: {operations:?}"
            );
            let loads = operations
                .iter()
                .filter(|operation| {
                    matches!(operation, RecordedOperation::Get { key, .. } if *key == manifest)
                })
                .count();
            assert_eq!(
                (loads, operations.len() - heads, heads),
                (1, gets, 1),
                "{target:?} loads the current manifest once and plans from the view after it \
                 (the manifest boundary, a listing of the manifests, the manifest, the \
                 boundary again, a GET of the missing WAL number after the manifest, a \
                 HEAD request for the missing manifest after it, and the boundary once \
                 more): {operations:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_repeated_collection_of_an_emptied_pool_lists_once_and_returns_pool_not_found() {
    use loonfs_core::limits::GC_SAFETY_MARGIN_MS;
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore, StoreCounts};

    let directory = tempdir().expect("directory");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::new(|key| key.starts_with("pools/") && key.contains("/content/")),
    ));
    let namespace_id = namespace_id("retired");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("retirement-test"))
        .build()
        .await
        .expect("writer");
    let pool_id = writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace")
        .pool_id;
    for _ in 0..3 {
        let key =
            loonfs_objectstore::keys::content_blob(&pool_id, &loonfs_types::ContentId::generate());
        store
            .put_if_absent(&key, bytes::Bytes::from_static(b"content"))
            .await
            .expect("content");
    }
    let mut context = loonfs_core::test_support::writer_at("deleter", 1_000);
    loonfs_core::test_support::NamespaceFixture::new(store.as_ref(), &namespace_id, &context)
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete");
    context.now_ms += crate::GC_GRACE_WINDOW_MS + GC_SAFETY_MARGIN_MS;
    loonfs_core::test_support::namespace_store(store.as_ref(), &namespace_id)
        .gc(&context.clock())
        .await
        .expect("retire the namespace");
    // The content's provider ages are real times, so the pool's collections
    // run far later than the content was written.
    context.now_ms = u64::MAX / 4;
    store.reset();
    let pool = loonfs_core::test_support::pool_store(store.as_ref(), &pool_id);
    let first = pool
        .gc(None, &context.clock())
        .await
        .expect("first collection");
    assert_eq!(first.deleted.content_objects, 3);
    assert_eq!(
        store.counts(),
        StoreCounts {
            lists: 1,
            deletes: 3,
            ..Default::default()
        }
    );
    store.reset();
    // Retirement deleted the member entry, and the first collection deleted
    // the content, so nothing is under the pool's prefix.
    let repeated = pool
        .gc(None, &context.clock())
        .await
        .expect_err("repeat collection");
    assert_eq!(repeated.code(), crate::ErrorCode::PoolNotFound);
    assert_eq!(
        store.counts(),
        StoreCounts {
            lists: 1,
            ..Default::default()
        }
    );
    writer.shutdown().await.expect("shutdown");
}

/// The file under `/large` that the read-after-fold tests read.
const LARGE_FOLD_FILE: &str = "/large/f-02048.txt";

/// Commits 8,192 new files under `/large`, whose bytes are their paths, in
/// two commits, and folds them with one explicit fold. Checks that the fold's
/// revisions segment, which a read of one of the files looks in, is larger
/// than 256 KiB stored, the largest segment that LoonFS fetches whole.
async fn write_a_large_batch_and_fold<S: ObjectStore + ?Sized>(
    store: &S,
    namespace: &crate::Namespace<Writable>,
    maintenance: &Maintenance,
    namespace_id: &NamespaceId,
) {
    use crate::publish::{CommitIntent, FilesystemOperation};

    for commit in 0..2 {
        let mut operations = Vec::new();
        let mut prepared = Vec::new();
        for index in commit * 4096..(commit + 1) * 4096 {
            let path = format!("/large/f-{index:05}.txt");
            let content = namespace
                .prepare_content(path.as_bytes())
                .await
                .expect("prepare the content");
            operations.push(FilesystemOperation::PutFile {
                path: loonfs_types::AbsolutePath::parse(path).expect("path"),
                content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
                behavior: crate::DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            });
            prepared.push(content);
        }
        let intent = CommitIntent {
            commit_id: loonfs_types::CommitId::generate(),
            actor_id: loonfs_test_support::test_actor(),
            subject: None,
            message: None,
            operations,
            preconditions: Vec::new(),
        };
        namespace
            .commit_prepared(intent, prepared)
            .await
            .expect("commit the batch");
    }
    maintenance
        .fold_wal(namespace_id)
        .await
        .expect("fold the batch");
    let manifest = current_manifest_payload(store, namespace_id).await;
    let folded = manifest.runs.last().expect("the fold's run");
    let revisions = folded
        .segments
        .iter()
        .find(|segment| segment.family == MetadataRowFamily::Revisions)
        .expect("the fold writes revisions");
    let stored = revisions.index_block.offset + u64::from(revisions.index_block.stored_bytes);
    assert!(
        stored > 256 * 1024,
        "the fold's revisions segment is {stored} bytes"
    );
}

/// Checks that the first read after an explicit fold, again after bounded
/// compaction units, and again after a fold whose segments are larger than a
/// segment that LoonFS fetches whole, finds the segments that maintenance
/// wrote in the runtime's segment cache and sends no segment GET.
#[tokio::test]
async fn a_read_after_a_fold_or_a_compaction_unit_sends_no_segment_get() {
    use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
    use std::collections::HashMap;

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .run_policy(loonfs_core::test_support::one_delta_run_policy())
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    let namespace_id = namespace_id("read-after-maintenance");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open the namespace");
    let read_gets_by_key_class = |path: &'static str| {
        let namespace = &namespace;
        let store = &store;
        async move {
            store.reset();
            let file = namespace.read_file(path).await.expect("read the file");
            assert_eq!(file.bytes, path.as_bytes());
            let mut gets = HashMap::<Option<DurableObjectFamily>, usize>::new();
            for key in store.take_get_keys() {
                *gets
                    .entry(parse_object_key(&key).map(|parsed| parsed.family()))
                    .or_default() += 1;
            }
            gets
        }
    };
    let segment = Some(DurableObjectFamily::MetadataSegment);

    write_and_fold(&writer, &maintenance, &namespace_id, "/docs/first.txt").await;
    let gets = read_gets_by_key_class("/docs/first.txt").await;
    assert_eq!(gets.get(&segment), None, "GETs after a fold: {gets:?}");

    write_and_fold(&writer, &maintenance, &namespace_id, "/docs/second.txt").await;
    let mut units = 0;
    loop {
        match maintenance
            .compact_metadata(&namespace_id, &crate::MaintenanceCancellation::new())
            .await
            .expect("compact one unit")
            .compaction
        {
            MetadataCompactionOutcome::Published { .. } => units += 1,
            MetadataCompactionOutcome::NotNeeded => break,
            other => panic!("expected a published unit, found {other:?}"),
        }
    }
    assert!(units > 0, "the second fold leaves runs to compact");
    let manifest = current_manifest_payload(store.as_ref(), &namespace_id).await;
    assert!(manifest.runs.iter().all(|run| run.tier == RunTier::Base));
    let gets = read_gets_by_key_class("/docs/second.txt").await;
    assert_eq!(gets.get(&segment), None, "GETs after compaction: {gets:?}");

    write_a_large_batch_and_fold(store.as_ref(), &namespace, &maintenance, &namespace_id).await;
    let gets = read_gets_by_key_class(LARGE_FOLD_FILE).await;
    assert_eq!(
        gets.get(&segment),
        None,
        "GETs after a large fold: {gets:?}"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a fold on a runtime whose cache has an SSD tier writes every
/// block of its segments to disk, so a runtime started over the same SSD
/// directory reads the folded file from disk and sends no segment GET.
#[tokio::test]
async fn a_restarted_runtime_reads_a_folded_file_from_the_ssd_tier() {
    use crate::{MetadataCache, SsdSegmentBlocks};
    use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore};

    let store_directory = tempdir().expect("tempdir");
    let ssd_directory = tempdir().expect("tempdir");
    let ssd_cache = || async {
        let budget = NonZeroUsize::new(1 << 30).expect("a nonzero budget");
        let ssd = SsdSegmentBlocks::open(ssd_directory.path(), budget)
            .await
            .expect("open the SSD tier");
        MetadataCache::builder().ssd_segment_blocks(ssd).build()
    };
    let namespace_id = namespace_id("ssd-after-fold");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(store_directory.path()).expect("store"));
    let cache = ssd_cache().await;
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .metadata_cache(cache.clone())
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open the namespace");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    write_a_large_batch_and_fold(store.as_ref(), &namespace, &maintenance, &namespace_id).await;
    writer.shutdown().await.expect("shutdown");
    cache.close().await;

    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(store_directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let cache = ssd_cache().await;
    let reader = LoonFs::builder_with_store(recording.clone())
        .metadata_cache(cache.clone())
        .read_only()
        .build()
        .await
        .expect("reader");
    let file = reader
        .namespace(&namespace_id)
        .read_file(LARGE_FOLD_FILE)
        .await
        .expect("read the file");
    assert_eq!(file.bytes, LARGE_FOLD_FILE.as_bytes());
    let segment_gets = recording
        .take_get_keys()
        .iter()
        .filter(|key| {
            parse_object_key(key)
                .is_some_and(|parsed| parsed.family() == DurableObjectFamily::MetadataSegment)
        })
        .count();
    assert_eq!(segment_gets, 0, "{:?}", cache.stats());
    assert!(cache.stats().segment_ssd.expect("the SSD tier").hits > 0);
}

/// Checks that an extension's publication in a runtime without the
/// namespace's writer session moves that runtime's cached head onto the
/// published manifest, and caches that manifest, so the next read sees the
/// published entry and the WAL tail from the cache, with no request.
#[tokio::test]
async fn a_read_after_an_extension_publication_sends_no_request() {
    use crate::{Consistency, ReadAt, StatOptions};
    use loonfs_test_support::stores::{KeyPredicate, RecordingStore, StoreCounts};
    use loonfs_types::format::manifest::ExtensionEntry;
    use loonfs_types::ExtensionName;

    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let actor = loonfs_test_support::test_actor();
    let namespace = namespace_id("extension");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace, &actor)
        .await
        .expect("create the namespace");
    writer
        .open_namespace(&namespace)
        .expect("open the namespace")
        .put_file("/file", b"body", &actor)
        .await
        .expect("put a file");
    writer.shutdown().await.expect("shut down the writer");
    // The runtime's timer doesn't move, so its cached head stays within an
    // eventual read's maximum staleness.
    let runtime = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("maintenance"))
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("maintenance runtime");
    let maintenance = runtime.maintenance(loonfs_test_support::ids::writer_id("maintenance"));
    let eventual = Consistency::Eventual {
        max_staleness_ms: Some(1_000),
    };
    let stat = StatOptions {
        at: ReadAt::Live(Some(eventual)),
        ..StatOptions::default()
    };
    let namespace_handle = runtime
        .open_namespace(&namespace)
        .expect("open the namespace");
    namespace_handle
        .stat_with_options("/file", &stat)
        .await
        .expect("stat the file");

    let name = ExtensionName::parse("grep").expect("extension name");
    let entry = ExtensionEntry::new(1, Vec::new(), serde_json::Map::new());
    let basis = maintenance
        .load_extension_basis(&namespace)
        .await
        .expect("load the basis");
    let published = maintenance
        .publish_extension(&basis, &name, entry.clone())
        .await
        .expect("publish the entry");
    let loonfs_core::ExtensionPublication::Published(published) = published else {
        panic!("no other publication races the test's own");
    };
    assert!(published.manifest().manifest_no > basis.manifest().manifest_no);

    store.reset();
    let pinned = maintenance
        .core
        .pinned_read(&namespace, eventual)
        .await
        .expect("read the namespace");
    let found = pinned
        .engine
        .find_extension_entry(&pinned.context, &name)
        .await
        .expect("find the entry");
    assert_eq!(found, Some(entry));
    namespace_handle
        .stat_with_options("/file", &stat)
        .await
        .expect("stat the file");
    assert_eq!(
        store.counts(),
        StoreCounts::default(),
        "{:?}",
        store.snapshot()
    );
}
