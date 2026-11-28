//! The compaction loop, `Namespace::compact_metadata`, and the store
//! requests of other maintenance calls, over a live runtime.

use crate::metrics::DefaultMetricsRecorder;
use crate::test_support::{counter, writer, writer_with_offset_clock};
use crate::{LoonFs, MetadataCompactionOutcome, NamespaceId, SharedObjectStore, Writable};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::namespace_id;
use loonfs_types::format::manifest::{
    decode_namespace_manifest_json, MetadataRowFamily, NamespaceManifestPayload, RunTier,
};
use loonfs_types::RunNo;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tempfile::tempdir;

/// A writer over one store with a metrics recorder of its own, and the
/// writer again, through which the tests run their maintenance calls.
async fn manual_deployment(
    root: &std::path::Path,
) -> (
    LoonFs<Writable>,
    LoonFs<Writable>,
    Arc<DefaultMetricsRecorder>,
) {
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(root).expect("create local-fs store"));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = LoonFs::builder_with_store(Arc::clone(&store), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("manual-writer"))
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("build the writer");
    (writer.clone(), writer, recorder)
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
        .maintenance()
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
        .maintenance()
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
        .maintenance()
        .gc_pool(&crate::PoolId::generate())
        .await
        .expect_err("a pool without a record");
    assert_eq!(missing.code(), crate::ErrorCode::PoolNotFound);
}

/// Writes one file and folds the tail, so each call leaves one more delta run.
async fn write_and_fold(
    writer: &LoonFs<Writable>,
    maintenance: &LoonFs<Writable>,
    namespace_id: &NamespaceId,
    path: &str,
) {
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    namespace
        .put_file(path, path.as_bytes(), &loonfs_test_support::test_actor())
        .await
        .expect("put a file");
    maintenance
        .open_namespace(namespace_id)
        .expect("open namespace")
        .fold_wal()
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
    maintenance: &LoonFs<Writable>,
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
    maintenance
        .open_namespace(namespace_id)
        .expect("open namespace")
        .fold_wal()
        .await
        .expect("fold base");
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
    let writer = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    let maintenance = writer.clone();
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
        maintenance
            .open_namespace(&namespace)
            .expect("open namespace")
            .fold_wal()
            .await
            .expect("fold delta");
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
        .open_namespace(&namespace)
        .expect("open namespace")
        .maintain_metadata_while_due(&crate::MaintenanceCancellation::new())
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
async fn a_metadata_loop_call_stops_after_its_unit_cap() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = writer(store.clone(), "writer").await;
    let maintenance = writer.clone();
    let namespace = namespace_id("backlog");
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("create the namespace");
    // One unit compacts at most eight runs of one family group, and each
    // folded write adds a run to several groups, so 32 folded writes need at
    // least four units in each of those groups.
    // One handle keeps the writer session open across the calls, so no call
    // claims the namespace again.
    let held = writer.open_namespace(&namespace).expect("open namespace");
    for index in 0..4 * loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS.get() {
        write_and_fold(&writer, &maintenance, &namespace, &format!("/file-{index}")).await;
    }
    let mut published = Vec::new();
    for _ in 0..2 {
        let before = current_manifest_payload(store.as_ref(), &namespace)
            .await
            .manifest_no;
        let response = held
            .maintain_metadata_while_due(&crate::MaintenanceCancellation::new())
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
        loonfs_types::u64_from_usize(crate::publisher::MAX_COMPACTION_UNITS_PER_CALL),
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
    let [idle, retired, missing] = ["idle", "retired", "missing"].map(namespace_id);
    let writer = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .build()
        .await
        .expect("writer");
    for namespace in [&idle, &retired] {
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
    let maintenance = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("maintenance"))
        .wall_clock(Arc::new(ManualClock::new(week_ahead_ms)))
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("maintenance");
    maintenance
        .open_namespace(&idle)
        .expect("open namespace")
        .fold_wal()
        .await
        .expect("fold the tail");
    // The first collection of `retired` retires the deleted namespace, and
    // the second deletes what is left under its prefix.
    for namespace in [&idle, &retired, &retired] {
        maintenance
            .maintenance()
            .gc(namespace)
            .await
            .expect("collect what the setup left behind");
    }

    store.reset();
    let cancelled = crate::MaintenanceCancellation::new();
    cancelled.cancel();
    assert_eq!(
        maintenance
            .open_namespace(&idle)
            .expect("open namespace")
            .maintain_metadata_while_due(&cancelled)
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
    // nothing. A collection of a live namespace reads no WAL object and
    // sends no HEAD request, because its roots come from the manifest that
    // its find returns. It lists each of its five families once, the
    // snapshots included, and a collection of a retired
    // namespace lists the namespace's prefix once. Neither lists anything
    // under its pool's prefix. The live namespaces' manifests are in the
    // segment cache from the setup's folds and collections in this runtime,
    // so neither the load nor the collection sends a GET for a manifest.
    let heads = 1;
    for (namespace, metadata_requests, metadata_error, gc_requests, gc_error) in [
        (&idle, [4, heads, 1, 0], None, [2, 0, 6, 0], None),
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
            .open_namespace(namespace)
            .expect("open namespace")
            .maintain_metadata_while_due(&crate::MaintenanceCancellation::new())
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
        let gc = maintenance.maintenance().gc(namespace).await;
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
    let writer = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
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
        .open_namespace(&namespace)
        .expect("open namespace")
        .fold_wal()
        .await
        .expect("fold commits 1 and 2");
    writer.shutdown().await.expect("shut down the writer");

    let boundary = loonfs_objectstore::keys::boundary(&namespace);
    let manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&namespace);
    let segments = loonfs_objectstore::keys::metadata_segment_prefix(&namespace);
    // Each advance runs in a new runtime, so no cache has the segments that
    // the fold wrote.
    let advancer = || writer_with_offset_clock(store.clone(), "advancer");
    for (floor, advances) in [
        (
            CommitSeq(1),
            [
                (RetentionTarget::Seq(CommitSeq(0)), false),
                (RetentionTarget::Seq(CommitSeq(1)), false),
                // The scan reads commit 2 from the commits segment, and
                // commit 2 is later than the cutoff.
                (RetentionTarget::Cutoff { cutoff_at_ms: 0 }, true),
            ],
        ),
        (
            CommitSeq(2),
            [
                (RetentionTarget::Head, false),
                (RetentionTarget::Seq(CommitSeq(u64::MAX)), false),
                // No folded commit is above the head, so the scan reads no
                // segment.
                (
                    RetentionTarget::Cutoff {
                        cutoff_at_ms: u64::MAX,
                    },
                    false,
                ),
            ],
        ),
    ] {
        let (advancing, clock) = advancer().await;
        let advancing = advancing
            .open_namespace(&namespace)
            .expect("open namespace");
        clock
            .past_the_grace_window(advancing.advance_retention_floor_with_options(
                &AdvanceRetentionFloorOptions {
                    target: RetentionTarget::Seq(floor),
                },
            ))
            .await
            .expect("advance the floor");
        let manifest = metadata_manifest_object(
            &namespace,
            &current_manifest_payload(store.as_ref(), &namespace)
                .await
                .manifest_no,
        );
        for (target, reads_a_segment) in advances {
            let (maintenance, clock) = advancer().await;
            let maintenance = maintenance
                .open_namespace(&namespace)
                .expect("open namespace");
            store.reset();
            let advanced =
                clock
                    .past_the_grace_window(maintenance.advance_retention_floor_with_options(
                        &AdvanceRetentionFloorOptions { target },
                    ))
                    .await
                    .expect("an advance that moves nothing");
            assert_eq!(advanced.retention_floor_seq, floor, "{target:?}");
            let operations: Vec<_> = store
                .take()
                .into_iter()
                .map(|operation| match &operation {
                    RecordedOperation::GetWithMetadata { key, .. } if *key == boundary => {
                        "boundary".to_owned()
                    }
                    RecordedOperation::List { prefix } if prefix.starts_with(&manifests) => {
                        "manifest listing".to_owned()
                    }
                    RecordedOperation::Get { key, .. } if *key == manifest => "manifest".to_owned(),
                    RecordedOperation::Get { key, .. } if key.starts_with(&segments) => {
                        "segment".to_owned()
                    }
                    other => format!("{other:?}"),
                })
                .collect();
            let mut expected = vec!["boundary", "manifest listing", "manifest", "boundary"];
            if reads_a_segment {
                expected.push("segment");
            }
            assert_eq!(
                operations, expected,
                "{target:?} finds the current manifest and plans from that manifest's floor, \
                 with no read of the WAL and no claim"
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
    let writer = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
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
        .delete_namespace(
            &loonfs_core::test_support::administrator_of(store.as_ref(), &namespace_id),
            Default::default(),
        )
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
    maintenance: &LoonFs<Writable>,
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
        .open_namespace(namespace_id)
        .expect("open namespace")
        .fold_wal()
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
    let writer = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("writer"))
        .run_policy(loonfs_core::test_support::one_delta_run_policy())
        .build()
        .await
        .expect("writer");
    let maintenance = writer.clone();
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
            .open_namespace(&namespace_id)
            .expect("open namespace")
            .compact_metadata(&crate::MaintenanceCancellation::new())
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
    let writer = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
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
    let maintenance = writer.clone();
    write_a_large_batch_and_fold(store.as_ref(), &namespace, &maintenance, &namespace_id).await;
    writer.shutdown().await.expect("shutdown");
    cache.close().await;

    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(store_directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let cache = ssd_cache().await;
    let reader = LoonFs::builder_with_store(recording.clone(), crate::Context::Service)
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
/// Checks that an extension's publication through a writer session that
/// hasn't committed, so reads there started from the cached head, moves the
/// session's position onto the published manifest, so the next read finds
/// the published entry with no request.
#[tokio::test]
async fn a_read_after_an_extension_publication_starts_from_the_sessions_position() {
    use crate::StatOptions;
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
    let writer = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
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
    let runtime = LoonFs::builder_with_store(store.clone(), crate::Context::Service)
        .writer_id(loonfs_test_support::ids::writer_id("maintenance"))
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("maintenance runtime");
    let maintenance = runtime.clone();
    let stat = StatOptions::default();
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
        .open_namespace(&namespace)
        .expect("open namespace")
        .load_extension_basis()
        .await
        .expect("load the basis");
    let published = maintenance
        .open_namespace(&namespace)
        .expect("open namespace")
        .publish_extension(&basis, &name, entry.clone())
        .await
        .expect("publish the entry");
    let loonfs_core::ExtensionPublication::Published {
        basis: published, ..
    } = published
    else {
        panic!("no other publication races the test's own");
    };
    assert!(published.manifest().manifest_no > basis.manifest().manifest_no);

    store.reset();
    let pinned = maintenance
        .core
        .pinned_read(&namespace, crate::Consistency::Eventual)
        .await
        .expect("read the namespace");
    let found = pinned
        .engine
        .find_extension_entry(&pinned.context, &name)
        .await
        .expect("find the entry");
    assert_eq!(found, Some(entry));
    // The publication is the writer session's, so the session's position
    // moved onto the published manifest, and the read starts from it with
    // no request.
    let counts = store.counts();
    assert_eq!(
        (
            counts.gets_with_metadata,
            counts.lists,
            counts.gets,
            counts.heads
        ),
        (0, 0, 0, 0),
        "{:?}",
        store.snapshot()
    );
    store.reset();
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
