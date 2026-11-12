//! GrepWorker lifecycle, rebootstrap, query contracts, and grep's entry.

use crate::common::{
    control, default_block_cache, fold_wal_and_advance_retention, grep_with, page_request, GrepHost,
};
use bytes::Bytes;
use loonfs::{
    CreateNamespaceOptions, ErrorCode, LoonFs, NamespaceId, PutFileOptions,
    RestoreDeletedDestination, RestoreDeletedOptions, SharedObjectStore,
};
use loonfs_grep::entry::{grep_extension_name, GrepIndexStatus};
use loonfs_grep::NamespaceReads;
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepReorganizeOutcome, GrepService,
    GrepWorker,
};
use loonfs_objectstore::keys::{
    extension_segment_prefix, metadata_manifest_prefix, snapshot_record,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::ids::{first_page, nonzero_usize};
use loonfs_test_support::stores::{
    replace_object, FailStore, InjectedError, KeyPredicate, OperationContext, OperationSelector,
    RecordingStore,
};
use loonfs_types::format::control::SnapshotOwner;
use loonfs_types::format::manifest::ExtensionEntry;
use loonfs_types::{
    AbsolutePath, CommitSeq, GrepRequest, GrepResponse, GrepScope, IndexSegmentId, PageRequest,
    RunNo, MAX_PUBLIC_INTEGER,
};
use std::assert_matches;
use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

fn request(pattern: &str) -> GrepRequest {
    GrepRequest {
        pattern: pattern.to_owned(),
        case_insensitive: false,
        scope: GrepScope::Namespace,
        allow_stale: false,
        allow_scan: false,
    }
}

async fn worker(store: &SharedObjectStore) -> GrepWorker<SharedObjectStore> {
    GrepHost::new(store, "grep-worker-tests").await.worker
}

async fn drive_worker_to_current(
    worker: &GrepWorker<SharedObjectStore>,
    namespace_id: &NamespaceId,
    policy: GramIndexBuildPolicy,
) {
    for _ in 0..512 {
        let build = worker
            .steps(namespace_id, 0)
            .build_step(policy)
            .await
            .expect("worker build step");
        let reorganize = worker
            .steps(namespace_id, 0)
            .reorganize_step(policy)
            .await
            .expect("worker reorganize step");
        if matches!(build, GrepBuildOutcome::UpToDate { .. })
            && matches!(reorganize, GrepReorganizeOutcome::NotNeeded { .. })
        {
            return;
        }
    }
    panic!("worker backlog must drain");
}

/// A cold query: a fresh reader and a fresh grep service, so nothing an
/// earlier query decoded can answer this one.
async fn new_query(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    grep_request: &GrepRequest,
) -> loonfs_grep::Result<GrepResponse> {
    new_query_page(store, namespace_id, grep_request, first_page()).await
}

async fn new_query_page(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    grep_request: &GrepRequest,
    page: PageRequest<String>,
) -> loonfs_grep::Result<GrepResponse> {
    let reader = LoonFs::builder_with_store(store.clone())
        .read_only()
        .build()
        .await
        .expect("new query reader");
    grep_with(
        &GrepService::new(default_block_cache()),
        &reader,
        store,
        namespace_id,
        grep_request,
        page,
    )
    .await
}

/// Clears what differs between the responses of two namespaces that contain
/// the same files: the namespace ID, and the inode IDs, which each namespace
/// generates separately and which also set the order of the matches.
fn normalize_namespace(mut response: GrepResponse) -> GrepResponse {
    response.namespace_id = NamespaceId::parse("normalized").expect("namespace id");
    for found in &mut response.matches {
        found.inode_id = loonfs_types::ROOT_INODE_ID;
    }
    response.matches.sort_by(|left, right| {
        (&left.path, left.byte_offset).cmp(&(&right.path, right.byte_offset))
    });
    response
}

#[tokio::test]
async fn grep_worker_lifecycle_uses_and_releases_its_backfill_snapshot() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-lifecycle").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("lifecycle-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for index in 0..3u32 {
        namespace
            .put_file(
                &format!("/before-{index}.txt"),
                format!("snapshot needle {index}\n").as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("write preexisting file");
    }

    let worker = worker(&store).await;
    let enabled = worker.enable(&namespace_id).await.expect("enable");
    assert_matches!(enabled, loonfs_grep::GrepEnableOutcome::Enabled { .. });
    let again = worker
        .enable(&namespace_id)
        .await
        .expect("idempotent enable");
    assert_matches!(again, loonfs_grep::GrepEnableOutcome::AlreadyEnabled { .. });
    let manifest = crate::common::control::grep_entry(&store, &namespace_id)
        .await
        .expect("manifest exists");
    let GrepIndexStatus::Backfilling { snapshot_id, .. } = manifest.status() else {
        panic!(
            "enable must publish a backfill over a snapshot: {:?}",
            manifest
        );
    };
    let snapshot_id = snapshot_id.clone();

    let policy = GramIndexBuildPolicy {
        max_files_per_step: NonZeroUsize::MIN,
        ..GramIndexBuildPolicy::default()
    };
    let first = worker
        .steps(&namespace_id, 0)
        .build_step(policy)
        .await
        .expect("first backfill page");
    assert_matches!(first, GrepBuildOutcome::Indexed { .. });
    let error = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect_err("backfill is not materialized");
    assert_eq!(error.code(), ErrorCode::NotSupported);

    drive_worker_to_current(&worker, &namespace_id, policy).await;
    assert!(
        control::snapshot_record(&store, &namespace_id, &snapshot_id)
            .await
            .is_none()
    );
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("materialized query");
    assert_eq!(response.matches.len(), 3);
    assert!(!control::grep_entry(&store, &namespace_id)
        .await
        .expect("entry exists")
        .runs()
        .is_empty());

    assert_eq!(
        worker.disable(&namespace_id).await.expect("disable"),
        loonfs_grep::GrepDisableOutcome::Disabled
    );
    let disabled_manifest = crate::common::control::grep_entry(&store, &namespace_id)
        .await
        .expect("disabled manifest remains");
    assert_matches!(disabled_manifest.status(), GrepIndexStatus::Disabled {});
    assert!(
        disabled_manifest.runs().is_empty(),
        "disabling empties the entry's runs, which leaves the segments to the core collector"
    );
    assert_eq!(
        worker
            .disable(&namespace_id)
            .await
            .expect("idempotent disable"),
        loonfs_grep::GrepDisableOutcome::NotEnabled
    );
    let reenabled = worker.enable(&namespace_id).await.expect("re-enable");
    assert_matches!(reenabled, loonfs_grep::GrepEnableOutcome::Enabled { .. });
    let manifest = crate::common::control::grep_entry(&store, &namespace_id)
        .await
        .expect("manifest exists");
    assert!(manifest.runs().is_empty());
    assert_matches!(manifest.status(), GrepIndexStatus::Backfilling { .. });
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn exhausted_run_numbers_fail_as_index_corruption_without_writing_the_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("run-number-limit").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "run-number-limit-writer",
        ))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace
        .put_file(
            "/initial.txt",
            b"initial run number boundary needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write initial file");

    let host = GrepHost::new(&store, "run-number-limit").await;
    let worker = &host.worker;
    worker.enable(&namespace_id).await.expect("enable grep");
    drive_worker_to_current(worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let current = control::grep_entry(&store, &namespace_id)
        .await
        .expect("current entry");
    crate::common::publish_grep_entry(
        &host.maintenance,
        &namespace_id,
        current.encode(),
        RunNo(MAX_PUBLIC_INTEGER),
    )
    .await;

    namespace
        .put_file(
            "/incremental.txt",
            b"incremental run number boundary needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write incremental file");
    let manifests_before = store
        .list_prefix(&metadata_manifest_prefix(&namespace_id))
        .await
        .expect("list manifests before failure");
    let segments_prefix = extension_segment_prefix(&namespace_id, &grep_extension_name());
    let segments_before = store
        .list_prefix(&segments_prefix)
        .await
        .expect("list segments before failure");

    for error in [
        worker
            .steps(&namespace_id, 0)
            .build_step(GramIndexBuildPolicy::default())
            .await
            .expect_err("a build cannot allocate a run above the public maximum"),
        worker
            .steps(&namespace_id, 0)
            .reorganize_step(GramIndexBuildPolicy {
                max_delta_runs: NonZeroUsize::MIN,
                ..GramIndexBuildPolicy::default()
            })
            .await
            .expect_err("a reorganization cannot allocate above the public maximum"),
    ] {
        assert_eq!(error.code(), ErrorCode::IndexCorrupt);
        assert_matches!(
            error,
            GrepError::IndexCorrupt { message } if message.contains("largest run number")
        );
    }

    assert_eq!(
        control::metadata_manifest(&store, &namespace_id)
            .await
            .payload()
            .next_run_no,
        RunNo(MAX_PUBLIC_INTEGER)
    );
    assert_eq!(
        store
            .list_prefix(&metadata_manifest_prefix(&namespace_id))
            .await
            .expect("list manifests after failures"),
        manifests_before
    );
    assert_eq!(
        store
            .list_prefix(&segments_prefix)
            .await
            .expect("list segments after failures"),
        segments_before,
        "the range check must run before writing a segment"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn enable_creates_no_snapshot_when_the_manifest_load_fails() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("enable-manifest-failure").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "enable-manifest-failure-writer",
        ))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let hint_key = loonfs_objectstore::keys::hint(&namespace_id);
    let manifest_loads = Arc::new(AtomicUsize::new(0));
    let observed_manifest_loads = Arc::clone(&manifest_loads);
    let failing_store = Arc::new(FailStore::matching(
        store.clone(),
        move |context: &OperationContext<'_>| {
            context.key() == hint_key
                && OperationSelector::Read.matches(context.kind())
                && observed_manifest_loads.fetch_add(1, Ordering::SeqCst) == 0
        },
        InjectedError::Transport("injected manifest load failure".to_owned()),
    ));
    failing_store.fail_all();
    let failing: SharedObjectStore = failing_store.clone();
    let host = GrepHost::new(&failing, "enable-manifest-failure-maintenance").await;

    let error = host
        .worker
        .enable(&namespace_id)
        .await
        .expect_err("the manifest load fails before snapshot creation");
    assert_eq!(error.code(), ErrorCode::ServerError, "{error:?}");
    assert_eq!(manifest_loads.load(Ordering::SeqCst), 1);

    let mut pager = host.maintenance.list_snapshot_records(&namespace_id);
    let snapshots = pager
        .next()
        .await
        .expect("a fresh pager has one page")
        .expect("list snapshots after failed enable");
    assert!(
        snapshots.snapshots.is_empty(),
        "a failure before manifest publication must not leave an active snapshot"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn enable_confirms_its_snapshot_after_an_ambiguous_manifest_write() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("ambiguous-enable").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "ambiguous-enable-writer",
        ))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let failing_store = Arc::new(
        FailStore::new(
            store.clone(),
            KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("injected failure after manifest publication".to_owned()),
        )
        .apply_then_fail(),
    );
    failing_store.fail_next(1);
    let failing: SharedObjectStore = failing_store.clone();
    let host = GrepHost::new(&failing, "ambiguous-enable-maintenance").await;
    let worker = &host.worker;

    let outcome = worker
        .enable(&namespace_id)
        .await
        .expect("confirm written manifest");
    assert_matches!(outcome, loonfs_grep::GrepEnableOutcome::Enabled { .. });
    assert_eq!(failing_store.attempts(), 1);

    assert_fresh_backfill_attempt(&store, &namespace_id).await;
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn restart_confirms_its_snapshot_after_an_ambiguous_manifest_write() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("ambiguous-restart").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "ambiguous-restart-writer",
        ))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let host = GrepHost::new(&store, "ambiguous-restart-maintenance").await;
    host.worker
        .enable(&namespace_id)
        .await
        .expect("enable grep");
    let previous_snapshot_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;
    host.writer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .delete_snapshot(&previous_snapshot_id)
        .await
        .expect("make the current backfill restart");

    let failing_store = Arc::new(
        FailStore::new(
            store.clone(),
            KeyPredicate::prefix(metadata_manifest_prefix(&namespace_id)),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("injected failure after manifest publication".to_owned()),
        )
        .apply_then_fail(),
    );
    failing_store.fail_next(1);
    let failing: SharedObjectStore = failing_store.clone();
    let restart_host = GrepHost::new(&failing, "ambiguous-restart-steps").await;
    let worker = &restart_host.worker;

    let outcome = worker
        .steps(&namespace_id, 0)
        .build_step(GramIndexBuildPolicy::default())
        .await
        .expect("confirm written restart");
    assert_matches!(outcome, GrepBuildOutcome::BackfillRestarted { .. });
    assert_eq!(failing_store.attempts(), 1);

    let snapshot_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;
    assert_ne!(snapshot_id, previous_snapshot_id);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn retention_gap_and_vanished_snapshot_restart_fresh_backfill() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-gap").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("gap-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let maintenance = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("gap-maintenance"))
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id("gap-maintenance"));
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    namespace
        .put_file(
            "/gap.txt",
            b"retention gap needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write after watermark");
    let retention_floor_seq =
        fold_wal_and_advance_retention(&store, &maintenance, &namespace_id).await;
    assert!(retention_floor_seq > CommitSeq(0));
    let indexed = crate::common::control::grep_entry(&store, &namespace_id)
        .await
        .expect("grep's entry")
        .runs()
        .to_vec();

    // The feed can no longer reach the watermark, which the worker must
    // read as "my basis is gone" and answer with a whole new attempt.
    let restart = worker
        .steps(&namespace_id, 0)
        .build_step(GramIndexBuildPolicy::default())
        .await
        .expect("gap restart");
    assert_matches!(restart, GrepBuildOutcome::BackfillRestarted { .. });
    let gap_snapshot_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;
    assert_eq!(
        crate::common::control::grep_entry(&store, &namespace_id)
            .await
            .expect("grep's entry")
            .runs(),
        indexed.as_slice(),
        "a rebootstrap keeps the index's segments, whose postings stay valid"
    );

    // The second trigger: the snapshot stops keeping its basis
    // mid-backfill. The enumeration says so out loud instead of quietly
    // answering current state, and the worker starts over again.
    namespace
        .delete_snapshot(&gap_snapshot_id)
        .await
        .expect("remove snapshot mid-backfill");
    let vanished = worker
        .steps(&namespace_id, 0)
        .build_step(GramIndexBuildPolicy::default())
        .await
        .expect("vanished snapshot restart");
    assert_matches!(vanished, GrepBuildOutcome::BackfillRestarted { .. });
    // A deleted record is finished for good, so the new attempt takes a
    // snapshot of its own: a new id, keeping its basis, with the walk starting
    // from nothing.
    let fresh_snapshot_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;
    assert_ne!(fresh_snapshot_id, gap_snapshot_id);

    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("query after rebootstrap");
    assert_eq!(response.matches.len(), 1);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn retention_passing_a_backfill_snapshot_never_serves_a_partial_query() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-handoff-gap").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("handoff-gap-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let maintenance = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "handoff-gap-maintenance",
        ))
        .build()
        .await
        .expect("maintenance")
        .maintenance(loonfs_test_support::ids::writer_id(
            "handoff-gap-maintenance",
        ));
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for index in 0..2u32 {
        namespace
            .put_file(
                &format!("/before-{index}.txt"),
                format!("handoff needle before {index}\n").as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("write a file the snapshot captures");
    }

    // The maintenance handle is a separate runtime. The feed reads the
    // retention floor from the manifest of the reader's snapshot, so the worker must
    // revalidate on every read to observe the advance within this test.
    let worker = GrepHost::with_reader(&store, "grep-worker-tests", |builder| {
        builder.config(loonfs::RuntimeConfig {
            manifest_revalidation_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
    })
    .await
    .worker;
    worker.enable(&namespace_id).await.expect("enable");
    let captured_seq = match worker
        .lifecycle(&namespace_id)
        .await
        .expect("backfill lifecycle")
    {
        GrepIndexStatus::Backfilling { captured_seq, .. } => captured_seq,
        status => panic!("newly enabled grep must be backfilling, got {status:?}"),
    };
    let policy = GramIndexBuildPolicy {
        max_files_per_step: NonZeroUsize::MIN,
        ..GramIndexBuildPolicy::default()
    };
    let first = worker
        .steps(&namespace_id, 0)
        .build_step(policy)
        .await
        .expect("first backfill page");
    assert_matches!(
        first,
        GrepBuildOutcome::Indexed {
            indexed_revisions: 1,
            ..
        }
    );

    namespace
        .put_file(
            "/during.txt",
            b"handoff needle during\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write during backfill");
    let retention_floor_seq =
        fold_wal_and_advance_retention(&store, &maintenance, &namespace_id).await;
    assert!(retention_floor_seq > captured_seq);

    // Snapshot pages remain readable after retention passes their basis,
    // so the final page may still publish the snapshot watermark as active.
    // The change-feed boundary is authoritative: a query at that watermark
    // must fail explicitly instead of treating the missing tail as empty.
    let completed = worker
        .steps(&namespace_id, 0)
        .build_step(policy)
        .await
        .expect("final backfill page");
    assert_matches!(
        completed,
        GrepBuildOutcome::Indexed {
            built_through_seq,
            ..
        } if built_through_seq == captured_seq
    );
    let error = new_query(&store, &namespace_id, &request("handoff needle"))
        .await
        .expect_err("an expired handoff cursor cannot answer a query");
    assert_eq!(error.code(), ErrorCode::RebootstrapRequired);

    let restarted = worker
        .steps(&namespace_id, 0)
        .build_step(policy)
        .await
        .expect("restart expired handoff");
    assert_matches!(restarted, GrepBuildOutcome::BackfillRestarted { .. });
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    let response = new_query(&store, &namespace_id, &request("handoff needle"))
        .await
        .expect("query after fresh backfill");
    assert_eq!(
        matched_paths(&response)
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "/before-0.txt".to_owned(),
            "/before-1.txt".to_owned(),
            "/during.txt".to_owned(),
        ])
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a backfill whose snapshot expired restarts from a fresh
/// snapshot, deletes the expired one, and builds an index that serves the
/// namespace's files.
#[tokio::test]
async fn an_expired_backfill_snapshot_restarts_the_backfill_from_a_fresh_snapshot() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-expiry").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("expiry-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace
        .put_file(
            "/expiring.txt",
            b"expiring needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    let snapshot_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;

    // Expire the snapshot under the backfill without deleting it.
    let key = snapshot_record(&namespace_id, &snapshot_id);
    let mut record = control::snapshot_record(&store, &namespace_id, &snapshot_id)
        .await
        .expect("backfill snapshot");
    assert!(
        record.owner.expires_at_ms().is_some(),
        "the backfill snapshot carries a ttl"
    );
    let SnapshotOwner::User { expires_at_ms, .. } = &mut record.owner else {
        panic!("the backfill snapshot is user-owned");
    };
    *expires_at_ms = Some(record.created_at_ms);
    let expired = record;
    replace_object(
        &store,
        &key,
        Bytes::from(
            loonfs_types::format::control::encode_control_state(
                loonfs_types::format::control::ControlObjectKind::Snapshot,
                &expired,
            )
            .expect("encode record"),
        ),
    )
    .await
    .expect("write the expired record");

    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    assert!(
        control::snapshot_record(&store, &namespace_id, &snapshot_id)
            .await
            .is_none()
    );
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("query after a backfill on an expired snapshot");
    assert_eq!(response.matches.len(), 1);
    writer.shutdown().await.expect("shutdown");
}

/// Asserts the namespace's grep entry is at the start of a backfill attempt
/// — nothing walked, an active snapshot keeping its basis — and returns
/// the snapshot that attempt holds.
async fn assert_fresh_backfill_attempt(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> loonfs_types::SnapshotId {
    let manifest = crate::common::control::grep_entry(store, namespace_id)
        .await
        .expect("grep entry exists");
    let GrepIndexStatus::Backfilling {
        cursor_inode_id,
        snapshot_id,
        ..
    } = manifest.status()
    else {
        panic!("expected a backfill over a snapshot: {:?}", manifest);
    };
    assert_eq!(
        *cursor_inode_id, None,
        "a fresh backfill starts the walk from the beginning"
    );
    assert!(control::snapshot_record(store, namespace_id, snapshot_id)
        .await
        .is_some());
    snapshot_id.clone()
}

/// Every grep segment the namespace's manifest names, for asserting that an
/// event changed no postings.
async fn grep_segment_ids(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> BTreeSet<IndexSegmentId> {
    crate::common::control::grep_entry(store, namespace_id)
        .await
        .expect("grep entry exists")
        .segments()
        .map(|segment| segment.segment_id.clone())
        .collect()
}

async fn grep_built_through_seq(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> CommitSeq {
    crate::common::control::grep_entry(store, namespace_id)
        .await
        .expect("grep entry exists")
        .status()
        .active_watermark()
        .expect("an active grep manifest has a watermark")
        .built_through_seq()
}

/// Returns the paths of a response's matches in path order. A response lists
/// its matches in inode ID order, and a test doesn't rely on how LoonFS orders
/// inode IDs, so it compares the paths in path order.
fn matched_paths(response: &GrepResponse) -> Vec<String> {
    let mut paths: Vec<String> = response
        .matches
        .iter()
        .map(|found| found.path.as_str().to_owned())
        .collect();
    paths.sort();
    paths
}

#[tokio::test]
async fn commits_during_backfill_are_indexed_once_by_the_feed_phase() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("backfill-overlap").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("overlap-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for index in 0..4u32 {
        namespace
            .put_file(
                &format!("/before-{index}.txt"),
                format!("overlap needle before {index}\n").as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("write preexisting file");
    }

    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    // One file per step, so the backfill is genuinely partway through the
    // snapshot's file set when the commits below are made.
    let policy = GramIndexBuildPolicy {
        max_files_per_step: NonZeroUsize::MIN,
        ..GramIndexBuildPolicy::default()
    };
    let first = worker
        .steps(&namespace_id, 0)
        .build_step(policy)
        .await
        .expect("first backfill page");
    assert_matches!(
        first,
        GrepBuildOutcome::Indexed {
            indexed_revisions: 1,
            ..
        },
        "one file per step must leave the backfill unfinished: {:?}",
        first
    );

    // Commits strictly after the snapshot's sequence: one new file, and a
    // replacement of a file the snapshot already captured.
    namespace
        .put_file(
            "/during.txt",
            b"overlap needle during\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write during backfill");
    namespace
        .put_file_with_options(
            "/before-0.txt",
            b"overlap needle replaced\n",
            &loonfs_test_support::test_actor(),
            &PutFileOptions {
                behavior: loonfs::DestinationBehavior::Replace,
                ..Default::default()
            },
        )
        .await
        .expect("replace a file the snapshot captured during backfill");

    drive_worker_to_current(&worker, &namespace_id, policy).await;

    let response = new_query(&store, &namespace_id, &request("overlap needle"))
        .await
        .expect("query after backfill and catch-up");
    let paths = matched_paths(&response);
    assert_eq!(
        paths,
        vec![
            "/before-0.txt",
            "/before-1.txt",
            "/before-2.txt",
            "/before-3.txt",
            "/during.txt",
        ],
        "every file matches exactly once across both phases"
    );

    let replaced = new_query(&store, &namespace_id, &request("needle replaced"))
        .await
        .expect("query the replacing revision");
    assert_eq!(matched_paths(&replaced), vec!["/before-0.txt"]);
    let superseded = new_query(&store, &namespace_id, &request("needle before 0"))
        .await
        .expect("query the superseded revision");
    assert!(
        superseded.matches.is_empty(),
        "a revision the snapshot kept but the feed replaced must not match"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_move_reindexes_nothing_and_answers_the_new_path() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("move-no-reindex").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("move-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace
        .put_file(
            "/docs/note.txt",
            b"moved needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write file");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let segments_before = grep_segment_ids(&store, &namespace_id).await;
    let built_before = grep_built_through_seq(&store, &namespace_id).await;

    let moved = namespace
        .move_path(
            "/docs/note.txt",
            "/docs/renamed.txt",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("move the indexed file");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    assert_eq!(
        grep_segment_ids(&store, &namespace_id).await,
        segments_before,
        "a move must write no new postings"
    );
    let built_after = grep_built_through_seq(&store, &namespace_id).await;
    assert!(
        built_after > built_before && built_after >= moved.committed_seq,
        "the watermark still advances past a move: {built_before:?} -> {built_after:?}"
    );
    let response = new_query(&store, &namespace_id, &request("moved needle"))
        .await
        .expect("query after the move");
    assert_eq!(matched_paths(&response), vec!["/docs/renamed.txt"]);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_recursive_delete_hides_matches_and_a_restore_brings_them_back() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("delete-restore").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("delete-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let reader = writer.read_only();
    let namespace = reader.namespace(&namespace_id);
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for name in ["a", "b"] {
        namespace_writer
            .put_file(
                &format!("/docs/{name}.txt"),
                format!("subtree needle {name}\n").as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("write file");
    }
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let segments_before = grep_segment_ids(&store, &namespace_id).await;
    let docs_inode_id = namespace
        .stat("/docs")
        .await
        .expect("stat the directory")
        .inode_id;
    assert_eq!(
        new_query(&store, &namespace_id, &request("subtree needle"))
            .await
            .expect("query before the delete")
            .matches
            .len(),
        2
    );

    let deleted = namespace_writer
        .delete_path_with_options(
            "/docs",
            &loonfs_test_support::test_actor(),
            &loonfs::DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                ..Default::default()
            },
        )
        .await
        .expect("delete the subtree");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let hidden = new_query(&store, &namespace_id, &request("subtree needle"))
        .await
        .expect("query after the delete");
    assert!(
        hidden.matches.is_empty(),
        "a deleted subtree's files must verify away: {:?}",
        matched_paths(&hidden)
    );
    assert_eq!(
        grep_segment_ids(&store, &namespace_id).await,
        segments_before,
        "a delete must write no new postings"
    );

    namespace_writer
        .restore_deleted_with_options(
            docs_inode_id,
            deleted.committed_seq,
            &loonfs_test_support::test_actor(),
            &RestoreDeletedOptions {
                destination: RestoreDeletedDestination::Path(
                    AbsolutePath::parse("/docs").expect("valid destination path"),
                ),
                ..Default::default()
            },
        )
        .await
        .expect("restore the subtree");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let restored = new_query(&store, &namespace_id, &request("subtree needle"))
        .await
        .expect("query after the restore");
    assert_eq!(matched_paths(&restored), vec!["/docs/a.txt", "/docs/b.txt"]);
    assert_eq!(
        grep_segment_ids(&store, &namespace_id).await,
        segments_before,
        "a restore must write no new postings"
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn restoring_a_subtree_deleted_before_backfill_needs_no_rebuild() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("restore-after-backfill").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "restore-backfill-writer",
        ))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let reader = writer.read_only();
    let namespace = reader.namespace(&namespace_id);
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for name in ["a", "b"] {
        namespace_writer
            .put_file(
                &format!("/docs/{name}.txt"),
                format!("restored needle {name}\n").as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("write file before delete");
    }
    let docs_inode_id = namespace
        .stat("/docs")
        .await
        .expect("stat docs before delete")
        .inode_id;
    let deleted = namespace_writer
        .delete_path_with_options(
            "/docs",
            &loonfs_test_support::test_actor(),
            &loonfs::DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                ..Default::default()
            },
        )
        .await
        .expect("delete subtree before backfill");

    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    assert!(
        new_query(&store, &namespace_id, &request("restored needle"))
            .await
            .expect("query hidden tree")
            .matches
            .is_empty()
    );
    let segments_before = grep_segment_ids(&store, &namespace_id).await;

    let restored_deleted = namespace_writer
        .restore_deleted_with_options(
            docs_inode_id,
            deleted.committed_seq,
            &loonfs_test_support::test_actor(),
            &RestoreDeletedOptions {
                destination: RestoreDeletedDestination::Path(
                    AbsolutePath::parse("/docs").expect("valid destination path"),
                ),
                ..Default::default()
            },
        )
        .await
        .expect("restore subtree after backfill");

    let restored = new_query(&store, &namespace_id, &request("restored needle"))
        .await
        .expect("an exact query across the restore");
    assert!(restored.tail_scanned);
    assert_eq!(matched_paths(&restored), vec!["/docs/a.txt", "/docs/b.txt"]);

    let step = worker
        .steps(&namespace_id, 0)
        .build_step(GramIndexBuildPolicy::default())
        .await
        .expect("build step after restore");
    assert_matches!(
        step,
        GrepBuildOutcome::Indexed {
            segments_written: 0,
            ..
        },
        "a restore only advances the watermark: {step:?}"
    );
    assert_eq!(
        grep_built_through_seq(&store, &namespace_id).await,
        restored_deleted.committed_seq
    );
    assert_eq!(
        grep_segment_ids(&store, &namespace_id).await,
        segments_before
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_failing_worker_step_never_blocks_a_concurrent_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-isolation").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("isolation-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let reader = writer.read_only();
    let namespace = reader.namespace(&namespace_id);
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let host = GrepHost::new(&store, "worker-isolation").await;
    let worker = &host.worker;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    publish_entry_of_another_version(&host, &namespace_id).await;

    let actor = loonfs_test_support::test_actor();
    let (build, commit) = tokio::join!(
        async {
            worker
                .steps(&namespace_id, 0)
                .build_step(GramIndexBuildPolicy::default())
                .await
        },
        namespace_writer.put_file("/during-failure.txt", b"isolated needle\n", &actor),
    );
    let error = build.expect_err("an unreadable grep entry fails the step");
    assert_eq!(error.code(), ErrorCode::IndexCorrupt);
    commit.expect("the filesystem commit is unaffected by grep");
    let read = namespace
        .read_file("/during-failure.txt")
        .await
        .expect("the committed file is readable");
    assert_eq!(read.bytes, b"isolated needle\n");
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn grep_entry_lifecycle_pins_not_materialized_error_surface() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("error-surface").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("error-writer"))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let host = GrepHost::new(&store, "error-surface").await;
    let worker = &host.worker;

    assert_not_enabled_error(
        "never enabled",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    worker.enable(&namespace_id).await.expect("enable");
    assert_backfilling_error(
        "backfilling",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    worker.disable(&namespace_id).await.expect("disable");
    assert_not_enabled_error(
        "disabled",
        new_query(&store, &namespace_id, &request("needle")).await,
    );

    publish_edited_entry(&host, &namespace_id, |entry| {
        entry["status"]["field_from_the_future"] = serde_json::Value::from(true);
    })
    .await;
    assert_corrupt_index_error(
        "an entry with a member grep doesn't define",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    publish_entry_of_another_version(&host, &namespace_id).await;
    assert_unsupported_version_error(
        "an entry of another version",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_backfill_snapshot_mismatch_is_corruption_without_writes() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("snapshot-mismatch").expect("namespace");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "snapshot-mismatch-writer",
        ))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create");
    let host = GrepHost::new(&store, "snapshot-mismatch").await;
    let worker = &host.worker;
    worker.enable(&namespace_id).await.expect("enable");
    publish_edited_entry(&host, &namespace_id, |entry| {
        entry["status"]["captured_seq"] = serde_json::Value::from(1);
    })
    .await;
    recording.reset();
    let error = worker
        .steps(&namespace_id, 0)
        .build_step(GramIndexBuildPolicy::default())
        .await
        .expect_err("mismatch");
    assert_matches!(error, GrepError::IndexCorrupt { .. });
    assert_eq!(recording.counts().puts, 0);
    assert_eq!(recording.counts().deletes, 0);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_backfilling_entry_without_a_snapshot_id_is_index_corrupt() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("missing-backfill-snapshot").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "missing-snapshot-writer",
        ))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let host = GrepHost::new(&store, "missing-snapshot").await;
    host.worker
        .enable(&namespace_id)
        .await
        .expect("enable grep");
    publish_edited_entry(&host, &namespace_id, |entry| {
        entry["status"]
            .as_object_mut()
            .expect("backfilling status is an object")
            .remove("snapshot_id")
            .expect("a backfilling entry has a snapshot id");
    })
    .await;

    assert_corrupt_index_error(
        "backfilling entry missing snapshot id",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that disabling grep replaces an invalid entry of grep's version,
/// which enabling grep refuses, so a query then reports grep as not enabled
/// and enabling grep starts a new backfill.
#[tokio::test]
async fn disable_resets_an_invalid_entry_and_enable_starts_a_new_backfill() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("disable-invalid-entry").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("invalid-entry-writer"))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    writer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .put_file(
            "/reset.txt",
            b"reset needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write a file");
    let host = GrepHost::new(&store, "invalid-entry").await;
    let worker = &host.worker;
    worker.enable(&namespace_id).await.expect("enable");

    type EntryEdit = fn(&mut serde_json::Value);
    let cases: [(&str, EntryEdit); 2] = [
        (
            "a backfilling entry with a member grep doesn't define",
            |entry| {
                entry["status"]["field_from_the_future"] = serde_json::Value::from(true);
            },
        ),
        ("a disabled entry that lists runs", |entry| {
            entry["status"] = serde_json::json!({ "kind": "disabled" });
        }),
    ];
    for (case, edit) in cases {
        let before = control::grep_entry(&store, &namespace_id)
            .await
            .expect("grep's entry");
        publish_edited_entry(&host, &namespace_id, edit).await;
        let refused = worker.enable(&namespace_id).await.expect_err(case);
        assert_matches!(refused, GrepError::IndexCorrupt { .. }, "{case}");
        assert!(
            refused.to_string().contains("disable and re-enable grep"),
            "{case}: {refused}"
        );

        assert_eq!(
            worker.disable(&namespace_id).await.expect(case),
            loonfs_grep::GrepDisableOutcome::Disabled,
            "{case}"
        );
        let disabled = control::grep_entry(&store, &namespace_id)
            .await
            .expect("grep's entry");
        assert_matches!(disabled.status(), GrepIndexStatus::Disabled {}, "{case}");
        assert!(disabled.runs().is_empty(), "{case}");
        if let GrepIndexStatus::Backfilling { snapshot_id, .. } = before.status() {
            assert!(
                control::snapshot_record(&store, &namespace_id, snapshot_id)
                    .await
                    .is_some(),
                "the invalid entry's backfill snapshot is left to expire"
            );
        }
        assert_not_enabled_error(
            case,
            new_query(&store, &namespace_id, &request("needle")).await,
        );

        let enabled = worker.enable(&namespace_id).await.expect(case);
        assert_matches!(enabled, loonfs_grep::GrepEnableOutcome::Enabled { .. });
        let backfill = control::grep_entry(&store, &namespace_id)
            .await
            .expect("grep's entry");
        assert_matches!(backfill.status(), GrepIndexStatus::Backfilling { .. });
        drive_worker_to_current(worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    }
    writer.shutdown().await.expect("shutdown");
}

/// Checks that enabling and disabling grep both refuse an entry of another
/// version and leave the manifest unchanged, so a build never discards an
/// index that another version wrote.
#[tokio::test]
async fn enable_and_disable_refuse_an_entry_of_another_version_and_leave_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("another-version").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id(
            "another-version-writer",
        ))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    writer
        .open_namespace(&namespace_id)
        .expect("open namespace")
        .put_file(
            "/kept.txt",
            b"kept needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write a file");
    let host = GrepHost::new(&store, "another-version").await;
    let worker = &host.worker;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    publish_entry_of_another_version(&host, &namespace_id).await;
    let before = control::metadata_manifest(&store, &namespace_id).await;
    assert!(
        !before.payload().extensions[&grep_extension_name()]
            .runs
            .is_empty(),
        "the other version's entry lists the index's runs"
    );

    assert_unsupported_version_error("enable", worker.enable(&namespace_id).await);
    assert_unsupported_version_error("disable", worker.disable(&namespace_id).await);
    assert_eq!(
        control::metadata_manifest(&store, &namespace_id)
            .await
            .payload(),
        before.payload(),
        "neither refusal publishes a manifest"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Publishes the current grep entry after `edit` changes its JSON, whatever
/// grep would make of it.
async fn publish_edited_entry(
    host: &GrepHost,
    namespace_id: &NamespaceId,
    edit: impl FnOnce(&mut serde_json::Value),
) {
    let manifest = control::metadata_manifest(&host.store, namespace_id).await;
    let current = manifest
        .payload()
        .extensions
        .get(&grep_extension_name())
        .expect("an enabled namespace has grep's entry");
    let mut document = serde_json::to_value(current).expect("encode the entry");
    edit(&mut document);
    let entry: ExtensionEntry = serde_json::from_value(document).expect("core decodes the entry");
    let next_run_no = manifest.payload().next_run_no;
    crate::common::publish_grep_entry(&host.maintenance, namespace_id, entry, next_run_no).await;
}

/// Publishes grep's entry with a version this build doesn't read.
async fn publish_entry_of_another_version(host: &GrepHost, namespace_id: &NamespaceId) {
    publish_edited_entry(host, namespace_id, |entry| {
        entry["version"] = serde_json::Value::from(3);
    })
    .await;
}

#[tokio::test]
async fn planless_scan_covers_wal_revisions_at_or_below_index_watermark() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("scan-gap").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("scan-gap-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    namespace
        .put_file(
            "/only-in-wal.txt",
            b"x\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write WAL-only file");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let head = control::head(&store, &namespace_id).await;
    let metadata_manifest = control::metadata_manifest(&store, &namespace_id).await;
    assert!(
        metadata_manifest.manifest().head_seq < head.seq,
        "the WAL-only revision must sit past metadata materialization"
    );
    let grep_manifest = crate::common::control::grep_entry(&store, &namespace_id)
        .await
        .expect("grep entry exists");
    assert_eq!(
        grep_manifest
            .status()
            .active_watermark()
            .map(|resume| (resume.built_through_seq(), resume.next_event_index())),
        Some((head.seq, 0)),
        "the independent worker can advance past metadata materialization"
    );

    let mut scan = request("x");
    scan.allow_scan = true;
    let response = new_query(&store, &namespace_id, &scan)
        .await
        .expect("plan-less scan");
    assert_eq!(
        response.matches.len(),
        1,
        "scan must cover the WAL-only revision"
    );
    assert_eq!(response.matches[0].path.as_str(), "/only-in-wal.txt");

    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn grep_serves_a_match_under_a_derived_path_over_the_byte_limit() {
    use loonfs::publish::{CommitIntent, FilesystemOperation};
    use loonfs::{CommitId, FilesystemEvent};
    use loonfs_types::{DisplayName, MAX_PATH_BYTES, ROOT_INODE_ID};

    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("deep-scan").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("deep-scan-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let commit = |operation| {
        CommitIntent::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            operation,
        )
    };

    let name = |level: usize| format!("{level:02}{}", "n".repeat(253));
    let mut directory = ROOT_INODE_ID;
    let mut directory_path = String::new();
    for level in 0..20 {
        let created = namespace
            .commit(commit(FilesystemOperation::CreateDirectoryByInode {
                parent_inode_id: directory,
                display_name: DisplayName::parse(name(level)).expect("display name"),
            }))
            .await
            .expect("create a directory by inode");
        directory = match created.events.as_slice() {
            [FilesystemEvent::DirectoryCreated { inode_id, .. }] => *inode_id,
            other => panic!("expected one created directory, got {other:?}"),
        };
        directory_path = format!("{directory_path}/{}", name(level));
    }
    assert!(directory_path.len() > MAX_PATH_BYTES);
    let content = namespace
        .prepare_content(b"a tree\n")
        .await
        .expect("prepare content");
    namespace
        .commit_prepared(
            commit(FilesystemOperation::CreateFileByInode {
                parent_inode_id: directory,
                display_name: DisplayName::parse("leaf.txt").expect("display name"),
                content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
            }),
            vec![content],
        )
        .await
        .expect("create a file by inode");
    let host = GrepHost::new(&store, "deep-scan").await;
    host.enable_grep_index(&namespace_id).await.expect("enable");

    // "ee" has no trigram, so it scans the directory tree; "tree" uses the index.
    let mut unscoped_scan = request("ee");
    unscoped_scan.allow_scan = true;
    let mut inode_scan = unscoped_scan.clone();
    inode_scan.scope = GrepScope::Inode(directory);
    let mut path_scan = unscoped_scan.clone();
    path_scan.scope =
        GrepScope::PathPrefix(AbsolutePath::parse(format!("/{}", name(0))).expect("scope path"));
    let file_path = format!("{directory_path}/leaf.txt");
    for (case, grep_request) in [
        ("unscoped scan", unscoped_scan),
        ("inode-scoped scan", inode_scan),
        ("path-scoped scan", path_scan),
        ("indexed query", request("tree")),
    ] {
        let response = host
            .grep(&namespace_id, &grep_request, first_page())
            .await
            .unwrap_or_else(|error| panic!("{case}: {error}"));
        assert_eq!(
            response
                .matches
                .iter()
                .map(|found| found.path.as_str())
                .collect::<Vec<_>>(),
            [file_path.as_str()],
            "{case}"
        );
    }

    writer.shutdown().await.expect("shutdown");
}

fn assert_not_enabled_error(case: &str, result: loonfs_grep::Result<GrepResponse>) {
    match result {
        Err(error @ GrepError::NotEnabled) => {
            assert_eq!(error.code(), ErrorCode::NotSupported, "code for {case}");
            assert_eq!(
                error.to_string(),
                "feature `query.grep` is not enabled on this namespace",
                "error text for {case}"
            );
        }
        outcome => panic!("expected not-enabled error for {case}, got {outcome:?}"),
    }
}

fn assert_backfilling_error(case: &str, result: loonfs_grep::Result<GrepResponse>) {
    match result {
        Err(error @ GrepError::Backfilling) => {
            assert_eq!(error.code(), ErrorCode::NotSupported, "code for {case}");
            assert_eq!(
                error.to_string(),
                "feature `query.grep` is enabled but its backfill has not completed on this \
                 namespace",
                "error text for {case}"
            );
        }
        outcome => panic!("expected backfilling error for {case}, got {outcome:?}"),
    }
}

fn assert_corrupt_index_error(case: &str, result: loonfs_grep::Result<GrepResponse>) {
    match result {
        Err(error @ GrepError::IndexCorrupt { .. }) => {
            assert_eq!(error.code(), ErrorCode::IndexCorrupt, "code for {case}");
            assert!(
                error
                    .to_string()
                    .contains("disable and re-enable grep to rebuild it"),
                "error text for {case}: {error}"
            );
        }
        outcome => panic!("expected corrupt-index error for {case}, got {outcome:?}"),
    }
}

fn assert_unsupported_version_error<T: std::fmt::Debug>(
    case: &str,
    result: loonfs_grep::Result<T>,
) {
    match result {
        Err(error @ GrepError::UnsupportedVersion { version: 3 }) => {
            assert_eq!(error.code(), ErrorCode::IndexCorrupt, "code for {case}");
            assert_eq!(
                error.to_string(),
                "grep index has format version `3`, and this build reads version `2`; query or \
                 disable the index with a build that reads version `3`",
                "error text for {case}"
            );
        }
        outcome => panic!("expected unsupported-version error for {case}, got {outcome:?}"),
    }
}

#[tokio::test]
async fn grep_worker_pins_reorganized_tail_and_pagination_results() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-results").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("worker-results-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let worker = worker(&store).await;
    let policy = GramIndexBuildPolicy {
        max_delta_runs: nonzero_usize(2),
        max_mid_runs: nonzero_usize(2),
        ..GramIndexBuildPolicy::default()
    };
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    for round in 0..6u32 {
        namespace
            .put_file(
                &format!("/docs/file-{round}.txt"),
                format!("shared needle {round}\nshared needle again {round}\n").as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("write indexed file");
        worker
            .steps(&namespace_id, 0)
            .build_step(policy)
            .await
            .expect("new build");
        worker
            .steps(&namespace_id, 0)
            .reorganize_step(policy)
            .await
            .expect("new reorganization");
    }
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    namespace
        .put_file(
            "/tail.txt",
            b"tail-only needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write tail file");

    let manifest = crate::common::control::grep_entry(&store, &namespace_id)
        .await
        .expect("manifest exists");
    let levels: BTreeSet<u32> = manifest.runs().iter().map(|run| run.level).collect();
    assert!(
        levels.contains(&1) && levels.contains(&2),
        "levels: {levels:?}"
    );

    let shared = new_query(&store, &namespace_id, &request("shared needle"))
        .await
        .expect("shared query");
    assert_eq!(shared.namespace_id, namespace_id);
    assert_eq!(shared.matches.len(), 12);
    assert!(shared.tail_scanned);
    assert!(shared.built_through_seq < shared.head_seq);
    assert!(shared.next_cursor.is_none());
    for found in &shared.matches {
        assert!(found.path.starts_with("/docs/file-"));
        assert_matches!(found.line_number, 1 | 2);
        assert_eq!(
            found.byte_offset,
            if found.line_number == 1 { 0 } else { 16 }
        );
        assert!(found.line.starts_with("shared needle"));
        assert!(!found.line_truncated);
    }

    let tail = new_query(&store, &namespace_id, &request("tail-only needle"))
        .await
        .expect("tail query");
    assert_eq!(tail.matches.len(), 1);
    assert_eq!(tail.matches[0].path, "/tail.txt");
    assert_eq!(tail.matches[0].line_number, 1);
    assert_eq!(tail.matches[0].byte_offset, 0);
    assert_eq!(tail.matches[0].line, "tail-only needle");
    assert!(tail.tail_scanned);
    assert!(tail.next_cursor.is_none());

    let absent = new_query(&store, &namespace_id, &request("absent needle"))
        .await
        .expect("absent query");
    assert!(absent.matches.is_empty());
    assert!(absent.next_cursor.is_none());

    let mut missing_scope = request("shared needle");
    missing_scope.scope =
        GrepScope::PathPrefix(AbsolutePath::parse("/missing").expect("scope path"));
    let error = new_query(&store, &namespace_id, &missing_scope)
        .await
        .expect_err("a missing scope must remain a missing path");
    assert_eq!(error.code(), ErrorCode::PathNotFound);

    let docs_inode_id = namespace.stat("/docs").await.expect("stat docs").inode_id;
    let mut inode_scope = request("needle");
    inode_scope.scope = GrepScope::Inode(docs_inode_id);
    let mut path_scope = request("needle");
    path_scope.scope = GrepScope::PathPrefix(AbsolutePath::parse("/docs").expect("scope path"));
    let scoped_by_inode = new_query(&store, &namespace_id, &inode_scope)
        .await
        .expect("inode scope query");
    assert_eq!(scoped_by_inode.matches.len(), 12);
    assert_eq!(
        scoped_by_inode.matches,
        new_query(&store, &namespace_id, &path_scope)
            .await
            .expect("path scope query")
            .matches
    );
    inode_scope.scope = GrepScope::Inode(loonfs_types::InodeId::generate(
        CommitSeq(1_000),
        NonZeroU32::MIN,
    ));
    let error = new_query(&store, &namespace_id, &inode_scope)
        .await
        .expect_err("a missing inode scope must remain a missing inode");
    assert_eq!(error.code(), ErrorCode::InodeNotFound);

    let shared_needle = request("shared needle");
    let mut found_matches = BTreeSet::new();
    let mut cursors = BTreeSet::new();
    let mut cursor = None;
    loop {
        let page = new_query_page(
            &store,
            &namespace_id,
            &shared_needle,
            page_request(cursor, 1),
        )
        .await
        .expect("query page");
        assert_eq!(page.namespace_id, namespace_id);
        assert_eq!(page.matches.len(), 1);
        let found = &page.matches[0];
        assert!(found_matches.insert((found.path.clone(), found.line_number)));
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        assert!(cursors.insert(next_cursor.clone()));
        cursor = Some(next_cursor);
    }
    assert_eq!(found_matches.len(), 12);
    assert_eq!(cursors.len(), 11);
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn snapshot_backfill_matches_incremental_worker_results() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let backfill_namespace = NamespaceId::parse("equiv-backfill").expect("namespace id");
    let incremental_namespace = NamespaceId::parse("equiv-incremental").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("equiv-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    for namespace_id in [&backfill_namespace, &incremental_namespace] {
        writer
            .create_namespace(namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
    }
    let worker = worker(&store).await;
    worker
        .enable(&incremental_namespace)
        .await
        .expect("enable incremental");
    drive_worker_to_current(
        &worker,
        &incremental_namespace,
        GramIndexBuildPolicy::default(),
    )
    .await;

    for index in 0..5u32 {
        for namespace_id in [&backfill_namespace, &incremental_namespace] {
            let namespace = writer.open_namespace(namespace_id).expect("open namespace");
            namespace
                .put_file(
                    &format!("/file-{index}.txt"),
                    format!("equivalence needle {index}\n").as_bytes(),
                    &loonfs_test_support::test_actor(),
                )
                .await
                .expect("write file");
        }
        drive_worker_to_current(
            &worker,
            &incremental_namespace,
            GramIndexBuildPolicy::default(),
        )
        .await;
    }
    worker
        .enable(&backfill_namespace)
        .await
        .expect("enable backfill");
    drive_worker_to_current(
        &worker,
        &backfill_namespace,
        GramIndexBuildPolicy {
            max_files_per_step: nonzero_usize(2),
            ..GramIndexBuildPolicy::default()
        },
    )
    .await;

    let backfill = new_query(&store, &backfill_namespace, &request("needle"))
        .await
        .expect("backfill query");
    let incremental = new_query(&store, &incremental_namespace, &request("needle"))
        .await
        .expect("incremental query");
    assert_eq!(
        normalize_namespace(incremental),
        normalize_namespace(backfill)
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_backfilling_manifest_never_reports_a_built_through_sequence() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("enable-honesty").expect("namespace id");
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("enable-honesty-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace
        .put_file(
            "/note.txt",
            b"honest needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write file");
    let worker = worker(&store).await;

    let loonfs_grep::GrepEnableOutcome::Enabled { state } =
        worker.enable(&namespace_id).await.expect("enable")
    else {
        panic!("a fresh enable publishes a backfill");
    };
    let backfilling = loonfs_types::GrepIndexStatus::from(&state);
    assert_eq!(
        backfilling,
        loonfs_types::GrepIndexStatus::Backfilling {
            captured_seq: CommitSeq(1),
            cursor_inode_id: None,
            snapshot_id: match &state {
                GrepIndexStatus::Backfilling { snapshot_id, .. } => snapshot_id.clone(),
                other => panic!("expected a backfill: {other:?}"),
            },
        }
    );
    let rendered = serde_json::to_string(&backfilling).expect("serialize the reported lifecycle");
    assert!(
        !rendered.contains("built_through_seq"),
        "a backfill must not report a watermark anywhere: {rendered}"
    );
    assert_eq!(
        worker
            .lifecycle(&namespace_id)
            .await
            .expect("read lifecycle"),
        state,
        "the enable response reports the lifecycle it published"
    );

    // Re-enabling an active manifest reports the same phase, still without a
    // watermark.
    let loonfs_grep::GrepEnableOutcome::AlreadyEnabled { state: again } = worker
        .enable(&namespace_id)
        .await
        .expect("idempotent enable")
    else {
        panic!("re-enabling an active manifest reports it as already enabled");
    };
    assert_eq!(again, state);

    // Once the walk finishes, the API reports the manifest as active with the
    // target it reached as its own watermark, and no target field survives.
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let active = loonfs_types::GrepIndexStatus::from(
        &worker
            .lifecycle(&namespace_id)
            .await
            .expect("read completed lifecycle"),
    );
    assert_eq!(
        active,
        loonfs_types::GrepIndexStatus::Active {
            built_through_seq: CommitSeq(1),
            next_event_index: 0,
        }
    );
    assert!(!serde_json::to_string(&active)
        .expect("serialize the active API lifecycle")
        .contains("captured_seq"));
    writer.shutdown().await.expect("shutdown");
}

/// Checks that enabling and disabling write no grep object, and that a query
/// reads grep's entry from its view's manifest, so a warm query of an index
/// with no segments reads nothing under grep's prefix.
#[tokio::test]
async fn enable_disable_and_warm_queries_read_and_write_no_grep_object() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("entry-query").expect("namespace");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::extension_prefix(
            &namespace_id,
            &grep_extension_name(),
        )),
    ));
    let store: SharedObjectStore = recording.clone();
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("entry-query-tests"))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    let host = GrepHost::new(&store, "entry-query").await;
    host.worker.enable(&namespace_id).await.expect("enable");
    host.worker
        .steps(&namespace_id, 0)
        .build_step(GramIndexBuildPolicy::default())
        .await
        .expect("empty backfill");
    let grep_request = request("needle");
    for query in ["cold", "warm"] {
        grep_with(
            &host.service,
            &host.reader,
            &store,
            &namespace_id,
            &grep_request,
            first_page(),
        )
        .await
        .expect(query);
    }
    host.worker.disable(&namespace_id).await.expect("disable");
    assert_eq!(recording.take(), Vec::new());
    assert_matches!(
        grep_with(
            &host.service,
            &host.reader,
            &store,
            &namespace_id,
            &grep_request,
            first_page()
        )
        .await,
        Err(GrepError::NotEnabled)
    );
    writer.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn grep_filters_candidates_the_subject_cannot_read() {
    use loonfs::publish::{CommitIntent, FilesystemOperation};
    use loonfs_types::{
        AccessGrants, AccessRight, AccessRights, CommitId, NamespaceAccess, PrincipalId,
        PrincipalScope, PrincipalSet, Subject, SubjectId,
    };
    let subject = |principal: &str| Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse(principal).expect("subject"),
        principals: PrincipalSet::new(std::collections::BTreeSet::from([PrincipalId::parse(
            principal,
        )
        .expect("principal")]))
        .expect("principals"),
    };
    let grants = |principal: &str, rights: &[AccessRight]| {
        AccessGrants::new(std::collections::BTreeMap::from([(
            PrincipalId::parse(principal).expect("principal"),
            AccessRights::from_iter(rights.iter().copied()),
        )]))
        .expect("grants")
    };
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("grep-access"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let namespace_id = NamespaceId::parse("grep-access").expect("namespace");
    writer
        .create_namespace_with_options(
            &namespace_id,
            &loonfs_test_support::test_actor(),
            &CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                    root_grants: grants("prn_root", &[AccessRight::Admin]),
                },
                ..Default::default()
            },
        )
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let host = GrepHost::new(&store, "grep-access").await;
    for operation in [
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/team").expect("path"),
            parents: false,
        },
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/team/secret").expect("path"),
            parents: false,
        },
        FilesystemOperation::UpdateAccess {
            path: AbsolutePath::parse("/team").expect("path"),
            boundary: false,
            grants: grants("viewer", &[AccessRight::Read]),
            expected_inode_id: None,
            expected_access_revision_no: None,
        },
        FilesystemOperation::UpdateAccess {
            path: AbsolutePath::parse("/team/secret").expect("path"),
            boundary: true,
            grants: grants("finance", &[AccessRight::Read, AccessRight::History]),
            expected_inode_id: None,
            expected_access_revision_no: None,
        },
    ] {
        namespace
            .commit(
                CommitIntent::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    operation,
                )
                .with_subject(subject("prn_root")),
            )
            .await
            .expect("seed");
    }
    for path in ["/team/file", "/team/secret/file"] {
        let content = namespace
            .prepare_content(b"needle\n")
            .await
            .expect("content");
        namespace
            .commit_prepared(
                CommitIntent::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(path).expect("path"),
                        content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
                        behavior: loonfs::DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                )
                .with_subject(subject("prn_root")),
                vec![content],
            )
            .await
            .expect("put");
    }
    host.enable_grep_index(&namespace_id).await.expect("index");
    for (principal, expected) in [
        ("viewer", vec!["/team/file"]),
        ("finance", vec!["/team/secret/file"]),
        ("prn_root", vec!["/team/file", "/team/secret/file"]),
    ] {
        let reads = NamespaceReads::new(host.reader.clone(), &namespace_id)
            .with_subject(subject(principal));
        let response = host
            .service
            .query(&request("needle"), first_page(), &reads, &store)
            .await
            .expect("grep");
        assert_eq!(matched_paths(&response), expected);
    }
    let mut scan = request("ee");
    scan.allow_scan = true;
    scan.scope = GrepScope::PathPrefix(AbsolutePath::parse("/team").expect("path"));
    let reads =
        NamespaceReads::new(host.reader.clone(), &namespace_id).with_subject(subject("viewer"));
    let scanned = host
        .service
        .query(&scan, first_page(), &reads, &store)
        .await
        .expect("a scan skips the folder the subject cannot read");
    assert_eq!(
        scanned
            .matches
            .iter()
            .map(|found| found.path.as_str())
            .collect::<Vec<_>>(),
        ["/team/file"]
    );
}

/// A later page reads at its cursor's sequence, but it serves a file whose
/// content hasn't changed since then with the subject's rights at the head,
/// so a subject without the `history` right follows a cursor over unchanged
/// files, and the page leaves out a file whose content changed.
#[tokio::test]
async fn a_subject_without_history_follows_a_cursor_over_unchanged_files() {
    use loonfs::publish::{CommitIntent, FilesystemOperation};
    use loonfs_types::{
        AccessGrants, AccessRight, AccessRights, CommitId, NamespaceAccess, PrincipalId,
        PrincipalScope, PrincipalSet, Subject, SubjectId,
    };
    let subject = |principal: &str| Subject {
        principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
        subject_id: SubjectId::parse(principal).expect("subject"),
        principals: PrincipalSet::new(BTreeSet::from([
            PrincipalId::parse(principal).expect("principal")
        ]))
        .expect("principals"),
    };
    let grants = |principal: &str, rights: &[AccessRight]| {
        AccessGrants::new(std::collections::BTreeMap::from([(
            PrincipalId::parse(principal).expect("principal"),
            AccessRights::from_iter(rights.iter().copied()),
        )]))
        .expect("grants")
    };
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("grep-pages-access"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let namespace_id = NamespaceId::parse("grep-pages-access").expect("namespace");
    writer
        .create_namespace_with_options(
            &namespace_id,
            &loonfs_test_support::test_actor(),
            &CreateNamespaceOptions {
                access: NamespaceAccess::Acl {
                    principal_scope: PrincipalScope::parse("org_demo").expect("scope"),
                    root_grants: grants("prn_root", &[AccessRight::Admin]),
                },
                ..Default::default()
            },
        )
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let commit = |operation| {
        namespace.commit(
            CommitIntent::single(
                CommitId::generate(),
                loonfs_test_support::test_actor(),
                None,
                operation,
            )
            .with_subject(subject("prn_root")),
        )
    };
    commit(FilesystemOperation::CreateDirectory {
        path: AbsolutePath::parse("/team").expect("path"),
        parents: false,
    })
    .await
    .expect("directory");
    commit(FilesystemOperation::UpdateAccess {
        path: AbsolutePath::parse("/team").expect("path"),
        boundary: false,
        grants: grants("viewer", &[AccessRight::Read]),
        expected_inode_id: None,
        expected_access_revision_no: None,
    })
    .await
    .expect("grant read");
    let namespace = &namespace;
    let put = |path: &'static str, bytes: &'static [u8]| async move {
        let content = namespace.prepare_content(bytes).await.expect("content");
        namespace
            .commit_prepared(
                CommitIntent::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(path).expect("path"),
                        content: loonfs_types::FileContent::Ref(content.content_ref().clone()),
                        behavior: loonfs::DestinationBehavior::Replace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                )
                .with_subject(subject("prn_root")),
                vec![content],
            )
            .await
            .expect("put")
    };
    put("/team/one", b"page needle one\n").await;
    put("/team/two", b"page needle two\n").await;
    let host = GrepHost::new(&store, "grep-pages-access").await;
    host.enable_grep_index(&namespace_id).await.expect("index");
    let page = |principal: &'static str, cursor: Option<String>| {
        let reads = NamespaceReads::new(host.reader.clone(), &namespace_id)
            .with_subject(subject(principal));
        let store = store.clone();
        let service = &host.service;
        async move {
            service
                .query(
                    &request("page needle"),
                    page_request(cursor, 1),
                    &reads,
                    &store,
                )
                .await
                .expect("grep page")
        }
    };

    let viewer_first = page("viewer", None).await;
    let root_first = page("prn_root", None).await;
    assert_eq!(matched_paths(&viewer_first), matched_paths(&root_first));
    let first = matched_paths(&viewer_first)[0].clone();
    let second = if first == "/team/one" {
        "/team/two"
    } else {
        "/team/one"
    };
    let viewer_cursor = viewer_first.next_cursor.expect("a cursor");
    let root_cursor = root_first.next_cursor.expect("a cursor");
    assert_eq!(
        matched_paths(&page("viewer", Some(viewer_cursor.clone())).await),
        [second],
        "with no writes between pages, the viewer's second page serves the other file"
    );

    put(second, b"page needle changed\n").await;
    assert!(
        page("viewer", Some(viewer_cursor)).await.matches.is_empty(),
        "the viewer can't read the revision that the page's sequence serves"
    );
    let root_second = page("prn_root", Some(root_cursor)).await;
    assert_eq!(matched_paths(&root_second), [second]);
    assert_eq!(
        root_second.matches[0].line,
        format!("page needle {}", &second[6..]),
        "an administrator's second page serves the revision at the page's sequence"
    );
}

#[tokio::test]
async fn a_direct_build_policy_rejects_oversized_pages_without_panicking() {
    let directory = tempdir().expect("directory");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let namespace_id = NamespaceId::parse("policy-limit").expect("namespace");
    let host = GrepHost::new(&store, "policy-limit").await;
    host.writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    host.worker
        .enable(&namespace_id)
        .await
        .expect("enable grep");
    for limit in [loonfs_types::MAX_PAGE_LIMIT as usize + 1, usize::MAX] {
        let error = host
            .worker
            .steps(&namespace_id, 0)
            .build_step(GramIndexBuildPolicy {
                max_files_per_step: nonzero_usize(limit),
                ..GramIndexBuildPolicy::default()
            })
            .await
            .expect_err("an oversized policy returns an error");
        assert_eq!(error.code(), ErrorCode::InvalidRequest);
        assert_eq!(
            error.to_error_response().param.as_deref(),
            Some("max_files_per_step")
        );
    }
    host.worker
        .steps(&namespace_id, 0)
        .build_step(GramIndexBuildPolicy {
            max_files_per_step: nonzero_usize(loonfs_types::MAX_PAGE_LIMIT as usize),
            ..GramIndexBuildPolicy::default()
        })
        .await
        .expect("the maximum page size is accepted");
}
