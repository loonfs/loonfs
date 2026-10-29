//! Namespace status, maintenance passes, snapshots, and retention hooks.

use crate::common::*;
use loonfs::publish::{parse_mutation_path, CommitIntent, FilesystemOperation};
use loonfs::{
    ChangeSeq, CommitId, CreateSnapshotOptions, ErrorCode, FoldWalOutcome, MaintainMetadataOptions,
    ManifestNo, MetadataCompactionOutcome, MetadataCompactionPolicy, NamespaceId,
    SharedObjectStore, WalFoldStepOutcome,
};
use loonfs_objectstore::keys::{hint, metadata_manifest_object};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationSelector, RecordingStore};
use loonfs_types::format::manifest::decode_namespace_manifest_json;
use std::assert_matches;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn namespace_diagnostics_reports_wal_tail_objects() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let fs = open_runtime(store.clone(), "status-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status for new namespace");
    assert_eq!(status.namespace_id, namespace_id);
    assert_eq!(status.head_seq, ChangeSeq(0));
    assert_eq!(status.current_manifest_no, Some(ManifestNo(1)));
    assert_eq!(status.wal_tail_objects, 0);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));

    fs.put_file_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        &loonfs_test_support::test_actor(),
    )
    .expect("put file");

    store.reset();
    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after commit");
    assert_eq!(status.head_seq, ChangeSeq(1));
    assert_eq!(status.current_manifest_no, Some(ManifestNo(2)));
    assert_eq!(status.wal_tail_objects, 2);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));
    assert_eq!(store.count(OperationSelector::List), 1);
}

/// Checks that namespace diagnostics count the snapshots that haven't
/// expired, with or without an expiry, and leave out expired snapshots and
/// fork pins.
#[tokio::test]
async fn namespace_diagnostics_count_unexpired_snapshots_only() {
    let temp_dir = tempdir().expect("tempdir");
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(
        1_750_000_000_000,
    ));
    let writer = loonfs::LoonFs::builder_with_store(store(temp_dir.path()))
        .writer_id(loonfs_test_support::ids::writer_id(
            "diagnostic-snapshot-count",
        ))
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("build writer");
    let source = namespace_id("source");
    writer
        .create_namespace(&source, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer.open_namespace(&source).expect("open namespace");
    for ttl_ms in [
        None,
        std::num::NonZeroU64::new(1_000),
        std::num::NonZeroU64::new(60_000),
    ] {
        namespace
            .create_snapshot("counted", &CreateSnapshotOptions { ttl_ms })
            .await
            .expect("create snapshot");
    }
    writer
        .fork_namespace(
            &source,
            &namespace_id("target"),
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("fork namespace");
    clock.advance_ms(1_000);

    let diagnostics = writer
        .maintenance(loonfs_test_support::ids::writer_id(
            "diagnostic-snapshot-count",
        ))
        .diagnostics(&source)
        .await
        .expect("read diagnostics");
    assert_eq!(diagnostics.live_snapshots, 2);
    writer.shutdown().await.expect("shutdown");
}

#[test]
fn namespace_diagnostics_and_step_reject_missing_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "missing-status-test");
    let namespace_id = namespace_id("demo");

    assert_core_error_kind(
        fs.namespace_diagnostics_blocking(&namespace_id),
        ErrorCode::NamespaceNotFound,
    );
    assert_core_error_kind(
        fs.maintain_metadata_blocking(&namespace_id, metadata_options(1)),
        ErrorCode::NamespaceNotFound,
    );
    assert_core_error_kind(
        block_on(fs.maintenance.gc(&namespace_id)),
        ErrorCode::NamespaceNotFound,
    );
}

#[test]
fn namespace_diagnostics_and_step_reject_a_namespace_whose_hint_is_gone() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "partial-status-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    block_on(raw_store.delete(&hint(&namespace_id))).expect("delete head");

    assert_core_error_kind(
        fs.namespace_diagnostics_blocking(&namespace_id),
        ErrorCode::NamespaceNotFound,
    );
    assert_core_error_kind(
        fs.maintain_metadata_blocking(&namespace_id, metadata_options(1)),
        ErrorCode::NamespaceNotFound,
    );
    assert_core_error_kind(
        block_on(fs.maintenance.gc(&namespace_id)),
        ErrorCode::NamespaceNotFound,
    );

    let deleted_namespace = NamespaceId::parse("deleted").expect("namespace id");
    fs.create_namespace_blocking(&deleted_namespace, &loonfs_test_support::test_actor())
        .expect("create namespace for deletion");
    let namespace = fs
        .writer
        .open_namespace(&deleted_namespace)
        .expect("open namespace");
    block_on(namespace.delete()).expect("delete namespace");
    block_on(fs.maintenance.gc(&deleted_namespace)).expect("GC accepts a deleted namespace");
}

#[test]
fn maintenance_step_at_wal_object_threshold_folds_the_wal() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-publish-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        &loonfs_test_support::test_actor(),
    )
    .expect("put file");

    let response = fs
        .maintain_metadata_blocking(&namespace_id, metadata_options(2))
        .expect("maintenance pass");
    assert_eq!(
        response.wal_fold,
        WalFoldStepOutcome::Folded {
            manifest_head_seq: ChangeSeq(1)
        }
    );

    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after wal fold");
    assert_eq!(status.current_manifest_no, Some(ManifestNo(3)));
    assert_eq!(status.wal_tail_objects, 0);

    // Maintenance is record-less: folding the WAL must leave nothing
    // under `checkpoints/`.
    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let records =
        block_on(raw_store.list_prefix(&loonfs_objectstore::keys::pin_prefix(&namespace_id)))
            .expect("list pins");
    assert!(
        records.is_empty(),
        "maintenance pass created pins: {records:?}"
    );
}

#[test]
fn metadata_run_does_not_advance_retention() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-retention-opt-in-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        &loonfs_test_support::test_actor(),
    )
    .expect("put file");

    let response = fs
        .maintain_metadata_blocking(&namespace_id, metadata_options(1))
        .expect("step without retention");
    assert_eq!(
        response.wal_fold,
        WalFoldStepOutcome::Folded {
            manifest_head_seq: ChangeSeq(1)
        }
    );
    assert_eq!(
        fs.namespace_diagnostics_blocking(&namespace_id)
            .expect("status")
            .retention_floor_seq,
        ChangeSeq(0)
    );

    let retention = fs
        .advance_retention_floor_blocking(&namespace_id)
        .expect("step with retention");
    assert_eq!(retention.retention_floor_seq, ChangeSeq(1));

    // A plan naming retention alone is the same opt-in.
    fs.put_file_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        &loonfs_test_support::test_actor(),
    )
    .expect("put second file");
    fs.maintain_metadata_blocking(&namespace_id, metadata_options(1))
        .expect("fold second WAL object");
    let retention = fs
        .advance_retention_floor_blocking(&namespace_id)
        .expect("retention-only step");
    assert_eq!(retention.retention_floor_seq, ChangeSeq(2));
}

#[test]
fn the_typed_wrappers_are_single_action_steps() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let fs = open_runtime(store.clone(), "typed-wrapper-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    store.reset();
    let folded = fs
        .fold_wal_blocking(&namespace_id)
        .expect("fold an empty tail");
    assert_eq!(
        (folded.outcome, folded.manifest_no),
        (FoldWalOutcome::AlreadyCurrent, ManifestNo(1)),
        "the wrapper folds a tail; it does not publish a manifest for a namespace with none"
    );
    assert_eq!(store.count(OperationSelector::Put), 0);

    fs.put_file_blocking(
        &namespace_id,
        "/docs/first.txt",
        b"first",
        &loonfs_test_support::test_actor(),
    )
    .expect("put first file");
    let folded = fs.fold_wal_blocking(&namespace_id).expect("fold the tail");
    assert_eq!(
        (folded.outcome, folded.manifest_head_seq),
        (FoldWalOutcome::Published, ChangeSeq(1))
    );
    let compaction = fs
        .maintain_metadata_blocking(
            &namespace_id,
            MaintainMetadataOptions {
                compaction_policy: MetadataCompactionPolicy::CompactImmediately,
                ..metadata_options(1)
            },
        )
        .expect("compaction step after the fold")
        .compaction;
    assert_eq!(
        compaction,
        MetadataCompactionOutcome::NotNeeded,
        "the upkeep pass reports its compaction half rather than hiding it"
    );

    fs.put_file_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        &loonfs_test_support::test_actor(),
    )
    .expect("put second file");
    let metadata = fs
        .maintain_metadata_blocking(&namespace_id, metadata_options(1))
        .expect("upkeep-only step");
    assert_eq!(
        metadata.wal_fold,
        WalFoldStepOutcome::Folded {
            manifest_head_seq: ChangeSeq(2)
        },
        "the metadata request runs only metadata maintenance"
    );

    let checkpoint = fs
        .create_snapshot_blocking(&namespace_id)
        .expect("create checkpoint");
    let advanced = fs
        .advance_retention_floor_blocking(&namespace_id)
        .expect("advance retention");
    assert_eq!(advanced.retention_floor_seq, checkpoint.captured_seq);
    let retention = fs
        .advance_retention_floor_blocking(&namespace_id)
        .expect("retention-only step");
    assert_eq!(retention.retention_floor_seq, advanced.retention_floor_seq);

    block_on(fs.maintenance.gc(&namespace_id)).expect("GC run");
    block_on(
        fs.maintenance
            .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new()),
    )
    .expect("metadata compaction run");
    assert_eq!(
        fs.namespace_diagnostics_blocking(&namespace_id)
            .expect("status")
            .retention_floor_seq,
        advanced.retention_floor_seq
    );
}

#[test]
fn maintenance_step_after_existing_manifest_writes_delta_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-delta-run-publish-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        &loonfs_test_support::test_actor(),
    )
    .expect("put first file");
    fs.maintain_metadata_blocking(&namespace_id, metadata_options(1))
        .expect("first maintenance pass");

    fs.put_file_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        &loonfs_test_support::test_actor(),
    )
    .expect("put second file");
    let step = fs
        .maintain_metadata_blocking(&namespace_id, metadata_options(1))
        .expect("second maintenance pass");
    assert_eq!(
        step.wal_fold,
        WalFoldStepOutcome::Folded {
            manifest_head_seq: ChangeSeq(2)
        }
    );

    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after delta wal fold");
    assert_eq!(status.current_manifest_no, Some(ManifestNo(4)));
    assert_eq!(status.wal_tail_objects, 0);

    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let root = block_on(loonfs_core::control::load_namespace_current_manifest(
        &raw_store,
        &namespace_id,
    ))
    .expect("metadata root");
    let manifest_key = metadata_manifest_object(&namespace_id, &root.state.manifest().manifest_no);
    let manifest_bytes = block_on(raw_store.get(&manifest_key, None))
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    // A WAL fold only appends: the base marker stays where the first
    // published manifest put it until compaction merges the delta runs.
    assert_eq!(manifest.payload().base_seq(), ChangeSeq(1));
    let delta_files = manifest
        .payload()
        .runs
        .iter()
        .filter(|run| run.tier == loonfs_types::format::manifest::RunTier::Delta)
        .collect::<Vec<_>>();
    assert!(!delta_files.is_empty());
    assert!(delta_files.iter().any(|run| run.run_seq == ChangeSeq(2)));
}

#[test]
fn a_standalone_maintenance_drives_metadata_compaction_itself() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "manual-compaction-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    assert_eq!(
        block_on(
            fs.maintenance
                .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
        )
        .expect("compact an empty namespace")
        .compaction,
        MetadataCompactionOutcome::NotNeeded
    );

    // Enough folds to put the manifest's delta run count over the compaction
    // trigger, which is what makes the planner select a group at all.
    for index in 0..9 {
        fs.put_file_blocking(
            &namespace_id,
            &format!("/docs/file-{index}.txt"),
            format!("file {index}").as_bytes(),
            &loonfs_test_support::test_actor(),
        )
        .expect("put a file");
        fs.fold_wal_blocking(&namespace_id).expect("fold the tail");
    }
    let manifest_before = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status before the call")
        .current_manifest_no;

    // Nothing here has outgrown the row budget, so this call publishes the
    // unit the planner chose, as the next maintenance pass would have.
    assert_matches!(
        block_on(
            fs.maintenance
                .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
        )
        .expect("plan a compaction")
        .compaction,
        MetadataCompactionOutcome::Published { .. }
    );
    assert_ne!(
        fs.namespace_diagnostics_blocking(&namespace_id)
            .expect("status after the call")
            .current_manifest_no,
        manifest_before,
        "the call must run the same planner a maintenance pass runs, and publish what it chose"
    );
}

fn create_directory_request(commit_id: &str, absolute_path: &str) -> CommitIntent {
    CommitIntent::single(
        CommitId::parse(commit_id).expect("valid commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: parse_mutation_path(absolute_path).expect("valid mutation path"),
            parents: false,
        },
    )
}

#[tokio::test]
async fn fenced_compaction_blocks_until_a_new_request_claims_and_publishes() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let first = open_runtime_async(store.clone(), "first").await;
    let second = open_runtime_async(store.clone(), "second").await;
    let namespace_id = namespace_id("demo");
    first
        .writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    for index in 0..2 {
        first
            .put_file(
                &namespace_id,
                &format!("/file-{index}"),
                b"data",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put file");
        first
            .maintenance
            .fold_wal(&namespace_id)
            .await
            .expect("fold WAL");
    }
    for runtime in [&first, &second] {
        assert_matches!(
            runtime
                .maintenance
                .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
                .await
                .expect("claim and compact")
                .compaction,
            MetadataCompactionOutcome::Published { .. }
        );
    }
    let before =
        loonfs_core::control::load_namespace_current_manifest(store.as_ref(), &namespace_id)
            .await
            .expect("manifest before fencing");
    assert_eq!(
        before.state.compactor_epoch(),
        loonfs_types::CompactorEpoch(2)
    );

    assert_eq!(
        first
            .maintenance
            .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
            .await
            .expect("fenced step")
            .compaction,
        MetadataCompactionOutcome::Fenced
    );
    let fenced =
        loonfs_core::control::load_namespace_current_manifest(store.as_ref(), &namespace_id)
            .await
            .expect("manifest after fencing");
    assert_eq!(fenced.state.manifest(), before.state.manifest());

    assert_matches!(
        first
            .maintenance
            .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
            .await
            .expect("claim again and compact")
            .compaction,
        MetadataCompactionOutcome::Published { .. }
    );
    let published =
        loonfs_core::control::load_namespace_current_manifest(store.as_ref(), &namespace_id)
            .await
            .expect("manifest after publication");
    assert_eq!(
        published.state.compactor_epoch(),
        loonfs_types::CompactorEpoch(3)
    );
    assert!(published.state.manifest().manifest_no > before.state.manifest().manifest_no);
}

#[tokio::test]
async fn a_delayed_fenced_compaction_does_not_forget_a_newer_claim() {
    let directory = tempdir().expect("directory");
    let namespace_id = namespace_id("delayed-fencing");
    let shared = store(directory.path());
    let blocked = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::metadata_manifest_prefix(
            &namespace_id,
        )),
        OperationSelector::Get,
    ));
    let first = open_runtime_async(blocked.clone(), "first").await;
    let second = open_runtime_async(shared.clone(), "second").await;
    first
        .writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    for index in 0..2 {
        first
            .put_file(
                &namespace_id,
                &format!("/file-{index}"),
                b"content",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put file");
        first
            .maintenance
            .fold_wal(&namespace_id)
            .await
            .expect("fold");
    }
    for runtime in [&first, &second] {
        assert_matches!(
            runtime
                .maintenance
                .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
                .await
                .expect("claim")
                .compaction,
            MetadataCompactionOutcome::Published { .. }
        );
    }

    blocked.block_next();
    let cancellation = loonfs::MaintenanceCancellation::new();
    let delayed = first
        .maintenance
        .compact_metadata(&namespace_id, &cancellation);
    let reclaim = async {
        loonfs_test_support::clock::bounded(
            std::time::Duration::from_secs(10),
            blocked.wait_until_blocked(),
        )
        .await
        .expect("old epoch attempt reached its manifest read");
        assert_eq!(
            first
                .maintenance
                .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
                .await
                .expect("fenced")
                .compaction,
            MetadataCompactionOutcome::Fenced,
        );
        assert_matches!(
            first
                .maintenance
                .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
                .await
                .expect("claim again")
                .compaction,
            MetadataCompactionOutcome::Published { .. }
        );
        let current =
            loonfs_core::control::load_namespace_current_manifest(shared.as_ref(), &namespace_id)
                .await
                .expect("new claim");
        assert_eq!(
            current.state.compactor_epoch(),
            loonfs_types::CompactorEpoch(3)
        );
        blocked.release();
    };
    let (delayed, ()) =
        loonfs_test_support::clock::bounded(std::time::Duration::from_secs(30), async {
            tokio::join!(delayed, reclaim)
        })
        .await
        .expect("concurrent compaction attempts finish");
    assert_eq!(
        delayed.expect("delayed result").compaction,
        MetadataCompactionOutcome::Fenced
    );

    first
        .maintenance
        .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
        .await
        .expect("use the current claim");
    let current =
        loonfs_core::control::load_namespace_current_manifest(shared.as_ref(), &namespace_id)
            .await
            .expect("current epoch");
    assert_eq!(
        current.state.compactor_epoch(),
        loonfs_types::CompactorEpoch(3),
        "a delayed result for epoch 1 must not evict epoch 3 and force another claim",
    );
}

#[test]
fn maintenance_step_counts_wal_objects_not_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-wal-object-count-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    let first_batch = fs.mutate_batch_blocking(
        &namespace_id,
        vec![
            create_directory_request("create-a", "/a"),
            create_directory_request("create-b", "/b"),
        ],
    );
    assert!(first_batch.iter().all(Result::is_ok));

    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after first batch");
    assert_eq!(status.head_seq, ChangeSeq(2));
    assert_eq!(status.wal_tail_objects, 2);

    let response = fs
        .maintain_metadata_blocking(&namespace_id, metadata_options(3))
        .expect("maintenance pass");
    assert_eq!(response.wal_fold, WalFoldStepOutcome::NotNeeded);

    fs.mutate_blocking(&namespace_id, create_directory_request("create-c", "/c"))
        .expect("second WAL object commit");

    let response = fs
        .maintain_metadata_blocking(&namespace_id, metadata_options(3))
        .expect("maintenance pass at WAL object threshold");
    assert_eq!(
        response.wal_fold,
        WalFoldStepOutcome::Folded {
            manifest_head_seq: ChangeSeq(3)
        }
    );
}

#[tokio::test]
async fn maintenance_step_treats_manifest_number_collision_as_benign_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let blocked = Arc::new(BlockingStore::new(
        raw_store.clone(),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::Put,
    ));
    let fs = open_runtime_async(blocked.clone(), "step-race-test").await;
    let winner = open_runtime_async(raw_store, "competing-fold").await;

    fs.create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    fs.put_file(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        &loonfs_test_support::test_actor(),
    )
    .await
    .expect("put file");

    blocked.block_next();
    let options = metadata_options(1);
    let (step, ()) = futures::join!(
        fs.maintenance
            .maintain_metadata_with_options(&namespace_id, &options),
        async {
            blocked.wait_until_blocked().await;
            winner
                .maintenance
                .fold_wal(&namespace_id)
                .await
                .expect("competing fold");
            blocked.release();
        }
    );
    let step = step.expect("maintenance pass should not fail on metadata root publish race");

    assert_eq!(
        step.wal_fold,
        WalFoldStepOutcome::AlreadyPublished {
            attempted_seq: ChangeSeq(1),
            current_manifest_no: ManifestNo(3),
        }
    );
    let status = fs
        .maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("status after lost race");
    assert_eq!(status.current_manifest_no, Some(ManifestNo(3)));
    assert_eq!(status.wal_tail_objects, 0);
}

/// Checks that a second `Maintenance::gc` call reads no completed upload
/// session record, because the runtime keeps the completed sessions between
/// passes.
#[tokio::test]
async fn a_second_collection_reads_no_completed_session_record() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::upload_session_prefix(
            &namespace_id,
        )),
    ));
    let fs = open_runtime_with_async(store.clone(), "session-cache-test", |builder| {
        builder.inline_content(loonfs::InlineContentPolicy {
            inline_content_threshold_bytes: None,
            ..Default::default()
        })
    })
    .await;
    fs.create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = fs.namespace_writer(&namespace_id).expect("open namespace");
    for body in [&b"one"[..], b"two", b"three"] {
        namespace
            .prepare_content(body)
            .await
            .expect("prepare content");
    }

    store.reset();
    let first = fs.maintenance.gc(&namespace_id).await.expect("first pass");
    let first_reads = store.count(OperationSelector::Read);
    store.reset();
    let second = fs.maintenance.gc(&namespace_id).await.expect("second pass");

    assert_eq!(first_reads, 3, "the first pass reads each record once");
    assert_eq!(store.count(OperationSelector::Read), 0);
    assert_eq!(first.retained.upload_session_window, 3);
    assert_eq!(second.retained.upload_session_window, 3);
}
