//! Namespace status, maintenance passes, snapshots, and retention hooks.

use crate::common::*;
use loonfs::publish::{parse_mutation_path, CommitIntent, FilesystemOperation};
use loonfs::{
    CommitId, CommitSeq, CreateSnapshotOptions, ErrorCode, FoldWalOutcome, FoldWalResponse,
    ManifestNo, MetadataCompactionOutcome, NamespaceId, SharedObjectStore,
};
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_manifest_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    KeyPredicate, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::format::manifest::decode_namespace_manifest_json;
use std::assert_matches;
use std::sync::Arc;
use tempfile::tempdir;

/// Checks that namespace diagnostics report the WAL tail's objects and the
/// time of its oldest commit, which they leave out for a tail without a
/// commit, and that they read the store with one listing of the manifests
/// and one of the snapshots.
#[test]
fn namespace_diagnostics_reports_wal_tail_objects() {
    const MINUTE_MS: u64 = 60 * 1_000;
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::at_system_time());
    let fs = open_runtime_with(store.clone(), "status-test", |builder| {
        builder.wall_clock(clock.clone())
    });
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status for new namespace");
    assert_eq!(status.namespace_id, namespace_id);
    assert_eq!(status.head_seq, CommitSeq(0));
    assert_eq!(status.current_manifest_no, Some(ManifestNo(2)));
    assert_eq!(status.wal_tail_objects, 0);
    assert_eq!(status.wal_tail_oldest_commit_at_ms, None);
    assert_eq!(status.retention_floor_seq, CommitSeq(0));

    let first = fs
        .put_file_blocking(
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
    assert_eq!(status.head_seq, CommitSeq(1));
    assert_eq!(status.current_manifest_no, Some(ManifestNo(3)));
    assert_eq!(status.wal_tail_objects, 2);
    assert_eq!(
        status.wal_tail_oldest_commit_at_ms,
        Some(first.committed_at_ms)
    );
    assert_eq!(status.retention_floor_seq, CommitSeq(0));
    let listings: Vec<_> = store
        .snapshot()
        .into_iter()
        .filter_map(|operation| match operation {
            RecordedOperation::List { prefix } => Some(prefix),
            _ => None,
        })
        .collect();
    // The diagnostics find the newest manifest with one listing of the
    // manifests, and the snapshot count lists the snapshots under the head
    // that the diagnostics loaded.
    assert_eq!(
        listings,
        [
            metadata_manifest_prefix(&namespace_id),
            loonfs_objectstore::keys::snapshot_prefix(&namespace_id),
        ]
    );

    clock.advance_ms(MINUTE_MS);
    let second = fs
        .put_file_blocking(
            &namespace_id,
            "/docs/later.txt",
            b"later",
            &loonfs_test_support::test_actor(),
        )
        .expect("put a later file");
    assert_eq!(second.committed_at_ms, first.committed_at_ms + MINUTE_MS);
    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after a later commit");
    assert_eq!(status.wal_tail_objects, 3);
    assert_eq!(
        status.wal_tail_oldest_commit_at_ms,
        Some(first.committed_at_ms),
        "the diagnostics report the oldest commit, not the newest"
    );

    // A delete of a missing path claims the namespace, so it writes a fence,
    // and commits nothing.
    let fenced = loonfs_test_support::ids::namespace_id("fences-alone");
    fs.create_namespace_blocking(&fenced, &loonfs_test_support::test_actor())
        .expect("create namespace");
    fs.delete_path_blocking(&fenced, "/missing", &loonfs_test_support::test_actor())
        .expect_err("a delete of a missing path");
    let status = fs
        .namespace_diagnostics_blocking(&fenced)
        .expect("status of a tail of fences alone");
    assert_eq!(status.wal_tail_objects, 1);
    assert_eq!(status.wal_tail_oldest_commit_at_ms, None);
}

/// Checks that namespace diagnostics count the snapshots that haven't
/// expired, with or without an expiry, and leave out expired snapshots and
/// fork snapshots.
#[tokio::test]
async fn namespace_diagnostics_count_unexpired_snapshots_only() {
    let temp_dir = tempdir().expect("tempdir");
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(
        1_750_000_000_000,
    ));
    let writer =
        loonfs::LoonFs::builder_with_store(store(temp_dir.path()), loonfs::Context::Service)
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
            .create_snapshot_with_options(&CreateSnapshotOptions {
                name: Some("counted".to_owned()),
                ttl_ms,
            })
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
        .maintenance()
        .diagnostics(&source)
        .await
        .expect("read diagnostics");
    // The permanent snapshot, the one with a 60 s TTL, and the fork's
    // temporary snapshot, which expires 5 minutes after the fork.
    assert_eq!(diagnostics.live_snapshots, 3);
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
fn namespace_diagnostics_and_step_reject_a_namespace_whose_manifests_are_gone() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "partial-status-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    for key in block_on(raw_store.list_prefix(&metadata_manifest_prefix(&namespace_id)))
        .expect("list manifests")
    {
        block_on(raw_store.delete(&key)).expect("delete a manifest");
    }

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
    assert_matches!(
        response.wal_fold,
        Some(FoldWalResponse {
            outcome: FoldWalOutcome::Published,
            manifest_head_seq: CommitSeq(1),
            ..
        })
    );

    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after wal fold");
    assert_eq!(status.current_manifest_no, Some(ManifestNo(4)));
    assert_eq!(status.wal_tail_objects, 0);

    // Maintenance is record-less: folding the WAL must leave nothing
    // under `snapshots/`.
    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let records =
        block_on(raw_store.list_prefix(&loonfs_objectstore::keys::snapshot_prefix(&namespace_id)))
            .expect("list snapshots");
    assert!(
        records.is_empty(),
        "maintenance pass created snapshots: {records:?}"
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
    assert_matches!(
        response.wal_fold,
        Some(FoldWalResponse {
            outcome: FoldWalOutcome::Published,
            manifest_head_seq: CommitSeq(1),
            ..
        })
    );
    assert_eq!(
        fs.namespace_diagnostics_blocking(&namespace_id)
            .expect("status")
            .retention_floor_seq,
        CommitSeq(0)
    );

    let retention = fs
        .advance_retention_floor_blocking(&namespace_id)
        .expect("step with retention");
    assert_eq!(retention.retention_floor_seq, CommitSeq(1));

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
    assert_eq!(retention.retention_floor_seq, CommitSeq(2));
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
        (FoldWalOutcome::AlreadyCurrent, ManifestNo(2)),
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
        (FoldWalOutcome::Published, CommitSeq(1))
    );
    let compaction = fs
        .maintain_metadata_blocking(&namespace_id, metadata_options(1))
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
    assert_matches!(
        metadata.wal_fold,
        Some(FoldWalResponse {
            outcome: FoldWalOutcome::Published,
            manifest_head_seq: CommitSeq(2),
            ..
        }),
        "the metadata request runs only metadata maintenance"
    );

    let snapshot = fs
        .create_snapshot_blocking(&namespace_id)
        .expect("create snapshot");
    let advanced = fs
        .advance_retention_floor_blocking(&namespace_id)
        .expect("advance retention");
    assert_eq!(advanced.retention_floor_seq, snapshot.captured_seq);
    let handle = fs.namespace_writer(&namespace_id).expect("open namespace");
    let retention = fs
        .advance_retention_floor_blocking(&namespace_id)
        .expect("retention-only step");
    assert_eq!(retention.retention_floor_seq, advanced.retention_floor_seq);

    block_on(fs.maintenance.gc(&namespace_id)).expect("GC run");
    block_on(handle.compact_metadata(&loonfs::MaintenanceCancellation::new()))
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
    assert_matches!(
        step.wal_fold,
        Some(FoldWalResponse {
            outcome: FoldWalOutcome::Published,
            manifest_head_seq: CommitSeq(2),
            ..
        })
    );

    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after delta wal fold");
    assert_eq!(status.current_manifest_no, Some(ManifestNo(5)));
    assert_eq!(status.wal_tail_objects, 0);

    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let root = block_on(
        loonfs_core::test_support::namespace_store(&raw_store, &namespace_id)
            .load_current_manifest(),
    )
    .expect("metadata root");
    let manifest_key = metadata_manifest_object(&namespace_id, &root.manifest().manifest_no);
    let manifest_bytes = block_on(raw_store.get(&manifest_key, None))
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    // A WAL fold only appends: the base marker stays where the first
    // published manifest put it until compaction rewrites the delta runs.
    assert_eq!(manifest.payload().base_seq(), CommitSeq(1));
    let delta_files = manifest
        .payload()
        .runs
        .iter()
        .filter(|run| run.tier == loonfs_types::format::manifest::RunTier::Delta)
        .collect::<Vec<_>>();
    assert!(!delta_files.is_empty());
    assert!(delta_files.iter().any(|run| run.run_seq == CommitSeq(2)));
}

#[test]
fn a_standalone_maintenance_drives_metadata_compaction_itself() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "manual-compaction-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    let namespace = fs.namespace_writer(&namespace_id).expect("open namespace");
    assert_eq!(
        block_on(namespace.compact_metadata(&loonfs::MaintenanceCancellation::new()))
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

    // The call publishes the unit that the planner chooses, as the next
    // maintenance pass would.
    assert_matches!(
        block_on(namespace.compact_metadata(&loonfs::MaintenanceCancellation::new()))
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
    assert_eq!(status.head_seq, CommitSeq(2));
    assert_eq!(status.wal_tail_objects, 2);

    let response = fs
        .maintain_metadata_blocking(&namespace_id, metadata_options(3))
        .expect("maintenance pass");
    assert_eq!(response.wal_fold, None);

    fs.mutate_blocking(&namespace_id, create_directory_request("create-c", "/c"))
        .expect("second WAL object commit");

    let response = fs
        .maintain_metadata_blocking(&namespace_id, metadata_options(3))
        .expect("maintenance pass at WAL object threshold");
    assert_matches!(
        response.wal_fold,
        Some(FoldWalResponse {
            outcome: FoldWalOutcome::Published,
            manifest_head_seq: CommitSeq(3),
            ..
        })
    );
}

/// Checks that a second `Maintenance::gc_pool` call reads no completed upload
/// session record, because the runtime keeps the completed sessions between
/// passes.
#[tokio::test]
async fn a_second_collection_reads_no_completed_session_record() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::upload_records(),
    ));
    let fs = open_runtime_with_async(store.clone(), "session-cache-test", |builder| {
        builder.config(loonfs::RuntimeConfig {
            inline_content: loonfs::InlineContentPolicy {
                inline_content_threshold_bytes: 0,
                ..Default::default()
            },
            ..loonfs::RuntimeConfig::default()
        })
    })
    .await;
    let pool_id = fs
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace")
        .pool_id;
    let namespace = fs.namespace_writer(&namespace_id).expect("open namespace");
    let actor = loonfs_test_support::test_actor();
    for body in [&b"one"[..], b"two", b"three"] {
        let upload = namespace
            .create_upload(&actor)
            .await
            .expect("create upload");
        namespace
            .put_upload_content(&upload.upload_id, body, &actor)
            .await
            .expect("upload content");
        namespace
            .complete_upload(
                &upload.upload_id,
                loonfs_types::CompleteUploadBody::ServiceProxied {},
                &actor,
            )
            .await
            .expect("complete upload");
    }

    store.reset();
    let first = fs.maintenance.gc_pool(&pool_id).await.expect("first pass");
    let first_reads = store.count(OperationSelector::Read);
    store.reset();
    let second = fs.maintenance.gc_pool(&pool_id).await.expect("second pass");

    assert_eq!(first_reads, 3, "the first pass reads each record once");
    assert_eq!(store.count(OperationSelector::Read), 0);
    assert_eq!(first.retained.upload_session_window, 3);
    assert_eq!(second.retained.upload_session_window, 3);
}

/// Checks that a namespace collection of a source sends one request for each
/// snapshot record whatever the number of its fork snapshots, and none under
/// a fork's prefix, so a fork snapshot costs no read of its target.
#[tokio::test]
async fn a_source_collection_reads_no_fork_target() {
    let actor = loonfs_test_support::test_actor();
    let mut requests_beyond_the_records = Vec::new();
    for forks in [1, 4] {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(RecordingStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::any(),
        ));
        let fs = open_runtime_async(store.clone(), "fork-targets").await;
        let source = namespace(&fs).await;
        let mut fork_prefixes = Vec::new();
        for index in 0..forks {
            let fork = namespace_id(&format!("fork-{index}"));
            fs.writer
                .fork_namespace(&source, &fork, &actor)
                .await
                .expect("fork");
            fork_prefixes.push(loonfs_objectstore::keys::namespace_prefix(&fork));
        }
        let records = store
            .inner()
            .list_prefix(&loonfs_objectstore::keys::snapshot_prefix(&source))
            .await
            .expect("snapshot records")
            .len();
        store.reset();
        let report = fs
            .maintenance
            .gc(&source)
            .await
            .expect("collect the source");
        assert_eq!(report.deleted_snapshots_by_owner.fork, 0);
        let operations = store.take();
        assert!(
            operations.iter().all(|operation| fork_prefixes
                .iter()
                .all(|prefix| !operation.key().starts_with(prefix.as_str()))),
            "{forks} forks: {operations:?}"
        );
        requests_beyond_the_records.push(operations.len() - records);
    }
    assert_eq!(
        requests_beyond_the_records[0],
        requests_beyond_the_records[1]
    );
}
