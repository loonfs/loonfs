//! Namespace status, maintenance steps, checkpointing, and retention hooks.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::publish::{parse_mutation_path, CommitRequest, FilesystemOperation};
use loonfs::{
    ChangeSeq, CheckpointOwnerSummary, CommitId, CreateCheckpointOptions, CreateNamespaceOptions,
    CreateSnapshotOptions, DeleteNamespaceOptions, ErrorCode, FsAdmin, FsWriter, MaintenancePlan,
    ManifestNo, MetadataCompactionOutcome, NamespaceId, PutFileOptions, ReorganizeStepOutcome,
    RuntimeError, SharedObjectStore, WalFlushStepOutcome,
};
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, ControlObjectEnvelope, ControlObjectKind,
    HeadState, HeadStateEnvelope,
};
use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, wal_head, wal_segment_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, CountingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
    OperationKind,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[test]
fn namespace_diagnostics_reports_wal_tail_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let fs = open_runtime(store.clone(), "status-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status for new namespace");
    assert_eq!(status.namespace_id, namespace_id);
    assert_eq!(status.head_seq, ChangeSeq(0));
    // A namespace that has never flushed has published no manifest of its
    // own; it reads from the built-in genesis state.
    assert_eq!(status.current_manifest_no, None);
    assert_eq!(status.wal_tail_segments, 0);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    store.reset();
    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after commit");
    assert_eq!(status.head_seq, ChangeSeq(1));
    assert_eq!(status.current_manifest_no, None);
    assert_eq!(status.wal_tail_segments, 1);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));
    assert_eq!(store.count(OperationClass::List), 1);
}

#[test]
fn namespace_diagnostics_counts_wal_tail_without_reading_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    // Count only WAL segment reads: the head's chain pointers must be
    // enough to size a hint-covered tail.
    let store = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
    ));
    let fs = open_runtime(store.clone(), "status-count-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for revision in 0..3 {
        fs.put_file_bytes_blocking(
            &namespace_id,
            &format!("/docs/hello-{revision}.txt"),
            format!("rev {revision}").as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
    }

    store.reset();
    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status with populated tail");
    assert_eq!(status.wal_tail_segments, 3);
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[test]
fn namespace_diagnostics_counts_user_and_live_snapshot_records_only() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "diagnostic-checkpoint-count-test");
    let source = namespace_id("source");
    let target = namespace_id("target");

    fs.create_namespace_blocking(&source, CreateNamespaceOptions::default())
        .expect("create namespace");
    block_on(fs.admin.create_checkpoint(
        &source,
        CreateCheckpointOptions {
            name: "durable".to_owned(),
            ttl_ms: None,
        },
    ))
    .expect("create durable checkpoint");
    block_on(fs.admin.create_checkpoint(
        &source,
        CreateCheckpointOptions {
            name: "expired".to_owned(),
            ttl_ms: Some(0),
        },
    ))
    .expect("create expired checkpoint");
    block_on(fs.admin.create_snapshot(
        &source,
        CreateSnapshotOptions {
            name: "expired".to_owned(),
            expires_at_ms: 1,
        },
    ))
    .expect("create expired snapshot record");
    block_on(fs.admin.create_snapshot(
        &source,
        CreateSnapshotOptions {
            name: "live".to_owned(),
            expires_at_ms: u64::MAX,
        },
    ))
    .expect("create live snapshot");
    fs.fork_namespace_blocking(&source, &target)
        .expect("fork namespace");

    let diagnostics = fs
        .namespace_diagnostics_blocking(&source)
        .expect("read diagnostics");
    assert_eq!(diagnostics.live_snapshots, 1);
    assert_eq!(diagnostics.live_checkpoints, 2);

    let step = fs
        .maintenance_step_namespace_blocking(&source, MaintenancePlan::metadata())
        .expect("run maintenance");
    assert_eq!(step.status_before.live_snapshots, 0);
    assert_eq!(step.status_before.live_checkpoints, 0);
}

/// Pointers a head published before the accelerator was sized to cover the
/// whole legal WAL tail.
const LEGACY_RECENT_SEGMENTS: usize = 32;

/// Rewrites a head's replay accelerator to its newest `keep` pointers,
/// which is the shape a head published by an older build carries.
fn truncate_recent_segments(store: &SharedObjectStore, namespace_id: &NamespaceId, keep: usize) {
    let key = wal_head(namespace_id);
    let bytes = block_on(store.get(&key, None))
        .expect("read head")
        .expect("head exists");
    let envelope: ControlObjectEnvelope<HeadState> =
        decode_control_object(&bytes, ControlObjectKind::WalHead).expect("decode head");
    let mut state = envelope.state;
    assert!(
        state.recent_segments.len() > keep,
        "the fixture must publish more pointers than the legacy window held"
    );
    state.recent_segments.truncate(keep);
    let truncated =
        HeadStateEnvelope::from_state(ControlObjectKind::WalHead, state).expect("head envelope");
    let encoded = encode_control_object(&truncated).expect("encode head");
    block_on(store.put_overwrite(&key, bytes::Bytes::from(encoded))).expect("rewrite head");
}

#[test]
fn a_head_that_under_describes_its_tail_is_repaired_by_an_explicit_flush() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let fs = open_runtime(store.clone(), "legacy-head-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for revision in 0..LEGACY_RECENT_SEGMENTS + 4 {
        fs.put_file_bytes_blocking(
            &namespace_id,
            &format!("/docs/hello-{revision}.txt"),
            format!("rev {revision}").as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put file");
    }
    truncate_recent_segments(&store, &namespace_id, LEGACY_RECENT_SEGMENTS);

    let error = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect_err("a head that does not describe its tail cannot be counted");
    assert_eq!(
        error.code(),
        ErrorCode::NamespaceCorrupt,
        "a head that under-describes its tail is corruption, not absence: {error}"
    );

    let flushed = fs
        .flush_wal_blocking(&namespace_id)
        .expect("an explicit flush does not read the status it cannot get");
    assert!(
        matches!(flushed.wal_flush, WalFlushStepOutcome::Flushed { .. }),
        "unexpected flush outcome: {:?}",
        flushed.wal_flush
    );

    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after the flush");
    assert_eq!(status.wal_tail_segments, 0);

    // And the pointer count answers again for the tail written after it.
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/after-the-flush.txt",
        b"after",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file after the flush");
    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after the flush");
    assert_eq!(status.wal_tail_segments, 1);
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
        fs.maintenance_step_namespace_blocking(&namespace_id, MaintenancePlan::metadata()),
        ErrorCode::NamespaceNotFound,
    );
}

#[test]
fn namespace_diagnostics_and_step_reject_a_namespace_whose_head_is_gone() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "partial-status-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    block_on(raw_store.delete(&wal_head(&namespace_id))).expect("delete head");

    assert_core_error_kind(
        fs.namespace_diagnostics_blocking(&namespace_id),
        ErrorCode::NamespaceNotFound,
    );
    assert_core_error_kind(
        fs.maintenance_step_namespace_blocking(&namespace_id, MaintenancePlan::metadata()),
        ErrorCode::NamespaceNotFound,
    );
}

#[test]
fn maintenance_step_below_threshold_is_not_needed() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-not-needed-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    let step = fs
        .maintenance_step_namespace_blocking(&namespace_id, metadata_plan(2))
        .expect("maintenance step");
    assert_eq!(step.namespace_id, namespace_id);
    assert_eq!(step.status_before.wal_tail_segments, 1);
    assert_eq!(upkeep(&step).wal_flush, WalFlushStepOutcome::NotNeeded);
}

#[test]
fn maintenance_step_at_segment_threshold_flushes_the_wal() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-publish-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    let step = fs
        .maintenance_step_namespace_blocking(&namespace_id, metadata_plan(1))
        .expect("maintenance step");
    assert_eq!(step.status_before.head_seq, ChangeSeq(1));
    assert_eq!(
        upkeep(&step).wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(1)
        }
    );

    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after wal flush");
    assert_eq!(status.current_manifest_no, Some(ManifestNo(1)));
    assert_eq!(status.wal_tail_segments, 0);

    // Maintenance is record-less: flushing the WAL must leave nothing
    // under `checkpoints/`.
    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let records = block_on(
        raw_store.list_prefix(&loonfs_objectstore::keys::checkpoint_prefix(&namespace_id)),
    )
    .expect("list checkpoint records");
    assert!(
        records.is_empty(),
        "maintenance step created checkpoint records: {records:?}"
    );
}

#[test]
fn maintenance_step_advances_the_floor_only_when_retention_opts_in() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-retention-opt-in-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    // A plan that names only upkeep flushes, and reports no retention at
    // all — replay history is never surrendered unnamed.
    let step = fs
        .maintenance_step_namespace_blocking(&namespace_id, metadata_plan(1))
        .expect("step without retention");
    assert_eq!(
        upkeep(&step).wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(1)
        }
    );
    assert_eq!(step.retention, None);
    assert_eq!(
        fs.namespace_diagnostics_blocking(&namespace_id)
            .expect("status")
            .retention_floor_seq,
        ChangeSeq(0)
    );

    // Naming it advances the floor to the flushed manifest head.
    let step = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenancePlan {
                advance_retention: true,
                ..metadata_plan(1)
            },
        )
        .expect("step with retention");
    assert_eq!(
        step.retention
            .expect("retention selected")
            .retention_floor_seq,
        ChangeSeq(1)
    );

    // A plan naming retention alone is the same opt-in.
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put second file");
    fs.maintenance_step_namespace_blocking(&namespace_id, metadata_plan(1))
        .expect("flush second segment");
    let step = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenancePlan {
                advance_retention: true,
                ..MaintenancePlan::default()
            },
        )
        .expect("retention-only step");
    assert_eq!(
        step.retention
            .expect("retention selected")
            .retention_floor_seq,
        ChangeSeq(2)
    );
    assert_eq!(
        step.metadata_maintenance, None,
        "an unnamed action reports nothing"
    );
}

#[test]
fn a_plan_that_names_nothing_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "empty-plan-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let error = fs
        .maintenance_step_namespace_blocking(&namespace_id, MaintenancePlan::default())
        .expect_err("an empty plan should fail");
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    match error {
        RuntimeError::Config(message) => assert!(
            message.contains("at least one action"),
            "unexpected message: {message}"
        ),
        other => panic!("expected config error, got {other:?}"),
    }
}

#[test]
fn the_typed_wrappers_are_single_action_steps() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "typed-wrapper-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    assert_eq!(
        fs.flush_wal_blocking(&namespace_id)
            .expect("flush an empty tail")
            .wal_flush,
        WalFlushStepOutcome::NotNeeded,
        "the wrapper folds a tail; it does not publish a manifest for a namespace with none"
    );

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/first.txt",
        b"first",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put first file");
    let flushed = fs
        .flush_wal_blocking(&namespace_id)
        .expect("flush the tail");
    assert_eq!(
        flushed.wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(1)
        }
    );
    assert_eq!(
        flushed.reorganize,
        ReorganizeStepOutcome::NotNeeded,
        "the upkeep pass reports its reorganization half rather than hiding it"
    );

    // The same request written out longhand: a metadata-only plan at a
    // one-segment threshold, which is all the wrapper is.
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put second file");
    let longhand = fs
        .maintenance_step_namespace_blocking(&namespace_id, metadata_plan(1))
        .expect("upkeep-only step");
    assert_eq!(
        upkeep(&longhand).wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(2)
        },
        "the wrapper and the step it delegates to are the same request"
    );

    // Retention keeps a name of its own because of what it costs, and the
    // floor it reports is the one the restricted step reports.
    let checkpoint = fs
        .create_checkpoint_blocking(&namespace_id)
        .expect("create checkpoint");
    let advanced = fs
        .advance_retention_floor_blocking(&namespace_id)
        .expect("advance retention");
    assert_eq!(advanced.retention_floor_seq, checkpoint.checkpoint_seq);
    let longhand = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenancePlan {
                advance_retention: true,
                ..MaintenancePlan::default()
            },
        )
        .expect("retention-only step");
    assert_eq!(
        longhand
            .retention
            .expect("retention selected")
            .retention_floor_seq,
        advanced.retention_floor_seq,
        "advancing an already-advanced floor is idempotent through either name"
    );
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

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put first file");
    fs.maintenance_step_namespace_blocking(&namespace_id, metadata_plan(1))
        .expect("first maintenance step");

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put second file");
    let step = fs
        .maintenance_step_namespace_blocking(&namespace_id, metadata_plan(1))
        .expect("second maintenance step");
    assert_eq!(
        upkeep(&step).wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(2)
        }
    );

    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after delta wal flush");
    assert_eq!(status.current_manifest_no, Some(ManifestNo(2)));
    assert_eq!(status.wal_tail_segments, 0);

    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let root = block_on(loonfs_core::control::load_namespace_metadata_root_control(
        &raw_store,
        &namespace_id,
    ))
    .expect("metadata root");
    let manifest_key =
        metadata_manifest_object(&namespace_id, &root.state.manifest.manifest_object_id);
    let manifest_bytes = block_on(raw_store.get(&manifest_key, None))
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    // A WAL flush only appends: the base marker stays where the first
    // published manifest put it until reorganization folds the delta runs.
    assert_eq!(manifest.payload.base_seq, ChangeSeq(1));
    let delta_files = manifest
        .payload
        .segments
        .iter()
        .filter(|descriptor| descriptor.level == 0)
        .collect::<Vec<_>>();
    assert!(!delta_files.is_empty());
    assert!(delta_files
        .iter()
        .any(|descriptor| descriptor.run_seq == ChangeSeq(2)));
}

#[test]
fn a_standalone_admin_drives_metadata_compaction_itself() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "manual-compaction-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    // A namespace that has published no manifest has no runs to rebuild.
    assert_eq!(
        block_on(fs.admin.compact_metadata(&namespace_id)).expect("compact an empty namespace"),
        MetadataCompactionOutcome::NoWork
    );

    // Enough flushes to put the manifest's delta run count over the fold
    // trigger, which is what makes the planner select a group at all.
    for index in 0..9 {
        fs.put_file_bytes_blocking(
            &namespace_id,
            &format!("/docs/file-{index}.txt"),
            format!("file {index}").as_bytes(),
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .expect("put a file");
        fs.flush_wal_blocking(&namespace_id)
            .expect("flush the tail");
    }
    let manifest_before = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status before the call")
        .current_manifest_no;

    // Nothing here has outgrown a bounded step, so the plan is a merge and
    // this call reports exactly that: it published the unit the planner
    // chose, as the next maintenance step would have, and ran no job.
    assert_eq!(
        block_on(fs.admin.compact_metadata(&namespace_id)).expect("plan a compaction"),
        MetadataCompactionOutcome::BoundedMergePublished
    );
    assert_ne!(
        fs.namespace_diagnostics_blocking(&namespace_id)
            .expect("status after the call")
            .current_manifest_no,
        manifest_before,
        "the call must run the same planner a maintenance step runs, and publish what it chose"
    );
}

fn create_directory_request(commit_id: &str, absolute_path: &str) -> CommitRequest {
    CommitRequest::single(
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
fn maintenance_step_counts_segments_not_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-segment-count-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
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
    assert_eq!(status.wal_tail_segments, 1);

    let step = fs
        .maintenance_step_namespace_blocking(&namespace_id, metadata_plan(2))
        .expect("maintenance step");
    assert_eq!(upkeep(&step).wal_flush, WalFlushStepOutcome::NotNeeded);

    fs.mutate_blocking(&namespace_id, create_directory_request("create-c", "/c"))
        .expect("second segment commit");

    let step = fs
        .maintenance_step_namespace_blocking(&namespace_id, metadata_plan(2))
        .expect("maintenance step at segment threshold");
    assert_eq!(step.status_before.head_seq, ChangeSeq(3));
    assert_eq!(step.status_before.wal_tail_segments, 2);
    assert_eq!(
        upkeep(&step).wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(3)
        }
    );
}

#[test]
fn maintenance_step_treats_metadata_root_cas_loss_as_benign_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(temp_dir.path(), &namespace_id));
    let object_store = raw_store.store();
    let fs = open_runtime(object_store, "step-race-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    raw_store.fail_root_cas();
    let step = fs
        .maintenance_step_namespace_blocking(&namespace_id, metadata_plan(1))
        .expect("maintenance step should not fail on metadata root publish race");

    assert_eq!(
        upkeep(&step).wal_flush,
        WalFlushStepOutcome::RetriesExhausted {
            observed_head_seq: ChangeSeq(1)
        }
    );
    let status = fs
        .namespace_diagnostics_blocking(&namespace_id)
        .expect("status after lost race");
    assert_eq!(status.current_manifest_no, None);
    assert_eq!(status.wal_tail_segments, 1);
}

#[test]
fn a_created_snapshot_is_listed_with_its_snapshot_owner() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "snapshot-create-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    let expires_at_ms = 4_102_444_800_000;
    let snapshot = block_on(fs.admin.create_snapshot(
        &namespace_id,
        CreateSnapshotOptions {
            name: "report-run".to_owned(),
            expires_at_ms,
        },
    ))
    .expect("create snapshot");
    assert_eq!(
        snapshot.owner,
        CheckpointOwnerSummary::Snapshot {
            name: "report-run".to_owned(),
        }
    );

    let listed = block_on(collect_checkpoints(&fs.admin, &namespace_id)).expect("list checkpoints");
    let listed_snapshot = listed
        .checkpoints
        .iter()
        .find(|checkpoint| checkpoint.checkpoint_id == snapshot.checkpoint_id)
        .expect("the snapshot is in the checkpoint listing");
    assert_eq!(listed_snapshot.owner, snapshot.owner);
    assert_eq!(listed_snapshot.expires_at_ms, Some(expires_at_ms));
    assert_eq!(listed_snapshot.checkpoint_seq, snapshot.checkpoint_seq);
}

#[tokio::test]
async fn snapshot_create_recovers_an_ambiguously_landed_record_write() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-ambiguous-write");
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
            OperationClass::PutCreateIfAbsent,
            InjectedError::Transport("lost checkpoint write acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-write").await;
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    store.fail_next(1);

    let snapshot = fs
        .admin
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "report-run".to_owned(),
                expires_at_ms: u64::MAX,
            },
        )
        .await
        .expect("reconcile the durable snapshot record");

    assert_eq!(store.attempts(), 2);
    let listed = collect_checkpoints(&fs.admin, &namespace_id)
        .await
        .expect("list checkpoints");
    assert_eq!(listed.checkpoints.len(), 1);
    assert_eq!(listed.checkpoints[0].checkpoint_id, snapshot.checkpoint_id);
}

#[tokio::test]
async fn snapshot_extension_recovers_an_ambiguously_landed_record_write() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-ambiguous-extension");
    let checkpoint_key_prefix = checkpoint_prefix(&namespace_id);
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(checkpoint_key_prefix),
            OperationClass::CompareAndSwap,
            InjectedError::Transport("lost snapshot extension acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-extension").await;
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let snapshot = fs
        .admin
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "report-run".to_owned(),
                expires_at_ms: u64::MAX - 1,
            },
        )
        .await
        .expect("create snapshot");
    store.fail_next(1);

    let extended = fs
        .admin
        .extend_snapshot(&namespace_id, &snapshot.checkpoint_id, u64::MAX, u64::MAX)
        .await
        .expect("reconcile the durable snapshot extension");

    assert_eq!(extended.expires_at_ms, u64::MAX);
    assert_eq!(store.attempts(), 1);
}

#[tokio::test]
async fn snapshot_release_recovers_an_ambiguously_landed_record_write() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-ambiguous-release");
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
            OperationClass::CompareAndSwap,
            InjectedError::Transport("lost snapshot release acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "snapshot-ambiguous-release").await;
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let snapshot = fs
        .admin
        .create_snapshot(
            &namespace_id,
            CreateSnapshotOptions {
                name: "report-run".to_owned(),
                expires_at_ms: u64::MAX,
            },
        )
        .await
        .expect("create snapshot");
    store.fail_next(1);

    fs.admin
        .release_snapshot(&namespace_id, &snapshot.checkpoint_id)
        .await
        .expect("reconcile the durable snapshot release");

    assert_eq!(store.attempts(), 1);
    let listed = collect_checkpoints(&fs.admin, &namespace_id)
        .await
        .expect("list checkpoints");
    assert!(listed
        .checkpoints
        .iter()
        .all(|checkpoint| checkpoint.checkpoint_id != snapshot.checkpoint_id));
}

#[tokio::test]
async fn fork_recovers_an_ambiguously_landed_checkpoint_renewal() {
    let temp_dir = tempdir().expect("tempdir");
    let source = namespace_id("fork-ambiguous-renewal-source");
    let target = namespace_id("fork-ambiguous-renewal-target");
    let store = Arc::new(
        FailStore::new(
            LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
            KeyPredicate::prefix(checkpoint_prefix(&source)),
            OperationClass::CompareAndSwap,
            InjectedError::Transport("lost fork checkpoint renewal acknowledgement".to_owned()),
        )
        .apply_then_fail(),
    );
    let object_store: SharedObjectStore = store.clone();
    let fs = open_runtime_async(object_store, "fork-ambiguous-renewal").await;
    fs.create_namespace(&source, CreateNamespaceOptions::default())
        .await
        .expect("create source namespace");
    fs.put_file_bytes(
        &source,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .await
    .expect("write source file");
    store.fail_next(1);

    let forked = fs
        .writer
        .fork_namespace(&source, &target)
        .await
        .expect("reconcile the durable fork checkpoint renewal");

    assert_eq!(store.attempts(), 1);
    assert_eq!(forked.namespace_id, target);
    let checkpoints = collect_checkpoints(&fs.admin, &source)
        .await
        .expect("list source checkpoints");
    let fork_checkpoint = checkpoints
        .checkpoints
        .iter()
        .find(|checkpoint| {
            matches!(
                &checkpoint.owner,
                CheckpointOwnerSummary::Fork {
                    target_namespace_id,
                    ..
                } if target_namespace_id == &target
            )
        })
        .expect("fork checkpoint remains installed");
    assert_eq!(fork_checkpoint.namespace_id, source);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_snapshot_creates_cannot_both_claim_the_last_quota_slot() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("snapshot-quota-race");
    let checkpoint_key_prefix = checkpoint_prefix(&namespace_id);
    let checkpoint_writes = Arc::new(AtomicUsize::new(0));
    let checkpoint_writes_seen = checkpoint_writes.clone();
    let checkpoint_write_gate = Arc::new(BlockingStore::matching(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        move |operation| {
            let matches = operation.key().starts_with(&checkpoint_key_prefix)
                && matches!(
                    operation.kind(),
                    OperationKind::Put { .. } | OperationKind::PutStreamed { .. }
                );
            if matches {
                checkpoint_writes_seen.fetch_add(1, Ordering::SeqCst);
            }
            matches
        },
    ));
    let checkpoint_list_prefix = checkpoint_prefix(&namespace_id);
    let checkpoint_lists = Arc::new(AtomicUsize::new(0));
    let checkpoint_lists_seen = checkpoint_lists.clone();
    let checkpoint_list_gate = Arc::new(BlockingStore::matching(
        checkpoint_write_gate.clone(),
        move |operation| {
            let matches = operation.key() == checkpoint_list_prefix
                && matches!(operation.kind(), OperationKind::List);
            if matches {
                checkpoint_lists_seen.fetch_add(1, Ordering::SeqCst);
            }
            matches
        },
    ));
    let object_store: SharedObjectStore = checkpoint_list_gate.clone();
    let fs = open_runtime_async(object_store, "snapshot-quota-race").await;
    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    checkpoint_list_gate.arm();
    checkpoint_write_gate.arm();
    let first_admin = fs.admin.clone();
    let first_namespace = namespace_id.clone();
    let first = tokio::spawn(async move {
        first_admin
            .create_snapshot_with_quota(
                &first_namespace,
                CreateSnapshotOptions {
                    name: "first".to_owned(),
                    expires_at_ms: u64::MAX,
                },
                0,
                1,
            )
            .await
    });
    let second_admin = fs.admin.clone();
    let second_namespace = namespace_id.clone();
    let second = tokio::spawn(async move {
        second_admin
            .create_snapshot_with_quota(
                &second_namespace,
                CreateSnapshotOptions {
                    name: "second".to_owned(),
                    expires_at_ms: u64::MAX,
                },
                0,
                1,
            )
            .await
    });

    wait_for_operations(&checkpoint_writes, 2).await;
    checkpoint_write_gate.release();
    wait_for_operations(&checkpoint_lists, 2).await;
    checkpoint_list_gate.release();

    let first = first.await.expect("first create task");
    let second = second.await.expect("second create task");
    assert_core_error_kind(first, ErrorCode::SnapshotQuotaExceeded);
    assert_core_error_kind(second, ErrorCode::SnapshotQuotaExceeded);
    let listed = collect_checkpoints(&fs.admin, &namespace_id)
        .await
        .expect("list checkpoints after raced creates");
    assert!(listed.checkpoints.is_empty());
}

async fn wait_for_operations(counter: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while counter.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {expected} operations"));
}

#[test]
fn tombstoned_namespace_keeps_checkpoint_inventory_and_user_release_available() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let fs = open_runtime(store.clone(), "checkpoint-tombstone-setup");
    let source = namespace_id("source");
    let target = namespace_id("target");

    fs.create_namespace_blocking(&source, CreateNamespaceOptions::default())
        .expect("create source namespace");
    fs.put_file_bytes_blocking(
        &source,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put source file");
    fs.fork_namespace_blocking(&source, &target)
        .expect("fork source namespace");
    let user_checkpoint = fs
        .create_checkpoint_blocking(&source)
        .expect("create user checkpoint");

    let before_delete = block_on(collect_checkpoints(&fs.admin, &source))
        .expect("list checkpoints before deletion");
    let fork_checkpoint = before_delete
        .checkpoints
        .iter()
        .find(|checkpoint| {
            matches!(
                &checkpoint.owner,
                CheckpointOwnerSummary::Fork {
                    target_namespace_id
                } if target_namespace_id == &target
            )
        })
        .expect("fork-owned checkpoint")
        .checkpoint_id
        .clone();

    let deleter = block_on(
        FsWriter::builder_with_store(store.clone())
            .writer_id("checkpoint-tombstone-deleter")
            .build(),
    )
    .expect("build deleting writer");
    block_on(deleter.delete_namespace(&source, DeleteNamespaceOptions::default()))
        .expect("delete source namespace");

    let admin = block_on(
        FsAdmin::builder_with_store(store)
            .actor_id("checkpoint-tombstone-observer")
            .build(),
    )
    .expect("build post-delete admin");
    let listed = block_on(collect_checkpoints(&admin, &source))
        .expect("list checkpoints on deleted namespace");
    assert_eq!(listed.checkpoints.len(), 2);
    assert!(listed
        .checkpoints
        .iter()
        .any(|checkpoint| checkpoint.checkpoint_id == user_checkpoint.checkpoint_id));
    assert!(listed
        .checkpoints
        .iter()
        .any(|checkpoint| checkpoint.checkpoint_id == fork_checkpoint));

    let released = block_on(admin.release_checkpoint(&source, &user_checkpoint.checkpoint_id))
        .expect("release user checkpoint on deleted namespace");
    assert_eq!(released.checkpoint_id, user_checkpoint.checkpoint_id);
    assert_core_error_kind(
        block_on(admin.release_checkpoint(&source, &fork_checkpoint)),
        ErrorCode::InvalidRequest,
    );
    assert_core_error_kind(
        block_on(admin.get_namespace_diagnostics(&source)),
        ErrorCode::NamespaceDeleted,
    );
    assert_core_error_kind(
        block_on(admin.create_checkpoint(
            &source,
            CreateCheckpointOptions {
                name: "after-delete".to_owned(),
                ttl_ms: None,
            },
        )),
        ErrorCode::NamespaceDeleted,
    );
}
