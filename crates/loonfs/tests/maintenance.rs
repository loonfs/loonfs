//! Namespace status, maintenance steps, checkpointing, and retention hooks.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

mod common;

use common::*;
use loonfs::{
    ChangeSeq, CommitId, CommitOp, CommitRequest, CreateNamespaceOptions, ErrorCode, InodeId,
    MaintenanceStepOptions, ManifestId, PutFileOptions, RuntimeError, SharedObjectStore,
    WalFlushStepOutcome,
};
use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_objectstore::keys::{metadata_manifest_object, namespace_config, wal_segment_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn namespace_status_reports_wal_tail_segments() {
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
        .namespace_status_blocking(&namespace_id)
        .expect("status for new namespace");
    assert_eq!(status.namespace_id, namespace_id);
    assert_eq!(status.head_seq, ChangeSeq(0));
    assert_eq!(status.current_manifest_id, Some(ManifestId(0)));
    assert_eq!(status.wal_tail_segments, 0);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    store.reset();
    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after commit");
    assert_eq!(status.head_seq, ChangeSeq(1));
    assert_eq!(status.current_manifest_id, Some(ManifestId(0)));
    assert_eq!(status.wal_tail_segments, 1);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));
    assert_eq!(store.count(OperationClass::List), 0);
}

#[test]
fn namespace_status_counts_wal_tail_without_reading_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    // Count only WAL segment reads: the head's chain pointers must be
    // enough to size a hint-covered tail.
    let store = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::prefix(wal_segment_prefix(namespace_id.as_str())),
    ));
    let fs = open_runtime(store.clone(), "status-count-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for revision in 0..3 {
        fs.put_file_bytes_blocking(
            &namespace_id,
            &format!("/docs/hello-{revision}.txt"),
            format!("rev {revision}").as_bytes(),
            PutFileOptions::default(),
        )
        .expect("put file");
    }

    store.reset();
    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status with populated tail");
    assert_eq!(status.wal_tail_segments, 3);
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[test]
fn namespace_status_and_step_reject_missing_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "missing-status-test");
    let namespace_id = namespace_id("demo");

    assert_core_error_kind(
        fs.namespace_status_blocking(&namespace_id),
        ErrorCode::NamespaceNotFound,
    );
    assert_core_error_kind(
        fs.maintenance_step_namespace_blocking(&namespace_id, MaintenanceStepOptions::default()),
        ErrorCode::NamespaceNotFound,
    );
}

#[test]
fn namespace_status_and_step_reject_partial_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "partial-status-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    block_on(raw_store.delete(&namespace_config(namespace_id.as_str())))
        .expect("delete namespace descriptor");

    assert_core_error_kind(
        fs.namespace_status_blocking(&namespace_id),
        ErrorCode::NamespacePartial,
    );
    assert_core_error_kind(
        fs.maintenance_step_namespace_blocking(&namespace_id, MaintenanceStepOptions::default()),
        ErrorCode::NamespacePartial,
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
        PutFileOptions::default(),
    )
    .expect("put file");

    let step = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenanceStepOptions {
                max_wal_tail_segments: 2,
                gc: None,
                only: None,
            },
        )
        .expect("maintenance step");
    assert_eq!(step.namespace_id, namespace_id);
    assert_eq!(step.status_before.wal_tail_segments, 1);
    assert_eq!(step.wal_flush, WalFlushStepOutcome::NotNeeded);
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
        PutFileOptions::default(),
    )
    .expect("put file");

    let step = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenanceStepOptions {
                max_wal_tail_segments: 1,
                gc: None,
                only: None,
            },
        )
        .expect("maintenance step");
    assert_eq!(step.status_before.head_seq, ChangeSeq(1));
    assert_eq!(
        step.wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(1)
        }
    );

    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after wal flush");
    assert_eq!(status.current_manifest_id, Some(ManifestId(1)));
    assert_eq!(status.wal_tail_segments, 0);

    // Maintenance is record-less: flushing the WAL must leave nothing
    // under `checkpoints/`.
    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let records = block_on(
        raw_store.list_prefix(&loonfs_objectstore::keys::checkpoint_prefix(
            namespace_id.as_str(),
        )),
    )
    .expect("list checkpoint records");
    assert!(
        records.is_empty(),
        "maintenance step created checkpoint records: {records:?}"
    );
}

#[test]
fn maintenance_step_after_existing_manifest_writes_l0_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-l0-run-publish-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put first file");
    fs.maintenance_step_namespace_blocking(
        &namespace_id,
        MaintenanceStepOptions {
            max_wal_tail_segments: 1,
            gc: None,
            only: None,
        },
    )
    .expect("first maintenance step");

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::default(),
    )
    .expect("put second file");
    let step = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenanceStepOptions {
                max_wal_tail_segments: 1,
                gc: None,
                only: None,
            },
        )
        .expect("second maintenance step");
    assert_eq!(
        step.wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(2)
        }
    );

    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after l0 wal flush");
    assert_eq!(status.current_manifest_id, Some(ManifestId(2)));
    assert_eq!(status.wal_tail_segments, 0);

    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let root = block_on(loonfs_core::control::load_namespace_metadata_root_control(
        &raw_store,
        &namespace_id,
    ))
    .expect("metadata root");
    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &root.state.manifest_object_id);
    let manifest_bytes = block_on(raw_store.get(&manifest_key, None))
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    // A WAL flush only appends: the base marker stays the seed's until
    // reorganization folds the delta runs.
    assert_eq!(manifest.payload.base_seq, ChangeSeq(0));
    let l0_files = manifest
        .payload
        .metadata_files
        .iter()
        .filter(|metadata_file| metadata_file.level == 0)
        .collect::<Vec<_>>();
    assert!(!l0_files.is_empty());
    assert!(l0_files
        .iter()
        .any(|metadata_file| metadata_file.run_seq == ChangeSeq(2)));
}

#[test]
fn maintenance_step_counts_segments_not_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-segment-count-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let first_batch = fs.commit_operations_batch_blocking(
        &namespace_id,
        vec![
            CommitRequest {
                commit_id: CommitId::parse("create-a").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("a").expect("valid display name"),
                }],
                message: None,
            },
            CommitRequest {
                commit_id: CommitId::parse("create-b").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: loonfs_api::DisplayName::parse("b").expect("valid display name"),
                }],
                message: None,
            },
        ],
    );
    assert!(first_batch.iter().all(Result::is_ok));

    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after first batch");
    assert_eq!(status.head_seq, ChangeSeq(2));
    assert_eq!(status.wal_tail_segments, 1);

    let step = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenanceStepOptions {
                max_wal_tail_segments: 2,
                gc: None,
                only: None,
            },
        )
        .expect("maintenance step");
    assert_eq!(step.wal_flush, WalFlushStepOutcome::NotNeeded);

    fs.commit_operations_blocking(
        &namespace_id,
        CommitRequest {
            commit_id: CommitId::parse("create-c").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: loonfs_api::DisplayName::parse("c").expect("valid display name"),
            }],
            message: None,
        },
    )
    .expect("second segment commit");

    let step = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenanceStepOptions {
                max_wal_tail_segments: 2,
                gc: None,
                only: None,
            },
        )
        .expect("maintenance step at segment threshold");
    assert_eq!(step.status_before.head_seq, ChangeSeq(3));
    assert_eq!(step.status_before.wal_tail_segments, 2);
    assert_eq!(
        step.wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(3)
        }
    );
}

#[test]
fn maintenance_step_rejects_zero_threshold() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-config-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let error = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenanceStepOptions {
                max_wal_tail_segments: 0,
                gc: None,
                only: None,
            },
        )
        .expect_err("zero threshold should fail");
    match error {
        RuntimeError::Config(message) => assert!(message.contains("max_wal_tail_segments")),
        other => panic!("expected config error, got {other:?}"),
    }
}

#[test]
fn maintenance_step_rejects_thresholds_above_the_write_rejection_cap() {
    // A threshold above the cap would make the step report `not needed`
    // while every publish is rejected with `maintenance_required`.
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "step-config-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let error = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenanceStepOptions {
                max_wal_tail_segments: 129,
                gc: None,
                only: None,
            },
        )
        .expect_err("over-cap threshold should fail");
    match error {
        RuntimeError::Config(message) => assert!(
            message.contains("write-rejection threshold"),
            "unexpected message: {message}"
        ),
        other => panic!("expected config error, got {other:?}"),
    }
}

#[test]
fn maintenance_step_treats_metadata_root_cas_loss_as_benign_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let raw_store = Arc::new(RuntimeStoreProbe::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store = raw_store.store();
    let fs = open_runtime(object_store, "step-race-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    raw_store.fail_root_cas();
    let step = fs
        .maintenance_step_namespace_blocking(
            &namespace_id,
            MaintenanceStepOptions {
                max_wal_tail_segments: 1,
                gc: None,
                only: None,
            },
        )
        .expect("maintenance step should not fail on metadata root publish race");

    assert_eq!(
        step.wal_flush,
        WalFlushStepOutcome::RaceLost {
            observed_head_seq: ChangeSeq(1)
        }
    );
    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after lost race");
    assert_eq!(status.current_manifest_id, Some(ManifestId(0)));
    assert_eq!(status.wal_tail_segments, 1);
}

#[test]
fn checkpoint_and_retention_hooks_are_available() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "maintenance-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let checkpoint = fs
        .create_checkpoint_blocking(&namespace_id)
        .expect("create checkpoint");
    let retention = fs
        .advance_retention_floor_blocking(&namespace_id)
        .expect("advance retention");
    assert_eq!(retention.retention_floor_seq, checkpoint.checkpoint_seq);
}
