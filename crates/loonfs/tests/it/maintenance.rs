//! Namespace status, maintenance steps, checkpointing, and retention hooks.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::publish::{parse_mutation_path, CommitRequest, FilesystemOperation};
use loonfs::{
    ChangeSeq, CheckpointOwnerSummary, CommitId, CreateCheckpointOptions, CreateNamespaceOptions,
    DeleteNamespaceOptions, ErrorCode, FsAdmin, FsWriter, MaintenancePlan, ManifestNo,
    MetadataCompactionOutcome, NamespaceId, PutFileOptions, ReorganizeStepOutcome, RuntimeError,
    SharedObjectStore, WalFlushStepOutcome,
};
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, ControlObjectEnvelope, ControlObjectKind,
    HeadState, HeadStateEnvelope,
};
use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_objectstore::keys::{metadata_manifest_object, wal_head, wal_segment_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass};
use std::sync::Arc;
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
    assert_eq!(store.count(OperationClass::List), 0);
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

/// The documented escape for a namespace whose WAL tail already outran the
/// pointer window the head was published with: status refuses to answer,
/// and an explicit flush is what repairs it.
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
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
    assert!(
        error
            .to_string()
            .contains("does not reach the tail boundary"),
        "unexpected message: {error}"
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

/// A namespace whose head is gone is a namespace that does not exist:
/// there is no third answer between present and absent.
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
    assert_eq!(step.metadata, None, "an unnamed action reports nothing");
}

/// A plan that names nothing is not a step, whichever surface built it.
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

/// The typed names are one name each over the one step path: what they do
/// is exactly the step with that one action named, and what they report
/// agrees with it.
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
        .expect("status after l0 wal flush");
    assert_eq!(status.current_manifest_no, Some(ManifestNo(2)));
    assert_eq!(status.wal_tail_segments, 0);

    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let root = block_on(loonfs_core::control::load_namespace_metadata_root_control(
        &raw_store,
        &namespace_id,
    ))
    .expect("metadata root");
    let manifest_key = metadata_manifest_object(&namespace_id, &root.state.manifest_object_id);
    let manifest_bytes = block_on(raw_store.get(&manifest_key, None))
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    // A WAL flush only appends: the base marker stays where the first
    // published manifest put it until reorganization folds the delta runs.
    assert_eq!(manifest.payload.base_seq, ChangeSeq(1));
    let l0_files = manifest
        .payload
        .segments
        .iter()
        .filter(|descriptor| descriptor.level == 0)
        .collect::<Vec<_>>();
    assert!(!l0_files.is_empty());
    assert!(l0_files
        .iter()
        .any(|descriptor| descriptor.run_seq == ChangeSeq(2)));
}

/// A handle with no background work behind it can still run the one piece of
/// upkeep a bounded step cannot do itself.
///
/// This is the manual deployment story: an operator drives bounded steps and
/// then drives this, and neither needs a scheduler. The call plans exactly as
/// a step does — including folding one bounded unit when that is what the
/// namespace needs — and reports what it ran.
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

    // Enough flushes to put the manifest's L0 run count over the fold
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
        WalFlushStepOutcome::RaceLost {
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
        PutFileOptions::new(loonfs_test_support::test_actor()),
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
