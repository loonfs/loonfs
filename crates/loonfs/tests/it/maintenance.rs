//! Namespace status, maintenance passes, checkpointing, and retention hooks.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::publish::{parse_mutation_path, CommitRequest, FilesystemOperation};
use loonfs::{
    ChangeSeq, CheckpointOwnerSummary, CommitId, CreateCheckpointOptions, CreateNamespaceOptions,
    CreateSnapshotOptions, DeleteNamespaceOptions, ErrorCode, ManifestNo,
    MetadataCompactionOutcome, NamespaceId, PutFileOptions, ReorganizeStepOutcome,
    RunMaintenanceRequest, RunMaintenanceResponse, SharedObjectStore, WalFlushStepOutcome,
};
use loonfs_api::wire::control::{
    decode_control_object, encode_control_object, ControlObjectEnvelope, ControlObjectKind,
    HeadState, HeadStateEnvelope,
};
use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_api::{AdvanceRetentionRequest, GcRequest, MetadataCompactionRequest};
use loonfs_core::test_support::append_wal_segments;
use loonfs_core::MutationContext;
use loonfs_objectstore::keys::{
    checkpoint_prefix, metadata_manifest_object, wal_head, wal_segment_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    FailStore, InjectedError, KeyPredicate, OperationClass, RecordingStore,
};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn namespace_diagnostics_reports_wal_tail_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
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
    let store = Arc::new(RecordingStore::new(
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
    block_on(fs.maintenance.create_checkpoint(
        &source,
        CreateCheckpointOptions {
            name: "durable".to_owned(),
            ttl_ms: None,
        },
    ))
    .expect("create durable checkpoint");
    block_on(fs.maintenance.create_checkpoint(
        &source,
        CreateCheckpointOptions {
            name: "expired".to_owned(),
            ttl_ms: Some(0),
        },
    ))
    .expect("create expired checkpoint");
    block_on(fs.writer.create_snapshot(
        &source,
        CreateSnapshotOptions {
            name: "expired".to_owned(),
            expires_at_ms: 1,
        },
    ))
    .expect("create expired snapshot record");
    block_on(fs.writer.create_snapshot(
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
    block_on(append_wal_segments(
        store.as_ref(),
        &namespace_id,
        u64::try_from(LEGACY_RECENT_SEGMENTS + 4).expect("segment count"),
        &MutationContext {
            writer_id: loonfs_api::WriterId::parse("legacy-tail-writer").expect("writer id"),
            now_ms: 1_000,
        },
    ))
    .expect("build legacy WAL tail");
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
        fs.maintenance_run_namespace_blocking(&namespace_id, metadata_request(1)),
        ErrorCode::NamespaceNotFound,
    );
    assert_core_error_kind(
        fs.maintenance_run_namespace_blocking(
            &namespace_id,
            RunMaintenanceRequest::Gc(GcRequest::default()),
        ),
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
        fs.maintenance_run_namespace_blocking(&namespace_id, metadata_request(1)),
        ErrorCode::NamespaceNotFound,
    );
    assert_core_error_kind(
        fs.maintenance_run_namespace_blocking(
            &namespace_id,
            RunMaintenanceRequest::Gc(GcRequest::default()),
        ),
        ErrorCode::NamespaceNotFound,
    );

    let deleted_namespace = NamespaceId::parse("deleted").expect("namespace id");
    fs.create_namespace_blocking(&deleted_namespace, CreateNamespaceOptions::default())
        .expect("create namespace for deletion");
    block_on(
        fs.writer
            .delete_namespace(&deleted_namespace, DeleteNamespaceOptions::default()),
    )
    .expect("delete namespace");
    let response = fs
        .maintenance_run_namespace_blocking(
            &deleted_namespace,
            RunMaintenanceRequest::Gc(GcRequest::default()),
        )
        .expect("GC accepts a deleted namespace");
    assert!(matches!(response, RunMaintenanceResponse::Gc(_)));
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

    let response = fs
        .maintenance_run_namespace_blocking(&namespace_id, metadata_request(2))
        .expect("maintenance pass");
    assert_eq!(upkeep(&response).wal_flush, WalFlushStepOutcome::NotNeeded);
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

    let response = fs
        .maintenance_run_namespace_blocking(&namespace_id, metadata_request(1))
        .expect("maintenance pass");
    assert_eq!(
        upkeep(&response).wal_flush,
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
        "maintenance pass created checkpoint records: {records:?}"
    );
}

#[test]
fn metadata_run_does_not_advance_retention() {
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

    let response = fs
        .maintenance_run_namespace_blocking(&namespace_id, metadata_request(1))
        .expect("step without retention");
    assert_eq!(
        upkeep(&response).wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(1)
        }
    );
    assert_eq!(
        fs.namespace_diagnostics_blocking(&namespace_id)
            .expect("status")
            .retention_floor_seq,
        ChangeSeq(0)
    );

    let response = fs
        .maintenance_run_namespace_blocking(
            &namespace_id,
            RunMaintenanceRequest::Retention(AdvanceRetentionRequest {}),
        )
        .expect("step with retention");
    let RunMaintenanceResponse::Retention(retention) = response else {
        panic!("retention request returned a different response")
    };
    assert_eq!(retention.retention_floor_seq, ChangeSeq(1));

    // A plan naming retention alone is the same opt-in.
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put second file");
    fs.maintenance_run_namespace_blocking(&namespace_id, metadata_request(1))
        .expect("flush second segment");
    let response = fs
        .maintenance_run_namespace_blocking(
            &namespace_id,
            RunMaintenanceRequest::Retention(AdvanceRetentionRequest {}),
        )
        .expect("retention-only step");
    let RunMaintenanceResponse::Retention(retention) = response else {
        panic!("retention request returned a different response")
    };
    assert_eq!(retention.retention_floor_seq, ChangeSeq(2));
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

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put second file");
    let metadata = fs
        .maintenance_run_namespace_blocking(&namespace_id, metadata_request(1))
        .expect("upkeep-only step");
    assert_eq!(
        upkeep(&metadata).wal_flush,
        WalFlushStepOutcome::Flushed {
            manifest_head_seq: ChangeSeq(2)
        },
        "the metadata request runs only metadata maintenance"
    );

    let checkpoint = fs
        .create_checkpoint_blocking(&namespace_id)
        .expect("create checkpoint");
    let advanced = fs
        .advance_retention_floor_blocking(&namespace_id)
        .expect("advance retention");
    assert_eq!(advanced.retention_floor_seq, checkpoint.checkpoint_seq);
    let retention = fs
        .maintenance_run_namespace_blocking(
            &namespace_id,
            RunMaintenanceRequest::Retention(AdvanceRetentionRequest {}),
        )
        .expect("retention-only step");
    let RunMaintenanceResponse::Retention(retention) = retention else {
        panic!("retention request returned a different response")
    };
    assert_eq!(retention.retention_floor_seq, advanced.retention_floor_seq);

    let gc = fs
        .maintenance_run_namespace_blocking(
            &namespace_id,
            RunMaintenanceRequest::Gc(GcRequest::default()),
        )
        .expect("GC run");
    assert!(matches!(gc, RunMaintenanceResponse::Gc(_)));

    let compaction = fs
        .maintenance_run_namespace_blocking(
            &namespace_id,
            RunMaintenanceRequest::MetadataCompaction(MetadataCompactionRequest {}),
        )
        .expect("metadata compaction run");
    assert!(matches!(
        compaction,
        RunMaintenanceResponse::MetadataCompaction(_)
    ));
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
    fs.maintenance_run_namespace_blocking(&namespace_id, metadata_request(1))
        .expect("first maintenance pass");

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put second file");
    let step = fs
        .maintenance_run_namespace_blocking(&namespace_id, metadata_request(1))
        .expect("second maintenance pass");
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
        .runs
        .iter()
        .filter(|run| run.tier == loonfs_api::wire::manifest::RunTier::Delta)
        .collect::<Vec<_>>();
    assert!(!delta_files.is_empty());
    assert!(delta_files.iter().any(|run| run.run_seq == ChangeSeq(2)));
}

#[test]
fn a_standalone_maintenance_drives_metadata_compaction_itself() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "manual-compaction-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    // A namespace that has published no manifest has no runs to rebuild.
    assert_eq!(
        block_on(fs.maintenance.compact_metadata(&namespace_id))
            .expect("compact an empty namespace")
            .compaction,
        MetadataCompactionOutcome::NotNeeded
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
    // chose, as the next maintenance pass would have, and ran no job.
    assert_eq!(
        block_on(fs.maintenance.compact_metadata(&namespace_id))
            .expect("plan a compaction")
            .compaction,
        MetadataCompactionOutcome::BoundedMergePublished
    );
    assert_ne!(
        fs.namespace_diagnostics_blocking(&namespace_id)
            .expect("status after the call")
            .current_manifest_no,
        manifest_before,
        "the call must run the same planner a maintenance pass runs, and publish what it chose"
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

    let response = fs
        .maintenance_run_namespace_blocking(&namespace_id, metadata_request(2))
        .expect("maintenance pass");
    assert_eq!(upkeep(&response).wal_flush, WalFlushStepOutcome::NotNeeded);

    fs.mutate_blocking(&namespace_id, create_directory_request("create-c", "/c"))
        .expect("second segment commit");

    let response = fs
        .maintenance_run_namespace_blocking(&namespace_id, metadata_request(2))
        .expect("maintenance pass at segment threshold");
    assert_eq!(
        upkeep(&response).wal_flush,
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
        .maintenance_run_namespace_blocking(&namespace_id, metadata_request(1))
        .expect("maintenance pass should not fail on metadata root publish race");

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
    let checkpoints = collect_checkpoints(&fs.maintenance, &source)
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

#[tokio::test]
async fn a_cold_metadata_job_probes_with_its_configured_options() {
    use loonfs::{
        MaintenanceCancellation, MaintenanceJob, MaintenanceProbe, MetadataMaintenanceJob,
    };
    use loonfs::{MetadataCompactionPolicy, MetadataMaintenanceOptions};
    use std::num::NonZeroU64;

    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace = namespace_id("probe-options");
    runtime
        .create_namespace(&namespace, CreateNamespaceOptions::default())
        .await
        .expect("namespace");
    runtime
        .put_file_bytes(
            &namespace,
            "/one.txt",
            b"one",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write one WAL segment");
    drop(runtime);
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-b").await;
    let defaults = MetadataMaintenanceJob::new(runtime.maintenance.clone());
    let eager_flush = MetadataMaintenanceJob::new(runtime.maintenance.clone()).options(
        MetadataMaintenanceOptions {
            max_wal_tail_segments: NonZeroU64::MIN,
            ..MetadataMaintenanceOptions::default()
        },
    );
    assert_eq!(
        defaults.probe(&namespace).await.expect("default probe"),
        MaintenanceProbe::Idle
    );
    assert_eq!(
        eager_flush.probe(&namespace).await.expect("custom probe"),
        MaintenanceProbe::Due
    );
    runtime
        .create_checkpoint(&namespace)
        .await
        .expect("flush the first run");
    runtime
        .put_file_bytes(
            &namespace,
            "/two.txt",
            b"two",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write another segment");
    runtime
        .create_checkpoint(&namespace)
        .await
        .expect("flush a delta below the default trigger");
    drop(runtime);
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-c").await;
    let defaults = MetadataMaintenanceJob::new(runtime.maintenance.clone());
    let immediate = MetadataMaintenanceJob::new(runtime.maintenance.clone()).options(
        MetadataMaintenanceOptions {
            compaction_policy: MetadataCompactionPolicy::CompactImmediately,
            ..MetadataMaintenanceOptions::default()
        },
    );
    assert_eq!(
        defaults
            .probe(&namespace)
            .await
            .expect("default merge probe"),
        MaintenanceProbe::Idle
    );
    assert_eq!(
        immediate
            .probe(&namespace)
            .await
            .expect("immediate merge probe"),
        MaintenanceProbe::Due
    );
    for _ in 0..16 {
        immediate
            .run(&namespace, None, &MaintenanceCancellation::new())
            .await
            .expect("run configured maintenance");
        if immediate
            .probe(&namespace)
            .await
            .expect("probe after progress")
            == MaintenanceProbe::Idle
        {
            assert_eq!(
                runtime
                    .reader
                    .get_file_bytes(&namespace, "/one.txt")
                    .await
                    .expect("first file")
                    .bytes,
                b"one"
            );
            assert_eq!(
                runtime
                    .reader
                    .get_file_bytes(&namespace, "/two.txt")
                    .await
                    .expect("second file")
                    .bytes,
                b"two"
            );
            return;
        }
    }
    panic!("configured maintenance must finish its finite work");
}
