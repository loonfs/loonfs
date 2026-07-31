#![allow(clippy::panic)]
// Lifecycle diagnostics deliberately panic with the full unexpected outcome.

//! GrepWorker lifecycle, rebootstrap, query contracts, and GC boundaries.

use crate::common::{control, grep_with, GrepHost};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    CreateNamespaceOptions, DeleteNamespaceOptions, ErrorCode, FsAdmin, FsReader, FsWriter,
    GcConfig, MaintenanceStepKind, MaintenanceStepOptions, NamespaceId, PutFileOptions,
    SharedObjectStore,
};
use loonfs_api::wire::control::CheckpointRecordLifecycle;
use loonfs_api::{sha256_digest, ChangeSeq, GrepRequest, GrepResponse, IndexSegmentId};
use loonfs_grep::keyspace::{
    manifest_key, manifests_prefix, namespace_prefix, root_key, segment_key,
};
use loonfs_grep::root::{
    advance_grep_root, encode_grep_manifest, encode_grep_root, load_grep_root, GrepIndexState,
    GrepLifecycle, GrepManifestEnvelope, GrepManifestId, GrepRootEnvelope, GrepRootPointer,
    GrepRootState,
};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepReorganizeOutcome, GrepService,
    GrepWorker, GREP_GC_GRACE_WINDOW_MS,
};
use loonfs_objectstore::keys::{
    checkpoint_prefix, checkpoint_record, metadata_manifest_object, metadata_manifest_prefix,
    metadata_table_prefix, upload_session_prefix, wal_segment_prefix,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tokio::sync::Semaphore;

fn request(pattern: &str) -> GrepRequest {
    GrepRequest {
        pattern: pattern.to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        limit: None,
        allow_stale: false,
        allow_scan: false,
    }
}

fn nonzero_usize(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test value should be nonzero")
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
            .build_step(namespace_id, policy)
            .await
            .expect("worker build step");
        let fold = worker
            .reorganize_step(namespace_id, policy)
            .await
            .expect("worker fold step");
        if matches!(build.outcome, GrepBuildOutcome::UpToDate { .. })
            && matches!(fold.outcome, GrepReorganizeOutcome::NotNeeded { .. })
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
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("new query reader");
    grep_with(
        &GrepService::new(),
        &reader,
        store,
        namespace_id,
        grep_request,
    )
    .await
}

#[derive(Debug)]
struct BlockingGrepRootCasStore {
    inner: SharedObjectStore,
    root_key: String,
    blocked_once: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
}

impl BlockingGrepRootCasStore {
    fn new(inner: SharedObjectStore, root_key: String) -> Self {
        Self {
            inner,
            root_key,
            blocked_once: AtomicBool::new(false),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }

    async fn wait_until_blocked(&self) {
        self.entered
            .acquire()
            .await
            .expect("blocking store remains open")
            .forget();
    }

    fn unblock(&self) {
        self.release.add_permits(1);
    }
}

#[async_trait]
impl ObjectStore for BlockingGrepRootCasStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let should_block = key == self.root_key
            && matches!(&mode, PutMode::CompareAndSwap { .. })
            && !self.blocked_once.swap(true, Ordering::SeqCst);
        if should_block {
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("blocking store remains open")
                .forget();
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

fn normalize_namespace(mut response: GrepResponse, namespace_id: &NamespaceId) -> GrepResponse {
    response.namespace_id = namespace_id.clone();
    response
}

#[tokio::test]
async fn grep_worker_lifecycle_uses_and_releases_checkpointed_backfill() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-lifecycle").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("lifecycle-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for index in 0..3u32 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/before-{index}.txt"),
                format!("checkpoint needle {index}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write preexisting file");
    }

    let worker = worker(&store).await;
    let enabled = worker.enable(&namespace_id).await.expect("enable");
    assert!(matches!(
        enabled,
        loonfs_grep::GrepEnableOutcome::Enabled { .. }
    ));
    let again = worker
        .enable(&namespace_id)
        .await
        .expect("idempotent enable");
    assert!(matches!(
        again,
        loonfs_grep::GrepEnableOutcome::AlreadyEnabled { .. }
    ));
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load root")
        .expect("root exists");
    let GrepLifecycle::Backfilling { checkpoint_id, .. } = root.state().lifecycle() else {
        panic!(
            "enable must publish checkpointed backfill: {:?}",
            root.state()
        );
    };
    let checkpoint_id = checkpoint_id.clone();

    let policy = GramIndexBuildPolicy {
        max_files_per_step: NonZeroUsize::MIN,
        ..GramIndexBuildPolicy::default()
    };
    let first = worker
        .build_step(&namespace_id, policy)
        .await
        .expect("first backfill page");
    assert!(matches!(
        first.outcome,
        GrepBuildOutcome::Published {
            materialized: false,
            ..
        }
    ));
    let error = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect_err("backfill is not materialized");
    assert_eq!(error.code(), ErrorCode::NotSupported);

    drive_worker_to_current(&worker, &namespace_id, policy).await;
    let checkpoint = control::checkpoint_record(&store, &namespace_id, &checkpoint_id)
        .await
        .expect("released checkpoint record remains until core GC");
    assert!(matches!(
        checkpoint.state,
        CheckpointRecordLifecycle::Released { .. }
    ));
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("materialized query");
    assert_eq!(response.matches.len(), 3);
    let materialized_root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load materialized root")
        .expect("root exists");
    let materialized_segment = segment_key(
        &namespace_id,
        &materialized_root.state().segments()[0].segment_id,
    );

    assert_eq!(
        worker.disable(&namespace_id).await.expect("disable"),
        loonfs_grep::GrepDisableOutcome::Disabled
    );
    worker
        .garbage_collect_namespace(&namespace_id, u64::MAX)
        .await
        .expect("collect disabled segments");
    assert!(
        store
            .head(&materialized_segment)
            .await
            .expect("head disabled segment")
            .is_none(),
        "disable must leave segments for grep-owned GC"
    );
    let disabled_root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load disabled root")
        .expect("disabled root remains");
    assert!(matches!(
        disabled_root.state().lifecycle(),
        GrepLifecycle::Disabled
    ));
    assert_eq!(
        worker
            .disable(&namespace_id)
            .await
            .expect("idempotent disable"),
        loonfs_grep::GrepDisableOutcome::NotEnabled
    );
    let reenabled = worker.enable(&namespace_id).await.expect("re-enable");
    assert!(matches!(
        reenabled,
        loonfs_grep::GrepEnableOutcome::Enabled { .. }
    ));
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load re-enabled root")
        .expect("root exists");
    assert!(root.state().segments().is_empty());
    assert!(matches!(
        root.state().lifecycle(),
        GrepLifecycle::Backfilling { .. }
    ));
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn retention_gap_and_vanished_checkpoint_restart_fresh_backfill() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-gap").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("gap-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("gap-admin")
        .build()
        .await
        .expect("admin");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    writer
        .put_file_bytes(
            &namespace_id,
            "/gap.txt",
            b"retention gap needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write after watermark");
    admin
        .maintenance_step_namespace(
            &namespace_id,
            MaintenanceStepOptions {
                max_wal_tail_segments: 1,
                only: Some(MaintenanceStepKind::WalFlush),
                ..MaintenanceStepOptions::default()
            },
        )
        .await
        .expect("flush wal");
    let floor = admin
        .maintenance_step_namespace(
            &namespace_id,
            MaintenanceStepOptions {
                only: Some(MaintenanceStepKind::Retention),
                ..MaintenanceStepOptions::default()
            },
        )
        .await
        .expect("advance retention");
    assert!(floor.retention_floor_seq > ChangeSeq(0));

    // The feed can no longer reach the watermark, which the worker must
    // read as "my basis is gone" and answer with a whole new attempt.
    let restart = worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect("gap restart");
    assert!(matches!(
        restart.outcome,
        GrepBuildOutcome::BackfillRestarted { .. }
    ));
    let gap_checkpoint_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;

    // The second trigger: the pinned checkpoint stops pinning its basis
    // mid-backfill. The enumeration says so out loud instead of quietly
    // answering current state, and the worker starts over again.
    admin
        .release_checkpoint(&namespace_id, &gap_checkpoint_id)
        .await
        .expect("remove checkpoint mid-backfill");
    let vanished = worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect("vanished checkpoint restart");
    assert!(matches!(
        vanished.outcome,
        GrepBuildOutcome::BackfillRestarted { .. }
    ));
    // A released record is finished for good, so the new attempt takes a
    // pin of its own: a new id, pinning its basis, with the walk starting
    // from nothing.
    let fresh_checkpoint_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;
    assert_ne!(fresh_checkpoint_id, gap_checkpoint_id);

    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("query after rebootstrap");
    assert_eq!(response.matches.len(), 1);
    writer.shutdown_background().await.expect("shutdown");
}

/// A backfill whose pin has outlived its ttl but has not been released
/// keeps enumerating. Nothing on the checkpoint read path consults a clock:
/// the record is still a garbage-collection root, so the state it pins is
/// still there. The worker only starts over when a pass actually releases
/// it — which the suite above covers.
#[tokio::test]
async fn an_expired_but_unreleased_backfill_pin_keeps_enumerating() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-expiry").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("expiry-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/expiring.txt",
            b"expiring needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    let checkpoint_id = assert_fresh_backfill_attempt(&store, &namespace_id).await;

    // Age the pin out from under the backfill without releasing it.
    let key = checkpoint_record(namespace_id.as_str(), checkpoint_id.as_str());
    let mut record = control::checkpoint_record(&store, &namespace_id, &checkpoint_id)
        .await
        .expect("backfill checkpoint record");
    assert!(
        record.expires_at_ms.is_some(),
        "the backfill pin carries a ttl"
    );
    record.expires_at_ms = Some(record.created_at_ms);
    let expired = loonfs_api::wire::control::CheckpointRecordEnvelope::from_state(
        loonfs_api::wire::control::ControlObjectKind::CheckpointRecord,
        record,
    )
    .expect("expired record envelope");
    store
        .put_overwrite(
            &key,
            Bytes::from(
                loonfs_api::wire::control::encode_control_object(&expired).expect("encode record"),
            ),
        )
        .await
        .expect("write the expired record");

    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let finished = control::checkpoint_record(&store, &namespace_id, &checkpoint_id)
        .await
        .expect("the record survives until core GC reaps it");
    assert!(
        matches!(finished.state, CheckpointRecordLifecycle::Released { .. }),
        "the attempt ran to steady state on that pin and then released it"
    );
    let response = new_query(&store, &namespace_id, &request("needle"))
        .await
        .expect("query after a backfill on an expired pin");
    assert_eq!(response.matches.len(), 1);
    writer.shutdown_background().await.expect("shutdown");
}

/// Asserts the namespace's grep root is at the start of a backfill attempt
/// — nothing indexed, nothing walked, an active checkpoint pinning its basis
/// — and returns the checkpoint that attempt holds.
async fn assert_fresh_backfill_attempt(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> loonfs_api::CheckpointId {
    let root = load_grep_root(&**store, namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists");
    let GrepLifecycle::Backfilling {
        backfill_cursor,
        checkpoint_id,
    } = root.state().lifecycle()
    else {
        panic!("expected a checkpointed backfill: {:?}", root.state());
    };
    assert_eq!(
        *backfill_cursor, None,
        "a rebootstrap restarts the walk from the beginning"
    );
    assert!(
        root.state().segments().is_empty(),
        "a rebootstrap discards the incomplete projection"
    );
    let record = control::checkpoint_record(store, namespace_id, checkpoint_id)
        .await
        .expect("the new attempt's checkpoint record exists");
    assert_eq!(
        record.state,
        CheckpointRecordLifecycle::Active,
        "a fresh backfill attempt must hold a checkpoint that still pins its basis"
    );
    checkpoint_id.clone()
}

/// Every grep segment the namespace's root names, for asserting that an
/// event changed no postings.
async fn grep_segment_ids(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> BTreeSet<IndexSegmentId> {
    load_grep_root(&**store, namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists")
        .state()
        .segments()
        .iter()
        .map(|segment| segment.segment_id.clone())
        .collect()
}

async fn grep_built_through_seq(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> ChangeSeq {
    load_grep_root(&**store, namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists")
        .state()
        .index()
        .built_through_seq
}

fn matched_paths(response: &GrepResponse) -> Vec<String> {
    response
        .matches
        .iter()
        .map(|found| found.absolute_path.as_str().to_owned())
        .collect()
}

/// A backfill of a populated namespace while commits keep landing: the
/// checkpoint enumeration answers the state it pinned, the change feed
/// answers everything after it, and the two phases neither miss a file nor
/// index one twice.
#[tokio::test]
async fn commits_during_backfill_are_indexed_once_by_the_feed_phase() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("backfill-overlap").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("overlap-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for index in 0..4u32 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/before-{index}.txt"),
                format!("overlap needle before {index}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write preexisting file");
    }

    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    // One file per step, so the backfill is genuinely partway through the
    // checkpointed file set when the commits below land.
    let policy = GramIndexBuildPolicy {
        max_files_per_step: NonZeroUsize::MIN,
        ..GramIndexBuildPolicy::default()
    };
    let first = worker
        .build_step(&namespace_id, policy)
        .await
        .expect("first backfill page");
    assert!(
        matches!(
            first.outcome,
            GrepBuildOutcome::Published {
                indexed_revisions: 1,
                materialized: false,
                ..
            }
        ),
        "one file per step must leave the backfill unfinished: {:?}",
        first.outcome
    );

    // Commits strictly after the pinned sequence: one new file, and a
    // replacement of a file the checkpoint already pinned.
    writer
        .put_file_bytes(
            &namespace_id,
            "/during.txt",
            b"overlap needle during\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write during backfill");
    writer
        .put_file_bytes(
            &namespace_id,
            "/before-0.txt",
            b"overlap needle replaced\n",
            PutFileOptions {
                behavior: loonfs::DestinationBehavior::Replace,
                ..PutFileOptions::default()
            },
        )
        .await
        .expect("replace a checkpointed file during backfill");

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
        "a revision the checkpoint pinned but the feed replaced must not match"
    );
    writer.shutdown_background().await.expect("shutdown");
}

/// A move changes no content, so it changes no posting: the index is left
/// exactly as it was and the query answers the file's new path because
/// verification reads current state.
#[tokio::test]
async fn a_move_reindexes_nothing_and_answers_the_new_path() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("move-no-reindex").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("move-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/docs/note.txt",
            b"moved needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write file");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let segments_before = grep_segment_ids(&store, &namespace_id).await;
    let built_before = grep_built_through_seq(&store, &namespace_id).await;

    let moved = writer
        .move_path(
            &namespace_id,
            "/docs/note.txt",
            "/docs/renamed.txt",
            loonfs::MoveOptions::default(),
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
    writer.shutdown_background().await.expect("shutdown");
}

/// A recursive delete hides every match under it and an undelete brings
/// them back, both without touching the index: verification against current
/// state is what decides whether a posting can still produce a match.
#[tokio::test]
async fn a_recursive_delete_hides_matches_and_an_undelete_restores_them() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("delete-undelete").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("delete-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let reader = writer.reader();
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for name in ["a", "b"] {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/docs/{name}.txt"),
                format!("subtree needle {name}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write file");
    }
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let segments_before = grep_segment_ids(&store, &namespace_id).await;
    let docs_inode_id = reader
        .stat_path(&namespace_id, "/docs")
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

    let deleted = writer
        .delete_path(
            &namespace_id,
            "/docs",
            loonfs::DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                ..loonfs::DeleteOptions::default()
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

    writer
        .undelete(
            &namespace_id,
            docs_inode_id,
            deleted.committed_seq,
            "/docs",
            loonfs::UndeleteOptions::default(),
        )
        .await
        .expect("undelete the subtree");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let restored = new_query(&store, &namespace_id, &request("subtree needle"))
        .await
        .expect("query after the undelete");
    assert_eq!(
        matched_paths(&restored),
        vec!["/docs/a.txt", "/docs/b.txt"],
        "an undelete makes the same postings eligible again"
    );
    assert_eq!(
        grep_segment_ids(&store, &namespace_id).await,
        segments_before,
        "an undelete must write no new postings"
    );
    writer.shutdown_background().await.expect("shutdown");
}

/// Grep is a projection beside the filesystem, not inside it: a worker step
/// that fails against unreadable grep state must not disturb a commit
/// running at the same time.
#[tokio::test]
async fn a_failing_worker_step_never_blocks_a_concurrent_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-isolation").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("isolation-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let reader = writer.reader();
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    store
        .put_overwrite(
            &root_key(&namespace_id),
            Bytes::from_static(b"corrupt pointer"),
        )
        .await
        .expect("poison the grep root");

    let (build, commit) = tokio::join!(
        worker.build_step(&namespace_id, GramIndexBuildPolicy::default()),
        writer.put_file_bytes(
            &namespace_id,
            "/during-failure.txt",
            b"isolated needle\n",
            PutFileOptions::default(),
        ),
    );
    let error = build.expect_err("an unreadable grep root fails the step");
    assert_eq!(error.code(), ErrorCode::IndexCorrupt);
    commit.expect("the filesystem commit is unaffected by grep");
    let read = reader
        .get_file_bytes(&namespace_id, "/during-failure.txt")
        .await
        .expect("the committed file is readable");
    assert_eq!(read.bytes, b"isolated needle\n");
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn grep_root_lifecycle_pins_not_materialized_error_surface() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("error-surface").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("error-writer")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store).await;

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

    let missing_manifest_id =
        GrepManifestId::parse("1111111111111111111111111111111111111111111111111111111111111111")
            .expect("valid manifest id");
    write_pointer(&*store, &namespace_id, missing_manifest_id).await;
    assert_corrupt_index_error(
        "missing manifest",
        new_query(&store, &namespace_id, &request("needle")).await,
    );

    let corrupt_manifest_id =
        GrepManifestId::parse("2222222222222222222222222222222222222222222222222222222222222222")
            .expect("valid manifest id");
    store
        .put_overwrite(
            &manifest_key(&namespace_id, &corrupt_manifest_id),
            Bytes::from_static(b"corrupt manifest"),
        )
        .await
        .expect("write corrupt manifest");
    write_pointer(&*store, &namespace_id, corrupt_manifest_id).await;
    assert_corrupt_index_error(
        "corrupt manifest",
        new_query(&store, &namespace_id, &request("needle")).await,
    );

    store
        .put_overwrite(
            &root_key(&namespace_id),
            Bytes::from_static(b"corrupt pointer"),
        )
        .await
        .expect("write corrupt pointer");
    assert_corrupt_index_error(
        "corrupt pointer",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn backfilling_root_without_checkpoint_id_is_index_corrupt() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("missing-backfill-checkpoint").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("missing-checkpoint-writer")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable grep");
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load backfilling root")
        .expect("backfilling root exists");
    let manifest_bytes = store
        .get(
            &manifest_key(&namespace_id, root.manifest_envelope().manifest_id()),
            None,
        )
        .await
        .expect("read backfilling manifest")
        .expect("backfilling manifest exists");
    let corrupt_manifest_id =
        write_manifest_without_checkpoint_id(&*store, &namespace_id, &manifest_bytes).await;
    write_pointer(&*store, &namespace_id, corrupt_manifest_id).await;

    assert_corrupt_index_error(
        "backfilling manifest missing checkpoint id",
        new_query(&store, &namespace_id, &request("needle")).await,
    );
    writer.shutdown_background().await.expect("shutdown");
}

async fn write_manifest_without_checkpoint_id(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
    manifest_bytes: &[u8],
) -> GrepManifestId {
    let mut document: serde_json::Value =
        serde_json::from_slice(manifest_bytes).expect("decode valid manifest document");
    document["payload"]["lifecycle"]
        .as_object_mut()
        .expect("backfilling lifecycle is an object")
        .remove("checkpoint_id")
        .expect("valid backfilling manifest carries checkpoint id");
    let payload_bytes =
        serde_json::to_vec(&document["payload"]).expect("encode corrupt manifest payload");
    let payload_checksum = sha256_digest(&payload_bytes);
    document["payload_checksum"] = serde_json::Value::String(payload_checksum.clone());
    let manifest_id = GrepManifestId::parse(
        payload_checksum
            .strip_prefix("sha256:")
            .expect("sha256 digest should carry its algorithm prefix"),
    )
    .expect("checksum is a valid manifest id");
    store
        .put_overwrite(
            &manifest_key(namespace_id, &manifest_id),
            Bytes::from(serde_json::to_vec(&document).expect("encode corrupt manifest document")),
        )
        .await
        .expect("write manifest missing checkpoint id");
    manifest_id
}

async fn write_pointer(
    store: &dyn ObjectStore,
    namespace_id: &NamespaceId,
    manifest_id: GrepManifestId,
) {
    let envelope =
        GrepRootEnvelope::from_pointer(GrepRootPointer::new(namespace_id.clone(), manifest_id))
            .expect("build pointer");
    store
        .put_overwrite(
            &root_key(namespace_id),
            Bytes::from(encode_grep_root(&envelope).expect("encode pointer")),
        )
        .await
        .expect("write pointer");
}

#[tokio::test]
async fn planless_scan_covers_wal_revisions_at_or_below_index_watermark() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("scan-gap").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("scan-gap-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    let worker = worker(&store).await;
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    writer
        .put_file_bytes(
            &namespace_id,
            "/only-in-wal.txt",
            b"x\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write WAL-only file");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    let head = control::head(&store, &namespace_id).await;
    let metadata_root = control::metadata_root(&store, &namespace_id).await;
    assert!(
        metadata_root.manifest_head_seq < head.seq,
        "the WAL-only revision must sit past metadata materialization"
    );
    let grep_root = loonfs_grep::root::load_grep_root(&*store, &namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists");
    assert_eq!(
        grep_root.state().index().built_through_seq,
        head.seq,
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
    assert_eq!(
        response.matches[0].absolute_path.as_str(),
        "/only-in-wal.txt"
    );

    writer.shutdown_background().await.expect("shutdown");
}

fn assert_not_enabled_error(case: &str, result: loonfs_grep::Result<GrepResponse>) {
    match result {
        Err(error @ GrepError::NotEnabled) => {
            assert_eq!(error.code(), ErrorCode::NotSupported, "code for {case}");
            assert_eq!(
                error.to_string(),
                "feature `grep.index` is not enabled on this namespace",
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
                "feature `grep.index` is enabled but its backfill has not completed on this \
                 namespace",
                "error text for {case}"
            );
        }
        outcome => panic!("expected backfilling error for {case}, got {outcome:?}"),
    }
}

fn assert_corrupt_index_error(case: &str, result: loonfs_grep::Result<GrepResponse>) {
    match result {
        Err(error @ GrepError::CorruptIndex { .. }) => {
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

#[tokio::test]
async fn grep_worker_pins_fold_tail_and_pagination_results() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("worker-results").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("worker-results-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store).await;
    let policy = GramIndexBuildPolicy {
        max_l0_runs: nonzero_usize(2),
        max_mid_runs: nonzero_usize(2),
        ..GramIndexBuildPolicy::default()
    };
    worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    for round in 0..6u32 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/docs/file-{round}.txt"),
                format!("shared needle {round}\nshared needle again {round}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write indexed file");
        worker
            .build_step(&namespace_id, policy)
            .await
            .expect("new build");
        worker
            .reorganize_step(&namespace_id, policy)
            .await
            .expect("new fold");
    }
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    writer
        .put_file_bytes(
            &namespace_id,
            "/tail.txt",
            b"tail-only needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write tail file");

    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load root")
        .expect("root exists");
    let levels: BTreeSet<u32> = root
        .state()
        .segments()
        .iter()
        .map(|segment| segment.level)
        .collect();
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
        assert!(found.absolute_path.starts_with("/docs/file-"));
        assert!(matches!(found.line_number, 1 | 2));
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
    assert_eq!(tail.matches[0].absolute_path, "/tail.txt");
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

    let mut page_request = request("shared needle");
    page_request.limit = Some(1);
    let mut found_matches = BTreeSet::new();
    let mut cursors = BTreeSet::new();
    loop {
        let page = new_query(&store, &namespace_id, &page_request)
            .await
            .expect("query page");
        assert_eq!(page.namespace_id, namespace_id);
        assert_eq!(page.matches.len(), 1);
        let found = &page.matches[0];
        assert!(found_matches.insert((found.absolute_path.clone(), found.line_number)));
        let Some(cursor) = page.next_cursor else {
            break;
        };
        assert!(cursors.insert(cursor.clone()));
        page_request.cursor = Some(cursor);
    }
    assert_eq!(found_matches.len(), 12);
    assert_eq!(cursors.len(), 11);
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn fork_of_grep_enabled_namespace_starts_unmaterialized_without_manifest_state() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let source = NamespaceId::parse("grep-fork-source").expect("source namespace");
    let target = NamespaceId::parse("grep-fork-target").expect("target namespace");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("fork-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&source, CreateNamespaceOptions::default())
        .await
        .expect("create source");
    writer
        .put_file_bytes(
            &source,
            "/source.txt",
            b"fork needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write source");
    let worker = worker(&store).await;
    worker.enable(&source).await.expect("enable source");
    drive_worker_to_current(&worker, &source, GramIndexBuildPolicy::default()).await;
    let source_root_before = load_grep_root(&*store, &source)
        .await
        .expect("load source root")
        .expect("source root exists")
        .state()
        .clone();

    writer
        .fork_namespace(&source, &target)
        .await
        .expect("fork source");

    assert!(
        load_grep_root(&*store, &target)
            .await
            .expect("load target root")
            .is_none(),
        "fork target must have no grep root until explicitly enabled"
    );
    assert_not_enabled_error(
        "fork target",
        new_query(&store, &target, &request("needle")).await,
    );

    // A fresh fork target has published no manifest of its own: its basis
    // is the source manifest its head authorizes.
    let target_basis = control::head(&store, &target)
        .await
        .fork_basis
        .expect("a fork target has a basis manifest");
    let manifest_key = metadata_manifest_object(
        target_basis.source_namespace_id.as_str(),
        &target_basis.source_manifest_object_id,
    );
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read target manifest")
        .expect("target manifest exists");
    let document: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).expect("decode target manifest JSON");
    let payload = document["payload"].as_object().expect("manifest payload");
    assert!(!payload.contains_key("index_files"));
    assert!(!payload.contains_key("features"));

    let source_root_after = load_grep_root(&*store, &source)
        .await
        .expect("reload source root")
        .expect("source root still exists")
        .state()
        .clone();
    assert_eq!(source_root_after, source_root_before);
    let source_response = new_query(&store, &source, &request("fork needle"))
        .await
        .expect("source query after fork");
    assert_eq!(source_response.matches.len(), 1);
    assert_eq!(source_response.matches[0].absolute_path, "/source.txt");
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn checkpoint_backfill_matches_incremental_worker_results() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let backfill_namespace = NamespaceId::parse("equiv-backfill").expect("namespace id");
    let incremental_namespace = NamespaceId::parse("equiv-incremental").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("equiv-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    for namespace_id in [&backfill_namespace, &incremental_namespace] {
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
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
            writer
                .put_file_bytes(
                    namespace_id,
                    &format!("/file-{index}.txt"),
                    format!("equivalence needle {index}\n").as_bytes(),
                    PutFileOptions::default(),
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
        normalize_namespace(incremental, &backfill_namespace),
        backfill
    );
    writer.shutdown_background().await.expect("shutdown");
}

#[tokio::test]
async fn pointer_advance_heals_a_manifest_deleted_after_already_exists() {
    let temp_dir = tempdir().expect("tempdir");
    let base: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("manifest-heal").expect("namespace id");
    let writer = FsWriter::builder_with_store(base.clone())
        .writer_id("manifest-heal-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/needle.txt",
            b"verify and heal needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write indexed file");
    let initial_worker = worker(&base).await;
    initial_worker.enable(&namespace_id).await.expect("enable");
    drive_worker_to_current(
        &initial_worker,
        &namespace_id,
        GramIndexBuildPolicy::default(),
    )
    .await;
    writer.shutdown_background().await.expect("shutdown writer");

    let current = load_grep_root(&*base, &namespace_id)
        .await
        .expect("load current root")
        .expect("root exists");
    let current_state = current.state();
    let next = GrepRootState::new(
        namespace_id.clone(),
        current_state.lifecycle().clone(),
        GrepIndexState::new(
            current_state.index().built_through_seq,
            current_state.index().next_event_index,
            current_state.index().reorganize.clone(),
            current_state.index().next_run_ordinal + 1,
        ),
        current_state.segments().to_vec(),
    )
    .expect("valid successor state");
    let candidate = GrepManifestEnvelope::from_state(next.clone()).expect("candidate manifest");
    let candidate_key = manifest_key(&namespace_id, candidate.manifest_id());
    base.put_if_absent(
        &candidate_key,
        Bytes::from(encode_grep_manifest(&candidate).expect("encode candidate")),
    )
    .await
    .expect("pre-land the content-addressed candidate");

    let blocking = Arc::new(BlockingGrepRootCasStore::new(
        base.clone(),
        root_key(&namespace_id),
    ));
    let store: SharedObjectStore = blocking.clone();
    let advance = advance_grep_root(&*store, &current, &next);
    let collect = async {
        blocking.wait_until_blocked().await;
        let report = worker(&store)
            .await
            .garbage_collect_namespace(&namespace_id, u64::MAX)
            .await;
        let candidate_after_gc = store.head(&candidate_key).await;
        blocking.unblock();
        (report, candidate_after_gc)
    };
    let (advanced, (report, candidate_after_gc)) = tokio::join!(advance, collect);

    assert!(
        report
            .expect("grep GC wins the manifest race")
            .deleted_other_objects
            >= 1
    );
    assert!(candidate_after_gc
        .expect("head candidate after GC")
        .is_none());
    advanced.expect("pointer advance verifies and heals its manifest");
    assert!(store
        .head(&candidate_key)
        .await
        .expect("head healed manifest")
        .is_some());
    let response = new_query(&store, &namespace_id, &request("verify and heal needle"))
        .await
        .expect("query follows the healed pointer");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/needle.txt");
}

#[derive(Debug)]
struct AgedMetadataStore {
    inner: LocalFsStore,
    listed_prefixes: Mutex<Vec<String>>,
}

impl AgedMetadataStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("local store"),
            listed_prefixes: Mutex::new(Vec::new()),
        }
    }

    fn age(mut metadata: ObjectMetadata) -> ObjectMetadata {
        metadata.last_modified_ms = Some(0);
        metadata
    }

    fn clear_listed_prefixes(&self) {
        self.listed_prefixes.lock().expect("prefix lock").clear();
    }

    fn listed_prefixes(&self) -> Vec<String> {
        self.listed_prefixes.lock().expect("prefix lock").clone()
    }
}

#[async_trait]
impl ObjectStore for AgedMetadataStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        Ok(self.inner.head(key).await?.map(Self::age))
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        Ok(self.inner.get_with_metadata(key).await?.map(|mut body| {
            body.metadata = Self::age(body.metadata);
            body
        }))
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await.map(Self::age)
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.listed_prefixes
            .lock()
            .expect("prefix lock")
            .push(prefix.to_owned());
        self.inner.list_prefix_stream(prefix)
    }
}

#[tokio::test]
async fn grep_gc_retains_live_roots_reaps_deleted_namespaces_and_never_crosses_keyspaces() {
    let temp_dir = tempdir().expect("tempdir");
    let aged_store = Arc::new(AgedMetadataStore::new(temp_dir.path()));
    let store: SharedObjectStore = aged_store.clone();
    let live_namespace = NamespaceId::parse("gc-live").expect("namespace id");
    let deleted_namespace = NamespaceId::parse("gc-deleted").expect("namespace id");
    let corrupt_namespace = NamespaceId::parse("gc-corrupt").expect("namespace id");
    let absent_namespace = NamespaceId::parse("gc-absent").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("gc-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("gc-admin")
        .build()
        .await
        .expect("admin");
    for namespace_id in [&live_namespace, &deleted_namespace, &corrupt_namespace] {
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                namespace_id,
                "/needle.txt",
                b"gc needle\n",
                PutFileOptions::default(),
            )
            .await
            .expect("write file");
    }
    let worker = worker(&store).await;
    for namespace_id in [&live_namespace, &deleted_namespace, &corrupt_namespace] {
        worker.enable(namespace_id).await.expect("enable");
        drive_worker_to_current(&worker, namespace_id, GramIndexBuildPolicy::default()).await;
    }
    let live_root = load_grep_root(&*store, &live_namespace)
        .await
        .expect("load live root")
        .expect("root exists");
    let live_segment_key =
        segment_key(&live_namespace, &live_root.state().segments()[0].segment_id);
    let live_manifest_key =
        manifest_key(&live_namespace, live_root.manifest_envelope().manifest_id());
    let orphan_key = segment_key(&live_namespace, &IndexSegmentId::generate());
    store
        .put(
            &orphan_key,
            Bytes::from_static(b"orphan"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write orphan");
    let non_grep_key =
        format!("namespaces/{live_namespace}/metadata/tables/grep-gc-sentinel.sst.zst");
    store
        .put(
            &non_grep_key,
            Bytes::from_static(b"non-grep"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write non-grep sentinel");
    let absent_grep_key = segment_key(&absent_namespace, &IndexSegmentId::generate());
    store
        .put(
            &absent_grep_key,
            Bytes::from_static(b"absent-namespace grep state"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write absent namespace grep state");
    let corrupt_keys = store
        .list_prefix(&namespace_prefix(&corrupt_namespace))
        .await
        .expect("list corrupt namespace grep state");
    store
        .put_overwrite(
            &root_key(&corrupt_namespace),
            Bytes::from_static(b"corrupt root pointer"),
        )
        .await
        .expect("corrupt root pointer");

    writer
        .delete_namespace(&deleted_namespace, DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");
    let live_report = worker
        .garbage_collect_namespace(&live_namespace, GREP_GC_GRACE_WINDOW_MS + 1)
        .await
        .expect("collect live namespace");
    let deleted_report = worker
        .garbage_collect_namespace(&deleted_namespace, GREP_GC_GRACE_WINDOW_MS + 1)
        .await
        .expect("collect deleted namespace");
    let corrupt_report = worker
        .garbage_collect_namespace(&corrupt_namespace, GREP_GC_GRACE_WINDOW_MS + 1)
        .await
        .expect("collect corrupt namespace");
    let absent_report = worker
        .garbage_collect_namespace(&absent_namespace, GREP_GC_GRACE_WINDOW_MS + 1)
        .await
        .expect("collect absent namespace");
    assert!(live_report.deleted_segments >= 1);
    assert!(deleted_report.namespace_reaped);
    assert!(absent_report.namespace_reaped);
    assert!(corrupt_report.namespace_degraded);
    assert!(store
        .head(&live_segment_key)
        .await
        .expect("head live")
        .is_some());
    assert!(store
        .head(&live_manifest_key)
        .await
        .expect("head live manifest")
        .is_some());
    assert_eq!(
        store
            .list_prefix(&manifests_prefix(&live_namespace))
            .await
            .expect("list retained live manifests"),
        vec![live_manifest_key],
        "only the root-referenced manifest remains live"
    );
    assert!(store
        .head(&orphan_key)
        .await
        .expect("head orphan")
        .is_none());
    assert!(store
        .list_prefix(&namespace_prefix(&deleted_namespace))
        .await
        .expect("list deleted grep prefix")
        .is_empty());
    assert!(store
        .list_prefix(&namespace_prefix(&absent_namespace))
        .await
        .expect("list absent grep prefix")
        .is_empty());
    for key in corrupt_keys {
        assert!(
            store
                .head(&key)
                .await
                .expect("head degraded-retained key")
                .is_some(),
            "corrupt live grep state must degrade to retention for `{key}`"
        );
    }
    assert!(store
        .head(&non_grep_key)
        .await
        .expect("head sentinel")
        .is_some());

    let core_only_grep_key = segment_key(&live_namespace, &IndexSegmentId::generate());
    store
        .put(
            &core_only_grep_key,
            Bytes::from_static(b"core-must-ignore"),
            PutMode::CreateIfAbsent,
        )
        .await
        .expect("write grep sentinel");
    aged_store.clear_listed_prefixes();
    admin
        .gc_namespace(&live_namespace, &GcConfig::default())
        .await
        .expect("core gc");
    assert_eq!(
        aged_store.listed_prefixes(),
        vec![
            checkpoint_prefix(live_namespace.as_str()),
            wal_segment_prefix(live_namespace.as_str()),
            metadata_table_prefix(live_namespace.as_str()),
            metadata_manifest_prefix(live_namespace.as_str()),
            checkpoint_prefix(live_namespace.as_str()),
            upload_session_prefix(live_namespace.as_str()),
        ],
        "core GC must list only its six core prefixes"
    );
    assert!(
        store
            .head(&core_only_grep_key)
            .await
            .expect("head grep sentinel")
            .is_some(),
        "core GC must not learn grep keys"
    );
    writer.shutdown_background().await.expect("shutdown");
}
