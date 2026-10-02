//! A writable session's own metadata compaction: started by its folds,
//! cancelled by close and shutdown, and run under the runtime's compactor
//! claim.

use super::*;
use crate::{InlineContentPolicy, MetadataCompactionOutcome};
use loonfs_core::control::CurrentManifest;
use loonfs_types::format::manifest::RunTier;
use loonfs_types::format::sst_blocks::DEFAULT_MAX_DELTA_RUNS;

/// Each write leaves one inline byte in the tail, so each write is folded.
fn fold_every_write() -> InlineContentPolicy {
    InlineContentPolicy {
        inline_content_fold_at_bytes: 1,
        ..InlineContentPolicy::default()
    }
}

/// Writes one file and waits for its fold and the compaction that fold
/// starts, so each call leaves one more delta run unless the session
/// compacted.
async fn write_and_settle(
    writer: &crate::LoonFs<crate::Writable>,
    namespace: &crate::Namespace<crate::Writable>,
    path: &str,
) {
    namespace
        .put_file(path, b"body", &loonfs_test_support::test_actor())
        .await
        .expect("put a file");
    namespace
        .wait_for_fold()
        .await
        .expect("the write's fold settles");
    writer.drain().await.expect("the fold's compaction settles");
}

async fn current_manifest<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> CurrentManifest {
    loonfs_core::control::load_namespace_current_manifest(store, namespace_id)
        .await
        .expect("load the current manifest")
        .state
}

/// Writes until a write's fold leaves fewer delta runs than the one before,
/// which only the session's own compaction does.
async fn write_until_the_session_compacts(
    writer: &crate::LoonFs<crate::Writable>,
    namespace: &crate::Namespace<crate::Writable>,
    store: &dyn ObjectStore,
    path_prefix: &str,
) {
    let mut before = 0;
    for index in 0..3 * DEFAULT_MAX_DELTA_RUNS {
        write_and_settle(writer, namespace, &format!("{path_prefix}-{index}")).await;
        let after = delta_runs(&current_manifest(store, namespace.id()).await);
        if after < before {
            return;
        }
        before = after;
    }
    panic!("the session never compacted its own folds");
}

/// Opens a session on a new namespace and writes until one more fold makes
/// compaction due.
async fn session_one_fold_short<S: ObjectStore + ?Sized>(
    writer: &crate::LoonFs<crate::Writable>,
    store: &S,
    name: &str,
) -> crate::Namespace<crate::Writable> {
    let namespace_id = NamespaceId::parse(name).expect("namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for write in 0..=DEFAULT_MAX_DELTA_RUNS {
        if delta_runs(&current_manifest(store, &namespace_id).await) == DEFAULT_MAX_DELTA_RUNS - 1 {
            return namespace;
        }
        write_and_settle(writer, &namespace, &format!("/file-{write}")).await;
    }
    panic!("every write should leave one delta run");
}

/// A budget whose merges run one at a time.
fn one_merge_at_a_time() -> ExecutionBudget {
    ExecutionBudget::builder()
        .max_concurrent_compactions(NonZeroUsize::MIN)
        .build()
}

fn delta_runs(manifest: &CurrentManifest) -> usize {
    manifest
        .envelope
        .payload()
        .runs
        .iter()
        .filter(|run| run.tier == RunTier::Delta)
        .count()
}

fn compactions(recorder: &DefaultMetricsRecorder, outcome: &str) -> u64 {
    let snapshot = recorder.snapshot();
    let entry = snapshot
        .by_name("loonfs.maintenance.compactions")
        .find(|entry| entry.labels == [("outcome", outcome)])
        .unwrap_or_else(|| panic!("no compactions counter for `{outcome}`"));
    match entry.value {
        MetricValue::Counter(value) => value,
        ref other => panic!("expected a counter, found {other:?}"),
    }
}

#[tokio::test]
async fn a_session_compacts_after_its_own_fold_without_any_hint() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("writer-a")
        .inline_content(fold_every_write())
        .build()
        .await
        .expect("build writer");
    let namespace_id = NamespaceId::parse("compacts-itself").expect("namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    write_until_the_session_compacts(&writer, &namespace, store.as_ref(), "/file").await;

    let manifest = current_manifest(store.as_ref(), &namespace_id).await;
    assert!(
        delta_runs(&manifest) < DEFAULT_MAX_DELTA_RUNS,
        "found {} delta runs",
        delta_runs(&manifest)
    );
    writer.shutdown().await.expect("shut down writer");
}

/// A session whose streaming compaction is parked at its first segment
/// write. Compaction became due on the session's own fold.
struct ParkedCompaction {
    _temp_dir: tempfile::TempDir,
    store: Arc<BlockingStore<LocalFsStore>>,
    recorder: Arc<DefaultMetricsRecorder>,
    writer: crate::LoonFs<crate::Writable>,
    namespace_id: NamespaceId,
    namespace: crate::Namespace<crate::Writable>,
}

impl ParkedCompaction {
    async fn manifest(&self) -> CurrentManifest {
        current_manifest(self.store.inner(), &self.namespace_id).await
    }
}

async fn park_a_session_compaction() -> ParkedCompaction {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_segment(),
        OperationClass::Put,
    ));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("writer-a")
        .inline_content(fold_every_write())
        // No run fits a one-byte step, so every compaction streams.
        .execution_budget(
            ExecutionBudget::builder()
                .max_merge_input_bytes(NonZeroUsize::MIN)
                .build(),
        )
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("build writer");
    let namespace = session_one_fold_short(&writer, store.inner(), "parked-compaction").await;
    let namespace_id = namespace.id().clone();

    // The job waits for a permit until the gate is armed, so the first
    // segment write it parks at is its own.
    let mut permits = Vec::new();
    for _ in 0..crate::DEFAULT_MAX_CONCURRENT_COMPACTIONS {
        permits.push(writer.execution_budget().compaction_permit().await);
    }
    namespace
        .put_file("/file-due", b"body", &loonfs_test_support::test_actor())
        .await
        .expect("put the file whose fold makes compaction due");
    namespace.wait_for_fold().await.expect("the fold settles");
    store.block_next();
    drop(permits);
    timeout(Duration::from_secs(10), store.wait_until_blocked())
        .await
        .expect("the streaming compaction reaches its first segment write");
    ParkedCompaction {
        _temp_dir: temp_dir,
        store,
        recorder,
        writer,
        namespace_id,
        namespace,
    }
}

#[tokio::test]
async fn a_session_streaming_compaction_survives_its_own_next_fold() {
    let parked = park_a_session_compaction().await;
    let parked_manifest_no = parked.manifest().await.manifest().manifest_no;

    parked
        .namespace
        .put_file(
            "/during-compaction",
            b"written mid-job",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("put a file while the job is parked");
    parked
        .namespace
        .wait_for_fold()
        .await
        .expect("the fold publishes while the job is parked");
    assert_eq!(
        parked.manifest().await.manifest().manifest_no,
        parked_manifest_no.successor().expect("next manifest"),
        "the fold published around the parked job"
    );

    parked.store.release();
    parked.writer.drain().await.expect("the compaction settles");
    assert!(compactions(&parked.recorder, "completed") >= 1);
    for outcome in ["abandoned", "cancelled", "fenced", "failed"] {
        assert_eq!(
            compactions(&parked.recorder, outcome),
            0,
            "the parked job must publish over the fold, not end `{outcome}`"
        );
    }
    let manifest = parked.manifest().await;
    assert!(
        delta_runs(&manifest) < DEFAULT_MAX_DELTA_RUNS,
        "found {} delta runs",
        delta_runs(&manifest)
    );
    assert_eq!(
        parked
            .writer
            .namespace(&parked.namespace_id)
            .read_file("/during-compaction")
            .await
            .expect("read the file the fold published")
            .bytes,
        b"written mid-job"
    );
    parked.writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn closing_a_session_cancels_its_compaction_and_returns() {
    let parked = park_a_session_compaction().await;
    let parked_manifest_no = parked.manifest().await.manifest().manifest_no;

    let mut close = Box::pin(parked.namespace.clone().close());
    assert!(
        futures::poll!(close.as_mut()).is_pending(),
        "close waits for the parked compaction"
    );
    parked.store.release();
    let report = timeout(Duration::from_secs(10), close)
        .await
        .expect("close returns once the job reaches its next block")
        .expect("close the session");

    assert!(report.was_open);
    assert_eq!(compactions(&parked.recorder, "cancelled"), 1);
    assert_eq!(compactions(&parked.recorder, "completed"), 0);
    assert_eq!(
        parked.manifest().await.manifest().manifest_no,
        parked_manifest_no,
        "a cancelled job publishes nothing"
    );
    parked.writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn shutdown_drains_session_compactions() {
    let parked = park_a_session_compaction().await;
    let parked_manifest_no = parked.manifest().await.manifest().manifest_no;

    let mut shutdown = Box::pin(parked.writer.shutdown());
    assert!(
        futures::poll!(shutdown.as_mut()).is_pending(),
        "shutdown waits for the parked compaction"
    );
    parked.store.release();
    timeout(Duration::from_secs(10), shutdown)
        .await
        .expect("shutdown returns once the job reaches its next block")
        .expect("shut down writer");

    assert_eq!(compactions(&parked.recorder, "cancelled"), 1);
    assert_eq!(compactions(&parked.recorder, "completed"), 0);
    assert_eq!(
        parked.manifest().await.manifest().manifest_no,
        parked_manifest_no,
        "a cancelled job publishes nothing"
    );
}

#[tokio::test]
async fn maintenance_values_from_one_runtime_share_one_compactor_claim() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedStore = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("writer-a")
        .inline_content(fold_every_write())
        .build()
        .await
        .expect("build writer");
    let namespace_id = NamespaceId::parse("shared-claim").expect("namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let epoch_before = current_manifest(store.as_ref(), &namespace_id)
        .await
        .compactor_epoch();

    write_until_the_session_compacts(&writer, &namespace, store.as_ref(), "/before").await;
    assert_eq!(
        current_manifest(store.as_ref(), &namespace_id)
            .await
            .compactor_epoch()
            .0,
        epoch_before.0 + 1,
        "the session's compaction claims the namespace once"
    );

    for index in 0..2 {
        write_and_settle(&writer, &namespace, &format!("/direct-{index}")).await;
    }
    let direct = writer.maintenance(loonfs_test_support::ids::writer_id("direct-maintenance"));
    assert_eq!(
        direct
            .compact_metadata(&namespace_id)
            .await
            .expect("compact through another maintenance value")
            .compaction,
        MetadataCompactionOutcome::BoundedMergePublished
    );

    write_until_the_session_compacts(&writer, &namespace, store.as_ref(), "/after").await;
    assert_eq!(
        current_manifest(store.as_ref(), &namespace_id)
            .await
            .compactor_epoch()
            .0,
        epoch_before.0 + 1,
        "one claim serves the session and the direct call, so neither fences the other"
    );
    writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn a_runtime_runs_no_more_merges_at_once_than_its_compaction_limit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(SegmentWriteWatch::new(temp_dir.path()));
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("writer-a")
        .inline_content(fold_every_write())
        .execution_budget(one_merge_at_a_time())
        .build()
        .await
        .expect("build writer");
    let mut sessions = Vec::new();
    for index in 0..4 {
        sessions.push(
            session_one_fold_short(&writer, &store.inner, &format!("merge-bound-{index}")).await,
        );
    }

    // Every session's next fold makes compaction due while the one permit is
    // held, so all of them are ready to merge when it is released.
    let permit = writer.execution_budget().compaction_permit().await;
    let actor = loonfs_test_support::test_actor();
    futures::future::try_join_all(
        sessions
            .iter()
            .map(|namespace| namespace.put_file("/file-due", b"body", &actor)),
    )
    .await
    .expect("every session writes at once");
    for namespace in &sessions {
        namespace.wait_for_fold().await.expect("the fold settles");
    }
    store.peak_namespaces.store(0, AtomicOrdering::SeqCst);
    drop(permit);
    timeout(Duration::from_secs(60), writer.drain())
        .await
        .expect("every session's compaction finishes")
        .expect("drain");

    assert_eq!(
        store.peak_namespaces.load(AtomicOrdering::SeqCst),
        1,
        "one merge at a time, across every session"
    );
    for namespace in &sessions {
        let manifest = current_manifest(&store.inner, namespace.id()).await;
        assert!(
            delta_runs(&manifest) < DEFAULT_MAX_DELTA_RUNS,
            "`{}` still has {} delta runs",
            namespace.id(),
            delta_runs(&manifest)
        );
    }
    writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn closing_a_session_does_not_wait_for_another_sessions_merge() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_segment(),
        OperationClass::Put,
    ));
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id("writer-a")
        .inline_content(fold_every_write())
        .execution_budget(one_merge_at_a_time())
        .build()
        .await
        .expect("build writer");
    let merging = session_one_fold_short(&writer, store.inner(), "merging").await;
    let closing = session_one_fold_short(&writer, store.inner(), "closing").await;
    let actor = loonfs_test_support::test_actor();

    // The other session's merge takes the only permit and parks at its first
    // segment write.
    let permit = writer.execution_budget().compaction_permit().await;
    merging
        .put_file("/file-due", b"body", &actor)
        .await
        .expect("put the file whose fold makes compaction due");
    merging.wait_for_fold().await.expect("the fold settles");
    store.block_next();
    drop(permit);
    timeout(Duration::from_secs(10), store.wait_until_blocked())
        .await
        .expect("the other merge parks holding the permit");
    let parked_manifest_no = current_manifest(store.inner(), merging.id())
        .await
        .manifest()
        .manifest_no;

    // This session's compaction is due and waits for that permit.
    closing
        .put_file("/file-due", b"body", &actor)
        .await
        .expect("put the file whose fold makes compaction due");
    closing.wait_for_fold().await.expect("the fold settles");
    let report = timeout(Duration::from_secs(10), closing.clone().close())
        .await
        .expect("close does not wait for the other session's merge")
        .expect("close the session");
    assert!(report.was_open);
    assert_eq!(
        current_manifest(store.inner(), merging.id())
            .await
            .manifest()
            .manifest_no,
        parked_manifest_no,
        "the other merge was still parked when close returned"
    );

    store.release();
    writer.drain().await.expect("the other merge finishes");
    assert!(
        delta_runs(&current_manifest(store.inner(), merging.id()).await) < DEFAULT_MAX_DELTA_RUNS
    );
    assert_eq!(
        writer.execution_budget().stats(),
        crate::ExecutionBudgetStats::default(),
        "the cancelled wait took no permit with it"
    );

    let reopened = writer
        .open_namespace(closing.id())
        .expect("reopen the closed namespace");
    write_and_settle(&writer, &reopened, "/after-close").await;
    assert!(
        delta_runs(&current_manifest(store.inner(), reopened.id()).await) < DEFAULT_MAX_DELTA_RUNS,
        "a later fold on a reopened session compacts"
    );
    writer.shutdown().await.expect("shut down writer");
}
