//! A writable session's own metadata compaction: started by its folds, and
//! cancelled by close and shutdown.

use super::*;
use loonfs_core::control::LoadedManifest;
use loonfs_types::format::manifest::RunTier;
use loonfs_types::format::segment_blocks::{DEFAULT_MAX_DELTA_RUNS, DEFAULT_MIN_COMPACTION_RUNS};
use std::num::NonZeroU64;

/// Each write publishes one WAL object, so each write is folded.
const FOLD_EVERY_WRITE: NonZeroU64 = NonZeroU64::MIN;

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
) -> LoadedManifest {
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load the current manifest")
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
    for index in 0..3 * DEFAULT_MAX_DELTA_RUNS.get() {
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
///
/// The first fold writes each group's base, and every later fold adds a
/// delta run of about the same size, so the fold that brings the manifest to
/// a tier's worth of runs makes compaction due.
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
    for write in 0..DEFAULT_MIN_COMPACTION_RUNS.get() {
        let runs = current_manifest(store, &namespace_id)
            .await
            .payload()
            .runs
            .len();
        if runs == DEFAULT_MIN_COMPACTION_RUNS.get() - 1 {
            return namespace;
        }
        write_and_settle(writer, &namespace, &format!("/file-{write}")).await;
    }
    panic!("every write should leave one more run");
}

/// A budget whose compactions run one at a time.
fn one_compaction_at_a_time() -> ExecutionBudget {
    ExecutionBudget::builder()
        .max_concurrent_compactions(NonZeroUsize::MIN)
        .build()
}

fn delta_runs(manifest: &LoadedManifest) -> usize {
    manifest
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
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .session_fold_at_wal_objects(FOLD_EVERY_WRITE)
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
        delta_runs(&manifest) < DEFAULT_MIN_COMPACTION_RUNS.get(),
        "found {} delta runs",
        delta_runs(&manifest)
    );
    writer.shutdown().await.expect("shut down writer");
}

/// A session whose compaction unit is parked at its first segment write.
/// Compaction became due on the session's own fold.
struct ParkedCompaction {
    _temp_dir: tempfile::TempDir,
    store: Arc<BlockingStore<LocalFsStore>>,
    recorder: Arc<DefaultMetricsRecorder>,
    writer: crate::LoonFs<crate::Writable>,
    namespace_id: NamespaceId,
    namespace: crate::Namespace<crate::Writable>,
}

impl ParkedCompaction {
    async fn manifest(&self) -> LoadedManifest {
        current_manifest(self.store.inner(), &self.namespace_id).await
    }
}

async fn park_a_session_compaction() -> ParkedCompaction {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_segment(),
        OperationSelector::Put,
    ));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .session_fold_at_wal_objects(FOLD_EVERY_WRITE)
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("build writer");
    let namespace = session_one_fold_short(&writer, store.inner(), "parked-compaction").await;
    let namespace_id = namespace.id().clone();

    // The unit waits for a permit until the gate is armed, so the first
    // segment write it parks at is its own.
    let mut permits = Vec::new();
    for _ in 0..crate::DEFAULT_MAX_CONCURRENT_COMPACTIONS.get() {
        permits.push(writer.execution_budget().compaction_permit().await);
    }
    namespace
        .put_file("/file-due", b"body", &loonfs_test_support::test_actor())
        .await
        .expect("put the file whose fold makes compaction due");
    namespace.wait_for_fold().await.expect("the fold settles");
    store.block_next();
    drop(permits);
    bounded(Duration::from_secs(10), store.wait_until_blocked())
        .await
        .expect("the compaction unit reaches its first segment write");
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
    for outcome in ["abandoned", "cancelled", "failed"] {
        assert_eq!(
            compactions(&parked.recorder, outcome),
            0,
            "the parked job must publish over the fold, not end `{outcome}`"
        );
    }
    let manifest = parked.manifest().await;
    assert!(
        delta_runs(&manifest) < DEFAULT_MIN_COMPACTION_RUNS.get(),
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

/// The session's compaction plans each unit from the current manifest and
/// takes the tail from the session's position. So while two of the session's
/// writes wait unfolded above the manifest, and the loop's own unit and the
/// next fold publish manifests under it, the loop reads none of the WAL
/// objects that the session wrote.
#[tokio::test]
async fn a_session_compaction_reads_no_wal_objects() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("compaction-reads").expect("namespace id");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
    ));
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .session_fold_at_wal_objects(FOLD_EVERY_WRITE)
        .build()
        .await
        .expect("build writer");
    let namespace = session_one_fold_short(&writer, store.inner(), namespace_id.as_str()).await;
    let actor = loonfs_test_support::test_actor();

    // The session's compaction finds a unit due after this fold, and waits
    // for a permit while two more writes wait for their fold.
    let mut compaction_permits = Vec::new();
    for _ in 0..crate::DEFAULT_MAX_CONCURRENT_COMPACTIONS.get() {
        compaction_permits.push(writer.execution_budget().compaction_permit().await);
    }
    namespace
        .put_file("/file-due", b"body", &actor)
        .await
        .expect("put the file whose fold makes compaction due");
    namespace.wait_for_fold().await.expect("the fold settles");
    let mut fold_permits = Vec::new();
    for _ in 0..crate::DEFAULT_MAX_CONCURRENT_FOLDS.get() {
        fold_permits.push(writer.execution_budget().fold_permit().await);
    }
    for path in ["/unfolded-1", "/unfolded-2"] {
        namespace
            .put_file(path, b"body", &actor)
            .await
            .expect("put a file whose fold waits for a permit");
    }
    let budget = writer.execution_budget();
    bounded(Duration::from_secs(10), async {
        while budget.stats().compactions_waiting == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the session's compaction waits for a permit");
    let before = current_manifest(store.inner(), &namespace_id)
        .await
        .manifest()
        .manifest_no;
    store.reset();

    drop(compaction_permits);
    bounded(Duration::from_secs(10), async {
        loop {
            let stats = budget.stats();
            if stats.compactions_running + stats.compactions_waiting == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the compaction unit finishes while the folds wait");
    drop(fold_permits);
    bounded(Duration::from_secs(10), writer.drain())
        .await
        .expect("the fold and the compactions settle")
        .expect("drain");

    let after = current_manifest(store.inner(), &namespace_id).await;
    assert!(
        after.manifest().manifest_no.0 >= before.0 + 2,
        "the unit and the fold both published"
    );
    assert!(delta_runs(&after) < DEFAULT_MIN_COMPACTION_RUNS.get());
    assert_eq!(
        store.take_get_keys(),
        Vec::<String>::new(),
        "the compaction loop reads no WAL objects"
    );
    writer.shutdown().await.expect("shut down writer");
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
    let report = bounded(Duration::from_secs(10), close)
        .await
        .expect("close returns once the job reaches its next block")
        .expect("close the session");

    assert!(report.drained_commits.is_some());
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
    bounded(Duration::from_secs(10), shutdown)
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
async fn a_runtime_runs_no_more_compactions_at_once_than_its_compaction_limit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(segment_write_watch(temp_dir.path()));
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .session_fold_at_wal_objects(FOLD_EVERY_WRITE)
        .execution_budget(one_compaction_at_a_time())
        .build()
        .await
        .expect("build writer");
    let mut sessions = Vec::new();
    for index in 0..4 {
        sessions.push(
            session_one_fold_short(&writer, store.inner(), &format!("compaction-bound-{index}"))
                .await,
        );
    }

    // Every session's next fold makes compaction due while the one permit is
    // held, so all of them are ready to compact when it is released.
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
    store.reset_peak();
    drop(permit);
    bounded(Duration::from_secs(60), writer.drain())
        .await
        .expect("every session's compaction finishes")
        .expect("drain");

    assert_eq!(
        store.peak_namespaces(),
        1,
        "one compaction at a time, across every session"
    );
    for namespace in &sessions {
        let manifest = current_manifest(store.inner(), namespace.id()).await;
        assert!(
            delta_runs(&manifest) < DEFAULT_MIN_COMPACTION_RUNS.get(),
            "`{}` still has {} delta runs",
            namespace.id(),
            delta_runs(&manifest)
        );
    }
    writer.shutdown().await.expect("shut down writer");
}

#[tokio::test]
async fn closing_a_session_does_not_wait_for_another_sessions_compaction() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::metadata_segment(),
        OperationSelector::Put,
    ));
    let writer = crate::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("writer-a"))
        .session_fold_at_wal_objects(FOLD_EVERY_WRITE)
        .execution_budget(one_compaction_at_a_time())
        .build()
        .await
        .expect("build writer");
    let compacting = session_one_fold_short(&writer, store.inner(), "compacting").await;
    let closing = session_one_fold_short(&writer, store.inner(), "closing").await;
    let actor = loonfs_test_support::test_actor();

    // The other session's compaction takes the only permit and parks at its first
    // segment write.
    let permit = writer.execution_budget().compaction_permit().await;
    compacting
        .put_file("/file-due", b"body", &actor)
        .await
        .expect("put the file whose fold makes compaction due");
    compacting.wait_for_fold().await.expect("the fold settles");
    store.block_next();
    drop(permit);
    bounded(Duration::from_secs(10), store.wait_until_blocked())
        .await
        .expect("the other compaction parks holding the permit");
    let parked_manifest_no = current_manifest(store.inner(), compacting.id())
        .await
        .manifest()
        .manifest_no;

    // This session's compaction is due and waits for that permit.
    closing
        .put_file("/file-due", b"body", &actor)
        .await
        .expect("put the file whose fold makes compaction due");
    closing.wait_for_fold().await.expect("the fold settles");
    let report = bounded(Duration::from_secs(10), closing.clone().close())
        .await
        .expect("close does not wait for the other session's compaction")
        .expect("close the session");
    assert!(report.drained_commits.is_some());
    assert_eq!(
        current_manifest(store.inner(), compacting.id())
            .await
            .manifest()
            .manifest_no,
        parked_manifest_no,
        "the other compaction was still parked when close returned"
    );

    store.release();
    writer.drain().await.expect("the other compaction finishes");
    assert!(
        delta_runs(&current_manifest(store.inner(), compacting.id()).await)
            < DEFAULT_MIN_COMPACTION_RUNS.get()
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
        delta_runs(&current_manifest(store.inner(), reopened.id()).await)
            < DEFAULT_MIN_COMPACTION_RUNS.get(),
        "a later fold on a reopened session compacts"
    );
    writer.shutdown().await.expect("shut down writer");
}
