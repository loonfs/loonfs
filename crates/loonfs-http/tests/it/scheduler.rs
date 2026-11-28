//! A maintenance scheduler that a host builds and drives without its loops.

use loonfs::metrics::DefaultMetricsRecorder;
use loonfs::{LoonFs, Maintenance, NamespaceId, SharedObjectStore};
use loonfs_http::{
    Namespaces, Scheduler, SchedulerConfig, SchedulerState, WriterSessions as _,
    DEFAULT_GC_INTERVAL_MS, DEFAULT_IDLE_SESSION_CLOSE_AFTER_MS, DEFAULT_MAINTENANCE_INTERVAL_MS,
    DEFAULT_MAX_CONCURRENT_MAINTENANCE, DEFAULT_METADATA_RETRY_INTERVAL_MS,
    DEFAULT_SWEEP_INTERVAL_MS,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_test_support::test_actor;
use std::sync::Arc;
use std::time::Duration;

const DAY_MS: u64 = 24 * 60 * 60 * 1_000;

/// Returns the number of WAL objects in the tail of `namespace_id`.
async fn wal_tail_objects(maintenance: &Maintenance, namespace_id: &NamespaceId) -> u64 {
    maintenance
        .diagnostics(namespace_id)
        .await
        .expect("namespace diagnostics")
        .wal_tail_objects
}

/// Checks that a scheduler that a host builds starts no loop, so the store
/// sees no request while two days pass on the host's clock and on the Tokio
/// clock. Then `run_due_steps` folds the tail of the session that the table
/// holds, `run_sweep` folds the tail of a namespace without a session (both
/// past the fold age), and a sweep after `cancel` sends no request.
#[tokio::test(start_paused = true)]
async fn a_scheduler_runs_steps_only_when_its_host_calls_them() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let shared: SharedObjectStore = store.clone();
    let [held, unheld] = ["held", "unheld"].map(namespace_id);

    // A writer that ran before the host started left an unfolded tail.
    let writer = LoonFs::builder_with_store(shared.clone(), loonfs::Context::Service)
        .writer_id(writer_id("before-the-host"))
        .build()
        .await
        .expect("build the writer");
    writer
        .create_namespace(&unheld, &test_actor())
        .await
        .expect("create the namespace");
    writer
        .open_namespace(&unheld)
        .expect("open the namespace")
        .put_file("/tail.txt", b"unfolded", &test_actor())
        .await
        .expect("write the tail");
    let now_ms = writer.now_ms();
    writer.shutdown().await.expect("stop the writer");

    let clock = Arc::new(ManualClock::new(now_ms));
    let runtime = LoonFs::builder_with_store(shared, loonfs::Context::Service)
        .writer_id(writer_id("host"))
        .wall_clock(clock.clone())
        .monotonic_timer(clock.clone())
        .build()
        .await
        .expect("build the host runtime");
    let maintenance = runtime.maintenance();
    let namespaces = Arc::new(Namespaces::<SchedulerState>::new(runtime.clone()));
    let scheduler = Scheduler::new(
        SchedulerConfig {
            max_concurrent_maintenance: DEFAULT_MAX_CONCURRENT_MAINTENANCE,
            maintenance_interval_ms: DEFAULT_MAINTENANCE_INTERVAL_MS,
            metadata_retry_interval_ms: DEFAULT_METADATA_RETRY_INTERVAL_MS,
            gc_interval_ms: DEFAULT_GC_INTERVAL_MS,
            sweep_interval_ms: DEFAULT_SWEEP_INTERVAL_MS,
            idle_session_close_after_ms: DEFAULT_IDLE_SESSION_CLOSE_AFTER_MS,
            sweep_store: true,
        },
        Arc::clone(&namespaces),
        None,
        &DefaultMetricsRecorder::new(),
    );
    runtime
        .create_namespace(&held, &test_actor())
        .await
        .expect("create the namespace");
    let handle = namespaces.open(&held).await.expect("hold the session");
    handle
        .put_file("/tail.txt", b"unfolded", &test_actor())
        .await
        .expect("write the tail");

    store.reset();
    clock.advance_ms(2 * DAY_MS);
    tokio::time::advance(Duration::from_millis(2 * DAY_MS)).await;
    // Lets any task that the scheduler had spawned run its steps.
    for _ in 0..1_000 {
        tokio::task::yield_now().await;
    }
    assert!(
        store.take().is_empty(),
        "a scheduler that its host built runs no step on its own"
    );

    scheduler.run_due_steps().await;
    assert!(!store.take().is_empty());
    assert_eq!(
        wal_tail_objects(&maintenance, &held).await,
        0,
        "the session steps fold the held session's idle tail"
    );
    assert!(
        wal_tail_objects(&maintenance, &unheld).await > 0,
        "the session steps leave a namespace without a session to the sweep"
    );

    store.reset();
    let report = scheduler.run_sweep().await;
    assert!(!store.take().is_empty());
    assert_eq!(report.namespaces, 2);
    assert!(report.complete);
    assert_eq!(
        wal_tail_objects(&maintenance, &unheld).await,
        0,
        "the sweep folds the idle tail of a namespace without a session"
    );

    scheduler.cancel();
    store.reset();
    assert!(!scheduler.run_sweep().await.complete);
    assert!(
        store.take().is_empty(),
        "a sweep after `cancel` starts no step"
    );
    drop(handle);
    runtime.shutdown().await.expect("shut down the runtime");
}
