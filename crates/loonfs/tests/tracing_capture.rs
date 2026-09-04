#![allow(clippy::panic)]
// Tracing-capture tests panic in helper assertions for precise diagnostics.

//! Asserts background maintenance conclusions are observable at debug level.
//!
//! This stays out of the crate's `tests/it` harness and keeps its own test
//! binary — its own process — on purpose.
//! Tracing caches per-callsite interest process-globally, and a callsite
//! first exercised on a thread with no subscriber can race the interest
//! rebuild that `set_default` performs. Sibling tests that drive background
//! maintenance on subscriber-less threads hit exactly the callsites this
//! capture greps for, so sharing a binary with them made the capture lose
//! events intermittently under parallel test execution. Alone in its
//! process, the callsites are first hit with the capture subscriber
//! installed, and the assertion is deterministic.

use loonfs::{
    CreateNamespaceOptions, FsWriter, MaintenanceJobId, MaintenanceRegistry, MaintenanceRunner,
    MetadataMaintenanceJob, MetadataMaintenanceOptions, NamespaceId, StoreConfig,
};
use loonfs_core::test_support::append_wal_segments;
use loonfs_core::MutationContext;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tracing_subscriber::fmt::format::FmtSpan;

fn store_config(root: &Path) -> StoreConfig {
    StoreConfig::LocalFs {
        root: root.to_string_lossy().into_owned(),
        key_prefix: None,
    }
}

fn writes_past_wal_tail_threshold() -> u32 {
    u32::try_from(
        MetadataMaintenanceOptions::default()
            .max_wal_tail_segments
            .get()
            + 1,
    )
    .expect("WAL tail threshold plus one should fit in u32")
}

async fn writer(root: &Path) -> FsWriter {
    FsWriter::builder(store_config(root))
        .writer_id("tracing-capture-writer")
        .build()
        .await
        .expect("build writer")
}

async fn fill_wal_tail_past_threshold(root: &Path, namespace_id: &NamespaceId) {
    let store = LocalFsStore::new(root).expect("open tail store");
    append_wal_segments(
        &store,
        namespace_id,
        u64::from(writes_past_wal_tail_threshold()),
        &MutationContext {
            writer_id: loonfs_api::WriterId::parse("tracing-tail-writer").expect("writer id"),
            now_ms: 1_000,
        },
    )
    .await
    .expect("fill WAL tail past threshold");
}

struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("capture lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn background_step_conclusions_emit_debug_events() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        // Closing spans are recorded so the maintenance spans name
        // themselves in the capture whether or not they emitted an event.
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(move || CaptureWriter(Arc::clone(&sink)))
        .finish();
    // Thread-local so the capture cannot leak into other tests; the
    // current-thread runtime keeps spawned background steps on this thread.
    let _guard = tracing::subscriber::set_default(subscriber);

    block_on(async {
        let writer = writer(temp_dir.path()).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        fill_wal_tail_past_threshold(temp_dir.path(), &namespace_id).await;
        let maintenance = writer
            .maintenance_handle("tracing-capture-maintenance")
            .expect("maintenance handle");
        let registry = MaintenanceRegistry::new();
        registry
            .register(Arc::new(MetadataMaintenanceJob::new(maintenance)))
            .expect("metadata job");
        let runner = MaintenanceRunner::builder(registry)
            .build()
            .expect("maintenance runner");
        runner
            .handle()
            .nudge(MaintenanceJobId::METADATA, &namespace_id);
        runner.drain().await.expect("maintenance quiesces");
        runner.shutdown().await.expect("runner shutdown");
    });

    let log = String::from_utf8(captured.lock().expect("capture lock").clone())
        .expect("captured log is utf8");
    // Two records, one per layer: what the executor did, and what the runner
    // made of it. Fields are matched with their `=` so a span carrying the
    // same word cannot satisfy the assertion.
    let step = find_event(&log, "metadata maintenance pass concluded");
    for field in ["wal_flush=", "reorganize=", "wal_tail_segments_before="] {
        assert!(step.contains(field), "missing `{field}` in: {step}");
    }
    let admission = find_event(&log, "maintenance pass settled");
    // What the step cost, in both halves: waiting for a permit, then running.
    for field in [
        "job=",
        "namespace_id=",
        "conclusion=",
        "queued_ms=",
        "elapsed_ms=",
    ] {
        assert!(
            admission.contains(field),
            "missing `{field}` in: {admission}"
        );
    }
    // What the queue looked like when the runner claimed permits.
    let dispatch = find_event(&log, "maintenance keys dispatched");
    for field in ["dispatched=", "ready_queued=", "oldest_queued_ms="] {
        assert!(dispatch.contains(field), "missing `{field}` in: {dispatch}");
    }
    // Record the WAL flush phase.
    let span_evidence = "loonfs.phase{phase=\"wal_flush\"";
    assert!(
        log.contains(span_evidence),
        "missing span evidence `{span_evidence}` in:\n{log}"
    );
    assert!(
        !log.contains("compaction"),
        "maintenance still traces as compaction:\n{log}"
    );
}

fn find_event<'log>(log: &'log str, message: &str) -> &'log str {
    log.lines()
        .find(|line| line.contains(message))
        .unwrap_or_else(|| panic!("`{message}` missing from captured tracing:\n{log}"))
}
