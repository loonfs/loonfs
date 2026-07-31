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
    CreateNamespaceOptions, FsBackgroundWork, FsWriter, MaintenanceStepOptions, NamespaceId,
    PutFileOptions, StoreConfig,
};
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
    u32::try_from(MaintenanceStepOptions::default().max_wal_tail_segments + 1)
        .expect("WAL tail threshold plus one should fit in u32")
}

async fn writer(root: &Path) -> FsWriter {
    FsWriter::builder(store_config(root))
        .writer_id("tracing-capture-writer")
        .background_work(FsBackgroundWork::Enabled)
        .build()
        .await
        .expect("build writer")
}

async fn fill_wal_tail_past_threshold(writer: &FsWriter, namespace_id: &NamespaceId) {
    for round in 0..writes_past_wal_tail_threshold() {
        writer
            .put_file_bytes(
                namespace_id,
                &format!("/docs/file-{round}.txt"),
                b"body",
                PutFileOptions::default(),
            )
            .await
            .expect("put file");
    }
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
        fill_wal_tail_past_threshold(&writer, &namespace_id).await;
        writer
            .wait_for_background_work()
            .await
            .expect("background maintenance quiesces");
    });

    let log = String::from_utf8(captured.lock().expect("capture lock").clone())
        .expect("captured log is utf8");
    // Two records, one per layer: what the executor did, and what the runner
    // made of it. Fields are matched with their `=` so a span carrying the
    // same word cannot satisfy the assertion.
    let step = find_event(&log, "metadata maintenance step concluded");
    for field in [
        "wal_flush=",
        "reorganize=",
        "wal_tail_segments_before=",
        "conclusion=",
    ] {
        assert!(step.contains(field), "missing `{field}` in: {step}");
    }
    let admission = find_event(&log, "maintenance step settled");
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
    // The spans this work runs under say maintenance, which is what it is.
    for span in ["loonfs.maintenance.step", "loonfs.maintenance.wal_flush"] {
        assert!(log.contains(span), "missing span `{span}` in:\n{log}");
    }
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
