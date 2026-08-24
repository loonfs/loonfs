#![allow(clippy::panic)]
// Tracing-capture tests panic in helper assertions for precise diagnostics.

//! Asserts a read names the anchor it answered from, and that a walk which
//! finds nothing names the lookup that came back empty.
//!
//! This keeps its own test binary — its own process — for the reason
//! `tracing_capture.rs` gives: tracing caches per-callsite interest
//! process-globally, and a callsite first exercised on a thread with no
//! subscriber can race the interest rebuild that `set_default` performs.
//! Alone in its process, the read callsites are first hit with the capture
//! subscriber installed, and the assertions are deterministic.

use loonfs::{CreateNamespaceOptions, FsBackgroundWork, FsWriter, PutFileOptions, StoreConfig};
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

async fn writer(root: &Path) -> FsWriter {
    FsWriter::builder(store_config(root))
        .writer_id("read-tracing-capture-writer")
        // No background maintenance: the only spans in the capture are the
        // ones the write and the two reads produce.
        .background_work(FsBackgroundWork::ManualOnly)
        .build()
        .await
        .expect("build writer")
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
fn reads_name_their_anchor_and_the_lookup_that_came_back_empty() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        // Closing spans are recorded so the walk span names its anchor in
        // the capture whether or not the walk emitted an event.
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(move || CaptureWriter(Arc::clone(&sink)))
        .finish();
    // Thread-local so the capture cannot leak into other tests.
    let _guard = tracing::subscriber::set_default(subscriber);

    block_on(async {
        let writer = writer(temp_dir.path()).await;
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        writer
            .put_file_bytes(
                &namespace_id,
                "/docs/report.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put file");
        let reader = writer.reader();
        reader
            .get_path_entry(&namespace_id, "/docs/report.txt", Default::default())
            .await
            .expect("stat the file that was just written");
        let missing = reader
            .get_path_entry(&namespace_id, "/docs/missing.txt", Default::default())
            .await;
        assert!(missing.is_err(), "a path that was never written is absent");
    });

    let log = String::from_utf8(captured.lock().expect("capture lock").clone())
        .expect("captured log is utf8");
    // The walk span records the head and manifest used by the read. Match the
    // complete field names so unrelated log text cannot satisfy the assertion.
    let walk = find_line(&log, "phase=\"walk_path\"");
    for field in ["head_seq=", "manifest_no=", "manifest_head_seq="] {
        assert!(walk.contains(field), "missing `{field}` in: {walk}");
    }
    // The walk that found nothing says which of the agreeing lookups was
    // empty. Nothing was ever bound under that name, so the forward binding
    // is the leg that stopped it.
    let absent = find_line(&log, "visibility walk found no child");
    for field in ["leg=", "forward_binding", "parent_inode_id="] {
        assert!(absent.contains(field), "missing `{field}` in: {absent}");
    }
}

fn find_line<'log>(log: &'log str, needle: &str) -> &'log str {
    log.lines()
        .find(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("`{needle}` missing from captured tracing:\n{log}"))
}
