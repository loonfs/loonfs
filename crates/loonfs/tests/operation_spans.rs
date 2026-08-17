#![allow(clippy::panic)]
// Tracing-capture tests panic in helper assertions for precise diagnostics.

//! Smoke-tests the facade operation-span contract in its own process.

use loonfs::{CreateNamespaceOptions, FsAdmin, FsBackgroundWork, FsWriter, StoreConfig};
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
fn every_handle_emits_an_operation_span_with_its_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("operation-spans");
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(move || CaptureWriter(Arc::clone(&sink)))
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    block_on(async {
        let writer = FsWriter::builder(store_config(temp_dir.path()))
            .writer_id("operation-span-writer")
            .background_work(FsBackgroundWork::ManualOnly)
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");

        writer
            .reader()
            .stat_path(&namespace_id, "/", Default::default())
            .await
            .expect("stat namespace root");

        FsAdmin::builder_with_store(writer.object_store())
            .actor_id("operation-span-admin")
            .build()
            .await
            .expect("build admin")
            .namespace_status(&namespace_id)
            .await
            .expect("read namespace status");
    });

    let log = String::from_utf8(captured.lock().expect("capture lock").clone())
        .expect("captured log is utf8");
    for span_name in [
        "loonfs.create_namespace",
        "loonfs.stat",
        "loonfs.namespace_status",
    ] {
        let span = log
            .lines()
            .find(|line| line.contains(span_name) && line.contains("namespace_id=operation-spans"))
            .unwrap_or_else(|| panic!("`{span_name}` lacks `namespace_id` in:\n{log}"));
        assert!(
            span.contains("close"),
            "`{span_name}` did not close: {span}"
        );
    }
}
