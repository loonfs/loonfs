#![allow(clippy::panic)]
// Include the captured output when an assertion fails.

//! Checks operation spans for the writer, reader, and maintenance handles.

use loonfs::{
    CreateDirectoryOptions, CreateNamespaceOptions, FsMaintenance, FsWriter, PutFileOptions,
    StoreConfig,
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

fn take_captured_log(captured: &Arc<Mutex<Vec<u8>>>) -> String {
    let bytes = std::mem::take(&mut *captured.lock().expect("capture lock"));
    String::from_utf8(bytes).expect("captured log is utf8")
}

fn closed_span_count(log: &str, span_name: &str) -> usize {
    log.lines()
        .filter(|line| line.contains(span_name) && line.contains("close"))
        .count()
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
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");

        let reader = writer.reader();
        reader
            .get_path_entry(&namespace_id, "/", Default::default())
            .await
            .expect("stat namespace root");
        reader
            .get_namespace(&namespace_id)
            .await
            .expect("read namespace state");

        FsMaintenance::builder_with_store(writer.object_store())
            .actor_id("operation-span-maintenance")
            .build()
            .await
            .expect("build maintenance")
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("read namespace diagnostics");
    });

    let log = String::from_utf8(captured.lock().expect("capture lock").clone())
        .expect("captured log is utf8");
    for span_name in [
        "loonfs.create_namespace",
        "loonfs.stat",
        "loonfs.get_namespace",
        "loonfs.get_namespace_diagnostics",
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

#[test]
fn delegated_writer_calls_close_one_operation_span() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("operation-span-counts");
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(move || CaptureWriter(Arc::clone(&sink)))
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let (create_log, put_log) = block_on(async {
        let writer = FsWriter::builder(store_config(temp_dir.path()))
            .writer_id("operation-span-count-writer")
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let _setup_log = take_captured_log(&captured);
        writer
            .create_directory(
                &namespace_id,
                "/docs",
                CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("create directory");
        let create_log = take_captured_log(&captured);
        writer
            .put_file_bytes(
                &namespace_id,
                "/docs/file.txt",
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("put file");
        let put_log = take_captured_log(&captured);
        (create_log, put_log)
    });

    let apply_commit = create_log
        .lines()
        .filter(|line| line.contains("loonfs.apply_commit") && line.contains("close"))
        .collect::<Vec<_>>();
    assert_eq!(
        apply_commit.len(),
        1,
        "create_directory closed the wrong number of apply_commit spans:\n{create_log}"
    );
    assert!(
        apply_commit[0].contains("method=\"create_directory\""),
        "create_directory apply_commit span lacks its method field:\n{}",
        apply_commit[0]
    );
    assert_eq!(
        closed_span_count(&put_log, "loonfs.put"),
        1,
        "put_file_bytes closed the wrong number of put spans:\n{put_log}"
    );
    assert_eq!(
        closed_span_count(&put_log, "loonfs.prepare"),
        0,
        "put_file_bytes closed a prepare span:\n{put_log}"
    );
    assert_eq!(
        closed_span_count(&put_log, "loonfs.apply_commit"),
        0,
        "put_file_bytes closed an apply_commit span:\n{put_log}"
    );
}
