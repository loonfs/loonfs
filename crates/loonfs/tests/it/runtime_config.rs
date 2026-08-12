//! Runtime builder validation and public handle boundary behavior.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::{
    CopyOptions, CreateNamespaceOptions, DestinationBehavior, FsReader, FsWriter, MoveOptions,
    NamespaceId, PutFileOptions, RuntimeError, TraceStoreKind,
};
use loonfs_objectstore::metrics::{ObjectStoreOperation, VecObjectStoreMetricsRecorder};
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use std::sync::Arc;
use tempfile::tempdir;

fn assert_config_error<T>(result: loonfs::Result<T>, expected: &str) {
    match result {
        Err(RuntimeError::Config(message)) => assert!(
            message.contains(expected),
            "expected {message:?} to contain {expected:?}"
        ),
        Err(error) => panic!("expected config error, got {error:?}"),
        Ok(_) => panic!("expected config error"),
    }
}

#[test]
fn open_validates_runtime_config() {
    let temp_dir = tempdir().expect("tempdir");
    let object_store = store(temp_dir.path());

    assert_config_error(
        block_on(FsWriter::builder_with_store(object_store.clone()).build()),
        "writer_id",
    );
    assert_config_error(
        block_on(
            FsWriter::builder_with_store(object_store.clone())
                .writer_id("   ")
                .build(),
        ),
        "writer_id",
    );
}

#[test]
fn builder_object_store_metrics_recorder_instruments_object_store() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let fs = open_runtime_with(store(temp_dir.path()), "metrics-test", |builder| {
        builder
            .trace_store_kind(TraceStoreKind::LocalFs)
            .object_store_metrics_recorder(recorder.clone())
    });

    fs.create_namespace_blocking(&namespace_id("demo"), CreateNamespaceOptions::default())
        .expect("create namespace");

    let samples = recorder.samples();
    assert!(!samples.is_empty());
    assert!(samples
        .iter()
        .any(|sample| sample.operation == ObjectStoreOperation::Put));
    assert!(samples
        .iter()
        .all(|sample| sample.store_kind.as_deref() == Some("local_fs")));
}

#[test]
fn filesystem_operations_match_core_semantics() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "filesystem-test");
    let namespace_id = namespace_id("demo");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let stat = fs
        .stat_path_blocking(&namespace_id, "/docs/hello.txt")
        .expect("stat file");
    assert_eq!(stat.absolute_path, "/docs/hello.txt");
    assert_eq!(stat.size_bytes(), Some(5));

    let entries = fs
        .list_path_blocking(&namespace_id, "/docs")
        .expect("list docs");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].absolute_path, "/docs/hello.txt");

    let read = fs
        .get_file_bytes_blocking(&namespace_id, "/docs/hello.txt")
        .expect("read file");
    assert_eq!(read.bytes, b"hello");

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            commit_id: None,
            message: None,
            expected_revision_no: None,
        },
    )
    .expect("replace file");
    let read = fs
        .get_file_bytes_blocking(&namespace_id, "/docs/hello.txt")
        .expect("read replaced file");
    assert_eq!(read.bytes, b"updated");

    fs.copy_path_blocking(
        &namespace_id,
        "/docs/hello.txt",
        "/docs/copy.txt",
        CopyOptions::default(),
    )
    .expect("copy file");
    fs.move_path_blocking(
        &namespace_id,
        "/docs/copy.txt",
        "/docs/moved.txt",
        MoveOptions::default(),
    )
    .expect("move file");
    assert_eq!(
        fs.get_file_bytes_blocking(&namespace_id, "/docs/moved.txt")
            .expect("read moved copy")
            .bytes,
        b"updated"
    );
}

#[tokio::test]
async fn async_runtime_methods_are_the_engine_boundary() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = open_runtime_async(store(temp_dir.path()), "async-runtime-test").await;
    let namespace_id = namespace_id("demo");

    FsWriter::create_namespace(&fs.writer, &namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    FsWriter::put_file_bytes(
        &fs.writer,
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .await
    .expect("put file");

    let async_stat = FsReader::stat_path(&fs.reader, &namespace_id, "/docs/hello.txt")
        .await
        .expect("async stat");

    assert_eq!(async_stat.absolute_path, "/docs/hello.txt");
    assert_eq!(async_stat.size_bytes(), Some(5));
}

#[test]
fn forked_namespace_shares_content_then_diverges() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "fork-test");
    let source = namespace_id("demo");
    let clone = NamespaceId::parse("clone").expect("valid namespace id");

    fs.create_namespace_blocking(&source, CreateNamespaceOptions::default())
        .expect("create source namespace");
    fs.put_file_bytes_blocking(
        &source,
        "/docs/shared.txt",
        b"source",
        PutFileOptions::default(),
    )
    .expect("put source file");
    fs.fork_namespace_blocking(&source, &clone)
        .expect("fork namespace");

    let source_entry = fs
        .stat_path_blocking(&source, "/docs/shared.txt")
        .expect("stat source");
    let clone_entry = fs
        .stat_path_blocking(&clone, "/docs/shared.txt")
        .expect("stat clone");
    assert_eq!(source_entry.content_ref(), clone_entry.content_ref());

    fs.put_file_bytes_blocking(
        &clone,
        "/docs/shared.txt",
        b"clone",
        PutFileOptions {
            behavior: DestinationBehavior::Replace,
            commit_id: None,
            message: None,
            expected_revision_no: None,
        },
    )
    .expect("replace clone file");

    assert_eq!(
        fs.get_file_bytes_blocking(&source, "/docs/shared.txt")
            .expect("read source")
            .bytes,
        b"source"
    );
    assert_eq!(
        fs.get_file_bytes_blocking(&clone, "/docs/shared.txt")
            .expect("read clone")
            .bytes,
        b"clone"
    );
}
