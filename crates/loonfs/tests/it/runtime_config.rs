//! Public handle boundary behavior: the object-store metrics seam, the
//! filesystem operations, and fork isolation.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::*;
use loonfs::{
    CopyOptions, CreateNamespaceOptions, DestinationBehavior, MoveOptions, NamespaceId,
    PutFileOptions, TraceStoreKind,
};
use loonfs_objectstore::metrics::{ObjectStoreOperation, VecObjectStoreMetricsRecorder};
use loonfs_test_support::ids::namespace_id;
use std::sync::Arc;
use tempfile::tempdir;

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
        PutFileOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("put file");

    let stat = fs
        .stat_path_blocking(&namespace_id, "/docs/hello.txt")
        .expect("stat file");
    assert_eq!(stat.path, "/docs/hello.txt");
    assert_eq!(stat.size_bytes(), Some(5));

    let entries = fs
        .list_path_blocking(&namespace_id, "/docs")
        .expect("list docs");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].path, "/docs/hello.txt");

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
            commit: loonfs_api::options::CommitOptions {
                actor: loonfs_test_support::test_actor(),
                commit_id: None,
                message: None,
            },
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
        CopyOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("copy file");
    fs.move_path_blocking(
        &namespace_id,
        "/docs/copy.txt",
        "/docs/moved.txt",
        MoveOptions::new(loonfs_test_support::test_actor()),
    )
    .expect("move file");
    assert_eq!(
        fs.get_file_bytes_blocking(&namespace_id, "/docs/moved.txt")
            .expect("read moved copy")
            .bytes,
        b"updated"
    );
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
        PutFileOptions::new(loonfs_test_support::test_actor()),
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
            commit: loonfs_api::options::CommitOptions {
                actor: loonfs_test_support::test_actor(),
                commit_id: None,
                message: None,
            },
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
