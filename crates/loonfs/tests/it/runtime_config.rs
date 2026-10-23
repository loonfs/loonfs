//! Public handle behavior and fork isolation.

use crate::common::*;
use loonfs::publish::{CommitIntent, FilesystemOperation};
use loonfs::{
    AbsolutePath, CommitId, ConfiguredObjectStoreKind, DestinationBehavior, LoonFs, NamespaceId,
    PutFileOptions,
};
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::metrics::{ObjectStoreOperation, VecObjectStoreMetricsRecorder};
use loonfs_objectstore::ByteRange;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_types::format::segment_blocks::decode_index_block;
use std::assert_matches;
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn builder_object_store_metrics_recorder_instruments_object_store() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let fs = open_runtime_with(store(temp_dir.path()), "metrics-test", |builder| {
        builder
            .store_kind(ConfiguredObjectStoreKind::LocalFs)
            .object_store_metrics_recorder(recorder.clone())
    });

    fs.create_namespace_blocking(&namespace_id("demo"), &loonfs_test_support::test_actor())
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

    fs.create_namespace_blocking(&namespace_id, &loonfs_test_support::test_actor())
        .expect("create namespace");
    fs.put_file_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        &loonfs_test_support::test_actor(),
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
        .read_file_blocking(&namespace_id, "/docs/hello.txt")
        .expect("read file");
    assert_eq!(read.bytes, b"hello");

    fs.put_file_with_options_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        &loonfs_test_support::test_actor(),
        &PutFileOptions {
            behavior: DestinationBehavior::Replace,
            commit: loonfs_types::options::CommitOptions {
                preconditions: Vec::new(),
                commit_id: None,
                message: None,
            },
            expected_inode_id: None,
            expected_revision_no: None,
        },
    )
    .expect("replace file");
    let read = fs
        .read_file_blocking(&namespace_id, "/docs/hello.txt")
        .expect("read replaced file");
    assert_eq!(read.bytes, b"updated");

    fs.copy_path_blocking(
        &namespace_id,
        "/docs/hello.txt",
        "/docs/copy.txt",
        &loonfs_test_support::test_actor(),
    )
    .expect("copy file");
    fs.move_path_blocking(
        &namespace_id,
        "/docs/copy.txt",
        "/docs/moved.txt",
        &loonfs_test_support::test_actor(),
    )
    .expect("move file");
    assert_eq!(
        fs.read_file_blocking(&namespace_id, "/docs/moved.txt")
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

    fs.create_namespace_blocking(&source, &loonfs_test_support::test_actor())
        .expect("create source namespace");
    fs.put_file_blocking(
        &source,
        "/docs/shared.txt",
        b"source",
        &loonfs_test_support::test_actor(),
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

    fs.put_file_with_options_blocking(
        &clone,
        "/docs/shared.txt",
        b"clone",
        &loonfs_test_support::test_actor(),
        &PutFileOptions {
            behavior: DestinationBehavior::Replace,
            commit: loonfs_types::options::CommitOptions {
                preconditions: Vec::new(),
                commit_id: None,
                message: None,
            },
            expected_inode_id: None,
            expected_revision_no: None,
        },
    )
    .expect("replace clone file");

    assert_eq!(
        fs.read_file_blocking(&source, "/docs/shared.txt")
            .expect("read source")
            .bytes,
        b"source"
    );
    assert_eq!(
        fs.read_file_blocking(&clone, "/docs/shared.txt")
            .expect("read clone")
            .bytes,
        b"clone"
    );
}

/// Checks that a writer's folds close the data blocks of metadata segments
/// at the target that its builder sets, and that the builder rejects a
/// target outside 4 KiB to 256 KiB.
#[tokio::test]
async fn folds_close_data_blocks_at_the_target_that_the_builder_sets() {
    let demo = namespace_id("demo");
    let mut data_blocks = Vec::new();
    for target_block_bytes in [16 * 1024, loonfs::DEFAULT_TARGET_BLOCK_BYTES] {
        let temp_dir = tempdir().expect("tempdir");
        let store = store(temp_dir.path());
        let writer = LoonFs::builder_with_store(store.clone())
            .writer_id(writer_id("block-target-writer"))
            .target_block_bytes(target_block_bytes)
            .build()
            .await
            .expect("build writer");
        writer
            .create_namespace(&demo, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        // Long names fill each family's segment with many data blocks.
        let operations = (0..3_000)
            .map(|index| FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse(format!("/{index:05}-{}", "n".repeat(200)))
                    .expect("path"),
                parents: false,
            })
            .collect();
        writer
            .open_namespace(&demo)
            .expect("open namespace")
            .commit(CommitIntent {
                commit_id: CommitId::parse("create-the-entries").expect("commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations,
                preconditions: Vec::new(),
            })
            .await
            .expect("create the entries");
        writer
            .maintenance(writer_id("block-target-writer"))
            .fold_wal(&demo)
            .await
            .expect("fold the entries into segments");
        writer.shutdown().await.expect("shutdown");

        let loaded = loonfs::control::load_namespace_current_manifest(store.as_ref(), &demo)
            .await
            .expect("load the current manifest");
        let mut blocks = 0;
        for segment in loaded
            .state
            .envelope
            .payload()
            .runs
            .iter()
            .flat_map(|run| &run.segments)
        {
            let index = &segment.index_block;
            let stored = store
                .get(
                    &metadata_segment_object_key(segment),
                    Some(ByteRange {
                        start_inclusive: index.offset,
                        end_exclusive: index.offset + u64::from(index.stored_bytes),
                    }),
                )
                .await
                .expect("read the index block")
                .expect("the segment exists");
            let entries = decode_index_block(&stored, index).expect("decode the index block");
            // The row that reaches the target closes a block, so only a
            // segment's last block can be smaller.
            for entry in &entries[..entries.len() - 1] {
                assert!(entry.block.decoded_bytes as usize >= target_block_bytes);
            }
            blocks += entries.len();
        }
        data_blocks.push(blocks);
    }
    let (small, large) = (data_blocks[0], data_blocks[1]);
    assert!(
        small >= 3 * large,
        "16 KiB blocks should be about four times as many: {small}, and {large} at 64 KiB"
    );

    for target_block_bytes in [4 * 1024 - 1, 256 * 1024 + 1] {
        let temp_dir = tempdir().expect("tempdir");
        let error = LoonFs::builder_with_store(store(temp_dir.path()))
            .writer_id(writer_id("block-target-writer"))
            .target_block_bytes(target_block_bytes)
            .build()
            .await
            .err()
            .expect("a target outside 4 KiB to 256 KiB is rejected");
        assert_matches!(error, loonfs::Error::Config(_));
    }
}
