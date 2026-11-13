//! Asserts a read names the anchor it answered from, and that a walk which
//! finds nothing names the lookup that came back empty.
//!
//! This keeps its own test binary — its own process — for the reason
//! `tracing_capture.rs` gives: tracing caches per-callsite interest
//! process-globally, and a callsite first exercised on a thread with no
//! subscriber can race the interest rebuild that `set_default` performs.
//! Alone in its process, the read callsites are first hit with the capture
//! subscriber installed, and the assertions are deterministic.

#![allow(clippy::panic, reason = "an integration test binary is test code")]

mod capture;

use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use tempfile::tempdir;

#[test]
fn reads_name_their_anchor_and_the_lookup_that_came_back_empty() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let (captured, _guard) = capture::capture(false);

    block_on(async {
        let writer = capture::writer(temp_dir.path(), "read-tracing-capture-writer").await;
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        let namespace_writer = writer
            .open_namespace(&namespace_id)
            .expect("open namespace");
        namespace_writer
            .put_file(
                "/docs/report.txt",
                b"body",
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("put file");
        let reader = writer.read_only();
        let namespace = reader.namespace(&namespace_id);
        namespace
            .stat("/docs/report.txt")
            .await
            .expect("stat the file that was just written");
        let missing = namespace.stat("/docs/missing.txt").await;
        assert_eq!(
            missing
                .expect_err("a path that was never written is absent")
                .code(),
            loonfs_types::ErrorCode::PathNotFound
        );
    });

    let log = capture::take_log(&captured);
    // The walk span records the head and manifest used by the read. Match the
    // complete field names so unrelated log text cannot satisfy the assertion.
    let walk = capture::find_line(&log, "phase=\"walk_path\"");
    for field in ["head_seq=", "manifest_no=", "manifest_head_seq="] {
        assert!(walk.contains(field), "missing `{field}` in: {walk}");
    }
    // The walk that found nothing says which of the agreeing lookups was
    // empty. Nothing was ever bound under that name, so the forward binding
    // is the leg that stopped it.
    let absent = capture::find_line(&log, "visibility walk found no child");
    for field in ["leg=", "forward_binding", "parent_inode_id="] {
        assert!(absent.contains(field), "missing `{field}` in: {absent}");
    }
}
