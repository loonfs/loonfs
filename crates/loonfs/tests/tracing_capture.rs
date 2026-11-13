//! Asserts a metadata maintenance step is observable at debug level.
//!
//! This stays out of the crate's `tests/it` harness and keeps its own test
//! binary — its own process — on purpose.
//! Tracing caches per-callsite interest process-globally, and a callsite
//! first exercised on a thread with no subscriber can race the interest
//! rebuild that `set_default` performs. Sibling tests that run maintenance
//! on subscriber-less threads hit exactly the callsites this
//! capture greps for, so sharing a binary with them made the capture lose
//! events intermittently under parallel test execution. Alone in its
//! process, the callsites are first hit with the capture subscriber
//! installed, and the assertion is deterministic.

#![allow(clippy::panic, reason = "an integration test binary is test code")]

mod capture;

use loonfs::{MaintainMetadataOptions, NamespaceId};
use loonfs_core::test_support::WriterAt;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::ids::namespace_id;
use std::path::Path;
use tempfile::tempdir;

fn writes_past_wal_tail_threshold() -> u32 {
    u32::try_from(
        MaintainMetadataOptions::default()
            .max_wal_tail_objects
            .get()
            + 1,
    )
    .expect("WAL tail threshold plus one should fit in u32")
}

async fn fill_wal_tail_past_threshold(root: &Path, namespace_id: &NamespaceId) {
    let store = LocalFsStore::new(root).expect("open tail store");
    loonfs_core::test_support::NamespaceFixture::new(
        &store,
        namespace_id,
        &WriterAt {
            writer_id: loonfs_types::WriterId::parse("tracing-tail-writer").expect("writer id"),
            now_ms: 1_000,
        },
    )
    .append_wal_objects(u64::from(writes_past_wal_tail_threshold()))
    .await
    .expect("fill WAL tail past threshold");
}

#[test]
fn a_metadata_maintenance_step_emits_debug_events() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id("demo");
    let (captured, _guard) = capture::capture(false);

    block_on(async {
        let writer = capture::writer(temp_dir.path(), "tracing-capture-writer").await;
        writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
        fill_wal_tail_past_threshold(temp_dir.path(), &namespace_id).await;
        writer
            .maintenance(loonfs_test_support::ids::writer_id(
                "tracing-capture-maintenance",
            ))
            .maintain_metadata(&namespace_id)
            .await
            .expect("metadata maintenance");
        writer.shutdown().await.expect("shut down writer");
    });

    let log = capture::take_log(&captured);
    // Fields are matched with their `=` so a span carrying the same word
    // cannot satisfy the assertion.
    let step = capture::find_line(&log, "metadata maintenance step concluded");
    for field in ["wal_fold=", "compaction="] {
        assert!(step.contains(field), "missing `{field}` in: {step}");
    }
    // Record the WAL fold phase.
    let span_evidence = "loonfs.phase{phase=\"wal_fold\"";
    assert!(
        log.contains(span_evidence),
        "missing span evidence `{span_evidence}` in:\n{log}"
    );
    assert!(
        !log.contains("loonfs.compaction"),
        "maintenance still traces as compaction:\n{log}"
    );
}
