//! Storage requests for whole-file reads of appended files.

use loonfs::{LoonFs, SharedObjectStore};
use loonfs_objectstore::keys::{content_span, wal_prefix};
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    ConcurrencyWatchStore, KeyPredicate, MemoryStore, RecordedOperation, RecordingStore,
};
use loonfs_types::{usize_from_u64, ChecksumAlgorithm};
use std::collections::BTreeSet;
use std::sync::Arc;

const PATH: &str = "/sessions/transcript.jsonl";
const LINES: u8 = 12;
const LINE_BYTES: u64 = 256;

/// Checks that a file whose twelve lines were folded one at a time has three
/// extents, and that a warm read of the file sends one GET for the next WAL
/// object and one GET per extent, with every extent GET in flight at once.
#[tokio::test]
async fn warm_read_of_an_appended_file_gets_every_extent_at_once() {
    let memory: SharedObjectStore = Arc::new(MemoryStore::new(ChecksumAlgorithm::Crc64nvme));
    let writer = LoonFs::builder_with_store(memory.clone())
        .writer_id(writer_id("append-read"))
        .build()
        .await
        .expect("writer");
    let namespace_id = namespace_id("append-read");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let maintenance = writer.maintenance(writer_id("append-read-maintenance"));
    let mut expected = Vec::new();
    for line_no in 0..LINES {
        let line = vec![b'a' + line_no; usize_from_u64(LINE_BYTES)];
        if line_no == 0 {
            namespace.put_file(PATH, &line, &actor).await.expect("put");
        } else {
            namespace
                .append_file(PATH, &line, &actor)
                .await
                .expect("append");
        }
        expected.extend_from_slice(&line);
        maintenance.fold_wal(&namespace_id).await.expect("fold");
    }

    let watched = Arc::new(ConcurrencyWatchStore::new(
        memory,
        KeyPredicate::content_blob(),
    ));
    let recording = Arc::new(RecordingStore::new(
        Arc::clone(&watched),
        KeyPredicate::any(),
    ));
    let reader = LoonFs::builder_with_store(recording.clone())
        .read_only()
        .build()
        .await
        .expect("reader")
        .namespace(&namespace_id);
    reader.read_file(PATH).await.expect("first read");
    recording.reset();
    let read = reader.read_file(PATH).await.expect("warm read");
    assert_eq!(read.bytes, expected);

    // A fold merges the new tail with the older extents, newest first, while
    // the next extent is at most twice as long as the bytes merged so far.
    // Twelve one-line tails leave extents of 8, 3, and 1 lines.
    let content_id = &read.entry.content_ref().expect("file content").content_id;
    let expected_keys = [(0, 8), (8, 11), (11, 12)].map(|(start_line, end_line)| {
        content_span(
            &namespace_id,
            content_id,
            start_line * LINE_BYTES,
            end_line * LINE_BYTES,
        )
    });
    // The extent GETs run concurrently, so the order of the GETs in the log
    // varies from run to run.
    let operations = recording.take();
    let (wal_requests, extent_requests): (Vec<_>, Vec<_>) = operations
        .iter()
        .partition(|operation| operation.key().starts_with(&wal_prefix(&namespace_id)));
    assert_eq!(wal_requests.len(), 1, "{operations:?}");
    assert_eq!(extent_requests.len(), expected_keys.len(), "{operations:?}");
    assert!(
        operations
            .iter()
            .all(|operation| matches!(operation, RecordedOperation::Get { .. })),
        "{operations:?}"
    );
    let extent_keys: BTreeSet<&str> = extent_requests
        .iter()
        .map(|operation| operation.key())
        .collect();
    assert_eq!(
        extent_keys,
        expected_keys.iter().map(String::as_str).collect(),
        "{operations:?}"
    );
    assert_eq!(watched.reads().peak_in_flight, expected_keys.len());
}
