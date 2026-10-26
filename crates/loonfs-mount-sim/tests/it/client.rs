//! A smoke test of the real client against the simulated server's service.

use crate::support::{fixture, mkdir, put};
use bytes::Bytes;
use futures::TryStreamExt as _;
use loonfs_client::{ListOptions, NamespacePath, ReadRange};
use loonfs_mount_sim::Operation;
use loonfs_types::{ChangeSeq, NamespaceNaming};
use std::num::NonZeroU64;

#[tokio::test]
async fn the_client_lists_follows_the_feed_reads_and_snapshots_through_the_service() {
    let fixture = fixture();
    let commit = fixture.commit(vec![
        mkdir("/docs"),
        put("/docs/report.txt", b"quarterly report"),
    ]);
    let docs = NamespacePath::parse("demo", "/docs").expect("namespace path");
    let report = NamespacePath::parse("demo", "/docs/report.txt").expect("namespace path");

    let metadata = fixture
        .client
        .get_namespace(&fixture.namespace_id)
        .await
        .expect("the namespace");
    assert_eq!(
        (
            metadata.naming,
            metadata.head_seq,
            metadata.retention_floor_seq
        ),
        (NamespaceNaming::CaseSensitive, ChangeSeq(1), ChangeSeq(0))
    );

    let listing = fixture
        .client
        .list_with_options(&docs, &ListOptions::default())
        .next()
        .await
        .expect("a page")
        .expect("a listing");
    let names: Vec<_> = listing
        .entries
        .iter()
        .filter_map(|entry| {
            entry
                .display_name
                .as_ref()
                .map(|name| name.as_str().to_owned())
        })
        .collect();
    assert_eq!(names, ["report.txt"]);

    let page = fixture
        .client
        .list_changes(&fixture.namespace_id, ChangeSeq(0))
        .next()
        .await
        .expect("a page")
        .expect("a feed page");
    assert_eq!(page.changes, std::slice::from_ref(&commit));

    assert_eq!(
        fixture
            .client
            .read_file(&report)
            .await
            .expect("a whole read"),
        b"quarterly report"
    );
    let range = ReadRange {
        start_offset: 10,
        length: NonZeroU64::new(6),
    };
    let ranged = fixture
        .client
        .read_file_range_stream(&report, range)
        .await
        .expect("a ranged read");
    assert_eq!(
        (ranged.start_offset, ranged.length, ranged.size_bytes),
        (10, 6, 16)
    );
    let chunks: Vec<Bytes> = ranged.body.try_collect().await.expect("the range's bytes");
    assert_eq!(chunks.concat(), b"report");

    let snapshot = fixture
        .client
        .create_snapshot(&fixture.namespace_id, "smoke", Some(60_000))
        .await
        .expect("create a snapshot");
    assert_eq!(snapshot.captured_seq, commit.committed_seq);
    fixture
        .client
        .delete_snapshot(&fixture.namespace_id, &snapshot.snapshot_id)
        .await
        .expect("delete the snapshot");

    let operations: Vec<_> = fixture
        .server
        .trace()
        .iter()
        .map(|record| record.operation)
        .collect();
    assert_eq!(
        operations,
        [
            Operation::GetNamespace,
            Operation::ListPathEntries,
            Operation::ListChanges,
            Operation::GetFileContent,
            Operation::GetFileContent,
            Operation::CreateSnapshot,
            Operation::DeleteSnapshot,
        ]
    );
}
