//! Snapshots: reads at a snapshot, expiry on the virtual clock, extension, and deletion.

use crate::support::{fixture, put};
use loonfs_client::{ListOptions, NamespacePath, ReadAt, StatOptions};
use loonfs_sim::{SimClock as _, SimDuration};
use loonfs_types::{ChangeSeq, ErrorCode};

#[tokio::test]
async fn a_snapshot_reads_its_captured_state_until_it_expires() {
    let fixture = fixture();
    fixture.commit(vec![put("/f", b"before")]);
    let snapshot = fixture
        .client
        .create_snapshot(&fixture.namespace_id, "scan", Some(10_000))
        .await
        .expect("create");
    assert_eq!(snapshot.captured_seq, ChangeSeq(1));
    fixture.commit(vec![put("/g", b"after")]);

    let spec = NamespacePath::parse("demo", "/g").expect("namespace path");
    let at_snapshot = StatOptions {
        at: ReadAt::Snapshot(snapshot.snapshot_id.clone()),
        ..StatOptions::default()
    };
    let missing = fixture.client.stat_with_options(&spec, &at_snapshot).await;
    assert_eq!(
        missing.expect_err("`/g` is newer").code(),
        Some(ErrorCode::PathNotFound)
    );
    let root = NamespacePath::parse("demo", "/").expect("namespace path");
    let listing = fixture
        .client
        .list_with_options(
            &root,
            &ListOptions {
                at: ReadAt::Snapshot(snapshot.snapshot_id.clone()),
                ..ListOptions::default()
            },
        )
        .next()
        .await
        .expect("a page")
        .expect("a listing at the snapshot");
    let paths: Vec<_> = listing
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    assert_eq!((paths, listing.head_seq), (vec!["/f"], ChangeSeq(1)));

    fixture
        .server
        .clock()
        .advance(SimDuration::from_millis(6_000));
    let extended = fixture
        .client
        .extend_snapshot(&fixture.namespace_id, &snapshot.snapshot_id, 10_000)
        .await
        .expect("extend an unexpired snapshot");
    assert_eq!(
        extended.expires_at_ms,
        Some(fixture.server.now_ms() + 10_000)
    );

    fixture
        .server
        .clock()
        .advance(SimDuration::from_millis(10_000));
    let expired = fixture.client.stat_with_options(&spec, &at_snapshot).await;
    assert_eq!(
        expired.expect_err("expired").code(),
        Some(ErrorCode::SnapshotNotFound)
    );
    let extend = fixture
        .client
        .extend_snapshot(&fixture.namespace_id, &snapshot.snapshot_id, 10_000)
        .await;
    assert_eq!(
        extend.expect_err("expired").code(),
        Some(ErrorCode::SnapshotNotFound)
    );
    let listed = fixture
        .client
        .get_snapshot(&fixture.namespace_id, &snapshot.snapshot_id)
        .await
        .expect("an expired snapshot is still retrievable");
    assert_eq!(listed.expires_at_ms, extended.expires_at_ms);

    for _ in 0..2 {
        let deleted = fixture
            .client
            .delete_snapshot(&fixture.namespace_id, &snapshot.snapshot_id)
            .await
            .expect("deleting a snapshot, even a missing one, succeeds");
        assert_eq!(deleted.snapshot_id, snapshot.snapshot_id);
    }
    let gone = fixture
        .client
        .get_snapshot(&fixture.namespace_id, &snapshot.snapshot_id)
        .await;
    assert_eq!(
        gone.expect_err("deleted").code(),
        Some(ErrorCode::SnapshotNotFound)
    );
}
