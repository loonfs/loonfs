//! The rebuild after `rebootstrap_required`: a feed that passed the
//! retention floor makes the engine list its tree again at a new snapshot.

use crate::support::{delete, engine_tree, eventually, fixture, item, move_path, put, refresh};
use loonfs_mount::Readiness;
use loonfs_mount_sim::Operation;
use loonfs_types::ErrorCode;

/// Checks that a poll below the retention floor returns
/// `rebootstrap_required`, that the engine lists the root's subtree again at
/// a new snapshot and deletes it, that the tree then equals a listing at the
/// live head with unchanged items keeping their item IDs, and that the feed
/// continues from the snapshot's sequence.
#[tokio::test]
async fn a_feed_past_the_retention_floor_rebuilds_the_tree_at_a_new_snapshot() {
    let fixture = fixture();
    fixture.commit(vec![
        put("docs/notes.txt", b"notes"),
        put("docs/sub/deep.txt", b"deep"),
        put("readme.md", b"read me"),
    ]);
    let engine = fixture.mount().await;
    let notes = item(&engine, "docs/notes.txt").expect("the notes");
    let applied = engine.status().applied_seq;
    fixture.commit(vec![put("added.txt", b"added"), delete("readme.md")]);
    fixture.commit(vec![move_path("docs/sub", "moved")]);
    let floor = fixture
        .server
        .advance_retention_floor(&fixture.namespace_id, fixture.head_seq());
    assert!(floor > applied, "the floor passed the applied sequence");
    let head = fixture.head_seq();
    fixture.server.take_trace();
    refresh(&engine, head).await;

    let status = engine.status();
    assert_eq!(
        (status.applied_seq, status.readiness),
        (head, Readiness::Ready)
    );
    assert_eq!(engine_tree(&engine), fixture.listed_tree().await);
    assert_eq!(
        item(&engine, "docs/notes.txt"),
        Some(notes),
        "an unchanged item stays"
    );
    let after_head = head.0.to_string();
    eventually(|| {
        fixture.server.trace().iter().any(|record| {
            record.operation == Operation::ListChanges
                && record.query_param("after_seq") == Some(after_head.as_str())
        })
    })
    .await;
    let trace = fixture.server.take_trace();
    assert_eq!(
        (trace[0].operation, trace[0].error_code),
        (Operation::ListChanges, Some(ErrorCode::RebootstrapRequired))
    );
    let count = |operation| {
        trace
            .iter()
            .filter(|record| record.operation == operation)
            .count()
    };
    assert_eq!(
        (
            count(Operation::CreateSnapshot),
            count(Operation::ListInodeChildren),
            count(Operation::DeleteSnapshot)
        ),
        (1, 1, 1),
        "one listing at one snapshot, which the engine deletes"
    );
    let listing = trace
        .iter()
        .find(|record| record.operation == Operation::ListInodeChildren)
        .expect("the listing");
    let snapshot_id = listing.query_param("snapshot_id").expect("at a snapshot");
    assert_eq!(listing.query_param("recursive"), Some("true"));
    assert!(trace.iter().any(|record| {
        record.operation == Operation::DeleteSnapshot && record.path.ends_with(snapshot_id)
    }));
}
