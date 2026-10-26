//! The change feed: commits grouped with their events, `append_offset`, page bounds, and
//! `rebootstrap_required` after the retention floor advances.

use crate::support::{append, fixture, fixture_with, mkdir, path, put, request};
use loonfs_mount_sim::SimulatedServerConfig;
use loonfs_types::api::v0::{FilesystemChange, FilesystemOperation, ListChangesResponse};
use loonfs_types::{
    ChangeSeq, DeleteDirectoryBehavior, DestinationBehavior, ErrorCode, ErrorResponse, RevisionNo,
};

/// Returns each event's kind, with the offset of an append.
fn kinds(events: &[FilesystemChange]) -> Vec<String> {
    events
        .iter()
        .map(|event| match event {
            FilesystemChange::DirectoryCreated { display_name, .. } => {
                format!("directory_created {display_name}")
            }
            FilesystemChange::FileCreated { display_name, .. } => {
                format!("file_created {display_name}")
            }
            FilesystemChange::ContentChanged {
                append_offset: Some(offset),
                ..
            } => format!("appended at {offset}"),
            FilesystemChange::ContentChanged { .. } => "content_changed".to_owned(),
            FilesystemChange::Moved {
                destination_display_name,
                ..
            } => format!("moved to {destination_display_name}"),
            FilesystemChange::Deleted {
                deleted_binding, ..
            } => format!("deleted {}", deleted_binding.display_name),
            FilesystemChange::Undeleted { display_name, .. } => format!("undeleted {display_name}"),
            FilesystemChange::DeletionExpired { deletion_seq, .. } => {
                format!("deletion_expired {deletion_seq}")
            }
            FilesystemChange::AttributesChanged { .. } => "attributes_changed".to_owned(),
            other => panic!("unexpected event {other:?}"),
        })
        .collect()
}

async fn page(fixture: &crate::support::Fixture, query: &str) -> ListChangesResponse {
    let (status, _, body) = fixture
        .get(&format!("/v0/namespaces/demo/changes?{query}"))
        .await;
    assert!(
        status.is_success(),
        "{query}: {}",
        String::from_utf8_lossy(&body)
    );
    serde_json::from_slice(&body).expect("a feed page")
}

#[tokio::test]
async fn the_feed_groups_each_commits_events_in_request_order() {
    let fixture = fixture();
    let first = fixture.commit(vec![put("/p/q/r.txt", b"r"), mkdir("/d")]);
    let r = fixture.stat("/p/q/r.txt").await.inode_id;
    let d = fixture.stat("/d").await.inode_id;
    let second = fixture.commit(vec![
        append(r, b"more"),
        FilesystemOperation::PutFile {
            path: path("/p/q/r.txt"),
            content_ref: None,
            inline_content: Some(b"replaced".to_vec()),
            behavior: DestinationBehavior::Replace,
            expected_inode_id: Some(r),
            expected_revision_no: Some(RevisionNo(2)),
        },
        FilesystemOperation::MovePath {
            source_path: path("/p/q/r.txt"),
            destination_path: path("/d/r.txt"),
            precondition: Default::default(),
        },
        FilesystemOperation::DeleteByInode {
            inode_id: d,
            expected_binding_version: fixture.stat("/d").await.binding_version.expect("version"),
            behavior: DeleteDirectoryBehavior::Recursive,
        },
    ]);

    let page = page(&fixture, "after_seq=0").await;
    assert_eq!(
        (page.after_seq, page.head_seq, page.next_after_seq),
        (ChangeSeq(0), ChangeSeq(2), None)
    );
    let grouped: Vec<_> = page
        .changes
        .iter()
        .map(|commit| {
            (
                commit.commit_id.clone(),
                commit.committed_seq,
                kinds(&commit.events),
            )
        })
        .collect();
    assert_eq!(
        grouped,
        vec![
            (
                first.commit_id,
                ChangeSeq(1),
                vec![
                    "directory_created p".to_owned(),
                    "directory_created q".to_owned(),
                    "file_created r.txt".to_owned(),
                    "directory_created d".to_owned(),
                ]
            ),
            (
                second.commit_id.clone(),
                ChangeSeq(2),
                vec![
                    "appended at 1".to_owned(),
                    "content_changed".to_owned(),
                    "moved to r.txt".to_owned(),
                    "deleted d".to_owned(),
                ]
            ),
        ],
        "a recursive delete is one event for the deleted directory"
    );
    assert_eq!(
        page.changes[1], second,
        "the feed returns the commit the response returned"
    );
}

#[tokio::test]
async fn a_page_ends_at_its_limit_or_budget_and_points_at_the_next_page() {
    let fixture = fixture_with(SimulatedServerConfig {
        feed_page_budget_bytes: 1,
        ..SimulatedServerConfig::default()
    });
    for index in 0..3 {
        fixture.commit(vec![put(&format!("/f{index}"), b"x")]);
    }
    // A page always returns its first commit, even one larger than the budget.
    let over_budget = page(&fixture, "after_seq=0").await;
    assert_eq!(
        (over_budget.changes.len(), over_budget.next_after_seq),
        (1, Some(ChangeSeq(1)))
    );

    let fixture = fixture_with(SimulatedServerConfig::default());
    for index in 0..3 {
        fixture.commit(vec![put(&format!("/f{index}"), b"x")]);
    }
    let limited = page(&fixture, "after_seq=0&limit=2").await;
    assert_eq!(
        (limited.changes.len(), limited.next_after_seq),
        (2, Some(ChangeSeq(2)))
    );
    let last = page(&fixture, "after_seq=2&limit=2").await;
    assert_eq!(
        (last.changes.len(), last.next_after_seq, last.head_seq),
        (1, None, ChangeSeq(3))
    );
    let empty = page(
        &fixture,
        "after_seq=3&consistency=eventual&max_staleness_ms=10",
    )
    .await;
    assert_eq!((empty.changes.len(), empty.next_after_seq), (0, None));
}

#[tokio::test]
async fn a_floor_advance_expires_deletions_and_makes_older_cursors_rebootstrap() {
    let fixture = fixture();
    fixture.commit(vec![put("/gone.txt", b"g")]);
    let gone = fixture.stat("/gone.txt").await;
    let deleted = request(vec![FilesystemOperation::DeleteByInode {
        inode_id: gone.inode_id,
        expected_binding_version: gone.binding_version.expect("version"),
        behavior: DeleteDirectoryBehavior::NonRecursive,
    }]);
    let deletion = fixture.try_commit(&deleted).expect("delete");
    fixture.commit(vec![put("/kept.txt", b"k")]);

    let floor = fixture
        .server
        .advance_retention_floor(&fixture.namespace_id, ChangeSeq(2));
    assert_eq!(floor, ChangeSeq(2));
    let (status, _, body) = fixture.get("/v0/namespaces/demo/changes?after_seq=1").await;
    let error: ErrorResponse = serde_json::from_slice(&body).expect("an error envelope");
    let details = error.details.expect("details");
    assert_eq!(
        (
            status.as_u16(),
            error.code.as_str(),
            details.after_seq,
            details.retention_floor_seq
        ),
        (
            409,
            ErrorCode::RebootstrapRequired.as_str(),
            Some(ChangeSeq(1)),
            Some(ChangeSeq(2))
        )
    );

    let after_floor = page(&fixture, "after_seq=2").await;
    let expiry = after_floor.changes.last().expect("the expiry commit");
    assert_eq!(expiry.committed_by.as_str(), "loonfs");
    assert_eq!(
        kinds(&expiry.events),
        vec![format!("deletion_expired {}", deletion.committed_seq)]
    );
    let undelete = request(vec![FilesystemOperation::undelete(
        gone.inode_id,
        deletion.committed_seq,
        &Default::default(),
    )]);
    let error = fixture
        .try_commit(&undelete)
        .expect_err("an expired deletion can't be undone");
    assert_eq!(error.code(), ErrorCode::NotDeleted);

    // The floor removed the delete's receipt, so its commit ID makes a new commit.
    let mut reused = deleted.clone();
    reused.operations = vec![put("/again.txt", b"a")];
    let again = fixture
        .try_commit(&reused)
        .expect("a reused commit ID commits again");
    assert_eq!(again.committed_seq, ChangeSeq(5));
}

#[tokio::test]
async fn an_undelete_restores_the_deleted_name() {
    let fixture = fixture();
    fixture.commit(vec![put("/dir/file", b"f")]);
    let dir = fixture.stat("/dir").await;
    let deletion = fixture.commit(vec![FilesystemOperation::DeletePath {
        path: path("/dir"),
        behavior: DeleteDirectoryBehavior::Recursive,
        expected_inode_id: Some(dir.inode_id),
    }]);
    let restored = fixture.commit(vec![FilesystemOperation::undelete(
        dir.inode_id,
        deletion.committed_seq,
        &Default::default(),
    )]);
    assert_eq!(kinds(&restored.events), vec!["undeleted dir".to_owned()]);
    assert_eq!(fixture.stat("/dir/file").await.size_bytes(), Some(1));
}
