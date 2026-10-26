//! Following the change feed: each kind of event that the simulated server
//! produces, applied to a mount without local changes.

use crate::support::{
    delete, engine_tree, eventually, fixture, item, mkdir, move_path, open, operations, put, read,
    refresh, request, undelete, update_attributes, Fixture, MOUNT_PATH,
};
use loonfs_mount::{ItemKind, PublicationStop};
use loonfs_mount_sim::{EchoOrder, Operation, RequestRecord};
use loonfs_types::api::v0::FilesystemChange;
use std::ffi::OsString;

/// A namespace whose mounted directory has `docs/notes.txt`,
/// `docs/sub/deep.txt`, and `readme.md`, and whose `/elsewhere` has a
/// directory and a file to move into the mount.
fn project() -> Fixture {
    let fixture = fixture();
    fixture.commit(vec![
        put("docs/notes.txt", b"notes"),
        put("docs/sub/deep.txt", b"deep"),
        put("readme.md", b"read me"),
        put("/elsewhere/incoming/a.txt", b"a"),
        put("/elsewhere/loose.txt", b"loose"),
    ]);
    fixture
}

/// Returns the requests other than feed polls.
fn other_than_polls(trace: &[RequestRecord]) -> Vec<Operation> {
    operations(trace)
        .into_iter()
        .filter(|operation| *operation != Operation::ListChanges)
        .collect()
}

/// Checks that created directories and files appear, including the parents
/// that a put creates, and that the feed is read with strong polls from the
/// applied sequence.
#[tokio::test]
async fn created_directories_and_files_appear_through_strong_polls_from_the_applied_sequence() {
    let fixture = project();
    let engine = fixture.mount().await;
    let admitted = engine.status().applied_seq;
    fixture.server.take_trace();
    let commit = fixture.commit(vec![
        mkdir("build", false),
        put("build/gen/out.log", b"log"),
    ]);
    let created: Vec<_> = commit
        .events
        .iter()
        .map(|event| matches!(event, FilesystemChange::FileCreated { .. }))
        .collect();
    assert_eq!(created, [false, false, true], "the put creates `build/gen`");
    refresh(&engine, commit.committed_seq).await;

    let out = item(&engine, "build/gen/out.log").expect("the created file");
    let attr = engine.getattr(out).expect("the file's attributes");
    assert_eq!((attr.kind, attr.size_bytes), (ItemKind::File, 3));
    assert_eq!(
        read(&open(&engine, "build/gen/out.log").await, 0, 10).await,
        "log"
    );
    assert_eq!(engine.status().applied_seq, commit.committed_seq);
    let trace = fixture.server.take_trace();
    let polls: Vec<_> = trace
        .iter()
        .filter(|record| record.operation == Operation::ListChanges)
        .collect();
    let admitted = admitted.0.to_string();
    assert_eq!(polls.len(), 1, "one poll for one refresh");
    assert_eq!(polls[0].query_param("after_seq"), Some(admitted.as_str()));
    assert_eq!(polls[0].query_param("consistency"), Some("strong"));
    assert_eq!(engine_tree(&engine), fixture.listed_tree().await);
}

/// Checks that a move within the mount keeps the item, that a move out of
/// the mount removes the subtree, and that a file moved into the mount is
/// read by inode, with no listing.
#[tokio::test]
async fn moves_within_out_of_and_into_the_mount_rebind_remove_and_read_items() {
    let fixture = project();
    let engine = fixture.mount().await;
    let readme = item(&engine, "readme.md").expect("the readme");
    let deep = item(&engine, "docs/sub/deep.txt").expect("the deep file");
    let entries = engine.status().entries;
    fixture.server.take_trace();
    fixture.commit(vec![move_path("readme.md", "docs/README")]);
    fixture.commit(vec![move_path("docs/sub", "/elsewhere/sub")]);
    let last = fixture.commit(vec![move_path("/elsewhere/loose.txt", "loose.txt")]);
    refresh(&engine, last.committed_seq).await;

    assert_eq!(
        item(&engine, "docs/README"),
        Some(readme),
        "a move keeps the item"
    );
    assert_eq!(item(&engine, "readme.md"), None);
    assert_eq!(item(&engine, "docs/sub"), None);
    assert!(
        engine.getattr(deep).is_some(),
        "an item that left the tree stays while the kernel has a lookup of it"
    );
    engine.forget(deep, 1);
    eventually(|| engine.getattr(deep).is_none()).await;
    assert_eq!(engine.status().entries, entries - 2 + 1);
    assert_eq!(
        read(&open(&engine, "loose.txt").await, 0, 10).await,
        "loose"
    );
    let trace = fixture.server.take_trace();
    assert_eq!(
        other_than_polls(&trace),
        [
            Operation::GetInode,
            Operation::GetFileRevisionContentByInode
        ]
    );
    let read_by_inode = trace
        .iter()
        .find(|record| record.operation == Operation::GetInode)
        .expect("the moved-in file's read");
    assert_eq!(read_by_inode.query_param("consistency"), Some("strong"));
    assert_eq!(engine_tree(&engine), fixture.listed_tree().await);
}

/// Checks that a directory moved into the mount is listed at a snapshot and
/// stays hidden while the feed is behind that snapshot, that the commits in
/// between apply to its rows, and that this listing is the one listing after
/// admission (scaling invariant 1).
#[tokio::test]
async fn a_directory_moved_in_is_hidden_until_the_feed_reaches_its_snapshot() {
    let fixture = project();
    let engine = fixture.mount().await;
    fixture.server.take_trace();
    let moved = fixture.commit(vec![move_path("/elsewhere/incoming", "incoming")]);
    let hold = fixture
        .server
        .order_next_commit_echo(EchoOrder::ResponseFirst);
    let later = fixture
        .client
        .commit(
            &fixture.namespace_id,
            &fixture.actor,
            &request(vec![put("incoming/b.txt", b"b")]),
        )
        .await
        .expect("the commit's response comes before its echo");
    refresh(&engine, moved.committed_seq).await;
    assert_eq!(engine.status().applied_seq, moved.committed_seq);
    assert_eq!(
        item(&engine, "incoming"),
        None,
        "the listing's snapshot is at {}, past the applied sequence",
        later.committed_seq
    );

    hold.release();
    refresh(&engine, later.committed_seq).await;
    let incoming = item(&engine, "incoming").expect("the shown directory");
    let names: Vec<OsString> = engine
        .readdir(incoming, 0)
        .map(|entry| entry.name)
        .collect();
    assert_eq!(names, ["a.txt", "b.txt"]);
    fixture.wait_for_request(Operation::DeleteSnapshot).await;
    let trace = fixture.server.take_trace();
    use Operation::*;
    assert_eq!(
        other_than_polls(&trace),
        [
            CreateCommit,
            GetInode,
            CreateSnapshot,
            GetInode,
            ListInodeChildren,
            DeleteSnapshot
        ]
    );
    let listing = trace
        .iter()
        .find(|record| record.operation == ListInodeChildren)
        .expect("the listing");
    let snapshot_id = listing.query_param("snapshot_id").expect("at a snapshot");
    assert_eq!(listing.query_param("recursive"), Some("true"));
    assert!(trace
        .iter()
        .any(|record| record.operation == DeleteSnapshot && record.path.ends_with(snapshot_id)));
    assert_eq!(engine_tree(&engine), fixture.listed_tree().await);
}

/// Checks that a recursive delete has one event for the deleted directory
/// and removes everything below it, and that an undelete reads and lists the
/// directory again.
#[tokio::test]
async fn a_recursive_delete_removes_a_subtree_and_an_undelete_brings_it_back() {
    let fixture = project();
    let engine = fixture.mount().await;
    let tree = engine_tree(&engine);
    let entries = engine.status().entries;
    let docs = fixture.inode("docs").await;
    let deleted = fixture.commit(vec![delete("docs")]);
    assert_eq!(
        deleted.events.len(),
        1,
        "one event for the deleted directory"
    );
    refresh(&engine, deleted.committed_seq).await;
    assert_eq!(item(&engine, "docs"), None);
    assert_eq!(engine.status().entries, entries - 4);

    fixture.server.take_trace();
    let undeleted = fixture.commit(vec![undelete(docs, deleted.committed_seq)]);
    refresh(&engine, undeleted.committed_seq).await;
    assert_eq!(engine_tree(&engine), tree);
    assert_eq!(engine.status().entries, entries);
    fixture.wait_for_request(Operation::DeleteSnapshot).await;
    use Operation::*;
    assert_eq!(
        other_than_polls(&fixture.server.take_trace()),
        [
            GetInode,
            CreateSnapshot,
            GetInode,
            ListInodeChildren,
            DeleteSnapshot
        ]
    );
}

/// Checks that the expiry of a deletion that the retention floor passed
/// changes nothing in the mount.
#[tokio::test]
async fn an_expired_deletion_changes_nothing() {
    let fixture = project();
    let engine = fixture.mount().await;
    let deleted = fixture.commit(vec![delete("readme.md")]);
    refresh(&engine, deleted.committed_seq).await;
    let tree = engine_tree(&engine);
    let entries = engine.status().entries;
    fixture.server.take_trace();
    fixture
        .server
        .advance_retention_floor(&fixture.namespace_id, deleted.committed_seq);
    let head = fixture.head_seq();
    assert_eq!(
        head.0,
        deleted.committed_seq.0 + 1,
        "the floor advance expires the deletion"
    );
    refresh(&engine, head).await;
    assert_eq!(engine.status().applied_seq, head);
    assert_eq!(
        (engine_tree(&engine), engine.status().entries),
        (tree, entries)
    );
    assert!(other_than_polls(&fixture.server.take_trace()).is_empty());
}

/// Checks that `fs.mode`, `fs.mtime_ms`, and `fs.type` with `fs.target`
/// change the mode, the modification time, and the kind of an item, and that
/// other keys change nothing that the mount shows.
#[tokio::test]
async fn attribute_changes_to_fs_keys_set_the_mode_the_time_and_a_symlink_target() {
    let fixture = project();
    fixture.commit(vec![put("link", b"")]);
    let engine = fixture.mount().await;
    let notes = item(&engine, "docs/notes.txt").expect("the notes");
    let readme = item(&engine, "readme.md").expect("the readme");
    let link = item(&engine, "link").expect("the future symlink");
    let commit = fixture.commit(vec![
        update_attributes("docs/notes.txt", &[("fs.mode", "600")], &[]),
        update_attributes(
            "readme.md",
            &[("fs.mtime_ms", "1234567"), ("fs.mtime_revision_no", "1")],
            &[],
        ),
        update_attributes(
            "link",
            &[("fs.type", "symlink"), ("fs.target", "../docs")],
            &[],
        ),
    ]);
    refresh(&engine, commit.committed_seq).await;
    assert_eq!(item(&engine, "docs/notes.txt"), Some(notes));
    assert_eq!(engine.getattr(notes).expect("the notes").mode, 0o600);
    assert_eq!(
        engine.getattr(readme).expect("the readme").mtime_ms,
        1_234_567
    );
    let symlink = item(&engine, "link").expect("the symlink");
    assert_ne!(symlink, link, "a file that becomes a symlink is a new item");
    let attr = engine.getattr(symlink).expect("the symlink");
    assert_eq!((attr.kind, attr.size_bytes), (ItemKind::Symlink, 7));
    assert_eq!(engine.readlink(symlink), Some(OsString::from("../docs")));

    let shown = engine.getattr(readme);
    let commit = fixture.commit(vec![
        update_attributes("docs/notes.txt", &[], &["fs.mode"]),
        update_attributes("readme.md", &[("owner", "docs")], &[]),
    ]);
    refresh(&engine, commit.committed_seq).await;
    assert_eq!(engine.getattr(notes).expect("the notes").mode, 0o644);
    assert_eq!(engine.getattr(readme), shown, "a key outside `fs.`");
    assert_eq!(engine_tree(&engine), fixture.listed_tree().await);
}

/// Checks that a deletion of the mount's root, or of a directory above it,
/// stops publication and that status reports it.
#[tokio::test]
async fn a_deleted_root_or_ancestor_stops_publication() {
    for deleted in [MOUNT_PATH, "/work"] {
        let fixture = project();
        let engine = fixture.mount().await;
        let inode_id = fixture.inode(deleted).await;
        let commit = fixture.commit(vec![delete(deleted)]);
        refresh(&engine, commit.committed_seq).await;
        assert_eq!(
            engine.status().publication_stop,
            Some(PublicationStop::RootDeleted {
                inode_id,
                deleted_seq: commit.committed_seq,
            }),
            "{deleted}"
        );
    }
}
