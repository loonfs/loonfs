//! Admitting a mount: the scan at a snapshot, the entry budget, the checks
//! that refuse a mount, and where the feed starts.

use crate::namespace::{
    commit, content, directory, file, file_created, refresh, ScriptedNamespace, ROOT,
};
use bytes::Bytes;
use loonfs_mount::{ItemId, ItemKind, MountAccess, MountError, Readiness};
use loonfs_types::{ChangeSeq, ContentId};
use std::assert_matches;
use std::ffi::OsStr;

fn three_entries() -> ScriptedNamespace {
    let notes = content(&ContentId::generate(), b"notes");
    let readme = content(&ContentId::generate(), b"read me");
    ScriptedNamespace::new(
        vec![
            directory(11, ROOT, "/work/project/docs"),
            file(12, 11, "/work/project/docs/notes.txt", &notes),
            file(13, ROOT, "/work/project/readme.md", &readme),
        ],
        vec![
            (12, Bytes::from_static(b"notes")),
            (13, Bytes::from_static(b"read me")),
        ],
    )
}

/// Checks that a mount whose tree fits the budget lists it at a snapshot
/// with attributes, deletes the snapshot, and shows the tree, and that a
/// budget one entry smaller refuses the mount after listing one entry past
/// it.
#[tokio::test]
async fn admission_lists_at_a_snapshot_and_refuses_one_entry_past_the_budget() {
    let namespace = three_entries();
    let mut config = namespace.config();
    config.max_entries = 3;
    let engine = namespace
        .mount_with(config)
        .await
        .expect("three entries fit");

    let docs = engine
        .lookup(ItemId::ROOT, OsStr::new("docs"))
        .expect("the listed directory");
    assert_eq!(docs.attr.kind, ItemKind::Directory);
    let notes = engine
        .lookup(docs.item, OsStr::new("notes.txt"))
        .expect("the listed file");
    assert_eq!(
        (notes.attr.kind, notes.attr.size_bytes),
        (ItemKind::File, 5)
    );
    let names: Vec<_> = engine
        .readdir(ItemId::ROOT, 0)
        .map(|entry| entry.name)
        .collect();
    assert_eq!(names, ["docs", "readme.md"]);
    let status = engine.status();
    assert_eq!(status.applied_seq, ChangeSeq(5));
    assert_eq!((status.entries, status.readiness), (4, Readiness::Ready));
    assert!(status.transports.direct_get);
    assert_eq!(status.inline_limit_bytes, 65_536);

    let listings = namespace.requests_to("/inodes/ino_10/children");
    assert_eq!(listings.len(), 1);
    let listing = &listings[0];
    assert_eq!(listing.query("recursive"), Some("true"));
    assert_eq!(listing.query("include_attributes"), Some("true"));
    let snapshot_id = listing.query("snapshot_id").expect("a snapshot");
    let deletes = namespace.requests_to(&format!("/snapshots/{snapshot_id}"));
    assert_eq!(deletes.len(), 1, "admission deletes its snapshot");

    let over_budget = three_entries();
    let mut config = over_budget.config();
    config.max_entries = 2;
    let refused = over_budget
        .mount_with(config)
        .await
        .expect_err("three entries don't fit");
    assert_matches!(
        refused,
        MountError::EntryBudgetExceeded {
            listed_entries: 3,
            max_entries: 2,
            ..
        }
    );
    let listing = &over_budget.requests_to("/inodes/ino_10/children")[0];
    assert_eq!(listing.query("limit"), Some("3"));
    assert_eq!(
        over_budget
            .requests_to("/snapshots/pin_00000000000000000001-0000000000000001")
            .len(),
        1
    );
    assert!(
        !over_budget.state_directory.path().join("tree.db").exists(),
        "a refused mount leaves no tree"
    );
}

/// Checks the refusals that don't depend on the tree's size.
#[tokio::test]
async fn admission_refuses_a_reserved_child_a_file_and_a_writable_case_insensitive_namespace() {
    let reserved =
        ScriptedNamespace::new(vec![directory(11, ROOT, "/work/project/.loonfs")], vec![]);
    assert_matches!(
        reserved.mount_with(reserved.config()).await,
        Err(MountError::ReservedName { .. })
    );

    let not_a_directory = three_entries();
    let mut config = not_a_directory.config();
    config.path = loonfs_types::AbsolutePath::parse("/work/project/readme.md").expect("a path");
    assert_matches!(
        not_a_directory.mount_with(config).await,
        Err(MountError::NotADirectory { .. })
    );

    let case_insensitive = three_entries();
    case_insensitive.edit(|script| script.case_sensitive = false);
    let mut config = case_insensitive.config();
    config.access = MountAccess::Writable;
    assert_matches!(
        case_insensitive.mount_with(config).await,
        Err(MountError::CaseInsensitiveNaming { .. })
    );
    let read_only = case_insensitive.config();
    case_insensitive
        .mount_with(read_only)
        .await
        .expect("a read-only mount of a case-insensitive namespace");
}

/// Checks that the feed starts after the snapshot's captured sequence, with
/// strong polls, and that `refresh_at_least` waits for the commit it names.
#[tokio::test]
async fn the_feed_starts_after_the_applied_sequence_with_strong_polls() {
    let namespace = three_entries();
    let engine = namespace.mount().await;
    let added = content(&ContentId::generate(), b"added");
    namespace.push_commit(commit(6, vec![file_created(14, ROOT, "added.txt", &added)]));
    namespace.push_commit(commit(7, vec![]));
    refresh(&engine, 7).await;

    assert_eq!(engine.status().applied_seq, ChangeSeq(7));
    assert!(engine
        .lookup(ItemId::ROOT, OsStr::new("added.txt"))
        .is_some());
    let polls = namespace.requests_to("/changes");
    assert_eq!(polls[0].query("after_seq"), Some("5"));
    assert!(polls
        .iter()
        .all(|poll| poll.query("consistency") == Some("strong")));
}
