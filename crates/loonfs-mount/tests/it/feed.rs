//! Following the change feed: each event kind's rule on clean rows, moves
//! into and out of the mount, recursive deletes, the root's ancestors, and a
//! rebuild after `rebootstrap_required`.

use crate::namespace::{
    commit, content, deleted, directory, eventually, file, file_created, moved, name, refresh,
    ScriptedNamespace, OUTSIDE, ROOT, WORK,
};
use bytes::Bytes;
use loonfs_mount::{
    CachedPages, Engine, Invalidation, ItemId, ItemKind, PublicationStop, Readiness,
};
use loonfs_types::api::v0::FilesystemChange;
use loonfs_types::{
    AttributeKey, AttributeValue, Attributes, AttributesRevisionNo, BindingVersion, ChangeSeq,
    ContentId, InodeId,
};
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};

fn project() -> ScriptedNamespace {
    let notes = content(&ContentId::generate(), b"notes");
    let deep = content(&ContentId::generate(), b"deep");
    let readme = content(&ContentId::generate(), b"read me");
    ScriptedNamespace::new(
        vec![
            directory(11, ROOT, "/work/project/docs"),
            file(12, 11, "/work/project/docs/notes.txt", &notes),
            directory(15, 11, "/work/project/docs/sub"),
            file(16, 15, "/work/project/docs/sub/deep.txt", &deep),
            file(13, ROOT, "/work/project/readme.md", &readme),
        ],
        vec![
            (12, Bytes::from_static(b"notes")),
            (16, Bytes::from_static(b"deep")),
            (13, Bytes::from_static(b"read me")),
        ],
    )
}

fn attributes(entries: &[(&str, &str)]) -> Attributes {
    Attributes::new(
        entries
            .iter()
            .map(|(key, value)| {
                (
                    AttributeKey::parse(key).expect("a key"),
                    AttributeValue::parse(value).expect("a value"),
                )
            })
            .collect::<BTreeMap<_, _>>(),
    )
    .expect("attributes")
}

fn item(engine: &Engine, parent: ItemId, entry_name: &str) -> Option<ItemId> {
    engine
        .lookup(parent, OsStr::new(entry_name))
        .map(|entry| entry.item)
}

/// Checks the creation, move, and attribute rules on clean rows, that the
/// events with no effect on a mount change nothing, and that the kernel
/// gets the invalidations of a page.
#[tokio::test]
async fn creations_moves_and_attribute_changes_apply_to_clean_rows() {
    let namespace = project();
    let engine = namespace.mount().await;
    let mut invalidations = engine.subscribe_invalidations();
    let readme = item(&engine, ItemId::ROOT, "readme.md").expect("the readme");
    let docs = item(&engine, ItemId::ROOT, "docs").expect("the docs");
    let notes = item(&engine, docs, "notes.txt").expect("the notes");
    let log = content(&ContentId::generate(), b"log");
    namespace.push_commit(commit(
        6,
        vec![
            FilesystemChange::DirectoryCreated {
                inode_id: InodeId(20),
                parent_inode_id: InodeId(ROOT),
                display_name: name("build"),
                binding_version: BindingVersion::parse("build").expect("a binding version"),
            },
            file_created(21, 20, "out.log", &log),
        ],
    ));
    namespace.push_commit(commit(
        7,
        vec![
            moved(13, ROOT, 20, "README"),
            FilesystemChange::AttributesChanged {
                inode_id: InodeId(12),
                attributes_revision_no: AttributesRevisionNo(1),
                attributes: attributes(&[("fs.mode", "600"), ("owner", "docs-team")]),
            },
        ],
    ));
    let access_changed = serde_json::from_value(serde_json::json!({
        "kind": "access_changed", "inode_id": "ino_11", "access_revision_no": 1,
        "boundary": false, "grants": {},
    }))
    .expect("an access event");
    namespace.push_commit(commit(
        8,
        vec![
            FilesystemChange::AttributesChanged {
                inode_id: InodeId(21),
                attributes_revision_no: AttributesRevisionNo(1),
                attributes: attributes(&[("fs.type", "symlink"), ("fs.target", "../docs")]),
            },
            access_changed,
            FilesystemChange::DeletionExpired {
                inode_id: InodeId(99),
                deletion_seq: ChangeSeq(3),
            },
        ],
    ));
    refresh(&engine, 8).await;

    let build = engine
        .lookup(ItemId::ROOT, OsStr::new("build"))
        .expect("the new directory");
    assert_eq!(build.attr.kind, ItemKind::Directory);
    assert_eq!(
        item(&engine, build.item, "README"),
        Some(readme),
        "a move keeps the item"
    );
    assert_eq!(item(&engine, ItemId::ROOT, "readme.md"), None);
    assert_eq!(engine.getattr(notes).expect("the notes").mode, 0o600);
    let link = engine
        .lookup(build.item, OsStr::new("out.log"))
        .expect("the symlink");
    assert_eq!(link.attr.kind, ItemKind::Symlink);
    assert_eq!(engine.readlink(link.item), Some(OsString::from("../docs")));
    assert_eq!(engine.status().applied_seq, ChangeSeq(8));

    let mut received = Vec::new();
    while let Ok(invalidation) = invalidations.try_recv() {
        received.push(invalidation);
    }
    for expected in [
        Invalidation::Entry {
            parent: ItemId::ROOT,
            name: OsString::from("build"),
        },
        Invalidation::Delete {
            parent: ItemId::ROOT,
            child: readme,
            name: OsString::from("readme.md"),
        },
        Invalidation::Entry {
            parent: build.item,
            name: OsString::from("README"),
        },
        Invalidation::Inode {
            item: notes,
            pages: CachedPages::None,
        },
    ] {
        assert!(received.contains(&expected), "{expected:?} in {received:?}");
    }
}

/// Checks that a recursive delete removes the subtree below the deleted
/// directory, that a move out of the mount removes the item, that a removed
/// item stays readable by attributes until the kernel forgets it, and that
/// an undelete reads the item again by inode.
#[tokio::test]
async fn a_recursive_delete_and_a_move_out_remove_items() {
    let namespace = project();
    let engine = namespace.mount().await;
    let docs = item(&engine, ItemId::ROOT, "docs").expect("the docs");
    let sub = item(&engine, docs, "sub").expect("the subdirectory");
    let deep = item(&engine, sub, "deep.txt").expect("the deep file");
    let readme = item(&engine, ItemId::ROOT, "readme.md").expect("the readme");
    namespace.push_commit(commit(6, vec![deleted(11, ROOT, "docs")]));
    namespace.push_commit(commit(7, vec![moved(13, ROOT, OUTSIDE, "elsewhere.md")]));
    refresh(&engine, 7).await;

    assert_eq!(item(&engine, ItemId::ROOT, "docs"), None);
    assert_eq!(item(&engine, ItemId::ROOT, "readme.md"), None);
    assert_eq!(engine.readdir(ItemId::ROOT, 0).count(), 0);
    assert!(
        engine.getattr(deep).is_some(),
        "the kernel still has a lookup of the deep file"
    );
    assert_eq!(engine.status().entries, 1);
    engine.forget(deep, 1);
    refresh(&engine, 7).await;
    assert_eq!(engine.getattr(deep), None);
    assert_eq!(engine.getattr(readme).map(|attr| attr.size_bytes), Some(7));
    engine.forget(readme, 1);

    namespace.push_commit(commit(
        8,
        vec![FilesystemChange::Undeleted {
            inode_id: InodeId(13),
            parent_inode_id: InodeId(ROOT),
            display_name: name("restored.md"),
            binding_version: BindingVersion::parse("restored").expect("a binding version"),
        }],
    ));
    refresh(&engine, 8).await;
    let restored = engine
        .lookup(ItemId::ROOT, OsStr::new("restored.md"))
        .expect("the undeleted file");
    assert_eq!(restored.attr.size_bytes, 7);
    assert_eq!(
        namespace.requests_to("/inodes/ino_13").len(),
        1,
        "read by inode"
    );
}

/// Checks that a directory moved into the mount is listed at a snapshot and
/// stays out of the tree until the feed reaches the snapshot, and that the
/// commits in between apply to its rows.
#[tokio::test]
async fn a_moved_in_directory_is_hidden_until_the_feed_reaches_its_snapshot() {
    let namespace = project();
    let engine = namespace.mount().await;
    let a = content(&ContentId::generate(), b"a");
    let b = content(&ContentId::generate(), b"b");
    namespace.edit(|script| {
        script
            .entries
            .insert(30, directory(30, OUTSIDE, "/elsewhere/incoming"));
        script.listings.insert(
            30,
            vec![
                file(31, 30, "/elsewhere/incoming/a.txt", &a),
                file(32, 30, "/elsewhere/incoming/b.txt", &b),
            ],
        );
        script.feed_through_seq = 6;
    });
    namespace.push_commit(commit(6, vec![moved(30, OUTSIDE, ROOT, "incoming")]));
    namespace.push_commit(commit(7, vec![file_created(32, 30, "b.txt", &b)]));
    refresh(&engine, 6).await;
    assert_eq!(engine.status().applied_seq, ChangeSeq(6));
    assert_eq!(
        item(&engine, ItemId::ROOT, "incoming"),
        None,
        "the feed hasn't reached the listing's snapshot at 7"
    );

    namespace.edit(|script| script.feed_through_seq = u64::MAX);
    refresh(&engine, 7).await;
    let incoming = item(&engine, ItemId::ROOT, "incoming").expect("the shown directory");
    let names: Vec<_> = engine
        .readdir(incoming, 0)
        .map(|entry| entry.name)
        .collect();
    assert_eq!(names, ["a.txt", "b.txt"]);
    let snapshots = namespace.requests_to("/snapshots");
    assert_eq!(snapshots.len(), 2, "admission's snapshot and the listing's");
    eventually(|| {
        namespace
            .requests_to("/snapshots/pin_00000000000000000002-0000000000000002")
            .len()
            == 1
    })
    .await;
}

/// Checks that a deletion of a directory above the root stops publication
/// and that status reports it.
#[tokio::test]
async fn a_deleted_root_ancestor_is_recorded_in_status() {
    let namespace = project();
    let engine = namespace.mount().await;
    namespace.push_commit(commit(6, vec![deleted(WORK, 1, "work")]));
    refresh(&engine, 6).await;
    assert_eq!(
        engine.status().publication_stop,
        Some(PublicationStop::RootDeleted {
            inode_id: InodeId(WORK),
            deleted_seq: ChangeSeq(6),
        })
    );
}

/// Checks that a feed that passed the retention floor makes the engine list
/// the tree again at a new snapshot, remove what the listing doesn't have,
/// and follow the feed from the new snapshot.
#[tokio::test]
async fn rebootstrap_required_lists_the_tree_again() {
    let namespace = project();
    let engine = namespace.mount().await;
    let notes = item(
        &engine,
        item(&engine, ItemId::ROOT, "docs").expect("docs"),
        "notes.txt",
    );
    let added = content(&ContentId::generate(), b"added");
    namespace.edit(|script| {
        script.head_seq = 9;
        script.retention_floor_seq = 9;
        let listing = script.listings.get_mut(&ROOT).expect("the root's listing");
        listing.retain(|entry| entry.inode_id != InodeId(13));
        listing.push(file(14, ROOT, "/work/project/added.txt", &added));
    });
    refresh(&engine, 9).await;

    assert_eq!(item(&engine, ItemId::ROOT, "readme.md"), None);
    assert!(item(&engine, ItemId::ROOT, "added.txt").is_some());
    let docs = item(&engine, ItemId::ROOT, "docs").expect("docs");
    assert_eq!(
        item(&engine, docs, "notes.txt"),
        notes,
        "an unchanged item stays"
    );
    let status = engine.status();
    assert_eq!(
        (status.applied_seq, status.readiness),
        (ChangeSeq(9), Readiness::Ready)
    );
    assert_eq!(namespace.requests_to("/snapshots").len(), 2);
    eventually(|| {
        namespace
            .requests_to("/changes")
            .last()
            .is_some_and(|poll| poll.query("after_seq") == Some("9"))
    })
    .await;
}
