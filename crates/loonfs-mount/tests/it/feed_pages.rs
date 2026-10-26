//! Feed pages: a commit stays out of the mount while the server delays the
//! page that includes it, and each page's changes reach the invalidation
//! stream together.

use crate::support::{delete, fixture, item, move_path, put, refresh, update_attributes, WAIT};
use loonfs_mount::{CachedPages, Invalidation, ItemId};
use loonfs_test_support::clock::bounded;
use std::ffi::OsString;
use tokio::sync::mpsc;

fn drain(invalidations: &mut mpsc::Receiver<Invalidation>) -> Vec<Invalidation> {
    let mut received = Vec::new();
    while let Ok(invalidation) = invalidations.try_recv() {
        received.push(invalidation);
    }
    received
}

fn entry(parent: ItemId, name: &str) -> Invalidation {
    Invalidation::Entry {
        parent,
        name: OsString::from(name),
    }
}

fn inode(item: ItemId) -> Invalidation {
    Invalidation::Inode {
        item,
        pages: CachedPages::None,
    }
}

fn removed(parent: ItemId, child: ItemId, name: &str) -> Invalidation {
    Invalidation::Delete {
        parent,
        child,
        name: OsString::from(name),
    }
}

/// Checks that while the server delays the feed's page, the commit stays out
/// of the mount, `refresh_at_least` waits, and the kernel gets no
/// invalidation, and that all three happen once the page arrives.
#[tokio::test]
async fn a_held_feed_page_delays_the_change_and_refresh_waits_for_it() {
    let fixture = fixture();
    let engine = fixture.mount().await;
    let applied = engine.status().applied_seq;
    let mut invalidations = engine.subscribe_invalidations();
    let hold = fixture.server.hold_feed_pages();
    let commit = fixture.commit(vec![put("new.txt", b"new")]);
    let refresh = engine.refresh_at_least(commit.committed_seq);
    tokio::pin!(refresh);
    tokio::select! {
        result = &mut refresh => panic!("the refresh ended while its page was held: {result:?}"),
        () = hold.wait_for_held_responses(1) => {}
    }
    assert_eq!(item(&engine, "new.txt"), None);
    assert_eq!(engine.status().applied_seq, applied);
    assert!(drain(&mut invalidations).is_empty());

    hold.release();
    bounded(WAIT, refresh)
        .await
        .expect("the refresh ends in time")
        .expect("the refresh");
    assert!(item(&engine, "new.txt").is_some());
    assert_eq!(
        drain(&mut invalidations),
        [entry(ItemId::ROOT, "new.txt"), inode(ItemId::ROOT)]
    );
}

/// Checks that the invalidation stream reports what each page changed: the
/// names that appeared and disappeared, and the items and directories whose
/// attributes changed, in the order the page's events changed them.
#[tokio::test]
async fn invalidations_report_the_entries_and_inodes_that_each_page_changed() {
    let fixture = fixture();
    fixture.commit(vec![
        put("docs/notes.txt", b"notes"),
        put("readme.md", b"read me"),
    ]);
    let engine = fixture.mount().await;
    let docs = item(&engine, "docs").expect("the docs");
    let notes = item(&engine, "docs/notes.txt").expect("the notes");
    let readme = item(&engine, "readme.md").expect("the readme");
    let mut invalidations = engine.subscribe_invalidations();
    fixture.commit(vec![
        put("new.txt", b"new"),
        update_attributes("docs/notes.txt", &[("fs.mode", "600")], &[]),
    ]);
    let moved = fixture.commit(vec![move_path("readme.md", "docs/README")]);
    refresh(&engine, moved.committed_seq).await;
    assert_eq!(
        drain(&mut invalidations),
        [
            entry(ItemId::ROOT, "new.txt"),
            inode(ItemId::ROOT),
            inode(notes),
            removed(ItemId::ROOT, readme, "readme.md"),
            inode(ItemId::ROOT),
            entry(docs, "README"),
            inode(docs),
        ]
    );

    let deleted = fixture.commit(vec![delete("docs/notes.txt")]);
    refresh(&engine, deleted.committed_seq).await;
    assert_eq!(
        drain(&mut invalidations),
        [removed(docs, notes, "notes.txt"), inode(docs)]
    );
    assert_eq!(engine.status().dropped_invalidations, 0);
}
