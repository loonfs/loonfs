//! The feed: commits that wait for listings, listing pages that fail,
//! events that change nothing, and refreshes.

use super::*;
use crate::item_table::ItemTable;
use crate::records::CONTENT_DIRTY;
use crate::status::Warning;

/// Checks that a commit that brings a directory into the mount isn't applied
/// until the listing it needs arrives, and that the directory stays hidden
/// until the applied sequence reaches the listing's snapshot.
#[test]
fn a_commit_that_brings_in_a_directory_waits_for_its_listing() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, shared, workers) = task(directory.path(), &[file(11, 10, "/project/a.txt")]);
    task.start();
    assert!(matches!(
        workers.take()[..],
        [Work::PollFeed {
            after_seq: ChangeSeq(5)
        }]
    ));

    task.handle(page(5, vec![commit(6, vec![moved_in()])]));
    let read = workers.take();
    assert!(
        matches!(
            read[..],
            [Work::ReadEnteringItem {
                inode_id: InodeId(30),
                snapshot: None
            }]
        ),
        "{read:?}"
    );
    assert_eq!(
        task.applied_seq,
        ChangeSeq(5),
        "the commit waits for its read"
    );

    let snapshot = SnapshotRef {
        snapshot_id: PinId::parse("pin_00000000000000000002-0000000000000002").expect("a pin"),
        captured_seq: ChangeSeq(7),
    };
    task.handle(done(WorkOutcome::EnteringDirectory {
        inode_id: InodeId(30),
        entry: entry(30, 2, "/elsewhere/incoming", PathEntryKind::Directory {}),
        snapshot: snapshot.clone(),
        created: true,
    }));
    let (processed, written) = oneshot::channel();
    task.handle(done(WorkOutcome::ListingPage {
        listing: Listing::Entering(InodeId(30)),
        entries: vec![file(31, 30, "/elsewhere/incoming/b.txt")],
        processed,
    }));
    written
        .blocking_recv()
        .expect("a reply")
        .expect("the page is written");
    task.handle(done(WorkOutcome::EnteringItemRead {
        inode_id: InodeId(30),
        snapshot: None,
        result: Ok(EnteringItem::Listed),
    }));
    assert_eq!(task.applied_seq, ChangeSeq(6));
    assert!(
        !shown(&shared, "incoming"),
        "the feed is behind the snapshot"
    );

    task.handle(ControlMessage::Tick);
    assert!(matches!(
        workers.take()[..],
        [Work::PollFeed {
            after_seq: ChangeSeq(6)
        }]
    ));
    task.handle(page(6, vec![commit(7, vec![])]));
    assert!(shown(&shared, "incoming"));
    let deleted = workers.take();
    assert!(
        matches!(&deleted[..], [Work::DeleteSnapshot { snapshot_id }] if *snapshot_id == snapshot.snapshot_id),
        "{deleted:?}"
    );
}

/// Returns the event of a commit that moves `/elsewhere/incoming` (inode 30)
/// into the mount's root.
fn moved_in() -> FilesystemChange {
    FilesystemChange::Moved {
        inode_id: InodeId(30),
        source_parent_inode_id: InodeId(2),
        source_display_name: DisplayName::parse("incoming").expect("a name"),
        destination_parent_inode_id: InodeId(10),
        destination_display_name: DisplayName::parse("incoming").expect("a name"),
        binding_version: BindingVersion::parse("moved").expect("a version"),
    }
}

/// Checks that a listing page that the database can't write fails its
/// listing: the control task replies to the page with the error, so the
/// worker stops the listing, and the commit that waits for the listing
/// isn't applied.
#[test]
fn a_listing_page_that_isnt_written_fails_its_listing() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, shared, workers) = task(directory.path(), &[file(11, 10, "/project/a.txt")]);
    task.start();
    workers.take();
    task.handle(page(5, vec![commit(6, vec![moved_in()])]));
    workers.take();
    let snapshot = SnapshotRef {
        snapshot_id: PinId::parse("pin_00000000000000000002-0000000000000002").expect("a pin"),
        captured_seq: ChangeSeq(7),
    };
    task.handle(done(WorkOutcome::EnteringDirectory {
        inode_id: InodeId(30),
        entry: entry(30, 2, "/elsewhere/incoming", PathEntryKind::Directory {}),
        snapshot,
        created: true,
    }));

    // Another connection keeps the database's write lock, and the task's
    // connection doesn't wait for it.
    let blocker =
        rusqlite::Connection::open(task.config.database_path()).expect("a second connection");
    blocker
        .execute_batch("BEGIN EXCLUSIVE")
        .expect("the write lock");
    task.database
        .connection()
        .pragma_update(None, "busy_timeout", 0)
        .expect("no busy wait");
    let (processed, written) = oneshot::channel();
    task.handle(done(WorkOutcome::ListingPage {
        listing: Listing::Entering(InodeId(30)),
        entries: vec![file(31, 30, "/elsewhere/incoming/b.txt")],
        processed,
    }));
    let error = written
        .blocking_recv()
        .expect("a reply")
        .expect_err("the page isn't written");
    blocker
        .execute_batch("ROLLBACK")
        .expect("the lock released");

    task.handle(done(WorkOutcome::EnteringItemRead {
        inode_id: InodeId(30),
        snapshot: None,
        result: Err(error),
    }));
    assert_eq!(task.applied_seq, ChangeSeq(5), "the commit isn't applied");
    assert!(!shown(&shared, "incoming"));
    assert!(
        task.entering.is_empty(),
        "the directory whose listing failed is removed, so the next poll reads it again"
    );
}

/// Checks that an `access_changed` event changes nothing but the applied
/// sequence. (The simulated server's namespaces are unrestricted, so the
/// integration tests can't produce this event.)
#[test]
fn an_access_change_changes_nothing_but_the_applied_sequence() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, shared, workers) = task(directory.path(), &[file(11, 10, "/project/a.txt")]);
    task.start();
    workers.take();
    let mut invalidations = shared.subscribers.subscribe();
    let access_changed = serde_json::from_value(serde_json::json!({
        "kind": "access_changed", "inode_id": "ino_11", "access_revision_no": 1,
        "boundary": false, "grants": {},
    }))
    .expect("an access event");
    task.handle(page(5, vec![commit(6, vec![access_changed])]));
    assert_eq!(task.applied_seq, ChangeSeq(6));
    assert!(shown(&shared, "a.txt"));
    assert!(invalidations.try_recv().is_err());
    assert!(workers.take().is_empty());
}

/// Checks that only a poll that starts after a refresh can reply to the
/// refresh: a page of the poll that was in flight when the refresh arrived
/// doesn't fail the refresh, and the next poll replies to a refresh whose sequence
/// the poll reached and fails a refresh whose sequence is past the head that
/// the poll read.
#[test]
fn a_refresh_past_the_head_that_a_later_poll_reads_fails_at_once() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, _shared, workers) = task(directory.path(), &[]);
    task.start();
    workers.take();
    let refresh = |task: &mut ControlTask, seq| {
        let (reply, replied) = oneshot::channel();
        task.handle(ControlMessage::RefreshAtLeast {
            seq: ChangeSeq(seq),
            reply,
        });
        replied
    };
    let mut reached = refresh(&mut task, 8);
    let mut past_head = refresh(&mut task, 9);
    task.handle(page(5, vec![commit(6, vec![]), commit(7, vec![])]));
    assert!(
        matches!(
            past_head.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ),
        "the poll in flight started before the call"
    );
    let polls = workers.take();
    assert!(
        matches!(
            polls[..],
            [Work::PollFeed {
                after_seq: ChangeSeq(7)
            }]
        ),
        "{polls:?}"
    );

    task.handle(page(7, vec![commit(8, vec![])]));
    assert!(matches!(reached.try_recv(), Ok(Ok(()))));
    let refused = past_head.try_recv().expect("a reply");
    assert!(
        matches!(
            refused,
            Err(MountError::SequenceNotCommitted {
                seq: ChangeSeq(9),
                head_seq: ChangeSeq(8)
            })
        ),
        "{refused:?}"
    );
}

/// Checks that a failed rebuild starts again after a wait that doubles
/// after each failure, and not at every tick in between.
#[test]
fn a_failed_rebuild_waits_longer_before_each_attempt() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, _shared, workers) = task(directory.path(), &[]);
    let clock = Arc::new(ManualClock::new(0));
    task.environment.timer = clock.clone();
    task.start();
    workers.take();
    let rebuilds = |work: Vec<Work>| {
        work.iter()
            .filter(|work| matches!(work, Work::Rebuild { .. }))
            .count()
    };
    let fail = |task: &mut ControlTask| {
        task.handle(done(WorkOutcome::RebuildDone(Err(MountError::Stopped))));
    };

    task.start_rebuild();
    assert_eq!(rebuilds(workers.take()), 1);
    fail(&mut task);
    task.handle(ControlMessage::Tick);
    assert_eq!(rebuilds(workers.take()), 0, "the first retry waits 250 ms");
    clock.advance_ms(250);
    task.handle(ControlMessage::Tick);
    assert_eq!(rebuilds(workers.take()), 1);

    fail(&mut task);
    clock.advance_ms(250);
    task.handle(ControlMessage::Tick);
    assert_eq!(rebuilds(workers.take()), 0, "the second retry waits 500 ms");
    clock.advance_ms(250);
    task.handle(ControlMessage::Tick);
    assert_eq!(rebuilds(workers.take()), 1);
}

/// Checks that a commit that moves the mount's root isn't applied when the
/// read of the root's new ancestors fails: the page fails, and the next poll
/// reads the commit again, so the mount never keeps ancestors that a later
/// deletion of a new ancestor can't be checked against.
#[test]
fn a_failed_read_of_the_roots_ancestors_fails_the_page() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, _shared, workers) = task(directory.path(), &[]);
    task.start();
    workers.take();
    let root_moved = FilesystemChange::Moved {
        inode_id: InodeId(10),
        source_parent_inode_id: InodeId(1),
        source_display_name: DisplayName::parse("project").expect("a name"),
        destination_parent_inode_id: InodeId(1),
        destination_display_name: DisplayName::parse("renamed").expect("a name"),
        binding_version: BindingVersion::parse("moved").expect("a version"),
    };
    task.handle(page(5, vec![commit(6, vec![root_moved])]));
    let read = workers.take();
    assert!(
        matches!(
            read[..],
            [Work::ReadAncestors {
                root_inode_id: InodeId(10)
            }]
        ),
        "{read:?}"
    );
    task.handle(done(WorkOutcome::AncestorsRead(Err(MountError::Stopped))));
    assert_eq!(task.applied_seq, ChangeSeq(5), "the commit isn't applied");
    task.handle(ControlMessage::Tick);
    assert!(matches!(
        workers.take()[..],
        [Work::PollFeed {
            after_seq: ChangeSeq(5)
        }]
    ));
}

/// Checks that an append from the feed keeps `append_only` set on a file
/// whose content has a local change, which the re-base of a local append on
/// remote appends depends on.
#[test]
fn an_append_keeps_append_only_set_on_a_file_with_a_local_change() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, _shared, workers) = task(directory.path(), &[file(11, 10, "/project/a.txt")]);
    task.start();
    workers.take();
    let mut record = ItemTable::new(task.database.connection())
        .find_by_inode(InodeId(11))
        .expect("a read")
        .expect("the file's row");
    record.write.dirty_groups |= CONTENT_DIRTY;
    write_item(task.database.connection(), &record).expect("the dirty row");
    let previous = record
        .server
        .content
        .clone()
        .expect("the file's content")
        .content_ref;
    let appended = ContentRef::blob_v1(
        namespace_id(),
        previous.content_id.clone(),
        b"/project/a.txt and more",
        ChecksumAlgorithm::Crc64nvme,
    );
    let append = FilesystemChange::ContentChanged {
        inode_id: InodeId(11),
        revision_no: RevisionNo(2),
        content_ref: appended,
        append_offset: Some(previous.size_bytes),
    };
    task.handle(page(5, vec![commit(6, vec![append])]));
    assert_eq!(task.applied_seq, ChangeSeq(6));
    let record = ItemTable::new(task.database.connection())
        .find_by_inode(InodeId(11))
        .expect("a read")
        .expect("the file's row");
    assert!(record.append_only);
}

/// Checks that the warning about a hidden `.loonfs` entry at the root
/// appears when a commit creates the entry, and goes away when a commit
/// moves the entry out of the mount.
#[test]
fn the_reserved_name_warning_follows_the_hidden_entry() {
    let directory = tempfile::tempdir().expect("a state directory");
    let (mut task, shared, workers) = task(directory.path(), &[]);
    task.start();
    workers.take();
    let warnings = |shared: &SharedState| {
        shared
            .status
            .lock()
            .expect("the status lock")
            .warnings
            .clone()
    };
    let name = DisplayName::parse(".loonfs").expect("a name");
    let created = FilesystemChange::FileCreated {
        inode_id: InodeId(40),
        parent_inode_id: InodeId(10),
        display_name: name.clone(),
        binding_version: BindingVersion::parse("b40").expect("a version"),
        revision_no: RevisionNo(1),
        content_ref: ContentRef::blob_v1(
            namespace_id(),
            ContentId::generate(),
            b"x",
            ChecksumAlgorithm::Crc64nvme,
        ),
    };
    task.handle(page(5, vec![commit(6, vec![created])]));
    assert_eq!(
        warnings(&shared),
        [Warning::ReservedName {
            inode_id: InodeId(40)
        }]
    );

    task.handle(ControlMessage::Tick);
    workers.take();
    let moved_out = FilesystemChange::Moved {
        inode_id: InodeId(40),
        source_parent_inode_id: InodeId(10),
        source_display_name: name,
        destination_parent_inode_id: InodeId(2),
        destination_display_name: DisplayName::parse("kept").expect("a name"),
        binding_version: BindingVersion::parse("moved").expect("a version"),
    };
    task.handle(page(6, vec![commit(7, vec![moved_out])]));
    assert_eq!(task.applied_seq, ChangeSeq(7));
    assert_eq!(warnings(&shared), []);
}
