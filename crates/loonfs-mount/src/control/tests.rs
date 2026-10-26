//! The control task as a deterministic state machine: a recorder replaces
//! its workers, and each test hands it the outcomes that workers would send.

use super::{ControlMessage, ControlTask, Environment, SharedState};
use crate::cache_file::CacheFile;
use crate::config::{MountAccess, MountConfig};
use crate::control::write_listing;
use crate::database::{Database, MountRow};
use crate::error::MountError;
use crate::fetch::{FetchJob, FetchTransport};
use crate::host::DiskSpace;
use crate::item::ItemId;
use crate::item_table::write_item;
use crate::read_handle::OpenedRead;
use crate::records::{ItemRecord, RemoteState, Visibility};
use crate::status::{ReflinkSupport, Transports};
use crate::verify::Verification;
use crate::workers::{EnteringItem, Fetched, Listing, SnapshotRef, Work, WorkOutcome, Workers};
use loonfs_types::api::v0::{
    Commit, FilesystemChange, ListChangesResponse, PathEntry, PathEntryKind,
};
use loonfs_types::{
    AbsolutePath, ActorId, BindingVersion, ChangeSeq, ChecksumAlgorithm, CommitId, ContentId,
    ContentRef, DisplayName, InodeId, NamespaceId, PinId, RevisionNo, StdMonotonicTimer,
};
use std::ffi::OsStr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

#[derive(Debug)]
struct EveryByteFree;

impl DiskSpace for EveryByteFree {
    fn available_bytes(&self, _path: &Path) -> std::io::Result<u64> {
        Ok(u64::MAX)
    }
}

/// Workers that record the work they're given and run none of it.
#[derive(Clone, Default)]
struct RecordedWork(Arc<Mutex<Vec<Work>>>);

impl Workers for RecordedWork {
    fn start(&mut self, work: Work) {
        self.0.lock().expect("the recorder lock").push(work);
    }
}

impl RecordedWork {
    fn take(&self) -> Vec<Work> {
        std::mem::take(&mut *self.0.lock().expect("the recorder lock"))
    }
}

fn namespace_id() -> NamespaceId {
    NamespaceId::parse("demo").expect("a namespace ID")
}

fn entry(inode: u64, parent: u64, path: &str, kind: PathEntryKind) -> PathEntry {
    let path = AbsolutePath::parse(path).expect("a path");
    PathEntry {
        namespace_id: namespace_id(),
        display_name: path.final_component().map(|name| name.to_display_name()),
        path,
        inode_id: InodeId(inode),
        created_by: ActorId::parse("tester").expect("an actor"),
        created_at_ms: 1_000,
        kind,
        head_seq: ChangeSeq(5),
        parent_inode_id: Some(InodeId(parent)),
        binding_version: Some(BindingVersion::parse(format!("b{inode}")).expect("a version")),
        attributes: None,
    }
}

fn file(inode: u64, parent: u64, path: &str) -> PathEntry {
    let content_ref = ContentRef::blob_v1(
        namespace_id(),
        ContentId::generate(),
        path.as_bytes(),
        ChecksumAlgorithm::Crc64nvme,
    );
    entry(
        inode,
        parent,
        path,
        PathEntryKind::File {
            revision_no: RevisionNo(1),
            size_bytes: content_ref.size_bytes,
            content_ref,
            revision_committed_by: ActorId::parse("tester").expect("an actor"),
            revision_committed_at_ms: 2_000,
        },
    )
}

/// Returns a control task over a mount of `/project` (inode 10) at sequence
/// 5, with `listing` as its tree.
fn task(directory: &Path, listing: &[PathEntry]) -> (ControlTask, SharedState, RecordedWork) {
    task_with(directory, listing, |_| {})
}

fn task_with(
    directory: &Path,
    listing: &[PathEntry],
    configure: impl FnOnce(&mut MountConfig),
) -> (ControlTask, SharedState, RecordedWork) {
    let mut config = MountConfig::new(
        namespace_id(),
        AbsolutePath::parse("/project").expect("a path"),
        MountAccess::ReadOnly,
        directory.to_path_buf(),
    );
    configure(&mut config);
    std::fs::create_dir_all(config.cache_directory()).expect("a cache directory");
    let root = entry(10, 1, "/project", PathEntryKind::Directory {});
    let mut database = Database::create(
        &config.database_path(),
        &MountRow {
            config: config.clone(),
            applied_seq: ChangeSeq(5),
            root_inode_id: InodeId(10),
            root_path: root.path.clone(),
            inline_limit_bytes: 0,
            transports: Transports::default(),
            publication_stop: None,
        },
    )
    .expect("a database");
    let transaction = database.transaction().expect("a transaction");
    let (kind, state) = RemoteState::from_entry(&root, None).expect("a root state");
    let root_record = ItemRecord::clean(
        ItemId::ROOT,
        Some(InodeId(10)),
        kind,
        state,
        Visibility::Shown,
    );
    write_item(&transaction, &root_record).expect("the root row");
    write_listing(&transaction, listing).expect("the listing");
    transaction.commit().expect("the commit");
    database.mark_admitted().expect("admitted");
    let workers = RecordedWork::default();
    let environment = Environment {
        timer: Arc::new(StdMonotonicTimer::default()),
        disk: Arc::new(EveryByteFree),
        reflinks: ReflinkSupport::Unsupported,
    };
    let (task, shared) =
        ControlTask::load(database, Box::new(workers.clone()), environment).expect("a task");
    (task, shared, workers)
}

fn page(after_seq: u64, changes: Vec<Commit>) -> ControlMessage {
    let head_seq = changes
        .last()
        .map_or(after_seq, |commit| commit.committed_seq.0);
    ControlMessage::WorkDone(Box::new(WorkOutcome::FeedPage(Ok(ListChangesResponse {
        namespace_id: namespace_id(),
        after_seq: ChangeSeq(after_seq),
        head_seq: ChangeSeq(head_seq),
        next_after_seq: None,
        changes,
    }))))
}

fn commit(seq: u64, events: Vec<FilesystemChange>) -> Commit {
    Commit {
        namespace_id: namespace_id(),
        commit_id: CommitId::parse(format!("commit-{seq}")).expect("a commit ID"),
        committed_seq: ChangeSeq(seq),
        committed_by: ActorId::parse("other").expect("an actor"),
        committed_at_ms: 3_000,
        message: None,
        events,
    }
}

fn shown(shared: &SharedState, name: &str) -> bool {
    shared
        .tree
        .read()
        .expect("the tree lock")
        .lookup(ItemId::ROOT, OsStr::new(name))
        .is_some()
}

fn done(outcome: WorkOutcome) -> ControlMessage {
    ControlMessage::WorkDone(Box::new(outcome))
}

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

    let moved_in = FilesystemChange::Moved {
        inode_id: InodeId(30),
        source_parent_inode_id: InodeId(2),
        source_display_name: DisplayName::parse("incoming").expect("a name"),
        destination_parent_inode_id: InodeId(10),
        destination_display_name: DisplayName::parse("incoming").expect("a name"),
        binding_version: BindingVersion::parse("moved").expect("a version"),
    };
    task.handle(page(5, vec![commit(6, vec![moved_in])]));
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
    written.blocking_recv().expect("the page is written");
    task.handle(done(WorkOutcome::EnteringItemRead {
        inode_id: InodeId(30),
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

fn only_fetch(work: Vec<Work>) -> FetchJob {
    let [Work::Fetch(job)] = &work[..] else {
        panic!("one fetch should start: {work:?}");
    };
    job.clone()
}

/// Sends the outcome of the one verification that `work` starts.
fn verify(task: &mut ControlTask, work: Vec<Work>, result: Verification) {
    let [Work::Verify(job)] = &work[..] else {
        panic!("one verification should start: {work:?}");
    };
    task.handle(done(WorkOutcome::Verified {
        content_id: job.content_id.clone(),
        file: job.file.clone(),
        target: job.target.clone(),
        result: Ok(result),
    }));
}

/// A read whose reply hasn't been received yet.
type PendingRead = oneshot::Receiver<crate::error::Result<Arc<CacheFile>>>;

/// Opens `a.txt` and reads all of it, which returns its bytes before they're
/// verified, and then fails their verification. Returns the handle, a read
/// sent after the mismatch, and the fetch that starts again.
fn read_and_mismatch() -> (
    tempfile::TempDir,
    ControlTask,
    OpenedRead,
    PendingRead,
    RecordedWork,
) {
    let directory = tempfile::tempdir().expect("a state directory");
    // Every fetch is a ranged read, which the server doesn't verify.
    let (mut task, shared, workers) = task_with(
        directory.path(),
        &[file(11, 10, "/project/a.txt")],
        |config| {
            config.max_whole_file_bytes = 1;
        },
    );
    task.start();
    workers.take();
    let item = shared
        .tree
        .read()
        .expect("the tree lock")
        .lookup(ItemId::ROOT, OsStr::new("a.txt"))
        .expect("a.txt is shown")
        .item;
    let (reply, opened) = oneshot::channel();
    task.handle(ControlMessage::OpenRead { item, reply });
    let opened = opened.blocking_recv().expect("a reply").expect("a handle");
    let size = opened
        .view
        .lock()
        .expect("the view lock")
        .revision
        .content_ref
        .size_bytes;
    let read = |task: &mut ControlTask| {
        let (reply, read) = oneshot::channel();
        task.handle(ControlMessage::Read {
            handle: opened.handle,
            demand: 0..size,
            needed: 0..size,
            reply,
        });
        read
    };

    let first = read(&mut task);
    let fetch = only_fetch(workers.take());
    assert_eq!(fetch.transport, FetchTransport::Ranged);
    task.handle(done(WorkOutcome::Fetched {
        fetch_id: fetch.fetch_id,
        result: Ok(Fetched {
            written: 0..size,
            server_verified: false,
        }),
    }));
    let first = first.blocking_recv().expect("a reply");
    assert!(
        first.is_ok(),
        "a read returns bytes before they're verified"
    );
    verify(&mut task, workers.take(), Verification::Mismatch);
    let refetch = only_fetch(workers.take());
    assert_eq!(refetch.range, 0..size);

    let mut after_mismatch = read(&mut task);
    assert!(
        workers.take().is_empty(),
        "the refetch fetches every byte already"
    );
    task.handle(done(WorkOutcome::Fetched {
        fetch_id: refetch.fetch_id,
        result: Ok(Fetched {
            written: 0..size,
            server_verified: false,
        }),
    }));
    assert!(
        matches!(
            after_mismatch.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ),
        "a read after a mismatch waits until the second fetch matches"
    );
    let file = opened.view.lock().expect("the view lock").file.clone();
    assert_eq!(
        file.map(|file| file.present_prefix()),
        Some(0),
        "a read handle has none of the second fetch's bytes"
    );
    (directory, task, opened, after_mismatch, workers)
}

/// Checks that a read that arrives after a mismatch waits for the second
/// fetch to match, and then returns its bytes.
#[test]
fn a_read_after_a_mismatch_waits_for_the_second_fetch() {
    let (_directory, mut task, opened, read, workers) = read_and_mismatch();
    verify(&mut task, workers.take(), Verification::Matches);
    let file = read.blocking_recv().expect("a reply").expect("the bytes");
    let size = opened
        .view
        .lock()
        .expect("the view lock")
        .revision
        .content_ref
        .size_bytes;
    assert_eq!(file.present_prefix(), size);
    assert_eq!(task.mismatched_files, 1);
}

/// Checks that a read that arrives after a mismatch fails with a content
/// mismatch, `EIO`, when the second fetch doesn't match either.
#[test]
fn a_read_after_a_mismatch_fails_when_the_second_fetch_mismatches() {
    let (_directory, mut task, _opened, read, workers) = read_and_mismatch();
    verify(&mut task, workers.take(), Verification::Mismatch);
    let result = read.blocking_recv().expect("a reply");
    assert!(
        matches!(result, Err(MountError::ContentMismatch { .. })),
        "{result:?}"
    );
    assert_eq!(task.mismatched_files, 2);
    assert!(
        workers.take().is_empty(),
        "a file is fetched again only once"
    );
}
