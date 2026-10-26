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
use loonfs_test_support::clock::ManualClock;
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

/// Returns a control task as [`task`] does, after `configure` changes the
/// mount's row: its settings and the transfers that the deployment
/// advertises.
fn task_with(
    directory: &Path,
    listing: &[PathEntry],
    configure: impl FnOnce(&mut MountRow),
) -> (ControlTask, SharedState, RecordedWork) {
    let root = entry(10, 1, "/project", PathEntryKind::Directory {});
    let mut mount = MountRow {
        config: MountConfig::new(
            namespace_id(),
            AbsolutePath::parse("/project").expect("a path"),
            MountAccess::ReadOnly,
            directory.to_path_buf(),
        ),
        applied_seq: ChangeSeq(5),
        root_inode_id: InodeId(10),
        root_path: root.path.clone(),
        inline_limit_bytes: 0,
        transports: Transports::default(),
        publication_stop: None,
    };
    configure(&mut mount);
    std::fs::create_dir_all(mount.config.cache_directory()).expect("a cache directory");
    let mut database = Database::create(&mount.config.database_path(), &mount).expect("a database");
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

mod feed;
mod reads;
