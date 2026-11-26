//! The retry of a floor object put whose number is taken: what it reads, and
//! when it loads its next anchor by discovery.

#![allow(
    clippy::disallowed_methods,
    reason = "these tests read and write WAL objects at their keys to stage a collision"
)]

use crate::commit::engine::{CommitCandidate, NamespaceCommitEngine};
use crate::namespace::state::NamespaceReadState;
use crate::wal::{prepare_wal_object, publish_wal_object};
use futures::future::BoxFuture;
use loonfs_objectstore::keys::{metadata_manifest_prefix, wal_object, wal_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::stores::{
    request_lines, BeforeStore, KeyPredicate, OperationContext, OperationKind, RecordedOperation,
    RecordingStore,
};
use loonfs_types::format::wal::{decode_wal_object_envelope_zstd, WalObjectBody, WalObjectPayload};
use loonfs_types::{CommitSeq, NamespaceId, WalNo, WriterEpoch};
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::Mutex;

/// The floor that every advance in these tests moves to.
const TARGET: CommitSeq = CommitSeq(1);

fn context() -> crate::test_support::WriterAt {
    crate::test_support::writer_at("writer", 1_000)
}

fn namespace_id() -> NamespaceId {
    NamespaceId::parse("advance").expect("namespace")
}

/// A namespace whose writer session committed once, with the session that
/// stages each collision in another process.
struct Staged {
    directory: TempDir,
    writer: Arc<Mutex<NamespaceCommitEngine<LocalFsStore>>>,
    /// The WAL tip after the commit, which the advance's discovery finds.
    tip: WalNo,
}

impl Staged {
    async fn new() -> Self {
        let directory = tempfile::tempdir().expect("directory");
        let store = LocalFsStore::new(directory.path()).expect("store");
        crate::test_support::NamespaceFixture::new(&store, &namespace_id(), &context())
            .create()
            .await
            .expect("create");
        let mut writer = NamespaceCommitEngine::new(
            crate::test_support::namespace_store(store, &namespace_id()),
            context().writer_id,
        );
        commit(&mut writer, "before").await;
        let tip = writer
            .writer_position()
            .expect("a position")
            .read_basis()
            .head
            .wal_no;
        Self {
            directory,
            writer: Arc::new(Mutex::new(writer)),
            tip,
        }
    }

    /// Returns another store over the namespace's directory, for the other
    /// process's requests, which the advance's store doesn't record.
    fn rival(&self) -> LocalFsStore {
        LocalFsStore::new(self.directory.path()).expect("store")
    }

    /// Returns the advance's store, which records every request and runs
    /// `action` before each operation that `selected` selects.
    fn advance_store(
        &self,
        selected: impl for<'a> Fn(&OperationContext<'a>) -> bool + Send + Sync + 'static,
        action: impl Fn(usize) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> RecordingStore<BeforeStore<LocalFsStore>> {
        RecordingStore::new(
            BeforeStore::before(self.rival(), selected, action),
            KeyPredicate::any(),
        )
    }

    /// Advances the floor to [`TARGET`] through `store`, and returns the
    /// floor that the advance reports and the requests after its planning.
    async fn advance(
        &self,
        store: &RecordingStore<BeforeStore<LocalFsStore>>,
    ) -> (CommitSeq, Vec<RecordedOperation>) {
        let advancer = crate::test_support::namespace_store(store, &namespace_id());
        let planned = advancer.load_current_manifest().await.expect("planned");
        store.reset();
        let report = advancer
            .publish_floor_object(&planned, TARGET)
            .await
            .expect("the advance");
        (report.retention_floor_seq, store.take())
    }

    /// Returns the payload of the WAL object at `wal_no`.
    async fn wal_payload(&self, wal_no: WalNo) -> WalObjectPayload {
        let bytes = self
            .rival()
            .get(&wal_object(&namespace_id(), &wal_no), None)
            .await
            .expect("get")
            .expect("a WAL object");
        decode_wal_object_envelope_zstd(&bytes)
            .expect("decode")
            .payload()
            .clone()
    }

    /// Returns the head that a discovery from the newest manifest finds.
    async fn head(&self) -> NamespaceReadState {
        crate::test_support::namespace_store(self.rival(), &namespace_id())
            .load_read_state()
            .await
            .expect("head")
    }
}

async fn commit(writer: &mut NamespaceCommitEngine<LocalFsStore>, name: &str) {
    writer
        .publish_batch(
            [CommitCandidate::new(
                crate::test_support::create_directory_request(name, name),
            )],
            &context().clock(),
        )
        .await
        .results
        .remove(0)
        .expect("the writer commits");
}

/// Selects the advance's WAL puts.
fn wal_puts(context: &OperationContext<'_>) -> bool {
    context.key().starts_with(&wal_prefix(&namespace_id()))
        && matches!(
            context.kind(),
            OperationKind::Put {
                mode: PutMode::CreateIfAbsent,
                ..
            }
        )
}

/// Returns an action that runs `staged` before the first selected operation
/// and does nothing before the others.
fn before_first(
    staged: impl Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static,
) -> impl Fn(usize) -> BoxFuture<'static, ()> + Send + Sync + 'static {
    move |index| {
        if index == 0 {
            staged()
        } else {
            Box::pin(async {})
        }
    }
}

/// Returns the lines of `operations` (see [`request_lines`]) with each run
/// of WAL GETs sorted, because a window's GETs finish in any order.
fn lines(operations: &[RecordedOperation]) -> Vec<String> {
    let mut lines = request_lines(operations);
    let get = |line: &String| line.starts_with("get advance WalObject");
    for run in lines.chunk_by_mut(|left, right| get(left) && get(right)) {
        run.sort();
    }
    lines
}

fn wal(kind: &str, wal_no: u64) -> String {
    format!("{kind} advance WalObject {wal_no:020}")
}

/// Returns the index of the first line of `lines` that is `line`, after
/// `from`.
fn position_after(lines: &[String], from: usize, line: &str) -> usize {
    from + lines[from..]
        .iter()
        .position(|candidate| candidate == line)
        .unwrap_or_else(|| panic!("`{line}` after line {from}: {lines:#?}"))
}

fn manifest_listings(lines: &[String]) -> usize {
    let listing = format!("list {}", metadata_manifest_prefix(&namespace_id()));
    lines.iter().filter(|line| **line == listing).count()
}

/// Writes a floor object for `retention_floor_seq` at the number after
/// `head`, a head that the namespace had before a collection emptied that
/// number, as a process that paused for a day after it read `head` does.
async fn put_stale_floor_object(
    store: &LocalFsStore,
    head: &NamespaceReadState,
    retention_floor_seq: CommitSeq,
) {
    let floor_object = prepare_wal_object(
        head,
        head.writer_epoch,
        WalObjectBody::Floor {
            retention_floor_seq,
        },
    )
    .expect("prepare");
    publish_wal_object(store, &floor_object)
        .await
        .expect("a put at the emptied number");
}

/// Folds the namespace through the commit at `wal_no` and deletes that
/// commit's WAL object, as a collection does once the object is a day old.
async fn fold_and_collect(store: &LocalFsStore, wal_no: WalNo) {
    crate::test_support::namespace_store(store, &namespace_id())
        .fold_wal()
        .await
        .expect("fold");
    store
        .delete(&wal_object(&namespace_id(), &wal_no))
        .await
        .expect("collect the folded commit");
}

/// Checks that an advance whose put collides with a writer's commit three
/// times reads each WAL object once and lists the manifests only in its
/// first discovery. After each collision, the advance reads from the taken
/// number on and puts at the first missing number, with no check before the
/// put, and the fourth put is the floor object.
#[tokio::test]
async fn an_advance_whose_put_collides_reads_each_wal_object_once() {
    const COLLISIONS: usize = 3;
    let staged = Staged::new().await;
    let writer = Arc::clone(&staged.writer);
    let store = staged.advance_store(wal_puts, move |index| {
        let writer = Arc::clone(&writer);
        Box::pin(async move {
            if index < COLLISIONS {
                commit(&mut *writer.lock().await, &format!("during-{index}")).await;
            }
        })
    });
    let (floor, operations) = staged.advance(&store).await;
    assert_eq!(floor, TARGET);
    let floor_object_no = staged.tip.0 + 1 + COLLISIONS as u64;
    assert_eq!(
        staged.wal_payload(WalNo(floor_object_no)).await.body,
        WalObjectBody::Floor {
            retention_floor_seq: TARGET
        }
    );
    let lines = lines(&operations);
    assert_eq!(manifest_listings(&lines), 1, "{lines:#?}");
    let puts = lines
        .iter()
        .filter(|line| line.starts_with("put advance WalObject"))
        .count();
    assert_eq!(puts, COLLISIONS + 1, "{lines:#?}");
    let mut read = BTreeSet::new();
    for operation in &operations {
        if let RecordedOperation::Get {
            key, result_bytes, ..
        } = operation
        {
            if key.starts_with(&wal_prefix(&namespace_id())) && *result_bytes > 0 {
                assert!(read.insert(key.clone()), "{key} read twice: {lines:#?}");
            }
        }
    }
    for collided in 1..=COLLISIONS as u64 {
        let key = wal_object(&namespace_id(), &WalNo(staged.tip.0 + collided));
        assert!(read.contains(&key), "{key} not read: {lines:#?}");
    }
}

/// Checks that an advance whose put collides with a new writer's fence loads
/// its next anchor by discovery, which finds the claim manifest, so the floor
/// object copies the new epoch. The read of the taken number stops at the
/// fence, whose epoch is higher than the anchor's manifest's.
#[tokio::test]
async fn an_advance_that_collides_with_a_new_writers_fence_copies_the_new_epoch() {
    let staged = Staged::new().await;
    let rival = staged.rival();
    let rival = Arc::new(rival);
    let store = staged.advance_store(
        wal_puts,
        before_first(move || {
            let rival = Arc::clone(&rival);
            Box::pin(async move {
                crate::test_support::namespace_store(&*rival, &namespace_id())
                    .acquire_writer_epoch(
                        &crate::test_support::writer_at("writer-b", 1_000).writer_id,
                    )
                    .await
                    .expect("writer b takes over");
            })
        }),
    );
    let (floor, operations) = staged.advance(&store).await;
    assert_eq!(floor, TARGET);
    let fence = staged.tip.0 + 1;
    assert_eq!(
        staged.wal_payload(WalNo(fence)).await.body,
        WalObjectBody::Fence
    );
    let floor_object = staged.wal_payload(WalNo(fence + 1)).await;
    assert_eq!(floor_object.writer_epoch, WriterEpoch(2));
    let lines = lines(&operations);
    let collided = position_after(&lines, 0, &wal("put", fence));
    let read = position_after(&lines, collided, &wal("get", fence));
    assert_eq!(lines[read + 1], "get advance Boundary", "{lines:#?}");
    assert_eq!(manifest_listings(&lines[read..]), 1, "{lines:#?}");
}

/// Checks that an advance whose put collides with an object that doesn't
/// continue the anchor's tip loads its next anchor by discovery. A writer's
/// commit at the number was folded and collected, and a process wrote an
/// object there that records another head. Discovery starts from the fold's
/// manifest, so the floor object follows the folded commit.
#[tokio::test]
async fn an_advance_that_collides_with_a_broken_head_loads_its_anchor_by_discovery() {
    let staged = Staged::new().await;
    let taken = WalNo(staged.tip.0 + 1);
    let writer = Arc::clone(&staged.writer);
    let rival = Arc::new(staged.rival());
    let store = staged.advance_store(
        wal_puts,
        before_first(move || {
            let (writer, rival) = (Arc::clone(&writer), Arc::clone(&rival));
            Box::pin(async move {
                let mut head = crate::test_support::namespace_store(&*rival, &namespace_id())
                    .load_read_state()
                    .await
                    .expect("head");
                commit(&mut *writer.lock().await, "folded").await;
                fold_and_collect(&rival, taken).await;
                head.seq = CommitSeq(head.seq.0 + 5);
                put_stale_floor_object(&rival, &head, CommitSeq(0)).await;
            })
        }),
    );
    let (floor, operations) = staged.advance(&store).await;
    assert_eq!(floor, TARGET);
    let head = staged.head().await;
    assert_eq!(head.wal_no, WalNo(taken.0 + 1));
    assert_eq!(head.retention_floor_seq, TARGET);
    let lines = lines(&operations);
    let collided = position_after(&lines, 0, &wal("put", taken.0));
    let read = position_after(&lines, collided, &wal("get", taken.0));
    assert_eq!(lines[read + 1], "get advance Boundary", "{lines:#?}");
    assert_eq!(manifest_listings(&lines[read..]), 1, "{lines:#?}");
}

/// Checks that an advance whose read of the taken number finds it empty
/// loads its next anchor by discovery. A writer's commit took the number,
/// and a fold and a collection emptied it before the advance's read.
#[tokio::test]
async fn an_advance_that_finds_the_taken_number_emptied_loads_its_anchor_by_discovery() {
    let staged = Staged::new().await;
    let taken = WalNo(staged.tip.0 + 1);
    let key = wal_object(&namespace_id(), &taken);
    let writer = Arc::clone(&staged.writer);
    let rival = Arc::new(staged.rival());
    // The selected operations on the taken number are the first discovery's
    // GET, the put, and the read after the collision.
    let store = staged.advance_store(
        move |context| context.key() == key,
        move |index| {
            let (writer, rival) = (Arc::clone(&writer), Arc::clone(&rival));
            Box::pin(async move {
                match index {
                    1 => commit(&mut *writer.lock().await, "taken").await,
                    2 => fold_and_collect(&rival, taken).await,
                    _ => {}
                }
            })
        },
    );
    let (floor, operations) = staged.advance(&store).await;
    assert_eq!(floor, TARGET);
    let head = staged.head().await;
    assert_eq!(head.wal_no, WalNo(taken.0 + 1));
    assert_eq!(head.retention_floor_seq, TARGET);
    let lines = lines(&operations);
    let collided = position_after(&lines, 0, &wal("put", taken.0));
    let read = position_after(&lines, collided, &wal("get", taken.0));
    assert_eq!(lines[read + 1], "get advance Boundary", "{lines:#?}");
    assert_eq!(manifest_listings(&lines[read..]), 1, "{lines:#?}");
}

/// Checks that an advance whose put collides with another advance's floor
/// object at the target returns that floor after a check of its manifest,
/// with no second put and no discovery.
#[tokio::test]
async fn an_advance_that_collides_with_a_live_floor_object_at_the_target_checks_and_returns() {
    let staged = Staged::new().await;
    let taken = staged.tip.0 + 1;
    let rival = Arc::new(staged.rival());
    let store = staged.advance_store(
        wal_puts,
        before_first(move || {
            let rival = Arc::clone(&rival);
            Box::pin(async move {
                let namespace = crate::test_support::namespace_store(&*rival, &namespace_id());
                let planned = namespace.load_current_manifest().await.expect("planned");
                namespace
                    .publish_floor_object(&planned, TARGET)
                    .await
                    .expect("another advance");
            })
        }),
    );
    let (floor, operations) = staged.advance(&store).await;
    assert_eq!(floor, TARGET);
    let lines = lines(&operations);
    let collided = position_after(&lines, 0, &wal("put", taken));
    let head = staged.head().await;
    assert_eq!(
        lines[collided + 1..],
        [
            wal("get", taken),
            wal("get", taken + 1),
            wal("get", taken + 2),
            format!(
                "head advance MetadataManifest {:020}",
                manifest_no_after(&lines)
            ),
            "get advance Boundary".to_owned(),
        ],
        "{lines:#?}"
    );
    assert_eq!(head.wal_no, WalNo(taken));
}

/// Returns the number after the manifest that the advance's discovery
/// loaded, from the HEAD of its successor that the discovery's check sent.
fn manifest_no_after(lines: &[String]) -> u64 {
    lines
        .iter()
        .find_map(|line| line.strip_prefix("head advance MetadataManifest "))
        .expect("the discovery's check")
        .parse()
        .expect("a manifest number")
}

/// Checks that an advance whose put collides with a dead floor object checks
/// its manifest before it uses that floor object, and loads its next anchor
/// by discovery when the check finds a newer manifest. A writer's commit at
/// the number was folded and collected, and a paused advance then wrote a
/// floor object there that no reader replays. Whether the dead floor object
/// is below the target or at it, the advance puts its floor object after the
/// folded commit, where a reader of the newest manifest replays it.
#[tokio::test]
async fn an_advance_that_collides_with_a_dead_floor_object_checks_before_it_uses_it() {
    for dead_floor in [CommitSeq(0), TARGET] {
        let staged = Staged::new().await;
        let taken = WalNo(staged.tip.0 + 1);
        let writer = Arc::clone(&staged.writer);
        let rival = Arc::new(staged.rival());
        let store = staged.advance_store(
            wal_puts,
            before_first(move || {
                let (writer, rival) = (Arc::clone(&writer), Arc::clone(&rival));
                Box::pin(async move {
                    let head = crate::test_support::namespace_store(&*rival, &namespace_id())
                        .load_read_state()
                        .await
                        .expect("head");
                    commit(&mut *writer.lock().await, "folded").await;
                    fold_and_collect(&rival, taken).await;
                    put_stale_floor_object(&rival, &head, dead_floor).await;
                })
            }),
        );
        let (floor, operations) = staged.advance(&store).await;
        assert_eq!(floor, TARGET, "dead floor {dead_floor:?}");
        let head = staged.head().await;
        assert_eq!(head.wal_no, WalNo(taken.0 + 1), "dead floor {dead_floor:?}");
        assert_eq!(
            head.retention_floor_seq, TARGET,
            "dead floor {dead_floor:?}"
        );
        let floor_object = staged.wal_payload(WalNo(taken.0 + 1)).await;
        assert_eq!(floor_object.head_seq, head.seq, "dead floor {dead_floor:?}");
        let lines = lines(&operations);
        let collided = position_after(&lines, 0, &wal("put", taken.0));
        let read = position_after(&lines, collided, &wal("get", taken.0));
        let check = position_after(&lines, read, "get advance Boundary");
        assert!(
            lines[read..check]
                .iter()
                .any(|line| line.starts_with("head advance MetadataManifest")),
            "dead floor {dead_floor:?}: {lines:#?}"
        );
        assert_eq!(
            manifest_listings(&lines[read..]),
            1,
            "dead floor {dead_floor:?}: {lines:#?}"
        );
    }
}

/// Checks that an advance whose put after a collision succeeds, and whose
/// check after the put finds a newer manifest, loads the next anchor by
/// discovery and returns it when the floor object is above the newer
/// manifest's `folded_wal_no`.
#[tokio::test]
async fn an_advance_whose_put_after_a_collision_finds_a_newer_manifest_loads_it() {
    let staged = Staged::new().await;
    let taken = staged.tip.0 + 1;
    let writer = Arc::clone(&staged.writer);
    let rival = Arc::new(staged.rival());
    let store = staged.advance_store(wal_puts, move |index| {
        let (writer, rival) = (Arc::clone(&writer), Arc::clone(&rival));
        Box::pin(async move {
            match index {
                0 => commit(&mut *writer.lock().await, "taken").await,
                1 => {
                    crate::test_support::namespace_store(&*rival, &namespace_id())
                        .fold_wal()
                        .await
                        .expect("fold the commit");
                }
                _ => {}
            }
        })
    });
    let (floor, operations) = staged.advance(&store).await;
    assert_eq!(floor, TARGET);
    let head = staged.head().await;
    assert_eq!(head.wal_no, WalNo(taken + 1));
    assert_eq!(head.retention_floor_seq, TARGET);
    let lines = lines(&operations);
    let collided = position_after(&lines, 0, &wal("put", taken));
    let put = position_after(&lines, collided, &wal("put", taken + 1));
    assert_eq!(manifest_listings(&lines[..put]), 1, "{lines:#?}");
    assert_eq!(manifest_listings(&lines[put..]), 1, "{lines:#?}");
    assert!(
        !lines[put + 1..].iter().any(|line| line.starts_with("put")),
        "{lines:#?}"
    );
}
