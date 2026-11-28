//! Discoveries that find a newer manifest after their WAL read.

use crate::commit::engine::NamespaceCommitEngine;
use crate::error::CoreError;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::test_support::{namespace_store, observed_now, writer_at, NamespaceFixture, WriterAt};
use crate::wal::tests::publish;
use async_trait::async_trait;
use bytes::Bytes;
use loonfs_objectstore::keys::{metadata_manifest_object, metadata_manifest_prefix, wal_prefix};
use loonfs_objectstore::layout::{manifest_no_of, wal_no_of};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore, ObjectStoreExtension};
use loonfs_test_support::stores::{
    publish_next_manifest, BlockingStore, Intercept, InterceptStore, Interceptor, KeyPredicate,
    MetadataMapStore, OperationContext, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::format::manifest::NamespaceManifestPayload;
use loonfs_types::{CommitSeq, ErrorCode, ManifestNo, NamespaceId, WalNo, WriterEpoch};
use std::assert_matches;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Mutex;
use tempfile::TempDir;

/// A namespace whose WAL tail has a fence at WAL 1 and `commits` commits
/// after it, written by a writer session that the fixture keeps.
struct Fixture {
    directory: TempDir,
    namespace_id: NamespaceId,
    writer: NamespaceCommitEngine<LocalFsStore>,
}

async fn fixture(name: &str, commits: usize) -> Fixture {
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse(name).expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let context = writer_at("writer", 1_000);
    NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("create");
    let mut writer =
        NamespaceCommitEngine::new(namespace_store(store, &namespace_id), context.writer_id);
    for index in 0..commits {
        publish(&mut writer, &format!("commit-{index}"))
            .await
            .expect("commit");
    }
    Fixture {
        directory,
        namespace_id,
        writer,
    }
}

/// Returns the fixture of [`fixture`] with four commits, and a fold of the
/// tail at WAL 3 (the fence and the first two commits) that the writer
/// session built from its position before the last two commits and hasn't
/// put yet.
///
/// The test builds the fold in the store, then takes its manifest out, so
/// that the manifest arrives later as a put that the process started before
/// the last two commits.
async fn fixture_with_an_earlier_fold(name: &str) -> (Fixture, Rival) {
    let mut fixture = fixture(name, 2).await;
    let namespace = namespace_store(fixture.store(), &fixture.namespace_id);
    let position = fixture
        .writer
        .writer_position()
        .expect("the writer's position");
    let folded = namespace
        .fold_wal_tail_in_new_lane(&position, &namespace.start_clock())
        .await
        .expect("fold WAL 1 to 3")
        .response;
    assert_eq!(folded.manifest_head_seq, CommitSeq(2));
    let key = metadata_manifest_object(&fixture.namespace_id, &folded.manifest_no);
    let store = fixture.store();
    let bytes = store
        .get(&key, None)
        .await
        .expect("get the fold's manifest")
        .expect("the fold's manifest");
    store
        .delete(&key)
        .await
        .expect("take the fold's manifest out");
    for index in 2..4 {
        publish(&mut fixture.writer, &format!("commit-{index}"))
            .await
            .expect("commit");
    }
    (fixture, Rival::Puts { key, bytes })
}

impl Fixture {
    /// Returns a store of the fixture's objects that no test wrapper
    /// intercepts.
    fn store(&self) -> LocalFsStore {
        LocalFsStore::new(self.directory.path()).expect("store")
    }

    /// Returns a store that records every request, over a store where
    /// another process takes one of `steps` before each check of a manifest,
    /// and the fixture's writer session commits for that process.
    fn rival_before_checks(self, steps: Vec<Rival>) -> (RecordingStore<RivalStore>, TempDir) {
        let store = RecordingStore::new(
            InterceptStore::with_interceptor(
                self.store(),
                RivalBeforeChecks {
                    rival: self.store(),
                    namespace_id: self.namespace_id.clone(),
                    writer: tokio::sync::Mutex::new(self.writer),
                    steps: Mutex::new(steps.into()),
                },
            ),
            KeyPredicate::any(),
        );
        (store, self.directory)
    }
}

/// An edit of a copy of the newest manifest.
type ManifestChange = fn(&mut NamespaceManifestPayload);

/// What another process does before one check of a manifest.
#[derive(Debug, Clone)]
enum Rival {
    /// Publishes a copy of the newest manifest with the next number, which
    /// the function then edits.
    Publishes(ManifestChange),
    /// Puts a manifest that it built earlier, a fold of part of the tail
    /// (see [`fixture_with_an_earlier_fold`]).
    Puts { key: String, bytes: Bytes },
    /// Folds the WAL tail in the fixture's writer session.
    Folds,
    /// Commits two more WAL objects in the fixture's writer session, and then
    /// folds the WAL tail in that session.
    CommitsAndFolds,
    /// Claims the namespace for another writer, which publishes a claim
    /// manifest and puts a fence.
    Claims,
    /// Claims the namespace for another writer and deletes it.
    Deletes,
}

type RivalStore = InterceptStore<LocalFsStore, RivalBeforeChecks>;

/// Takes the next step of another process before each successor HEAD of a
/// manifest, the first request of a check.
struct RivalBeforeChecks {
    rival: LocalFsStore,
    namespace_id: NamespaceId,
    /// Stays locked across the session's commits, which a step awaits.
    writer: tokio::sync::Mutex<NamespaceCommitEngine<LocalFsStore>>,
    steps: Mutex<VecDeque<Rival>>,
}

impl fmt::Debug for RivalBeforeChecks {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RivalBeforeChecks")
            .field("namespace_id", &self.namespace_id)
            .field("steps", &self.steps)
            .finish_non_exhaustive()
    }
}

impl RivalBeforeChecks {
    fn steps_left(&self) -> usize {
        self.steps.lock().expect("the steps lock").len()
    }

    async fn take(&self, step: Rival) {
        let (rival, namespace_id) = (&self.rival, &self.namespace_id);
        match step {
            Rival::Publishes(change) => {
                publish_next_manifest(rival, namespace_id, change).await;
            }
            Rival::Puts { key, bytes } => {
                rival
                    .put_if_absent(&key, bytes)
                    .await
                    .expect("the rival publishes its fold");
            }
            Rival::Folds => {
                self.writer
                    .lock()
                    .await
                    .fold()
                    .await
                    .expect("the writer folds");
            }
            Rival::CommitsAndFolds => {
                let mut writer = self.writer.lock().await;
                for name in ["raced-0", "raced-1"] {
                    publish(&mut writer, name)
                        .await
                        .expect("the writer commits");
                }
                writer.fold().await.expect("the writer folds");
            }
            Rival::Claims => {
                namespace_store(rival, namespace_id)
                    .acquire_writer_epoch(&writer_at("rival", 1_000).writer_id)
                    .await
                    .expect("the rival claims");
            }
            Rival::Deletes => {
                let namespace = namespace_store(rival, namespace_id);
                let acquired = namespace
                    .acquire_writer_epoch(&writer_at("rival", 1_000).writer_id)
                    .await
                    .expect("the rival claims");
                namespace
                    .delete_namespace(
                        Default::default(),
                        crate::commit::session_epoch::SessionEpoch::new(
                            &std::sync::Mutex::default(),
                            acquired,
                        ),
                        None,
                        &crate::publish::PublicationLane::new().lock().await,
                        &namespace.start_clock(),
                    )
                    .await
                    .expect("the rival deletes the namespace");
            }
        }
    }
}

#[async_trait]
impl Interceptor for RivalBeforeChecks {
    type Guard = ();

    async fn before(&self, context: &OperationContext<'_>) -> (Intercept, ()) {
        if OperationSelector::Head.matches(context.kind())
            && manifest_no_of(context.key()).is_some()
        {
            let step = self.steps.lock().expect("the steps lock").pop_front();
            if let Some(step) = step {
                self.take(step).await;
            }
        }
        (Intercept::Continue, ())
    }
}

/// The requests that a discovery sent: the WAL numbers it read, sorted
/// because a window's GETs run concurrently, and how many listings and HEAD
/// requests of manifests it sent. A pass sends one listing to find the newest
/// manifest and one HEAD to check it.
#[derive(Debug, PartialEq, Eq)]
struct Sent {
    wal_gets: Vec<u64>,
    manifest_lists: usize,
    manifest_heads: usize,
}

fn sent(namespace_id: &NamespaceId, requests: &[RecordedOperation]) -> Sent {
    let manifests = metadata_manifest_prefix(namespace_id);
    let wal = wal_prefix(namespace_id);
    let mut wal_gets = requests
        .iter()
        .filter(|request| {
            matches!(request, RecordedOperation::Get { .. }) && request.key().starts_with(&wal)
        })
        .map(|request| wal_no_of(request.key()).expect("a WAL key").0)
        .collect::<Vec<_>>();
    wal_gets.sort_unstable();
    Sent {
        wal_gets,
        manifest_lists: requests
            .iter()
            .filter(|request| {
                matches!(request, RecordedOperation::List { .. })
                    && request.key().starts_with(&manifests)
            })
            .count(),
        manifest_heads: requests
            .iter()
            .filter(|request| {
                matches!(request, RecordedOperation::Head { .. })
                    && request.key().starts_with(&manifests)
            })
            .count(),
    }
}

/// Returns the WAL numbers of the objects in `anchor`'s tail.
fn tail_wal_numbers(anchor: &NamespaceReadAnchor) -> Vec<u64> {
    anchor
        .tail
        .objects()
        .iter()
        .map(|object| object.envelope().payload().wal_no.0)
        .collect()
}

/// Checks that a discovery whose check finds a newer manifest continues from
/// that manifest without reading the WAL again, keeping the objects above
/// its `folded_wal_no`, when the newer manifest folds part or all of what
/// the discovery read, when two newer manifests come one after the other,
/// and when the newer manifest is another writer's claim. A newer manifest
/// that folds past what the discovery read makes the discovery read on from
/// that manifest's `folded_wal_no`.
#[tokio::test]
async fn a_discovery_continues_from_a_newer_manifest_with_the_objects_it_read() {
    // The tail is the fence at WAL 1 and commits 1 to 4 at WAL 2 to 5, and the
    // first pass reads WAL 1 to 7.
    let first_pass = (1..=7).collect::<Vec<_>>();
    for (case, folded, tail, head_seq, epoch, wal_gets, passes) in [
        ("part", 3, vec![4, 5], 4, 1, first_pass.clone(), 2),
        ("all", 5, vec![], 4, 1, first_pass.clone(), 2),
        ("two", 3, vec![4, 5], 4, 1, first_pass.clone(), 3),
        // The rival commits at WAL 6 and 7 after the first pass found them
        // missing, and its fold folds them.
        ("past", 7, vec![], 6, 1, (1..=8).collect(), 2),
        // The claim's fence at WAL 6 was put after the first pass found WAL 6
        // missing, so the view ends at WAL 5.
        ("claim", 0, vec![1, 2, 3, 4, 5], 4, 2, first_pass.clone(), 2),
    ] {
        let name = format!("continues-{case}");
        let (fixture, steps) = match case {
            "part" => {
                let (fixture, fold) = fixture_with_an_earlier_fold(&name).await;
                (fixture, vec![fold])
            }
            "two" => {
                let (fixture, fold) = fixture_with_an_earlier_fold(&name).await;
                (fixture, vec![fold, Rival::Publishes(|_| {})])
            }
            "all" => (fixture(&name, 4).await, vec![Rival::Folds]),
            "past" => (fixture(&name, 4).await, vec![Rival::CommitsAndFolds]),
            _ => (fixture(&name, 4).await, vec![Rival::Claims]),
        };
        let namespace_id = fixture.namespace_id.clone();
        let (store, _directory) = fixture.rival_before_checks(steps);
        let anchor = namespace_store(&store, &namespace_id)
            .load_read_anchor(observed_now())
            .await
            .unwrap_or_else(|error| panic!("case {case}: {error}"));
        assert_eq!(store.inner().interceptor().steps_left(), 0, "case {case}");
        let manifest = anchor.current_manifest().payload();
        assert_eq!(manifest.folded_wal_no, WalNo(folded), "case {case}");
        assert_eq!(tail_wal_numbers(&anchor), tail, "case {case}");
        assert_eq!(anchor.read_state.seq, CommitSeq(head_seq), "case {case}");
        assert_eq!(
            anchor.read_state.writer_epoch,
            WriterEpoch(epoch),
            "case {case}"
        );
        assert_eq!(
            sent(&namespace_id, &store.take()),
            Sent {
                wal_gets,
                manifest_lists: passes,
                manifest_heads: passes,
            },
            "case {case}"
        );
    }
}

/// Checks that a writer session's discovery keeps the objects it read when
/// its check finds a newer manifest of the session's writer epoch, and
/// returns `writer_fenced` before it reads the WAL again when the newer
/// manifest is another writer's claim.
#[tokio::test]
async fn a_writer_session_discovery_keeps_its_objects_or_is_fenced_by_a_newer_manifest() {
    for (case, step) in [
        ("same-epoch", Rival::Publishes(|_| {})),
        ("claim", Rival::Claims),
    ] {
        let fixture = fixture(&format!("session-{case}"), 4).await;
        let namespace_id = fixture.namespace_id.clone();
        let (store, _directory) = fixture.rival_before_checks(vec![]);
        let namespace = namespace_store(&store, &namespace_id);
        let acquired = namespace
            .acquire_writer_epoch(&writer_at("session", 1_000).writer_id)
            .await
            .expect("the session claims");
        store
            .inner()
            .interceptor()
            .steps
            .lock()
            .expect("the steps lock")
            .push_back(step);
        store.reset();
        let loaded = namespace
            .load_writer_read_anchor(&acquired, observed_now())
            .await;
        // The session's claim put a fence at WAL 6, and the reads end at 7.
        let heads = match case {
            "same-epoch" => {
                let anchor = loaded.expect("a discovery of the session's own epoch");
                assert_eq!(tail_wal_numbers(&anchor), (1..=6).collect::<Vec<_>>());
                assert_eq!(anchor.read_state.writer_epoch, WriterEpoch(2));
                2
            }
            _ => {
                assert_matches!(loaded.map(|_| ()), Err(CoreError::WriterFenced(_)));
                1
            }
        };
        let sent = sent(&namespace_id, &store.take());
        assert_eq!(sent.wal_gets, (1..=7).collect::<Vec<_>>(), "case {case}");
        assert_eq!(
            (sent.manifest_lists, sent.manifest_heads),
            (2, heads),
            "case {case}"
        );
    }
}

/// Checks that a discovery that finds a tombstone after its WAL read returns
/// `namespace_deleted` without another WAL read.
#[tokio::test]
async fn a_discovery_that_finds_a_tombstone_reads_no_more_wal() {
    let fixture = fixture("deleted", 4).await;
    let namespace_id = fixture.namespace_id.clone();
    let (store, _directory) = fixture.rival_before_checks(vec![Rival::Deletes]);
    let deleted = namespace_store(&store, &namespace_id)
        .load_live_read_anchor(observed_now())
        .await
        .map(|_| ());
    assert_matches!(deleted, Err(CoreError::NamespaceDeleted { .. }));
    let sent = sent(&namespace_id, &store.take());
    assert_eq!(sent.wal_gets, (1..=7).collect::<Vec<_>>());
    assert_eq!((sent.manifest_lists, sent.manifest_heads), (2, 1));
}

/// Checks that a discovery reads the WAL tail above a newer manifest again
/// when an object that it read doesn't continue that manifest's head or is
/// above that manifest's writer epoch, and then reports the corruption,
/// because the newer manifest is the newest.
#[tokio::test]
async fn a_kept_object_that_a_newer_manifest_makes_invalid_sends_the_discovery_back_to_the_wal() {
    // The newest manifest before the rival's is the session's claim, with head
    // 0, folded WAL number 0, and writer epoch 1.
    let cases: [(&str, ManifestChange, u64); 2] = [
        // WAL 3 has commit 2, which doesn't continue head 0.
        ("head", |payload| payload.folded_wal_no = WalNo(2), 3),
        // The fence at WAL 1 has writer epoch 1.
        ("epoch", |payload| payload.writer_epoch = WriterEpoch(0), 1),
    ];
    for (case, change, read_again) in cases {
        let fixture = fixture(&format!("broken-{case}"), 4).await;
        let namespace_id = fixture.namespace_id.clone();
        let (store, _directory) = fixture.rival_before_checks(vec![Rival::Publishes(change)]);
        let error = namespace_store(&store, &namespace_id)
            .load_read_anchor(observed_now())
            .await
            .map(|_| ())
            .expect_err("a kept object that the newer manifest makes invalid");
        assert_eq!(
            error.code(),
            ErrorCode::NamespaceCorrupt,
            "case {case}: {error}"
        );
        // The second pass reads the first object above the newer manifest
        // again, and the load after the corrupt object finds the same
        // manifest.
        let mut wal_gets = (1..=7).collect::<Vec<_>>();
        wal_gets.push(read_again);
        wal_gets.sort_unstable();
        assert_eq!(
            sent(&namespace_id, &store.take()),
            Sent {
                wal_gets,
                manifest_lists: 3,
                manifest_heads: 1,
            },
            "case {case}"
        );
    }
}

/// Checks that a discovery whose every check finds a newer manifest returns
/// `store_contention` after its pass limit, and that it reads the WAL only in
/// its first pass.
#[tokio::test]
async fn a_discovery_that_finds_a_newer_manifest_after_every_pass_returns_store_contention() {
    let fixture = fixture("forever", 4).await;
    let namespace_id = fixture.namespace_id.clone();
    let passes = crate::limits::MAX_CONTENTION_ATTEMPTS;
    let (store, _directory) = fixture.rival_before_checks(vec![Rival::Publishes(|_| {}); passes]);
    let error = namespace_store(&store, &namespace_id)
        .load_read_anchor(observed_now())
        .await
        .map(|_| ())
        .expect_err("a newer manifest on every pass");
    assert_matches!(error, CoreError::StoreContention { .. });
    assert_eq!(store.inner().interceptor().steps_left(), 0);
    let sent = sent(&namespace_id, &store.take());
    assert_eq!(sent.wal_gets, (1..=7).collect::<Vec<_>>());
    // The first listing comes before the first pass, and each pass's check
    // that finds a newer manifest lists again.
    assert_eq!(
        (sent.manifest_lists, sent.manifest_heads),
        (passes + 1, passes)
    );
}

/// Checks that a discovery returns every commit that was acknowledged before
/// it began when a fold and a collection empty WAL numbers while the
/// discovery reads them: the read stops at the first emptied number, the
/// check finds the fold's manifest, and the discovery reads on above it.
#[tokio::test]
async fn a_collection_during_a_discovery_hides_no_acknowledged_commit() {
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("collected").expect("namespace");
    let grace = crate::limits::GC_GRACE_WINDOW_MS;
    // Every object is older than the grace window except the manifests from
    // 3 up, so the collection deletes the folded commits.
    let aged = || {
        MetadataMapStore::new(
            MetadataMapStore::aged(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::any(),
            ),
            KeyPredicate::new(|key| {
                manifest_no_of(key).is_some_and(|number| number >= ManifestNo(3))
            }),
            move |mut metadata| {
                metadata.last_modified_at_ms = Some(grace + 1);
                metadata
            },
        )
    };
    let store = RecordingStore::new(
        BlockingStore::new(
            aged(),
            KeyPredicate::new(|key| wal_no_of(key) == Some(WalNo(1))),
            OperationSelector::Read,
        ),
        KeyPredicate::any(),
    );
    let rival = aged();
    let context = writer_at("writer", 1_000);
    NamespaceFixture::new(&rival, &namespace_id, &context)
        .create()
        .await
        .expect("create");
    let mut writer = NamespaceCommitEngine::new(
        namespace_store(&rival, &namespace_id),
        context.writer_id.clone(),
    );
    for index in 0..4 {
        publish(&mut writer, &format!("commit-{index}"))
            .await
            .expect("commit");
    }
    store.inner().block_next();
    let reader = namespace_store(&store, &namespace_id);
    let (anchor, ()) = futures::join!(reader.load_read_anchor(observed_now()), async {
        store.inner().wait_until_blocked().await;
        writer.fold().await.expect("fold the tail");
        // A manifest after the fold, so the fold's manifest isn't current.
        writer
            .set_retention_window(loonfs_types::RetentionWindow::new(2 * grace).ok())
            .await
            .expect("set the retention window");
        let collector = WriterAt {
            now_ms: grace + 1,
            ..writer_at("writer", 1_000)
        };
        namespace_store(&rival, &namespace_id)
            .gc(&collector.clock())
            .await
            .expect("collect the folded commits");
        store.inner().release();
    });
    let anchor = anchor.expect("discovery across a collection");
    assert_eq!(anchor.read_state.seq, CommitSeq(4));
    assert_eq!(anchor.current_manifest().payload().head_seq, CommitSeq(4));
    assert_eq!(tail_wal_numbers(&anchor), Vec::<u64>::new());
    // The fence at WAL 1 stays, the collection deleted WAL 2 and 3, and the
    // second pass reads on above the fold at WAL 6.
    let sent = sent(&namespace_id, &store.take());
    assert_eq!(sent.wal_gets, vec![1, 2, 3, 6]);
    assert_eq!((sent.manifest_lists, sent.manifest_heads), (2, 2));
}

/// Checks that a discovery that loads an initializing manifest, whose pass
/// reads no WAL, keeps nothing when its check finds the active manifest, and
/// reads the WAL above the active manifest.
#[tokio::test]
async fn a_discovery_above_an_initializing_manifest_keeps_nothing() {
    let fixture = fixture("initializing", 0).await;
    let store = fixture.store();
    // Takes the active manifest out, so that the newest manifest is the
    // initializing manifest 1, and puts it back before the check of manifest
    // 1.
    let manifests = store
        .list_prefix(&metadata_manifest_prefix(&fixture.namespace_id))
        .await
        .expect("list the manifests");
    assert_eq!(
        manifests
            .iter()
            .filter_map(|key| manifest_no_of(key))
            .collect::<Vec<_>>(),
        vec![ManifestNo(1), ManifestNo(2)]
    );
    let key = metadata_manifest_object(&fixture.namespace_id, &ManifestNo(2));
    let bytes = store
        .get(&key, None)
        .await
        .expect("get the active manifest")
        .expect("the active manifest");
    store
        .delete(&key)
        .await
        .expect("take the active manifest out");
    let namespace_id = fixture.namespace_id.clone();
    let (store, _directory) = fixture.rival_before_checks(vec![Rival::Puts { key, bytes }]);
    let anchor = namespace_store(&store, &namespace_id)
        .load_read_anchor(observed_now())
        .await
        .expect("a discovery across the activation");
    assert!(anchor.read_state.status.is_active());
    assert_eq!(
        anchor.current_manifest().manifest().manifest_no,
        ManifestNo(2)
    );
    assert_eq!(tail_wal_numbers(&anchor), Vec::<u64>::new());
    // The pass above manifest 1 reads no WAL, and the pass above manifest 2
    // reads WAL 1.
    assert_eq!(
        sent(&namespace_id, &store.take()),
        Sent {
            wal_gets: vec![1],
            manifest_lists: 2,
            manifest_heads: 2,
        }
    );
}

/// Checks that a discovery whose WAL read finds a gap, a codec error, reads
/// the whole tail above the newer manifest that the reload finds, because a
/// pass after a codec error keeps nothing.
#[tokio::test]
async fn a_discovery_after_a_codec_error_reads_the_whole_tail_above_the_newer_manifest() {
    let (fixture, fold) = fixture_with_an_earlier_fold("codec").await;
    let Rival::Puts { key, bytes } = fold else {
        panic!("an earlier fold is a put");
    };
    let namespace_id = fixture.namespace_id.clone();
    let rival = fixture.store();
    let wal_2 = rival
        .list_prefix(&wal_prefix(&namespace_id))
        .await
        .expect("list the WAL")
        .into_iter()
        .find(|key| wal_no_of(key) == Some(WalNo(2)))
        .expect("WAL 2");
    let store = RecordingStore::new(
        BlockingStore::new(
            fixture.store(),
            KeyPredicate::new(|key| wal_no_of(key) == Some(WalNo(2))),
            OperationSelector::Read,
        ),
        KeyPredicate::any(),
    );
    store.inner().block_next();
    let reader = namespace_store(&store, &namespace_id);
    // While the first GET of WAL 2 waits, the fold at WAL 3 is published and
    // a collection deletes WAL 2, so the read finds WAL 2 missing below the
    // present WAL 3.
    let (anchor, ()) = futures::join!(reader.load_read_anchor(observed_now()), async {
        store.inner().wait_until_blocked().await;
        rival
            .put_if_absent(&key, bytes)
            .await
            .expect("publish the fold");
        rival.delete(&wal_2).await.expect("delete WAL 2");
        store.inner().release();
    });
    let anchor = anchor.expect("a discovery after a codec error");
    assert_eq!(anchor.current_manifest().payload().folded_wal_no, WalNo(3));
    assert_eq!(tail_wal_numbers(&anchor), vec![4, 5]);
    assert_eq!(anchor.read_state.seq, CommitSeq(4));
    // The first pass reads WAL 1 to 3 and WAL 2 again, and the second pass
    // reads WAL 4 to 6 above the fold.
    assert_eq!(
        sent(&namespace_id, &store.take()),
        Sent {
            wal_gets: vec![1, 2, 2, 3, 4, 5, 6],
            manifest_lists: 2,
            manifest_heads: 1,
        }
    );
}

#[test]
fn a_higher_epoch_is_writer_fenced_with_its_writer_block_and_one_without_a_block_is_refused() {
    let session = crate::namespace::writer_epoch::AcquiredWriter {
        writer_id: loonfs_types::WriterId::parse("writer-a").expect("writer id"),
        writer_epoch: WriterEpoch(1),
    };
    let block = loonfs_types::format::control::WriterBlock {
        writer_id: loonfs_types::WriterId::parse("writer-b").expect("writer id"),
        acquired_at_ms: 2_000,
    };
    let namespace_id = NamespaceId::parse("fenced").expect("namespace");
    let mut manifest = NamespaceManifestPayload::initial(
        namespace_id.clone(),
        loonfs_types::PoolId::parse("pool_00000000000000000000000001").expect("pool"),
        1_000,
        loonfs_test_support::test_actor(),
    )
    .activated();
    manifest.manifest_no = ManifestNo(2);
    manifest.writer_epoch = WriterEpoch(2);
    manifest.writer = Some(block.clone());
    assert_matches!(
        super::ensure_stored_writer_epoch(&manifest, &session),
        Err(CoreError::WriterFenced(fence)) if fence.active_writer == block
            && fence.active_epoch == WriterEpoch(2)
            && fence.fenced_epoch == WriterEpoch(1)
    );

    // Decoding refuses the same manifest without its writer block, so the
    // writer's load never compares an epoch that has no writer.
    manifest.writer = None;
    let key = metadata_manifest_object(&namespace_id, &ManifestNo(2));
    let (_, bytes) = loonfs_types::format::manifest::encode_namespace_manifest_json(manifest)
        .expect("manifest envelope")
        .into_parts();
    let error =
        crate::manifest::load::decode_manifest_at(&namespace_id, ManifestNo(2), &key, &bytes)
            .expect_err("a raised epoch without a writer block is refused");
    assert_matches!(
        error,
        crate::manifest::ManifestLoadError::RunManifestMismatch { message, .. }
            if message == "writer epoch 2 has no writer block"
    );
}
