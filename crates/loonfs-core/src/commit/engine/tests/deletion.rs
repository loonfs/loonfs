//! Writer fencing and deletion budgets.

use super::*;
use crate::test_support::WriterAt;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationSelector, RecordingStore};
use loonfs_types::AbsolutePath;
use std::assert_matches;
use tempfile::tempdir;

fn context() -> WriterAt {
    crate::test_support::writer_at("writer", 1_000)
}

#[tokio::test]
async fn deletion_budget_includes_writer_acquisition() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("deletion-budget").expect("namespace");
    // The acquisition's check for a successor of its claim (manifest 3) is
    // held while the clock passes the budget.
    let store = BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
        KeyPredicate::exact(metadata_manifest_object(
            &namespace_id,
            &loonfs_types::ManifestNo(4),
        )),
        OperationSelector::Head,
    );
    let context = context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let clock = Arc::new(ManualClock::new(0));
    let mut publisher = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            timer: clock.clone(),
            ..crate::test_support::namespace_store(&store, &namespace_id)
        },
        context.writer_id.clone(),
    );
    store.block_next();
    let administrator = crate::test_support::administrator_of(&store, &namespace_id);
    let delete = publisher.delete_namespace_alone(&administrator, Default::default());
    let delay = async {
        store.wait_until_blocked().await;
        clock.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
        store.release();
    };
    let (result, ()) = tokio::join!(delete, delay);
    assert_matches!(
        result,
        Err(CoreError::MetadataPublicationBudgetExceeded { .. })
    );
    let manifest = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("manifest");
    assert!(!manifest.payload().status.is_deleted());
    assert_eq!(manifest.payload().manifest_no, loonfs_types::ManifestNo(3));
}

#[tokio::test]
async fn a_stale_writer_stays_fenced_after_namespace_deletion() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("terminal-writer").expect("namespace");
    let context = context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let mut stale = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context.writer_id.clone(),
    );
    stale
        .publish_batch_alone(
            [CommitCandidate::new(CommitIntent::single(
                CommitId::parse("before-delete").expect("commit"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::CreateDirectory {
                    path: AbsolutePath::parse("/early").expect("path"),
                    parents: false,
                },
            ))],
            &context.clock(),
        )
        .await
        .results
        .remove(0)
        .expect("publish before deletion");
    NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context.writer_id.clone(),
    )
    .delete_namespace_alone(
        &crate::test_support::administrator_of(&store, &namespace_id),
        Default::default(),
    )
    .await
    .expect("delete");
    let candidate = CommitCandidate::new(CommitIntent::single(
        CommitId::parse("after-delete").expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse("/late").expect("path"),
            parents: false,
        },
    ));
    for attempt in 0..3 {
        store.reset();
        let error = stale
            .publish_batch_alone([candidate.clone()], &context.clock())
            .await
            .results
            .remove(0)
            .expect_err("stale writer");
        assert_eq!(
            error.code(),
            if attempt == 0 {
                loonfs_types::ErrorCode::StoreContention
            } else {
                loonfs_types::ErrorCode::WriterFenced
            }
        );
        assert_eq!(store.counts().puts, usize::from(attempt == 0));
        assert_eq!(store.counts().compare_and_swaps, 0);
    }
}

#[tokio::test]
async fn rejected_deletion_writes_nothing_before_folding_inline_content() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("deletion-before-fold").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let context = context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let clock = Arc::new(ManualClock::new(0));
    let mut engine = NamespaceCommitEngine::new(
        crate::NamespaceStore {
            timer: clock.clone(),
            ..crate::test_support::namespace_store(&store, &namespace_id)
        },
        context.writer_id.clone(),
    );
    let value = InlineContent::new(
        ContentId::generate(),
        bytes::Bytes::from_static(b"unfolded content"),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let candidate = CommitCandidate::with_inline_content(
        CommitIntent::single(
            CommitId::generate(),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/content").expect("path"),
                content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
                behavior: loonfs_types::DestinationBehavior::NoReplace,
                expected_inode_id: None,
                expected_revision_no: None,
            },
        ),
        Vec::new(),
        vec![value],
    );
    engine
        .publish_batch_alone(
            [candidate],
            &OperationClock::start(context.now_ms, clock.clone()),
        )
        .await
        .results
        .pop()
        .expect("result")
        .expect("publish inline content");
    let position = engine
        .writer_position()
        .expect("the session keeps its position after its commit");
    let lane = engine.publication_lane();
    for expected_head_seq in [CommitSeq(0), CommitSeq(1)] {
        let deadline = OperationClock::start(context.now_ms, clock.clone());
        if expected_head_seq == CommitSeq(1) {
            clock.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
        }
        store.reset();
        let error = crate::test_support::namespace_store(&store, &namespace_id)
            .delete_namespace(
                DeleteNamespaceOptions {
                    expected_head_seq: Some(expected_head_seq),
                },
                position.clone(),
                &lane.lock().await,
                &deadline,
            )
            .await
            .expect_err("deletion rejected before folding");
        if expected_head_seq == CommitSeq(0) {
            assert_matches!(
                error,
                CoreError::StaleHeadPrecondition {
                    expected: CommitSeq(0),
                    actual: CommitSeq(1),
                    ..
                }
            );
        } else {
            assert_matches!(error, CoreError::MetadataPublicationBudgetExceeded { .. });
        }
        assert_eq!(store.counts().puts, 0);
        assert_eq!(store.counts().compare_and_swaps, 0);
    }
}

/// How a deletion's tombstone put ends, in a case whose outcome the
/// deletion learns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LearnedTombstonePut {
    /// The put is created.
    Created,
    /// The put is created, and its response is lost.
    CreatedResponseLost,
}

/// Checks that a deletion returns its response for each outcome of its
/// tombstone put that the deletion learns, and that the session keeps its
/// epoch after each one (format spec, "Failed and unknown outcomes").
#[tokio::test]
async fn a_tombstone_put_whose_outcome_the_deletion_learns_leaves_the_session_its_epoch() {
    use loonfs_test_support::stores::{FailStore, InjectedError};

    for put in [
        LearnedTombstonePut::Created,
        LearnedTombstonePut::CreatedResponseLost,
    ] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("lost-tombstone").expect("namespace");
        let base = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let context = context();
        crate::test_support::NamespaceFixture::new(&base, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
        // The deletion claims the writer epoch at manifest 3, folds the
        // claim's fence into manifest 4, and puts its tombstone at manifest 5.
        let tombstone = metadata_manifest_object(&namespace_id, &loonfs_types::ManifestNo(5));
        let store = FailStore::new(
            base.clone(),
            KeyPredicate::exact(&tombstone),
            OperationSelector::PutCreateIfAbsent,
            InjectedError::Transport("lost response".to_owned()),
        );
        let store = match put {
            LearnedTombstonePut::CreatedResponseLost => {
                let store = store.apply_then_fail();
                store.fail_next(1);
                store
            }
            LearnedTombstonePut::Created => store,
        };
        let mut engine = NamespaceCommitEngine::new(
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context).namespace,
            context.writer_id.clone(),
        );
        let administrator = crate::test_support::administrator_of(&store, &namespace_id);
        let deleted = engine
            .delete_namespace_alone(&administrator, Default::default())
            .await;
        assert_eq!(store.remaining(), 0, "{put:?}");
        assert_eq!(deleted.expect("the deletion").namespace_id, namespace_id);
        assert_matches!(
            *engine.session_status().lock(),
            WriterSessionStatus::Acquired { .. },
            "{put:?}"
        );
        let newest = newest_manifest(&base, &namespace_id).await;
        assert_eq!(newest.manifest_no, loonfs_types::ManifestNo(5), "{put:?}");
        assert!(newest.status.is_deleted(), "{put:?}");
    }
}

/// Returns the payload of the newest manifest of `namespace_id`.
async fn newest_manifest<S: ObjectStore>(
    store: S,
    namespace_id: &NamespaceId,
) -> loonfs_types::format::manifest::NamespaceManifestPayload {
    let crate::namespace::control::NewestManifest::Manifest(newest) =
        crate::test_support::namespace_store(store, namespace_id)
            .find_newest_manifest(crate::test_support::observed_now())
            .await
            .expect("newest manifest")
    else {
        panic!("the namespace has a newest manifest");
    };
    newest.manifest().payload().clone()
}

/// The store that [`UnknownTombstone`]'s session writes through: it fails the
/// put of manifest 5 before the put reaches the store.
type TombstoneFailStore = loonfs_test_support::stores::FailStore<LocalFsStore>;

/// A session whose deletion's tombstone put has an unknown outcome: the put
/// of manifest 5 failed before it reached the store, so the deletion read
/// manifest 5 back empty. The put stays in flight, and a test writes its
/// bytes when the put in flight would be applied.
///
/// The session claimed at manifest 3 and committed `alpha`, and the deletion
/// folded into manifest 4.
struct UnknownTombstone {
    _directory: tempfile::TempDir,
    namespace_id: NamespaceId,
    writer: WriterAt,
    local: LocalFsStore,
    store: Arc<TombstoneFailStore>,
    engine: NamespaceCommitEngine<Arc<TombstoneFailStore>>,
    tombstone_key: String,
    tombstone: bytes::Bytes,
}

impl UnknownTombstone {
    /// Runs the deletion up to its unknown outcome, and checks that it
    /// returns `outcome_unknown` and leaves the session without its epoch.
    async fn new() -> Self {
        use loonfs_objectstore::PutMode;
        use loonfs_test_support::stores::{FailStore, InjectedError, OperationKind};
        use std::sync::Mutex;

        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("late-tombstone").expect("namespace");
        let local = LocalFsStore::new(directory.path()).expect("store");
        let writer = context();
        crate::test_support::NamespaceFixture::new(&local, &namespace_id, &writer)
            .create()
            .await
            .expect("bootstrap");
        let tombstone_key = metadata_manifest_object(&namespace_id, &loonfs_types::ManifestNo(5));
        let in_flight: Arc<Mutex<Option<bytes::Bytes>>> = Arc::default();
        let recorded = Arc::clone(&in_flight);
        let selected = tombstone_key.clone();
        let store = Arc::new(FailStore::matching(
            LocalFsStore::new(directory.path()).expect("store"),
            move |operation| match operation.kind() {
                OperationKind::Put {
                    bytes,
                    mode: PutMode::CreateIfAbsent,
                } if operation.key() == selected => {
                    recorded
                        .lock()
                        .expect("lock")
                        .get_or_insert_with(|| bytes::Bytes::clone(bytes));
                    true
                }
                _ => false,
            },
            InjectedError::Transport("tombstone put still in flight".into()),
        ));
        let mut engine = NamespaceCommitEngine::new(
            crate::test_support::namespace_store(Arc::clone(&store), &namespace_id),
            writer.writer_id.clone(),
        );
        publish_one(&mut engine, &writer, "alpha").await;

        store.fail_next(1);
        let deleted = engine
            .delete_namespace_alone(
                &crate::test_support::administrator_of(Arc::clone(&store), &namespace_id),
                Default::default(),
            )
            .await;
        assert_matches!(deleted, Err(CoreError::OutcomeUnknown { .. }));
        assert_matches!(
            *engine.session_status().lock(),
            WriterSessionStatus::Unacquired
        );
        let tombstone = in_flight
            .lock()
            .expect("lock")
            .take()
            .expect("the tombstone put");
        Self {
            _directory: directory,
            namespace_id,
            writer,
            local,
            store,
            engine,
            tombstone_key,
            tombstone,
        }
    }

    /// Writes the tombstone put in flight, as the provider would apply it
    /// now.
    async fn apply_the_put_in_flight(
        &self,
    ) -> std::result::Result<loonfs_objectstore::ObjectMetadata, loonfs_objectstore::ObjectStoreError>
    {
        self.local
            .put_if_absent(&self.tombstone_key, self.tombstone.clone())
            .await
    }

    async fn newest(&self) -> loonfs_types::format::manifest::NamespaceManifestPayload {
        newest_manifest(&self.local, &self.namespace_id).await
    }
}

/// Checks that a deletion whose tombstone put has an unknown outcome can't
/// end the namespace below a commit that the session acknowledged after the
/// unknown outcome, and that the session doesn't acknowledge commits after
/// the tombstone is written (API spec, "DELETE /v0/namespaces/{ns}": deletion
/// linearizes at the manifest put).
///
/// After the unknown outcome (see [`UnknownTombstone`]), the session commits
/// `gamma`, and the put in flight is written at manifest 5. The session then
/// tries to commit `delta`.
#[tokio::test]
async fn a_late_tombstone_put_never_ends_the_namespace_below_an_acknowledged_commit() {
    let mut unknown = UnknownTombstone::new().await;
    let writer = unknown.writer.clone();

    let gamma = publish_one(&mut unknown.engine, &writer, "gamma")
        .await
        .results[0]
        .clone()
        .expect("gamma");
    // The provider applies the put in flight now.
    let late_put = unknown.apply_the_put_in_flight().await;
    let delta = unknown
        .engine
        .publish_batch_alone([create_dir("delta", "delta")], &writer.clock())
        .await
        .results
        .remove(0);

    let newest = unknown.newest().await;
    let mut broken = Vec::new();
    if newest.status.is_deleted() && newest.head_seq < gamma.committed_seq {
        broken.push(format!(
            "the late tombstone put ({late_put:?}) ended the namespace at manifest {} with head {:?}, below the acknowledged commit gamma at {:?}",
            newest.manifest_no.0, newest.head_seq, gamma.committed_seq
        ));
    }
    if newest.status.is_deleted() {
        if let Ok(commit) = &delta {
            broken.push(format!(
                "the session acknowledged delta at {:?} after the tombstone was written",
                commit.committed_seq
            ));
        }
    }
    assert!(broken.is_empty(), "{broken:#?}");
}

/// Checks that the session's next commit after a tombstone put with an
/// unknown outcome claims a new epoch at the tombstone's manifest number, so
/// the late tombstone put collides with the claim and the namespace stays
/// active with the commit.
#[tokio::test]
async fn a_commit_after_an_unknown_tombstone_put_claims_the_tombstones_number() {
    let mut unknown = UnknownTombstone::new().await;
    let writer = unknown.writer.clone();

    let gamma = publish_one(&mut unknown.engine, &writer, "gamma")
        .await
        .results[0]
        .clone()
        .expect("gamma");
    let claim = unknown.newest().await;
    assert_eq!(claim.manifest_no, loonfs_types::ManifestNo(5));
    assert_eq!(claim.writer_epoch, WriterEpoch(2));

    assert_matches!(
        unknown.apply_the_put_in_flight().await,
        Err(loonfs_objectstore::ObjectStoreError::PreconditionFailed { .. })
    );
    let delta = publish_one(&mut unknown.engine, &writer, "delta")
        .await
        .results[0]
        .clone()
        .expect("delta");
    assert!(delta.committed_seq > gamma.committed_seq);
    assert!(!unknown.newest().await.status.is_deleted());
}

/// Checks that the session's next commit after a tombstone put with an
/// unknown outcome returns `namespace_deleted` when the late tombstone put
/// was written before the commit's claim.
#[tokio::test]
async fn a_commit_after_a_late_tombstone_is_written_returns_namespace_deleted() {
    let mut unknown = UnknownTombstone::new().await;
    let writer = unknown.writer.clone();

    unknown
        .apply_the_put_in_flight()
        .await
        .expect("the late tombstone put is written");
    let gamma = unknown
        .engine
        .publish_batch_alone([create_dir("gamma", "gamma")], &writer.clock())
        .await
        .results
        .remove(0);
    assert_matches!(gamma, Err(CoreError::NamespaceDeleted { .. }));
    assert_matches!(
        *unknown.engine.session_status().lock(),
        WriterSessionStatus::NamespaceGone
    );
    let newest = unknown.newest().await;
    assert_eq!(newest.manifest_no, loonfs_types::ManifestNo(5));
    assert!(newest.status.is_deleted());
}

/// Checks that a retry of a deletion whose tombstone put has an unknown
/// outcome claims a new epoch at the tombstone's manifest number and
/// publishes its own tombstone, so the late tombstone put collides.
#[tokio::test]
async fn a_retried_deletion_after_an_unknown_tombstone_put_claims_and_deletes() {
    let mut unknown = UnknownTombstone::new().await;

    let administrator =
        crate::test_support::administrator_of(Arc::clone(&unknown.store), &unknown.namespace_id);
    let deleted = unknown
        .engine
        .delete_namespace_alone(&administrator, Default::default())
        .await
        .expect("the retried deletion");
    assert_eq!(deleted.head_seq, CommitSeq(1));
    assert_matches!(
        unknown.apply_the_put_in_flight().await,
        Err(loonfs_objectstore::ObjectStoreError::PreconditionFailed { .. })
    );
    let newest = unknown.newest().await;
    assert!(newest.status.is_deleted());
    assert_eq!(newest.writer_epoch, WriterEpoch(2));
    assert_eq!(newest.head_seq, CommitSeq(1));
}
