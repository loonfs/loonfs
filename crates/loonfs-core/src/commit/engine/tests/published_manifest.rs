//! A manifest that the session's runtime publishes while a publication unit
//! has the engine: the position that the next unit plans from follows it.

use super::*;
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;

/// Checks that a fold that the session's runtime publishes while a commit's
/// WAL put is in flight isn't undone when the commit puts its position back:
/// the commit, planned on the manifest that the fold replaced, ends in the
/// tail above the fold's manifest.
#[tokio::test]
async fn a_commit_planned_before_a_fold_is_in_the_tail_above_the_folds_manifest() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    );
    let writer = context("writer-a");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &writer)
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer.writer_id.clone(),
    );
    publish_one(&mut engine, &writer, "alpha").await;
    let planned = engine.writer_position().expect("the position after alpha");
    let published = engine.published_manifest();

    store.block_next();
    let clock = writer.clock();
    let beta = engine.publish_batch_alone(vec![create_dir("beta", "beta")], &clock);
    let fold = async {
        store.wait_until_blocked().await;
        let folded = crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal_tail_in_new_lane(&planned, &writer.clock())
            .await
            .expect("fold");
        published.record(&folded.current);
        store.release();
        folded
    };
    let (mut beta, folded) = tokio::join!(beta, fold);
    assert_eq!(
        beta.wal_tail.map(|tail| tail.objects),
        Some(1),
        "the tail report describes the position that the engine keeps"
    );
    let beta = beta
        .results
        .pop()
        .expect("one result")
        .expect("beta commits");

    // The position that the next unit plans from.
    let position = engine
        .position
        .current
        .take()
        .expect("a position after beta");
    assert_eq!(
        position.read_basis().basis().manifest_no(),
        folded.current.manifest_no(),
        "the commit keeps the fold's move"
    );
    assert_eq!(position.read_basis().head.seq, beta.committed_seq);
    assert_eq!(
        position.read_basis().head.folded_wal_no,
        planned.read_basis().head.wal_no
    );
    assert_eq!(position.wal_tail_objects(), 1);
    assert!(position
        .tail
        .find_commit_receipt(&CommitId::parse("beta").expect("id"))
        .is_some());
}

/// Checks that two publications during one commit's WAL put, a fold and a
/// retention window change over the fold's manifest, move the position that
/// the next unit plans from straight onto the window change's manifest,
/// with the commit as its tail.
#[tokio::test]
async fn two_publications_during_a_put_move_the_position_onto_the_second() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    );
    let writer = context("writer-a");
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &writer)
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer.writer_id.clone(),
    );
    publish_one(&mut engine, &writer, "alpha").await;
    let planned = engine.writer_position().expect("the position after alpha");
    let published = engine.published_manifest();

    store.block_next();
    let clock = writer.clock();
    let beta = engine.publish_batch_alone(vec![create_dir("beta", "beta")], &clock);
    let publications = async {
        store.wait_until_blocked().await;
        let folded = crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal_tail_in_new_lane(&planned, &writer.clock())
            .await
            .expect("fold");
        published.record(&folded.current);
        let changed = crate::test_support::change_retention_window(
            &crate::test_support::NamespaceFixture::new(&store, &namespace_id, &writer).engine(),
        )
        .await;
        published.record(&changed);
        store.release();
        (folded.current, changed)
    };
    let (beta, (folded, changed)) = tokio::join!(beta, publications);
    beta.results[0].as_ref().expect("beta commits");
    assert!(changed.manifest_no() > folded.manifest_no());

    let position = engine
        .position
        .current
        .take()
        .expect("a position after beta");
    assert_eq!(
        position.read_basis().basis().manifest_no(),
        changed.manifest_no(),
        "the position moves straight onto the second publication"
    );
    assert_eq!(
        position.read_basis().head.folded_wal_no,
        planned.read_basis().head.wal_no
    );
    assert_eq!(position.wal_tail_objects(), 1);
}

/// A store that answers the next HEAD request for one manifest and then
/// keeps the answer back until the test releases it, so a fold can publish
/// that manifest after a load's successor check found none and before the
/// load returns.
#[derive(Debug)]
struct HeldManifestHead {
    inner: LocalFsStore,
    /// The key whose next HEAD request the store keeps back, if any.
    held_key: std::sync::Mutex<Option<String>>,
    held: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl HeldManifestHead {
    fn new(directory: &tempfile::TempDir) -> Self {
        Self {
            inner: LocalFsStore::new(directory.path()).expect("store"),
            held_key: std::sync::Mutex::default(),
            held: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }

    /// Keeps back the answer to the next HEAD request for manifest
    /// `manifest_no`.
    fn hold_next(&self, namespace_id: &NamespaceId, manifest_no: loonfs_types::ManifestNo) {
        *self.held_key.lock().expect("lock") = Some(
            loonfs_objectstore::keys::metadata_manifest_object(namespace_id, &manifest_no),
        );
    }
}

#[async_trait::async_trait]
impl ObjectStore for HeldManifestHead {
    loonfs_test_support::delegate_object_store!(self => self.inner;
        checksum_algorithm,
        identity,
        create_multipart_upload,
        complete_multipart_upload,
        abort_multipart_upload,
        get_with_metadata,
        get,
        put,
        put_streamed,
        put_immutable_verified,
        assemble,
        delete,
        list_entries_from_stream,
        list_child_prefixes,
    );

    async fn head(
        &self,
        key: &str,
    ) -> std::result::Result<
        Option<loonfs_objectstore::ObjectMetadata>,
        loonfs_objectstore::ObjectStoreError,
    > {
        let answer = self.inner.head(key).await;
        let hold = {
            let mut held_key = self.held_key.lock().expect("lock");
            let hold = held_key.as_deref() == Some(key);
            if hold {
                *held_key = None;
            }
            hold
        };
        if hold {
            self.held.notify_one();
            self.release.notified().await;
        }
        answer
    }
}

/// Publishes `beta` through `engine` while `store` keeps back the answer to
/// the HEAD request that [`HeldManifestHead::hold_next`] selected, and
/// meanwhile folds the tail from `local` and records the fold's manifest as
/// the session's runtime does. Returns the fold's manifest.
async fn publish_while_a_fold_publishes(
    engine: &mut NamespaceCommitEngine<&HeldManifestHead>,
    store: &HeldManifestHead,
    local: &LocalFsStore,
    writer: &WriterAt,
) -> Held<LoadedManifest> {
    let namespace_id = engine.namespace.namespace_id.clone();
    let published = engine.published_manifest();
    let clock = writer.clock();
    let beta = engine.publish_batch_alone(vec![create_dir("beta", "beta")], &clock);
    let fold = async {
        store.held.notified().await;
        // The runtime's fold, as a session of the writer's epoch that
        // doesn't wait for `engine`.
        let folded = crate::test_support::namespace_store(local, &namespace_id)
            .fixture_writer_session()
            .await
            .expect("the writer's session")
            .fold()
            .await
            .expect("fold");
        assert_eq!(folded.response.outcome, crate::FoldWalOutcome::Published);
        published.record(&folded.current);
        store.release.notify_one();
        folded.current
    };
    let (mut beta, folded) = tokio::join!(beta, fold);
    beta.results
        .pop()
        .expect("one result")
        .expect("beta commits");
    folded
}

/// Checks that a fold that publishes while a unit loads the namespace from
/// the store moves the position that the unit then installs, so the
/// session's position is never below a manifest that its runtime published.
///
/// The session has no position, so its next commit loads the namespace. The
/// load's check for a successor of the manifest it loaded finds none, and
/// the fold publishes that successor before the load returns.
#[tokio::test]
async fn a_fold_published_while_a_unit_loads_moves_the_loaded_position() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let store = HeldManifestHead::new(&directory);
    let writer = context("writer-a");
    crate::test_support::NamespaceFixture::new(&local, &namespace_id, &writer)
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer.writer_id.clone(),
    );
    publish_one(&mut engine, &writer, "alpha").await;
    let loaded = engine
        .writer_position()
        .expect("the position after alpha")
        .read_basis()
        .basis()
        .manifest_no();
    engine.invalidate_projection();
    store.hold_next(
        &namespace_id,
        loaded.successor().expect("a next manifest number"),
    );
    let folded = publish_while_a_fold_publishes(&mut engine, &store, &local, &writer).await;

    // The position that the next unit plans from.
    let position = engine
        .position
        .current
        .take()
        .expect("a position after beta");
    assert_eq!(
        position.read_basis().basis().manifest_no(),
        folded.manifest_no(),
        "the loaded position moves onto the fold's manifest"
    );
    assert_eq!(position.wal_tail_objects(), 1);
}

/// Checks that a fold that publishes while a session claims the namespace
/// moves the position that the claim installs.
///
/// Another writer's commit is in the tail. The claim's discovery checks for
/// a successor of the claim's manifest and finds none, and the fold folds the
/// tail over the claim's manifest before the claim returns.
#[tokio::test]
async fn a_fold_published_during_a_claim_moves_the_claimed_position() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let first = context("writer-a");
    crate::test_support::NamespaceFixture::new(&local, &namespace_id, &first)
        .create()
        .await
        .expect("bootstrap");
    let mut previous = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&local, &namespace_id),
        first.writer_id.clone(),
    );
    publish_one(&mut previous, &first, "alpha").await;
    let current = previous
        .writer_position()
        .expect("the position after alpha")
        .read_basis()
        .basis()
        .manifest_no();

    let store = HeldManifestHead::new(&directory);
    let claim = current.successor().expect("a next manifest number");
    store.hold_next(
        &namespace_id,
        claim.successor().expect("a next manifest number"),
    );
    let writer = context("writer-b");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer.writer_id.clone(),
    );
    let folded = publish_while_a_fold_publishes(&mut engine, &store, &local, &writer).await;

    // The position that the next unit plans from.
    let position = engine
        .position
        .current
        .take()
        .expect("a position after beta");
    assert_eq!(
        position.read_basis().basis().manifest_no(),
        folded.manifest_no(),
        "the claimed position moves onto the fold's manifest"
    );
}

/// Brings the engine's position and the lane's copy of it up to the
/// session's newest published manifest, as the runtime's release of the
/// engine does, and drops the replaced positions after the unlock.
fn follow_as_the_release_does<S: ObjectStore>(engine: &mut NamespaceCommitEngine<S>) {
    let published = engine.published_manifest();
    let locked = published.lock();
    let replaced = engine.follow_published(&locked);
    drop(locked);
    drop(replaced);
}

/// Returns a new session of `writer` whose first commit is published, with
/// the session's position copied into its publication lane.
async fn session_with_a_kept_position<'a>(
    store: &'a LocalFsStore,
    namespace_id: &NamespaceId,
    writer: &WriterAt,
) -> NamespaceCommitEngine<&'a LocalFsStore> {
    crate::test_support::NamespaceFixture::new(store, namespace_id, writer)
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(store, namespace_id),
        writer.writer_id.clone(),
    );
    publish_one(&mut engine, writer, "alpha").await;
    follow_as_the_release_does(&mut engine);
    assert!(
        engine.publication_lane().lock().await.turn().is_some(),
        "the lane keeps a copy of the position"
    );
    engine
}

/// Checks that a publication planned outside the lane, such as a compaction
/// unit's or a snapshot's, builds on the newer of the manifest it planned
/// from and the session's newest published manifest, whatever the lane
/// keeps: a position that can't follow that manifest, no position, or a
/// position above it.
#[tokio::test]
async fn a_publication_planned_outside_the_lane_builds_on_the_newest_published_manifest() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let writer = context("writer-a");
    let mut engine = session_with_a_kept_position(&store, &namespace_id, &writer).await;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let position = engine.writer_position().expect("the position after alpha");
    let planned = namespace
        .load_position_basis(&position)
        .await
        .expect("load the position's manifest")
        .expect("the position's manifest exists");
    // A fold that folds past the kept position's tip, recorded in the lane
    // as a publication of the lane records its manifest.
    let mut other = namespace
        .fixture_writer_session()
        .await
        .expect("a session of the writer's epoch");
    publish_one(&mut other, &writer, "beta").await;
    let folded = other.fold().await.expect("fold").current;
    let lane = engine.publication_lane();
    lane.lock().await.record(&folded);
    assert!(folded.manifest_no() > planned.manifest_no());

    let basis_over = |lane: &LaneGuard<'_>| lane.basis_over(planned.clone()).manifest_no();
    {
        let guard = lane.lock().await;
        assert!(
            guard.turn().is_none(),
            "the kept position can't follow a fold past its tip"
        );
        assert_eq!(
            basis_over(&guard),
            folded.manifest_no(),
            "a dropped position"
        );
    }
    engine.invalidate_projection();
    assert_eq!(
        basis_over(&lane.lock().await),
        folded.manifest_no(),
        "no position"
    );
    let above = crate::test_support::change_retention_window(
        &crate::test_support::NamespaceFixture::new(&store, &namespace_id, &writer).engine(),
    )
    .await;
    engine
        .load_position_alone()
        .await
        .expect("load the position");
    follow_as_the_release_does(&mut engine);
    {
        let guard = lane.lock().await;
        assert_eq!(
            guard.turn().map(|turn| turn.position().basis.manifest_no()),
            Some(above.manifest_no()),
            "the kept position is above the newest published manifest"
        );
        assert_eq!(
            basis_over(&guard),
            folded.manifest_no(),
            "a position above the newest published manifest"
        );
    }
}

/// Checks that a follow returns the copy of the position that it replaces
/// in the session's publication lane, so the runtime drops that copy, and
/// the folded tail that it can be the last owner of, after it unlocks the
/// published manifest.
#[tokio::test]
async fn a_follow_returns_the_lanes_replaced_copy() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let writer = context("writer-a");
    let mut engine = session_with_a_kept_position(&store, &namespace_id, &writer).await;
    let (tail, folded) = {
        let position = engine.writer_position().expect("the position after alpha");
        let folded = crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal_tail_in_new_lane(&position, &writer.clock())
            .await
            .expect("fold");
        (Arc::downgrade(position.tail()), folded.current)
    };
    engine.publication_lane().lock().await.record(&folded);
    // The engine's own position follows the fold first, as a unit's store
    // does, so the lane's copy is the last owner of the folded tail.
    let stored = engine.position.current.take().expect("a position");
    engine.position.current.set(stored);
    assert_eq!(
        std::sync::Weak::strong_count(&tail),
        1,
        "only the lane's copy keeps the folded tail"
    );
    let published = engine.published_manifest();
    let locked = published.lock();
    let replaced = engine.follow_published(&locked);
    assert_eq!(
        std::sync::Weak::strong_count(&tail),
        1,
        "the lane's replaced copy isn't dropped with the published manifest locked"
    );
    drop(locked);
    drop(replaced);
    assert_eq!(std::sync::Weak::strong_count(&tail), 0);
}

/// Checks that a fenced session leaves no copy of its position in its
/// publication lane, so no publication of the lane builds on a head that
/// the new writer has moved past.
#[tokio::test]
async fn a_fenced_session_leaves_no_position_in_its_lane() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let writer = context("writer-a");
    let mut engine = session_with_a_kept_position(&store, &namespace_id, &writer).await;
    engine
        .position
        .record_session_error(&CoreError::WriterFenced(WriterFence {
            fenced_epoch: WriterEpoch(1),
            active_epoch: WriterEpoch(2),
            active_writer: loonfs_types::format::control::WriterBlock {
                writer_id: loonfs_types::WriterId::parse("writer-b").expect("writer id"),
                acquired_at_ms: 2_000,
            },
        }));
    assert!(
        engine.writer_position().is_some(),
        "the engine keeps its position"
    );
    follow_as_the_release_does(&mut engine);
    assert!(
        engine.publication_lane().lock().await.turn().is_none(),
        "the lane keeps no copy of a fenced session's position"
    );
}

/// Checks that a load of the session's position beside a publication of its
/// lane installs a position on the publication's manifest. The load's check
/// for a successor of the manifest it loaded finds none, and the publication
/// records its manifest before the load returns.
#[tokio::test]
async fn a_load_beside_a_lane_publication_installs_a_position_on_its_manifest() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let store = HeldManifestHead::new(&directory);
    let writer = context("writer-a");
    crate::test_support::NamespaceFixture::new(&local, &namespace_id, &writer)
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer.writer_id.clone(),
    );
    publish_one(&mut engine, &writer, "alpha").await;
    let loaded = engine
        .writer_position()
        .expect("the position after alpha")
        .read_basis()
        .basis()
        .manifest_no();
    engine.invalidate_projection();
    store.hold_next(
        &namespace_id,
        loaded.successor().expect("a next manifest number"),
    );
    let lane = engine.publication_lane();
    let publication = async {
        store.held.notified().await;
        let folded = crate::test_support::namespace_store(&local, &namespace_id)
            .fixture_writer_session()
            .await
            .expect("the writer's session")
            .fold()
            .await
            .expect("fold");
        assert_eq!(folded.response.outcome, crate::FoldWalOutcome::Published);
        lane.lock().await.record(&folded.current);
        store.release.notify_one();
        folded.current
    };
    let (load, folded) = tokio::join!(engine.load_position_alone(), publication);
    load.expect("load the position");
    assert_eq!(
        engine
            .position
            .current
            .stored()
            .map(|position| position.read_basis().basis().manifest_no()),
        Some(folded.manifest_no()),
        "the installed position is on the publication's manifest"
    );
}
