//! A writer session after a put whose outcome is unknown: a commit's WAL
//! put, a floor object's put, or a manifest put of its publication lane.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::manifest::FoldWalOutcome;
use crate::wal::WalPublishError;
use bytes::Bytes;
use loonfs_objectstore::keys::metadata_segment_prefix;
use loonfs_objectstore::PutMode;
use loonfs_test_support::stores::{FailStore, InjectedError, MetadataMapStore, OperationKind};
use loonfs_types::format::wal::decode_wal_object_envelope_zstd;
use loonfs_types::options::AttributeInclusion;
use loonfs_types::WalNo;
use std::sync::Mutex;

/// The store that the session writes through. It can hold a WAL put before
/// the store sees it, and it can fail the next WAL put with a lost
/// acknowledgment.
type HoldingStore = FailStore<BlockingStore<LocalFsStore>>;

/// The key and bytes of the newest WAL put the store was sent.
type LastWalPut = Arc<Mutex<Option<(String, Bytes)>>>;

fn holding_store(
    directory: &tempfile::TempDir,
    namespace_id: &NamespaceId,
) -> (HoldingStore, LastWalPut) {
    let last: LastWalPut = Arc::default();
    let recorded = Arc::clone(&last);
    let wal_keys = wal_prefix(namespace_id);
    let store = FailStore::matching(
        BlockingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::prefix(wal_prefix(namespace_id)),
            OperationSelector::PutCreateIfAbsent,
        ),
        move |operation| match operation.kind() {
            OperationKind::Put {
                bytes,
                mode: PutMode::CreateIfAbsent,
            } if operation.key().starts_with(&wal_keys) => {
                *recorded.lock().expect("lock") =
                    Some((operation.key().to_owned(), Bytes::clone(bytes)));
                true
            }
            _ => false,
        },
        InjectedError::Transport("lost WAL acknowledgment".into()),
    );
    (store, last)
}

/// Checks that a writer session whose WAL put has an unknown outcome claims
/// a new epoch and writes a fence at its next publication, so its own put
/// can't leave an emptied number at the session's next WAL number (format
/// spec, "Publication and group commit").
///
/// The put of `beta` at WAL 3 loses its acknowledgment and stays in flight
/// at the provider. The session's next publication, of `gamma`, claims and
/// then stalls before its first WAL put, the fence. Meanwhile the put of
/// `beta` is written at 3, a fold covers 3, and a collection that runs after
/// the grace window deletes the commit object at 3. A session that planned
/// `gamma` from a fresh discovery would plan it at 3, and its put there
/// would succeed and acknowledge a commit that no reader replays. A session
/// without its epoch puts the fence first, and the check after that put
/// finds the number emptied and folded, which only a fold of another
/// process under the claim's epoch can do, so `gamma` returns
/// `namespace_corrupt` and no commit is acknowledged at that number.
#[tokio::test]
async fn a_stalled_put_after_an_unknown_outcome_is_never_written_at_an_emptied_number() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let (store, last_wal_put) = holding_store(&directory, &namespace_id);
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

    store.fail_next(1);
    let lost = engine
        .publish_batch_alone(vec![create_dir("beta", "beta")], &writer.clock())
        .await;
    assert_matches!(
        lost.results.as_slice(),
        [Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(
            _
        )))]
    );
    let (in_flight_key, in_flight_bytes) = last_wal_put
        .lock()
        .expect("lock")
        .take()
        .expect("the put of beta");

    let gamma = publish_while_the_put_is_written_and_collected(
        &mut engine,
        &store,
        &local,
        &writer,
        (in_flight_key, in_flight_bytes),
        "gamma",
    )
    .await;
    assert_matches!(gamma, Err(CoreError::NamespaceCorrupt(_)));
    assert_readable(&local, &namespace_id, &["alpha", "beta"]).await;
}

/// Publishes `name` while the session's first WAL put is held. Meanwhile
/// `in_flight`, an earlier put of the session's, is written, a fold covers
/// it, and a collection that runs after the grace window deletes it. Then the
/// held put goes on. Returns the result of `name`.
async fn publish_while_the_put_is_written_and_collected(
    engine: &mut NamespaceCommitEngine<&HoldingStore>,
    store: &HoldingStore,
    local: &LocalFsStore,
    writer: &WriterAt,
    in_flight: (String, Bytes),
    name: &str,
) -> Result<Commit> {
    let namespace_id = engine.namespace.namespace_id.clone();
    let (in_flight_key, in_flight_bytes) = in_flight;
    store.inner().block_next();
    let clock = writer.clock();
    let stalled = engine.publish_batch_alone(vec![create_dir(name, name)], &clock);
    let meanwhile = async {
        store.inner().wait_until_blocked().await;
        local
            .put_if_absent(&in_flight_key, in_flight_bytes)
            .await
            .expect("the put in flight is written");
        let namespace = crate::test_support::namespace_store(local, &namespace_id);
        assert_eq!(
            namespace
                .fixture_writer_session()
                .await
                .expect("the writer's session")
                .fold()
                .await
                .expect("fold")
                .response
                .outcome,
            FoldWalOutcome::Published
        );
        let aged = MetadataMapStore::aged(local, KeyPredicate::prefix(wal_prefix(&namespace_id)));
        crate::test_support::namespace_store(&aged, &namespace_id)
            .gc(&WriterAt {
                now_ms: writer.now_ms + crate::limits::GC_GRACE_WINDOW_MS + 1,
                ..writer.clone()
            }
            .clock())
            .await
            .expect("collect the folded WAL");
        assert_matches!(
            local.head(&in_flight_key).await,
            Ok(None),
            "the collection emptied the number of the put in flight"
        );
        store.inner().release();
    };
    let (mut stalled, ()) = tokio::join!(stalled, meanwhile);
    stalled.results.pop().expect("one result")
}

/// Checks that a fresh reader finds a directory for each of `names`.
async fn assert_readable(local: &LocalFsStore, namespace_id: &NamespaceId, names: &[&str]) {
    let namespace = crate::test_support::namespace_store(local, namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("a fresh view");
    for name in names {
        view.resolve_path(
            &format!("/{name}"),
            AttributeInclusion::Omit,
            &ReadAccess::live(Authorizer::Service),
        )
        .await
        .unwrap_or_else(|error| panic!("a reader finds {name}: {error}"));
    }
}

/// Checks that a caller that drops the session's publish future while its
/// WAL put is in flight leaves the session without its epoch, so the put
/// can't be written at a number that the session's next commit uses after a
/// collection empties it.
///
/// The put of `beta` is held before the store sees it, and the caller drops
/// the future. The session's next publication, of `gamma`, claims and then
/// stalls before its fence put while the put of `beta` is written at 3,
/// folded and collected. A session that kept its epoch would plan `gamma`
/// at 3 and acknowledge a commit that no reader replays. The claim's fence
/// finds its number folded instead, and `gamma` returns `namespace_corrupt`.
#[tokio::test]
async fn a_dropped_publish_future_leaves_the_session_without_its_epoch() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let (store, last_wal_put) = holding_store(&directory, &namespace_id);
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

    store.inner().block_next();
    {
        let clock = writer.clock();
        let dropped = engine.publish_batch_alone(vec![create_dir("beta", "beta")], &clock);
        tokio::select! {
            _ = dropped => panic!("the put of beta is held"),
            () = store.inner().wait_until_blocked() => {}
        }
    }
    store.inner().release();
    let in_flight = last_wal_put
        .lock()
        .expect("lock")
        .take()
        .expect("the put of beta");

    let gamma = publish_while_the_put_is_written_and_collected(
        &mut engine,
        &store,
        &local,
        &writer,
        in_flight,
        "gamma",
    )
    .await;
    assert_matches!(gamma, Err(CoreError::NamespaceCorrupt(_)));
    assert_readable(&local, &namespace_id, &["alpha", "beta"]).await;
}

/// Checks that a caller that drops the session's publish future while the
/// batch is still planning, before the session sends a WAL put, leaves the
/// session with its epoch (format spec, "Failed and unknown outcomes").
///
/// A fold moves `alpha` into a metadata segment, so planning `beta`, which
/// goes under `alpha`, reads that segment from the store. The read is held,
/// and the caller drops the future. The session's next commit, `gamma`, is
/// written at the number after the tip, and the store has no other WAL
/// object after the tip.
#[tokio::test]
async fn a_publish_future_dropped_while_planning_leaves_the_session_acquired() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(metadata_segment_prefix(&namespace_id)),
        OperationSelector::Read,
    );
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
    let tip = engine
        .writer_position()
        .expect("the position after alpha")
        .read_basis()
        .head
        .wal_no;
    assert_eq!(
        engine.fold().await.expect("fold").response.outcome,
        FoldWalOutcome::Published
    );
    engine.invalidate_projection();

    store.block_next();
    {
        let clock = writer.clock();
        let dropped = engine.publish_batch_alone(vec![create_dir("beta", "alpha/beta")], &clock);
        tokio::select! {
            _ = dropped => panic!("planning beta is held"),
            () = store.wait_until_blocked() => {}
        }
    }
    store.release();

    publish_one(&mut engine, &writer, "gamma").await;
    assert_eq!(
        commit_ids_after(&local, &namespace_id, tip).await,
        [(
            tip.successor().expect("a next number"),
            vec!["gamma".to_owned()]
        )],
        "the session writes gamma at the number after the tip, and no fence"
    );
}

/// Returns the number and commit IDs of each WAL object after `tip`, in
/// order. A fence has no commit IDs.
async fn commit_ids_after(
    local: &LocalFsStore,
    namespace_id: &NamespaceId,
    tip: WalNo,
) -> Vec<(WalNo, Vec<String>)> {
    let mut objects = Vec::new();
    for key in local
        .list_prefix(&wal_prefix(namespace_id))
        .await
        .expect("list WAL")
    {
        let bytes = local.get(&key, None).await.expect("get").expect("WAL");
        let object = decode_wal_object_envelope_zstd(&bytes).expect("decode");
        let payload = object.payload();
        if payload.wal_no > tip {
            let commit_ids = payload
                .records()
                .iter()
                .map(|record| record.commit_id.to_string())
                .collect();
            objects.push((payload.wal_no, commit_ids));
        }
    }
    objects.sort();
    objects
}

/// Checks that a lone writer session whose fence put after an unknown
/// outcome also loses its acknowledgment isn't fenced by itself. Its next
/// publication claims again and puts a fence, and its commit is readable.
#[tokio::test]
async fn a_lost_fence_acknowledgment_never_fences_a_lone_writer() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let (store, _last_wal_put) = holding_store(&directory, &namespace_id);
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

    store.fail_next(1);
    let lost = engine
        .publish_batch_alone(vec![create_dir("beta", "beta")], &writer.clock())
        .await;
    assert_matches!(
        lost.results.as_slice(),
        [Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(
            _
        )))]
    );
    // The next WAL put is the fence, and its acknowledgment is lost too.
    store.fail_next(1);
    let fence_lost = engine
        .publish_batch_alone(vec![create_dir("gamma", "gamma")], &writer.clock())
        .await;
    assert_eq!(
        fence_lost.results[0]
            .as_ref()
            .expect_err("the fence's outcome is unknown")
            .code(),
        ErrorCode::OutcomeUnknown
    );
    let gamma = engine
        .publish_batch_alone(vec![create_dir("gamma", "gamma")], &writer.clock())
        .await;
    gamma.results[0]
        .as_ref()
        .expect("the lone writer commits gamma");
    assert_readable(&local, &namespace_id, &["alpha", "gamma"]).await;
}

/// Checks that a session whose WAL put had an unknown outcome takes the
/// namespace back from a writer that took it over in the meantime. The
/// session has no epoch after the unknown outcome, so its next publication
/// claims the epoch after the takeover's, writes a fence and commits, and
/// the other writer is fenced (format spec, "Writer ownership").
#[tokio::test]
async fn a_session_whose_put_was_unknown_claims_past_a_takeover() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let (store, _last_wal_put) = holding_store(&directory, &namespace_id);
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

    store.fail_next(1);
    let lost = engine
        .publish_batch_alone(vec![create_dir("beta", "beta")], &writer.clock())
        .await;
    assert_matches!(
        lost.results.as_slice(),
        [Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(
            _
        )))]
    );
    let writer_b = context("writer-b");
    let takeover = crate::test_support::namespace_store(&local, &namespace_id)
        .acquire_writer(&writer_b.writer_id)
        .await
        .expect("writer b takes over");

    publish_one(&mut engine, &writer, "gamma").await;
    let reclaimed = match &*engine.position.lock_session() {
        WriterSessionStatus::Acquired { writer, .. } => writer.writer_epoch,
        other => panic!("the session claims the namespace back: {other:?}"),
    };
    assert!(reclaimed > takeover.writer.writer_epoch, "{reclaimed:?}");
    assert_readable(&local, &namespace_id, &["alpha", "gamma"]).await;

    let mut engine_b = NamespaceCommitEngine::with_acquired_session(
        crate::test_support::namespace_store(&local, &namespace_id),
        writer_b.writer_id.clone(),
        takeover.writer,
        takeover.claim,
    );
    let fenced = engine_b
        .publish_batch_alone(vec![create_dir("delta", "delta")], &writer_b.clock())
        .await;
    assert_eq!(
        fenced.results[0]
            .as_ref()
            .expect_err("writer a has the namespace again")
            .code(),
        ErrorCode::WriterFenced
    );
}

/// Checks that a session whose acquisition finds another writer's claim
/// after its own is recorded as fenced. The session's fence put is held
/// while writer b claims and writes its fence, so the session's acquisition
/// returns `writer_fenced`, and its next publication returns
/// `writer_fenced` without claiming again (format spec, "Writer ownership").
#[tokio::test]
async fn an_acquisition_that_finds_a_later_claim_fences_the_session() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
    );
    let writer = context("writer-a");
    crate::test_support::NamespaceFixture::new(&local, &namespace_id, &writer)
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        writer.writer_id.clone(),
    );
    let namespace = crate::test_support::namespace_store(&local, &namespace_id);

    store.block_next();
    let clock = writer.clock();
    let first = engine.publish_batch_alone(vec![create_dir("alpha", "alpha")], &clock);
    let takeover = async {
        store.wait_until_blocked().await;
        namespace
            .acquire_writer_epoch(&context("writer-b").writer_id)
            .await
            .expect("writer b takes over");
        store.release();
    };
    let (first, ()) = tokio::join!(first, takeover);
    assert_eq!(
        first.results[0]
            .as_ref()
            .expect_err("writer b claimed after the session")
            .code(),
        ErrorCode::WriterFenced
    );
    assert_matches!(
        *engine.position.lock_session(),
        WriterSessionStatus::Fenced(_)
    );
    let manifest_no = namespace
        .load_current_manifest()
        .await
        .expect("writer b's manifest")
        .payload()
        .manifest_no;

    let second = engine
        .publish_batch_alone(vec![create_dir("beta", "beta")], &writer.clock())
        .await;
    assert_eq!(
        second.results[0]
            .as_ref()
            .expect_err("the session stays fenced")
            .code(),
        ErrorCode::WriterFenced
    );
    assert_eq!(
        namespace
            .load_current_manifest()
            .await
            .expect("the current manifest")
            .payload()
            .manifest_no,
        manifest_no,
        "the fenced session claims nothing"
    );
}

/// Returns the writer epoch of the namespace's newest manifest.
async fn newest_epoch(local: &LocalFsStore, namespace_id: &NamespaceId) -> WriterEpoch {
    crate::test_support::namespace_store(local, namespace_id)
        .load_current_manifest()
        .await
        .expect("the newest manifest")
        .payload()
        .writer_epoch
}

/// Returns a session whose first commit, `alpha`, has claimed the namespace
/// through `store`, a store over `local`.
async fn session_after_alpha<'s, S: ObjectStore>(
    store: &'s S,
    local: &LocalFsStore,
    namespace_id: &NamespaceId,
    writer: &WriterAt,
) -> NamespaceCommitEngine<&'s S> {
    crate::test_support::NamespaceFixture::new(local, namespace_id, writer)
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(store, namespace_id),
        writer.writer_id.clone(),
    );
    publish_one(&mut engine, writer, "alpha").await;
    engine
}

/// Checks that the session's next publication after a lane item's put whose
/// outcome is unknown claims a new epoch and commits: the session has no
/// epoch and no position, and `beta` is readable with `alpha`.
async fn assert_the_next_publication_claims(
    engine: &mut NamespaceCommitEngine<impl ObjectStore>,
    local: &LocalFsStore,
    namespace_id: &NamespaceId,
    writer: &WriterAt,
) {
    assert_matches!(
        *engine.position.lock_session(),
        WriterSessionStatus::Unacquired,
        "the session has no epoch"
    );
    assert!(
        engine.writer_position().is_none(),
        "the session has no position"
    );
    let before = newest_epoch(local, namespace_id).await;
    publish_one(engine, writer, "beta").await;
    assert!(
        newest_epoch(local, namespace_id).await > before,
        "the session's next commit claims"
    );
    assert_readable(local, namespace_id, &["alpha", "beta"]).await;
}

/// Checks that a fold whose manifest put has an unknown outcome ends the
/// session's epoch and drops its position, and that the session's next
/// publication claims (`writer_lane` `MC_TwoQueuesNoReleaseAroundManifestPut`,
/// `lane_two_folds` `MC_KeepPositionOnUnknown`). A position kept at the
/// fold's starting manifest could be one manifest behind the late put, so a
/// deletion from it would fold WAL objects again.
#[tokio::test]
async fn an_unknown_manifest_outcome_drops_the_epoch_and_the_position_and_the_next_publication_claims(
) {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost manifest acknowledgment".into()),
    );
    let writer = context("writer-a");
    let mut engine = session_after_alpha(&store, &local, &namespace_id, &writer).await;

    store.fail_next(1);
    assert_matches!(
        engine.fold().await.map(|folded| folded.response),
        Err(CoreError::OutcomeUnknown { .. })
    );
    assert_the_next_publication_claims(&mut engine, &local, &namespace_id, &writer).await;
}

/// Checks that a fold whose manifest put succeeds but whose boundary read
/// after the put fails has an unknown outcome, which ends the session's
/// epoch, because the session can't tell whether the manifest counts
/// (`writer_lane` `MC_LoadCompareBoundaryReadKnown`).
#[tokio::test]
async fn a_failed_boundary_read_after_a_written_put_is_an_unknown_outcome() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let store = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(loonfs_objectstore::keys::boundary(&namespace_id)),
        OperationSelector::Read,
        InjectedError::Transport("boundary read failed".into()),
    );
    let writer = context("writer-a");
    let mut engine = session_after_alpha(&store, &local, &namespace_id, &writer).await;
    let folded_no = newest_manifest_no(&local, &namespace_id)
        .await
        .successor()
        .expect("a next manifest number");

    store.fail_next(1);
    assert_matches!(
        engine.fold().await.map(|folded| folded.response),
        Err(CoreError::OutcomeUnknown { .. })
    );
    assert_eq!(
        newest_manifest_no(&local, &namespace_id).await,
        folded_no,
        "the fold's manifest put was written"
    );
    assert_the_next_publication_claims(&mut engine, &local, &namespace_id, &writer).await;
}

/// Returns the number of the namespace's newest manifest.
async fn newest_manifest_no(
    local: &LocalFsStore,
    namespace_id: &NamespaceId,
) -> loonfs_types::ManifestNo {
    crate::test_support::namespace_store(local, namespace_id)
        .load_current_manifest()
        .await
        .expect("the newest manifest")
        .payload()
        .manifest_no
}

/// Checks that a caller that drops a fold's future while the fold's
/// manifest put is in flight leaves the session without its epoch and
/// without a position, because the put can still be written at its number,
/// and that the session's next publication claims (`commit_outcome`
/// `MC_ReleaseOnOutcome`).
#[tokio::test]
async fn a_dropped_manifest_put_future_leaves_the_session_without_its_epoch() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let local = LocalFsStore::new(directory.path()).expect("store");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::PutCreateIfAbsent,
    );
    let writer = context("writer-a");
    let mut engine = session_after_alpha(&store, &local, &namespace_id, &writer).await;

    store.block_next();
    {
        let dropped = engine.fold();
        tokio::select! {
            _ = dropped => panic!("the fold's manifest put is held"),
            () = store.wait_until_blocked() => {}
        }
    }
    store.release();
    assert_the_next_publication_claims(&mut engine, &local, &namespace_id, &writer).await;
}

/// Who folds the tail in
/// [`a_fold_whose_session_claims_before_its_turn_builds_on_the_claim`]
/// between the fold's writes and its turn.
#[derive(Debug, Clone, Copy)]
enum FoldBeforeTurn {
    /// Nobody: the session's own claim is the only manifest in between.
    Nobody,
    /// Another writer claims the namespace and folds the tail first.
    AnotherWriter,
}

/// Checks a fold whose session loses its writer epoch between the fold's
/// writes and its turn, so the turn claims first. With no other writer, the
/// claim keeps the folded WAL number that the fold started from, the fold
/// publishes over its own claim manifest, and a reader sees every commit
/// that was written.
/// When another writer claimed and folded the tail first, the session's
/// claim builds on that fold, and the fold publishes nothing and returns the
/// claim manifest, which covers its tail.
#[tokio::test]
async fn a_fold_whose_session_claims_before_its_turn_builds_on_the_claim() {
    for before in [FoldBeforeTurn::Nobody, FoldBeforeTurn::AnotherWriter] {
        let directory = tempdir().expect("tempdir");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let local = LocalFsStore::new(directory.path()).expect("store");
        let (store, _) = holding_store(&directory, &namespace_id);
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
        let position = engine.writer_position().expect("the position after alpha");
        let clock = writer.clock();
        let crate::manifest::FoldWriteOutcome::Written(writes) = engine
            .namespace
            .write_fold_segments(&position, &clock)
            .await
            .expect("the fold's writes")
        else {
            panic!("the tail has a commit to fold");
        };
        store.fail_next(1);
        let lost = engine
            .publish_batch_alone(vec![create_dir("beta", "beta")], &writer.clock())
            .await;
        assert_matches!(
            lost.results.as_slice(),
            [Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(
                _
            )))]
        );
        if let FoldBeforeTurn::AnotherWriter = before {
            crate::test_support::namespace_store(&local, &namespace_id)
                .fold_as_test_writer()
                .await
                .expect("another writer folds");
        }
        let lane = engine.publication_lane();
        let guard = lane.lock().await;
        let turn = engine.lane_turn(&guard).await.expect("the turn claims");
        let claim = turn.basis().manifest_no();
        let folded = engine
            .namespace
            .publish_fold_in_turn(turn, writes, &clock)
            .await
            .expect("the fold's turn");
        drop(guard);
        match before {
            FoldBeforeTurn::Nobody => {
                assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
                assert_eq!(
                    Some(folded.response.manifest_no),
                    claim.successor().ok(),
                    "the fold publishes over its session's claim"
                );
                // The put of `beta` failed before it reached the store.
                assert_readable(&local, &namespace_id, &["alpha"]).await;
            }
            FoldBeforeTurn::AnotherWriter => {
                assert_eq!(folded.response.outcome, FoldWalOutcome::AlreadyCurrent);
                assert_eq!(folded.response.manifest_no, claim);
                assert_eq!(
                    crate::test_support::namespace_store(&local, &namespace_id)
                        .load_current_manifest()
                        .await
                        .expect("current manifest")
                        .payload()
                        .manifest_no,
                    claim,
                    "the fold publishes nothing"
                );
            }
        }
    }
}
