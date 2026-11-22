//! A writer session after a WAL put whose outcome is unknown.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::manifest::FoldWalOutcome;
use crate::wal::WalPublishError;
use bytes::Bytes;
use loonfs_objectstore::PutMode;
use loonfs_test_support::stores::{FailStore, InjectedError, MetadataMapStore, OperationKind};
use loonfs_types::options::AttributeInclusion;
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

/// Checks that a writer session whose WAL put has an unknown outcome writes
/// a fence with its epoch at its next publication, so its own put can't
/// leave an emptied number at the session's next WAL number (format spec,
/// "Publication and group commit").
///
/// The put of `beta` at WAL 3 loses its acknowledgment and stays in flight
/// at the provider. The session's next publication, of `gamma`, stalls before
/// its first WAL put. Meanwhile the put of `beta` is written at 3, a fold covers
/// 3, and a collection that runs after the grace window deletes the commit
/// object at 3. A session that planned `gamma` from a fresh discovery would
/// plan it at 3, and its put there would succeed and acknowledge a commit
/// that no reader replays. A session that owes a fence puts the fence first,
/// and the check after that put finds the number emptied.
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
        .publish_batch(vec![create_dir("beta", "beta")], &writer.clock())
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
    gamma.expect("gamma commits");
    assert_readable(&local, &namespace_id, &["alpha", "beta", "gamma"]).await;
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
    let stalled = engine.publish_batch(vec![create_dir(name, name)], &clock);
    let meanwhile = async {
        store.inner().wait_until_blocked().await;
        local
            .put_if_absent(&in_flight_key, in_flight_bytes)
            .await
            .expect("the put in flight is written");
        assert_eq!(
            crate::test_support::namespace_store(local, &namespace_id)
                .fold_wal()
                .await
                .expect("fold")
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
/// WAL put is in flight leaves the session owing a fence, so the put can't
/// be written at a number that the session's next commit uses after a
/// collection empties it.
///
/// The put of `beta` is held before the store sees it, and the caller drops
/// the future. The session's next publication, of `gamma`, stalls before its
/// first WAL put while the put of `beta` is written at 3, folded and
/// collected. A session that didn't owe a fence would plan `gamma` at 3 and
/// acknowledge a commit that no reader replays.
#[tokio::test]
async fn a_dropped_publish_future_leaves_the_session_owing_a_fence() {
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
        let dropped = engine.publish_batch(vec![create_dir("beta", "beta")], &clock);
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
    gamma.expect("gamma commits");
    assert_readable(&local, &namespace_id, &["alpha", "beta", "gamma"]).await;
}

/// Checks that a lone writer session whose fence put after an unknown
/// outcome also loses its acknowledgment isn't fenced by itself. Its next
/// publication puts a fence again, and its commit is readable.
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
        .publish_batch(vec![create_dir("beta", "beta")], &writer.clock())
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
        .publish_batch(vec![create_dir("gamma", "gamma")], &writer.clock())
        .await;
    assert_eq!(
        fence_lost.results[0]
            .as_ref()
            .expect_err("the fence's outcome is unknown")
            .code(),
        ErrorCode::OutcomeUnknown
    );
    let gamma = engine
        .publish_batch(vec![create_dir("gamma", "gamma")], &writer.clock())
        .await;
    gamma.results[0]
        .as_ref()
        .expect("the lone writer commits gamma");
    assert_readable(&local, &namespace_id, &["alpha", "gamma"]).await;
}

/// Checks that a session's floor object put with an unknown outcome leaves
/// the session owing a fence too, because that put also skips the check
/// after the put (format spec, "Advancing the retention floor").
#[tokio::test]
async fn an_unknown_floor_object_put_leaves_the_session_owing_a_fence() {
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
    let planned = engine
        .namespace
        .load_current_manifest()
        .await
        .expect("the planned manifest");

    store.fail_next(1);
    assert_matches!(
        engine
            .publish_floor_object(&planned, CommitSeq(1), &writer.clock())
            .await,
        Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(_)))
    );
    assert!(engine.writer_position().is_none());
    assert_matches!(
        *engine.position.lock_session(),
        WriterSessionStatus::HoldsEpochOwesFence(_)
    );
}

/// Checks that a session whose WAL put had an unknown outcome is fenced by a
/// writer that took the namespace over in the meantime. The tail read of
/// its fence finds the higher epoch, and its next publication returns
/// `writer_fenced` and writes no manifest and no WAL object (format spec,
/// "Writer ownership").
#[tokio::test]
async fn a_session_whose_put_was_unknown_is_fenced_by_a_takeover() {
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
        .publish_batch(vec![create_dir("beta", "beta")], &writer.clock())
        .await;
    assert_matches!(
        lost.results.as_slice(),
        [Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(
            _
        )))]
    );
    let namespace = crate::test_support::namespace_store(&local, &namespace_id);
    namespace
        .acquire_writer_epoch(&context("writer-b").writer_id)
        .await
        .expect("writer b takes over");
    let manifest_no = namespace
        .load_current_manifest()
        .await
        .expect("the takeover's manifest")
        .payload()
        .manifest_no;
    let wal_objects = wal_object_count(&local, &namespace_id).await;

    let fenced = engine
        .publish_batch(vec![create_dir("gamma", "gamma")], &writer.clock())
        .await;
    assert_eq!(
        fenced.results[0]
            .as_ref()
            .expect_err("writer b holds the namespace")
            .code(),
        ErrorCode::WriterFenced
    );
    assert_matches!(
        *engine.position.lock_session(),
        WriterSessionStatus::Fenced(_)
    );
    assert_eq!(
        namespace
            .load_current_manifest()
            .await
            .expect("the current manifest")
            .payload()
            .manifest_no,
        manifest_no,
        "the session publishes no manifest"
    );
    assert_eq!(
        wal_object_count(&local, &namespace_id).await,
        wal_objects,
        "the session writes no WAL object"
    );
}
