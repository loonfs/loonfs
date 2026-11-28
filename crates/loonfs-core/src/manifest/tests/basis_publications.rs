//! Manifest publications that start from a basis the caller already has:
//! the items of a writer session's publication lane, which build on the
//! session's newest published manifest and put once.

use super::*;
use crate::limits::{MANIFEST_PROBE_WINDOW, READ_REVALIDATION_BOUND_MS};
use crate::namespace::control::LoadedManifest;
use crate::namespace::held::Held;
use loonfs_test_support::clock::ManualClock;

/// The probe window, as a count of manifest numbers.
const MANIFEST_PROBE_WINDOW_U64: u64 = MANIFEST_PROBE_WINDOW as u64;

/// Creates a namespace whose current manifest is the create's active
/// manifest 2.
async fn created_namespace(
    directory: &std::path::Path,
) -> (RecordingStore<LocalFsStore>, NamespaceId) {
    let namespace_id = NamespaceId::parse("basis-publication").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory).expect("store"),
        KeyPredicate::any(),
    );
    let context = test_context();
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("create");
    store.reset();
    (store, namespace_id)
}

/// Loads the current manifest as a basis, with the manifest boundary.
async fn current_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> Held<LoadedManifest> {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_basis(crate::test_support::observed_now())
        .await
        .expect("current manifest")
}

/// Publishes a copy of `basis` as its successor, as the one item of a
/// publication lane whose session has `basis`'s writer epoch and `basis` as
/// its newest published manifest, and returns the publication's result and
/// whether the session is fenced after it.
async fn republish<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: Held<LoadedManifest>,
    deadline: &OperationClock,
) -> (crate::Result<Held<LoadedManifest>>, bool) {
    let payload = basis.manifest().payload().clone();
    let lane = crate::test_support::lane_at_manifest(basis);
    let published = crate::test_support::namespace_store(store, namespace_id)
        .publish_manifest_in_turn(lane.lock().await.acquired_turn(), payload, deadline)
        .await;
    let fenced = matches!(
        *lane.published_manifest().lock(),
        crate::publish::WriterSessionStatus::Fenced(_)
    );
    (published, fenced)
}

/// Returns the manifest objects from `first` through `last`.
fn manifests(namespace_id: &NamespaceId, first: u64, last: u64) -> Vec<String> {
    (first..=last)
        .map(|number| metadata_manifest_object(namespace_id, &ManifestNo(number)))
        .collect()
}

/// Who wins the put of a publication of the lane in
/// [`a_lost_manifest_put_returns_writer_fenced_with_the_newer_writer`].
#[derive(Debug, Clone, Copy)]
enum Winner {
    /// Another writer's claim, with the next writer epoch.
    Claim,
    /// A manifest of the session's own writer epoch, which no other process
    /// publishes.
    SameEpoch,
}

/// Returns a retention window that differs from every earlier one in a
/// test, so each change publishes a manifest.
fn retention_window(extra_ms: u64) -> Option<loonfs_types::RetentionWindow> {
    loonfs_types::RetentionWindow::new(2 * crate::limits::GC_GRACE_WINDOW_MS + extra_ms).ok()
}

/// Checks that a publication of the lane whose put loses returns `writer_fenced` with the
/// newer writer's writer block when another writer's claim won, records the
/// session as fenced, and builds nothing again (`writer_lane`
/// `MC_LostPutFencedWitness`), and that a winner with the session's own
/// epoch returns `namespace_corrupt`. After the lost put, the item probes
/// above its basis (one HEAD request for the next number, which exists, and
/// then one window of numbers above that), loads the newest manifest, reads
/// the manifest boundary, and puts nothing more.
#[tokio::test]
async fn a_lost_manifest_put_returns_writer_fenced_with_the_newer_writer() {
    for winner in [Winner::Claim, Winner::SameEpoch] {
        let directory = tempdir().expect("directory");
        let (store, namespace_id) = created_namespace(directory.path()).await;
        let mut engine = NamespaceCommitEngine::new(
            crate::test_support::namespace_store(&store, &namespace_id),
            test_context().writer_id,
        );
        // The session claims manifest 3 and writes its fence.
        engine
            .load_position_alone()
            .await
            .expect("the session claims");
        let other = crate::test_support::namespace_store(store.inner(), &namespace_id);
        match winner {
            Winner::Claim => {
                other
                    .acquire_writer_epoch(&crate::test_support::writer_at("b", 0).writer_id)
                    .await
                    .expect("another writer claims");
            }
            Winner::SameEpoch => {
                other
                    .fixture_writer_session()
                    .await
                    .expect("a session of the writer's epoch")
                    .set_retention_window(retention_window(1))
                    .await
                    .expect("a manifest of the session's epoch");
            }
        }
        store.reset();
        let published = engine.set_retention_window(retention_window(0)).await;
        let status = engine.session_status();
        match winner {
            Winner::Claim => {
                let Err(CoreError::WriterFenced(fence)) = published else {
                    panic!("expected writer_fenced, got {published:?}");
                };
                assert_eq!(fence.active_writer.writer_id.as_str(), "b");
                assert_eq!(fence.active_epoch, loonfs_types::WriterEpoch(2));
                assert_matches!(
                    *status.lock(),
                    crate::publish::WriterSessionStatus::Fenced(_)
                );
            }
            Winner::SameEpoch => {
                assert_matches!(published, Err(CoreError::NamespaceCorrupt(_)));
                assert_matches!(
                    *status.lock(),
                    crate::publish::WriterSessionStatus::Acquired { .. }
                );
            }
        }
        let manifest = |number| metadata_manifest_object(&namespace_id, &ManifestNo(number));
        let mut expected = vec![("put", manifest(4))];
        expected.extend(
            manifests(&namespace_id, 4, 4 + MANIFEST_PROBE_WINDOW_U64)
                .into_iter()
                .map(|key| ("head", key)),
        );
        expected.extend([("get", manifest(4)), ("get", boundary(&namespace_id))]);
        assert_eq!(
            control_requests(&namespace_id, &store.take()),
            expected,
            "{winner:?}"
        );
    }
}

/// Checks that a publication of the lane whose put loses, and whose read of
/// the manifest that won fails, returns that read's error and leaves the
/// session its epoch, and that the lane's next publication puts at the same
/// number, loses, reads the winner, and returns `writer_fenced` (see
/// `publish_manifest_in_turn`).
#[tokio::test]
async fn a_lost_put_whose_read_of_the_winner_fails_leaves_the_next_publication_to_read_it() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("basis-publication").expect("namespace");
    let winner = metadata_manifest_object(&namespace_id, &ManifestNo(4));
    let store = RecordingStore::new(
        FailStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::exact(winner.clone()),
            OperationSelector::Read,
            InjectedError::Transport("manifest read failed".to_owned()),
        ),
        KeyPredicate::any(),
    );
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .create()
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        test_context().writer_id,
    );
    // The session claims manifest 3, and another writer's claim wins 4.
    engine
        .load_position_alone()
        .await
        .expect("the session claims");
    crate::test_support::namespace_store(store.inner().inner(), &namespace_id)
        .acquire_writer_epoch(&crate::test_support::writer_at("b", 0).writer_id)
        .await
        .expect("another writer claims");
    store.inner().fail_next(1);
    store.reset();
    let failed = engine.set_retention_window(retention_window(0)).await;
    assert_matches!(failed, Err(CoreError::ControlObjectLoad(_)));
    assert_eq!(store.inner().remaining(), 0);
    assert_matches!(
        *engine.session_status().lock(),
        crate::publish::WriterSessionStatus::Acquired { .. }
    );
    let first = control_requests(&namespace_id, &store.take());
    assert_eq!(first.first(), Some(&("put", winner.clone())), "{first:?}");
    let Err(CoreError::WriterFenced(fence)) =
        engine.set_retention_window(retention_window(1)).await
    else {
        panic!("the next publication should find the claim");
    };
    assert_eq!(fence.active_writer.writer_id.as_str(), "b");
    assert_matches!(
        *engine.session_status().lock(),
        crate::publish::WriterSessionStatus::Fenced(_)
    );
    let second = control_requests(&namespace_id, &store.take());
    assert_eq!(second.first(), Some(&("put", winner)), "{second:?}");
}

/// Checks that a window change and an extension's entry of a session with
/// its writer epoch send no request before their put, one put, and one read
/// of the manifest boundary after it: each builds on the session's newest
/// published manifest, which the session has in memory.
#[tokio::test]
async fn a_window_change_and_an_extension_entry_put_with_no_request_first() {
    let directory = tempdir().expect("directory");
    let (store, namespace_id) = created_namespace(directory.path()).await;
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        test_context().writer_id,
    );
    engine
        .load_position_alone()
        .await
        .expect("the session claims");
    let basis = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &test_context())
        .engine()
        .load_extension_basis()
        .await
        .expect("the extension's basis");
    let manifest = |number| metadata_manifest_object(&namespace_id, &ManifestNo(number));
    store.reset();
    engine
        .set_retention_window(retention_window(0))
        .await
        .expect("change the window");
    assert_eq!(
        control_requests(&namespace_id, &store.take()),
        [("put", manifest(4)), ("get", boundary(&namespace_id))]
    );
    let published = engine
        .publish_extension(
            &basis,
            &loonfs_types::ExtensionName::parse("grep").expect("name"),
            loonfs_types::format::manifest::ExtensionEntry::new(
                1,
                Vec::new(),
                serde_json::Map::new(),
            ),
        )
        .await
        .expect("publish the entry");
    assert_matches!(
        published.response,
        crate::ExtensionPublication::Published { .. }
    );
    assert_eq!(
        control_requests(&namespace_id, &store.take()),
        [("put", manifest(5)), ("get", boundary(&namespace_id))]
    );
}

/// Checks that a publication of the lane from a basis loaded more than the revalidation
/// bound ago sends its manifest put with no request before it, and one read
/// of the manifest boundary after it.
#[tokio::test]
async fn a_publication_from_an_old_basis_puts_with_no_request_first() {
    let directory = tempdir().expect("directory");
    let (store, namespace_id) = created_namespace(directory.path()).await;
    crate::test_support::namespace_store(store.inner(), &namespace_id)
        .raise_boundary(ManifestNo(1))
        .await
        .expect("raise the boundary");
    let basis = current_basis(store.inner(), &namespace_id).await;
    let clock = Arc::new(ManualClock::new(0));
    clock.advance_ms(READ_REVALIDATION_BOUND_MS + 1);
    let deadline = OperationClock::start(0, clock);
    let (published, _) = republish(&store, &namespace_id, basis, &deadline).await;
    assert_eq!(published.expect("publish").manifest_no(), ManifestNo(3));
    assert_eq!(
        control_requests(&namespace_id, &store.take()),
        vec![
            (
                "put",
                metadata_manifest_object(&namespace_id, &ManifestNo(3))
            ),
            ("get", boundary(&namespace_id)),
        ]
    );
}

/// Checks that a publication whose manifest breaks a successor rule (here, a
/// changed pool) panics before its put, so the store has no manifest at that
/// number.
#[tokio::test]
async fn a_publication_that_breaks_a_successor_rule_panics_before_its_put() {
    use futures::FutureExt;
    let directory = tempdir().expect("directory");
    let (store, namespace_id) = created_namespace(directory.path()).await;
    let basis = current_basis(store.inner(), &namespace_id).await;
    let deadline = OperationClock::start(0, crate::test_support::std_timer());
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let payload = NamespaceManifestPayload {
        pool_id: loonfs_types::PoolId::generate(),
        ..basis.manifest().payload().clone()
    };
    let Err(panic) = std::panic::AssertUnwindSafe(namespace.publish_manifest_from(
        payload,
        &deadline,
        Some(&basis),
    ))
    .catch_unwind()
    .await
    else {
        panic!("a manifest that changes the pool should panic");
    };
    let message = panic
        .downcast_ref::<String>()
        .expect("the assertion's message");
    assert!(
        message.starts_with("a candidate manifest should be a legal successor")
            && message.contains("pool_id"),
        "{message}"
    );
    assert_eq!(control_requests(&namespace_id, &store.take()), Vec::new());
    assert!(store
        .inner()
        .head(&metadata_manifest_object(&namespace_id, &ManifestNo(3)))
        .await
        .expect("head")
        .is_none());
}
