//! Manifest publications that start from a basis the caller already has.

use super::super::publish::{ManifestChange, ManifestUpdate};
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

/// Publishes a copy of the current manifest as its successor, starting from
/// `basis`, and returns the published manifest.
async fn republish<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: Option<Held<LoadedManifest>>,
    deadline: &OperationClock,
) -> LoadedManifest {
    let ManifestUpdate {
        current: published, ..
    } = crate::test_support::namespace_store(store, namespace_id)
        .update_manifest(basis, deadline, |current: LoadedManifest| async move {
            Ok(ManifestChange::Next(
                Box::new(current.payload().clone()),
                (),
            ))
        })
        .await
        .expect("publish");
    published.into_manifest()
}

/// Returns the manifest objects from `first` through `last`.
fn manifests(namespace_id: &NamespaceId, first: u64, last: u64) -> Vec<String> {
    (first..=last)
        .map(|number| metadata_manifest_object(namespace_id, &ManifestNo(number)))
        .collect()
}

/// Checks that a publication whose put loses to three manifests that the
/// test support's same-epoch publisher puts (its claim of the new namespace
/// and two window changes, as the session's own publications), probes above
/// its basis (one HEAD request for the next
/// number, which exists, and then one window of numbers above that), loads
/// only the newest manifest, reads the manifest boundary, and publishes on
/// it.
#[tokio::test]
async fn a_lost_put_probes_above_the_basis_and_publishes_on_the_newest_manifest() {
    let directory = tempdir().expect("directory");
    let (store, namespace_id) = created_namespace(directory.path()).await;
    let basis = current_basis(store.inner(), &namespace_id).await;
    let manifest = |number| metadata_manifest_object(&namespace_id, &ManifestNo(number));
    let gate = BlockingStore::new(
        store,
        KeyPredicate::exact(manifest(3)),
        OperationSelector::PutCreateIfAbsent,
    );
    gate.block_next();
    let other = LocalFsStore::new(directory.path()).expect("store");
    let deadline = OperationClock::start(0, crate::test_support::std_timer());
    let (published, newest) = tokio::join!(
        republish(&gate, &namespace_id, Some(basis), &deadline),
        async {
            gate.wait_until_blocked().await;
            let mut newest = None;
            for _ in 0..2 {
                newest = Some(
                    crate::test_support::change_retention_window(
                        &crate::test_support::NamespaceFixture::new(
                            &other,
                            &namespace_id,
                            &crate::test_support::writer_at("test-writer", 0),
                        )
                        .engine(),
                    )
                    .await,
                );
            }
            gate.release();
            newest.expect("two window changes")
        }
    );
    assert_eq!(published.manifest().manifest_no, ManifestNo(6));
    assert_eq!(
        published.payload().retention_window_ms,
        newest.manifest().payload().retention_window_ms,
        "the publication builds on the newest window change"
    );
    let mut expected = vec![("put", manifest(3))];
    // The put lost, so the attempt probes the numbers above its basis, loads
    // the newest manifest that the probe found, and reads the boundary.
    expected.extend(
        manifests(&namespace_id, 3, 3 + MANIFEST_PROBE_WINDOW_U64)
            .into_iter()
            .map(|key| ("head", key)),
    );
    expected.extend([("get", manifest(5)), ("get", boundary(&namespace_id))]);
    // The next attempt builds on that manifest without a request first.
    expected.extend([("put", manifest(6)), ("get", boundary(&namespace_id))]);
    assert_eq!(
        control_requests(&namespace_id, &gate.inner().take()),
        expected
    );
}

/// Checks that a publication from a basis loaded more than the revalidation
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
    let published = republish(&store, &namespace_id, Some(basis), &deadline).await;
    assert_eq!(published.manifest().manifest_no, ManifestNo(3));
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
    let Err(panic) = std::panic::AssertUnwindSafe(namespace.update_manifest(
        Some(basis),
        &deadline,
        |current: LoadedManifest| async move {
            Ok(ManifestChange::Next(
                Box::new(NamespaceManifestPayload {
                    pool_id: loonfs_types::PoolId::generate(),
                    ..current.payload().clone()
                }),
                (),
            ))
        },
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
