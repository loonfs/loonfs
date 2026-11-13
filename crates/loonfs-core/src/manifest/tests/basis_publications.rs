//! Manifest publications that start from a basis the caller already has.

use super::super::publish::{ManifestChange, ManifestUpdate};
use super::*;
use crate::limits::{MANIFEST_PROBE_WINDOW, READ_REVALIDATION_BOUND_MS};
use crate::namespace::control::LoadedManifest;
use crate::namespace::PublicationBasis;
use loonfs_test_support::clock::ManualClock;

/// The probe window, as a count of manifest numbers.
const MANIFEST_PROBE_WINDOW_U64: u64 = MANIFEST_PROBE_WINDOW as u64;

/// Creates a namespace whose current manifest is a writer claim at
/// manifest 2.
async fn claimed_namespace(
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
    crate::test_support::namespace_store(&store, &namespace_id)
        .acquire_writer_epoch(&context.writer_id)
        .await
        .expect("claim manifest 2");
    store.reset();
    (store, namespace_id)
}

/// Loads the current manifest as a basis, with the manifest boundary.
async fn current_basis<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> PublicationBasis {
    crate::test_support::namespace_store(store, namespace_id)
        .load_current_basis()
        .await
        .expect("current manifest")
}

/// Publishes a copy of the current manifest as its successor, starting from
/// `basis`, and returns the published manifest.
async fn republish<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: Option<PublicationBasis>,
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
    published.manifest
}

/// Returns the manifest objects from `first` through `last`.
fn manifests(namespace_id: &NamespaceId, first: u64, last: u64) -> Vec<String> {
    (first..=last)
        .map(|number| metadata_manifest_object(namespace_id, &ManifestNo(number)))
        .collect()
}

/// Checks that a publication from a basis loaded more than the revalidation
/// bound ago sends its manifest put with no request before it, and one read
/// of the manifest boundary after it.
#[tokio::test]
async fn a_publication_from_an_old_basis_puts_with_no_request_first() {
    let directory = tempdir().expect("directory");
    let (store, namespace_id) = claimed_namespace(directory.path()).await;
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

/// Checks that a publication whose put loses to two manifests that another
/// process published probes above its basis (one HEAD request for the next
/// number, which exists, and then one window of numbers above that), loads
/// only the newest manifest, reads the manifest boundary, and publishes on
/// it.
#[tokio::test]
async fn a_lost_put_probes_above_the_basis_and_publishes_on_the_newest_manifest() {
    let directory = tempdir().expect("directory");
    let (store, namespace_id) = claimed_namespace(directory.path()).await;
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
    let (published, ()) = tokio::join!(
        republish(&gate, &namespace_id, Some(basis), &deadline),
        async {
            gate.wait_until_blocked().await;
            for _ in 0..2 {
                crate::test_support::NamespaceFixture::new(
                    &other,
                    &namespace_id,
                    &crate::test_support::writer_at("test-writer", 0),
                )
                .engine()
                .claim_compactor()
                .await
                .expect("another process claims the compactor");
            }
            gate.release();
        }
    );
    assert_eq!(published.manifest().manifest_no, ManifestNo(5));
    assert_eq!(
        published.compactor_epoch(),
        loonfs_types::CompactorEpoch(2),
        "the publication builds on the newest claim"
    );
    let mut expected = vec![("put", manifest(3))];
    // The put lost, so the attempt probes the numbers above its basis, loads
    // the newest manifest that the probe found, and reads the boundary.
    expected.extend(
        manifests(&namespace_id, 3, 3 + MANIFEST_PROBE_WINDOW_U64)
            .into_iter()
            .map(|key| ("head", key)),
    );
    expected.extend([("get", manifest(4)), ("get", boundary(&namespace_id))]);
    // The next attempt builds on that manifest without a request first.
    expected.extend([("put", manifest(5)), ("get", boundary(&namespace_id))]);
    assert_eq!(
        control_requests(&namespace_id, &gate.inner().take()),
        expected
    );
}
