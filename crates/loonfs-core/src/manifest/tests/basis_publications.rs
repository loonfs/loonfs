//! Manifest publications that start from a basis the caller already has.

use super::*;
use crate::limits::{MANIFEST_PROBE_WINDOW, MAX_CONTENTION_ATTEMPTS, READ_REVALIDATION_BOUND_MS};
use crate::manifest::publish::ManifestChange;
use crate::namespace::control::LoadedManifest;
use crate::namespace::PublicationBasis;
use crate::time::Observation;
use loonfs_test_support::clock::ManualClock;

/// The probe window, as a count of manifest numbers.
const MANIFEST_PROBE_WINDOW_U64: u64 = MANIFEST_PROBE_WINDOW as u64;

/// Creates a namespace whose current manifest is a writer claim at
/// manifest 2, with the hint left at manifest 1 when `hint_lags` and raised
/// to the claim otherwise.
async fn claimed_namespace(
    directory: &std::path::Path,
    hint_lags: bool,
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
        .acquire_writer_epoch(&context)
        .await
        .expect("claim manifest 2");
    if !hint_lags {
        crate::test_support::namespace_store(&store, &namespace_id)
            .raise_hint_to_current_manifest()
            .await;
    }
    store.reset();
    (store, namespace_id)
}

/// Loads the current manifest as a basis checked at `checked`.
async fn basis_checked_at<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checked: Observation,
) -> PublicationBasis {
    PublicationBasis {
        manifest: crate::test_support::namespace_store(store, namespace_id)
            .load_current_manifest()
            .await
            .expect("current manifest"),
        checked,
    }
}

/// Publishes a copy of the current manifest as its successor, starting from
/// `basis`, and returns the published manifest.
async fn republish<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    basis: Option<PublicationBasis>,
    deadline: &Deadline,
) -> LoadedManifest {
    let ((), published, _) = crate::test_support::namespace_store(store, namespace_id)
        .update_manifest(
            basis,
            deadline,
            MAX_CONTENTION_ATTEMPTS,
            |current: LoadedManifest| async move {
                Ok(ManifestChange::Next(
                    Box::new(current.state.envelope.payload().clone()),
                    (),
                ))
            },
        )
        .await
        .expect("publish");
    published
}

/// Returns the manifest objects from `first` through `last`.
fn manifests(namespace_id: &NamespaceId, first: u64, last: u64) -> Vec<String> {
    (first..=last)
        .map(|number| metadata_manifest_object(namespace_id, &ManifestNo(number)))
        .collect()
}

/// Checks that a publication from a basis checked just now sends its
/// manifest put and no request before it, whether the hint points to the
/// basis or lags below it.
#[tokio::test]
async fn a_publication_from_a_fresh_basis_sends_only_its_put() {
    for hint_lags in [false, true] {
        let directory = tempdir().expect("directory");
        let (store, namespace_id) = claimed_namespace(directory.path(), hint_lags).await;
        let basis = basis_checked_at(
            store.inner(),
            &namespace_id,
            crate::test_support::observed_now(),
        )
        .await;
        let deadline = Deadline::start(crate::test_support::std_timer());
        let published = republish(&store, &namespace_id, Some(basis), &deadline).await;
        assert_eq!(published.state.manifest().manifest_no, ManifestNo(3));
        assert_eq!(
            control_requests(&namespace_id, &store.take()),
            vec![(
                "put",
                metadata_manifest_object(&namespace_id, &ManifestNo(3))
            )],
            "the hint lags: {hint_lags}"
        );
    }
}

/// Checks that a publication from a basis checked less than the revalidation
/// bound ago sends only its put, and that a basis checked the bound ago or
/// longer makes the publication discover the current manifest through the
/// hint: the hint, the hinted manifest, a HEAD request for the missing
/// manifest after it, and the hint again.
#[tokio::test]
async fn a_basis_at_the_revalidation_bound_discovers_through_the_hint() {
    for (age_ms, discovers) in [
        (READ_REVALIDATION_BOUND_MS - 1, false),
        (READ_REVALIDATION_BOUND_MS, true),
    ] {
        let directory = tempdir().expect("directory");
        let (store, namespace_id) = claimed_namespace(directory.path(), false).await;
        let clock = Arc::new(ManualClock::new(0));
        let basis = basis_checked_at(
            store.inner(),
            &namespace_id,
            Observation::now(clock.clone()),
        )
        .await;
        clock.advance_ms(age_ms);
        let published =
            republish(&store, &namespace_id, Some(basis), &Deadline::start(clock)).await;
        assert_eq!(published.state.manifest().manifest_no, ManifestNo(3));
        let hint = hint(&namespace_id);
        let manifest = |number| metadata_manifest_object(&namespace_id, &ManifestNo(number));
        let mut expected = Vec::new();
        if discovers {
            expected.extend([
                ("get", hint.clone()),
                ("get", manifest(2)),
                ("head", manifest(3)),
                ("get", hint),
            ]);
        }
        expected.push(("put", manifest(3)));
        assert_eq!(
            control_requests(&namespace_id, &store.take()),
            expected,
            "a basis checked {age_ms} ms ago"
        );
    }
}

/// Checks that a publication whose put loses to two manifests that another
/// process published probes above its basis (one HEAD request for the next
/// number, which exists, and then one window of numbers above that), loads
/// only the newest manifest, publishes on it, and never reads the hint,
/// which still points below the basis.
#[tokio::test]
async fn a_lost_put_probes_above_the_basis_and_publishes_on_the_newest_manifest() {
    let directory = tempdir().expect("directory");
    let (store, namespace_id) = claimed_namespace(directory.path(), true).await;
    let basis = basis_checked_at(
        store.inner(),
        &namespace_id,
        crate::test_support::observed_now(),
    )
    .await;
    let manifest = |number| metadata_manifest_object(&namespace_id, &ManifestNo(number));
    let gate = BlockingStore::new(
        store,
        KeyPredicate::exact(manifest(3)),
        OperationSelector::PutCreateIfAbsent,
    );
    gate.block_next();
    let other = LocalFsStore::new(directory.path()).expect("store");
    let deadline = Deadline::start(crate::test_support::std_timer());
    let (published, ()) = tokio::join!(
        republish(&gate, &namespace_id, Some(basis), &deadline),
        async {
            gate.wait_until_blocked().await;
            for _ in 0..2 {
                crate::test_support::NamespaceFixture::new(
                    &other,
                    &namespace_id,
                    &crate::test_support::mutation_context("test-writer", 0),
                )
                .engine()
                .claim_compactor()
                .await
                .expect("another process claims the compactor");
            }
            gate.release();
        }
    );
    assert_eq!(published.state.manifest().manifest_no, ManifestNo(5));
    assert_eq!(
        published.state.compactor_epoch(),
        loonfs_types::CompactorEpoch(2),
        "the publication builds on the newest claim"
    );
    let mut expected = vec![("put", manifest(3))];
    // The put lost, so the attempt probes the numbers above its basis, and
    // loads the newest manifest that the probe found.
    expected.extend(
        manifests(&namespace_id, 3, 3 + MANIFEST_PROBE_WINDOW_U64)
            .into_iter()
            .map(|key| ("head", key)),
    );
    expected.push(("get", manifest(4)));
    // The next attempt builds on that manifest without a request first.
    expected.push(("put", manifest(5)));
    assert_eq!(
        control_requests(&namespace_id, &gate.inner().take()),
        expected
    );
}
