//! Manifest publications that start from a basis the caller already has.

use super::*;
use crate::limits::{MAX_CONTENTION_ATTEMPTS, READ_REVALIDATION_BOUND_MS};
use crate::manifest::claim_compactor;
use crate::manifest::publish::{update_manifest, ManifestChange};
use crate::namespace::control::LoadedManifest;
use crate::namespace::PublicationBasis;
use crate::time::Observation;
use loonfs_test_support::clock::ManualClock;

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
    create(&store, &namespace_id, &context)
        .await
        .expect("create");
    acquire_writer_epoch(&store, &namespace_id, &context)
        .await
        .expect("claim manifest 2");
    if !hint_lags {
        crate::test_support::raise_hint_to_current_manifest(&store, &namespace_id).await;
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
        manifest: load_current_manifest(store, namespace_id)
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
    let ((), published, _) = update_manifest(
        store,
        namespace_id,
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

/// Checks that a publication from a basis checked just now sends one HEAD of
/// the basis's successor, then its manifest put, and reads neither the hint
/// nor a manifest, whether the hint points to the basis or lags below it.
#[tokio::test]
async fn a_publication_from_a_fresh_basis_sends_one_head_and_no_hint_read() {
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
        let manifest = |number| metadata_manifest_object(&namespace_id, &ManifestNo(number));
        assert_eq!(
            control_requests(&namespace_id, &store.take()),
            vec![("head", manifest(3)), ("put", manifest(3))],
            "the hint lags: {hint_lags}"
        );
    }
}

/// Checks that a publication from a basis checked less than the revalidation
/// bound ago finds the current manifest with one HEAD, and that a basis
/// checked the bound ago or longer makes the publication load the current
/// manifest through the hint.
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
        let mut expected = match discovers {
            true => vec![
                ("get", hint.clone()),
                ("get", manifest(2)),
                ("get", manifest(3)),
                ("get", hint),
            ],
            false => vec![("head", manifest(3))],
        };
        expected.push(("put", manifest(3)));
        assert_eq!(
            control_requests(&namespace_id, &store.take()),
            expected,
            "a basis checked {age_ms} ms ago"
        );
    }
}

/// Checks that a publication whose put loses to two manifests that another
/// process published loads each manifest above its basis, publishes on the
/// newest one, and never reads the hint, which still points below the basis.
#[tokio::test]
async fn a_lost_put_walks_from_the_basis_and_publishes_on_the_current_manifest() {
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
                claim_compactor(&other, &namespace_id, crate::test_support::std_timer())
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
    assert_eq!(
        control_requests(&namespace_id, &gate.inner().take()),
        vec![
            ("head", manifest(3)),
            ("put", manifest(3)),
            // The put lost, so the attempt loads the manifests above its
            // basis until one is missing.
            ("get", manifest(3)),
            ("get", manifest(4)),
            ("get", manifest(5)),
            // The next attempt starts from the newest of them.
            ("head", manifest(5)),
            ("put", manifest(5)),
        ]
    );
}
