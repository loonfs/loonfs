//! Snapshot expiry must include time spent loading and retrying its record.

use super::*;
use crate::snapshot::lifecycle::ensure_live;
use loonfs_objectstore::keys;
use loonfs_test_support::clock::ManualClock;
use loonfs_types::format::control::SnapshotOwner;
use std::assert_matches;

#[tokio::test]
async fn snapshot_expiry_after_the_renewal_cas_starts_preserves_success() {
    for lose_ack in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("snapshot-ack").expect("namespace");
        let inner = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let context = writer_at("writer", 1_000);
        bootstrap_namespace(&inner, &namespace_id, &context)
            .await
            .expect("bootstrap");
        let namespace = crate::test_support::namespace_store(&inner, &namespace_id);
        let snapshot = namespace
            .create_snapshot_record(
                SnapshotOwner::User {
                    name: Some("short lease".to_owned()),
                    expires_at_ms: Some(1_001),
                    deleted_at_ms: None,
                },
                crate::test_support::folded_manifest(&namespace)
                    .await
                    .expect("fold the WAL tail"),
                &context.clock(),
            )
            .await
            .map(|(record, _)| record)
            .expect("snapshot");
        let key = keys::snapshot_record(&namespace_id, &snapshot.snapshot_id);
        let failing = FailStore::new(
            inner.clone(),
            KeyPredicate::exact(&key),
            OperationSelector::CompareAndSwap,
            InjectedError::Transport("lost renewal acknowledgement".to_owned()),
        )
        .apply_then_fail();
        if lose_ack {
            failing.fail_next(1);
        }
        let store = BlockingStore::new(
            failing,
            KeyPredicate::exact(&key),
            OperationSelector::CompareAndSwap,
        );
        let clock = Arc::new(ManualClock::new(context.now_ms));
        store.block_next();
        let namespace = {
            let mut fixture =
                crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
            fixture.namespace.timer = clock.clone();
            fixture
        }
        .engine();
        let administrator = crate::test_support::administrator(&namespace);
        let live = crate::test_support::read_context(&inner, &namespace_id).await;
        let renew = namespace.extend_snapshot(&administrator, &live, &snapshot.snapshot_id, 1_002);
        let expire = async {
            store.wait_until_blocked().await;
            clock.advance_ms(3);
            store.release();
        };
        let (result, ()) = futures::join!(renew, expire);
        assert_eq!(
            result.expect("the extension succeeded").expires_at_ms,
            Some(1_002)
        );
        let stored = crate::test_support::namespace_store(&inner, &namespace_id)
            .load_snapshot_record(&snapshot.snapshot_id)
            .await
            .expect("load");
        assert_eq!(
            stored
                .as_ref()
                .expect("snapshot")
                .state
                .owner
                .expires_at_ms(),
            Some(1_002)
        );
        assert_eq!(
            ensure_live(stored.expect("snapshot"), clock.now_ms())
                .expect_err("later reads see expiry")
                .code(),
            ErrorCode::SnapshotNotFound
        );
    }
}

#[tokio::test]
async fn snapshot_expiring_during_renewal_load_cannot_be_extended_or_reported_live() {
    for requested_expiry in [10_000, 1_001] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("snapshot-renewal").expect("namespace");
        let inner = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let context = writer_at("writer", 1_000);
        bootstrap_namespace(&inner, &namespace_id, &context)
            .await
            .expect("bootstrap");
        let namespace = crate::test_support::namespace_store(&inner, &namespace_id);
        let snapshot = namespace
            .create_snapshot_record(
                SnapshotOwner::User {
                    name: Some("short lease".to_owned()),
                    expires_at_ms: Some(1_001),
                    deleted_at_ms: None,
                },
                crate::test_support::folded_manifest(&namespace)
                    .await
                    .expect("fold the WAL tail"),
                &context.clock(),
            )
            .await
            .map(|(record, _)| record)
            .expect("snapshot");
        let store = BlockingStore::new(
            inner.clone(),
            KeyPredicate::exact(keys::snapshot_record(&namespace_id, &snapshot.snapshot_id)),
            OperationSelector::Read,
        );
        let clock = Arc::new(ManualClock::new(context.now_ms));
        store.block_next();
        let namespace = {
            let mut fixture =
                crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
            fixture.namespace.timer = clock.clone();
            fixture
        }
        .engine();
        let administrator = crate::test_support::administrator(&namespace);
        let live = crate::test_support::read_context(&inner, &namespace_id).await;
        let renew = namespace.extend_snapshot(
            &administrator,
            &live,
            &snapshot.snapshot_id,
            requested_expiry,
        );
        let expire = async {
            store.wait_until_blocked().await;
            clock.advance_ms(2);
            let error = ensure_live(
                crate::test_support::namespace_store(&inner, &namespace_id)
                    .load_snapshot_record(&snapshot.snapshot_id)
                    .await
                    .expect("load")
                    .expect("snapshot"),
                clock.now_ms(),
            )
            .expect_err("an independent reader observes expiry");
            assert_eq!(error.code(), ErrorCode::SnapshotNotFound);
            store.release();
        };
        let (result, ()) = futures::join!(renew, expire);
        assert_eq!(
            result.expect_err("expired snapshot must not renew").code(),
            ErrorCode::SnapshotNotFound
        );
        let stored = crate::test_support::namespace_store(&inner, &namespace_id)
            .load_snapshot_record(&snapshot.snapshot_id)
            .await
            .expect("load")
            .expect("retained expired snapshot");
        assert_eq!(stored.state.owner.expires_at_ms(), Some(1_001));
    }
}

#[tokio::test]
async fn snapshot_renewal_contention_does_not_restart_the_expiry_clock() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("snapshot-retry").expect("namespace");
    let inner = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let context = writer_at("writer", 1_000);
    bootstrap_namespace(&inner, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let namespace = crate::test_support::namespace_store(&inner, &namespace_id);
    let snapshot = namespace
        .create_snapshot_record(
            SnapshotOwner::User {
                name: Some("short lease".to_owned()),
                expires_at_ms: Some(1_001),
                deleted_at_ms: None,
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context.clock(),
        )
        .await
        .map(|(record, _)| record)
        .expect("snapshot");
    let store = BlockingStore::new(
        inner.clone(),
        KeyPredicate::exact(keys::snapshot_record(&namespace_id, &snapshot.snapshot_id)),
        OperationSelector::CompareAndSwap,
    );
    let clock = Arc::new(ManualClock::new(context.now_ms));
    store.block_next();
    let namespace = {
        let mut fixture =
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
        fixture.namespace.timer = clock.clone();
        fixture
    }
    .engine();
    let administrator = crate::test_support::administrator(&namespace);
    let live = crate::test_support::read_context(&inner, &namespace_id).await;
    let renew = namespace.extend_snapshot(&administrator, &live, &snapshot.snapshot_id, 10_000);
    let contender = async {
        store.wait_until_blocked().await;
        {
            let mut fixture =
                crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context);
            fixture.namespace.timer = clock.clone();
            fixture
        }
        .engine()
        .extend_snapshot(
            &crate::test_support::administrator_of(&inner, &namespace_id),
            &crate::test_support::read_context(&inner, &namespace_id).await,
            &snapshot.snapshot_id,
            1_002,
        )
        .await
        .expect("competing extension");
        clock.advance_ms(3);
        store.release();
    };
    let (result, ()) = futures::join!(renew, contender);
    assert_eq!(
        result.expect_err("expiry survives a CAS retry").code(),
        ErrorCode::SnapshotNotFound
    );
    let stored = crate::test_support::namespace_store(&inner, &namespace_id)
        .load_snapshot_record(&snapshot.snapshot_id)
        .await
        .expect("load")
        .expect("retained expired snapshot");
    assert_eq!(stored.state.owner.expires_at_ms(), Some(1_002));
}

/// Checks that an extension whose publication budget passed while it loaded
/// the record returns the budget error and sends no compare-and-swap, so the
/// expiry stays as it was.
#[tokio::test]
async fn an_extension_past_its_publication_budget_writes_nothing() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("snapshot-budget").expect("namespace");
    let inner = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let context = writer_at("writer", 1_000);
    bootstrap_namespace(&inner, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let namespace = crate::test_support::namespace_store(&inner, &namespace_id);
    let expires_at_ms = 1_000 + 10 * crate::limits::GC_GRACE_WINDOW_MS;
    let snapshot = namespace
        .create_snapshot_record(
            SnapshotOwner::User {
                name: Some("stalled extension".to_owned()),
                expires_at_ms: Some(expires_at_ms),
                deleted_at_ms: None,
            },
            crate::test_support::folded_manifest(&namespace)
                .await
                .expect("fold the WAL tail"),
            &context.clock(),
        )
        .await
        .map(|(record, _)| record)
        .expect("snapshot");
    let key = keys::snapshot_record(&namespace_id, &snapshot.snapshot_id);
    let store = BlockingStore::new(
        RecordingStore::new(inner.clone(), KeyPredicate::exact(&key)),
        KeyPredicate::exact(&key),
        OperationSelector::Read,
    );
    let clock = Arc::new(ManualClock::new(context.now_ms));
    store.block_next();
    let namespace = {
        let mut fixture =
            crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context);
        fixture.namespace.timer = clock.clone();
        fixture
    }
    .engine();
    let administrator = crate::test_support::administrator(&namespace);
    let live = crate::test_support::read_context(&inner, &namespace_id).await;
    let extend = namespace.extend_snapshot(
        &administrator,
        &live,
        &snapshot.snapshot_id,
        expires_at_ms + 1,
    );
    let stall = async {
        store.wait_until_blocked().await;
        clock.advance_ms(crate::limits::METADATA_PUBLICATION_BUDGET_MS + 1);
        store.release();
    };
    let (result, ()) = futures::join!(extend, stall);
    assert_matches!(
        result,
        Err(CoreError::MetadataPublicationBudgetExceeded { .. })
    );
    let counts = store.inner().counts();
    assert_eq!(
        (counts.puts, counts.compare_and_swaps),
        (0, 0),
        "an extension past its budget sends no write"
    );
    let stored = crate::test_support::namespace_store(&inner, &namespace_id)
        .load_snapshot_record(&snapshot.snapshot_id)
        .await
        .expect("load")
        .expect("snapshot");
    assert_eq!(stored.state.owner.expires_at_ms(), Some(expires_at_ms));
}
