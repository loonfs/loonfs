//! Snapshot expiry must include time spent loading and retrying its pin.

use super::*;
use crate::checkpoint::snapshot::{classify_live_snapshot, extend_snapshot_expiry};
use loonfs_api::wire::control::PinOwner;
use loonfs_objectstore::keys::checkpoint_record;
use loonfs_test_support::clock::ManualClock;

#[tokio::test]
async fn snapshot_expiry_after_the_renewal_cas_starts_preserves_success() {
    for lose_ack in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("snapshot-ack").expect("namespace");
        let inner = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let context = mutation_context("writer", 1_000);
        bootstrap_namespace(&inner, &namespace_id, &context)
            .await
            .expect("bootstrap");
        let pin = create::create_checkpoint(
            &inner,
            &namespace_id,
            PinOwner::Snapshot {
                name: "short lease".to_owned(),
                expires_at_ms: 1_001,
            },
            &context,
        )
        .await
        .expect("snapshot");
        let key = checkpoint_record(&namespace_id, &pin.pin_id);
        let failing = FailStore::new(
            inner.clone(),
            KeyPredicate::exact(&key),
            OperationClass::CompareAndSwap,
            InjectedError::Transport("lost renewal acknowledgement".to_owned()),
        )
        .apply_then_fail();
        if lose_ack {
            failing.fail_next(1);
        }
        let store = BlockingStore::new(
            failing,
            KeyPredicate::exact(&key),
            OperationClass::CompareAndSwap,
        );
        let clock = Arc::new(ManualClock::new(context.now_ms));
        store.block_next();
        let renew = extend_snapshot_expiry(
            &store,
            &namespace_id,
            &pin.pin_id,
            1_002,
            20_000,
            &context,
            clock.clone(),
        );
        let expire = async {
            store.wait_until_blocked().await;
            clock.advance_ms(3);
            store.release();
        };
        let (result, ()) = futures::join!(renew, expire);
        assert_eq!(
            result.expect("the extension landed").expires_at_ms,
            Some(1_002)
        );
        let stored = load_checkpoint_record(&inner, &namespace_id, &pin.pin_id)
            .await
            .expect("load");
        assert_eq!(
            stored.as_ref().expect("pin").state.owner.expires_at_ms(),
            Some(1_002)
        );
        assert_eq!(
            classify_live_snapshot(stored, &pin.pin_id, clock.now_ms())
                .expect_err("subsequent reads see expiry")
                .code(),
            ErrorCode::SnapshotGone
        );
    }
}

#[tokio::test]
async fn snapshot_expiring_during_renewal_load_cannot_be_extended_or_reported_live() {
    for requested_expiry in [10_000, 1_001] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("snapshot-renewal").expect("namespace");
        let inner = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
        let context = mutation_context("writer", 1_000);
        bootstrap_namespace(&inner, &namespace_id, &context)
            .await
            .expect("bootstrap");
        let pin = create::create_checkpoint(
            &inner,
            &namespace_id,
            PinOwner::Snapshot {
                name: "short lease".to_owned(),
                expires_at_ms: 1_001,
            },
            &context,
        )
        .await
        .expect("snapshot");
        let store = BlockingStore::new(
            inner.clone(),
            KeyPredicate::exact(checkpoint_record(&namespace_id, &pin.pin_id)),
            OperationClass::Read,
        );
        let clock = Arc::new(ManualClock::new(context.now_ms));
        store.block_next();
        let renew = extend_snapshot_expiry(
            &store,
            &namespace_id,
            &pin.pin_id,
            requested_expiry,
            20_000,
            &context,
            clock.clone(),
        );
        let expire = async {
            store.wait_until_blocked().await;
            clock.advance_ms(2);
            let error = classify_live_snapshot(
                load_checkpoint_record(&inner, &namespace_id, &pin.pin_id)
                    .await
                    .expect("pin"),
                &pin.pin_id,
                clock.now_ms(),
            )
            .expect_err("an independent reader observes expiry");
            assert_eq!(error.code(), ErrorCode::SnapshotGone);
            store.release();
        };
        let (result, ()) = futures::join!(renew, expire);
        assert_eq!(
            result.expect_err("expired snapshot must not renew").code(),
            ErrorCode::SnapshotGone
        );
        let stored = load_checkpoint_record(&inner, &namespace_id, &pin.pin_id)
            .await
            .expect("load")
            .expect("retained expired pin");
        assert_eq!(stored.state.owner.expires_at_ms(), Some(1_001));
    }
}

#[tokio::test]
async fn snapshot_renewal_contention_does_not_restart_the_expiry_clock() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("snapshot-retry").expect("namespace");
    let inner = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let context = mutation_context("writer", 1_000);
    bootstrap_namespace(&inner, &namespace_id, &context)
        .await
        .expect("bootstrap");
    let pin = create::create_checkpoint(
        &inner,
        &namespace_id,
        PinOwner::Snapshot {
            name: "short lease".to_owned(),
            expires_at_ms: 1_001,
        },
        &context,
    )
    .await
    .expect("snapshot");
    let store = BlockingStore::new(
        inner.clone(),
        KeyPredicate::exact(checkpoint_record(&namespace_id, &pin.pin_id)),
        OperationClass::CompareAndSwap,
    );
    let clock = Arc::new(ManualClock::new(context.now_ms));
    store.block_next();
    let renew = extend_snapshot_expiry(
        &store,
        &namespace_id,
        &pin.pin_id,
        10_000,
        20_000,
        &context,
        clock.clone(),
    );
    let contender = async {
        store.wait_until_blocked().await;
        extend_snapshot_expiry(
            &inner,
            &namespace_id,
            &pin.pin_id,
            1_002,
            20_000,
            &context,
            clock.clone(),
        )
        .await
        .expect("competing extension");
        clock.advance_ms(3);
        store.release();
    };
    let (result, ()) = futures::join!(renew, contender);
    assert_eq!(
        result.expect_err("expiry survives a CAS retry").code(),
        ErrorCode::SnapshotGone
    );
    let stored = load_checkpoint_record(&inner, &namespace_id, &pin.pin_id)
        .await
        .expect("load")
        .expect("retained expired pin");
    assert_eq!(stored.state.owner.expires_at_ms(), Some(1_002));
}
