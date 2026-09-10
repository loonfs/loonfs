//! Retirement publication timing and outstanding multipart uploads.

use super::*;
use crate::gc::collect::gc_namespace_with_timer;
use crate::limits::{
    DIRECT_TRANSFER_URL_TTL_MS, NAMESPACE_RETIREMENT_GRACE_MS, RETIREMENT_PUBLICATION_BUDGET_MS,
};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::FakeMultipartStore;

#[tokio::test]
async fn retirement_includes_listing_time_in_its_budget_and_retries_with_a_fresh_clock() {
    for elapsed_ms in [
        RETIREMENT_PUBLICATION_BUDGET_MS,
        RETIREMENT_PUBLICATION_BUDGET_MS + 1,
    ] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("retirement-budget").expect("namespace");
        let store = RecordingStore::new(
            BlockingStore::new(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::exact(checkpoint_prefix(&namespace_id)),
                OperationClass::List,
            ),
            KeyPredicate::any(),
        );
        let clock = ManualClock::new(1_000);
        let call = context(clock.now_ms());
        bootstrap_namespace(&store, &namespace_id, &call, false)
            .await
            .expect("bootstrap");
        delete_namespace(&store, &namespace_id, Default::default(), &call)
            .await
            .expect("delete");
        let config = GcConfig {
            grace_window_ms: GC_MIN_GRACE_WINDOW_MS,
        };
        store.reset();
        store.inner().block_next();
        let (result, ()) = tokio::join!(
            gc_namespace_with_timer(&store, &namespace_id, &config, &call, &clock),
            async {
                store.inner().wait_until_blocked().await;
                clock.advance_ms(elapsed_ms);
                store.inner().release();
            }
        );
        if elapsed_ms == RETIREMENT_PUBLICATION_BUDGET_MS {
            assert_eq!(
                result.expect("within budget").reclaim_after_ms,
                Some(call.now_ms + NAMESPACE_RETIREMENT_GRACE_MS)
            );
        } else {
            assert!(matches!(
                result,
                Err(CoreError::MetadataPublicationBudgetExceeded {
                    elapsed_ms: actual_elapsed_ms,
                    budget_ms: RETIREMENT_PUBLICATION_BUDGET_MS,
                }) if actual_elapsed_ms == elapsed_ms
            ));
            assert_eq!(store.counts().puts, 0);
            assert_eq!(store.counts().deletes, 0);
            let head = crate::namespace::control::load_head_object(&store, &namespace_id)
                .await
                .expect("head");
            assert_eq!(head.status.reclaim_after_ms(), None);
            let retry = context(clock.now_ms());
            let report = gc_namespace_with_timer(&store, &namespace_id, &config, &retry, &clock)
                .await
                .expect("fresh call");
            assert_eq!(
                report.reclaim_after_ms,
                Some(retry.now_ms + NAMESPACE_RETIREMENT_GRACE_MS)
            );
        }
    }
}

#[tokio::test]
async fn open_direct_upload_outlives_retirement_and_still_gets_provider_cleanup() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("retired-multipart").expect("namespace");
    let inner = FakeMultipartStore::new(LocalFsStore::new(directory.path()).expect("store"));
    let clock = ManualClock::new(1_000);
    let setup = context(clock.now_ms());
    bootstrap_namespace(&inner, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let upload = crate::protocol::begin_direct_multipart_upload_target(
        &inner,
        &namespace_id,
        Default::default(),
        &setup,
    )
    .await
    .expect("open multipart upload");
    let targets = crate::protocol::direct_multipart_part_targets(
        &inner,
        &namespace_id,
        &upload.upload_id,
        &[loonfs_api::v0::UploadPartChecksumClaim {
            part_number: 1,
            checksum: loonfs_api::Checksum::crc64nvme(b"part"),
        }],
    )
    .await
    .expect("prepare capability");
    let expires_at_ms = clock.now_ms() + DIRECT_TRANSFER_URL_TTL_MS;
    let content_store_id =
        crate::namespace::catalog::load_namespace_content_store_id(&inner, &namespace_id)
            .await
            .expect("content store");
    let owner_prefix =
        loonfs_objectstore::keys::content_owner_prefix(&content_store_id, &namespace_id);
    let store = RecordingStore::new(inner, KeyPredicate::prefix(owner_prefix));
    clock.advance_ms(1);
    delete_namespace(
        &store,
        &namespace_id,
        Default::default(),
        &context(clock.now_ms()),
    )
    .await
    .expect("delete");
    store
        .inner()
        .upload_part(&targets.provider_upload_id, 1, b"part")
        .expect("issued capability still writes after deletion");
    let config = GcConfig {
        grace_window_ms: GC_MIN_GRACE_WINDOW_MS,
    };
    let report = gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
        .await
        .expect("retire");
    let deadline = report.reclaim_after_ms.expect("deadline");
    assert_eq!(deadline, clock.now_ms() + NAMESPACE_RETIREMENT_GRACE_MS);
    assert!(deadline >= expires_at_ms);
    clock.advance_ms(deadline - clock.now_ms() - 1);
    let before = gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
        .await
        .expect("before deadline");
    assert_eq!(before.next_reclamation_at_ms, Some(deadline));
    assert_eq!(store.counts().lists, 0);
    assert_eq!(store.counts().deletes, 0);
    clock.advance_ms(1);
    gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
        .await
        .expect("owner sweep");
    assert_eq!(store.counts().lists, 1);
    assert_eq!(store.inner().open_uploads(), 1);
    assert_eq!(store.inner().aborts(), 0);
    assert!(matches!(
        read_upload_session(&store, &namespace_id, &upload.upload_id)
            .await
            .expect("open session")
            .status,
        UploadSessionRecordStatus::Open { .. }
    ));
    clock.advance_ms(
        setup.now_ms + UPLOAD_SESSION_LEASE_MS + config.grace_window_ms - clock.now_ms(),
    );
    gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
        .await
        .expect("provider cleanup after retirement");
    assert_eq!(store.inner().aborts(), 1);
    assert_eq!(store.inner().open_uploads(), 0);
    clock.advance_ms(config.grace_window_ms);
    let reaped = gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
        .await
        .expect("reap session");
    assert_eq!(reaped.deleted.upload_sessions, 1);
    assert_eq!(store.inner().aborts(), 2);
    assert!(
        read_upload_session(&store, &namespace_id, &upload.upload_id)
            .await
            .is_none()
    );
}
