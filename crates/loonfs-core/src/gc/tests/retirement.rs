//! Derived retirement deadlines and outstanding multipart uploads.

use super::*;
use crate::limits::{DIRECT_TRANSFER_URL_TTL_MS, NAMESPACE_RETIREMENT_GRACE_MS};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::FakeMultipartStore;

#[tokio::test]
async fn deleted_generation_uses_its_deletion_clock_without_publishing_a_manifest() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("retirement-clock").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let (content_store_id, deadline) = retired_content_namespace(&store, &namespace_id).await;
    let keys = owned_content_keys(&store, &content_store_id, &namespace_id).await;
    let before = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("tombstone");
    store.reset();
    let waiting = gc_namespace(
        &store,
        &namespace_id,
        &config(),
        &context(deadline.now_ms - 1),
    )
    .await
    .expect("before deadline");
    assert_eq!(waiting.reclaim_after_ms, Some(1_000 + GRACE_MS));
    assert_eq!(waiting.next_reclamation_at_ms, Some(deadline.now_ms));
    assert_eq!(waiting.deleted.retired_content_objects, 0);
    let reclaimed = gc_namespace(&store, &namespace_id, &config(), &deadline)
        .await
        .expect("at deadline");
    assert_eq!(reclaimed.deleted.retired_content_objects, keys.len() as u64);
    assert_eq!(reclaimed.next_reclamation_at_ms, None);
    let after = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("same tombstone");
    assert_eq!(
        before.state.manifest.manifest_no,
        after.state.manifest.manifest_no
    );
    assert_eq!(store.counts().puts, 0);
}

#[tokio::test]
async fn open_direct_upload_outlives_retirement_and_still_gets_provider_cleanup() {
    for recreate in [false, true] {
        let directory = tempdir().expect("directory");
        let namespace_id = NamespaceId::parse("retired-multipart").expect("namespace");
        let inner = FakeMultipartStore::new(LocalFsStore::new(directory.path()).expect("store"));
        let clock = ManualClock::new(1_000);
        let setup = context(clock.now_ms());
        bootstrap_namespace(
            &inner,
            &namespace_id,
            &setup,
            &loonfs_test_support::test_actor(),
            &loonfs_api::NamespaceAccess::Unrestricted {},
            false,
        )
        .await
        .expect("bootstrap");
        let upload = crate::protocol::begin_direct_multipart_upload_target(
            &inner,
            &namespace_id,
            None,
            Default::default(),
            &setup,
        )
        .await
        .expect("open multipart upload");
        let targets = crate::protocol::direct_multipart_part_targets(
            &inner,
            &namespace_id,
            &upload.session.upload_id,
            None,
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
        let owner_prefix = loonfs_objectstore::keys::content_owner_prefix(
            &content_store_id,
            &namespace_id,
            loonfs_api::NamespaceGeneration(1),
        );
        let late_upload = crate::protocol::begin_direct_multipart_upload_target(
            &inner,
            &namespace_id,
            None,
            Default::default(),
            &setup,
        )
        .await
        .expect("late multipart upload");
        let late_key =
            loonfs_objectstore::keys::upload_session(&namespace_id, &late_upload.session.upload_id);
        let late_session = inner
            .get(&late_key, None)
            .await
            .expect("late session read")
            .expect("late session");
        inner
            .delete(&late_key)
            .await
            .expect("hold back session record");
        let late_state = loonfs_api::wire::control::decode_control_object::<UploadSessionPayload>(
            &late_session,
            ControlObjectKind::UploadSession,
        )
        .expect("decode late session")
        .into_payload();
        let UploadSessionMode::DirectMultipart {
            provider_upload_id: late_provider_id,
            ..
        } = &late_state.mode
        else {
            panic!("expected multipart session");
        };
        let late_content_key = loonfs_objectstore::keys::content_blob(
            &content_store_id,
            &namespace_id,
            late_state.owner_generation,
            &late_state.content_id,
        );
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
        let tombstone = crate::namespace::control::load_current_manifest(&store, &namespace_id)
            .await
            .expect("tombstone");
        let pin_id = PinId::retired(&namespace_id, tombstone.envelope.payload().manifest_no);
        if recreate {
            bootstrap_namespace(
                &store,
                &namespace_id,
                &context(clock.now_ms()),
                &loonfs_test_support::test_actor(),
                &loonfs_api::NamespaceAccess::Unrestricted {},
                false,
            )
            .await
            .expect("recreate");
        }

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
        assert_eq!(store.inner().open_uploads(), 2);
        assert_eq!(store.inner().aborts(), 0);
        assert_eq!(
            checkpoint_exists(&store, &namespace_id, &pin_id).await,
            recreate
        );
        let open_session = read_upload_session(&store, &namespace_id, &upload.session.upload_id)
            .await
            .expect("open session");
        assert!(matches!(
            open_session.status,
            UploadSessionRecordStatus::Open { .. }
        ));
        clock.advance_ms(
            setup.now_ms + UPLOAD_SESSION_LEASE_MS + config.grace_window_ms - clock.now_ms(),
        );
        gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
            .await
            .expect("provider cleanup after retirement");
        assert_eq!(store.inner().aborts(), 1);
        assert_eq!(store.inner().open_uploads(), 1);
        assert_eq!(
            checkpoint_exists(&store, &namespace_id, &pin_id).await,
            recreate
        );
        clock.advance_ms(config.grace_window_ms);
        let reaped = gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
            .await
            .expect("reap session");
        assert_eq!(reaped.deleted.upload_sessions, 1);
        assert_eq!(
            reaped.deleted_checkpoints_by_owner.retired,
            u64::from(recreate)
        );
        assert!(!checkpoint_exists(&store, &namespace_id, &pin_id).await);
        assert_eq!(store.inner().aborts(), 2);
        assert!(
            read_upload_session(&store, &namespace_id, &upload.session.upload_id)
                .await
                .is_none()
        );
        if recreate {
            store
                .put_if_absent(&late_key, late_session)
                .await
                .expect("late session record");
            let store = RecordingStore::new(store, KeyPredicate::prefix("content-stores/"));
            let reclaimed = gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
                .await
                .expect("session from reclaimed generation");
            assert_eq!(reclaimed.deleted.upload_sessions, 1);
            assert_eq!(reclaimed.deleted.content_objects, 0);
            assert_eq!(store.counts().deletes, 0);
            assert_eq!(store.inner().inner().aborts(), 3);
            assert_eq!(
                store.inner().inner().abort_records().last(),
                Some(&(late_content_key, late_provider_id.clone()))
            );
            assert_eq!(store.inner().inner().open_uploads(), 0);
            assert!(store
                .inner()
                .inner()
                .upload_part(late_provider_id, 1, b"late")
                .is_err());
        }
    }
}

#[tokio::test]
async fn a_retired_pin_to_a_collected_manifest_is_deleted_without_protecting_objects() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("missing-retired-manifest").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let (_, deadline) = retired_content_namespace(&store, &namespace_id).await;
    let tombstone = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("tombstone");
    bootstrap_namespace(
        &store,
        &namespace_id,
        &deadline,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::unrestricted(),
        false,
    )
    .await
    .expect("recreate");
    let pin_id = PinId::retired(&namespace_id, tombstone.envelope.payload().manifest_no);
    let pin_key = loonfs_objectstore::keys::checkpoint_record(&namespace_id, &pin_id);
    let pin_bytes = store
        .get(&pin_key, None)
        .await
        .expect("read pin")
        .expect("pin");
    gc_namespace(&store, &namespace_id, &config(), &deadline)
        .await
        .expect("reclaim generation");
    assert!(!checkpoint_exists(&store, &namespace_id, &pin_id).await);
    store
        .delete(&tombstone.object_key)
        .await
        .expect("collect tombstone");
    store
        .put_if_absent(&pin_key, pin_bytes)
        .await
        .expect("late pin");
    store.reset();
    let report = gc_namespace(&store, &namespace_id, &config(), &deadline)
        .await
        .expect("collect late pin");
    assert_eq!(report.deleted_checkpoints_by_owner.retired, 1);
    assert!(!checkpoint_exists(&store, &namespace_id, &pin_id).await);
    assert_eq!(store.counts().puts, 0);
}
