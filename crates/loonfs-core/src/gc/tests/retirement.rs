//! Derived retirement deadlines and outstanding multipart uploads.

use super::*;
use crate::limits::{DIRECT_TRANSFER_URL_TTL_MS, NAMESPACE_RETIREMENT_GRACE_MS};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::stores::FakeMultipartStore;

#[tokio::test]
async fn retired_fork_reclaims_without_reading_inherited_segments() {
    let directory = tempdir().expect("directory");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace");
    let target = NamespaceId::parse("target").expect("namespace");
    let setup = context(1_000);
    bootstrap_namespace(
        &inner,
        &source,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::unrestricted(),
        false,
    )
    .await
    .expect("bootstrap source");
    publish_owned_content(&inner, &source, 1).await;
    fork_namespace(
        &inner,
        &source,
        &target,
        &loonfs_test_support::test_actor(),
        None,
        &setup,
    )
    .await
    .expect("fork target");
    write_test_file(&inner, &target, "/owned.txt", "target-write", &setup).await;
    let upload = crate::protocol::begin_service_proxied_upload(&inner, &target, None, &setup)
        .await
        .expect("open upload");
    delete_namespace(&inner, &target, Default::default(), &setup)
        .await
        .expect("delete target");
    let tombstone = crate::namespace::control::load_current_manifest(&inner, &target)
        .await
        .expect("target tombstone");
    let payload = tombstone.envelope.payload();
    let record_key =
        loonfs_objectstore::keys::retired_generation_record(&target, payload.generation);
    let source_pin = &payload
        .fork_basis
        .as_ref()
        .expect("fork basis")
        .source_pin_id;
    let source_segments = payload
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.owner_namespace_id == source)
        .map(loonfs_objectstore::keys::metadata_segment_object_key)
        .collect::<Vec<_>>();
    assert!(!source_segments.is_empty());
    bootstrap_namespace(
        &inner,
        &target,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::unrestricted(),
        false,
    )
    .await
    .expect("recreate target");
    let store = RecordingStore::new(
        inner,
        KeyPredicate::prefix(metadata_segment_prefix(&source)),
    );
    let first = gc_namespace(
        &store,
        &target,
        &config(),
        &context(setup.now_ms + GRACE_MS),
    )
    .await
    .expect("release source pin with retained session");
    assert_eq!(first.deleted.retired_content_objects, 1);
    assert_eq!(first.deleted_checkpoints_by_owner.fork, 1);
    assert_eq!(first.deleted.retired_generation_records, 0);
    assert!(!checkpoint_exists(&store, &source, source_pin).await);
    assert!(store
        .head(&record_key)
        .await
        .expect("retired record")
        .is_some());
    assert!(read_upload_session(&store, &target, &upload.upload_id)
        .await
        .is_some());
    assert!(store.snapshot().is_empty());

    for key in source_segments {
        store
            .inner()
            .delete(&key)
            .await
            .expect("collect source segment");
    }
    let expired_at_ms = setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS;
    let repeated = gc_namespace(&store, &target, &config(), &context(expired_at_ms))
        .await
        .expect("repeat retirement after source collection");
    assert_eq!(repeated.deleted.retired_content_objects, 1);
    assert_eq!(repeated.deleted_checkpoints_by_owner.fork, 0);
    assert_eq!(repeated.deleted.retired_generation_records, 0);
    assert!(store
        .head(&record_key)
        .await
        .expect("retired record")
        .is_some());
    let finished = gc_namespace(
        &store,
        &target,
        &config(),
        &context(expired_at_ms + GRACE_MS),
    )
    .await
    .expect("finish retirement after session cleanup");
    assert_eq!(finished.deleted.retired_content_objects, 1);
    assert_eq!(finished.deleted.upload_sessions, 1);
    assert_eq!(finished.deleted.retired_generation_records, 1);
    assert!(store
        .head(&record_key)
        .await
        .expect("retired record")
        .is_none());
    assert!(read_upload_session(&store, &target, &upload.upload_id)
        .await
        .is_none());
    assert!(store.snapshot().is_empty());
}

#[tokio::test]
async fn deleted_generation_uses_its_deletion_clock_without_publishing_a_manifest() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("retirement-clock").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let (deadline, keys) = retired_content_namespace(&store, &namespace_id).await;
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
        let late_content_key =
            loonfs_objectstore::keys::content_blob(&namespace_id, &late_state.content_id);
        let store = RecordingStore::new(
            inner,
            KeyPredicate::prefix(format!("namespaces/{namespace_id}/content/")),
        );
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
        let record_key = loonfs_objectstore::keys::retired_generation_record(
            &namespace_id,
            tombstone.state.generation,
        );
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
        assert_eq!(store.counts().lists, 0);
        assert_eq!(store.inner().open_uploads(), 2);
        assert_eq!(store.inner().aborts(), 0);
        assert_eq!(
            store
                .head(&record_key)
                .await
                .expect("retired record")
                .is_some(),
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
            store
                .head(&record_key)
                .await
                .expect("retired record")
                .is_some(),
            recreate
        );
        clock.advance_ms(config.grace_window_ms);
        let reaped = gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
            .await
            .expect("reap session");
        assert_eq!(reaped.deleted.upload_sessions, 1);
        assert_eq!(
            reaped.deleted.retired_generation_records,
            u64::from(recreate)
        );
        assert!(store
            .head(&record_key)
            .await
            .expect("retired record")
            .is_none());
        assert_eq!(store.inner().aborts(), 2);
        assert!(
            read_upload_session(&store, &namespace_id, &upload.session.upload_id)
                .await
                .is_none()
        );
        if recreate {
            store
                .put_if_absent(&late_content_key, Bytes::from_static(b"late object"))
                .await
                .expect("late content object");
            store
                .put_if_absent(&late_key, late_session)
                .await
                .expect("late session record");
            let store = RecordingStore::new(store, KeyPredicate::content_blob());
            let reclaimed = gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
                .await
                .expect("session from reclaimed generation");
            assert_eq!(reclaimed.deleted.upload_sessions, 1);
            assert_eq!(reclaimed.deleted.content_objects, 0);
            assert_eq!(store.counts().deletes, 1);
            assert_eq!(store.inner().inner().aborts(), 3);
            assert_eq!(
                store.inner().inner().abort_records().last(),
                Some(&(late_content_key.clone(), late_provider_id.clone()))
            );
            assert!(store
                .head(&late_content_key)
                .await
                .expect("collected late content")
                .is_none());
            assert_eq!(store.counts().lists, 0);
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
async fn a_retired_record_to_a_collected_manifest_is_deleted_without_protecting_objects() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("missing-retired-manifest").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let (deadline, _) = retired_content_namespace(&store, &namespace_id).await;
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
    let record_key = loonfs_objectstore::keys::retired_generation_record(
        &namespace_id,
        tombstone.state.generation,
    );
    let record_bytes = store
        .get(&record_key, None)
        .await
        .expect("read record")
        .expect("record");
    gc_namespace(&store, &namespace_id, &config(), &deadline)
        .await
        .expect("reclaim generation");
    assert!(store
        .head(&record_key)
        .await
        .expect("retired record")
        .is_none());
    store
        .delete(&tombstone.object_key)
        .await
        .expect("collect tombstone");
    store
        .put_if_absent(&record_key, record_bytes)
        .await
        .expect("late record");
    store.reset();
    let report = gc_namespace(&store, &namespace_id, &config(), &deadline)
        .await
        .expect("collect late record");
    assert_eq!(report.deleted.retired_generation_records, 1);
    assert!(store
        .head(&record_key)
        .await
        .expect("retired record")
        .is_none());
    assert_eq!(store.counts().puts, 0);
}

#[tokio::test]
async fn reclaimed_generation_sessions_delete_content_before_the_record_for_every_status() {
    use loonfs_objectstore::keys::{content_blob, upload_session};
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("reclaimed-sessions").expect("namespace");
    let store = std::sync::Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let (deadline, _) = retired_content_namespace(&store, &namespace_id).await;
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
    gc_namespace(&store, &namespace_id, &config(), &deadline)
        .await
        .expect("reclaim");
    for status in 0..3 {
        let content_id = loonfs_api::ContentId::generate();
        let content_ref = ContentRef::blob_v1(
            namespace_id.clone(),
            loonfs_api::NamespaceGeneration(1),
            content_id.clone(),
            b"late",
        );
        let state = UploadSessionPayload {
            namespace_id: namespace_id.clone(),
            owner_generation: loonfs_api::NamespaceGeneration(1),
            upload_id: UploadId::generate(),
            content_id,
            created_at_ms: 1_000,
            subject_id: None,
            mode: UploadSessionMode::ServiceProxied {
                staging: ProxiedStaging::Idle,
            },
            status: match status {
                0 => UploadSessionRecordStatus::Open {
                    expires_at_ms: u64::MAX,
                },
                1 => UploadSessionRecordStatus::Aborted {
                    aborted_at_ms: deadline.now_ms,
                },
                _ => UploadSessionRecordStatus::Completed {
                    completed_at_ms: deadline.now_ms,
                    content_ref,
                },
            },
        };
        let content_key = content_blob(&namespace_id, &state.content_id);
        let session_key = upload_session(&namespace_id, &state.upload_id);
        store
            .put_if_absent(&content_key, Bytes::from_static(b"late"))
            .await
            .expect("late content");
        store
            .put_if_absent(
                &session_key,
                loonfs_api::wire::control::encode_control_state(
                    ControlObjectKind::UploadSession,
                    &state,
                )
                .expect("encode session")
                .into(),
            )
            .await
            .expect("late session");
        let failing = FailStore::new(
            store.clone(),
            KeyPredicate::exact(&content_key),
            OperationClass::Delete,
            InjectedError::Transport("content cleanup failed".to_owned()),
        );
        failing.fail_next(1);
        let retained = gc_namespace(&failing, &namespace_id, &config(), &deadline)
            .await
            .expect("retain failed cleanup");
        assert_eq!(retained.deleted.upload_sessions, 0);
        assert!(store
            .head(&session_key)
            .await
            .expect("retry record")
            .is_some());
        let recording = RecordingStore::new(store.clone(), KeyPredicate::any());
        let cleaned = gc_namespace(&recording, &namespace_id, &config(), &deadline)
            .await
            .expect("retry cleanup");
        assert_eq!(cleaned.deleted.upload_sessions, 1);
        let deleted = recording
            .snapshot()
            .into_iter()
            .filter_map(|operation| match operation {
                loonfs_test_support::stores::RecordedOperation::Delete { key } => Some(key),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(deleted, [content_key.clone(), session_key.clone()]);
        assert!(store.head(&content_key).await.expect("content").is_none());
        assert!(store.head(&session_key).await.expect("session").is_none());
    }
}
