//! Derived retirement deadlines and outstanding multipart uploads.

use super::*;

#[tokio::test]
async fn deleted_namespace_uses_its_deletion_clock_without_publishing_a_manifest() {
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
    let source_content = publish_owned_content(&inner, &source, 2).await;
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
    let store = RecordingStore::new(
        inner,
        KeyPredicate::new(|key| {
            key.starts_with("namespaces/source/segments/") || key.contains("/content/")
        }),
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
    let requests = store.take();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().any(|operation| matches!(operation,
        loonfs_test_support::stores::RecordedOperation::List { prefix } if prefix == "namespaces/target/content/"
    )));
    assert!(requests.iter().any(|operation| matches!(operation,
        loonfs_test_support::stores::RecordedOperation::Delete { key } if key.starts_with("namespaces/target/content/")
    )));
    assert!(!checkpoint_exists(&store, &source, source_pin).await);
    assert!(read_upload_session(&store, &target, &upload.upload_id)
        .await
        .is_some());
    for key in source_content.iter().chain(&source_segments) {
        assert!(store
            .inner()
            .head(key)
            .await
            .expect("source object")
            .is_some());
    }
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
    assert_eq!(repeated.deleted.retired_content_objects, 0);
    assert_eq!(repeated.deleted_checkpoints_by_owner.fork, 0);
    let finished = gc_namespace(
        &store,
        &target,
        &config(),
        &context(expired_at_ms + GRACE_MS),
    )
    .await
    .expect("finish retirement after session cleanup");
    assert_eq!(finished.deleted.retired_content_objects, 0);
    assert_eq!(finished.deleted.upload_sessions, 1);
    assert!(read_upload_session(&store, &target, &upload.upload_id)
        .await
        .is_none());
    assert!(store
        .snapshot()
        .iter()
        .all(|operation| { !operation.key().starts_with("namespaces/source/") }));
}

#[tokio::test]
async fn open_direct_upload_outlives_retirement_and_still_gets_provider_cleanup() {
    use crate::limits::{DIRECT_TRANSFER_URL_TTL_MS, NAMESPACE_RETIREMENT_GRACE_MS};
    use loonfs_test_support::{clock::ManualClock, stores::FakeMultipartStore};

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
        &loonfs_api::NamespaceAccess::unrestricted(),
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
    assert!(deadline > expires_at_ms);
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
    assert_eq!(store.inner().open_uploads(), 0);
    clock.advance_ms(config.grace_window_ms);
    let reaped = gc_namespace(&store, &namespace_id, &config, &context(clock.now_ms()))
        .await
        .expect("reap session");
    assert_eq!(reaped.deleted.upload_sessions, 1);
    assert_eq!(store.inner().aborts(), 2);
    assert!(
        read_upload_session(&store, &namespace_id, &upload.session.upload_id)
            .await
            .is_none()
    );
}

#[tokio::test]
async fn a_fork_basis_naming_its_pin_with_a_different_checksum_is_corrupt() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let source = NamespaceId::parse("source").expect("namespace");
    let target = NamespaceId::parse("target").expect("namespace");
    let setup = context(1_000);
    bootstrap_namespace(
        &store,
        &source,
        &setup,
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::unrestricted(),
        false,
    )
    .await
    .expect("bootstrap");
    fork_namespace(
        &store,
        &source,
        &target,
        &loonfs_test_support::test_actor(),
        None,
        &setup,
    )
    .await
    .expect("fork");
    let current = crate::namespace::control::load_current_manifest(&store, &target)
        .await
        .expect("target");
    let mut payload = current.envelope.payload().clone();
    let basis = payload.fork_basis.as_mut().expect("fork basis");
    basis.manifest.payload_checksum = format!("sha256:{}", "0".repeat(64));
    let pin_id = basis.source_pin_id.clone();
    let bytes = crate::checkpoint::publish::encode_manifest(payload).expect("manifest");
    store
        .put_overwrite(&current.object_key, bytes.into_bytes().into())
        .await
        .expect("corrupt basis");
    store.reset();
    let error = gc_namespace(
        &store,
        &source,
        &config(),
        &context(setup.now_ms + GRACE_MS),
    )
    .await
    .expect_err("corrupt basis");
    assert!(matches!(error, CoreError::NamespaceCorrupt(_)));
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().deletes, 0);
    assert!(checkpoint_exists(&store, &source, &pin_id).await);
}
