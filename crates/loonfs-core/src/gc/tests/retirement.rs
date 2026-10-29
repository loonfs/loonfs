//! Derived retirement deadlines and outstanding multipart uploads.

use super::*;
use loonfs_test_support::stores::replace_object;
use std::assert_matches;

#[tokio::test]
async fn deleted_namespace_uses_its_deletion_clock_without_publishing_a_manifest() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("retirement-clock").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let (deadline, keys) = retired_content_namespace(&store, &namespace_id).await;
    let before = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("tombstone");
    store.reset();
    let waiting = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &context(deadline.now_ms - 1).clock())
        .await
        .expect("before deadline");
    assert_eq!(waiting.reclaimable_at_ms, Some(1_000 + retirement_ms()));
    assert_eq!(waiting.next_reclamation_at_ms, Some(deadline.now_ms));
    assert_eq!(waiting.deleted.retired_content_objects, 0);
    let reclaimed = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options(), &deadline.clock())
        .await
        .expect("at deadline");
    assert_eq!(reclaimed.deleted.retired_content_objects, keys.len() as u64);
    assert_eq!(reclaimed.next_reclamation_at_ms, None);
    let after = crate::test_support::namespace_store(&store, &namespace_id)
        .load_current_manifest()
        .await
        .expect("same tombstone");
    assert_eq!(before.manifest().manifest_no, after.manifest().manifest_no);
    assert_eq!(store.counts().puts, 0);
}

#[tokio::test]
async fn retired_fork_reclaims_without_reading_inherited_segments() {
    let directory = tempdir().expect("directory");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace");
    let target = NamespaceId::parse("target").expect("namespace");
    let setup = context(1_000);
    crate::test_support::NamespaceFixture::new(&inner, &source, &setup)
        .create()
        .await
        .expect("bootstrap source");
    let source_content = publish_owned_content(&inner, &source, 2).await;
    crate::test_support::NamespaceFixture::new(&inner, &source, &setup)
        .engine()
        .fork_namespace(&target, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork target");
    write_test_file(&inner, &target, "/owned.txt", "target-write", &setup).await;
    let upload = crate::test_support::NamespaceFixture::new(&inner, &target, &setup)
        .engine()
        .begin_upload()
        .await
        .expect("open upload");
    crate::test_support::NamespaceFixture::new(&inner, &target, &setup)
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete target");
    let tombstone = crate::test_support::namespace_store(&inner, &target)
        .load_current_manifest()
        .await
        .expect("target tombstone");
    let payload = tombstone.payload();
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
    let first = crate::test_support::namespace_store(&store, &target)
        .gc(
            None,
            &options(),
            &context(setup.now_ms + retirement_ms()).clock(),
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
    assert!(!pin_exists(&store, &source, source_pin).await);
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
    let repeated = crate::test_support::namespace_store(&store, &target)
        .gc(None, &options(), &context(expired_at_ms).clock())
        .await
        .expect("repeat retirement after source collection");
    assert_eq!(repeated.deleted.retired_content_objects, 0);
    assert_eq!(repeated.deleted_checkpoints_by_owner.fork, 0);
    let finished = crate::test_support::namespace_store(&store, &target)
        .gc(None, &options(), &context(expired_at_ms + GRACE_MS).clock())
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
    use crate::limits::{DIRECT_TRANSFER_URL_TTL_MS, NAMESPACE_RETIREMENT_GRACE_WINDOW_MS};
    use loonfs_test_support::{clock::ManualClock, stores::FakeMultipartStore};

    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("retired-multipart").expect("namespace");
    let inner = FakeMultipartStore::new(LocalFsStore::new(directory.path()).expect("store"));
    let clock = ManualClock::new(1_000);
    let setup = context(clock.now_ms());
    crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap");
    let upload = crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &setup)
        .engine()
        .begin_direct_multipart_upload_target(Default::default())
        .await
        .expect("open multipart upload");
    let targets = crate::test_support::NamespaceFixture::new(
        &inner,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", clock.now_ms()),
    )
    .engine()
    .direct_multipart_part_targets(
        &upload.session.upload_id,
        &[loonfs_types::UploadPartChecksumClaim {
            part_number: 1,
            checksum: loonfs_types::Checksum::crc64nvme(b"part"),
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
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(clock.now_ms()))
        .engine()
        .delete_namespace(Default::default())
        .await
        .expect("delete");
    store
        .inner()
        .upload_part(&targets.provider_upload_id, 1, b"part")
        .expect("issued capability still writes after deletion");
    let options = GcOptions {
        grace_window_ms: GC_MIN_GRACE_WINDOW_MS,
        ..Default::default()
    };
    let report = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &context(clock.now_ms()).clock())
        .await
        .expect("retire");
    let deadline = report.reclaimable_at_ms.expect("deadline");
    assert_eq!(
        deadline,
        clock.now_ms() + NAMESPACE_RETIREMENT_GRACE_WINDOW_MS
    );
    assert!(deadline > expires_at_ms);
    clock.advance_ms(deadline - clock.now_ms() - 1);
    let before = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &context(clock.now_ms()).clock())
        .await
        .expect("before deadline");
    assert_eq!(before.next_reclamation_at_ms, Some(deadline));
    // Each pass inside the grace lists the content prefix once to measure the
    // content, and deletes nothing.
    assert_eq!(store.counts().lists, 2);
    assert_eq!(store.counts().deletes, 0);
    clock.advance_ms(1);
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &context(clock.now_ms()).clock())
        .await
        .expect("owner sweep");
    assert_eq!(store.counts().lists, 3);
    assert_eq!(store.inner().open_uploads(), 1);
    assert_eq!(store.inner().aborts(), 0);
    let open_session = read_upload_session(&store, &namespace_id, &upload.session.upload_id)
        .await
        .expect("open session");
    assert_eq!(open_session.outcome, None);
    clock.advance_ms(
        setup.now_ms + UPLOAD_SESSION_LEASE_MS + options.grace_window_ms - clock.now_ms(),
    );
    crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &context(clock.now_ms()).clock())
        .await
        .expect("provider cleanup after retirement");
    assert_eq!(store.inner().aborts(), 1);
    assert_eq!(store.inner().open_uploads(), 0);
    clock.advance_ms(options.grace_window_ms);
    let reaped = crate::test_support::namespace_store(&store, &namespace_id)
        .gc(None, &options, &context(clock.now_ms()).clock())
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
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .create()
        .await
        .expect("bootstrap");
    crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .engine()
        .fork_namespace(&target, &loonfs_test_support::test_actor(), None)
        .await
        .expect("fork");
    let current = crate::test_support::namespace_store(&store, &target)
        .load_current_manifest()
        .await
        .expect("target");
    let mut payload = current.payload().clone();
    let basis = payload.fork_basis.as_mut().expect("fork basis");
    basis.manifest.payload_checksum = loonfs_types::sha256_digest(b"retired");
    let pin_id = basis.source_pin_id.clone();
    let bytes = crate::manifest::publish::encode_manifest(payload);
    replace_object(&store, &current.object_key, bytes.into_bytes().into())
        .await
        .expect("corrupt basis");
    store.reset();
    let error = crate::test_support::namespace_store(&store, &source)
        .gc(None, &options(), &context(setup.now_ms + GRACE_MS).clock())
        .await
        .expect_err("corrupt basis");
    assert_matches!(error, CoreError::NamespaceCorrupt(_));
    assert_eq!(store.counts().puts, 0);
    assert_eq!(store.counts().deletes, 0);
    assert!(pin_exists(&store, &source, &pin_id).await);
}
