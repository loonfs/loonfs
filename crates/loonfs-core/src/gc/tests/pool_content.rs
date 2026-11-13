//! Content and temporary objects in the pool's collection: the roots that keep
//! an object, and when an object that no root protects is old enough to delete.

use super::*;

/// Folds the tail, moves the floor to the head, which the advance's fold
/// records in a manifest, and compacts every family group, so each file
/// keeps only its newest revision.
async fn compact_below_the_floor<S: ObjectStore>(store: &S, namespace_id: &NamespaceId) {
    crate::test_support::namespace_store(store, namespace_id)
        .fold_wal()
        .await
        .expect("fold wal");
    let floor_writer = crate::test_support::writer_at(
        "test-writer",
        crate::test_support::grace_elapsed_after(1_000),
    );
    crate::test_support::NamespaceFixture::new(store, namespace_id, &floor_writer)
        .engine()
        .advance_retention_floor(RetentionTarget::Head)
        .await
        .expect("advance floor");
    crate::test_support::compact_every_group(store, namespace_id).await;
}

async fn read_context<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> crate::RuntimeReadContext {
    let anchor = crate::test_support::namespace_store(store, namespace_id)
        .load_read_anchor(crate::test_support::observed_now())
        .await
        .expect("read anchor");
    crate::RuntimeReadContext {
        read_basis: crate::control::ReadBasis {
            basis: anchor.basis(),
            head: anchor.read_state,
        },
        tail: None,
        visible_seq: None,
    }
}

#[tokio::test]
async fn content_gc_retains_completed_content_inside_its_grace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let (upload_id, content_ref) =
        complete_upload_for_gc(&store, &namespace_id, b"unpublished\n", &setup).await;
    let content_key = loonfs_objectstore::keys::content_blob(&pool_id, &content_ref.content_id);

    let inside = context(setup.now_ms + CONTENT_RECLAMATION_GRACE_WINDOW_MS - 1);
    let report = collect_pool(&store, &pool_id, &inside).await;

    assert_eq!(report.deleted.upload_sessions, 0);
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store.head(&content_key).await.expect("head").is_some());
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_some());
}

#[tokio::test]
async fn an_unpublished_completed_upload_loses_its_record_then_its_object() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    write_test_file(&store, &namespace_id, "/docs/other.txt", "gc-other", &setup).await;
    let (upload_id, content_ref) =
        complete_upload_for_gc(&store, &namespace_id, b"unpublished\n", &setup).await;
    let content_key = loonfs_objectstore::keys::content_blob(&pool_id, &content_ref.content_id);

    let past = context(
        now_after_newest_object(
            &store,
            &namespace_id,
            CONTENT_RECLAMATION_GRACE_WINDOW_MS + 1,
        )
        .await,
    );
    let report = collect_pool(&store, &pool_id, &past).await;
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(
        report.deleted.content_objects, 0,
        "the collection read the record as a root before it removed it"
    );
    assert!(read_upload_session(&store, &namespace_id, &upload_id)
        .await
        .is_none());
    assert!(store.head(&content_key).await.expect("head").is_some());

    let report = collect_pool(&store, &pool_id, &past).await;
    assert_eq!(report.deleted.content_objects, 1);
    assert!(store.head(&content_key).await.expect("head").is_none());
}

#[tokio::test]
async fn a_revision_compacted_away_below_the_floor_loses_its_object_after_the_grace() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let pool_id =
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(1_000))
            .create()
            .await
            .expect("bootstrap")
            .pool_id;
    let first = put_file_content_with_id(
        &store,
        &namespace_id,
        "/file",
        b"first",
        loonfs_types::ContentId::parse("con_00000000000000000000000001").expect("first id"),
    )
    .await;
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the first revision into the base run");
    let second = put_file_content_with_id(
        &store,
        &namespace_id,
        "/file",
        b"second",
        loonfs_types::ContentId::parse("con_00000000000000000000000002").expect("second id"),
    )
    .await;
    compact_below_the_floor(&store, &namespace_id).await;
    let first_key = loonfs_objectstore::keys::content_blob(&pool_id, &first.content_id);

    // No root names the object any more. The manifests and segments report
    // an ancient age, and the object itself is younger than the grace.
    let young = context(now_after_newest_object(&store, &namespace_id, 0).await);
    let manifests = KeyPredicate::manifest(&namespace_id);
    let segments = KeyPredicate::metadata_segment();
    let aged_metadata = MetadataMapStore::aged(
        &store,
        KeyPredicate::new(move |key| manifests.matches(key) || segments.matches(key)),
    );
    let report = collect_pool(&aged_metadata, &pool_id, &young).await;
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store.head(&first_key).await.expect("head").is_some());

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 1);
    assert!(store.head(&first_key).await.expect("head").is_none());
    assert!(store
        .head(&loonfs_objectstore::keys::content_blob(
            &pool_id,
            &second.content_id
        ))
        .await
        .expect("head")
        .is_some());
    let error =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .read_content_ref(&first, u64::MAX, &read_context(&store, &namespace_id).await)
            .await
            .expect_err("a reclaimed object reads as an unpublished one");
    assert_eq!(error.code(), crate::error::ErrorCode::PathNotFound);
}

/// Checks that once a later commit acknowledges the floor that expired a
/// deletion, a base compaction of each group removes the rows that the
/// deletion hides, and a later layout compaction drops their chains, a collection
/// past the grace deletes the hidden file's content and keeps the content
/// of a visible file.
#[tokio::test]
async fn content_an_expired_deletion_hid_is_deleted_once_its_rows_are_removed() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let hidden = put_file_content_with_id(
        &store,
        &namespace_id,
        "/old/file",
        b"hidden",
        numbered_content_id(1),
    )
    .await;
    let kept = put_file_content_with_id(
        &store,
        &namespace_id,
        "/kept",
        b"kept",
        numbered_content_id(2),
    )
    .await;
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .delete_path("/old", None)
        .await
        .expect("delete /old");
    crate::test_support::namespace_store(&store, &namespace_id)
        .fold_wal()
        .await
        .expect("fold the deletion");
    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at(
            "test-writer",
            crate::test_support::grace_elapsed_after(setup.now_ms),
        ),
    )
    .engine()
    .advance_retention_floor(RetentionTarget::Head)
    .await
    .expect("move the floor past the deletion");
    // Each new file's WAL object records the advanced floor, and each collection
    // folds it and compacts every group that the file gave a delta run. The
    // first collection removes the hidden revision, but its layout compaction can run
    // before its revisions compaction, so the second collection drops the chain.
    for (number, path) in [(3, "/later-1"), (4, "/later-2")] {
        put_file_content_with_id(
            &store,
            &namespace_id,
            path,
            b"later",
            numbered_content_id(number),
        )
        .await;
        compact_below_the_floor(&store, &namespace_id).await;
    }

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = collect_pool(&store, &pool_id, &aged).await;
    let content_key = |content_ref: &ContentRef| {
        loonfs_objectstore::keys::content_blob(&pool_id, &content_ref.content_id)
    };
    assert!(report.deleted.content_objects >= 1);
    assert!(store
        .head(&content_key(&hidden))
        .await
        .expect("head")
        .is_none());
    assert!(store
        .head(&content_key(&kept))
        .await
        .expect("head")
        .is_some());
}

#[tokio::test]
async fn content_only_a_snapshot_manifest_names_stays_until_the_snapshot_is_released() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let kept = put_file_content_with_id(
        &store,
        &namespace_id,
        "/file",
        b"pinned",
        loonfs_types::ContentId::parse("con_00000000000000000000000001").expect("kept id"),
    )
    .await;
    let snapshot = create_snapshot(&store, &namespace_id, &setup)
        .await
        .expect("snapshot");
    put_file_content_with_id(
        &store,
        &namespace_id,
        "/file",
        b"current",
        loonfs_types::ContentId::parse("con_00000000000000000000000002").expect("current id"),
    )
    .await;
    compact_below_the_floor(&store, &namespace_id).await;
    let kept_key = loonfs_objectstore::keys::content_blob(&pool_id, &kept.content_id);

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store.head(&kept_key).await.expect("head").is_some());

    crate::test_support::NamespaceFixture::new(
        &store,
        &namespace_id,
        &crate::test_support::writer_at("test-writer", 0),
    )
    .engine()
    .delete_snapshot(&snapshot.snapshot_id)
    .await
    .expect("release the snapshot");
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 1);
    assert!(store.head(&kept_key).await.expect("head").is_none());
}

#[tokio::test]
async fn a_source_keeps_content_its_fork_inherited_while_the_fork_snapshot_exists() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let source = NamespaceId::parse("source").expect("namespace id");
    let clone = NamespaceId::parse("clone").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &source, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    put_file_content(&store, &source, "/file", b"inherited").await;
    let fixture = crate::test_support::NamespaceFixture::new(&store, &source, &setup);
    fixture
        .engine()
        .fork_namespace(
            &clone,
            &loonfs_test_support::test_actor(),
            crate::test_support::folded_manifest(&fixture.namespace)
                .await
                .expect("fold the WAL tail"),
        )
        .await
        .expect("fork");
    put_file_content(&store, &source, "/file", b"replaced").await;
    compact_below_the_floor(&store, &source).await;

    let aged = context(now_after_newest_object(&store, &source, GRACE_MS + 1).await);
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 0);
    let file = crate::test_support::namespace_store(&store, &clone)
        .load_current_metadata_view()
        .await
        .expect("clone view")
        .read_file_bytes(
            "/file",
            None,
            &crate::authorize::ReadAccess::live(crate::authorize::Authorizer::Unrestricted),
        )
        .await
        .expect("the fork reads what it inherited");
    assert_eq!(file.bytes, b"inherited");
}

#[tokio::test]
async fn the_wal_tail_roots_inline_content_a_direct_download_wrote_early() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let setup = context(1_000);
    let pool_id = crate::test_support::NamespaceFixture::new(&store, &namespace_id, &setup)
        .create()
        .await
        .expect("bootstrap")
        .pool_id;
    let key = publish_inline(&store, &namespace_id, 0, &setup).await;
    let target =
        crate::NamespaceEngine::reader(crate::test_support::namespace_store(&store, &namespace_id))
            .direct_download_target(
                "/owned-0",
                None,
                0,
                &read_context(&store, &namespace_id).await,
            )
            .await
            .expect("download target");
    assert_eq!(target.ranges[0].object_key, key);

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(report.deleted.content_objects, 0);
    assert!(store.head(&key).await.expect("head").is_some());
}

#[tokio::test]
async fn content_and_temporary_keys_the_layout_does_not_recognize_are_kept() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let pool_id =
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context(1_000))
            .create()
            .await
            .expect("bootstrap")
            .pool_id;
    let content = loonfs_objectstore::keys::content_prefix(&pool_id);
    let temporary = loonfs_objectstore::keys::temporary_prefix(&pool_id);
    let collected = [
        loonfs_objectstore::keys::content_blob(&pool_id, &loonfs_types::ContentId::generate()),
        format!("{temporary}piece"),
    ];
    let kept = [
        format!("{content}not-a-content-id"),
        format!("{content}nested/object"),
        format!("{temporary}nested/piece"),
    ];
    for key in collected.iter().chain(&kept) {
        store
            .put_if_absent(key, Bytes::from_static(b"bytes"))
            .await
            .expect("write object");
    }

    let aged = context(now_after_newest_object(&store, &namespace_id, GRACE_MS + 1).await);
    let report = collect_pool(&store, &pool_id, &aged).await;
    assert_eq!(
        (
            report.deleted.content_objects,
            report.deleted.temporary_objects
        ),
        (1, 1)
    );
    // The collection lists the whole content prefix, so both unrecognized content
    // keys are listed and kept, as is the nested temporary key.
    assert_eq!(report.retained.unrecognized_key, 3);
    for key in &collected {
        assert!(store.head(key).await.expect("head").is_none(), "{key}");
    }
    for key in &kept {
        assert!(store.head(key).await.expect("head").is_some(), "{key}");
    }
}

#[tokio::test]
async fn provider_age_reserves_the_clock_margin_before_deletion() {
    use crate::limits::GC_SAFETY_MARGIN_MS;

    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("clock-margin").expect("namespace");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    let pool_id =
        crate::test_support::NamespaceFixture::new(&inner, &namespace_id, &context(1_000))
            .create()
            .await
            .expect("namespace")
            .pool_id;
    let key =
        loonfs_objectstore::keys::content_blob(&pool_id, &loonfs_types::ContentId::generate());
    let provider_stamp = 1_000_000;
    let store = RecordingStore::new(
        MetadataMapStore::new(inner, KeyPredicate::exact(&key), move |mut metadata| {
            metadata.last_modified_at_ms = Some(provider_stamp);
            metadata
        }),
        KeyPredicate::exact(&key),
    );
    store
        .put_if_absent(&key, Bytes::from_static(b"orphan"))
        .await
        .expect("orphan");
    store.reset();
    let options = GcOptions {
        grace_window_ms: GC_MIN_GRACE_WINDOW_MS,
    };
    let publication_bound = GC_MIN_GRACE_WINDOW_MS - GC_SAFETY_MARGIN_MS;
    let now_ms = provider_stamp + publication_bound - 1 + GC_SAFETY_MARGIN_MS;
    let retained = crate::test_support::pool_store(&store, &pool_id)
        .gc(None, &options, &context(now_ms).clock())
        .await
        .expect("collection before cutoff");
    assert_eq!(retained.deleted.content_objects, 0);
    assert_eq!(retained.retained.within_grace_window, 1);
    assert_eq!(store.counts().deletes, 0);
    let collected = crate::test_support::pool_store(&store, &pool_id)
        .gc(None, &options, &context(now_ms + 1).clock())
        .await
        .expect("collection at cutoff");
    assert_eq!(collected.deleted.content_objects, 1);
    assert_eq!(store.counts().deletes, 1);
    assert_eq!(store.counts().heads, 0);
}
