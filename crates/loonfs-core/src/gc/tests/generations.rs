//! Collection across namespace generation boundaries.

use super::*;
use loonfs_objectstore::keys::{checkpoint_record, content_blob};

async fn create<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId, now_ms: u64) {
    bootstrap_namespace(
        store,
        namespace_id,
        &context(now_ms),
        &loonfs_test_support::test_actor(),
        &loonfs_api::NamespaceAccess::Unrestricted {},
        false,
    )
    .await
    .expect("create");
}

async fn retired_pin<S: ObjectStore + ?Sized>(store: &S, namespace_id: &NamespaceId) -> PinId {
    let head = crate::namespace::control::load_current_manifest(store, namespace_id)
        .await
        .expect("tombstone");
    PinId::retired(namespace_id, head.envelope.payload().manifest_no)
}

#[tokio::test]
async fn recreated_namespace_reclaims_only_eligible_prior_content_and_its_retired_pin() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("recreated").expect("namespace");
    let inner = LocalFsStore::new(directory.path()).expect("store");
    create(&inner, &namespace_id, 1_000).await;
    let (upload_id, old_unpublished, _) =
        complete_upload_for_gc(&inner, &namespace_id, b"old", &context(1_000)).await;
    let old_keys = publish_owned_content(&inner, &namespace_id, 257).await;
    delete_namespace(&inner, &namespace_id, Default::default(), &context(2_000))
        .await
        .expect("delete");
    let pin_id = retired_pin(&inner, &namespace_id).await;
    create(&inner, &namespace_id, 3_000).await;
    let (_, current_content, _) =
        complete_upload_for_gc(&inner, &namespace_id, b"new", &context(3_000)).await;
    let current_key = content_blob(&namespace_id, &current_content.content_id);
    let current_keys = publish_owned_content(&inner, &namespace_id, 1).await;
    let prefix = format!("namespaces/{namespace_id}/content/");
    let store = RecordingStore::new(inner, KeyPredicate::prefix(&prefix));
    let deadline = 2_000 + GRACE_MS;
    let before = gc_namespace(&store, &namespace_id, &config(), &context(deadline - 1))
        .await
        .expect("before deadline");
    assert_eq!(before.reclaim_after_ms, None);
    assert_eq!(before.next_reclamation_at_ms, Some(deadline));
    assert_eq!(before.deleted.retired_content_objects, 0);
    assert_eq!(store.counts().lists, 0);
    assert_eq!(store.counts().deletes, 0);
    assert!(checkpoint_exists(&store, &namespace_id, &pin_id).await);
    let at = gc_namespace(&store, &namespace_id, &config(), &context(deadline))
        .await
        .expect("at deadline");
    assert_eq!(at.deleted.retired_content_objects, old_keys.len() as u64);
    assert_eq!(at.deleted.content_objects, 1);
    assert_eq!(at.deleted.upload_sessions, 1);
    assert_eq!(at.deleted_checkpoints_by_owner.retired, 1);
    assert!(!checkpoint_exists(&store, &namespace_id, &pin_id).await);
    assert_eq!(store.counts().lists, 0);
    assert_eq!(store.counts().deletes, old_keys.len() + 1);
    let unpublished_key = content_blob(&namespace_id, &old_unpublished.content_id);
    assert!(store
        .head(&unpublished_key)
        .await
        .expect("old unpublished content")
        .is_none());
    for key in &current_keys {
        assert!(store
            .head(key)
            .await
            .expect("current published content")
            .is_some());
    }
    for key in &old_keys {
        assert!(store.head(key).await.expect("old content").is_none());
    }
    assert!(store
        .head(&current_key)
        .await
        .expect("new content")
        .is_some());
    assert!(store
        .head(&loonfs_objectstore::keys::upload_session(
            &namespace_id,
            &upload_id
        ))
        .await
        .expect("old session")
        .is_none());
}

#[tokio::test]
async fn prior_generation_pin_blocks_reclamation_for_the_whole_pass_that_deletes_it() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("pinned-generation").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    create(&store, &namespace_id, 1_000).await;
    let keys = publish_owned_content(&store, &namespace_id, 3).await;
    let pin = create_checkpoint(&store, &namespace_id, &context(2_100))
        .await
        .expect("user pin");
    delete_namespace(&store, &namespace_id, Default::default(), &context(2_000))
        .await
        .expect("delete");
    let retired_id = retired_pin(&store, &namespace_id).await;
    create(&store, &namespace_id, 3_000).await;
    let deadline = 2_000 + GRACE_MS;
    let held = gc_namespace(&store, &namespace_id, &config(), &context(deadline + 1))
        .await
        .expect("pin still young");
    assert_eq!(
        held.next_reclamation_at_ms,
        Some(2_100 + GRACE_MS),
        "the collector wakes when the pin holding the generation ages out"
    );
    assert_eq!(held.deleted.retired_content_objects, 0);
    assert!(checkpoint_exists(&store, &namespace_id, &pin.checkpoint_id).await);
    let aged = context(2_100 + GRACE_MS);
    let reaped = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("pin ages out");
    assert_eq!(reaped.deleted_checkpoints_by_owner.expired, 1);
    assert_eq!(reaped.deleted.retired_content_objects, 0);
    assert!(checkpoint_exists(&store, &namespace_id, &retired_id).await);
    let reclaimed = gc_namespace(&store, &namespace_id, &config(), &aged)
        .await
        .expect("next pass");
    assert_eq!(reclaimed.deleted.retired_content_objects, keys.len() as u64);
    assert_eq!(reclaimed.deleted_checkpoints_by_owner.retired, 1);
}

#[tokio::test]
async fn fork_pins_follow_recreated_targets_until_the_prior_generation_is_reclaimed() {
    let directory = tempdir().expect("directory");
    let source = NamespaceId::parse("source").expect("namespace");
    let target = NamespaceId::parse("target").expect("namespace");
    let unrelated = NamespaceId::parse("unrelated").expect("namespace");
    let store = LocalFsStore::new(directory.path()).expect("store");
    create(&store, &source, 1_000).await;
    fork_namespace(
        &store,
        &source,
        &target,
        &loonfs_test_support::test_actor(),
        None,
        &context(1_000),
    )
    .await
    .expect("fork");
    let fork_pin = read_fork_record(&store, &source).await;
    let abandoned = crate::checkpoint::create_checkpoint(
        &store,
        &source,
        PinOwner::Fork {
            target_namespace_id: unrelated.clone(),
        },
        &context(1_000),
    )
    .await
    .expect("abandoned fork");
    create(&store, &unrelated, 1_000).await;
    for namespace_id in [&target, &unrelated] {
        delete_namespace(&store, namespace_id, Default::default(), &context(2_000))
            .await
            .expect("delete");
        create(&store, namespace_id, 3_000).await;
    }
    let deadline = context(2_000 + GRACE_MS);
    let pass = gc_namespace(&store, &source, &config(), &deadline)
        .await
        .expect("source pass");
    assert_eq!(pass.deleted_checkpoints_by_owner.fork, 1);
    assert!(checkpoint_exists(&store, &source, &fork_pin.pin_id).await);
    assert!(!checkpoint_exists(&store, &source, &abandoned.checkpoint_id).await);
    let keys = store
        .list_prefix(&checkpoint_prefix(&target))
        .await
        .expect("target pins");
    let (_, retired_id) =
        crate::checkpoint::record::checkpoint_key_ids(&keys[0]).expect("retired pin");
    let store = FailStore::new(
        store,
        KeyPredicate::exact(metadata_manifest_object(&target, &retired_id.manifest_no())),
        OperationClass::Read,
        InjectedError::Transport("tombstone unavailable".to_owned()),
    );
    store.fail_next(1);
    let uncertain = gc_namespace(&store, &source, &config(), &deadline)
        .await
        .expect("unreadable prior target");
    assert_eq!(uncertain.deleted_checkpoints_by_owner.fork, 0);
    assert!(checkpoint_exists(&store, &source, &fork_pin.pin_id).await);
    let target_pass = gc_namespace(&store, &target, &config(), &deadline)
        .await
        .expect("reclaim target");
    assert_eq!(target_pass.deleted_checkpoints_by_owner.fork, 1);
    assert_eq!(target_pass.deleted_checkpoints_by_owner.retired, 1);
    let source_pass = gc_namespace(&store, &source, &config(), &deadline)
        .await
        .expect("source after reclaim");
    assert_eq!(source_pass.deleted_checkpoints_by_owner.fork, 0);
    assert!(store
        .head(&checkpoint_record(&source, &fork_pin.pin_id))
        .await
        .expect("source pin")
        .is_none());
}

#[tokio::test]
async fn tombstone_segments_survive_scan_failures_until_the_retired_pin_is_deleted() {
    let directory = tempdir().expect("directory");
    let config = GcConfig {
        grace_window_ms: UNREFERENCED_SEGMENT_MIN_AGE_MS + GRACE_MS,
    };
    let namespace_id = NamespaceId::parse("tombstone-segments").expect("namespace");
    let source = NamespaceId::parse("segment-source").expect("namespace");
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::metadata_segment(),
    );
    create(&store, &source, 1_000).await;
    fork_namespace(
        &store,
        &source,
        &namespace_id,
        &loonfs_test_support::test_actor(),
        None,
        &context(1_000),
    )
    .await
    .expect("fork");
    let source_pin = read_fork_record(store.inner(), &source).await;
    let content_keys = publish_owned_content(&store, &namespace_id, 1).await;
    delete_namespace(&store, &namespace_id, Default::default(), &context(2_000))
        .await
        .expect("delete");
    let tombstone = crate::namespace::control::load_current_manifest(&store, &namespace_id)
        .await
        .expect("tombstone");
    let segment_keys = tombstone
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(loonfs_objectstore::keys::metadata_segment_object_key)
        .collect::<Vec<_>>();
    assert!(!segment_keys.is_empty());
    let waiting = gc_namespace(
        &store,
        &namespace_id,
        &config,
        &context(2_000 + config.grace_window_ms - 1),
    )
    .await
    .expect("current tombstone roots");
    assert_eq!(waiting.deleted.metadata_segments, 0);
    let pin_id = retired_pin(&store, &namespace_id).await;
    create(&store, &namespace_id, 3_000).await;
    let store = FailStore::new(
        store,
        KeyPredicate::content_blob(),
        OperationClass::Delete,
        InjectedError::Transport("content delete failed".to_owned()),
    );
    let store = FailStore::new(
        store,
        KeyPredicate::metadata_segment(),
        OperationClass::Read,
        InjectedError::Transport("segment read failed".to_owned()),
    );
    let deadline = context(2_000 + config.grace_window_ms);
    for fail_read in [true, false] {
        if fail_read {
            store.fail_next(1);
        } else {
            store.inner().fail_next(1);
        }
        assert!(gc_namespace(&store, &namespace_id, &config, &deadline)
            .await
            .is_err());
        assert!(checkpoint_exists(&store, &namespace_id, &pin_id).await);
        assert!(checkpoint_exists(&store, &source, &source_pin.pin_id).await);
        for key in segment_keys.iter().chain(&content_keys) {
            assert!(store.head(key).await.expect("retained object").is_some());
        }
    }
    let reclaimed = gc_namespace(&store, &namespace_id, &config, &deadline)
        .await
        .expect("reclaim generation");
    assert_eq!(reclaimed.deleted.metadata_segments, 0);
    assert_eq!(reclaimed.deleted_checkpoints_by_owner.retired, 1);
    assert_eq!(reclaimed.deleted_checkpoints_by_owner.fork, 1);
    for key in &segment_keys {
        assert!(store
            .head(key)
            .await
            .expect("rooted for whole pass")
            .is_some());
    }
    let swept = gc_namespace(&store, &namespace_id, &config, &deadline)
        .await
        .expect("collect unrooted segments");
    assert_eq!(swept.deleted.metadata_segments, segment_keys.len() as u64);
    for key in &segment_keys {
        assert!(store.head(key).await.expect("collected segment").is_none());
    }
}
