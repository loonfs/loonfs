//! Bounded collection across pin and upload families.

use super::*;
use loonfs_objectstore::keys::checkpoint_record;

#[tokio::test]
async fn exhausted_pin_budget_does_not_stop_upload_cleanup() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = NamespaceId::parse("many-pins").expect("namespace");
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    for _ in 0..8 {
        create_checkpoint(&store, &namespace_id, &setup)
            .await
            .expect("permanent pin");
    }
    let session_key = write_upload_session(&store, &namespace_id).await;
    let expired = context(setup.now_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS);
    let bounded = GcConfig {
        max_steps: Some(2),
        ..config()
    };
    gc_namespace(&store, &namespace_id, &bounded, &expired)
        .await
        .expect("abort expired upload");
    let store = RecordingStore::new(store, KeyPredicate::exact(&session_key));
    let report = gc_namespace(
        &store,
        &namespace_id,
        &bounded,
        &context(expired.now_ms + GRACE_MS),
    )
    .await
    .expect("collect aged upload");
    assert!(report.budget_exhausted);
    assert_eq!(report.deleted.upload_sessions, 1);
    assert_eq!(store.counts().deletes, 1);
    assert!(store
        .inner()
        .head(&session_key)
        .await
        .expect("session")
        .is_none());
}

#[tokio::test]
async fn changing_clocks_reach_expired_pins_among_permanent_pins() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("rotating-pins").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(checkpoint_prefix(&namespace_id)),
    );
    let setup = context(1_000);
    bootstrap_namespace(&store, &namespace_id, &setup, false)
        .await
        .expect("bootstrap");
    let pin = create_checkpoint(&store, &namespace_id, &setup)
        .await
        .expect("pin basis");
    let basis =
        crate::checkpoint::load_checkpoint_record(&store, &namespace_id, &pin.checkpoint_id)
            .await
            .expect("pin")
            .expect("record")
            .state;
    release_checkpoint_record(&store, &namespace_id, &pin.checkpoint_id)
        .await
        .expect("release basis pin");
    let mut records = Vec::new();
    for number in 0..48 {
        let random = number * (u64::MAX / 48);
        let record = CheckpointRecordState {
            pin_id: CheckpointId::parse(format!("pin_{:020}-{random:016x}", basis.manifest_no.0))
                .expect("positioned pin"),
            owner: if number % 2 == 0 {
                CheckpointOwner::Snapshot {
                    name: "expired".to_owned(),
                    expires_at_ms: 2_000,
                }
            } else {
                basis.owner.clone()
            },
            ..basis.clone()
        };
        crate::checkpoint::record::write_checkpoint_record(&store, &record)
            .await
            .expect("pin");
        records.push(record);
    }
    let bounded = GcConfig {
        max_steps: Some(3),
        ..config()
    };
    let mut subsets = Vec::new();
    for now_ms in [GRACE_MS + 2_000, GRACE_MS + 2_001] {
        store.reset();
        let report = gc_namespace(&store, &namespace_id, &bounded, &context(now_ms))
            .await
            .expect("bounded collection");
        assert!(report.budget_exhausted);
        assert_eq!(store.counts().gets_with_metadata, 3);
        subsets.push(
            store
                .take()
                .into_iter()
                .filter_map(|operation| match operation {
                    loonfs_test_support::stores::RecordedOperation::Delete { key, .. } => Some(key),
                    _ => None,
                })
                .collect::<BTreeSet<_>>(),
        );
        for record in &records {
            store
                .inner()
                .put_overwrite(
                    &checkpoint_record(&namespace_id, &record.pin_id),
                    crate::checkpoint::record::encode_checkpoint_record(record)
                        .expect("encode pin"),
                )
                .await
                .expect("restore pins");
        }
    }
    assert!(!subsets[0].is_empty());
    assert!(!subsets[1].is_empty());
    assert_ne!(subsets[0], subsets[1]);
    let mut deleted = 0;
    for offset in 0..256 {
        deleted += gc_namespace(
            &store,
            &namespace_id,
            &bounded,
            &context(GRACE_MS + 2_000 + offset),
        )
        .await
        .expect("next clock")
        .released_checkpoints
        .snapshot;
        if deleted == 24 {
            break;
        }
    }
    assert_eq!(deleted, 24);
    let remaining = store
        .inner()
        .list_prefix(&checkpoint_prefix(&namespace_id))
        .await
        .expect("remaining pins");
    assert_eq!(remaining.len(), 24);
    for record in records
        .iter()
        .filter(|record| matches!(record.owner, CheckpointOwner::User { .. }))
    {
        assert!(remaining.contains(&checkpoint_record(&namespace_id, &record.pin_id)));
    }
}

#[tokio::test]
async fn fork_pin_grace_skips_targets_and_aged_pins_read_only_manifest_discovery() {
    let directory = tempdir().expect("directory");
    let source = NamespaceId::parse("source").expect("source");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix("namespaces/target-"),
    );
    let setup = context(1_000);
    bootstrap_namespace(&store, &source, &setup, false)
        .await
        .expect("bootstrap");
    let mut targets = Vec::new();
    for number in 0..6 {
        let target = NamespaceId::parse(format!("target-{number}")).expect("target");
        fork_namespace(&store, &source, &target, None, &setup)
            .await
            .expect("fork");
        targets.push(target);
    }
    write_test_file(&store, &targets[0], "/own.txt", "target-write", &setup).await;
    store
        .inner()
        .delete(&hint(&targets[5]))
        .await
        .expect("absent target hint");
    store.reset();
    let young = gc_namespace(
        &store,
        &source,
        &config(),
        &context(setup.now_ms + GRACE_MS - 1),
    )
    .await
    .expect("young pins");
    assert_eq!(young.released_checkpoints.fork, 0);
    assert!(store.snapshot().is_empty());
    let aged = gc_namespace(
        &store,
        &source,
        &config(),
        &context(setup.now_ms + GRACE_MS),
    )
    .await
    .expect("aged pins");
    assert_eq!(aged.released_checkpoints.fork, 1);
    let mut expected = vec![hint(&targets[5])];
    for (index, target) in targets[..5].iter().enumerate() {
        let manifest_no = if index == 0 {
            ManifestNo(2)
        } else {
            ManifestNo(1)
        };
        expected.extend([
            hint(target),
            metadata_manifest_object(target, &manifest_no),
            metadata_manifest_object(target, &manifest_no.successor().expect("successor")),
        ]);
    }
    let mut actual = store.take_get_keys();
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
    assert_eq!(
        store
            .inner()
            .list_prefix(&checkpoint_prefix(&source))
            .await
            .expect("source pins")
            .len(),
        5
    );
}
