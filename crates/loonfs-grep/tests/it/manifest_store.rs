//! Numbered grep publication, hint discovery, and input validation.

#![allow(clippy::panic)]

use bytes::Bytes;
use loonfs::Deadline;
use loonfs_api::{ChangeSeq, ManifestNo, NamespaceId, RunNo};
use loonfs_grep::keyspace::{grep_prefix, hint_key, manifest_key, manifests_prefix};
use loonfs_grep::manifest::{
    encode_grep_hint, encode_grep_manifest, load_current_grep_manifest, load_grep_manifest,
    publish_grep_manifest, raise_grep_hint, GrepHint, GrepIndexState, GrepIndexStatus,
    GrepManifestError, GrepManifestState,
};
use loonfs_grep::GrepError;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::timing::StdMonotonicTimer;
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    FailStore, InjectedError, KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};
use std::sync::Arc;

#[tokio::test]
async fn publications_race_one_number_and_the_loser_replans_at_the_next() {
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = namespace_id("docs");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(grep_prefix(&namespace_id)),
    );
    let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
    let first = publish_grep_manifest(
        &store,
        None,
        &state(namespace_id.clone(), ManifestNo(1), RunNo(0)),
        &deadline,
    )
    .await
    .expect("enable");
    assert_eq!(
        store
            .take()
            .into_iter()
            .filter(|operation| matches!(operation, RecordedOperation::Put { .. }))
            .collect::<Vec<_>>()
            .iter()
            .map(|operation| match operation {
                RecordedOperation::Put {
                    key,
                    mode: PutMode::CreateIfAbsent,
                    ..
                } => key.clone(),
                other => panic!("expected create-only put, got {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![
            hint_key(&namespace_id),
            manifest_key(&namespace_id, &ManifestNo(1))
        ]
    );

    let candidate = state(namespace_id.clone(), ManifestNo(2), RunNo(1));
    let (left, right) = tokio::join!(
        publish_grep_manifest(&store, Some(&first), &candidate, &deadline),
        publish_grep_manifest(&store, Some(&first), &candidate, &deadline),
    );
    let outcomes = [left, right];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(GrepError::PublicationConflict { .. })))
            .count(),
        1
    );
    let writes = store.take();
    let manifest_writes: Vec<_> = writes.iter().filter(|operation| matches!(operation, RecordedOperation::Put { key, .. } if key.starts_with(&manifests_prefix(&namespace_id)))).collect();
    assert_eq!(manifest_writes.len(), 2);
    assert!(manifest_writes.iter().all(|operation| matches!(operation, RecordedOperation::Put { key, mode: PutMode::CreateIfAbsent, .. } if key == &manifest_key(&namespace_id, &ManifestNo(2)))));
    assert!(writes.iter().filter(|operation| matches!(operation, RecordedOperation::CompareAndSwap { .. } | RecordedOperation::Put { mode: PutMode::CompareAndSwap { .. }, .. })).all(|operation| matches!(operation, RecordedOperation::CompareAndSwap { key, .. } if key == &hint_key(&namespace_id))));
    let current = load_current_grep_manifest(&store, &namespace_id)
        .await
        .expect("discover")
        .expect("enabled");
    let replanned = state(
        namespace_id.clone(),
        current.manifest_no().successor().expect("successor"),
        RunNo(current.manifest_state().index().next_run_no.0 + 1),
    );
    let published = publish_grep_manifest(&store, Some(&current), &replanned, &deadline)
        .await
        .expect("replanned publication");
    assert_eq!(published.manifest_no(), ManifestNo(3));
    assert_eq!(published.manifest_state().index().next_run_no, RunNo(2));
    assert_eq!(
        store
            .list_prefix(&manifests_prefix(&namespace_id))
            .await
            .expect("manifests")
            .len(),
        3
    );
    let raised = raise_grep_hint(&store, &namespace_id, ManifestNo(2), first.hint, &deadline)
        .await
        .expect("stale hint raise");
    assert_eq!(raised.state.manifest_no, ManifestNo(3));
}

#[tokio::test]
async fn discovery_follows_a_lagging_hint_and_rejects_missing_or_misnamed_manifests() {
    let directory = tempfile::tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let namespace_id = namespace_id("docs");
    assert!(load_current_grep_manifest(&store, &namespace_id)
        .await
        .expect("absent hint")
        .is_none());
    write_hint(&store, &namespace_id, ManifestNo(1)).await;
    assert!(load_current_grep_manifest(&store, &namespace_id)
        .await
        .expect("unfinished enable")
        .is_none());
    for number in 1..=3 {
        let state = state(namespace_id.clone(), ManifestNo(number), RunNo(number));
        store
            .put_if_absent(
                &manifest_key(&namespace_id, &ManifestNo(number)),
                Bytes::from(encode_grep_manifest(state).expect("manifest").into_bytes()),
            )
            .await
            .expect("manifest");
    }
    assert_eq!(
        load_current_grep_manifest(&store, &namespace_id)
            .await
            .expect("lagging hint")
            .expect("enabled")
            .manifest_no(),
        ManifestNo(3)
    );
    store
        .delete(&hint_key(&namespace_id))
        .await
        .expect("delete hint");
    assert!(load_current_grep_manifest(&store, &namespace_id)
        .await
        .expect("missing hint")
        .is_none());
    write_hint(&store, &namespace_id, ManifestNo(4)).await;
    assert!(matches!(
        load_current_grep_manifest(&store, &namespace_id).await,
        Err(GrepManifestError::Corrupt { .. })
    ));
    let wrong = state(namespace_id.clone(), ManifestNo(5), RunNo(0));
    store
        .put_if_absent(
            &manifest_key(&namespace_id, &ManifestNo(4)),
            Bytes::from(encode_grep_manifest(wrong).expect("manifest").into_bytes()),
        )
        .await
        .expect("misnamed manifest");
    assert!(matches!(
        load_grep_manifest(&store, &namespace_id, ManifestNo(4)).await,
        Err(GrepManifestError::Corrupt { .. })
    ));
}

#[tokio::test]
async fn a_failed_hint_raise_leaves_a_discoverable_publication_and_invalid_candidates_write_nothing(
) {
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = namespace_id("docs");
    let failing = FailStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(hint_key(&namespace_id)),
        OperationClass::CompareAndSwap,
        InjectedError::Transport("unavailable".to_owned()),
    );
    failing.fail_all();
    let store = RecordingStore::new(failing, KeyPredicate::any());
    let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
    let first = publish_grep_manifest(
        &store,
        None,
        &state(namespace_id.clone(), ManifestNo(1), RunNo(0)),
        &deadline,
    )
    .await
    .expect("enable");
    let next = state(namespace_id.clone(), ManifestNo(2), RunNo(1));
    publish_grep_manifest(&store, Some(&first), &next, &deadline)
        .await
        .expect("publication survives hint failure");
    let loaded = load_current_grep_manifest(&store, &namespace_id)
        .await
        .expect("discover")
        .expect("enabled");
    assert_eq!(loaded.hint.state.manifest_no, ManifestNo(1));
    assert_eq!(loaded.manifest_no(), ManifestNo(2));
    store.reset();
    assert!(
        publish_grep_manifest(&store, Some(&loaded), &next, &deadline)
            .await
            .is_err()
    );
    let wrong = state(
        loonfs_test_support::ids::namespace_id("other"),
        ManifestNo(3),
        RunNo(2),
    );
    assert!(
        publish_grep_manifest(&store, Some(&loaded), &wrong, &deadline)
            .await
            .is_err()
    );
    let clock = Arc::new(ManualClock::new(0));
    let expired = Deadline::start(clock.clone());
    clock.advance_ms(loonfs::METADATA_PUBLICATION_BUDGET_MS + 1);
    let valid = state(namespace_id, ManifestNo(3), RunNo(2));
    assert!(
        publish_grep_manifest(&store, Some(&loaded), &valid, &expired)
            .await
            .is_err()
    );
    assert_eq!(store.counts().puts, 0);
}

async fn write_hint(store: &impl ObjectStore, namespace_id: &NamespaceId, manifest_no: ManifestNo) {
    let hint = GrepHint {
        namespace_id: namespace_id.clone(),
        manifest_no,
    };
    store
        .put_overwrite(
            &hint_key(namespace_id),
            Bytes::from(encode_grep_hint(hint).expect("hint").into_bytes()),
        )
        .await
        .expect("write hint");
}

fn state(
    namespace_id: NamespaceId,
    manifest_no: ManifestNo,
    next_run_no: RunNo,
) -> GrepManifestState {
    GrepManifestState::new(
        namespace_id,
        manifest_no,
        GrepIndexStatus::Active {
            built_through_seq: ChangeSeq(0),
            next_event_index: 0,
        },
        GrepIndexState {
            reorganize: None,
            next_run_no,
        },
        Vec::new(),
    )
    .expect("valid manifest")
}

#[tokio::test]
async fn missing_hint_etags_fail_without_repeated_store_attempts() {
    use loonfs_test_support::stores::MetadataMapStore;
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = namespace_id("etag");
    let raw = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let store = RecordingStore::new(
        MetadataMapStore::without_etag(raw.clone(), KeyPredicate::exact(hint_key(&namespace_id))),
        KeyPredicate::any(),
    );
    let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
    let first = state(namespace_id.clone(), ManifestNo(1), RunNo(0));
    assert!(matches!(
        publish_grep_manifest(&store, None, &first, &deadline).await,
        Err(GrepError::StoreUnavailable { .. })
    ));
    assert_eq!(store.counts().puts, 1);
    assert!(load_grep_manifest(&raw, &namespace_id, ManifestNo(1))
        .await
        .expect("manifest")
        .is_none());
    assert!(matches!(
        load_current_grep_manifest(&store, &namespace_id).await,
        Err(GrepManifestError::Store { .. })
    ));
    let published = publish_grep_manifest(&raw, None, &first, &deadline)
        .await
        .expect("first manifest");
    store.reset();
    assert!(matches!(
        raise_grep_hint(
            &store,
            &namespace_id,
            ManifestNo(2),
            published.hint,
            &deadline
        )
        .await,
        Err(GrepManifestError::Store { .. })
    ));
    assert_eq!(
        store
            .take()
            .iter()
            .filter(|operation| matches!(operation, RecordedOperation::CompareAndSwap { .. }))
            .count(),
        1
    );
}

#[tokio::test]
async fn ambiguous_manifest_puts_reconcile_the_exact_landed_state() {
    for apply in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let namespace_id = namespace_id("ambiguous");
        let store = FailStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::prefix(manifests_prefix(&namespace_id)),
            OperationClass::PutCreateIfAbsent,
            InjectedError::Transport("lost acknowledgement".to_owned()),
        );
        let store = if apply {
            store.apply_then_fail()
        } else {
            store
        };
        let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
        let first = publish_grep_manifest(
            &store,
            None,
            &state(namespace_id.clone(), ManifestNo(1), RunNo(0)),
            &deadline,
        )
        .await
        .expect("first");
        store.fail_next(1);
        let next = state(namespace_id.clone(), ManifestNo(2), RunNo(1));
        let result = publish_grep_manifest(&store, Some(&first), &next, &deadline).await;
        if apply {
            assert_eq!(result.expect("landed manifest").manifest_state(), &next);
        } else {
            assert!(matches!(result, Err(GrepError::PublicationConflict { .. })));
            let current = load_current_grep_manifest(&store, &namespace_id)
                .await
                .expect("reload")
                .expect("current");
            publish_grep_manifest(&store, Some(&current), &next, &deadline)
                .await
                .expect("caller replans");
        }
        store.fail_next(1);
        let different = state(namespace_id.clone(), ManifestNo(2), RunNo(2));
        assert!(matches!(
            publish_grep_manifest(&store, Some(&first), &different, &deadline).await,
            Err(GrepError::PublicationConflict { .. })
        ));
    }
}

#[tokio::test]
async fn regressing_successors_fail_on_publication_and_discovery() {
    for (before_run, after_run, before_seq, after_seq, before_event, after_event) in [
        (2, 1, 4, 4, 0, 0),
        (2, 2, 4, 3, 0, 0),
        (2, 2, 4, 4, 0, 1),
        (2, 2, 4, 4, 2, 1),
    ] {
        let directory = tempfile::tempdir().expect("directory");
        let namespace_id = namespace_id("successor");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        let manifest = |number, run, seq, event| {
            GrepManifestState::new(
                namespace_id.clone(),
                ManifestNo(number),
                GrepIndexStatus::Active {
                    built_through_seq: ChangeSeq(seq),
                    next_event_index: event,
                },
                GrepIndexState {
                    next_run_no: RunNo(run),
                    reorganize: None,
                },
                Vec::new(),
            )
            .expect("state")
        };
        let deadline = Deadline::start(Arc::new(StdMonotonicTimer::default()));
        let first = publish_grep_manifest(
            &store,
            None,
            &manifest(1, before_run, before_seq, before_event),
            &deadline,
        )
        .await
        .expect("first");
        let next = manifest(2, after_run, after_seq, after_event);
        store.reset();
        assert!(matches!(
            publish_grep_manifest(&store, Some(&first), &next, &deadline).await,
            Err(GrepError::CorruptIndex { .. })
        ));
        assert_eq!(store.counts().puts, 0);
        store
            .put_if_absent(
                &manifest_key(&namespace_id, &ManifestNo(2)),
                Bytes::from(encode_grep_manifest(next).expect("encode").into_bytes()),
            )
            .await
            .expect("corrupt chain");
        assert!(matches!(
            load_current_grep_manifest(&store, &namespace_id).await,
            Err(GrepManifestError::Corrupt { .. })
        ));
    }
}

#[tokio::test]
async fn a_late_ambiguous_put_cannot_confirm_a_recreated_manifest() {
    use crate::common::GrepHost;
    use loonfs::{CreateNamespaceOptions, FsWriter, SharedObjectStore, StoreFailureClass};
    use loonfs_test_support::stores::{BlockingStore, MetadataMapStore};

    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = namespace_id("late-manifest");
    let store: SharedObjectStore = Arc::new(MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(manifests_prefix(&namespace_id)),
    ));
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("late-manifest")
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(
            &namespace_id,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    let host = GrepHost::new(&store, "collector").await;
    let deadline = Deadline::start(Arc::new(ManualClock::new(0)));
    let first = publish_grep_manifest(
        &store,
        None,
        &state(namespace_id.clone(), ManifestNo(1), RunNo(0)),
        &deadline,
    )
    .await
    .expect("first manifest");
    let candidate = state(namespace_id.clone(), ManifestNo(2), RunNo(1));
    let object_key = manifest_key(&namespace_id, &ManifestNo(2));
    let recorded = Arc::new(RecordingStore::new(store.clone(), KeyPredicate::any()));
    let failing = FailStore::new(
        recorded.clone(),
        KeyPredicate::exact(&object_key),
        OperationClass::PutCreateIfAbsent,
        InjectedError::Transport("lost acknowledgement".to_owned()),
    )
    .apply_then_fail();
    failing.fail_next(1);
    let blocked = BlockingStore::new(
        failing,
        KeyPredicate::exact(&object_key),
        OperationClass::PutCreateIfAbsent,
    );
    let timer = Arc::new(ManualClock::new(0));
    let late_deadline = Deadline::start(timer.clone());
    blocked.block_next();
    let (outcome, ()) = tokio::join!(
        publish_grep_manifest(&blocked, Some(&first), &candidate, &late_deadline),
        async {
            blocked.wait_until_blocked().await;
            let second = publish_grep_manifest(&store, Some(&first), &candidate, &deadline)
                .await
                .expect("second manifest");
            publish_grep_manifest(
                &store,
                Some(&second),
                &state(namespace_id.clone(), ManifestNo(3), RunNo(2)),
                &deadline,
            )
            .await
            .expect("third manifest");
            timer.advance_ms(loonfs_grep::GREP_GC_GRACE_WINDOW_MS + 1);
            let report = host
                .worker
                .garbage_collect_namespace(&namespace_id, timer.now_ms())
                .await
                .expect("collect old manifests");
            assert_eq!(report.deleted_other_objects, 2);
            assert!(store
                .get(&object_key, None)
                .await
                .expect("collected manifest")
                .is_none());
            recorded.reset();
            blocked.release();
        }
    );
    assert!(matches!(outcome, Err(GrepError::StoreUnavailable {
        object_key: actual_key, class: StoreFailureClass::RetryableTransport, ..
    }) if actual_key == object_key));
    assert_eq!(blocked.inner().remaining(), 0);
    assert_eq!(recorded.counts().create_if_absent_puts, 1);
    assert_eq!(recorded.counts().compare_and_swaps, 0);
    assert!(recorded
        .take_gets()
        .iter()
        .any(|(key, _)| key == &object_key));
    assert_eq!(
        store
            .get(&object_key, None)
            .await
            .expect("recreated manifest"),
        Some(Bytes::from(
            encode_grep_manifest(candidate)
                .expect("manifest")
                .into_parts()
                .1
        ))
    );
    let current = load_current_grep_manifest(&store, &namespace_id)
        .await
        .expect("current manifest")
        .expect("enabled");
    assert_eq!(current.manifest_no(), ManifestNo(3));
    assert_eq!(current.hint.state.manifest_no, ManifestNo(3));
}
