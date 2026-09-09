//! Numbered grep publication, hint discovery, and input validation.

#![allow(clippy::panic)]

use bytes::Bytes;
use loonfs_api::{ChangeSeq, ManifestNo, NamespaceId, RunNo};
use loonfs_grep::keyspace::{grep_prefix, hint_key, manifest_key, manifests_prefix};
use loonfs_grep::root::{
    encode_grep_hint, encode_grep_manifest, load_current_grep_manifest, load_grep_manifest,
    publish_grep_manifest, raise_grep_hint, GrepHint, GrepIndexState, GrepIndexStatus,
    GrepManifestState, GrepRootError,
};
use loonfs_grep::GrepError;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::timing::{MonotonicTimer, StdMonotonicTimer};
use loonfs_objectstore::{ObjectStore, PutMode};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    FailStore, InjectedError, KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};

#[tokio::test]
async fn publications_race_one_number_and_the_loser_replans_at_the_next() {
    let directory = tempfile::tempdir().expect("directory");
    let namespace_id = namespace_id("docs");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(grep_prefix(&namespace_id)),
    );
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let first = publish_grep_manifest(
        &store,
        None,
        &state(namespace_id.clone(), ManifestNo(1), RunNo(0)),
        &timer,
        started_ms,
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
        publish_grep_manifest(&store, Some(&first), &candidate, &timer, started_ms),
        publish_grep_manifest(&store, Some(&first), &candidate, &timer, started_ms),
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
    let published = publish_grep_manifest(&store, Some(&current), &replanned, &timer, started_ms)
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
    let raised = raise_grep_hint(
        &store,
        &namespace_id,
        ManifestNo(2),
        first.hint,
        &timer,
        started_ms,
    )
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
        Err(GrepRootError::Corrupt { .. })
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
        Err(GrepRootError::Corrupt { .. })
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
    let timer = StdMonotonicTimer::default();
    let started_ms = timer.monotonic_now_ms();
    let first = publish_grep_manifest(
        &store,
        None,
        &state(namespace_id.clone(), ManifestNo(1), RunNo(0)),
        &timer,
        started_ms,
    )
    .await
    .expect("enable");
    let next = state(namespace_id.clone(), ManifestNo(2), RunNo(1));
    publish_grep_manifest(&store, Some(&first), &next, &timer, started_ms)
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
        publish_grep_manifest(&store, Some(&loaded), &next, &timer, started_ms)
            .await
            .is_err()
    );
    let wrong = state(
        loonfs_test_support::ids::namespace_id("other"),
        ManifestNo(3),
        RunNo(2),
    );
    assert!(
        publish_grep_manifest(&store, Some(&loaded), &wrong, &timer, started_ms)
            .await
            .is_err()
    );
    let expired = FixedTimer(loonfs::METADATA_PUBLICATION_BUDGET_MS + 1);
    let valid = state(namespace_id, ManifestNo(3), RunNo(2));
    assert!(
        publish_grep_manifest(&store, Some(&loaded), &valid, &expired, 0)
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

#[derive(Debug)]
struct FixedTimer(u64);

impl MonotonicTimer for FixedTimer {
    fn monotonic_now_ms(&self) -> u64 {
        self.0
    }
}
