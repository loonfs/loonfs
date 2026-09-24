//! Inline WAL-to-object handoff while collection and competing folds run.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::gc::{gc_namespace, GcConfig};
use crate::storage::content::content_object_key_for_ref;
use loonfs_test_support::stores::{BlockingStore, MetadataMapStore, OperationClass};

async fn collect_aged_wal(
    store: &Arc<RecordingStore<LocalFsStore>>,
    namespace_id: &NamespaceId,
    context: &MutationContext,
) {
    let config = GcConfig::default();
    let aged = MetadataMapStore::aged(
        store.clone(),
        KeyPredicate::prefix(wal_segment_prefix(namespace_id)),
    );
    gc_namespace(
        &aged,
        namespace_id,
        &config,
        &MutationContext {
            now_ms: config.grace_window_ms + 1,
            ..context.clone()
        },
    )
    .await
    .expect("collect aged WAL");
}

async fn assert_files_readable(
    store: &Arc<RecordingStore<LocalFsStore>>,
    namespace_id: &NamespaceId,
    commit_id: &str,
    values: &[InlineContent],
) {
    // A fresh view must work without the publishing engine's cached WAL bytes.
    let view = load_current_metadata_view(store, namespace_id)
        .await
        .expect("fresh view");
    for (index, value) in values.iter().enumerate() {
        let file = view
            .get_file_bytes(
                store,
                &format!("/{commit_id}-{index}"),
                None,
                &ReadAccess::live(Authorizer::Unrestricted),
            )
            .await
            .expect("read committed bytes");
        assert_eq!(file.bytes.as_slice(), value.bytes().as_ref());
    }
}

#[tokio::test]
async fn gc_keeps_inline_wal_until_fold_publication_then_reads_use_objects() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = &engine.namespace_id.clone();
    let values = vec![
        inline(namespace_id, Bytes::from_static(b"first")),
        inline(namespace_id, Bytes::from_static(b"last")),
        inline(namespace_id, Bytes::new()),
    ];
    publish(
        &mut engine,
        &store,
        &context,
        candidate("handoff", values.clone()),
    )
    .await
    .expect("publish");
    let wal_before = store
        .list_prefix(&wal_segment_prefix(namespace_id))
        .await
        .expect("list WAL");
    assert!(!wal_before.is_empty());
    let key = content_object_key_for_ref(values[1].content_ref()).expect("content key");
    let blocked = BlockingStore::new(
        store.clone(),
        KeyPredicate::exact(&key),
        OperationClass::Put,
    );
    blocked.block_next();
    let timer = Arc::new(StdMonotonicTimer::default());
    let deadline = crate::time::Deadline::start(timer.clone());
    let fold = fold_wal_tail(
        &blocked,
        None,
        namespace_id,
        engine.wal_fold_input(),
        &deadline,
    );
    let collect_during_fold = async {
        blocked.wait_until_blocked().await;
        assert!(store
            .get(&key, None)
            .await
            .expect("get blocked object")
            .is_none());
        collect_aged_wal(&store, namespace_id, &context).await;
        assert_eq!(
            store
                .list_prefix(&wal_segment_prefix(namespace_id))
                .await
                .expect("WAL"),
            wal_before
        );
        assert_files_readable(&store, namespace_id, "handoff", &values).await;
        blocked.release();
    };
    let (folded, ()) = tokio::join!(fold, collect_during_fold);
    assert_eq!(folded.expect("fold").outcome, FlushWalOutcome::Published);
    collect_aged_wal(&store, namespace_id, &context).await;
    assert!(store
        .list_prefix(&wal_segment_prefix(namespace_id))
        .await
        .expect("collected WAL")
        .is_empty());
    assert_files_readable(&store, namespace_id, "handoff", &values).await;
}

#[tokio::test]
async fn losing_fold_keeps_objects_after_the_winners_wal_is_collected() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = &engine.namespace_id.clone();
    let values = vec![inline(namespace_id, Bytes::from_static(b"shared"))];
    publish(
        &mut engine,
        &store,
        &context,
        candidate("competing-gc", values.clone()),
    )
    .await
    .expect("publish");
    let input = engine.wal_fold_input().expect("tail");
    let blocked = BlockingStore::new(
        store.clone(),
        KeyPredicate::content_blob(),
        OperationClass::Put,
    );
    blocked.block_next();
    let timer = Arc::new(StdMonotonicTimer::default());
    let deadline = crate::time::Deadline::start(timer.clone());
    let loser = fold_wal_tail(&blocked, None, namespace_id, Some(input.clone()), &deadline);
    let winner = async {
        blocked.wait_until_blocked().await;
        let folded = fold_wal_tail(
            &store,
            None,
            namespace_id,
            Some(input),
            &crate::time::Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .expect("winning fold");
        assert_eq!(folded.outcome, FlushWalOutcome::Published);
        collect_aged_wal(&store, namespace_id, &context).await;
        assert!(store
            .list_prefix(&wal_segment_prefix(namespace_id))
            .await
            .expect("collected WAL")
            .is_empty());
        assert_files_readable(&store, namespace_id, "competing-gc", &values).await;
        blocked.release();
    };
    let (lost, ()) = tokio::join!(loser, winner);
    assert_eq!(
        lost.expect("losing fold").outcome,
        FlushWalOutcome::ManifestAdvanced
    );
    assert_files_readable(&store, namespace_id, "competing-gc", &values).await;
}
