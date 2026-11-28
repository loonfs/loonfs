//! Inline WAL-to-object handoff while collection and competing folds run.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::test_support::WriterAt;
use loonfs_test_support::stores::{BlockingStore, MetadataMapStore, OperationSelector};

async fn collect_aged_wal(
    store: &Arc<RecordingStore<LocalFsStore>>,
    namespace_id: &NamespaceId,
    context: &WriterAt,
) {
    let aged = MetadataMapStore::aged(
        store.clone(),
        KeyPredicate::prefix(wal_prefix(namespace_id)),
    );
    crate::test_support::namespace_store(&aged, namespace_id)
        .gc(&WriterAt {
            now_ms: crate::limits::GC_GRACE_WINDOW_MS + 1,
            ..context.clone()
        }
        .clock())
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
    let namespace = crate::test_support::namespace_store(store, namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("fresh view");
    for (index, value) in values.iter().enumerate() {
        let file = view
            .read_file_bytes(
                &format!("/{commit_id}-{index}"),
                None,
                &ReadAccess::live(Authorizer::Service),
            )
            .await
            .expect("read committed bytes");
        assert_eq!(file.bytes.as_slice(), value.bytes().as_ref());
    }
}

#[tokio::test]
async fn gc_keeps_inline_wal_until_fold_publication_then_reads_use_objects() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = &engine.namespace.namespace_id.clone();
    let values = vec![
        inline(Bytes::from_static(b"first")),
        inline(Bytes::from_static(b"last")),
        inline(Bytes::new()),
    ];
    publish(&mut engine, &context, candidate("handoff", values.clone()))
        .await
        .expect("publish");
    let wal_before = store
        .list_prefix(&wal_prefix(namespace_id))
        .await
        .expect("list WAL");
    assert!(!wal_before.is_empty());
    let blocked = BlockingStore::new(
        store.clone(),
        span_keys(&store, values[1].content_ref()).await,
        OperationSelector::Put,
    );
    blocked.block_next();
    let timer = Arc::new(StdMonotonicTimer::default());
    let deadline = crate::time::OperationClock::start(0, timer.clone());
    let namespace = crate::test_support::namespace_store(&blocked, namespace_id);
    let position = engine.writer_position().expect("a writer position");
    let fold = namespace.fold_wal_tail_in_new_lane(&position, &deadline);
    let collect_during_fold = async {
        blocked.wait_until_blocked().await;
        assert!(crate::test_support::span_object_keys(
            &store,
            &pool_id(&store).await,
            &values[1].content_ref().content_id
        )
        .await
        .is_empty());
        collect_aged_wal(&store, namespace_id, &context).await;
        assert_eq!(
            store
                .list_prefix(&wal_prefix(namespace_id))
                .await
                .expect("WAL"),
            wal_before
        );
        assert_files_readable(&store, namespace_id, "handoff", &values).await;
        blocked.release();
    };
    let (folded, ()) = tokio::join!(fold, collect_during_fold);
    assert_eq!(
        folded.expect("fold").response.outcome,
        FoldWalOutcome::Published
    );
    collect_aged_wal(&store, namespace_id, &context).await;
    assert!(crate::test_support::commit_wal_keys(&store, namespace_id)
        .await
        .is_empty());
    assert_files_readable(&store, namespace_id, "handoff", &values).await;
}

/// Checks that a fold that another writer's claim fences at its turn, after
/// it wrote its content objects, leaves the files readable: the new writer
/// folds the same tail, a collection deletes the folded WAL objects, and
/// the objects that the new writer's manifest lists stay.
#[tokio::test]
async fn a_fenced_fold_leaves_the_new_writers_fold_of_its_tail_readable() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = &engine.namespace.namespace_id.clone();
    let values = vec![inline(Bytes::from_static(b"shared"))];
    publish(
        &mut engine,
        &context,
        candidate("fenced-gc", values.clone()),
    )
    .await
    .expect("publish");
    let input = engine.writer_position().expect("tail");
    let blocked = BlockingStore::new(
        store.clone(),
        KeyPredicate::content_blob(),
        OperationSelector::Put,
    );
    blocked.block_next();
    let deadline = crate::time::OperationClock::start(0, Arc::new(StdMonotonicTimer::default()));
    let namespace = crate::test_support::namespace_store(&blocked, namespace_id);
    let fenced = namespace.fold_wal_tail_in_new_lane(&input, &deadline);
    let new_writer = async {
        blocked.wait_until_blocked().await;
        let folded = crate::test_support::namespace_store(&store, namespace_id)
            .fold_as_test_writer()
            .await
            .expect("the new writer's fold");
        assert_eq!(folded.response.outcome, FoldWalOutcome::Published);
        collect_aged_wal(&store, namespace_id, &context).await;
        assert!(crate::test_support::commit_wal_keys(&store, namespace_id)
            .await
            .is_empty());
        assert_files_readable(&store, namespace_id, "fenced-gc", &values).await;
        blocked.release();
    };
    let (fenced, ()) = tokio::join!(fenced, new_writer);
    assert!(matches!(fenced, Err(CoreError::WriterFenced(_))));
    assert_files_readable(&store, namespace_id, "fenced-gc", &values).await;
}
