//! Receipt recovery after a lost WAL acknowledgment, folding, and collection.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::gc::GcOptions;
use crate::test_support::WriterAt;
use loonfs_test_support::stores::{FailStore, InjectedError, MetadataMapStore, OperationSelector};
use loonfs_types::ErrorCode;
use std::assert_matches;

#[tokio::test]
async fn inline_retry_after_lost_ack_and_wal_collection_replays_the_original_commit() {
    let (_directory, store, engine, context) = setup().await;
    let namespace_id = engine.namespace.namespace_id.clone();
    let original = inline(&namespace_id, Bytes::from_static(b"durable bytes"));
    let failed = FailStore::new(
        store.clone(),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
        OperationSelector::PutCreateIfAbsent,
        InjectedError::Transport("lost WAL acknowledgment".into()),
    )
    .apply_then_fail();
    failed.fail_next(1);
    // The writer session publishes through the store that loses the
    // acknowledgment, which passes later requests through to `store`.
    let mut engine = NamespaceCommitEngine {
        namespace: crate::test_support::namespace_store(&failed, &namespace_id),
        writer_id: engine.writer_id,
        position: engine.position,
    };
    let error = engine
        .publish_batch(
            [candidate("lost-ack", vec![original.clone()])],
            &context.clock(),
        )
        .await
        .results
        .pop()
        .expect("result")
        .expect_err("the landed write has an unknown outcome");
    assert_eq!(error.code(), ErrorCode::CommitOutcomeUnknown);
    assert_eq!(failed.attempts(), 1);
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let landed = namespace
        .load_current_metadata_view()
        .await
        .expect("landed commit");
    assert_eq!(landed.head().seq, ChangeSeq(1));
    drop(landed);

    // Remove the physical evidence from WAL before the client retries. The
    // folded receipt and commit record must supply the original result.
    assert_eq!(
        crate::test_support::namespace_store(&store, &namespace_id)
            .fold_wal()
            .await
            .expect("fold landed commit")
            .outcome,
        FoldWalOutcome::Published
    );
    let options = GcOptions::default();
    let aged = MetadataMapStore::aged(
        store.clone(),
        KeyPredicate::prefix(wal_prefix(&namespace_id)),
    );
    let report = crate::test_support::namespace_store(&aged, &namespace_id)
        .gc(
            None,
            &options,
            &WriterAt {
                now_ms: options.grace_window_ms + 1,
                ..context.clone()
            }
            .clock(),
        )
        .await
        .expect("collect folded WAL");
    assert_eq!(report.deleted.wal_objects, 2);
    assert!(store
        .list_prefix(&wal_prefix(&namespace_id))
        .await
        .expect("WAL list")
        .is_empty());

    // Inline retry identity is based on bytes, not the newly allocated ID.
    let retried_value = inline(&namespace_id, original.bytes().clone());
    assert_ne!(original.content_ref(), retried_value.content_ref());
    let retry = candidate("lost-ack", vec![retried_value]);
    store.reset();
    let committed = publish(&mut engine, &context, retry.clone())
        .await
        .expect("recover the original receipt");
    assert_eq!(committed.committed_seq, ChangeSeq(1));
    assert_eq!(committed.committed_at_ms, context.now_ms);
    assert_no_writes(&store);

    drop(engine);
    let mut restarted = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(&store, &namespace_id),
        context.writer_id.clone(),
    );
    restarted
        .position
        .session_writer_epoch(&restarted.namespace, &context.writer_id)
        .await
        .expect("new writer fence");
    store.reset();
    assert_eq!(
        publish(&mut restarted, &context, retry)
            .await
            .expect("replay after restart"),
        committed
    );
    let changed = candidate(
        "lost-ack",
        vec![inline(&namespace_id, Bytes::from_static(b"changed bytes"))],
    );
    assert_matches!(
        publish(&mut restarted, &context, changed).await,
        Err(CoreError::CommitIdReuseConflict { .. })
    );
    assert_no_writes(&store);

    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let view = namespace
        .load_current_metadata_view()
        .await
        .expect("fresh view");
    assert_eq!(view.head().seq, ChangeSeq(1));
    let file = view
        .read_file_bytes(
            "/lost-ack-0",
            None,
            &ReadAccess::live(Authorizer::Unrestricted),
        )
        .await
        .expect("original bytes survive collection");
    assert_eq!(file.bytes.as_slice(), original.bytes().as_ref());
    let entry = view
        .resolve_path(
            "/lost-ack-0",
            AttributeInclusion::Omit,
            &ReadAccess::live(Authorizer::Unrestricted),
        )
        .await
        .expect("original reference");
    assert_matches!(
        entry.kind,
        PathEntryKind::File { content_ref: Some(content_ref), .. }
            if content_ref == *original.content_ref()
    );
}
