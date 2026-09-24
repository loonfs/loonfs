//! Receipt recovery after a lost WAL acknowledgment, folding, and collection.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::gc::{gc_namespace, GcConfig};
use loonfs_api::ErrorCode;
use loonfs_test_support::stores::{FailStore, InjectedError, MetadataMapStore, OperationClass};

#[tokio::test]
async fn inline_retry_after_lost_ack_and_wal_collection_replays_the_original_commit() {
    let (_directory, store, mut engine, context) = setup().await;
    let namespace_id = engine.namespace_id.clone();
    let original = inline(&namespace_id, Bytes::from_static(b"durable bytes"));
    let failed = FailStore::new(
        store.clone(),
        KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
        OperationClass::PutCreateIfAbsent,
        InjectedError::Transport("lost WAL acknowledgment".into()),
    )
    .apply_then_fail();
    failed.fail_next(1);
    let error = engine
        .publish_batch(
            &failed,
            [candidate("lost-ack", vec![original.clone()])],
            &context,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .pop()
        .expect("result")
        .expect_err("the landed write has an unknown outcome");
    assert_eq!(error.code(), ErrorCode::CommitOutcomeUnknown);
    assert_eq!(failed.attempts(), 1);
    let landed = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("landed commit");
    assert_eq!(landed.head().seq, ChangeSeq(1));
    drop(landed);

    // Remove the physical evidence from WAL before the client retries. The
    // folded receipt and commit record must supply the original result.
    assert_eq!(
        flush_wal(&store, &namespace_id)
            .await
            .expect("fold landed commit")
            .outcome,
        FlushWalOutcome::Published
    );
    let config = GcConfig::default();
    let aged = MetadataMapStore::aged(
        store.clone(),
        KeyPredicate::prefix(wal_segment_prefix(&namespace_id)),
    );
    let report = gc_namespace(
        &aged,
        &namespace_id,
        &config,
        &MutationContext {
            now_ms: config.grace_window_ms + 1,
            ..context.clone()
        },
    )
    .await
    .expect("collect folded WAL");
    assert_eq!(report.deleted.wal_segments, 2);
    assert!(store
        .list_prefix(&wal_segment_prefix(&namespace_id))
        .await
        .expect("WAL list")
        .is_empty());

    // Inline retry identity is based on bytes, not the newly allocated ID.
    let retried_value = inline(&namespace_id, original.bytes().clone());
    assert_ne!(original.content_ref(), retried_value.content_ref());
    let retry = candidate("lost-ack", vec![retried_value]);
    store.reset();
    let committed = publish(&mut engine, &store, &context, retry.clone())
        .await
        .expect("recover the original receipt");
    assert_eq!(committed.committed_seq, ChangeSeq(1));
    assert_eq!(committed.committed_at_ms, context.now_ms);
    assert_no_writes(&store);

    drop(engine);
    let mut restarted = NamespaceCommitEngine::new(namespace_id.clone());
    restarted
        .session_writer_epoch(&store, &context)
        .await
        .expect("new writer fence");
    store.reset();
    assert_eq!(
        publish(&mut restarted, &store, &context, retry)
            .await
            .expect("replay after restart"),
        committed
    );
    let changed = candidate(
        "lost-ack",
        vec![inline(&namespace_id, Bytes::from_static(b"changed bytes"))],
    );
    assert!(matches!(
        publish(&mut restarted, &store, &context, changed).await,
        Err(CoreError::CommitIdReuseConflict { .. })
    ));
    assert_no_writes(&store);

    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("fresh view");
    assert_eq!(view.head().seq, ChangeSeq(1));
    let file = view
        .get_file_bytes(
            &store,
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
    assert!(matches!(entry.kind, PathEntryKind::File { content_ref, .. }
        if content_ref == *original.content_ref()));
}
