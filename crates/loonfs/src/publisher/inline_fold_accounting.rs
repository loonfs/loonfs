//! Fold completion must not subtract bytes from an already-refreshed tail.

use super::*;

#[tokio::test]
async fn a_delayed_fold_callback_preserves_a_freshly_observed_tail() {
    check_delayed_fold_callback(RuntimeCacheConfig::default()).await;
}

#[tokio::test]
async fn a_delayed_fold_callback_preserves_an_uncached_tail() {
    check_delayed_fold_callback(RuntimeCacheConfig::disabled()).await;
}

async fn check_delayed_fold_callback(cache: RuntimeCacheConfig) {
    let directory = tempdir().expect("directory");
    let namespace = NamespaceId::parse("fold-accounting").expect("namespace");
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(loonfs_objectstore::keys::hint(&namespace)),
        OperationClass::CompareAndSwap,
    ));
    let writer = crate::FsWriter::builder_with_store(store.clone())
        .writer_id("inline-writer")
        .runtime_cache(cache)
        .inline_content(InlineContentOptions {
            inline_content_threshold_bytes: Some(4),
            inline_content_fold_at_bytes: 4,
            inline_content_tail_limit_bytes: 8,
            ..Default::default()
        })
        .min_publish_interval_ms(0)
        .monotonic_timer(Arc::new(ManualClock::new(0)))
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(
            &namespace,
            CreateNamespaceOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("namespace");
    let permits = writer
        .bits
        .wal_fold_permits
        .acquire_many(crate::DEFAULT_MAX_CONCURRENT_FOLDS as u32)
        .await
        .expect("hold automatic folds");
    writer
        .put_file_bytes(&namespace, "/first", b"four", put_options("first"))
        .await
        .expect("first inline commit");
    let maintenance = writer
        .maintenance_handle("maintenance")
        .expect("maintenance");
    store.block_next();
    let fold = maintenance.flush_wal(&namespace);
    let publish_after_fold = async {
        timeout(Duration::from_secs(10), store.wait_until_blocked())
            .await
            .expect("fold reached its hint update");
        // The immutable manifest is durable; its best-effort hint update waits.
        let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
            .await
            .expect("observe the completed fold");
        assert_eq!(usage.wal_tail_inline_bytes, 0);
        writer.invalidate_namespace(&namespace);
        writer
            .put_file_bytes(&namespace, "/during", b"next", put_options("during"))
            .await
            .expect("publish against the new manifest");
        assert_eq!(
            writer.publisher().wal_tail_inline_bytes(&namespace).await,
            Some(4)
        );
        store.release();
    };
    let (folded, ()) = tokio::join!(fold, publish_after_fold);
    folded.expect("delayed fold completion");
    assert_eq!(
        writer.publisher().wal_tail_inline_bytes(&namespace).await,
        Some(4)
    );
    let prepared = vec![
        writer
            .prepare_file_bytes(&namespace, b"more")
            .await
            .expect("prepare"),
        writer
            .prepare_file_bytes(&namespace, b"last")
            .await
            .expect("prepare"),
    ];
    let request = CommitRequest {
        commit_id: CommitId::parse("after-fold").expect("commit"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        preconditions: Vec::new(),
        operations: vec![
            put_operation("/more", &prepared[0]),
            put_operation("/last", &prepared[1]),
        ],
    };
    writer
        .commit_candidate(&namespace, CommitCandidate::prepared(request, prepared))
        .await
        .expect("publish with only four inline bytes available");
    let usage = loonfs_core::cache::load_namespace_wal_tail_usage(store.as_ref(), &namespace)
        .await
        .expect("actual tail usage");
    assert_eq!(usage.wal_tail_inline_bytes, 8);
    for (path, bytes) in [
        ("/first", b"four"),
        ("/during", b"next"),
        ("/more", b"more"),
        ("/last", b"last"),
    ] {
        assert_eq!(
            writer
                .reader()
                .get_file_bytes(&namespace, path)
                .await
                .expect("read")
                .bytes,
            bytes
        );
    }
    drop(permits);
    writer.shutdown().await.expect("shutdown");
}

#[test]
fn overlapping_completions_credit_an_observed_basis_only_once() {
    let captured = InlineTailEstimate {
        bytes: 4,
        manifest_no: Some(loonfs_api::ManifestNo(1)),
    };
    let mut slot = EngineSlot {
        engine: None,
        session: Arc::new(Mutex::new(WriterSessionState::default())),
        last_known_inline_tail: Some(InlineTailEstimate {
            bytes: 8,
            ..captured
        }),
    };
    slot.record_successful_fold(Some(captured));
    assert_eq!(
        slot.inline_tail_estimate()
            .expect("remaining estimate")
            .bytes,
        4
    );
    slot.record_successful_fold(Some(captured));
    assert_eq!(
        slot.inline_tail_estimate()
            .expect("not credited twice")
            .bytes,
        4
    );
    let estimated = slot.inline_tail_estimate();
    slot.record_successful_fold(estimated);
    assert_eq!(
        slot.inline_tail_estimate()
            .expect("unknown basis retained")
            .bytes,
        4
    );
}
