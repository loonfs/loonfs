//! The read position that each writer session keeps for reads in its runtime.

use super::*;

/// Creates a writer and a namespace, and commits one directory through the
/// writer's session.
async fn published_namespace(
    store: SharedStore,
) -> (
    crate::LoonFs<crate::Writable>,
    crate::Namespace<crate::Writable>,
) {
    let writer = test_writer_with_interval(store, 0).await;
    let namespace = publish_once_into_each(
        &writer,
        &[NamespaceId::parse("demo").expect("valid namespace id")],
    )
    .await
    .pop()
    .expect("one namespace");
    (writer, namespace)
}

async fn current_manifest_no(
    store: &SharedStore,
    namespace_id: &NamespaceId,
) -> loonfs_types::ManifestNo {
    loonfs_core::control::load_namespace_current_manifest(store, namespace_id)
        .await
        .expect("current manifest")
        .state
        .manifest()
        .manifest_no
}

/// Checks that the position moves to each commit before the commit's caller
/// hears the result, and that a read-only view of the writable runtime finds
/// the same position.
#[tokio::test]
async fn the_read_position_follows_each_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store.clone()).await;
    let namespace_id = namespace.id().clone();
    for name in ["second", "third"] {
        let commit = namespace
            .commit_candidate(CommitCandidate::new(create_directory_request(name, name)))
            .await
            .expect("commit");
        let position = writer
            .core
            .writer_position(&namespace_id)
            .expect("a position after a commit");
        assert_eq!(position.head.seq, commit.committed_seq, "{name}");
        assert_eq!(
            position.basis.manifest_no(),
            current_manifest_no(&store, &namespace_id).await,
            "{name}"
        );
        assert_eq!(position.tail_key.head_seq, commit.committed_seq, "{name}");
        assert_eq!(
            position.tail_key.manifest_no,
            position.basis.manifest_no(),
            "{name}"
        );
    }
    let reader = writer.read_only();
    assert_eq!(
        reader
            .core
            .writer_position(&namespace_id)
            .map(|position| position.head.seq),
        writer
            .core
            .writer_position(&namespace_id)
            .map(|position| position.head.seq),
        "a read-only view shares the writer's core"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a fold through maintenance moves the position to the fold's
/// manifest at the same head.
#[tokio::test]
async fn a_fold_moves_the_read_position_to_its_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store).await;
    let namespace_id = namespace.id().clone();
    let before = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after a commit");
    let folded = writer
        .maintenance(loonfs_test_support::ids::writer_id("writer-a"))
        .fold_wal(&namespace_id)
        .await
        .expect("fold");
    assert_eq!(folded.outcome, FoldWalOutcome::Published);
    let after = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after the fold");
    assert_eq!(after.basis.manifest_no(), folded.manifest_no);
    assert!(after.basis.manifest_no() > before.basis.manifest_no());
    assert_eq!(after.head.seq, before.head.seq);
    assert_eq!(after.head.folded_wal_no, before.head.wal_no);
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a change of unknown effect drops the position at once when
/// the engine is idle, and that the next commit stores one again.
#[tokio::test]
async fn an_unknown_change_drops_the_read_position_until_the_next_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store).await;
    let namespace_id = namespace.id().clone();
    writer
        .mode
        .publisher
        .invalidate_namespace(&namespace_id, NamespaceChange::Unknown);
    assert!(writer.core.writer_position(&namespace_id).is_none());
    let commit = namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "after", "after",
        )))
        .await
        .expect("commit");
    assert_eq!(
        writer
            .core
            .writer_position(&namespace_id)
            .map(|position| position.head.seq),
        Some(commit.committed_seq)
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a drop that arrives while a publish has the engine locked
/// clears the position at once, and that the publish applies the drop when
/// it releases the engine.
#[tokio::test]
async fn a_drop_during_a_publish_clears_the_read_position_at_once() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let (writer, namespace) = published_namespace(store.clone() as SharedStore).await;
    store.block_next();
    let (during, ()) = futures::join!(
        namespace.commit_candidate(CommitCandidate::new(create_directory_request(
            "during", "during"
        ))),
        async {
            store.wait_until_blocked().await;
            assert!(writer.core.writer_position(&namespace_id).is_some());
            writer
                .mode
                .publisher
                .invalidate_namespace(&namespace_id, NamespaceChange::Unknown);
            assert!(
                writer.core.writer_position(&namespace_id).is_none(),
                "the queued drop clears the position before the publish ends"
            );
            store.release();
        }
    );
    during.expect("the publish succeeds");
    assert!(
        writer.core.writer_position(&namespace_id).is_none(),
        "the publish applied the drop when it released the engine"
    );
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a manifest that this runtime publishes while a publish has
/// the engine locked leaves the position for reads in place until the
/// publish releases the engine, which then moves the position to that
/// manifest.
#[tokio::test]
async fn a_manifest_published_during_a_publish_moves_the_read_position_when_it_ends() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = Arc::new(blocking_publication_store(temp_dir.path(), &namespace_id));
    let (writer, namespace) = published_namespace(store.clone() as SharedStore).await;
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id("writer-a"));
    let before = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after a commit");
    store.block_next();
    let (during, checkpoint) = futures::join!(
        namespace.commit_candidate(CommitCandidate::new(create_directory_request(
            "during", "during"
        ))),
        async {
            store.wait_until_blocked().await;
            let checkpoint = maintenance
                .create_checkpoint(&namespace_id, "during")
                .await
                .expect("checkpoint");
            let kept = writer
                .core
                .writer_position(&namespace_id)
                .expect("reads keep the position while the publish runs");
            assert_eq!(kept.basis.manifest_no(), before.basis.manifest_no());
            store.release();
            checkpoint
        }
    );
    let during = during.expect("the publish succeeds");
    let after = writer
        .core
        .writer_position(&namespace_id)
        .expect("a position after the publish");
    assert_eq!(after.basis.manifest_no(), checkpoint.manifest_no);
    assert_eq!(after.head.seq, during.committed_seq);
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a fenced session keeps no position, so reads in its runtime
/// stop starting from the head it last published.
#[tokio::test]
async fn a_fenced_session_keeps_no_read_position() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store.clone()).await;
    let rival = crate::LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id("writer-b"))
        .build()
        .await
        .expect("rival");
    rival
        .open_namespace(namespace.id())
        .expect("open namespace")
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "rival", "rival",
        )))
        .await
        .expect("the rival takes over the namespace");
    assert!(
        namespace
            .session()
            .publisher
            .read_position
            .current()
            .is_some(),
        "the session hasn't found the fence yet"
    );
    let fenced = namespace
        .commit_candidate(CommitCandidate::new(create_directory_request(
            "fenced", "fenced",
        )))
        .await
        .expect_err("the old session is fenced");
    assert_eq!(fenced.code(), ErrorCode::WriterFenced);
    assert!(namespace
        .session()
        .publisher
        .read_position
        .current()
        .is_none());
    assert!(writer.core.writer_position(namespace.id()).is_none());
    rival.shutdown().await.expect("rival shutdown");
    writer.shutdown().await.expect("shutdown");
}

/// Checks that a read-only runtime keeps no writer sessions for its reads.
#[tokio::test]
async fn a_read_only_runtime_has_no_read_positions() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
    let (writer, namespace) = published_namespace(store.clone()).await;
    let reader = crate::LoonFs::builder_with_store(store)
        .read_only()
        .build()
        .await
        .expect("reader");
    assert!(reader.core.inner.head_positions.get().is_none());
    assert!(reader.core.writer_position(namespace.id()).is_none());
    assert!(writer.core.writer_position(namespace.id()).is_some());
    writer.shutdown().await.expect("shutdown");
}
