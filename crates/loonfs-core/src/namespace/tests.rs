//! Namespace installation, numbered publication, and fork ownership contracts.

use super::{bootstrap::bootstrap_namespace, control::load_current_manifest, fork::fork_namespace};
use crate::commit_engine::{CommitCandidate, NamespaceCommitEngine};
use crate::context::MutationContext;
use crate::path::read::load_current_metadata_view;
use crate::protocol::PublishTailOptions;
use loonfs_api::{
    AbsolutePath, AttributeInclusion, ChangeSeq, CommitId, ErrorCode, ManifestNo, NamespaceId,
    WalNo, WriterId,
};
use loonfs_objectstore::{
    keys::{content_store, hint, metadata_manifest_object, wal_segment},
    local_fs_store::LocalFsStore,
    ObjectStore,
};
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass, RecordingStore};
use tempfile::tempdir;

fn context() -> MutationContext {
    MutationContext {
        writer_id: WriterId::parse("writer").expect("writer"),
        now_ms: 1_000,
    }
}

fn directory(name: &str) -> CommitCandidate {
    CommitCandidate::new(crate::path::write::CommitRequest::single(
        CommitId::parse(name).expect("commit"),
        loonfs_test_support::test_actor(),
        None,
        crate::path::write::FilesystemOperation::CreateDirectory {
            path: AbsolutePath::parse(format!("/{name}")).expect("path"),
            parents: false,
        },
    ))
}

async fn publish<S: ObjectStore>(
    engine: &mut NamespaceCommitEngine,
    store: &S,
    name: &str,
) -> crate::error::Result<loonfs_api::CommitResponse> {
    engine
        .publish_batch(
            store,
            vec![directory(name)],
            &context(),
            &PublishTailOptions::default(),
        )
        .await
        .results
        .remove(0)
}

#[tokio::test]
async fn creation_installs_descriptor_hint_and_manifest_then_reads_genesis() {
    let directory = tempdir().expect("directory");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    bootstrap_namespace(&store, &namespace_id, &context(), false)
        .await
        .expect("create");
    let manifest = load_current_manifest(&store, &namespace_id)
        .await
        .expect("manifest");
    let payload = manifest.envelope.payload();
    assert_eq!(payload.manifest_no, ManifestNo(1));
    assert!(payload.runs.is_empty());
    assert_eq!(payload.last_folded_wal_no, WalNo(0));
    let puts = store
        .snapshot()
        .into_iter()
        .filter(|operation| {
            matches!(
                operation,
                loonfs_test_support::stores::RecordedOperation::Put { .. }
            )
        })
        .map(|operation| operation.key().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        puts,
        vec![
            content_store(&payload.content_store_id),
            hint(&namespace_id),
            metadata_manifest_object(&namespace_id, &ManifestNo(1))
        ]
    );
    assert_eq!(store.counts().create_if_absent_puts, 3);
    let root = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view")
        .resolve_path("/", AttributeInclusion::Omit)
        .await
        .expect("root");
    assert_eq!(root.inode_id, loonfs_api::ROOT_INODE_ID);
}

#[tokio::test]
async fn two_creations_race_at_manifest_one() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("created").expect("namespace");
    let store = BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::exact(metadata_manifest_object(&namespace_id, &ManifestNo(1))),
        OperationClass::PutCreateIfAbsent,
    );
    store.block_next();
    let context = context();
    let (loser, winner) = futures::join!(
        bootstrap_namespace(&store, &namespace_id, &context, false),
        async {
            store.wait_until_blocked().await;
            let result = bootstrap_namespace(store.inner(), &namespace_id, &context, false).await;
            store.release();
            result
        }
    );
    winner.expect("winner");
    assert_eq!(loser.expect_err("loser").code(), ErrorCode::NamespaceExists);
}

#[tokio::test]
async fn a_number_collision_replans_and_commits_the_next_number_without_a_swap() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("race").expect("namespace");
    let store = std::sync::Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    bootstrap_namespace(&store, &namespace_id, &context(), false)
        .await
        .expect("create");
    let mut first = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut first, &store, "seed").await.expect("seed");
    let mut second = first.clone();
    store.reset();
    let blocked = BlockingStore::new(
        store.clone(),
        KeyPredicate::exact(wal_segment(&namespace_id, &WalNo(3))),
        OperationClass::PutCreateIfAbsent,
    );
    blocked.block_next();
    let (loser, winner) = futures::join!(publish(&mut first, &blocked, "left"), async {
        blocked.wait_until_blocked().await;
        let result = publish(&mut second, &store, "right").await;
        blocked.release();
        result
    });
    assert_eq!(winner.expect("winner").committed_seq, ChangeSeq(2));
    assert_eq!(loser.expect("replanned").committed_seq, ChangeSeq(3));
    assert_eq!(store.counts().create_if_absent_puts, 3);
    assert_eq!(store.counts().compare_and_swaps, 0);
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    for path in ["/left", "/right"] {
        view.resolve_path(path, AttributeInclusion::Omit)
            .await
            .expect("committed");
    }
}

#[tokio::test]
async fn a_stale_writer_collides_with_the_fence_and_writes_nothing_else() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("fencing").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    bootstrap_namespace(&store, &namespace_id, &context(), false)
        .await
        .expect("create");
    let mut stale = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut stale, &store, "old")
        .await
        .expect("old writer");
    let acquired = super::writer_epoch::acquire_writer_epoch(&store, &namespace_id, &context())
        .await
        .expect("takeover fence");
    store.reset();
    assert_eq!(
        publish(&mut stale, &store, "stale")
            .await
            .expect_err("fenced")
            .code(),
        ErrorCode::WriterFenced
    );
    assert_eq!(store.counts().puts, 1);
    assert_eq!(store.counts().compare_and_swaps, 0);
    assert!(store
        .head(&wal_segment(&namespace_id, &WalNo(4)))
        .await
        .expect("next")
        .is_none());
    let session = std::sync::Arc::new(std::sync::Mutex::new(
        crate::commit_engine::WriterSessionState::Acquired(acquired),
    ));
    let mut active = NamespaceCommitEngine::new(namespace_id.clone()).writer_session(session);
    assert_eq!(
        publish(&mut active, &store, "new")
            .await
            .expect("new writer")
            .committed_seq,
        ChangeSeq(2)
    );
}

#[tokio::test]
async fn cold_open_probes_past_a_lagging_hint_and_reads_a_missing_hint_as_absent() {
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("lagging").expect("namespace");
    let store = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    bootstrap_namespace(&store, &namespace_id, &context(), false)
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut engine, &store, "one").await.expect("one");
    publish(&mut engine, &store, "two").await.expect("two");
    let hint_bytes = loonfs_api::wire::control::encode_control_state(
        loonfs_api::wire::control::ControlObjectKind::Hint,
        &loonfs_api::wire::control::HintState {
            namespace_id: namespace_id.clone(),
            manifest_no: ManifestNo(1),
            wal_no: WalNo(0),
        },
    )
    .expect("hint");
    store
        .put_overwrite(&hint(&namespace_id), hint_bytes.into())
        .await
        .expect("lag hint");
    drop(engine);
    store.reset();
    load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("cold view")
        .resolve_path("/two", AttributeInclusion::Omit)
        .await
        .expect("tip");
    assert!(store.snapshot().iter().any(|operation| operation.key()
        == wal_segment(&namespace_id, &WalNo(4))
        && matches!(
            operation,
            loonfs_test_support::stores::RecordedOperation::Get { .. }
        )));
    store
        .delete(&hint(&namespace_id))
        .await
        .expect("remove hint");
    assert_eq!(
        super::status::load_namespace(&store, &namespace_id)
            .await
            .expect_err("missing hint")
            .code(),
        ErrorCode::NamespaceNotFound
    );
}

#[tokio::test]
async fn nested_forks_read_copied_runs_without_source_control_reads() {
    let directory = tempdir().expect("directory");
    let store = LocalFsStore::new(directory.path()).expect("store");
    let source = NamespaceId::parse("source").expect("source");
    let target = NamespaceId::parse("target").expect("target");
    let nested = NamespaceId::parse("nested").expect("nested");
    bootstrap_namespace(&store, &source, &context(), false)
        .await
        .expect("create");
    publish(
        &mut NamespaceCommitEngine::new(source.clone()),
        &store,
        "inherited",
    )
    .await
    .expect("source data");
    fork_namespace(&store, &source, &target, None, &context())
        .await
        .expect("fork");
    let target_manifest = load_current_manifest(&store, &target)
        .await
        .expect("target manifest");
    assert_eq!(target_manifest.state.manifest.manifest_no, ManifestNo(1));
    assert!(target_manifest
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .all(|segment| segment.owner_namespace_id == source));
    publish(
        &mut NamespaceCommitEngine::new(target.clone()),
        &store,
        "child",
    )
    .await
    .expect("unflushed child data");
    fork_namespace(&store, &target, &nested, None, &context())
        .await
        .expect("nested fork");
    let manifest = load_current_manifest(&store, &nested)
        .await
        .expect("nested manifest");
    assert_eq!(manifest.state.manifest.manifest_no, ManifestNo(1));
    let owners = manifest
        .envelope
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .map(|segment| segment.owner_namespace_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        owners,
        [source.clone(), target.clone()].into_iter().collect()
    );
    let source_hint = hint(&source);
    let target_hint = hint(&target);
    let source_manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&source);
    let target_manifests = loonfs_objectstore::keys::metadata_manifest_prefix(&target);
    let store = RecordingStore::new(
        store,
        KeyPredicate::new(move |key| {
            key == source_hint
                || key == target_hint
                || key.starts_with(&source_manifests)
                || key.starts_with(&target_manifests)
        }),
    );
    let view = load_current_metadata_view(&store, &nested)
        .await
        .expect("nested view");
    for path in ["/inherited", "/child"] {
        view.resolve_path(path, AttributeInclusion::Omit)
            .await
            .expect("inherited path");
    }
    assert_eq!(store.count(OperationClass::Any), 0);
}

#[tokio::test]
async fn a_pending_hint_cannot_name_a_manifest_collected_after_its_replacement() {
    use loonfs_test_support::stores::MetadataMapStore;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("hint-gc").expect("namespace");
    let grace = crate::limits::GC_MIN_GRACE_WINDOW_MS;
    let config = crate::gc::GcConfig {
        grace_window_ms: grace,
        max_steps: None,
    };
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let store = MetadataMapStore::new(
        store,
        KeyPredicate::new(|key| {
            loonfs_objectstore::layout::manifest_no_of(key)
                .is_some_and(|number| number >= ManifestNo(3))
        }),
        move |mut metadata| {
            metadata.last_modified_ms = Some(grace + 1);
            metadata
        },
    );
    let store = BlockingStore::new(
        store,
        KeyPredicate::hint(&namespace_id),
        OperationClass::CompareAndSwap,
    );
    bootstrap_namespace(&store, &namespace_id, &context(), false)
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut engine, &store, "seed").await.expect("seed");
    store.block_next();
    let (hint_result, ()) = futures::join!(
        super::control::raise_namespace_hint(&store, &namespace_id, WalNo(2), None),
        async {
            store.wait_until_blocked().await;
            crate::checkpoint::flush_wal(store.inner(), &namespace_id, &context())
                .await
                .expect("replace manifest");
            crate::checkpoint::advance_retention_floor(store.inner(), &namespace_id, &context())
                .await
                .expect("advance floor");
            let aged = MutationContext {
                now_ms: grace + 1,
                ..context()
            };
            crate::gc::gc_namespace(store.inner(), &namespace_id, &config, &aged)
                .await
                .expect("collect during pending hint");
            assert!(store
                .inner()
                .head(&metadata_manifest_object(&namespace_id, &ManifestNo(2)))
                .await
                .expect("predecessor")
                .is_some());
            store.release();
        }
    );
    hint_result.expect("finish pending hint");
    let current = load_current_manifest(&store, &namespace_id)
        .await
        .expect("discover past lagging hint");
    assert_eq!(current.state.manifest.manifest_no, ManifestNo(4));
    load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("read committed state");
    super::control::raise_namespace_hint(&store, &namespace_id, WalNo(2), None)
        .await
        .expect("refresh hint");
    let aged = MutationContext {
        now_ms: 2 * grace + 2,
        ..context()
    };
    crate::gc::gc_namespace(&store, &namespace_id, &config, &aged)
        .await
        .expect("collect after grace");
    assert!(store
        .head(&metadata_manifest_object(&namespace_id, &ManifestNo(2)))
        .await
        .expect("old manifest")
        .is_none());
}

#[tokio::test]
async fn a_flush_and_collection_during_tip_discovery_cannot_reuse_a_wal_number() {
    use loonfs_test_support::stores::MetadataMapStore;
    let directory = tempdir().expect("directory");
    let namespace_id = NamespaceId::parse("tip-gc").expect("namespace");
    let grace = crate::limits::GC_MIN_GRACE_WINDOW_MS;
    let config = crate::gc::GcConfig {
        grace_window_ms: grace,
        max_steps: None,
    };
    let store = MetadataMapStore::aged(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    );
    let store = MetadataMapStore::new(
        store,
        KeyPredicate::new(|key| {
            loonfs_objectstore::layout::manifest_no_of(key)
                .is_some_and(|number| number >= ManifestNo(3))
        }),
        move |mut metadata| {
            metadata.last_modified_ms = Some(grace + 1);
            metadata
        },
    );
    let store = BlockingStore::new(
        store,
        KeyPredicate::exact(wal_segment(&namespace_id, &WalNo(1))),
        OperationClass::Read,
    );
    bootstrap_namespace(&store, &namespace_id, &context(), false)
        .await
        .expect("create");
    let mut engine = NamespaceCommitEngine::new(namespace_id.clone());
    publish(&mut engine, &store, "seed").await.expect("seed");
    engine.invalidate_projection();
    store.block_next();
    let (published, ()) = futures::join!(publish(&mut engine, &store, "after-gc"), async {
        store.wait_until_blocked().await;
        crate::checkpoint::flush_wal(store.inner(), &namespace_id, &context())
            .await
            .expect("fold old WAL");
        crate::checkpoint::advance_retention_floor(store.inner(), &namespace_id, &context())
            .await
            .expect("advance floor");
        let aged = MutationContext {
            now_ms: grace + 1,
            ..context()
        };
        crate::gc::gc_namespace(store.inner(), &namespace_id, &config, &aged)
            .await
            .expect("collect old WAL");
        assert!(store
            .inner()
            .head(&wal_segment(&namespace_id, &WalNo(1)))
            .await
            .expect("old fence")
            .is_none());
        store.release();
    });
    assert_eq!(
        published
            .expect("replanned from folded state")
            .committed_seq,
        ChangeSeq(2)
    );
    assert!(store
        .head(&wal_segment(&namespace_id, &WalNo(1)))
        .await
        .expect("old number")
        .is_none());
    assert!(store
        .head(&wal_segment(&namespace_id, &WalNo(3)))
        .await
        .expect("new number")
        .is_some());
    let view = load_current_metadata_view(&store, &namespace_id)
        .await
        .expect("view");
    for path in ["/seed", "/after-gc"] {
        view.resolve_path(path, AttributeInclusion::Omit)
            .await
            .expect("file");
    }
}
