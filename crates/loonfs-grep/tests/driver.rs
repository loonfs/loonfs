#![allow(clippy::panic)]
//! Per-namespace driver isolation, backoff, and explicit-GC boundaries.

use bytes::Bytes;
use loonfs_api::{AbsolutePath, CommitId, DestinationBehavior, IndexSegmentId, NamespaceId};
use loonfs_core::content::{prepare_stored_content, store_bytes_as_content};
use loonfs_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
use loonfs_core::{BootstrapOptions, DeleteNamespaceOptions, NamespaceEngine};
use loonfs_grep::keyspace::{root_key, segment_key};
use loonfs_grep::{
    GramIndexBuildPolicy, GrepDriver, GrepDriverParked, GrepDriverState, GrepWorker,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn poisoned_driver_backs_off_while_sibling_catches_up_and_gc_stays_explicit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let poisoned = NamespaceId::parse("poisoned").expect("namespace id");
    let healthy = NamespaceId::parse("healthy").expect("namespace id");
    let poisoned_engine = bootstrap(store.clone(), &poisoned).await;
    let healthy_engine = bootstrap(store.clone(), &healthy).await;
    put_file(&store, &poisoned_engine, &poisoned, "poisoned-put").await;
    put_file(&store, &healthy_engine, &healthy, "healthy-put").await;

    let worker = GrepWorker::new(
        store.clone(),
        "driver-test-worker",
        "driver-test-session",
        "driver-test/0.1",
    );
    worker.enable(&poisoned).await.expect("enable poisoned");
    worker.enable(&healthy).await.expect("enable healthy");
    let orphan = segment_key(&healthy, &IndexSegmentId::generate());
    store
        .put_overwrite(&orphan, Bytes::from_static(b"orphan"))
        .await
        .expect("write orphan");
    store
        .put_overwrite(&root_key(&poisoned), Bytes::from_static(b"poison"))
        .await
        .expect("poison root");

    let runtime = tokio::runtime::Handle::current();
    let poisoned_task = GrepDriver::new(worker.clone(), poisoned, GramIndexBuildPolicy::default())
        .spawn_on(&runtime);
    let healthy_task =
        GrepDriver::new(worker, healthy, GramIndexBuildPolicy::default()).spawn_on(&runtime);
    assert_eq!(
        healthy_task.handle().wait_for_quiescence().await,
        Some(GrepDriverParked::CaughtUp {
            built_through_seq: loonfs_api::ChangeSeq(1)
        })
    );

    let mut poisoned_state = poisoned_task.handle().subscribe_state();
    loop {
        if matches!(
            *poisoned_state.borrow_and_update(),
            GrepDriverState::BackingOff { .. }
        ) {
            break;
        }
        poisoned_state
            .changed()
            .await
            .expect("poisoned driver remains observable");
    }
    assert!(
        store.head(&orphan).await.expect("head orphan").is_some(),
        "driver catch-up must never run garbage collection implicitly"
    );

    poisoned_task
        .shutdown()
        .await
        .expect("stop poisoned driver");
    healthy_task.shutdown().await.expect("stop healthy driver");
}

#[tokio::test]
async fn tombstoned_namespace_driver_parks_as_not_enabled() {
    let temp_dir = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store"));
    let namespace_id = NamespaceId::parse("deleted").expect("namespace id");
    let engine = bootstrap(store.clone(), &namespace_id).await;
    let worker = GrepWorker::new(
        store,
        "deleted-driver-worker",
        "deleted-driver-session",
        "deleted-driver/0.1",
    );
    worker.enable(&namespace_id).await.expect("enable grep");
    engine
        .delete_namespace(DeleteNamespaceOptions::default())
        .await
        .expect("delete namespace");

    let task = GrepDriver::new(worker, namespace_id, GramIndexBuildPolicy::default())
        .spawn_on(&tokio::runtime::Handle::current());
    assert_eq!(
        task.handle().wait_for_quiescence().await,
        Some(GrepDriverParked::NotEnabled)
    );
    task.shutdown().await.expect("stop deleted driver");
}

async fn bootstrap(
    store: Arc<LocalFsStore>,
    namespace_id: &NamespaceId,
) -> NamespaceEngine<Arc<LocalFsStore>> {
    let engine = NamespaceEngine::builder(store)
        .namespace_id(namespace_id.clone())
        .writer_id(format!("seed-{namespace_id}"))
        .writer_session_id(format!("seed-{namespace_id}-session"))
        .writer_version("driver-test/0.1")
        .build()
        .expect("engine");
    engine
        .bootstrap_namespace(BootstrapOptions::default())
        .await
        .expect("bootstrap namespace");
    engine
}

async fn put_file(
    store: &Arc<LocalFsStore>,
    engine: &NamespaceEngine<Arc<LocalFsStore>>,
    namespace_id: &NamespaceId,
    commit_id: &str,
) {
    let stored = store_bytes_as_content(&**store, namespace_id, b"driver needle\n")
        .await
        .expect("store content");
    let content_ref = stored.content_ref.clone();
    let prepared = prepare_stored_content(namespace_id.clone(), stored);
    engine
        .publish_namespace_mutations_batch(vec![NamespaceMutationCandidate::path_prepared(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse(commit_id).expect("commit id"),
                absolute_path: AbsolutePath::parse("/note.txt").expect("path"),
                content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
            vec![prepared],
        )])
        .await
        .pop()
        .expect("one result")
        .expect("publish file");
}
