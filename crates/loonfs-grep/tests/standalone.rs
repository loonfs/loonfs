#![allow(clippy::panic)]
//! Standalone worker one-shot and configuration smoke coverage.

use loonfs_api::{AbsolutePath, CommitId, DestinationBehavior, NamespaceId};
use loonfs_core::content::{prepare_stored_content, store_bytes_as_content};
use loonfs_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
use loonfs_core::{BootstrapOptions, NamespaceEngine};
use loonfs_grep::keyspace::{root_key, segments_prefix};
use loonfs_grep::root::{load_grep_root, GrepLifecycle};
use loonfs_grep::GrepWorker;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn standalone_once_advances_the_index_is_idempotent_and_rejects_bad_config() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let store = Arc::new(LocalFsStore::new(&store_root).expect("store"));
    let namespace_id = NamespaceId::parse("standalone").expect("namespace id");
    let engine = NamespaceEngine::builder(store.clone())
        .namespace_id(namespace_id.clone())
        .writer_id("standalone-seed")
        .writer_session_id("standalone-seed-session")
        .writer_version("standalone-test/0.1")
        .build()
        .expect("engine");
    engine
        .bootstrap_namespace(BootstrapOptions::default())
        .await
        .expect("bootstrap namespace");
    put_file(&store, &engine, &namespace_id).await;
    GrepWorker::new(
        store.clone(),
        "standalone-enable",
        "standalone-enable-session",
        "standalone-enable/0.1",
    )
    .enable(&namespace_id)
    .await
    .expect("enable grep");

    let config_path = temp_dir.path().join("worker.toml");
    write_config(&config_path, &store_root, "");
    let first = run_once(&config_path);
    assert!(
        first.status.success(),
        "first one-shot failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load root")
        .expect("root exists");
    assert_eq!(first_root.state().index().built_through_seq.0, 1);
    assert!(matches!(
        first_root.state().lifecycle(),
        GrepLifecycle::Steady
    ));
    assert!(!first_root.state().segments().is_empty());
    let first_root_bytes = store
        .get(&root_key(&namespace_id), None)
        .await
        .expect("read root")
        .expect("root bytes");
    let first_segments = store
        .list_prefix(&segments_prefix(&namespace_id))
        .await
        .expect("list segments");

    let second = run_once(&config_path);
    assert!(
        second.status.success(),
        "second one-shot failed: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(
        store
            .get(&root_key(&namespace_id), None)
            .await
            .expect("read root again")
            .expect("root bytes again"),
        first_root_bytes,
        "an up-to-date one-shot must not republish the root"
    );
    assert_eq!(
        store
            .list_prefix(&segments_prefix(&namespace_id))
            .await
            .expect("list segments again"),
        first_segments,
        "an up-to-date one-shot must not write segments"
    );

    let bad_config_path = temp_dir.path().join("bad-worker.toml");
    write_config(&bad_config_path, &store_root, "step_interval_ms = 0\n");
    let bad = run_once(&bad_config_path);
    assert!(!bad.status.success(), "bad config must exit nonzero");
    let stderr = String::from_utf8_lossy(&bad.stderr);
    assert!(stderr.contains("invalid grep config"), "{stderr}");
    assert!(stderr.contains("step_interval_ms"), "{stderr}");
    assert!(stderr.contains("greater than zero"), "{stderr}");
}

async fn put_file(
    store: &Arc<LocalFsStore>,
    engine: &NamespaceEngine<Arc<LocalFsStore>>,
    namespace_id: &NamespaceId,
) {
    let stored = store_bytes_as_content(&**store, namespace_id, b"standalone needle\n")
        .await
        .expect("store content");
    let content_ref = stored.content_ref.clone();
    let prepared = prepare_stored_content(namespace_id.clone(), stored);
    let result = engine
        .publish_namespace_mutations_batch(vec![NamespaceMutationCandidate::path_prepared(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse("standalone-put").expect("commit id"),
                absolute_path: AbsolutePath::parse("/note.txt").expect("path"),
                content_ref,
                behavior: DestinationBehavior::NoReplace,
            },
            vec![prepared],
        )])
        .await
        .pop()
        .expect("one result");
    result.expect("publish file");
}

fn write_config(path: &Path, store_root: &Path, grep_fields: &str) {
    std::fs::write(
        path,
        format!(
            r#"
[store]
kind = "local-fs"
root = "{}"

[grep]
{}"#,
            store_root.display(),
            grep_fields
        ),
    )
    .expect("write config");
}

fn run_once(config_path: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_loonfs-grep"))
        .arg("--config")
        .arg(config_path)
        .arg("--once")
        .output()
        .expect("run loonfs-grep")
}
