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
use std::process::{Command, Stdio};
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
    write_config(&config_path, &store_root, "", "");
    let first = run_once(&config_path, &[&namespace_id]);
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

    let second = run_once(&config_path, &[&namespace_id]);
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
    let gc = run_once_with_gc(&config_path, &[&namespace_id]);
    assert!(
        gc.status.success(),
        "explicit one-shot GC failed: {}",
        String::from_utf8_lossy(&gc.stderr)
    );

    let bad_config_path = temp_dir.path().join("bad-worker.toml");
    write_config(&bad_config_path, &store_root, "poll_interval_ms = 0\n", "");
    let bad = run_once(&bad_config_path, &[&namespace_id]);
    assert!(!bad.status.success(), "bad config must exit nonzero");
    let stderr = String::from_utf8_lossy(&bad.stderr);
    assert!(stderr.contains("poll_interval_ms"), "{stderr}");
    assert!(stderr.contains("greater than zero"), "{stderr}");

    let missing_namespace = Command::new(env!("CARGO_BIN_EXE_loonfs-grep"))
        .arg("--config")
        .arg(&config_path)
        .arg("--once")
        .output()
        .expect("run without namespace");
    assert!(!missing_namespace.status.success());
    let stderr = String::from_utf8_lossy(&missing_namespace.stderr);
    assert!(stderr.contains("--namespace"), "{stderr}");
    assert!(stderr.contains("Usage:"), "{stderr}");
}

#[tokio::test]
async fn standalone_namespace_list_is_exact_and_long_running_poll_catches_new_writes() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let store = Arc::new(LocalFsStore::new(&store_root).expect("store"));
    let assigned = NamespaceId::parse("assigned").expect("namespace id");
    let unassigned = NamespaceId::parse("unassigned").expect("namespace id");
    let assigned_engine = bootstrap(&store, &assigned).await;
    let unassigned_engine = bootstrap(&store, &unassigned).await;
    put_file(&store, &assigned_engine, &assigned).await;
    put_file(&store, &unassigned_engine, &unassigned).await;
    let worker = GrepWorker::new(
        store.clone(),
        "standalone-enable",
        "standalone-enable-session",
        "standalone-enable/0.1",
    );
    worker.enable(&assigned).await.expect("enable assigned");
    worker.enable(&unassigned).await.expect("enable unassigned");

    let config_path = temp_dir.path().join("worker.toml");
    write_config(&config_path, &store_root, "poll_interval_ms = 10\n", "");
    let once = run_once(&config_path, &[&assigned]);
    assert!(
        once.status.success(),
        "assigned one-shot failed: {}",
        String::from_utf8_lossy(&once.stderr)
    );
    let assigned_root = load_grep_root(&*store, &assigned)
        .await
        .expect("load assigned root")
        .expect("assigned root");
    assert!(matches!(
        assigned_root.state().lifecycle(),
        GrepLifecycle::Steady
    ));
    let unassigned_root = load_grep_root(&*store, &unassigned)
        .await
        .expect("load unassigned root")
        .expect("unassigned root");
    assert!(matches!(
        unassigned_root.state().lifecycle(),
        GrepLifecycle::Backfilling { .. }
    ));

    let mut child = Command::new(env!("CARGO_BIN_EXE_loonfs-grep"))
        .arg("--config")
        .arg(&config_path)
        .arg("--namespace")
        .arg(assigned.as_str())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start long-running worker");
    wait_for_process_poll().await;
    put_file_at(
        &store,
        &assigned_engine,
        &assigned,
        "/second.txt",
        "standalone-put-second",
    )
    .await;
    wait_for_watermark(&store, &assigned, 2).await;
    child.kill().expect("stop long-running worker");
    child.wait().expect("join long-running worker");
}

async fn bootstrap(
    store: &Arc<LocalFsStore>,
    namespace_id: &NamespaceId,
) -> NamespaceEngine<Arc<LocalFsStore>> {
    let engine = NamespaceEngine::builder(store.clone())
        .namespace_id(namespace_id.clone())
        .writer_id(format!("standalone-seed-{namespace_id}"))
        .writer_session_id(format!("standalone-seed-{namespace_id}-session"))
        .writer_version("standalone-test/0.1")
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
) {
    put_file_at(store, engine, namespace_id, "/note.txt", "standalone-put").await;
}

async fn put_file_at(
    store: &Arc<LocalFsStore>,
    engine: &NamespaceEngine<Arc<LocalFsStore>>,
    namespace_id: &NamespaceId,
    path: &str,
    commit_id: &str,
) {
    let stored = store_bytes_as_content(&**store, namespace_id, b"standalone needle\n")
        .await
        .expect("store content");
    let content_ref = stored.content_ref.clone();
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&**store, namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared = prepare_stored_content(&catalog, stored).expect("prepare stored content");
    let result = engine
        .publish_namespace_mutations_batch(vec![NamespaceMutationCandidate::path_prepared(
            PathMutationIntent::PutFile {
                commit_id: CommitId::parse(commit_id).expect("commit id"),
                absolute_path: AbsolutePath::parse(path).expect("path"),
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

fn write_config(path: &Path, store_root: &Path, root_fields: &str, grep_fields: &str) {
    std::fs::write(
        path,
        format!(
            r#"{}
[store]
kind = "local-fs"
root = "{}"

[grep]
{}"#,
            root_fields,
            store_root.display(),
            grep_fields
        ),
    )
    .expect("write config");
}

fn run_once(config_path: &Path, namespace_ids: &[&NamespaceId]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_loonfs-grep"));
    command.arg("--config").arg(config_path).arg("--once");
    for namespace_id in namespace_ids {
        command.arg("--namespace").arg(namespace_id.as_str());
    }
    command.output().expect("run loonfs-grep")
}

fn run_once_with_gc(config_path: &Path, namespace_ids: &[&NamespaceId]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_loonfs-grep"));
    command
        .arg("--config")
        .arg(config_path)
        .arg("--once")
        .arg("--gc");
    for namespace_id in namespace_ids {
        command.arg("--namespace").arg(namespace_id.as_str());
    }
    command.output().expect("run loonfs-grep with GC")
}

#[allow(clippy::disallowed_methods)]
async fn wait_for_process_poll() {
    // The standalone process boundary offers no readiness channel; this only
    // waits for its first configured 10 ms head poll before the test write.
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
}

#[allow(clippy::disallowed_methods)]
async fn wait_for_watermark(store: &Arc<LocalFsStore>, namespace_id: &NamespaceId, target: u64) {
    // Bounded observation of the real per-namespace poll timer under test.
    for _ in 0..100 {
        let root = load_grep_root(&**store, namespace_id)
            .await
            .expect("load polled root")
            .expect("polled root");
        if root.state().index().built_through_seq.0 >= target {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("long-running worker did not reach watermark {target}");
}
