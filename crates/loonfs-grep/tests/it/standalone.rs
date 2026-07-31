#![allow(clippy::panic)]
//! Standalone worker one-shot and configuration smoke coverage.

use loonfs::{FsAdmin, FsReader, FsWriter, SharedObjectStore};
use loonfs_api::NamespaceId;
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
async fn standalone_once_after_external_enable_indexes_in_one_sweep_and_is_idempotent() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let store = Arc::new(LocalFsStore::new(&store_root).expect("store"));
    let namespace_id = NamespaceId::parse("standalone").expect("namespace id");
    let writer = seed(&store, &namespace_id).await;
    put_file(&writer, &namespace_id).await;

    let config_path = temp_dir.path().join("worker.toml");
    write_config(&config_path, &store_root, "", "");
    let disabled = run_once(&config_path, &[&namespace_id]);
    assert!(
        disabled.status.success(),
        "disabled one-shot failed: {}",
        String::from_utf8_lossy(&disabled.stderr)
    );
    assert!(
        load_grep_root(&*store, &namespace_id)
            .await
            .expect("load absent root")
            .is_none(),
        "a disabled one-shot must leave grep disabled"
    );

    worker(store.clone())
        .await
        .enable(&namespace_id)
        .await
        .expect("enable grep");

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

#[test]
fn standalone_config_rejects_zero_work_limits() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let namespace_id = NamespaceId::parse("zero-config").expect("namespace id");

    for (filename, root_fields, grep_fields, rejected_field) in [
        (
            "poll.toml",
            "poll_interval_ms = 0\n",
            "",
            "poll_interval_ms",
        ),
        (
            "budget.toml",
            "",
            "max_files_per_step = 0\n",
            "max_files_per_step",
        ),
    ] {
        let path = temp_dir.path().join(filename);
        write_config(&path, &store_root, root_fields, grep_fields);
        let output = run_once(&path, &[&namespace_id]);
        assert!(
            !output.status.success(),
            "zero `{rejected_field}` must exit nonzero"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(rejected_field), "{stderr}");
        assert!(stderr.contains("greater than zero"), "{stderr}");
    }
}

/// Step concurrency is the maintenance runner's to bound, so the key that
/// used to configure it here is now simply not a key.
#[test]
fn standalone_config_rejects_the_retired_step_concurrency_key() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let namespace_id = NamespaceId::parse("retired-key").expect("namespace id");
    let path = temp_dir.path().join("retired.toml");
    write_config(&path, &store_root, "", "max_concurrent_steps = 2\n");

    let output = run_once(&path, &[&namespace_id]);
    assert!(
        !output.status.success(),
        "a config naming a deleted key must not start"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("max_concurrent_steps"), "{stderr}");
    assert!(stderr.contains("unknown field"), "{stderr}");
}

#[tokio::test]
async fn standalone_namespace_list_is_exact_and_long_running_poll_catches_new_writes() {
    let temp_dir = tempdir().expect("tempdir");
    let store_root = temp_dir.path().join("store");
    let store = Arc::new(LocalFsStore::new(&store_root).expect("store"));
    let assigned = NamespaceId::parse("assigned").expect("namespace id");
    let unassigned = NamespaceId::parse("unassigned").expect("namespace id");
    let assigned_writer = seed(&store, &assigned).await;
    let unassigned_writer = seed(&store, &unassigned).await;
    put_file(&assigned_writer, &assigned).await;
    put_file(&unassigned_writer, &unassigned).await;
    let worker = worker(store.clone()).await;
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
        &assigned_writer,
        &assigned,
        "/second.txt",
        "standalone-put-second",
    )
    .await;
    wait_for_watermark(&store, &assigned, 2).await;
    child.kill().expect("stop long-running worker");
    child.wait().expect("join long-running worker");
}

async fn worker(store: Arc<LocalFsStore>) -> GrepWorker<Arc<LocalFsStore>> {
    let shared: SharedObjectStore = store.clone();
    let reader = FsReader::builder_with_store(shared.clone())
        .build()
        .await
        .expect("build reader");
    let admin = FsAdmin::builder_with_store(shared)
        .actor_id("standalone-enable")
        .build()
        .await
        .expect("build admin");
    GrepWorker::new(store, reader, admin)
}

async fn seed(store: &Arc<LocalFsStore>, namespace_id: &NamespaceId) -> FsWriter {
    crate::test_seeding::writer(
        store.clone(),
        namespace_id,
        format!("standalone-seed-{namespace_id}"),
    )
    .await
}

async fn put_file(writer: &FsWriter, namespace_id: &NamespaceId) {
    put_file_at(writer, namespace_id, "/note.txt", "standalone-put").await;
}

async fn put_file_at(writer: &FsWriter, namespace_id: &NamespaceId, path: &str, commit_id: &str) {
    crate::test_seeding::put_file(
        writer,
        namespace_id,
        b"standalone needle\n",
        path,
        commit_id,
    )
    .await;
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
