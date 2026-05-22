use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{namespace_descriptor, namespace_head};
use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use loonfs::{
    ChangeSeq, CommitId, CommitOp, CommitRequest, CompleteUploadRequest, CopyOptions,
    CoreErrorKind, CreateNamespaceOptions, DeleteOptions, Fs, FsConfig, InodeId,
    MaintenanceTickOptions, MaintenanceTickOutcome, MoveOptions, NamespaceId, PutFileBehavior,
    PutFileOptions, RuntimeError, SharedObjectStore,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

fn store(root: &Path) -> SharedObjectStore {
    Arc::new(LocalFsStore::new(root).expect("create local-fs store"))
}

fn runtime(root: &Path, writer_id: &str) -> Fs {
    Fs::builder(store(root))
        .writer_id(writer_id)
        .build()
        .expect("build runtime")
}

fn namespace() -> NamespaceId {
    NamespaceId::parse("demo").expect("valid namespace id")
}

fn assert_config_error(result: loonfs::Result<Fs>, expected: &str) {
    match result {
        Err(RuntimeError::Config(message)) => assert!(
            message.contains(expected),
            "expected {message:?} to contain {expected:?}"
        ),
        Err(error) => panic!("expected config error, got {error:?}"),
        Ok(_) => panic!("expected config error"),
    }
}

fn assert_core_error_kind<T>(result: loonfs::Result<T>, expected: CoreErrorKind) {
    match result {
        Err(RuntimeError::Core(error)) => assert_eq!(error.kind(), expected),
        Err(error) => panic!("expected core error {expected:?}, got {error:?}"),
        Ok(_) => panic!("expected core error {expected:?}"),
    }
}

#[test]
fn open_validates_runtime_config() {
    let temp_dir = tempdir().expect("tempdir");
    let object_store = store(temp_dir.path());

    assert_config_error(Fs::builder(object_store.clone()).build(), "writer_id");
    assert_config_error(
        Fs::open(
            object_store.clone(),
            FsConfig {
                writer_id: "   ".to_owned(),
                writer_version: "runtime-test/0.1.0".to_owned(),
                lease_duration_ms: 5_000,
            },
        ),
        "writer_id",
    );
    assert_config_error(
        Fs::open(
            object_store.clone(),
            FsConfig {
                writer_id: "runtime-test".to_owned(),
                writer_version: "   ".to_owned(),
                lease_duration_ms: 5_000,
            },
        ),
        "writer_version",
    );
    assert_config_error(
        Fs::open(
            object_store,
            FsConfig {
                writer_id: "runtime-test".to_owned(),
                writer_version: "runtime-test/0.1.0".to_owned(),
                lease_duration_ms: 0,
            },
        ),
        "lease_duration_ms",
    );
}

#[test]
fn filesystem_operations_match_core_semantics() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "filesystem-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let stat = fs
        .stat_path(&namespace_id, "/docs/hello.txt")
        .expect("stat file");
    assert_eq!(stat.absolute_path, "/docs/hello.txt");
    assert_eq!(stat.size_bytes, Some(5));

    let entries = fs.list_path(&namespace_id, "/docs").expect("list docs");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].absolute_path, "/docs/hello.txt");

    let read = fs
        .read_file_bytes(&namespace_id, "/docs/hello.txt")
        .expect("read file");
    assert_eq!(read.bytes, b"hello");

    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        PutFileOptions {
            behavior: PutFileBehavior::ReplaceExisting,
            commit_id: None,
        },
    )
    .expect("replace file");
    let read = fs
        .read_file_bytes(&namespace_id, "/docs/hello.txt")
        .expect("read replaced file");
    assert_eq!(read.bytes, b"updated");

    fs.copy_path(
        &namespace_id,
        "/docs/hello.txt",
        "/docs/copy.txt",
        CopyOptions::default(),
    )
    .expect("copy file");
    fs.move_path(
        &namespace_id,
        "/docs/copy.txt",
        "/docs/moved.txt",
        MoveOptions::default(),
    )
    .expect("move file");
    assert_eq!(
        fs.read_file_bytes(&namespace_id, "/docs/moved.txt")
            .expect("read moved copy")
            .bytes,
        b"updated"
    );
}

#[test]
fn delete_options_select_recursive_behavior() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "delete-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let error = fs
        .delete_path(&namespace_id, "/docs", DeleteOptions::default())
        .expect_err("non-recursive delete should reject non-empty directory");
    assert!(matches!(
        error,
        RuntimeError::Core(error) if error.kind() == loonfs::CoreErrorKind::DirectoryNotEmpty
    ));

    fs.delete_path(
        &namespace_id,
        "/docs",
        DeleteOptions {
            recursive: true,
            commit_id: None,
        },
    )
    .expect("recursive delete");
    let error = fs
        .stat_path(&namespace_id, "/docs/hello.txt")
        .expect_err("deleted file should not stat");
    assert!(matches!(
        error,
        RuntimeError::Core(error) if error.kind() == loonfs::CoreErrorKind::PathNotFound
    ));
}

#[test]
fn forked_namespace_shares_content_then_diverges() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "fork-test");
    let source = namespace();
    let clone = NamespaceId::parse("clone").expect("valid namespace id");

    fs.create_namespace(&source, CreateNamespaceOptions::default())
        .expect("create source namespace");
    fs.put_file_bytes(
        &source,
        "/docs/shared.txt",
        b"source",
        PutFileOptions::default(),
    )
    .expect("put source file");
    fs.fork_namespace(&source, &clone).expect("fork namespace");

    let source_entry = fs
        .stat_path(&source, "/docs/shared.txt")
        .expect("stat source");
    let clone_entry = fs
        .stat_path(&clone, "/docs/shared.txt")
        .expect("stat clone");
    assert_eq!(source_entry.content_ref, clone_entry.content_ref);

    fs.put_file_bytes(
        &clone,
        "/docs/shared.txt",
        b"clone",
        PutFileOptions {
            behavior: PutFileBehavior::ReplaceExisting,
            commit_id: None,
        },
    )
    .expect("replace clone file");

    assert_eq!(
        fs.read_file_bytes(&source, "/docs/shared.txt")
            .expect("read source")
            .bytes,
        b"source"
    );
    assert_eq!(
        fs.read_file_bytes(&clone, "/docs/shared.txt")
            .expect("read clone")
            .bytes,
        b"clone"
    );
}

#[test]
fn upload_flow_is_available_from_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "upload-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = fs.begin_upload(&namespace_id).expect("begin upload");
    let staged = fs
        .upload_content(&namespace_id, &begin.upload_id, b"uploaded")
        .expect("upload content");
    let staged_again = fs
        .upload_content(&namespace_id, &begin.upload_id, b"uploaded")
        .expect("repeat upload content");
    assert_eq!(staged.content_ref, staged_again.content_ref);

    let request = CompleteUploadRequest {
        content_ref: staged.content_ref,
    };
    let completed = fs
        .complete_upload(&namespace_id, &begin.upload_id, &request)
        .expect("complete upload");
    let completed_again = fs
        .complete_upload(&namespace_id, &begin.upload_id, &request)
        .expect("repeat complete upload");
    assert_eq!(completed.content_ref, completed_again.content_ref);
}

#[test]
fn explicit_commit_appears_in_change_feed() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "commit-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let commit_id = CommitId::new("explicit-create-dir");
    let response = fs
        .commit_operations(
            &namespace_id,
            CommitRequest {
                commit_id: commit_id.clone(),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDir {
                    parent_inode: InodeId(1),
                    display_name: "docs".to_owned(),
                }],
                message: Some("create docs".to_owned()),
                annotations: None,
            },
        )
        .expect("commit operation");

    let changes = fs
        .list_changes_after(&namespace_id, ChangeSeq(0))
        .expect("list changes");
    assert_eq!(changes.through_seq, response.committed_seq);
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(changes.changes[0].commit_id, commit_id);
}

#[test]
fn namespace_status_reports_checkpoint_lag_from_head() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "status-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let status = fs
        .namespace_status(&namespace_id)
        .expect("status for new namespace");
    assert_eq!(status.namespace_id, namespace_id);
    assert_eq!(status.head_seq, ChangeSeq(0));
    assert_eq!(status.checkpoint_hint_seq, None);
    assert_eq!(status.uncheckpointed_commits, 0);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));

    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let status = fs
        .namespace_status(&namespace_id)
        .expect("status after commit");
    assert_eq!(status.head_seq, ChangeSeq(1));
    assert_eq!(status.checkpoint_hint_seq, None);
    assert_eq!(status.uncheckpointed_commits, 1);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));
}

#[test]
fn namespace_status_and_tick_reject_missing_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "missing-status-test");
    let namespace_id = namespace();

    assert_core_error_kind(
        fs.namespace_status(&namespace_id),
        CoreErrorKind::NamespaceNotFound,
    );
    assert_core_error_kind(
        fs.maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default()),
        CoreErrorKind::NamespaceNotFound,
    );
}

#[test]
fn namespace_status_and_tick_reject_partial_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("partial-status-test")
        .build()
        .expect("build runtime");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    raw_store
        .delete(&namespace_descriptor(namespace_id.as_str()))
        .expect("delete namespace descriptor");

    assert_core_error_kind(
        fs.namespace_status(&namespace_id),
        CoreErrorKind::NamespacePartial,
    );
    assert_core_error_kind(
        fs.maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default()),
        CoreErrorKind::NamespacePartial,
    );
}

#[test]
fn maintenance_tick_below_threshold_is_not_needed() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-not-needed-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let tick = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_uncheckpointed_commits: 2,
            },
        )
        .expect("maintenance tick");
    assert_eq!(tick.namespace_id, namespace_id);
    assert_eq!(tick.status_before.uncheckpointed_commits, 1);
    assert_eq!(tick.outcome, MaintenanceTickOutcome::NotNeeded);
}

#[test]
fn maintenance_tick_at_threshold_publishes_checkpoint() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-publish-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let tick = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_uncheckpointed_commits: 1,
            },
        )
        .expect("maintenance tick");
    assert_eq!(tick.status_before.head_seq, ChangeSeq(1));
    assert_eq!(
        tick.outcome,
        MaintenanceTickOutcome::CheckpointPublished {
            checkpoint_seq: ChangeSeq(1)
        }
    );

    let status = fs
        .namespace_status(&namespace_id)
        .expect("status after checkpoint");
    assert_eq!(status.checkpoint_hint_seq, Some(ChangeSeq(1)));
    assert_eq!(status.uncheckpointed_commits, 0);
}

#[test]
fn maintenance_tick_rejects_zero_threshold() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-config-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let error = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_uncheckpointed_commits: 0,
            },
        )
        .expect_err("zero threshold should fail");
    match error {
        RuntimeError::Config(message) => assert!(message.contains("max_uncheckpointed_commits")),
        other => panic!("expected config error, got {other:?}"),
    }
}

#[test]
fn maintenance_tick_treats_checkpoint_hint_cas_loss_as_benign_race() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("tick-race-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    raw_store.fail_head_cas();
    let tick = fs
        .maintenance_tick_namespace(
            &namespace_id,
            MaintenanceTickOptions {
                max_uncheckpointed_commits: 1,
            },
        )
        .expect("maintenance tick should not fail on checkpoint hint race");

    assert_eq!(
        tick.outcome,
        MaintenanceTickOutcome::CheckpointPublishRaceLost {
            observed_head_seq: ChangeSeq(1)
        }
    );
    let status = fs
        .namespace_status(&namespace_id)
        .expect("status after lost race");
    assert_eq!(status.checkpoint_hint_seq, None);
    assert_eq!(status.uncheckpointed_commits, 1);
}

#[test]
fn checkpoint_and_retention_hooks_are_available() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "maintenance-test");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let checkpoint = fs
        .create_checkpoint(&namespace_id)
        .expect("create checkpoint");
    let retention = fs
        .advance_retention_floor(&namespace_id)
        .expect("advance retention");
    assert_eq!(retention.retention_floor_seq, checkpoint.checkpoint_seq);
}

#[test]
fn separate_runtime_instances_share_object_store_state() {
    let temp_dir = tempdir().expect("tempdir");
    let writer = runtime(temp_dir.path(), "writer");
    let reader = runtime(temp_dir.path(), "reader");
    let namespace_id = namespace();

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/docs/shared.txt",
            b"shared",
            PutFileOptions::default(),
        )
        .expect("put file");

    let namespaces = reader.list_namespaces().expect("list namespaces");
    assert_eq!(namespaces.len(), 1);
    assert_eq!(namespaces[0].namespace_id, namespace_id);
    let file = reader
        .read_file_bytes(&namespace_id, "/docs/shared.txt")
        .expect("read shared file");
    assert_eq!(file.bytes, b"shared");
}

#[derive(Debug)]
struct HeadCasFailureStore {
    inner: LocalFsStore,
    head_key: String,
    fail_head_cas: AtomicBool,
}

impl HeadCasFailureStore {
    fn new(root: &Path, namespace: &str) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            head_key: namespace_head(namespace),
            fail_head_cas: AtomicBool::new(false),
        }
    }

    fn fail_head_cas(&self) {
        self.fail_head_cas.store(true, Ordering::SeqCst);
    }
}

impl ObjectStore for HeadCasFailureStore {
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key)
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.inner.get(key, range)
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.head_key
            && matches!(&mode, PutMode::CompareAndSwap { .. })
            && self.fail_head_cas.load(Ordering::SeqCst)
        {
            return Err(ObjectStoreError::PreconditionFailed);
        }
        self.inner.put(key, bytes, mode)
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        self.inner.list_prefix(prefix)
    }
}
