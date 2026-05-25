use loon_objectstore::fs::LocalFsStore;
use loon_objectstore::keys::{namespace_descriptor, namespace_head, namespace_lease};
use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use loonfs::{
    ChangeSeq, CommitId, CommitOp, CommitRequest, CompleteUploadRequest, CopyOptions,
    CoreErrorKind, CreateDirOptions, CreateNamespaceOptions, DeleteOptions, Fs, FsConfig, InodeId,
    MaintenanceTickOptions, MaintenanceTickOutcome, MoveOptions, NamespaceId, PutFileBehavior,
    PutFileOptions, RuntimeCacheConfig, RuntimeError, SharedObjectStore,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
                runtime_cache: RuntimeCacheConfig::default(),
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
                runtime_cache: RuntimeCacheConfig::default(),
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
                runtime_cache: RuntimeCacheConfig::default(),
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
fn runtime_cache_reuses_verified_basis_for_repeated_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("basis-cache-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    raw_store.reset_wal_get_count();
    fs.stat_path(&namespace_id, "/docs")
        .expect("first stat should load basis");
    assert_eq!(raw_store.wal_get_count(), 1);

    fs.stat_path(&namespace_id, "/docs")
        .expect("second stat should reuse cached basis");
    assert_eq!(raw_store.wal_get_count(), 1);

    fs.create_dir(&namespace_id, "/other", CreateDirOptions::default())
        .expect("create other");
    raw_store.reset_wal_get_count();
    fs.stat_path(&namespace_id, "/docs")
        .expect("stat after mutation should reload basis");
    assert!(raw_store.wal_get_count() > 0);
}

#[test]
fn runtime_cache_observes_head_advanced_by_another_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let reader = Fs::builder(object_store.clone())
        .writer_id("basis-cache-reader")
        .build()
        .expect("build reader runtime");
    let writer = Fs::builder(object_store)
        .writer_id("basis-cache-writer")
        .build()
        .expect("build writer runtime");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    reader
        .stat_path(&namespace_id, "/docs")
        .expect("prime reader cache");

    writer
        .create_dir(&namespace_id, "/docs/new", CreateDirOptions::default())
        .expect("advance head from another runtime");

    raw_store.reset_wal_get_count();
    let stat = reader
        .stat_path(&namespace_id, "/docs/new")
        .expect("reader should observe external head advance");
    assert_eq!(stat.absolute_path, "/docs/new");
    assert_eq!(stat.authoritative_head_seq, ChangeSeq(2));
    assert!(raw_store.wal_get_count() > 0);
}

#[test]
fn runtime_cache_can_be_disabled() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("basis-cache-disabled-test")
        .runtime_cache(RuntimeCacheConfig::disabled())
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    raw_store.reset_wal_get_count();
    fs.stat_path(&namespace_id, "/docs")
        .expect("first stat should load basis");
    fs.stat_path(&namespace_id, "/docs")
        .expect("second stat should load basis again");
    assert_eq!(raw_store.wal_get_count(), 2);
}

#[test]
fn stale_head_write_error_invalidates_runtime_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("basis-cache-stale-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");
    fs.stat_path(&namespace_id, "/docs")
        .expect("prime basis cache");

    raw_store.fail_head_cas();
    assert_core_error_kind(
        fs.create_dir(&namespace_id, "/stale", CreateDirOptions::default()),
        CoreErrorKind::StaleHead,
    );

    raw_store.allow_head_cas();
    raw_store.reset_wal_get_count();
    fs.stat_path(&namespace_id, "/docs")
        .expect("read after stale head should reload basis");
    assert!(raw_store.wal_get_count() > 0);
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
fn begin_upload_validates_controls_without_replay_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("begin-upload-cache-test")
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
    fs.create_checkpoint(&namespace_id).expect("checkpoint");
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

    raw_store.reset_control_get_counts();
    fs.begin_upload(&namespace_id).expect("first begin upload");
    fs.begin_upload(&namespace_id).expect("second begin upload");

    assert_eq!(raw_store.wal_get_count(), 0);
    assert_eq!(raw_store.checkpoint_get_count(), 0);
}

#[test]
fn runtime_control_cache_reuses_head_for_basis_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("control-cache-head-test")
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    fs.stat_path(&namespace_id, "/docs")
        .expect("prime basis cache");

    raw_store.reset_control_get_counts();
    fs.stat_path(&namespace_id, "/docs")
        .expect("first cached basis validation loads head body");
    fs.stat_path(&namespace_id, "/docs")
        .expect("second cached basis validation reuses head body");

    assert_eq!(raw_store.head_get_count(), 1);
}

#[test]
fn control_cache_eviction_reloads_head_for_basis_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let other_namespace = NamespaceId::parse("other").expect("valid namespace id");
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("control-cache-eviction-test")
        .runtime_cache(RuntimeCacheConfig {
            basis_cache_enabled: true,
            control_cache_enabled: true,
            max_cached_namespaces: 1,
        })
        .build()
        .expect("build runtime");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");
    fs.create_namespace(&other_namespace, CreateNamespaceOptions::default())
        .expect("create other namespace");
    fs.create_dir(&other_namespace, "/docs", CreateDirOptions::default())
        .expect("create other docs");

    fs.stat_path(&namespace_id, "/docs")
        .expect("prime first namespace basis");
    fs.stat_path(&namespace_id, "/docs")
        .expect("prime first namespace head cache");

    raw_store.reset_control_get_counts();
    fs.stat_path(&other_namespace, "/docs")
        .expect("load other namespace basis and evict first head cache");
    fs.stat_path(&namespace_id, "/docs")
        .expect("reload first namespace basis and head cache");

    assert_eq!(raw_store.head_get_count(), 1);
}

#[test]
fn runtime_control_cache_reloads_head_after_external_change() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let reader = Fs::builder(object_store.clone())
        .writer_id("control-cache-reader")
        .build()
        .expect("build reader runtime");
    let writer = Fs::builder(object_store)
        .writer_id("control-cache-writer")
        .build()
        .expect("build writer runtime");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .create_dir(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");
    reader
        .stat_path(&namespace_id, "/docs")
        .expect("prime basis cache");
    raw_store.reset_control_get_counts();
    reader
        .stat_path(&namespace_id, "/docs")
        .expect("prime control cache");
    reader
        .stat_path(&namespace_id, "/docs")
        .expect("reuse unchanged control cache");
    assert_eq!(raw_store.head_get_count(), 1);

    writer
        .create_dir(&namespace_id, "/docs/new", CreateDirOptions::default())
        .expect("advance head");
    raw_store.reset_control_get_counts();
    reader
        .stat_path(&namespace_id, "/docs/new")
        .expect("reload changed head");
    assert!(raw_store.head_get_count() > 0);
}

#[test]
fn begin_upload_rejects_missing_and_partial_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("begin-upload-missing-partial-test")
        .build()
        .expect("build runtime");
    let namespace_id = namespace();

    assert_core_error_kind(
        fs.begin_upload(&namespace_id),
        CoreErrorKind::NamespaceNotFound,
    );

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    raw_store
        .delete(&namespace_descriptor(namespace_id.as_str()))
        .expect("delete namespace descriptor");

    assert_core_error_kind(
        fs.begin_upload(&namespace_id),
        CoreErrorKind::NamespacePartial,
    );
}

#[test]
fn begin_upload_rejects_malformed_descriptors() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("begin-upload-malformed-test")
        .build()
        .expect("build runtime");
    let namespace_id = namespace();

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    raw_store
        .put_overwrite(
            &namespace_descriptor(namespace_id.as_str()),
            br#"{"not":"a namespace descriptor"}"#,
        )
        .expect("corrupt namespace descriptor");
    assert_core_error_kind(
        fs.begin_upload(&namespace_id),
        CoreErrorKind::NamespaceCorrupt,
    );

    let content_bad = NamespaceId::parse("content-bad").expect("valid namespace id");
    fs.create_namespace(&content_bad, CreateNamespaceOptions::default())
        .expect("create content-bad namespace");
    for key in raw_store
        .list_prefix("content-stores/")
        .expect("list content stores")
        .into_iter()
        .filter(|key| key.ends_with("/descriptor.json"))
    {
        raw_store
            .put_overwrite(&key, br#"{"not":"a content store descriptor"}"#)
            .expect("corrupt content store descriptor");
    }
    assert_core_error_kind(
        fs.begin_upload(&content_bad),
        CoreErrorKind::NamespaceCorrupt,
    );
}

#[test]
fn begin_upload_rejects_malformed_head_and_lease_when_cache_disabled() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("create local-fs store"));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("begin-upload-malformed-control-test")
        .runtime_cache(RuntimeCacheConfig::disabled())
        .build()
        .expect("build runtime");

    let head_bad = NamespaceId::parse("head-bad").expect("valid namespace id");
    fs.create_namespace(&head_bad, CreateNamespaceOptions::default())
        .expect("create head-bad namespace");
    raw_store
        .put_overwrite(&namespace_head(head_bad.as_str()), br#"{"not":"a head"}"#)
        .expect("corrupt head");
    assert_core_error_kind(fs.begin_upload(&head_bad), CoreErrorKind::NamespaceCorrupt);

    let lease_bad = NamespaceId::parse("lease-bad").expect("valid namespace id");
    fs.create_namespace(&lease_bad, CreateNamespaceOptions::default())
        .expect("create lease-bad namespace");
    raw_store
        .put_overwrite(
            &namespace_lease(lease_bad.as_str()),
            br#"{"not":"a lease"}"#,
        )
        .expect("corrupt lease");
    assert_core_error_kind(fs.begin_upload(&lease_bad), CoreErrorKind::NamespaceCorrupt);
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
    wal_prefix: String,
    checkpoint_prefix: String,
    fail_head_cas: AtomicBool,
    wal_get_count: AtomicUsize,
    checkpoint_get_count: AtomicUsize,
    head_get_count: AtomicUsize,
}

impl HeadCasFailureStore {
    fn new(root: &Path, namespace: &str) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            head_key: namespace_head(namespace),
            wal_prefix: format!("namespaces/{namespace}/wal/"),
            checkpoint_prefix: format!("namespaces/{namespace}/checkpoints/"),
            fail_head_cas: AtomicBool::new(false),
            wal_get_count: AtomicUsize::new(0),
            checkpoint_get_count: AtomicUsize::new(0),
            head_get_count: AtomicUsize::new(0),
        }
    }

    fn fail_head_cas(&self) {
        self.fail_head_cas.store(true, Ordering::SeqCst);
    }

    fn allow_head_cas(&self) {
        self.fail_head_cas.store(false, Ordering::SeqCst);
    }

    fn reset_wal_get_count(&self) {
        self.wal_get_count.store(0, Ordering::SeqCst);
    }

    fn reset_control_get_counts(&self) {
        self.checkpoint_get_count.store(0, Ordering::SeqCst);
        self.head_get_count.store(0, Ordering::SeqCst);
        self.reset_wal_get_count();
    }

    fn wal_get_count(&self) -> usize {
        self.wal_get_count.load(Ordering::SeqCst)
    }

    fn checkpoint_get_count(&self) -> usize {
        self.checkpoint_get_count.load(Ordering::SeqCst)
    }

    fn head_get_count(&self) -> usize {
        self.head_get_count.load(Ordering::SeqCst)
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
        if key.starts_with(&self.wal_prefix) {
            self.wal_get_count.fetch_add(1, Ordering::SeqCst);
        }
        if key.starts_with(&self.checkpoint_prefix) {
            self.checkpoint_get_count.fetch_add(1, Ordering::SeqCst);
        }
        if key == self.head_key {
            self.head_get_count.fetch_add(1, Ordering::SeqCst);
        }
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
