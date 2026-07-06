#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, BeginUploadResponse,
    ChangeSeq, ChangesResponse, CommitId, CommitOp, CommitRequest, CommitResponse,
    CompleteUploadRequest, CompleteUploadResponse, ContentRef, CopyOptions,
    CreateCheckpointResponse, CreateDirOptions, CreateNamespaceOptions, DeleteOptions,
    DirectoryPageCursor, ErrorCode, Fs, FsConfig, InodeId, InodeKind, MaintenanceTickOptions,
    MaintenanceTickOutcome, MaintenanceTickResult, ManifestId, MoveOptions, MutationResult,
    NamespaceId, NamespaceStatus, PageRequest, PaginationPolicy, PutBehavior, PutFileOptions,
    RuntimeCacheConfig, RuntimeError, SharedObjectStore, TraceMode, TraceStoreKind,
    UploadContentResponse, UploadId,
};
use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_objectstore::fs::LocalFsStore;
use loonfs_objectstore::keys::{metadata_manifest, namespace_config, wal_head};
use loonfs_objectstore::metrics::{ObjectStoreOperation, VecObjectStoreMetricsRecorder};
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::future::Future;
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

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(future)
}

fn page_limit(limit: u32) -> loonfs::EffectiveLimit {
    PaginationPolicy::from_values(limit, limit)
        .expect("valid pagination policy")
        .resolve_limit(Some(limit))
        .expect("valid page limit")
}

fn decode_directory_page_cursor(value: &str) -> DirectoryPageCursor {
    loonfs_api::decode_directory_cursor(value).expect("decode directory cursor")
}

fn decode_file_revisions_page_cursor(value: &str) -> loonfs::FileRevisionsPageCursor {
    loonfs_api::decode_file_revisions_cursor(value).expect("decode file revisions cursor")
}

fn display_names(entries: &[AuthoritativePathEntry]) -> Vec<&str> {
    entries
        .iter()
        .map(|entry| entry.display_name.as_str())
        .collect()
}

trait FsTestExt {
    fn create_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> loonfs::Result<loonfs::NamespaceSummary>;
    fn fork_namespace_blocking(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> loonfs::Result<loonfs::NamespaceSummary>;
    fn namespace_status_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<NamespaceStatus>;
    fn maintenance_tick_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> loonfs::Result<MaintenanceTickResult>;
    fn stat_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<AuthoritativePathEntry>;
    fn list_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<Vec<AuthoritativePathEntry>>;
    fn read_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<AuthoritativeFileBytes>;
    fn put_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> loonfs::Result<MutationResult>;
    fn create_dir_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirOptions,
    ) -> loonfs::Result<MutationResult>;
    fn delete_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> loonfs::Result<MutationResult>;
    fn move_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> loonfs::Result<MutationResult>;
    fn copy_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> loonfs::Result<MutationResult>;
    fn begin_upload_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<BeginUploadResponse>;
    fn upload_content_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> loonfs::Result<UploadContentResponse>;
    fn complete_upload_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> loonfs::Result<CompleteUploadResponse>;
    fn commit_operations_blocking(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> loonfs::Result<CommitResponse>;
    fn commit_operations_batch_blocking(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<loonfs::Result<CommitResponse>>;
    fn list_changes_after_blocking(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> loonfs::Result<ChangesResponse>;
    fn create_checkpoint_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<CreateCheckpointResponse>;
    fn advance_retention_floor_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<AdvanceRetentionResponse>;
}

impl FsTestExt for Fs {
    fn create_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> loonfs::Result<loonfs::NamespaceSummary> {
        block_on(self.create_namespace(namespace_id, options))
    }

    fn fork_namespace_blocking(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> loonfs::Result<loonfs::NamespaceSummary> {
        block_on(self.fork_namespace(source, target))
    }

    fn namespace_status_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<NamespaceStatus> {
        block_on(self.namespace_status(namespace_id))
    }

    fn maintenance_tick_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> loonfs::Result<MaintenanceTickResult> {
        block_on(self.maintenance_tick_namespace(namespace_id, options))
    }

    fn stat_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<AuthoritativePathEntry> {
        block_on(self.stat_path(namespace_id, absolute_path))
    }

    fn list_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<Vec<AuthoritativePathEntry>> {
        block_on(self.list_path(namespace_id, absolute_path))
    }

    fn read_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<AuthoritativeFileBytes> {
        block_on(self.read_file_bytes(namespace_id, absolute_path))
    }

    fn put_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> loonfs::Result<MutationResult> {
        block_on(self.put_file_bytes(namespace_id, absolute_path, bytes, options))
    }

    fn create_dir_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirOptions,
    ) -> loonfs::Result<MutationResult> {
        block_on(self.create_dir(namespace_id, absolute_path, options))
    }

    fn delete_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> loonfs::Result<MutationResult> {
        block_on(self.delete_path(namespace_id, absolute_path, options))
    }

    fn move_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> loonfs::Result<MutationResult> {
        block_on(self.move_path(namespace_id, from_path, to_path, options))
    }

    fn copy_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> loonfs::Result<MutationResult> {
        block_on(self.copy_path(namespace_id, from_path, to_path, options))
    }

    fn begin_upload_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<BeginUploadResponse> {
        block_on(self.begin_upload(namespace_id))
    }

    fn upload_content_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> loonfs::Result<UploadContentResponse> {
        block_on(self.upload_content(namespace_id, upload_id, bytes))
    }

    fn complete_upload_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> loonfs::Result<CompleteUploadResponse> {
        block_on(self.complete_upload(namespace_id, upload_id, request))
    }

    fn commit_operations_blocking(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> loonfs::Result<CommitResponse> {
        block_on(self.commit_operations(namespace_id, request))
    }

    fn commit_operations_batch_blocking(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<loonfs::Result<CommitResponse>> {
        block_on(self.commit_operations_batch(namespace_id, requests))
    }

    fn list_changes_after_blocking(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> loonfs::Result<ChangesResponse> {
        block_on(self.list_changes_after(namespace_id, after_seq))
    }

    fn create_checkpoint_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<CreateCheckpointResponse> {
        block_on(self.create_checkpoint(namespace_id))
    }

    fn advance_retention_floor_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<AdvanceRetentionResponse> {
        block_on(self.advance_retention_floor(namespace_id))
    }
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

fn assert_core_error_kind<T>(result: loonfs::Result<T>, expected: ErrorCode) {
    match result {
        Err(RuntimeError::Core(error)) => assert_eq!(error.code(), expected),
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
                runtime_cache: RuntimeCacheConfig::default(),
                trace_mode: TraceMode::Embedded,
                trace_store_kind: TraceStoreKind::LocalFs,
            },
        ),
        "writer_id",
    );
    assert_config_error(
        Fs::open(
            object_store,
            FsConfig {
                writer_id: "runtime-test".to_owned(),
                writer_version: "   ".to_owned(),
                runtime_cache: RuntimeCacheConfig::default(),
                trace_mode: TraceMode::Embedded,
                trace_store_kind: TraceStoreKind::LocalFs,
            },
        ),
        "writer_version",
    );
}

#[test]
fn builder_metrics_recorder_instruments_object_store() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let fs = Fs::builder(store(temp_dir.path()))
        .writer_id("metrics-test")
        .trace_store_kind(TraceStoreKind::LocalFs)
        .with_metrics_recorder(recorder.clone())
        .build()
        .expect("build runtime");

    fs.create_namespace_blocking(&namespace(), CreateNamespaceOptions::default())
        .expect("create namespace");

    let samples = recorder.samples();
    assert!(!samples.is_empty());
    assert!(samples
        .iter()
        .any(|sample| sample.operation == ObjectStoreOperation::Put));
    assert!(samples
        .iter()
        .all(|sample| sample.store_kind.as_deref() == Some("local_fs")));
}

#[test]
fn filesystem_operations_match_core_semantics() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "filesystem-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let stat = fs
        .stat_path_blocking(&namespace_id, "/docs/hello.txt")
        .expect("stat file");
    assert_eq!(stat.absolute_path, "/docs/hello.txt");
    assert_eq!(stat.size_bytes, Some(5));

    let entries = fs
        .list_path_blocking(&namespace_id, "/docs")
        .expect("list docs");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].absolute_path, "/docs/hello.txt");

    let read = fs
        .read_file_bytes_blocking(&namespace_id, "/docs/hello.txt")
        .expect("read file");
    assert_eq!(read.bytes, b"hello");

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        PutFileOptions {
            behavior: PutBehavior::Replace,
            commit_id: None,
        },
    )
    .expect("replace file");
    let read = fs
        .read_file_bytes_blocking(&namespace_id, "/docs/hello.txt")
        .expect("read replaced file");
    assert_eq!(read.bytes, b"updated");

    fs.copy_path_blocking(
        &namespace_id,
        "/docs/hello.txt",
        "/docs/copy.txt",
        CopyOptions::default(),
    )
    .expect("copy file");
    fs.move_path_blocking(
        &namespace_id,
        "/docs/copy.txt",
        "/docs/moved.txt",
        MoveOptions::default(),
    )
    .expect("move file");
    assert_eq!(
        fs.read_file_bytes_blocking(&namespace_id, "/docs/moved.txt")
            .expect("read moved copy")
            .bytes,
        b"updated"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_runtime_methods_are_the_engine_boundary() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "async-runtime-test");
    let namespace_id = namespace();

    Fs::create_namespace(&fs, &namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    Fs::put_file_bytes(
        &fs,
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .await
    .expect("put file");

    let async_stat = Fs::stat_path(&fs, &namespace_id, "/docs/hello.txt")
        .await
        .expect("async stat");

    assert_eq!(async_stat.absolute_path, "/docs/hello.txt");
    assert_eq!(async_stat.size_bytes, Some(5));
}

#[test]
fn runtime_cache_reuses_wal_tail_projection_for_repeated_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("tail-projection-cache-test")
        .build()
        .expect("build runtime");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::default(),
    )
    .expect("put file");

    raw_store.reset_wal_get_count();
    fs.read_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("first read should project WAL tail");
    assert!(raw_store.wal_get_count() > 0);
    let after_first = fs.runtime_cache_stats();
    assert_eq!(after_first.wal_tail_projection_cache_misses, 1);
    assert_eq!(after_first.wal_tail_projection_cache_inserts, 1);

    raw_store.reset_wal_get_count();
    fs.read_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("second read should reuse cached WAL-tail projection");
    assert_eq!(raw_store.wal_get_count(), 0);
    let after_second = fs.runtime_cache_stats();
    assert_eq!(after_second.wal_tail_projection_cache_hits, 1);

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/other.txt",
        b"other",
        PutFileOptions::default(),
    )
    .expect("put other");
    raw_store.reset_wal_get_count();
    fs.read_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("read after local mutation should rebuild WAL-tail projection");
    assert!(raw_store.wal_get_count() > 0);
    let after_write = fs.runtime_cache_stats();
    assert!(after_write.wal_tail_projection_cache_evictions > 0);
    assert!(
        after_write.wal_tail_projection_cache_misses
            > after_second.wal_tail_projection_cache_misses
    );
}

#[test]
fn runtime_publish_reuses_wal_tail_projection_for_sequential_writes() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let setup = Fs::builder(object_store.clone())
        .writer_id("publish-tail")
        .build()
        .expect("build setup runtime");
    let measured = Fs::builder(object_store)
        .writer_id("publish-tail")
        .build()
        .expect("build measured runtime");

    setup
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    setup
        .create_dir_blocking(&namespace_id, "/seed-a", CreateDirOptions::default())
        .expect("seed first WAL segment");
    setup
        .create_dir_blocking(&namespace_id, "/seed-b", CreateDirOptions::default())
        .expect("seed second WAL segment");

    raw_store.reset_wal_get_count();
    measured
        .create_dir_blocking(&namespace_id, "/measured-a", CreateDirOptions::default())
        .expect("first measured write loads existing tail");
    assert!(
        raw_store.wal_get_count() > 0,
        "first measured write should read the existing WAL tail"
    );

    raw_store.reset_wal_get_count();
    measured
        .create_dir_blocking(&namespace_id, "/measured-b", CreateDirOptions::default())
        .expect("second measured write advances cached publish tail");
    assert_eq!(
        raw_store.wal_get_count(),
        0,
        "second measured write should not reread WAL tail"
    );
}

#[test]
fn runtime_publish_allows_multi_segment_wal_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let setup = Fs::builder(object_store.clone())
        .writer_id("publish-tail")
        .build()
        .expect("build setup runtime");
    let measured = Fs::builder(object_store)
        .writer_id("publish-tail")
        .build()
        .expect("build measured runtime");

    setup
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    setup
        .create_dir_blocking(&namespace_id, "/seed-a", CreateDirOptions::default())
        .expect("seed first WAL segment");
    setup
        .create_dir_blocking(&namespace_id, "/seed-b", CreateDirOptions::default())
        .expect("seed second WAL segment");

    measured
        .create_dir_blocking(
            &namespace_id,
            "/should-succeed",
            CreateDirOptions::default(),
        )
        .expect("publish projects the visible WAL tail without a segment limit");
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
        .writer_id("tail-cache-reader")
        .build()
        .expect("build reader runtime");
    let writer = Fs::builder(object_store)
        .writer_id("tail-cache-writer")
        .build()
        .expect("build writer runtime");

    writer
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .create_dir_blocking(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    reader
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("prime reader cache");

    writer
        .create_dir_blocking(&namespace_id, "/docs/new", CreateDirOptions::default())
        .expect("advance head from another runtime");

    raw_store.reset_wal_get_count();
    let stat = reader
        .stat_path_blocking(&namespace_id, "/docs/new")
        .expect("reader should observe external head advance");
    assert_eq!(stat.absolute_path, "/docs/new");
    assert_eq!(stat.head_seq, ChangeSeq(2));
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
        .writer_id("tail-cache-disabled-test")
        .runtime_cache(RuntimeCacheConfig::disabled())
        .build()
        .expect("build runtime");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::default(),
    )
    .expect("put file");

    raw_store.reset_wal_get_count();
    fs.read_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("first read should project WAL tail");
    fs.read_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("second read should project WAL tail again");
    assert_eq!(raw_store.wal_get_count(), 2);
    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.wal_tail_projection_cache_hits, 0);
    assert_eq!(stats.wal_tail_projection_cache_misses, 0);
}

#[test]
fn runtime_wal_tail_projection_cache_evicts_by_namespace_count() {
    let temp_dir = tempdir().expect("tempdir");
    let shared_store = store(temp_dir.path());
    let setup = Fs::builder(shared_store.clone())
        .writer_id("tail-count-setup")
        .build()
        .expect("build setup runtime");
    let first = NamespaceId::parse("first").expect("valid namespace id");
    let second = NamespaceId::parse("second").expect("valid namespace id");

    setup
        .create_namespace_blocking(&first, CreateNamespaceOptions::default())
        .expect("create first namespace");
    setup
        .put_file_bytes_blocking(&first, "/file.txt", b"first", PutFileOptions::default())
        .expect("put first file");
    setup
        .create_namespace_blocking(&second, CreateNamespaceOptions::default())
        .expect("create second namespace");
    setup
        .put_file_bytes_blocking(&second, "/file.txt", b"second", PutFileOptions::default())
        .expect("put second file");

    let fs = Fs::builder(shared_store)
        .writer_id("tail-count-budget")
        .runtime_cache(RuntimeCacheConfig {
            max_cached_namespaces: 1,
            ..RuntimeCacheConfig::default()
        })
        .build()
        .expect("build runtime");

    fs.read_file_bytes_blocking(&first, "/file.txt")
        .expect("cache first tail projection");
    fs.read_file_bytes_blocking(&second, "/file.txt")
        .expect("cache second tail projection and evict first");
    let after_second = fs.runtime_cache_stats();
    assert_eq!(after_second.wal_tail_projection_cache_evictions, 1);
    assert!(after_second.wal_tail_projection_cache_cached_rows > 0);

    fs.read_file_bytes_blocking(&first, "/file.txt")
        .expect("first tail projection reloads after eviction");
    let after_reload = fs.runtime_cache_stats();
    assert_eq!(after_reload.wal_tail_projection_cache_misses, 3);
    assert_eq!(after_reload.wal_tail_projection_cache_evictions, 2);
}

#[test]
fn runtime_wal_tail_projection_cache_skips_oversized_projection() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("tail-oversized-test")
        .runtime_cache(RuntimeCacheConfig {
            max_cached_wal_tail_projection_rows: 0,
            ..RuntimeCacheConfig::default()
        })
        .build()
        .expect("build runtime");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/file.txt",
        b"file",
        PutFileOptions::default(),
    )
    .expect("put file");

    raw_store.reset_wal_get_count();
    fs.read_file_bytes_blocking(&namespace_id, "/file.txt")
        .expect("first read projects oversized tail");
    fs.read_file_bytes_blocking(&namespace_id, "/file.txt")
        .expect("second read projects oversized tail again");
    assert_eq!(raw_store.wal_get_count(), 2);
    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.wal_tail_projection_cache_misses, 2);
    assert_eq!(stats.wal_tail_projection_cache_hits, 0);
    assert_eq!(stats.wal_tail_projection_cache_uncacheable_count, 2);
    assert_eq!(stats.wal_tail_projection_cache_cached_rows, 0);
}

#[test]
fn runtime_read_allows_multi_segment_wal_tail() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let fs = Fs::builder(store(temp_dir.path()))
        .writer_id("tail-read-test")
        .build()
        .expect("build runtime");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir_blocking(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");
    fs.create_dir_blocking(&namespace_id, "/more", CreateDirOptions::default())
        .expect("create another WAL segment");

    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("read projects the visible WAL tail without a segment limit");
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
        .writer_id("tail-cache-stale-test")
        .build()
        .expect("build runtime");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir_blocking(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("prime read cache");

    raw_store.fail_head_cas();
    assert_core_error_kind(
        fs.create_dir_blocking(&namespace_id, "/stale", CreateDirOptions::default()),
        ErrorCode::StaleHead,
    );

    raw_store.allow_head_cas();
    raw_store.reset_wal_get_count();
    fs.create_dir_blocking(&namespace_id, "/after-stale", CreateDirOptions::default())
        .expect("write after stale head should reload publish tail");
    assert!(
        raw_store.wal_get_count() > 0,
        "failed publish attempt should invalidate cached publish tail"
    );

    raw_store.reset_wal_get_count();
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("read after stale head should reload materialization");
    assert!(raw_store.wal_get_count() > 0);
}

#[test]
fn delete_options_select_recursive_behavior() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "delete-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let error = fs
        .delete_path_blocking(&namespace_id, "/docs", DeleteOptions::default())
        .expect_err("non-recursive delete should reject non-empty directory");
    assert!(matches!(
        error,
        RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::DirectoryNotEmpty
    ));

    fs.delete_path_blocking(
        &namespace_id,
        "/docs",
        DeleteOptions {
            behavior: loonfs::DeleteDirectoryBehavior::Recursive,
            commit_id: None,
        },
    )
    .expect("recursive delete");
    let error = fs
        .stat_path_blocking(&namespace_id, "/docs/hello.txt")
        .expect_err("deleted file should not stat");
    assert!(matches!(
        error,
        RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::PathNotFound
    ));
}

#[test]
fn directory_pages_use_canonical_name_key_order() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "directory-page-order-test");
    let namespace_id = namespace();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in [
        "/docs/Zebra.txt",
        "/docs/apple.txt",
        "/docs/B.txt",
        "/docs/a.txt",
    ] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::default(),
        )
        .expect("put file");
    }
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint");

    let limit = page_limit(2);
    let first = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit,
            cursor: None,
        },
    ))
    .expect("first directory page");
    assert_eq!(display_names(&first.entries), vec!["a.txt", "apple.txt"]);

    let cursor = decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));
    let second = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit,
            cursor: Some(cursor),
        },
    ))
    .expect("second directory page");
    assert_eq!(display_names(&second.entries), vec!["B.txt", "Zebra.txt"]);
    assert!(second.next_cursor.is_none());

    let full = block_on(fs.list_path_entries(&namespace_id, "/docs")).expect("full listing");
    assert_eq!(
        display_names(&full.entries),
        vec!["a.txt", "apple.txt", "B.txt", "Zebra.txt"]
    );
}

#[test]
fn file_revision_pages_merge_manifest_and_wal_tail_newest_first() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "file-revision-page-test");
    let namespace_id = namespace();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    let replace = PutFileOptions {
        behavior: PutBehavior::Replace,
        commit_id: None,
    };
    fs.put_file_bytes_blocking(&namespace_id, "/doc.txt", b"v1", PutFileOptions::default())
        .expect("put v1");
    fs.put_file_bytes_blocking(&namespace_id, "/doc.txt", b"v2", replace.clone())
        .expect("put v2");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint after v2");
    fs.put_file_bytes_blocking(&namespace_id, "/doc.txt", b"v3", replace.clone())
        .expect("put v3");
    fs.put_file_bytes_blocking(&namespace_id, "/doc.txt", b"v4", replace)
        .expect("put v4");

    let limit = page_limit(2);
    let first = block_on(fs.list_file_revisions_page(
        &namespace_id,
        "/doc.txt",
        PageRequest {
            limit,
            cursor: None,
        },
    ))
    .expect("first revision page");
    assert_eq!(
        first
            .revisions
            .iter()
            .map(|revision| revision.revision_no.0)
            .collect::<Vec<_>>(),
        vec![4, 3]
    );

    let cursor =
        decode_file_revisions_page_cursor(first.next_cursor.as_deref().expect("next cursor"));
    let second = block_on(fs.list_file_revisions_page(
        &namespace_id,
        "/doc.txt",
        PageRequest {
            limit,
            cursor: Some(cursor),
        },
    ))
    .expect("second revision page");
    assert_eq!(
        second
            .revisions
            .iter()
            .map(|revision| revision.revision_no.0)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert!(second.next_cursor.is_none());

    let inode_page = block_on(fs.list_file_revisions_for_inode_page(
        &namespace_id,
        first.inode_id,
        PageRequest {
            limit,
            cursor: None,
        },
    ))
    .expect("inode revision page");
    assert_eq!(
        inode_page
            .revisions
            .iter()
            .map(|revision| revision.revision_no.0)
            .collect::<Vec<_>>(),
        vec![4, 3]
    );
}

#[test]
fn directory_cursor_after_later_writes_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "directory-page-snapshot-test");
    let namespace_id = namespace();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::default(),
        )
        .expect("put file");
    }

    let limit = page_limit(2);
    let first = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit,
            cursor: None,
        },
    ))
    .expect("first directory page");
    assert_eq!(display_names(&first.entries), vec!["a.txt", "b.txt"]);
    let cursor = decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/z.txt",
        b"newer",
        PutFileOptions::default(),
    )
    .expect("put later file");

    assert_core_error_kind(
        block_on(fs.list_path_entries_page(
            &namespace_id,
            "/docs",
            PageRequest {
                limit,
                cursor: Some(cursor),
            },
        )),
        ErrorCode::RebootstrapRequired,
    );
}

#[test]
fn directory_cursor_older_than_materialized_snapshot_floor_is_rejected() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "directory-page-floor-test");
    let namespace_id = namespace();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt", "/docs/c.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::default(),
        )
        .expect("put file");
    }

    let first = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit: page_limit(2),
            cursor: None,
        },
    ))
    .expect("first directory page");
    let cursor = decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/z.txt",
        b"newer",
        PutFileOptions::default(),
    )
    .expect("put later file");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint newer snapshot");

    assert_core_error_kind(
        block_on(fs.list_path_entries_page(
            &namespace_id,
            "/docs",
            PageRequest {
                limit: page_limit(2),
                cursor: Some(cursor),
            },
        )),
        ErrorCode::RebootstrapRequired,
    );
}

#[test]
fn directory_cursor_rejects_path_inode_mismatch() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "directory-page-mismatch-test");
    let namespace_id = namespace();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    for path in ["/docs/a.txt", "/docs/b.txt"] {
        fs.put_file_bytes_blocking(
            &namespace_id,
            path,
            path.as_bytes(),
            PutFileOptions::default(),
        )
        .expect("put file");
    }

    let first = block_on(fs.list_path_entries_page(
        &namespace_id,
        "/docs",
        PageRequest {
            limit: page_limit(1),
            cursor: None,
        },
    ))
    .expect("first directory page");
    let cursor = decode_directory_page_cursor(first.next_cursor.as_deref().expect("next cursor"));

    assert_core_error_kind(
        block_on(fs.list_path_entries_page(
            &namespace_id,
            "/",
            PageRequest {
                limit: page_limit(1),
                cursor: Some(cursor),
            },
        )),
        ErrorCode::InvalidRequest,
    );
}

#[test]
fn forked_namespace_shares_content_then_diverges() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "fork-test");
    let source = namespace();
    let clone = NamespaceId::parse("clone").expect("valid namespace id");

    fs.create_namespace_blocking(&source, CreateNamespaceOptions::default())
        .expect("create source namespace");
    fs.put_file_bytes_blocking(
        &source,
        "/docs/shared.txt",
        b"source",
        PutFileOptions::default(),
    )
    .expect("put source file");
    fs.fork_namespace_blocking(&source, &clone)
        .expect("fork namespace");

    let source_entry = fs
        .stat_path_blocking(&source, "/docs/shared.txt")
        .expect("stat source");
    let clone_entry = fs
        .stat_path_blocking(&clone, "/docs/shared.txt")
        .expect("stat clone");
    assert_eq!(source_entry.content_ref, clone_entry.content_ref);

    fs.put_file_bytes_blocking(
        &clone,
        "/docs/shared.txt",
        b"clone",
        PutFileOptions {
            behavior: PutBehavior::Replace,
            commit_id: None,
        },
    )
    .expect("replace clone file");

    assert_eq!(
        fs.read_file_bytes_blocking(&source, "/docs/shared.txt")
            .expect("read source")
            .bytes,
        b"source"
    );
    assert_eq!(
        fs.read_file_bytes_blocking(&clone, "/docs/shared.txt")
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

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = fs
        .begin_upload_blocking(&namespace_id)
        .expect("begin upload");
    let staged = fs
        .upload_content_blocking(&namespace_id, &begin.upload_id, b"uploaded")
        .expect("upload content");
    let staged_again = fs
        .upload_content_blocking(&namespace_id, &begin.upload_id, b"uploaded")
        .expect("repeat upload content");
    assert_eq!(staged.content_ref, staged_again.content_ref);

    let request = CompleteUploadRequest {
        content_ref: staged.content_ref,
    };
    let completed = fs
        .complete_upload_blocking(&namespace_id, &begin.upload_id, &request)
        .expect("complete upload");
    let completed_again = fs
        .complete_upload_blocking(&namespace_id, &begin.upload_id, &request)
        .expect("repeat complete upload");
    assert_eq!(completed.content_ref, completed_again.content_ref);
}

#[test]
fn direct_put_upload_flow_validates_durable_object_on_complete() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "direct-put-upload-test");
    let namespace_id = namespace();
    let bytes = b"direct uploaded";
    let content_ref = ContentRef::whole_file_v0(bytes);

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = block_on(fs.begin_direct_put_upload_target(&namespace_id, content_ref.clone()))
        .expect("begin direct put");
    assert_eq!(begin.target.content_ref, content_ref);

    let complete_request = CompleteUploadRequest {
        content_ref: content_ref.clone(),
    };
    assert!(fs
        .complete_upload_blocking(&namespace_id, &begin.upload_id, &complete_request)
        .is_err());

    let direct_store = LocalFsStore::new(temp_dir.path()).expect("direct object-store handle");
    block_on(direct_store.put_if_absent(&begin.target.object_key, Bytes::copy_from_slice(bytes)))
        .expect("write direct object");

    let completed = fs
        .complete_upload_blocking(&namespace_id, &begin.upload_id, &complete_request)
        .expect("complete direct put");
    assert_eq!(completed.content_ref, content_ref);

    block_on(fs.put_file_content_ref(
        &namespace_id,
        "/docs/direct.txt",
        content_ref,
        PutFileOptions::default(),
    ))
    .expect("publish direct put content");
    assert_eq!(
        fs.read_file_bytes_blocking(&namespace_id, "/docs/direct.txt")
            .expect("read direct put file")
            .bytes,
        bytes
    );
}

#[test]
fn stat_and_list_use_initial_manifest_without_checkpoint() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let fs = runtime(temp_dir.path(), "read-fallback-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir_blocking(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("stat docs");
    fs.list_path_blocking(&namespace_id, "/")
        .expect("list root");

    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.latest_metadata_view_reads, 2);
}

#[test]
fn stat_and_list_use_materialized_tables_after_checkpoint_without_content_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(ContentBlobGetCountingStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("read-materialized-test")
        .build()
        .expect("build runtime");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::default(),
    )
    .expect("put file");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint");

    raw_store.reset_content_blob_counters();
    fs.stat_path_blocking(&namespace_id, "/docs/file.txt")
        .expect("stat materialized file");
    fs.list_path_blocking(&namespace_id, "/docs")
        .expect("list materialized docs");

    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.latest_metadata_view_reads, 2);
    assert_eq!(raw_store.content_blob_get_count(), 0);
    assert_eq!(raw_store.content_blob_checksum_head_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_materialized_stat_and_list_share_async_store() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let fs = runtime(temp_dir.path(), "concurrent-materialized-read-test");

    fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    fs.put_file_bytes(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::default(),
    )
    .await
    .expect("put file");
    fs.create_checkpoint(&namespace_id)
        .await
        .expect("checkpoint");

    let (stat, list) = tokio::join!(
        fs.stat_path(&namespace_id, "/docs/file.txt"),
        fs.list_path(&namespace_id, "/docs")
    );
    let stat = stat.expect("stat file");
    let list = list.expect("list docs");

    assert_eq!(stat.absolute_path, "/docs/file.txt");
    assert_eq!(stat.size_bytes, Some(4));
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].absolute_path, "/docs/file.txt");

    let stats = fs.runtime_cache_stats();
    assert_eq!(stats.latest_metadata_view_reads, 2);
}

#[test]
fn repeated_materialized_stat_uses_metadata_table_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let fs = runtime(temp_dir.path(), "metadata-table-cache-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/file.txt",
        b"file",
        PutFileOptions::default(),
    )
    .expect("put file");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint");

    fs.stat_path_blocking(&namespace_id, "/docs/file.txt")
        .expect("first materialized stat");
    let after_first = fs.runtime_cache_stats();
    fs.stat_path_blocking(&namespace_id, "/docs/file.txt")
        .expect("second materialized stat");
    let after_second = fs.runtime_cache_stats();

    assert!(after_first.metadata_table_cache_inserts > 0);
    assert!(after_second.metadata_table_cache_hits > after_first.metadata_table_cache_hits);
    assert_eq!(after_second.latest_metadata_view_reads, 2);
}

#[test]
fn put_file_bytes_validates_content_ref_before_publish() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace();
    let raw_store = Arc::new(ContentBlobGetCountingStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = Fs::builder(object_store)
        .writer_id("put-file-content-validation-test")
        .build()
        .expect("build runtime");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    raw_store.reset_content_blob_counters();
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/direct.txt",
        b"direct bytes",
        PutFileOptions::default(),
    )
    .expect("put file bytes");

    assert_eq!(raw_store.content_blob_get_count(), 0);
    assert_eq!(raw_store.content_blob_checksum_head_count(), 1);
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

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");
    fs.create_checkpoint_blocking(&namespace_id)
        .expect("checkpoint");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"updated",
        PutFileOptions {
            behavior: PutBehavior::Replace,
            commit_id: None,
        },
    )
    .expect("replace file");

    raw_store.reset_control_get_counts();
    fs.begin_upload_blocking(&namespace_id)
        .expect("first begin upload");
    fs.begin_upload_blocking(&namespace_id)
        .expect("second begin upload");

    assert_eq!(raw_store.wal_get_count(), 0);
    assert_eq!(raw_store.manifest_get_count(), 0);
}

#[test]
fn runtime_control_cache_reuses_head_for_materialization_validation() {
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

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir_blocking(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");

    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("prime read cache");

    raw_store.reset_control_get_counts();
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("first cached materialization validation reuses cached head state");
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("second cached materialization validation reuses cached head state");

    assert_eq!(raw_store.head_get_count(), 0);
}

#[test]
fn control_cache_eviction_reloads_head_for_materialization_validation() {
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
            max_cached_namespaces: 1,
            ..RuntimeCacheConfig::default()
        })
        .build()
        .expect("build runtime");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_dir_blocking(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");
    fs.create_namespace_blocking(&other_namespace, CreateNamespaceOptions::default())
        .expect("create other namespace");
    fs.create_dir_blocking(&other_namespace, "/docs", CreateDirOptions::default())
        .expect("create other docs");

    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("prime first namespace materialization");
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("prime first namespace head cache");

    raw_store.reset_control_get_counts();
    fs.stat_path_blocking(&other_namespace, "/docs")
        .expect("load other namespace materialization and evict first head cache");
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("reload first namespace materialization and head cache");

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
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .create_dir_blocking(&namespace_id, "/docs", CreateDirOptions::default())
        .expect("create docs");
    reader
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("prime read cache");
    raw_store.reset_control_get_counts();
    reader
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("prime control cache");
    reader
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("reuse unchanged control cache");
    assert_eq!(raw_store.head_get_count(), 0);

    writer
        .create_dir_blocking(&namespace_id, "/docs/new", CreateDirOptions::default())
        .expect("advance head");
    raw_store.reset_control_get_counts();
    reader
        .stat_path_blocking(&namespace_id, "/docs/new")
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
        fs.begin_upload_blocking(&namespace_id),
        ErrorCode::NamespaceNotFound,
    );

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    block_on(raw_store.delete(&namespace_config(namespace_id.as_str())))
        .expect("delete namespace descriptor");

    assert_core_error_kind(
        fs.begin_upload_blocking(&namespace_id),
        ErrorCode::NamespacePartial,
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

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    block_on(raw_store.put_overwrite(
        &namespace_config(namespace_id.as_str()),
        Bytes::from_static(br#"{"not":"a namespace descriptor"}"#),
    ))
    .expect("corrupt namespace descriptor");
    assert_core_error_kind(
        fs.begin_upload_blocking(&namespace_id),
        ErrorCode::NamespaceCorrupt,
    );

    let content_bad = NamespaceId::parse("content-bad").expect("valid namespace id");
    fs.create_namespace_blocking(&content_bad, CreateNamespaceOptions::default())
        .expect("create content-bad namespace");
    for key in block_on(raw_store.list_prefix("content-stores/"))
        .expect("list content stores")
        .into_iter()
        .filter(|key| key.ends_with("/descriptor.json"))
    {
        block_on(raw_store.put_overwrite(
            &key,
            Bytes::from_static(br#"{"not":"a content store descriptor"}"#),
        ))
        .expect("corrupt content store descriptor");
    }
    assert_core_error_kind(
        fs.begin_upload_blocking(&content_bad),
        ErrorCode::NamespaceCorrupt,
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
    fs.create_namespace_blocking(&head_bad, CreateNamespaceOptions::default())
        .expect("create head-bad namespace");
    block_on(raw_store.put_overwrite(
        &wal_head(head_bad.as_str()),
        Bytes::from_static(br#"{"not":"a head"}"#),
    ))
    .expect("corrupt head");
    assert_core_error_kind(
        fs.begin_upload_blocking(&head_bad),
        ErrorCode::NamespaceCorrupt,
    );
}

#[test]
fn explicit_commit_appears_in_change_feed() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "commit-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let commit_id = CommitId::parse("explicit-create-dir").expect("valid commit id");
    let response = fs
        .commit_operations_blocking(
            &namespace_id,
            CommitRequest {
                commit_id: commit_id.clone(),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: "docs".to_owned(),
                }],
                message: Some("create docs".to_owned()),
            },
        )
        .expect("commit operation");

    let changes = fs
        .list_changes_after_blocking(&namespace_id, ChangeSeq(0))
        .expect("list changes");
    assert_eq!(changes.through_seq, response.committed_seq);
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(changes.changes[0].commit_id, commit_id);
}

#[test]
fn publish_auto_ticks_maintenance_once_tail_reaches_threshold() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "auto-tick-test");
    let namespace_id = namespace();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    for round in 0..33u32 {
        fs.put_file_bytes_blocking(
            &namespace_id,
            &format!("/docs/file-{round}.txt"),
            b"body",
            PutFileOptions::default(),
        )
        .expect("put file");
    }

    // The tick runs on the maintenance worker thread; quiesce, then assert.
    fs.wait_for_background_maintenance();
    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after auto tick");
    assert!(
        status.current_manifest_id > Some(ManifestId(0)),
        "auto tick should have published a manifest: {status:?}"
    );
    assert!(
        status.wal_tail_segments < 32,
        "auto tick should have bounded the tail: {status:?}"
    );
}

#[test]
fn namespace_status_reports_wal_tail_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "status-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status for new namespace");
    assert_eq!(status.namespace_id, namespace_id);
    assert_eq!(status.head_seq, ChangeSeq(0));
    assert_eq!(status.current_manifest_id, Some(ManifestId(0)));
    assert_eq!(status.wal_tail_segments, 0);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after commit");
    assert_eq!(status.head_seq, ChangeSeq(1));
    assert_eq!(status.current_manifest_id, Some(ManifestId(0)));
    assert_eq!(status.wal_tail_segments, 1);
    assert_eq!(status.retention_floor_seq, ChangeSeq(0));
}

#[test]
fn root_stat_and_list_work_immediately_after_namespace_create() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "initial-manifest-read-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    let root = fs
        .stat_path_blocking(&namespace_id, "/")
        .expect("stat root after create");
    assert_eq!(root.absolute_path, "/");
    assert_eq!(root.inode_id, InodeId(1));
    assert_eq!(root.inode_kind, InodeKind::Dir);
    assert_eq!(root.head_seq, ChangeSeq(0));

    let entries = fs
        .list_path_blocking(&namespace_id, "/")
        .expect("list root after create");
    assert!(entries.is_empty());
}

#[test]
fn namespace_status_and_tick_reject_missing_namespace() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "missing-status-test");
    let namespace_id = namespace();

    assert_core_error_kind(
        fs.namespace_status_blocking(&namespace_id),
        ErrorCode::NamespaceNotFound,
    );
    assert_core_error_kind(
        fs.maintenance_tick_namespace_blocking(&namespace_id, MaintenanceTickOptions::default()),
        ErrorCode::NamespaceNotFound,
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

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    block_on(raw_store.delete(&namespace_config(namespace_id.as_str())))
        .expect("delete namespace descriptor");

    assert_core_error_kind(
        fs.namespace_status_blocking(&namespace_id),
        ErrorCode::NamespacePartial,
    );
    assert_core_error_kind(
        fs.maintenance_tick_namespace_blocking(&namespace_id, MaintenanceTickOptions::default()),
        ErrorCode::NamespacePartial,
    );
}

#[test]
fn maintenance_tick_below_threshold_is_not_needed() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-not-needed-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let tick = fs
        .maintenance_tick_namespace_blocking(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 2,
                gc: None,
            },
        )
        .expect("maintenance tick");
    assert_eq!(tick.namespace_id, namespace_id);
    assert_eq!(tick.status_before.wal_tail_segments, 1);
    assert_eq!(tick.outcome, MaintenanceTickOutcome::NotNeeded);
}

#[test]
fn maintenance_tick_at_segment_threshold_publishes_checkpoint() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-publish-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let tick = fs
        .maintenance_tick_namespace_blocking(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 1,
                gc: None,
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
        .namespace_status_blocking(&namespace_id)
        .expect("status after checkpoint");
    assert_eq!(status.current_manifest_id, Some(ManifestId(1)));
    assert_eq!(status.wal_tail_segments, 0);
}

#[test]
fn maintenance_tick_after_existing_checkpoint_writes_l0_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-l0-run-publish-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put first file");
    fs.maintenance_tick_namespace_blocking(
        &namespace_id,
        MaintenanceTickOptions {
            max_wal_tail_segments: 1,
            gc: None,
        },
    )
    .expect("first maintenance tick");

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/second.txt",
        b"second",
        PutFileOptions::default(),
    )
    .expect("put second file");
    let tick = fs
        .maintenance_tick_namespace_blocking(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 1,
                gc: None,
            },
        )
        .expect("second maintenance tick");
    assert_eq!(
        tick.outcome,
        MaintenanceTickOutcome::CheckpointPublished {
            checkpoint_seq: ChangeSeq(2)
        }
    );

    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after l0 checkpoint");
    assert_eq!(status.current_manifest_id, Some(ManifestId(2)));
    assert_eq!(status.wal_tail_segments, 0);

    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let manifest_key = metadata_manifest(namespace_id.as_str(), ManifestId(2));
    let manifest_bytes = block_on(raw_store.get(&manifest_key, None))
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    assert_eq!(manifest.payload.base_seq, ChangeSeq(1));
    let l0_files = manifest
        .payload
        .metadata_files
        .iter()
        .filter(|metadata_file| metadata_file.level == 0)
        .collect::<Vec<_>>();
    assert!(!l0_files.is_empty());
    assert!(l0_files
        .iter()
        .all(|metadata_file| metadata_file.run_seq == ChangeSeq(2)));
}

#[test]
fn maintenance_tick_counts_segments_not_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-segment-count-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let first_batch = fs.commit_operations_batch_blocking(
        &namespace_id,
        vec![
            CommitRequest {
                commit_id: CommitId::parse("create-a").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: "a".to_owned(),
                }],
                message: None,
            },
            CommitRequest {
                commit_id: CommitId::parse("create-b").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateDirectory {
                    parent_inode_id: InodeId(1),
                    display_name: "b".to_owned(),
                }],
                message: None,
            },
        ],
    );
    assert!(first_batch.iter().all(Result::is_ok));

    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after first batch");
    assert_eq!(status.head_seq, ChangeSeq(2));
    assert_eq!(status.wal_tail_segments, 1);

    let tick = fs
        .maintenance_tick_namespace_blocking(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 2,
                gc: None,
            },
        )
        .expect("maintenance tick");
    assert_eq!(tick.outcome, MaintenanceTickOutcome::NotNeeded);

    fs.commit_operations_blocking(
        &namespace_id,
        CommitRequest {
            commit_id: CommitId::parse("create-c").expect("valid commit id"),
            preconditions: Vec::new(),
            ops: vec![CommitOp::CreateDirectory {
                parent_inode_id: InodeId(1),
                display_name: "c".to_owned(),
            }],
            message: None,
        },
    )
    .expect("second segment commit");

    let tick = fs
        .maintenance_tick_namespace_blocking(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 2,
                gc: None,
            },
        )
        .expect("maintenance tick at segment threshold");
    assert_eq!(tick.status_before.head_seq, ChangeSeq(3));
    assert_eq!(tick.status_before.wal_tail_segments, 2);
    assert_eq!(
        tick.outcome,
        MaintenanceTickOutcome::CheckpointPublished {
            checkpoint_seq: ChangeSeq(3)
        }
    );
}

#[test]
fn maintenance_tick_rejects_zero_threshold() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-config-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let error = fs
        .maintenance_tick_namespace_blocking(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 0,
                gc: None,
            },
        )
        .expect_err("zero threshold should fail");
    match error {
        RuntimeError::Config(message) => assert!(message.contains("max_wal_tail_segments")),
        other => panic!("expected config error, got {other:?}"),
    }
}

#[test]
fn maintenance_tick_treats_metadata_root_cas_loss_as_benign_race() {
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

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    raw_store.fail_root_cas();
    let tick = fs
        .maintenance_tick_namespace_blocking(
            &namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 1,
                gc: None,
            },
        )
        .expect("maintenance tick should not fail on metadata root publish race");

    assert_eq!(
        tick.outcome,
        MaintenanceTickOutcome::CheckpointPublishRaceLost {
            observed_head_seq: ChangeSeq(1)
        }
    );
    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after lost race");
    assert_eq!(status.current_manifest_id, Some(ManifestId(0)));
    assert_eq!(status.wal_tail_segments, 1);
}

#[test]
fn checkpoint_and_retention_hooks_are_available() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "maintenance-test");
    let namespace_id = namespace();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .expect("put file");

    let checkpoint = fs
        .create_checkpoint_blocking(&namespace_id)
        .expect("create checkpoint");
    let retention = fs
        .advance_retention_floor_blocking(&namespace_id)
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
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .put_file_bytes_blocking(
            &namespace_id,
            "/docs/shared.txt",
            b"shared",
            PutFileOptions::default(),
        )
        .expect("put file");

    let file = reader
        .read_file_bytes_blocking(&namespace_id, "/docs/shared.txt")
        .expect("read shared file");
    assert_eq!(file.bytes, b"shared");
}

#[derive(Debug)]
struct HeadCasFailureStore {
    inner: LocalFsStore,
    head_key: String,
    root_key: String,
    wal_prefix: String,
    manifest_prefix: String,
    fail_head_cas: AtomicBool,
    fail_root_cas: AtomicBool,
    wal_get_count: AtomicUsize,
    manifest_get_count: AtomicUsize,
    head_get_count: AtomicUsize,
}

impl HeadCasFailureStore {
    fn new(root: &Path, namespace: &str) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            head_key: wal_head(namespace),
            root_key: loonfs_objectstore::keys::metadata_root(namespace),
            wal_prefix: format!("namespaces/{namespace}/wal/segments/"),
            manifest_prefix: format!("namespaces/{namespace}/metadata/manifests/"),
            fail_head_cas: AtomicBool::new(false),
            fail_root_cas: AtomicBool::new(false),
            wal_get_count: AtomicUsize::new(0),
            manifest_get_count: AtomicUsize::new(0),
            head_get_count: AtomicUsize::new(0),
        }
    }

    fn fail_head_cas(&self) {
        self.fail_head_cas.store(true, Ordering::SeqCst);
    }

    fn allow_head_cas(&self) {
        self.fail_head_cas.store(false, Ordering::SeqCst);
    }

    fn fail_root_cas(&self) {
        self.fail_root_cas.store(true, Ordering::SeqCst);
    }

    fn reset_wal_get_count(&self) {
        self.wal_get_count.store(0, Ordering::SeqCst);
    }

    fn reset_control_get_counts(&self) {
        self.manifest_get_count.store(0, Ordering::SeqCst);
        self.head_get_count.store(0, Ordering::SeqCst);
        self.reset_wal_get_count();
    }

    fn wal_get_count(&self) -> usize {
        self.wal_get_count.load(Ordering::SeqCst)
    }

    fn manifest_get_count(&self) -> usize {
        self.manifest_get_count.load(Ordering::SeqCst)
    }

    fn head_get_count(&self) -> usize {
        self.head_get_count.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStore for HeadCasFailureStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        if key.starts_with(&self.wal_prefix) {
            self.wal_get_count.fetch_add(1, Ordering::SeqCst);
        }
        if key.starts_with(&self.manifest_prefix) {
            self.manifest_get_count.fetch_add(1, Ordering::SeqCst);
        }
        if key == self.head_key {
            self.head_get_count.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        if key.starts_with(&self.wal_prefix) {
            self.wal_get_count.fetch_add(1, Ordering::SeqCst);
        }
        if key.starts_with(&self.manifest_prefix) {
            self.manifest_get_count.fetch_add(1, Ordering::SeqCst);
        }
        if key == self.head_key {
            self.head_get_count.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key == self.head_key
            && matches!(&mode, PutMode::CompareAndSwap { .. })
            && self.fail_head_cas.load(Ordering::SeqCst)
        {
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
        if key == self.root_key
            && matches!(&mode, PutMode::CompareAndSwap { .. })
            && self.fail_root_cas.load(Ordering::SeqCst)
        {
            return Err(ObjectStoreError::PreconditionFailed {
                object_key: key.to_owned(),
            });
        }
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

#[derive(Debug)]
struct ContentBlobGetCountingStore {
    inner: LocalFsStore,
    content_blob_gets: AtomicUsize,
    content_blob_checksum_heads: AtomicUsize,
}

impl ContentBlobGetCountingStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            content_blob_gets: AtomicUsize::new(0),
            content_blob_checksum_heads: AtomicUsize::new(0),
        }
    }

    fn reset_content_blob_counters(&self) {
        self.content_blob_gets.store(0, Ordering::SeqCst);
        self.content_blob_checksum_heads.store(0, Ordering::SeqCst);
    }

    fn content_blob_get_count(&self) -> usize {
        self.content_blob_gets.load(Ordering::SeqCst)
    }

    fn content_blob_checksum_head_count(&self) -> usize {
        self.content_blob_checksum_heads.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStore for ContentBlobGetCountingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        if key.starts_with("content-stores/") && key.contains("/blobs/") {
            self.content_blob_checksum_heads
                .fetch_add(1, Ordering::SeqCst);
        }
        self.inner.head_with_checksum(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        if key.starts_with("content-stores/") && key.contains("/blobs/") {
            self.content_blob_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        if key.starts_with("content-stores/") && key.contains("/blobs/") {
            self.content_blob_gets.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}
