#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, BeginUploadRequest,
    BeginUploadResponse, ChangeSeq, ChangesResponse, CommitId, CommitOp, CommitRequest,
    CommitResponse, CompleteUploadRequest, CompleteUploadResponse, ContentRef, CopyOptions,
    CreateCheckpointOptions, CreateCheckpointResponse, CreateDirectoryOptions,
    CreateNamespaceOptions, DeleteOptions, DirectoryPageCursor, ErrorCode, FsAdmin, FsReader,
    FsWriter, FsWriterBuilder, InodeId, InodeKind, ListChangesOptions, MaintenanceTickOptions,
    MaintenanceTickOutcome, MaintenanceTickResult, ManifestId, MoveOptions, NamespaceId,
    NamespaceStatusResponse, PageRequest, PaginationPolicy, PutBehavior, PutFileOptions,
    RuntimeCacheConfig, RuntimeError, SharedObjectStore, TraceStoreKind, UploadContentResponse,
    UploadId,
};
use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_objectstore::keys::{metadata_manifest_object, namespace_config, wal_head};
use loonfs_objectstore::local_fs_store::LocalFsStore;
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

/// One handle set per test fixture: a writer, its derived reader, and an
/// admin handle sharing the same store, exercised through the blocking
/// helpers below.
struct TestRuntime {
    writer: FsWriter,
    reader: FsReader,
    admin: FsAdmin,
}

fn runtime(root: &Path, writer_id: &str) -> TestRuntime {
    open_runtime(store(root), writer_id)
}

fn open_runtime(store: SharedObjectStore, writer_id: &str) -> TestRuntime {
    open_runtime_with(store, writer_id, |builder| builder)
}

fn open_runtime_with(
    store: SharedObjectStore,
    writer_id: &str,
    configure: impl FnOnce(FsWriterBuilder) -> FsWriterBuilder,
) -> TestRuntime {
    block_on(open_runtime_with_async(store, writer_id, configure))
}

/// Async-test variant: opens the fixture inside the test's own runtime.
async fn open_runtime_async(store: SharedObjectStore, writer_id: &str) -> TestRuntime {
    open_runtime_with_async(store, writer_id, |builder| builder).await
}

async fn open_runtime_with_async(
    store: SharedObjectStore,
    writer_id: &str,
    configure: impl FnOnce(FsWriterBuilder) -> FsWriterBuilder,
) -> TestRuntime {
    let writer = configure(FsWriter::builder_with_store(store.clone()).writer_id(writer_id))
        .build()
        .await
        .expect("build writer");
    let reader = writer.reader();
    let admin = FsAdmin::builder_with_store(store)
        .actor_id(writer_id)
        .build()
        .await
        .expect("build admin");
    TestRuntime {
        writer,
        reader,
        admin,
    }
}

/// Direct async access for tests that drive several operations inside one
/// runtime; everything else goes through the blocking trait below.
impl TestRuntime {
    async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> loonfs::Result<loonfs::NamespaceSummary> {
        self.writer.create_namespace(namespace_id, options).await
    }

    async fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> loonfs::Result<CommitResponse> {
        self.writer
            .put_file_bytes(namespace_id, absolute_path, bytes, options)
            .await
    }

    async fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> loonfs::Result<CommitResponse> {
        self.writer
            .put_file_content_ref(namespace_id, absolute_path, content_ref, options)
            .await
    }

    async fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<AuthoritativePathEntry> {
        self.reader.stat_path(namespace_id, absolute_path).await
    }

    async fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<Vec<AuthoritativePathEntry>> {
        self.reader.list_path(namespace_id, absolute_path).await
    }

    async fn list_path_entries(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<loonfs::ListPathEntriesResponse> {
        self.reader
            .list_path_entries(namespace_id, absolute_path)
            .await
    }

    async fn list_path_entries_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
    ) -> loonfs::Result<loonfs::ListPathEntriesResponse> {
        self.reader
            .list_path_entries_page(namespace_id, absolute_path, request)
            .await
    }

    async fn list_file_revisions_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<loonfs::FileRevisionsPageCursor>,
    ) -> loonfs::Result<loonfs::ListFileRevisionsResponse> {
        self.reader
            .list_file_revisions_page(namespace_id, absolute_path, request)
            .await
    }

    async fn list_file_revisions_for_inode_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: InodeId,
        request: PageRequest<loonfs::FileRevisionsPageCursor>,
    ) -> loonfs::Result<loonfs::ListFileRevisionsResponse> {
        self.reader
            .list_file_revisions_for_inode_page(namespace_id, inode_id, request)
            .await
    }

    async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<CreateCheckpointResponse> {
        self.admin
            .create_checkpoint(
                namespace_id,
                CreateCheckpointOptions {
                    name: "test-pin".to_owned(),
                    ttl_ms: None,
                },
            )
            .await
    }

    async fn begin_direct_put_upload_target(
        &self,
        namespace_id: &NamespaceId,
        content_ref: ContentRef,
    ) -> loonfs::Result<loonfs::BeginDirectPutUploadTargetResponse> {
        self.writer
            .begin_direct_put_upload_target(namespace_id, content_ref)
            .await
    }

    fn runtime_cache_stats(&self) -> loonfs::RuntimeCacheStats {
        self.writer.runtime_cache_stats()
    }
}

fn namespace_id() -> NamespaceId {
    NamespaceId::parse("demo").expect("valid namespace id")
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(future)
}

async fn wal_segment_count(store: &SharedObjectStore, namespace_id: &NamespaceId) -> usize {
    use futures::StreamExt;
    store
        .list_prefix_stream(&format!(
            "namespaces/{}/wal/segments/",
            namespace_id.as_str()
        ))
        .map(|key| key.expect("list wal segments"))
        .collect::<Vec<_>>()
        .await
        .len()
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
    ) -> loonfs::Result<NamespaceStatusResponse>;
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
    ) -> loonfs::Result<CommitResponse>;
    fn create_directory_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> loonfs::Result<CommitResponse>;
    fn delete_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> loonfs::Result<CommitResponse>;
    fn move_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> loonfs::Result<CommitResponse>;
    fn copy_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> loonfs::Result<CommitResponse>;
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

impl FsTestExt for TestRuntime {
    fn create_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> loonfs::Result<loonfs::NamespaceSummary> {
        block_on(self.writer.create_namespace(namespace_id, options))
    }

    fn fork_namespace_blocking(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> loonfs::Result<loonfs::NamespaceSummary> {
        block_on(self.writer.fork_namespace(source, target))
    }

    fn namespace_status_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<NamespaceStatusResponse> {
        block_on(self.admin.namespace_status(namespace_id))
    }

    fn maintenance_tick_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: MaintenanceTickOptions,
    ) -> loonfs::Result<MaintenanceTickResult> {
        block_on(self.admin.maintenance_tick_namespace(namespace_id, options))
    }

    fn stat_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<AuthoritativePathEntry> {
        block_on(self.reader.stat_path(namespace_id, absolute_path))
    }

    fn list_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<Vec<AuthoritativePathEntry>> {
        block_on(self.reader.list_path(namespace_id, absolute_path))
    }

    fn read_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<AuthoritativeFileBytes> {
        block_on(self.reader.read_file_bytes(namespace_id, absolute_path))
    }

    fn put_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> loonfs::Result<CommitResponse> {
        block_on(
            self.writer
                .put_file_bytes(namespace_id, absolute_path, bytes, options),
        )
    }

    fn create_directory_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> loonfs::Result<CommitResponse> {
        block_on(
            self.writer
                .create_directory(namespace_id, absolute_path, options),
        )
    }

    fn delete_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> loonfs::Result<CommitResponse> {
        block_on(
            self.writer
                .delete_path(namespace_id, absolute_path, options),
        )
    }

    fn move_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: MoveOptions,
    ) -> loonfs::Result<CommitResponse> {
        block_on(
            self.writer
                .move_path(namespace_id, from_path, to_path, options),
        )
    }

    fn copy_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        from_path: &str,
        to_path: &str,
        options: CopyOptions,
    ) -> loonfs::Result<CommitResponse> {
        block_on(
            self.writer
                .copy_path(namespace_id, from_path, to_path, options),
        )
    }

    fn begin_upload_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<BeginUploadResponse> {
        block_on(
            self.writer
                .begin_upload(namespace_id, BeginUploadRequest::default()),
        )
    }

    fn upload_content_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> loonfs::Result<UploadContentResponse> {
        block_on(self.writer.upload_content(namespace_id, upload_id, bytes))
    }

    fn complete_upload_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> loonfs::Result<CompleteUploadResponse> {
        block_on(
            self.writer
                .complete_upload(namespace_id, upload_id, request),
        )
    }

    fn commit_operations_blocking(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> loonfs::Result<CommitResponse> {
        block_on(self.writer.commit_operations(namespace_id, request))
    }

    fn commit_operations_batch_blocking(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<loonfs::Result<CommitResponse>> {
        block_on(self.writer.commit_operations_batch(namespace_id, requests))
    }

    fn list_changes_after_blocking(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> loonfs::Result<ChangesResponse> {
        block_on(self.reader.list_changes_after(
            namespace_id,
            after_seq,
            ListChangesOptions::default(),
        ))
    }

    fn create_checkpoint_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<CreateCheckpointResponse> {
        block_on(self.admin.create_checkpoint(
            namespace_id,
            CreateCheckpointOptions {
                name: "test-pin".to_owned(),
                ttl_ms: None,
            },
        ))
    }

    fn advance_retention_floor_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<AdvanceRetentionResponse> {
        block_on(self.admin.advance_retention_floor(namespace_id))
    }
}

fn assert_config_error<T>(result: loonfs::Result<T>, expected: &str) {
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

    assert_config_error(
        block_on(FsWriter::builder_with_store(object_store.clone()).build()),
        "writer_id",
    );
    assert_config_error(
        block_on(
            FsWriter::builder_with_store(object_store.clone())
                .writer_id("   ")
                .build(),
        ),
        "writer_id",
    );
    assert_config_error(
        block_on(
            FsWriter::builder_with_store(object_store)
                .writer_id("runtime-test")
                .writer_version("   ")
                .build(),
        ),
        "writer_version",
    );
}

#[test]
fn builder_metrics_recorder_instruments_object_store() {
    let temp_dir = tempdir().expect("tempdir");
    let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
    let fs = open_runtime_with(store(temp_dir.path()), "metrics-test", |builder| {
        builder
            .trace_store_kind(TraceStoreKind::LocalFs)
            .metrics_recorder(recorder.clone())
    });

    fs.create_namespace_blocking(&namespace_id(), CreateNamespaceOptions::default())
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
    let namespace_id = namespace_id();

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
    let fs = open_runtime_async(store(temp_dir.path()), "async-runtime-test").await;
    let namespace_id = namespace_id();

    FsWriter::create_namespace(&fs.writer, &namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    FsWriter::put_file_bytes(
        &fs.writer,
        &namespace_id,
        "/docs/hello.txt",
        b"hello",
        PutFileOptions::default(),
    )
    .await
    .expect("put file");

    let async_stat = FsReader::stat_path(&fs.reader, &namespace_id, "/docs/hello.txt")
        .await
        .expect("async stat");

    assert_eq!(async_stat.absolute_path, "/docs/hello.txt");
    assert_eq!(async_stat.size_bytes, Some(5));
}

#[test]
fn runtime_cache_reuses_wal_tail_projection_for_repeated_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "tail-projection-cache-test");

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
        .expect("first read is served from the projection the put seeded");
    assert_eq!(raw_store.wal_get_count(), 0);
    let after_first = fs.runtime_cache_stats();
    assert_eq!(after_first.wal_tail_projection_cache_misses, 0);
    assert!(after_first.wal_tail_projection_cache_inserts >= 1);
    assert!(after_first.wal_tail_projection_cache_hits >= 1);

    raw_store.reset_wal_get_count();
    fs.read_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("second read should reuse cached WAL-tail projection");
    assert_eq!(raw_store.wal_get_count(), 0);
    let after_second = fs.runtime_cache_stats();
    assert!(
        after_second.wal_tail_projection_cache_hits > after_first.wal_tail_projection_cache_hits
    );

    fs.put_file_bytes_blocking(
        &namespace_id,
        "/other.txt",
        b"other",
        PutFileOptions::default(),
    )
    .expect("put other");
    raw_store.reset_wal_get_count();
    fs.read_file_bytes_blocking(&namespace_id, "/docs/file.txt")
        .expect("read after local mutation reuses the newly seeded projection");
    assert_eq!(
        raw_store.wal_get_count(),
        0,
        "the put seeds the projection for its own resulting head"
    );
}

#[test]
fn runtime_publish_reuses_wal_tail_projection_for_sequential_writes() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let setup = open_runtime(object_store.clone(), "publish-tail");
    let measured = open_runtime(object_store, "publish-tail");

    setup
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    setup
        .create_directory_blocking(&namespace_id, "/seed-a", CreateDirectoryOptions::default())
        .expect("seed first WAL segment");
    setup
        .create_directory_blocking(&namespace_id, "/seed-b", CreateDirectoryOptions::default())
        .expect("seed second WAL segment");

    raw_store.reset_wal_get_count();
    measured
        .create_directory_blocking(
            &namespace_id,
            "/measured-a",
            CreateDirectoryOptions::default(),
        )
        .expect("first measured write loads existing tail");
    assert!(
        raw_store.wal_get_count() > 0,
        "first measured write should read the existing WAL tail"
    );

    raw_store.reset_wal_get_count();
    measured
        .create_directory_blocking(
            &namespace_id,
            "/measured-b",
            CreateDirectoryOptions::default(),
        )
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
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let setup = open_runtime(object_store.clone(), "publish-tail");
    let measured = open_runtime(object_store, "publish-tail");

    setup
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    setup
        .create_directory_blocking(&namespace_id, "/seed-a", CreateDirectoryOptions::default())
        .expect("seed first WAL segment");
    setup
        .create_directory_blocking(&namespace_id, "/seed-b", CreateDirectoryOptions::default())
        .expect("seed second WAL segment");

    measured
        .create_directory_blocking(
            &namespace_id,
            "/should-succeed",
            CreateDirectoryOptions::default(),
        )
        .expect("publish projects the visible WAL tail without a segment limit");
}

#[test]
fn runtime_cache_observes_head_advanced_by_another_runtime() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let reader = open_runtime(object_store.clone(), "tail-cache-reader");
    let writer = open_runtime(object_store, "tail-cache-writer");

    writer
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .create_directory_blocking(&namespace_id, "/docs", CreateDirectoryOptions::default())
        .expect("create docs");

    reader
        .stat_path_blocking(&namespace_id, "/docs")
        .expect("prime reader cache");

    writer
        .create_directory_blocking(
            &namespace_id,
            "/docs/new",
            CreateDirectoryOptions::default(),
        )
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
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime_with(object_store, "tail-cache-disabled-test", |builder| {
        builder.runtime_cache(RuntimeCacheConfig::disabled())
    });

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
    let setup = open_runtime(shared_store.clone(), "tail-count-setup");
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

    let fs = open_runtime_with(shared_store, "tail-count-budget", |builder| {
        builder.runtime_cache(RuntimeCacheConfig {
            max_cached_namespaces: 1,
            ..RuntimeCacheConfig::default()
        })
    });

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
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime_with(object_store, "tail-oversized-test", |builder| {
        builder.runtime_cache(RuntimeCacheConfig {
            max_cached_wal_tail_projection_rows: 0,
            ..RuntimeCacheConfig::default()
        })
    });

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
    let namespace_id = namespace_id();
    let fs = open_runtime(store(temp_dir.path()), "tail-read-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(&namespace_id, "/docs", CreateDirectoryOptions::default())
        .expect("create docs");
    fs.create_directory_blocking(&namespace_id, "/more", CreateDirectoryOptions::default())
        .expect("create another WAL segment");

    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("read projects the visible WAL tail without a segment limit");
}

#[test]
fn stale_head_write_error_recovers_and_reseeds_caches() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "tail-cache-stale-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(&namespace_id, "/docs", CreateDirectoryOptions::default())
        .expect("create docs");
    fs.stat_path_blocking(&namespace_id, "/docs")
        .expect("prime read cache");

    raw_store.fail_head_cas();
    assert_core_error_kind(
        fs.create_directory_blocking(&namespace_id, "/stale", CreateDirectoryOptions::default()),
        ErrorCode::StaleHead,
    );

    raw_store.allow_head_cas();
    fs.create_directory_blocking(
        &namespace_id,
        "/after-stale",
        CreateDirectoryOptions::default(),
    )
    .expect("write after stale head succeeds (the engine revalidates its projection by etag)");

    raw_store.reset_wal_get_count();
    fs.stat_path_blocking(&namespace_id, "/after-stale")
        .expect("read after the recovered write");
    assert_eq!(
        raw_store.wal_get_count(),
        0,
        "the recovered write seeds the read caches like any landed publish"
    );
}

#[test]
fn delete_options_select_recursive_behavior() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "delete-test");
    let namespace_id = namespace_id();

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
            expected_inode_id: None,
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
fn undelete_recovers_a_deleted_file_and_generations_stay_scoped() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-test");
    let namespace_id = namespace_id();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"draft one",
        PutFileOptions::default(),
    )
    .expect("put revision one");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"draft two",
        PutFileOptions {
            behavior: PutBehavior::Replace,
            commit_id: None,
        },
    )
    .expect("put revision two");
    let inode_id = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat before delete")
        .inode_id;

    let first_deletion = fs
        .delete_path_blocking(&namespace_id, "/docs/report.txt", DeleteOptions::default())
        .expect("delete file")
        .committed_seq;

    // Recovery re-attaches the same inode — identity, content, and the full
    // revision history come back, even at a new path.
    block_on(fs.writer.undelete(
        &namespace_id,
        inode_id,
        first_deletion,
        "/docs/recovered.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect("undelete");
    let recovered = fs
        .stat_path_blocking(&namespace_id, "/docs/recovered.txt")
        .expect("stat recovered file");
    assert_eq!(recovered.inode_id, inode_id);
    assert_eq!(
        fs.read_file_bytes_blocking(&namespace_id, "/docs/recovered.txt")
            .expect("read recovered content")
            .bytes,
        b"draft two"
    );
    assert_eq!(
        block_on(fs.reader.read_file_revision_bytes(
            &namespace_id,
            "/docs/recovered.txt",
            loonfs::RevisionNo(1),
        ))
        .expect("read prior revision through the recovered path")
        .bytes,
        b"draft one"
    );

    // The recovered inode is no longer deleted: replaying the handle
    // conflicts.
    let error = block_on(fs.writer.undelete(
        &namespace_id,
        inode_id,
        first_deletion,
        "/docs/again.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect_err("double undelete should conflict");
    assert!(matches!(
        &error,
        RuntimeError::Core(error) if error.code() == ErrorCode::NotDeleted
    ));

    // Delete again: the old generation handle must not cancel the new
    // deletion, and the failure names both generations.
    let second_deletion = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/recovered.txt",
            DeleteOptions::default(),
        )
        .expect("delete recovered file again")
        .committed_seq;
    let error = block_on(fs.writer.undelete(
        &namespace_id,
        inode_id,
        first_deletion,
        "/docs/stale.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect_err("stale generation handle must not clear the newer deletion");
    match &error {
        RuntimeError::Core(error) => {
            assert_eq!(error.code(), ErrorCode::NotDeleted);
            let details = error.details().expect("generation mismatch details");
            assert_eq!(details.requested_deletion_seq, Some(first_deletion));
            assert_eq!(details.active_deletion_seq, Some(second_deletion));
        }
        other => panic!("expected core error, got {other:?}"),
    }
    let still_gone = fs.stat_path_blocking(&namespace_id, "/docs/stale.txt");
    assert!(still_gone.is_err(), "stale undelete must not bind anything");

    // The current generation's handle recovers to the original path.
    block_on(fs.writer.undelete(
        &namespace_id,
        inode_id,
        second_deletion,
        "/docs/report.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect("undelete the active generation");
    assert_eq!(
        fs.stat_path_blocking(&namespace_id, "/docs/report.txt")
            .expect("stat restored original path")
            .inode_id,
        inode_id
    );
}

#[test]
fn undelete_recovers_a_deleted_subtree_and_rejects_covered_children() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-subtree-test");
    let namespace_id = namespace_id();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/notes/a.txt",
        b"alpha",
        PutFileOptions::default(),
    )
    .expect("put nested file");
    let directory_inode = fs
        .stat_path_blocking(&namespace_id, "/docs/notes")
        .expect("stat directory")
        .inode_id;
    let child_inode = fs
        .stat_path_blocking(&namespace_id, "/docs/notes/a.txt")
        .expect("stat child")
        .inode_id;

    let deletion = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/notes",
            DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                commit_id: None,
                expected_inode_id: None,
            },
        )
        .expect("recursive delete")
        .committed_seq;

    // A child is covered by the subtree root's tombstone, not its own:
    // recovery targets the root.
    let error = block_on(fs.writer.undelete(
        &namespace_id,
        child_inode,
        deletion,
        "/docs/a-alone.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect_err("child of a deleted directory is not the deletion root");
    assert!(matches!(
        &error,
        RuntimeError::Core(error) if error.code() == ErrorCode::NotDeleted
    ));

    block_on(fs.writer.undelete(
        &namespace_id,
        directory_inode,
        deletion,
        "/docs/notes",
        loonfs::UndeleteOptions::default(),
    ))
    .expect("undelete the subtree root");
    assert_eq!(
        fs.read_file_bytes_blocking(&namespace_id, "/docs/notes/a.txt")
            .expect("nested file is visible again")
            .bytes,
        b"alpha"
    );
}

#[test]
fn undelete_of_an_ancestor_keeps_independently_deleted_children_hidden() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-nested-test");
    let namespace_id = namespace_id();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/notes/secret.txt",
        b"independently deleted",
        PutFileOptions::default(),
    )
    .expect("put nested file");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/notes/kept.txt",
        b"kept",
        PutFileOptions::default(),
    )
    .expect("put sibling file");
    let directory_inode = fs
        .stat_path_blocking(&namespace_id, "/docs/notes")
        .expect("stat directory")
        .inode_id;

    // Delete the child on its own, then the whole ancestor directory.
    fs.delete_path_blocking(
        &namespace_id,
        "/docs/notes/secret.txt",
        DeleteOptions::default(),
    )
    .expect("delete child independently");
    let ancestor_deletion = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/notes",
            DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::Recursive,
                commit_id: None,
                expected_inode_id: None,
            },
        )
        .expect("recursive delete of the ancestor")
        .committed_seq;

    // Recovering the ancestor revokes exactly its own deletion: the
    // independently deleted child stays hidden behind its own tombstone.
    block_on(fs.writer.undelete(
        &namespace_id,
        directory_inode,
        ancestor_deletion,
        "/docs/notes",
        loonfs::UndeleteOptions::default(),
    ))
    .expect("undelete the ancestor");
    assert_eq!(
        fs.read_file_bytes_blocking(&namespace_id, "/docs/notes/kept.txt")
            .expect("sibling is visible again")
            .bytes,
        b"kept"
    );
    let hidden = fs.stat_path_blocking(&namespace_id, "/docs/notes/secret.txt");
    assert!(matches!(
        hidden,
        Err(RuntimeError::Core(error)) if error.code() == ErrorCode::PathNotFound
    ));
}

#[test]
fn undelete_survives_checkpoints_and_reopen_in_both_orders() {
    let temp_dir = tempdir().expect("tempdir");
    let object_store = store(temp_dir.path());
    let namespace_id = namespace_id();

    // Order one: delete + undelete in the WAL tail, then checkpoint,
    // then reopen cold from object storage.
    let deletion = {
        let fs = open_runtime(object_store.clone(), "undelete-persist-a");
        fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
            .expect("create namespace");
        fs.put_file_bytes_blocking(
            &namespace_id,
            "/docs/report.txt",
            b"persisted",
            PutFileOptions::default(),
        )
        .expect("put file");
        let inode_id = fs
            .stat_path_blocking(&namespace_id, "/docs/report.txt")
            .expect("stat")
            .inode_id;
        let deletion = fs
            .delete_path_blocking(&namespace_id, "/docs/report.txt", DeleteOptions::default())
            .expect("delete")
            .committed_seq;
        block_on(fs.writer.undelete(
            &namespace_id,
            inode_id,
            deletion,
            "/docs/report.txt",
            loonfs::UndeleteOptions::default(),
        ))
        .expect("undelete before checkpoint");
        // The default threshold (32 segments) would answer NotNeeded for
        // this short history; force the flush so reopen reads Set and
        // Revoke rows out of durable tables, not WAL replay.
        let tick = fs
            .maintenance_tick_namespace_blocking(
                &namespace_id,
                MaintenanceTickOptions {
                    max_wal_tail_segments: 1,
                    gc: None,
                },
            )
            .expect("checkpoint the revoke into durable tables");
        assert!(
            matches!(
                tick.outcome,
                loonfs::MaintenanceTickOutcome::WalFlushed { .. }
            ),
            "tick must materialize the tail, got {:?}",
            tick.outcome
        );
        deletion
    };
    {
        let fs = open_runtime(object_store.clone(), "undelete-persist-b");
        assert_eq!(
            fs.read_file_bytes_blocking(&namespace_id, "/docs/report.txt")
                .expect("recovered file survives checkpoint and reopen")
                .bytes,
            b"persisted"
        );

        // Order two: delete, checkpoint, reopen, THEN undelete — the
        // revoke must resolve a deletion that lives in durable tables,
        // not the WAL tail.
        let inode_id = fs
            .stat_path_blocking(&namespace_id, "/docs/report.txt")
            .expect("stat")
            .inode_id;
        let second_deletion = fs
            .delete_path_blocking(&namespace_id, "/docs/report.txt", DeleteOptions::default())
            .expect("delete again")
            .committed_seq;
        assert!(second_deletion > deletion);
        let tick = fs
            .maintenance_tick_namespace_blocking(
                &namespace_id,
                MaintenanceTickOptions {
                    max_wal_tail_segments: 1,
                    gc: None,
                },
            )
            .expect("checkpoint the deletion");
        assert!(
            matches!(
                tick.outcome,
                loonfs::MaintenanceTickOutcome::WalFlushed { .. }
            ),
            "tick must materialize the tail, got {:?}",
            tick.outcome
        );
        let fs = open_runtime(object_store.clone(), "undelete-persist-c");
        block_on(fs.writer.undelete(
            &namespace_id,
            inode_id,
            second_deletion,
            "/docs/report.txt",
            loonfs::UndeleteOptions::default(),
        ))
        .expect("undelete a checkpointed deletion after reopen");
        let tick = fs
            .maintenance_tick_namespace_blocking(
                &namespace_id,
                MaintenanceTickOptions {
                    max_wal_tail_segments: 1,
                    gc: None,
                },
            )
            .expect("checkpoint the second revoke");
        assert!(
            matches!(
                tick.outcome,
                loonfs::MaintenanceTickOutcome::WalFlushed { .. }
            ),
            "tick must materialize the tail, got {:?}",
            tick.outcome
        );
    }
    let fs = open_runtime(object_store, "undelete-persist-d");
    assert_eq!(
        fs.read_file_bytes_blocking(&namespace_id, "/docs/report.txt")
            .expect("recovered file survives the second cycle")
            .bytes,
        b"persisted"
    );
}

#[test]
fn change_feed_carries_the_exact_revoked_generation() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-feed-test");
    let namespace_id = namespace_id();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"feed",
        PutFileOptions::default(),
    )
    .expect("put file");
    let inode_id = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat")
        .inode_id;
    let deletion = fs
        .delete_path_blocking(&namespace_id, "/docs/report.txt", DeleteOptions::default())
        .expect("delete")
        .committed_seq;
    block_on(fs.writer.undelete(
        &namespace_id,
        inode_id,
        deletion,
        "/docs/report.txt",
        loonfs::UndeleteOptions::default(),
    ))
    .expect("undelete");

    let changes = block_on(fs.reader.list_changes_after(
        &namespace_id,
        ChangeSeq(0),
        ListChangesOptions::default(),
    ))
    .expect("list changes");
    let mut tombstone_target = None;
    let mut revoke_target = None;
    for change in &changes.changes {
        for delta in &change.deltas {
            match delta {
                loonfs::CommitDelta::TombstoneSubtree {
                    delta_index,
                    root_inode_id,
                    ..
                } if *root_inode_id == inode_id => {
                    tombstone_target = Some((change.seq, *delta_index));
                }
                loonfs::CommitDelta::RevokeSubtreeTombstone {
                    root_inode_id,
                    target_seq,
                    target_delta_index,
                    ..
                } if *root_inode_id == inode_id => {
                    revoke_target = Some((*target_seq, *target_delta_index));
                }
                _ => {}
            }
        }
    }
    // The revoke names the exact deletion event it cancels, so a
    // projection can reduce state without guessing at "newest".
    let tombstone_target = tombstone_target.expect("delete emitted a tombstone delta");
    assert_eq!(tombstone_target.0, deletion);
    assert_eq!(revoke_target, Some(tombstone_target));
}

#[test]
fn undelete_rejects_deletions_from_the_same_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "undelete-same-commit-test");
    let namespace_id = namespace_id();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"cycled",
        PutFileOptions::default(),
    )
    .expect("put file");
    let entry = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat");

    // Assigned sequences are head + 1 and therefore guessable: without the
    // earlier-commit bound, one commit could delete, undelete, and
    // re-delete the inode, minting two deletion generations that share a
    // sequence. The undelete must refuse a target in its own commit.
    let guessed_seq = ChangeSeq(entry.head_seq.0 + 1);
    let error = fs
        .commit_operations_blocking(
            &namespace_id,
            CommitRequest {
                commit_id: CommitId::parse("same-commit-cycle").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![
                    CommitOp::DeleteFile {
                        inode_id: entry.inode_id,
                    },
                    CommitOp::Undelete {
                        inode_id: entry.inode_id,
                        deleted_at_seq: guessed_seq,
                        parent_inode_id: InodeId(1),
                        display_name: "resurrected.txt".to_owned(),
                    },
                ],
                message: None,
            },
        )
        .expect_err("same-commit delete/undelete cycling must be rejected");
    assert!(matches!(
        &error,
        RuntimeError::Core(error) if error.code() == ErrorCode::NotDeleted
    ));
    // The rejected commit changed nothing.
    assert_eq!(
        fs.read_file_bytes_blocking(&namespace_id, "/docs/report.txt")
            .expect("file untouched")
            .bytes,
        b"cycled"
    );
}

#[test]
fn delete_with_expected_inode_refuses_a_raced_rebinding() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "delete-expectation-test");
    let namespace_id = namespace_id();
    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"original",
        PutFileOptions::default(),
    )
    .expect("put file");
    let inode_id = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect("stat")
        .inode_id;

    // Stand-in for a rebinding that raced the caller's stat: the path now
    // holds a different inode than the one the caller resolved.
    let error = fs
        .delete_path_blocking(
            &namespace_id,
            "/docs/report.txt",
            DeleteOptions {
                behavior: loonfs::DeleteDirectoryBehavior::NonRecursive,
                commit_id: None,
                expected_inode_id: Some(InodeId(inode_id.0 + 1)),
            },
        )
        .expect_err("a mismatched expectation must fail the delete");
    assert!(matches!(
        &error,
        RuntimeError::Core(error) if error.code() == ErrorCode::PathConflict
    ));
    assert_eq!(
        fs.read_file_bytes_blocking(&namespace_id, "/docs/report.txt")
            .expect("file untouched")
            .bytes,
        b"original"
    );

    // The matching expectation deletes exactly that inode.
    fs.delete_path_blocking(
        &namespace_id,
        "/docs/report.txt",
        DeleteOptions {
            behavior: loonfs::DeleteDirectoryBehavior::NonRecursive,
            commit_id: None,
            expected_inode_id: Some(inode_id),
        },
    )
    .expect("matching expectation deletes");
}

#[test]
fn directory_pages_use_canonical_name_key_order() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "directory-page-order-test");
    let namespace_id = namespace_id();
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
    let namespace_id = namespace_id();
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
    let namespace_id = namespace_id();
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
    let namespace_id = namespace_id();
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
    let namespace_id = namespace_id();
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
    let source = namespace_id();
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
    let namespace_id = namespace_id();

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
    let namespace_id = namespace_id();
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
fn direct_put_completion_proves_upload_without_reading_content() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let raw_store = Arc::new(ContentBlobGetCountingStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "direct-put-probe-test");
    let bytes = b"direct uploaded, provider verified";
    let content_ref = ContentRef::whole_file_v0(bytes);

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = block_on(fs.begin_direct_put_upload_target(&namespace_id, content_ref.clone()))
        .expect("begin direct put");

    // Stands in for the provider-verified presigned upload.
    let direct_store = LocalFsStore::new(temp_dir.path()).expect("direct object-store handle");
    block_on(direct_store.put_if_absent(&begin.target.object_key, Bytes::copy_from_slice(bytes)))
        .expect("write direct object");

    raw_store.reset_content_blob_counters();
    let completed = fs
        .complete_upload_blocking(
            &namespace_id,
            &begin.upload_id,
            &CompleteUploadRequest {
                content_ref: content_ref.clone(),
            },
        )
        .expect("complete direct put");
    assert_eq!(completed.content_ref, content_ref);
    assert_eq!(
        raw_store.content_blob_get_count(),
        0,
        "completion proves the upload from object metadata alone"
    );
}

#[test]
fn direct_put_completion_rejects_a_mis_declared_size() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "direct-put-size-test");
    let namespace_id = namespace_id();
    let bytes = b"direct put bytes with a lying size";
    // The digest names the object; the declared size rides the reference
    // unchecked by the provider, so completion's size check must catch it.
    let mut content_ref = ContentRef::whole_file_v0(bytes);
    content_ref.size_bytes += 1;

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    let begin = block_on(fs.begin_direct_put_upload_target(&namespace_id, content_ref.clone()))
        .expect("begin direct put");

    let direct_store = LocalFsStore::new(temp_dir.path()).expect("direct object-store handle");
    block_on(direct_store.put_if_absent(&begin.target.object_key, Bytes::copy_from_slice(bytes)))
        .expect("write direct object");

    let error = fs
        .complete_upload_blocking(
            &namespace_id,
            &begin.upload_id,
            &CompleteUploadRequest { content_ref },
        )
        .expect_err("mis-declared size must fail completion");
    assert!(
        error.to_string().contains("content length mismatch"),
        "completion names the size mismatch: {error}"
    );
}

#[test]
fn stat_and_list_use_initial_manifest_without_checkpoint() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let fs = runtime(temp_dir.path(), "read-fallback-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(&namespace_id, "/docs", CreateDirectoryOptions::default())
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
    let namespace_id = namespace_id();
    let raw_store = Arc::new(ContentBlobGetCountingStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "read-materialized-test");

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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_materialized_stat_and_list_share_async_store() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let fs = open_runtime_async(store(temp_dir.path()), "concurrent-materialized-read-test").await;

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
    let namespace_id = namespace_id();
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
fn put_file_bytes_gates_publish_on_its_own_content_write_without_probing() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let raw_store = Arc::new(ContentBlobGetCountingStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "put-file-content-validation-test");

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

    // The put writes the blob exactly once and never reads it back: the
    // write's own ack is the durability proof the head CAS waits on, so
    // validation issues no probe for content the put itself is writing.
    assert_eq!(raw_store.content_blob_put_count(), 1);
    assert_eq!(raw_store.content_blob_get_count(), 0);

    // A replace put rides the same overlapped path: new blob, no probe.
    raw_store.reset_content_blob_counters();
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/direct.txt",
        b"replaced bytes",
        PutFileOptions {
            behavior: PutBehavior::Replace,
            commit_id: None,
        },
    )
    .expect("replace file bytes");

    assert_eq!(raw_store.content_blob_put_count(), 1);
    assert_eq!(raw_store.content_blob_get_count(), 0);
}

#[test]
fn put_file_bytes_content_write_failure_leaves_nothing_visible_and_a_retry_lands() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let raw_store = Arc::new(FailContentBlobPutsStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "content-write-failure-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    let commit_id = CommitId::parse("overlap-put-retry").expect("valid commit id");
    raw_store.fail_next_content_blob_puts(1);
    let error = fs
        .put_file_bytes_blocking(
            &namespace_id,
            "/docs/report.txt",
            b"overlap survives",
            PutFileOptions {
                behavior: PutBehavior::NoReplace,
                commit_id: Some(commit_id.clone()),
            },
        )
        .expect_err("put should surface the failed content write");
    // A put reports its own write's error, not publish plumbing.
    assert!(
        error.to_string().contains("injected content write failure"),
        "unexpected error: {error}"
    );

    // Content is staged before the publish is submitted, so the failed write
    // stopped the put with the head unmoved and nothing visible.
    let error = fs
        .stat_path_blocking(&namespace_id, "/docs/report.txt")
        .expect_err("failed put should leave the path unbound");
    assert!(matches!(
        error,
        RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::PathNotFound
    ));

    // The failed attempt never committed, so the same commit id retries
    // cleanly instead of resolving as a duplicate.
    fs.put_file_bytes_blocking(
        &namespace_id,
        "/docs/report.txt",
        b"overlap survives",
        PutFileOptions {
            behavior: PutBehavior::NoReplace,
            commit_id: Some(commit_id),
        },
    )
    .expect("same-commit-id retry should land");
    let read = fs
        .read_file_bytes_blocking(&namespace_id, "/docs/report.txt")
        .expect("read retried file");
    assert_eq!(read.bytes, b"overlap survives");
}

#[test]
fn path_mutations_return_the_commit_id_they_committed_under() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let object_store = store(temp_dir.path());
    block_on(async {
        let fs = open_runtime_async(object_store, "commit-id-echo-test").await;
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");

        let commit_id = CommitId::parse("retry-key-1").expect("valid commit id");
        let first = fs
            .put_file_bytes(
                &namespace_id,
                "/docs/a.txt",
                b"alpha",
                PutFileOptions {
                    commit_id: Some(commit_id.clone()),
                    ..PutFileOptions::default()
                },
            )
            .await
            .expect("first put");
        assert_eq!(first.namespace_id, namespace_id);
        assert_eq!(first.commit_id, commit_id);

        // Resubmitting the identical mutation with the same commit id
        // replays the original commit instead of committing again.
        let replay = fs
            .put_file_bytes(
                &namespace_id,
                "/docs/a.txt",
                b"alpha",
                PutFileOptions {
                    commit_id: Some(commit_id.clone()),
                    ..PutFileOptions::default()
                },
            )
            .await
            .expect("identical resubmission replays the original commit");
        assert_eq!(replay.commit_id, first.commit_id);
        assert_eq!(replay.committed_seq, first.committed_seq);

        // Without a caller-supplied id, the generated one is still returned,
        // so every caller holds a reconciliation handle.
        let generated = fs
            .writer
            .create_directory(
                &namespace_id,
                "/docs/sub",
                CreateDirectoryOptions::default(),
            )
            .await
            .expect("mkdir");
        assert!(!generated.commit_id.as_str().is_empty());
        assert_ne!(generated.commit_id, first.commit_id);
        assert!(generated.committed_seq > first.committed_seq);
    });
}

#[test]
fn concurrent_puts_coalesce_into_one_wal_segment() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let object_store = store(temp_dir.path());
    block_on(async {
        let fs = open_runtime_async(object_store.clone(), "publication-batch-test").await;
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");

        // Stage every file's content first: a put publishes only after its
        // bytes are durable, so racing already-staged publishes is what
        // reaches the publication queue together deterministically.
        let mut content_refs = Vec::new();
        for bytes in [b"alpha" as &[u8], b"beta", b"gamma", b"delta"] {
            let begin = fs
                .writer
                .begin_upload(&namespace_id, BeginUploadRequest::default())
                .await
                .expect("begin upload");
            let staged = fs
                .writer
                .upload_content(&namespace_id, &begin.upload_id, bytes)
                .await
                .expect("upload content");
            let completed = fs
                .writer
                .complete_upload(
                    &namespace_id,
                    &begin.upload_id,
                    &CompleteUploadRequest {
                        content_ref: staged.content_ref,
                    },
                )
                .await
                .expect("complete upload");
            content_refs.push(completed.content_ref);
        }
        let [ref_a, ref_b, ref_c, ref_d] = content_refs.try_into().expect("four staged refs");
        let segments_before = wal_segment_count(&object_store, &namespace_id).await;

        let puts = tokio::join!(
            fs.put_file_content_ref(
                &namespace_id,
                "/docs/a.txt",
                ref_a,
                PutFileOptions::default()
            ),
            fs.put_file_content_ref(
                &namespace_id,
                "/docs/b.txt",
                ref_b,
                PutFileOptions::default()
            ),
            fs.put_file_content_ref(
                &namespace_id,
                "/docs/c.txt",
                ref_c,
                PutFileOptions::default()
            ),
            fs.put_file_content_ref(
                &namespace_id,
                "/docs/d.txt",
                ref_d,
                PutFileOptions::default()
            ),
        );
        puts.0.expect("put a");
        puts.1.expect("put b");
        puts.2.expect("put c");
        puts.3.expect("put d");

        // All four submissions were admitted before the publish task's
        // first take and published as one batch: one WAL segment, one
        // head CAS.
        let segments_after = wal_segment_count(&object_store, &namespace_id).await;
        assert_eq!(segments_after - segments_before, 1);

        for (path, bytes) in [
            ("/docs/a.txt", b"alpha" as &[u8]),
            ("/docs/b.txt", b"beta"),
            ("/docs/c.txt", b"gamma"),
            ("/docs/d.txt", b"delta"),
        ] {
            let read = fs
                .reader
                .read_file_bytes(&namespace_id, path)
                .await
                .expect("read coalesced file");
            assert_eq!(read.bytes, bytes);
        }
    });
}

#[test]
fn zero_interval_publishes_sequential_submissions_immediately() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let object_store = store(temp_dir.path());
    block_on(async {
        let fs = open_runtime_with_async(object_store.clone(), "zero-interval-test", |builder| {
            builder.min_publish_interval_ms(0)
        })
        .await;
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
        let segments_before = wal_segment_count(&object_store, &namespace_id).await;

        // Sequential awaited puts leave nothing to batch: with a zero
        // pacing interval each publishes immediately as its own WAL
        // segment. (Concurrent submissions may still batch behind an
        // in-flight publication — that is load-driven, not timer-driven.)
        for (path, bytes) in [
            ("/docs/a.txt", b"alpha".as_slice()),
            ("/docs/b.txt", b"beta".as_slice()),
            ("/docs/c.txt", b"gamma".as_slice()),
        ] {
            fs.put_file_bytes(&namespace_id, path, bytes, PutFileOptions::default())
                .await
                .expect("sequential put");
        }

        let segments_after = wal_segment_count(&object_store, &namespace_id).await;
        assert_eq!(segments_after - segments_before, 3);
    });
}

#[test]
fn concurrent_put_content_write_failure_leaves_the_other_put_committed() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let raw_store = Arc::new(FailContentBlobPutsStore::new(temp_dir.path()));
    let object_store: SharedObjectStore = raw_store.clone();
    block_on(async {
        let fs = open_runtime_async(object_store, "window-abort-test").await;
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");

        // Exactly one of the two concurrent content writes fails. Content is
        // staged before a submission enters the commit window, so only the
        // put whose own upload failed errors; the other publishes normally.
        raw_store.fail_next_content_blob_puts(1);
        let (a, b) = tokio::join!(
            fs.put_file_bytes(
                &namespace_id,
                "/docs/a.txt",
                b"alpha",
                PutFileOptions::default()
            ),
            fs.put_file_bytes(
                &namespace_id,
                "/docs/b.txt",
                b"beta",
                PutFileOptions::default()
            ),
        );

        let mut failed = Vec::new();
        for (path, bytes, result) in [
            ("/docs/a.txt", b"alpha" as &[u8], a),
            ("/docs/b.txt", b"beta" as &[u8], b),
        ] {
            match result {
                Ok(_) => {
                    let read = fs
                        .reader
                        .read_file_bytes(&namespace_id, path)
                        .await
                        .expect("read the committed peer");
                    assert_eq!(read.bytes, bytes);
                }
                Err(error) => {
                    // The failing member reports its own write's error, and
                    // its path stays unbound behind an unmoved head.
                    assert!(
                        error.to_string().contains("injected content write failure"),
                        "unexpected error: {error}"
                    );
                    let error = fs
                        .reader
                        .stat_path(&namespace_id, path)
                        .await
                        .expect_err("failed put should leave the path unbound");
                    assert!(matches!(
                        error,
                        RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::PathNotFound
                    ));
                    failed.push((path, bytes));
                }
            }
        }
        let [(failed_path, failed_bytes)] = failed[..] else {
            panic!("exactly one put should fail its own content write");
        };

        // The failed member retries cleanly; its peer never needed one.
        fs.put_file_bytes(
            &namespace_id,
            failed_path,
            failed_bytes,
            PutFileOptions::default(),
        )
        .await
        .expect("retried put");
        let read = fs
            .reader
            .read_file_bytes(&namespace_id, failed_path)
            .await
            .expect("read retried file");
        assert_eq!(read.bytes, failed_bytes);
    });
}

#[test]
fn begin_upload_validates_controls_without_replay_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "begin-upload-cache-test");

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
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "control-cache-head-test");

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(&namespace_id, "/docs", CreateDirectoryOptions::default())
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
    let namespace_id = namespace_id();
    let other_namespace = NamespaceId::parse("other").expect("valid namespace id");
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime_with(object_store, "control-cache-eviction-test", |builder| {
        builder.runtime_cache(RuntimeCacheConfig {
            max_cached_namespaces: 1,
            ..RuntimeCacheConfig::default()
        })
    });

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    fs.create_directory_blocking(&namespace_id, "/docs", CreateDirectoryOptions::default())
        .expect("create docs");
    fs.create_namespace_blocking(&other_namespace, CreateNamespaceOptions::default())
        .expect("create other namespace");
    fs.create_directory_blocking(&other_namespace, "/docs", CreateDirectoryOptions::default())
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
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let reader = open_runtime(object_store.clone(), "control-cache-reader");
    let writer = open_runtime(object_store, "control-cache-writer");

    writer
        .create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");
    writer
        .create_directory_blocking(&namespace_id, "/docs", CreateDirectoryOptions::default())
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
        .create_directory_blocking(
            &namespace_id,
            "/docs/new",
            CreateDirectoryOptions::default(),
        )
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
    let fs = open_runtime(object_store, "begin-upload-missing-partial-test");
    let namespace_id = namespace_id();

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
    let fs = open_runtime(object_store, "begin-upload-malformed-test");
    let namespace_id = namespace_id();

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
    let fs = open_runtime_with(
        object_store,
        "begin-upload-malformed-control-test",
        |builder| builder.runtime_cache(RuntimeCacheConfig::disabled()),
    );

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
    let namespace_id = namespace_id();

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
fn namespace_status_reports_wal_tail_segments() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "status-test");
    let namespace_id = namespace_id();

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
    let namespace_id = namespace_id();

    fs.create_namespace_blocking(&namespace_id, CreateNamespaceOptions::default())
        .expect("create namespace");

    let root = fs
        .stat_path_blocking(&namespace_id, "/")
        .expect("stat root after create");
    assert_eq!(root.absolute_path, "/");
    assert_eq!(root.inode_id, InodeId(1));
    assert_eq!(root.inode_kind, InodeKind::Directory);
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
    let namespace_id = namespace_id();

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
    let fs = open_runtime(object_store, "partial-status-test");
    let namespace_id = namespace_id();

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
    let namespace_id = namespace_id();

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
fn maintenance_tick_at_segment_threshold_flushes_the_wal() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-publish-test");
    let namespace_id = namespace_id();

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
        MaintenanceTickOutcome::WalFlushed {
            manifest_head_seq: ChangeSeq(1)
        }
    );

    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after wal flush");
    assert_eq!(status.current_manifest_id, Some(ManifestId(1)));
    assert_eq!(status.wal_tail_segments, 0);

    // Maintenance is record-less: flushing the WAL must leave nothing
    // under `checkpoints/`.
    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let records = block_on(
        raw_store.list_prefix(&loonfs_objectstore::keys::checkpoint_prefix(
            namespace_id.as_str(),
        )),
    )
    .expect("list checkpoint records");
    assert!(
        records.is_empty(),
        "maintenance tick created checkpoint records: {records:?}"
    );
}

#[test]
fn maintenance_tick_after_existing_manifest_writes_l0_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-l0-run-publish-test");
    let namespace_id = namespace_id();

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
        MaintenanceTickOutcome::WalFlushed {
            manifest_head_seq: ChangeSeq(2)
        }
    );

    let status = fs
        .namespace_status_blocking(&namespace_id)
        .expect("status after l0 wal flush");
    assert_eq!(status.current_manifest_id, Some(ManifestId(2)));
    assert_eq!(status.wal_tail_segments, 0);

    let raw_store = LocalFsStore::new(temp_dir.path()).expect("store");
    let root = block_on(loonfs_core::control::load_namespace_metadata_root_control(
        &raw_store,
        &namespace_id,
    ))
    .expect("metadata root");
    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &root.state.manifest_object_id);
    let manifest_bytes = block_on(raw_store.get(&manifest_key, None))
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    // A WAL flush only appends: the base marker stays the seed's until
    // reorganization folds the delta runs.
    assert_eq!(manifest.payload.base_seq, ChangeSeq(0));
    let l0_files = manifest
        .payload
        .metadata_files
        .iter()
        .filter(|metadata_file| metadata_file.level == 0)
        .collect::<Vec<_>>();
    assert!(!l0_files.is_empty());
    assert!(l0_files
        .iter()
        .any(|metadata_file| metadata_file.run_seq == ChangeSeq(2)));
}

#[test]
fn maintenance_tick_counts_segments_not_commits() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-segment-count-test");
    let namespace_id = namespace_id();

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
        MaintenanceTickOutcome::WalFlushed {
            manifest_head_seq: ChangeSeq(3)
        }
    );
}

#[test]
fn maintenance_tick_rejects_zero_threshold() {
    let temp_dir = tempdir().expect("tempdir");
    let fs = runtime(temp_dir.path(), "tick-config-test");
    let namespace_id = namespace_id();

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
    let namespace_id = namespace_id();
    let raw_store = Arc::new(HeadCasFailureStore::new(
        temp_dir.path(),
        namespace_id.as_str(),
    ));
    let object_store: SharedObjectStore = raw_store.clone();
    let fs = open_runtime(object_store, "tick-race-test");

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
        MaintenanceTickOutcome::WalFlushRaceLost {
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
    let namespace_id = namespace_id();

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
    let namespace_id = namespace_id();

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
    content_blob_puts: AtomicUsize,
}

impl ContentBlobGetCountingStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            content_blob_gets: AtomicUsize::new(0),
            content_blob_puts: AtomicUsize::new(0),
        }
    }

    fn reset_content_blob_counters(&self) {
        self.content_blob_gets.store(0, Ordering::SeqCst);
        self.content_blob_puts.store(0, Ordering::SeqCst);
    }

    fn content_blob_get_count(&self) -> usize {
        self.content_blob_gets.load(Ordering::SeqCst)
    }

    fn content_blob_put_count(&self) -> usize {
        self.content_blob_puts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStore for ContentBlobGetCountingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
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
        if key.starts_with("content-stores/") && key.contains("/blobs/") {
            self.content_blob_puts.fetch_add(1, Ordering::SeqCst);
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
struct FailContentBlobPutsStore {
    inner: LocalFsStore,
    fail_remaining: AtomicUsize,
}

impl FailContentBlobPutsStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            fail_remaining: AtomicUsize::new(0),
        }
    }

    /// Arms the store to fail the next `count` content-blob puts, then
    /// recover.
    fn fail_next_content_blob_puts(&self, count: usize) {
        self.fail_remaining.store(count, Ordering::SeqCst);
    }

    fn should_fail_content_blob_put(&self) -> bool {
        self.fail_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }
}

#[async_trait]
impl ObjectStore for FailContentBlobPutsStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        if key.starts_with("content-stores/")
            && key.contains("/blobs/")
            && self.should_fail_content_blob_put()
        {
            return Err(ObjectStoreError::transport(
                key,
                "injected content write failure",
            ));
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
