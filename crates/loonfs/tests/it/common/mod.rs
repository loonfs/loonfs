//! Shared fixtures for the crate's integration tests.

#![allow(dead_code)]
#![allow(clippy::panic)]
// Fixture assertions panic for precise diagnostics, as the test modules do.

use loonfs::publish::{CommitCandidate, CommitRequest};
use loonfs::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, BeginUploadRequest,
    BeginUploadResponse, ChangeSeq, ChangesResponse, Checkpoint, ChecksumAlgorithm, CommitResponse,
    ContentRef, CopyOptions, CreateCheckpointOptions, CreateDirectoryOptions,
    CreateNamespaceOptions, DeleteOptions, DirectoryPageCursor, ErrorCode, FsAdmin, FsReader,
    FsWriter, FsWriterBuilder, ListChangesOptions, MaintenancePlan, MaintenanceStepResponse,
    MetadataMaintenanceResponse, MoveOptions, NamespaceDiagnostics, NamespaceId, PageRequest,
    PaginationPolicy, PutFileOptions, RuntimeError, SharedObjectStore, UploadContentResponse,
    UploadId, UploadSessionResponse,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::block_on::block_on;
use loonfs_test_support::stores::{
    CountingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use std::path::Path;
use std::sync::Arc;

pub(crate) fn store(root: &Path) -> SharedObjectStore {
    Arc::new(LocalFsStore::new(root).expect("create local-fs store"))
}

pub(crate) async fn collect_path_entries(
    reader: &FsReader,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> loonfs::Result<loonfs::ListPathEntriesResponse> {
    let request = PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(None)
            .expect("default page limit"),
        cursor: None,
    };
    let mut pager =
        reader.list_path_entries_pager(namespace_id, absolute_path, request, Default::default());
    let mut response = pager.next().await.expect("first page")?;
    while let Some(page) = pager.next().await {
        let page = page?;
        response.head_seq = page.head_seq;
        response.entries.extend(page.entries);
        response.next_cursor = page.next_cursor;
    }
    Ok(response)
}

pub(crate) async fn collect_checkpoints(
    admin: &FsAdmin,
    namespace_id: &NamespaceId,
) -> loonfs::Result<loonfs::ListCheckpointsResponse> {
    let request = PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(None)
            .expect("default page limit"),
        cursor: None,
    };
    let mut pager = admin.list_checkpoints_pager(namespace_id, request);
    let mut response = pager.next().await.expect("first page")?;
    while let Some(page) = pager.next().await {
        let page = page?;
        response.checkpoints.extend(page.checkpoints);
        response.next_cursor = page.next_cursor;
    }
    Ok(response)
}

/// The upkeep report a step that selected metadata is obliged to carry.
pub(crate) fn upkeep(step: &MaintenanceStepResponse) -> &MetadataMaintenanceResponse {
    step.metadata
        .as_ref()
        .expect("a plan selecting metadata upkeep reports it")
}

/// A plan selecting metadata upkeep alone, at an explicit flush threshold.
pub(crate) fn metadata_plan(max_wal_tail_segments: u64) -> MaintenancePlan {
    MaintenancePlan {
        metadata: Some(loonfs::MetadataMaintenanceOptions {
            max_wal_tail_segments: std::num::NonZeroU64::new(max_wal_tail_segments)
                .expect("a flush threshold is non-zero"),
        }),
        ..MaintenancePlan::default()
    }
}

/// One handle set per test fixture: a writer, its derived reader, and an
/// admin handle sharing the same store, exercised through the blocking
/// helpers below.
pub(crate) struct TestRuntime {
    pub(crate) writer: FsWriter,
    pub(crate) reader: FsReader,
    pub(crate) admin: FsAdmin,
}

pub(crate) fn runtime(root: &Path, writer_id: &str) -> TestRuntime {
    open_runtime(store(root), writer_id)
}

pub(crate) fn open_runtime(store: SharedObjectStore, writer_id: &str) -> TestRuntime {
    open_runtime_with(store, writer_id, |builder| builder)
}

pub(crate) fn open_runtime_with(
    store: SharedObjectStore,
    writer_id: &str,
    configure: impl FnOnce(FsWriterBuilder) -> FsWriterBuilder,
) -> TestRuntime {
    block_on(open_runtime_with_async(store, writer_id, configure))
}

/// Async-test variant: opens the fixture inside the test's own runtime.
pub(crate) async fn open_runtime_async(store: SharedObjectStore, writer_id: &str) -> TestRuntime {
    open_runtime_with_async(store, writer_id, |builder| builder).await
}

pub(crate) async fn open_runtime_with_async(
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
    pub(crate) async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> loonfs::Result<loonfs::Namespace> {
        self.writer.create_namespace(namespace_id, options).await
    }

    pub(crate) async fn put_file_bytes(
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

    pub(crate) async fn put_file_content_ref(
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

    pub(crate) async fn stat_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<AuthoritativePathEntry> {
        self.reader
            .stat_path(namespace_id, absolute_path, Default::default())
            .await
    }

    pub(crate) async fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<Vec<AuthoritativePathEntry>> {
        Ok(
            collect_path_entries(&self.reader, namespace_id, absolute_path)
                .await?
                .entries,
        )
    }

    pub(crate) async fn list_path_entries(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<loonfs::ListPathEntriesResponse> {
        collect_path_entries(&self.reader, namespace_id, absolute_path).await
    }

    pub(crate) async fn list_path_entries_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<DirectoryPageCursor>,
    ) -> loonfs::Result<loonfs::ListPathEntriesResponse> {
        self.reader
            .list_path_entries_page(namespace_id, absolute_path, request, Default::default())
            .await
    }

    pub(crate) async fn list_file_revisions_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<loonfs::FileRevisionsPageCursor>,
    ) -> loonfs::Result<loonfs::ListFileRevisionsResponse> {
        self.reader
            .list_file_revisions_page(namespace_id, absolute_path, request)
            .await
    }

    pub(crate) async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<Checkpoint> {
        self.admin
            .create_checkpoint(
                namespace_id,
                CreateCheckpointOptions {
                    name: "test-pin".to_owned(),
                    ttl_ms: None,
                },
            )
            .await
            .map(|response| response.checkpoint)
    }

    pub(crate) async fn begin_direct_put_upload_target(
        &self,
        namespace_id: &NamespaceId,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> loonfs::Result<loonfs::uploads::BeginDirectPutUploadTargetResponse> {
        self.writer
            .begin_direct_put_upload_target(namespace_id, checksum_algorithm)
            .await
    }

    pub(crate) async fn complete_direct_put(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        content: loonfs::UploadContentClaim,
    ) -> loonfs::Result<UploadSessionResponse> {
        self.writer
            .complete_upload_prepared_for_mode(namespace_id, upload_id, |_| {
                Ok(loonfs::uploads::ResolvedUploadCompletion::DirectPut {
                    content,
                    max_content_bytes: u64::MAX,
                })
            })
            .await
            .map(|completed| completed.response)
    }

    pub(crate) fn runtime_cache_stats(&self) -> loonfs::RuntimeCacheStats {
        self.writer.runtime_cache_stats()
    }
}

pub(crate) fn decode_directory_page_cursor(value: &str) -> DirectoryPageCursor {
    loonfs_api::decode_cursor(value).expect("decode directory cursor")
}

pub(crate) fn decode_file_revisions_page_cursor(value: &str) -> loonfs::FileRevisionsPageCursor {
    loonfs_api::decode_cursor(value).expect("decode file revisions cursor")
}

pub(crate) trait RuntimeTestExt {
    fn create_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> loonfs::Result<loonfs::Namespace>;
    fn fork_namespace_blocking(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> loonfs::Result<loonfs::Namespace>;
    fn namespace_diagnostics_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<NamespaceDiagnostics>;
    fn maintenance_step_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        plan: MaintenancePlan,
    ) -> loonfs::Result<MaintenanceStepResponse>;
    fn flush_wal_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<MetadataMaintenanceResponse>;
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
    fn get_file_bytes_blocking(
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
    ) -> loonfs::Result<UploadSessionResponse>;
    fn mutate_blocking(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> loonfs::Result<CommitResponse>;
    fn mutate_batch_blocking(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<loonfs::Result<CommitResponse>>;
    fn list_changes_blocking(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> loonfs::Result<ChangesResponse>;
    fn create_checkpoint_blocking(&self, namespace_id: &NamespaceId) -> loonfs::Result<Checkpoint>;
    fn advance_retention_floor_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<AdvanceRetentionResponse>;
}

impl RuntimeTestExt for TestRuntime {
    fn create_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> loonfs::Result<loonfs::Namespace> {
        block_on(self.writer.create_namespace(namespace_id, options))
    }

    fn fork_namespace_blocking(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> loonfs::Result<loonfs::Namespace> {
        block_on(self.writer.fork_namespace(source, target))
    }

    fn namespace_diagnostics_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<NamespaceDiagnostics> {
        block_on(self.admin.get_namespace_diagnostics(namespace_id))
    }

    fn maintenance_step_namespace_blocking(
        &self,
        namespace_id: &NamespaceId,
        plan: MaintenancePlan,
    ) -> loonfs::Result<MaintenanceStepResponse> {
        block_on(self.admin.maintenance_step_namespace(namespace_id, plan))
    }

    fn flush_wal_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<MetadataMaintenanceResponse> {
        block_on(self.admin.flush_wal(namespace_id))
    }

    fn stat_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<AuthoritativePathEntry> {
        block_on(
            self.reader
                .stat_path(namespace_id, absolute_path, Default::default()),
        )
    }

    fn list_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<Vec<AuthoritativePathEntry>> {
        block_on(collect_path_entries(
            &self.reader,
            namespace_id,
            absolute_path,
        ))
        .map(|response| response.entries)
    }

    fn get_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<AuthoritativeFileBytes> {
        block_on(self.reader.get_file_bytes(namespace_id, absolute_path))
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
                .begin_upload(namespace_id, BeginUploadRequest::ServiceProxied {}),
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
    ) -> loonfs::Result<UploadSessionResponse> {
        block_on(self.writer.complete_upload(namespace_id, upload_id))
    }

    fn mutate_blocking(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> loonfs::Result<CommitResponse> {
        block_on(self.writer.commit(namespace_id, request))
    }

    fn mutate_batch_blocking(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<loonfs::Result<CommitResponse>> {
        let publisher = self.writer.publisher();
        block_on(async move {
            // Admitted in one pass, before the publisher's worker can take
            // any of them, so the requests coalesce into one publication.
            let submissions = requests.into_iter().map(|request| {
                publisher.submit_candidate(namespace_id.clone(), CommitCandidate::new(request))
            });
            futures::future::join_all(submissions).await
        })
    }

    fn list_changes_blocking(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> loonfs::Result<ChangesResponse> {
        block_on(
            self.reader
                .list_changes(namespace_id, after_seq, ListChangesOptions::default()),
        )
    }

    fn create_checkpoint_blocking(&self, namespace_id: &NamespaceId) -> loonfs::Result<Checkpoint> {
        block_on(self.create_checkpoint(namespace_id))
    }

    fn advance_retention_floor_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<AdvanceRetentionResponse> {
        block_on(self.admin.advance_retention_floor(namespace_id))
    }
}

pub(crate) fn assert_core_error_kind<T>(result: loonfs::Result<T>, expected: ErrorCode) {
    match result {
        Err(RuntimeError::Core(error)) => assert_eq!(error.code(), expected),
        Err(error) => panic!("expected core error {expected:?}, got {error:?}"),
        Ok(_) => panic!("expected core error {expected:?}"),
    }
}

#[derive(Debug)]
pub(crate) struct RuntimeStoreProbe {
    pub(crate) store: SharedObjectStore,
    pub(crate) fail_head_cas: Arc<FailStore<SharedObjectStore>>,
    pub(crate) fail_root_cas: Arc<FailStore<SharedObjectStore>>,
    pub(crate) wal_gets: Arc<CountingStore<SharedObjectStore>>,
    pub(crate) manifest_gets: Arc<CountingStore<SharedObjectStore>>,
    pub(crate) head_gets: Arc<CountingStore<SharedObjectStore>>,
}

impl RuntimeStoreProbe {
    pub(crate) fn new(root: &Path, namespace_id: &NamespaceId) -> Self {
        let inner: SharedObjectStore =
            Arc::new(LocalFsStore::new(root).expect("create local-fs store"));
        let wal_gets = Arc::new(CountingStore::new(
            inner,
            KeyPredicate::prefix(format!("namespaces/{namespace_id}/wal/segments/")),
        ));
        let manifest_gets = Arc::new(CountingStore::new(
            wal_gets.clone() as SharedObjectStore,
            KeyPredicate::prefix(format!("namespaces/{namespace_id}/metadata/manifests/")),
        ));
        let head_gets = Arc::new(CountingStore::new(
            manifest_gets.clone() as SharedObjectStore,
            KeyPredicate::wal_head(namespace_id),
        ));
        let fail_head_cas = Arc::new(FailStore::new(
            head_gets.clone() as SharedObjectStore,
            KeyPredicate::wal_head(namespace_id),
            OperationClass::CompareAndSwap,
            InjectedError::PreconditionFailed,
        ));
        // Any conditional publication of the root: create-if-absent for a
        // namespace that has never flushed, compare-and-swap after that.
        let fail_root_cas = Arc::new(FailStore::new(
            fail_head_cas.clone() as SharedObjectStore,
            KeyPredicate::metadata_root(namespace_id),
            OperationClass::Put,
            InjectedError::PreconditionFailed,
        ));
        Self {
            store: fail_root_cas.clone(),
            fail_head_cas,
            fail_root_cas,
            wal_gets,
            manifest_gets,
            head_gets,
        }
    }

    pub(crate) fn store(&self) -> SharedObjectStore {
        self.store.clone()
    }

    pub(crate) fn fail_head_cas(&self) {
        self.fail_head_cas.fail_all();
    }

    pub(crate) fn allow_head_cas(&self) {
        self.fail_head_cas.clear();
    }

    pub(crate) fn fail_root_cas(&self) {
        self.fail_root_cas.fail_all();
    }

    pub(crate) fn reset_wal_get_count(&self) {
        self.wal_gets.reset();
    }

    pub(crate) fn reset_control_get_counts(&self) {
        self.manifest_gets.reset();
        self.head_gets.reset();
        self.reset_wal_get_count();
    }

    pub(crate) fn wal_get_count(&self) -> usize {
        self.wal_gets.count(OperationClass::Read)
    }

    pub(crate) fn manifest_get_count(&self) -> usize {
        self.manifest_gets.count(OperationClass::Read)
    }

    pub(crate) fn head_get_count(&self) -> usize {
        self.head_gets.count(OperationClass::Read)
    }
}
