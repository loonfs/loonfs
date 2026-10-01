//! Shared fixtures for the crate's integration tests.

#![allow(dead_code)]
#![allow(clippy::panic)]
// Fixture assertions panic for precise diagnostics, as the test modules do.

use loonfs::publish::{CommitCandidate, CommitRequest};
use loonfs::uploads::ResolvedUploadCompletion;
use loonfs::{
    AdvanceRetentionResponse, ChangeSeq, Checkpoint, ChecksumAlgorithm, Commit, ContentRef,
    CopyOptions, CreateCheckpointOptions, CreateDirectoryOptions, CreateNamespaceOptions,
    DeleteOptions, DirectoryPageCursor, ErrorCode, FileBytes, FoldWalResponse, ListChangesOptions,
    ListChangesResponse, LoonFs, LoonFsBuilder, Maintenance, MetadataMaintenanceOptions,
    MetadataMaintenanceResponse, MoveOptions, Namespace, NamespaceDiagnostics, NamespaceId,
    PageRequest, PaginationPolicy, PathEntry, PutFileOptions, ReadOnly, RuntimeError,
    SharedObjectStore, UploadId, UploadSession, Writable,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    FailStore, InjectedError, KeyPredicate, OperationClass, RecordingStore,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// A wall clock a test moves by storing a new time.
#[derive(Debug)]
pub(crate) struct SettableWallClock(pub(crate) AtomicU64);

impl loonfs::WallClock for SettableWallClock {
    fn now_ms(&self) -> Result<u64, loonfs::CoreError> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

/// The GET of a WAL number that finds nothing.
pub(crate) fn wal_probe(
    namespace_id: &NamespaceId,
    wal_no: loonfs_api::WalNo,
) -> loonfs_test_support::stores::RecordedOperation {
    loonfs_test_support::stores::RecordedOperation::Get {
        key: format!("namespaces/{namespace_id}/wal/{:020}.wal.zst", wal_no.0),
        range: None,
        result_bytes: 0,
    }
}

pub(crate) fn assert_wal_probe(
    operations: Vec<loonfs_test_support::stores::RecordedOperation>,
    namespace_id: &NamespaceId,
    wal_no: loonfs_api::WalNo,
) {
    assert_eq!(operations, vec![wal_probe(namespace_id, wal_no)]);
}

pub(crate) fn data_wal_put_for(
    namespace_id: &NamespaceId,
) -> impl Fn(&loonfs_test_support::stores::OperationContext<'_>) -> bool + Send + Sync + 'static {
    let prefix = loonfs_objectstore::keys::wal_prefix(namespace_id);
    move |operation| match operation.kind() {
        loonfs_test_support::stores::OperationKind::Put {
            bytes,
            mode: loonfs_objectstore::PutMode::CreateIfAbsent,
        } if operation.key().starts_with(&prefix) => {
            loonfs_api::wire::wal::decode_wal_object_envelope_zstd(bytes)
                .is_ok_and(|envelope| !envelope.payload().records.is_empty())
        }
        _ => false,
    }
}

pub(crate) fn folded_manifest_put(
    operation: &loonfs_test_support::stores::OperationContext<'_>,
) -> bool {
    match operation.kind() {
        loonfs_test_support::stores::OperationKind::Put {
            bytes,
            mode: loonfs_objectstore::PutMode::CreateIfAbsent,
        } if operation.key().contains("/manifests/") => {
            loonfs_api::wire::manifest::decode_namespace_manifest_json(bytes).is_ok_and(
                |envelope| {
                    envelope.payload().folded_wal_no > loonfs_api::WalNo(0)
                        && !envelope.payload().status.is_deleted()
                },
            )
        }
        _ => false,
    }
}

thread_local! {
    static BLOCKING_RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
}

pub(crate) fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    BLOCKING_RUNTIME.with(|runtime| runtime.block_on(future))
}

pub(crate) fn store(root: &Path) -> SharedObjectStore {
    Arc::new(LocalFsStore::new(root).expect("create local-fs store"))
}

pub(crate) async fn writer(store: SharedObjectStore, writer_id: &str) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store)
        .writer_id(writer_id)
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer")
}

pub(crate) async fn writer_epoch(store: &SharedObjectStore, namespace_id: &NamespaceId) -> u64 {
    loonfs::control::load_namespace_read_state(store, namespace_id)
        .await
        .expect("load namespace head")
        .writer_epoch
        .0
}

pub(crate) fn directory_options() -> CreateDirectoryOptions {
    CreateDirectoryOptions::new(loonfs_test_support::test_actor())
}

pub(crate) fn expect_code<T: std::fmt::Debug>(result: loonfs::Result<T>, code: ErrorCode) {
    let error = result.expect_err("operation must fail");
    assert_eq!(error.code(), code, "unexpected error: {error:?}");
}

pub(crate) async fn collect_path_entries(
    reader: &LoonFs<ReadOnly>,
    namespace_id: &NamespaceId,
    absolute_path: &str,
) -> loonfs::Result<loonfs::ListPathEntriesResponse> {
    let namespace = reader.namespace(namespace_id);
    let request = PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(None)
            .expect("default page limit"),
        cursor: None,
    };
    let mut pager = namespace.list_path_entries_pager(absolute_path, request, Default::default());
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
    maintenance: &Maintenance,
    namespace_id: &NamespaceId,
) -> loonfs::Result<loonfs::ListCheckpointsResponse> {
    let request = PageRequest {
        limit: PaginationPolicy::default()
            .resolve_limit(None)
            .expect("default page limit"),
        cursor: None,
    };
    let mut pager = maintenance.list_checkpoints_pager(namespace_id, request);
    let mut response = pager.next().await.expect("first page")?;
    while let Some(page) = pager.next().await {
        let page = page?;
        response.checkpoints.extend(page.checkpoints);
        response.next_cursor = page.next_cursor;
    }
    Ok(response)
}

/// Metadata maintenance options with an explicit fold threshold.
pub(crate) fn metadata_options(max_wal_tail_objects: u64) -> MetadataMaintenanceOptions {
    MetadataMaintenanceOptions {
        max_wal_tail_objects: std::num::NonZeroU64::new(max_wal_tail_objects)
            .expect("a fold threshold should be nonzero"),
        ..MetadataMaintenanceOptions::default()
    }
}

/// One handle set per test fixture: a writer, its derived reader, and an
/// maintenance handle sharing the same store, exercised through the blocking
/// helpers below.
pub(crate) struct TestRuntime {
    pub(crate) writer: LoonFs<Writable>,
    pub(crate) reader: LoonFs<ReadOnly>,
    pub(crate) maintenance: Maintenance,
    /// The fixture holds one handle per namespace it writes, as a host
    /// would, so its helpers keep publishing through one session.
    namespaces: Mutex<HashMap<NamespaceId, Namespace<Writable>>>,
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
    configure: impl FnOnce(LoonFsBuilder<Writable>) -> LoonFsBuilder<Writable>,
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
    configure: impl FnOnce(LoonFsBuilder<Writable>) -> LoonFsBuilder<Writable>,
) -> TestRuntime {
    let writer = configure(LoonFs::builder_with_store(store.clone()).writer_id(writer_id))
        .build()
        .await
        .expect("build writer");
    let reader = writer.read_only();
    let maintenance = LoonFs::builder_with_store(store)
        .writer_id(writer_id)
        .build()
        .await
        .expect("build maintenance")
        .maintenance(loonfs_test_support::ids::writer_id(writer_id));
    TestRuntime {
        writer,
        reader,
        maintenance,
        namespaces: Mutex::default(),
    }
}

/// Direct async access for tests that drive several operations inside one
/// runtime; everything else goes through the blocking trait below.
impl TestRuntime {
    /// Returns the handle this fixture holds for `namespace_id`, opening it
    /// on first use.
    pub(crate) fn namespace_writer(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<Namespace<Writable>> {
        let mut held = self.namespaces.lock().expect("namespace writer map lock");
        if let Some(namespace) = held.get(namespace_id) {
            return Ok(namespace.clone());
        }
        let namespace = self.writer.open_namespace(namespace_id)?;
        held.insert(namespace_id.clone(), namespace.clone());
        Ok(namespace)
    }

    pub(crate) async fn create_namespace(
        &self,
        namespace_id: &NamespaceId,
        options: CreateNamespaceOptions,
    ) -> loonfs::Result<loonfs_api::NamespaceMetadata> {
        self.writer.create_namespace(namespace_id, options).await
    }

    pub(crate) async fn put_file_bytes(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> loonfs::Result<Commit> {
        let namespace = self.namespace_writer(namespace_id)?;
        namespace
            .put_file_bytes(absolute_path, bytes, options)
            .await
    }

    pub(crate) async fn put_file_content_ref(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        content_ref: ContentRef,
        options: PutFileOptions,
    ) -> loonfs::Result<Commit> {
        let namespace = self.namespace_writer(namespace_id)?;
        namespace
            .put_file_content_ref(absolute_path, content_ref, options)
            .await
    }

    pub(crate) async fn get_path_entry(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<PathEntry> {
        let namespace = self.reader.namespace(namespace_id);
        namespace
            .get_path_entry(absolute_path, Default::default())
            .await
    }

    pub(crate) async fn list_path(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<Vec<PathEntry>> {
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
        let namespace = self.reader.namespace(namespace_id);
        namespace
            .list_path_entries_page(absolute_path, request, Default::default())
            .await
    }

    pub(crate) async fn list_inode_children_page(
        &self,
        namespace_id: &NamespaceId,
        inode_id: loonfs::InodeId,
        request: PageRequest<DirectoryPageCursor>,
    ) -> loonfs::Result<loonfs::ListInodeChildrenResponse> {
        let namespace = self.reader.namespace(namespace_id);
        namespace
            .list_inode_children_page(inode_id, request, Default::default())
            .await
    }

    pub(crate) async fn list_file_revisions_page(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        request: PageRequest<loonfs::FileRevisionsPageCursor>,
    ) -> loonfs::Result<loonfs::ListFileRevisionsResponse> {
        let namespace = self.reader.namespace(namespace_id);
        namespace
            .list_file_revisions_page(absolute_path, request)
            .await
    }

    pub(crate) async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<Checkpoint> {
        self.maintenance
            .create_checkpoint(
                namespace_id,
                CreateCheckpointOptions {
                    name: "test-pin".to_owned(),
                    ttl_ms: None,
                },
            )
            .await
    }

    pub(crate) async fn create_direct_put_upload_target(
        &self,
        namespace_id: &NamespaceId,
        checksum_algorithm: ChecksumAlgorithm,
    ) -> loonfs::Result<loonfs::uploads::BeginDirectPutUploadTargetResponse> {
        let namespace = self.namespace_writer(namespace_id)?;
        namespace
            .create_direct_put_upload_target(checksum_algorithm)
            .await
    }

    pub(crate) async fn complete_direct_put(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        content: loonfs::UploadContentClaim,
    ) -> loonfs::Result<UploadSession> {
        let namespace = self.namespace_writer(namespace_id)?;
        namespace
            .complete_upload_for_mode(upload_id, |_| {
                Ok(loonfs::uploads::ResolvedUploadCompletion::DirectPut { content })
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
    ) -> loonfs::Result<loonfs_api::NamespaceMetadata>;
    fn fork_namespace_blocking(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> loonfs::Result<loonfs_api::NamespaceMetadata>;
    fn namespace_diagnostics_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<NamespaceDiagnostics>;
    fn maintain_metadata_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: MetadataMaintenanceOptions,
    ) -> loonfs::Result<MetadataMaintenanceResponse>;
    fn fold_wal_blocking(&self, namespace_id: &NamespaceId) -> loonfs::Result<FoldWalResponse>;
    fn stat_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<PathEntry>;
    fn list_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<Vec<PathEntry>>;
    fn get_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<FileBytes>;
    fn put_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> loonfs::Result<Commit>;
    fn create_directory_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> loonfs::Result<Commit>;
    fn delete_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> loonfs::Result<Commit>;
    fn move_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        source_path: &str,
        destination_path: &str,
        options: MoveOptions,
    ) -> loonfs::Result<Commit>;
    fn copy_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        source_path: &str,
        destination_path: &str,
        options: CopyOptions,
    ) -> loonfs::Result<Commit>;
    fn begin_upload_blocking(&self, namespace_id: &NamespaceId) -> loonfs::Result<UploadSession>;
    fn upload_content_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> loonfs::Result<UploadSession>;
    fn complete_upload_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> loonfs::Result<UploadSession>;
    fn mutate_blocking(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> loonfs::Result<Commit>;
    fn mutate_batch_blocking(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<loonfs::Result<Commit>>;
    fn list_changes_blocking(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> loonfs::Result<ListChangesResponse>;
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
    ) -> loonfs::Result<loonfs_api::NamespaceMetadata> {
        block_on(self.writer.create_namespace(namespace_id, options))
    }

    fn fork_namespace_blocking(
        &self,
        source: &NamespaceId,
        target: &NamespaceId,
    ) -> loonfs::Result<loonfs_api::NamespaceMetadata> {
        block_on(self.writer.fork_namespace(
            source,
            target,
            loonfs_api::options::ForkNamespaceOptions::new(loonfs_test_support::test_actor()),
        ))
    }

    fn namespace_diagnostics_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<NamespaceDiagnostics> {
        block_on(self.maintenance.get_namespace_diagnostics(namespace_id))
    }

    fn maintain_metadata_blocking(
        &self,
        namespace_id: &NamespaceId,
        options: MetadataMaintenanceOptions,
    ) -> loonfs::Result<MetadataMaintenanceResponse> {
        block_on(self.maintenance.maintain_metadata(namespace_id, options))
    }

    fn fold_wal_blocking(&self, namespace_id: &NamespaceId) -> loonfs::Result<FoldWalResponse> {
        block_on(self.maintenance.fold_wal(namespace_id))
    }

    fn stat_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<PathEntry> {
        let namespace = self.reader.namespace(namespace_id);
        block_on(namespace.get_path_entry(absolute_path, Default::default()))
    }

    fn list_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
    ) -> loonfs::Result<Vec<PathEntry>> {
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
    ) -> loonfs::Result<FileBytes> {
        let namespace = self.reader.namespace(namespace_id);
        block_on(namespace.get_file_bytes(absolute_path))
    }

    fn put_file_bytes_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        options: PutFileOptions,
    ) -> loonfs::Result<Commit> {
        let namespace = self.namespace_writer(namespace_id)?;
        block_on(namespace.put_file_bytes(absolute_path, bytes, options))
    }

    fn create_directory_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: CreateDirectoryOptions,
    ) -> loonfs::Result<Commit> {
        let namespace = self.namespace_writer(namespace_id)?;
        block_on(namespace.create_directory(absolute_path, options))
    }

    fn delete_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        options: DeleteOptions,
    ) -> loonfs::Result<Commit> {
        let namespace = self.namespace_writer(namespace_id)?;
        block_on(namespace.delete_path(absolute_path, options))
    }

    fn move_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        source_path: &str,
        destination_path: &str,
        options: MoveOptions,
    ) -> loonfs::Result<Commit> {
        let namespace = self.namespace_writer(namespace_id)?;
        block_on(namespace.move_path(source_path, destination_path, options))
    }

    fn copy_path_blocking(
        &self,
        namespace_id: &NamespaceId,
        source_path: &str,
        destination_path: &str,
        options: CopyOptions,
    ) -> loonfs::Result<Commit> {
        let namespace = self.namespace_writer(namespace_id)?;
        block_on(namespace.copy_path(source_path, destination_path, options))
    }

    fn begin_upload_blocking(&self, namespace_id: &NamespaceId) -> loonfs::Result<UploadSession> {
        let namespace = self.namespace_writer(namespace_id)?;
        block_on(namespace.create_upload())
    }

    fn upload_content_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> loonfs::Result<UploadSession> {
        let namespace = self.namespace_writer(namespace_id)?;
        block_on(namespace.put_upload_content(upload_id, bytes))
    }

    fn complete_upload_blocking(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> loonfs::Result<UploadSession> {
        let namespace = self.namespace_writer(namespace_id)?;
        block_on(namespace.complete_upload(upload_id, ResolvedUploadCompletion::KnownContent))
            .map(|completed| completed.response)
    }

    fn mutate_blocking(
        &self,
        namespace_id: &NamespaceId,
        request: CommitRequest,
    ) -> loonfs::Result<Commit> {
        let namespace = self.namespace_writer(namespace_id)?;
        block_on(namespace.create_commit(request))
    }

    fn mutate_batch_blocking(
        &self,
        namespace_id: &NamespaceId,
        requests: Vec<CommitRequest>,
    ) -> Vec<loonfs::Result<Commit>> {
        let namespace = match self.namespace_writer(namespace_id) {
            Ok(namespace) => namespace,
            Err(error) => return requests.iter().map(|_| Err(error.clone())).collect(),
        };
        block_on(async move {
            // Admitted in one pass, before the publisher's worker can take
            // any of them, so the requests coalesce into one publication.
            let submissions = requests
                .into_iter()
                .map(|request| namespace.commit_candidate(CommitCandidate::new(request)));
            futures::future::join_all(submissions).await
        })
    }

    fn list_changes_blocking(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
    ) -> loonfs::Result<ListChangesResponse> {
        let namespace = self.reader.namespace(namespace_id);
        block_on(namespace.list_changes_page(after_seq, ListChangesOptions::default()))
    }

    fn create_checkpoint_blocking(&self, namespace_id: &NamespaceId) -> loonfs::Result<Checkpoint> {
        block_on(self.create_checkpoint(namespace_id))
    }

    fn advance_retention_floor_blocking(
        &self,
        namespace_id: &NamespaceId,
    ) -> loonfs::Result<AdvanceRetentionResponse> {
        block_on(self.maintenance.advance_retention_floor(namespace_id))
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
    pub(crate) fail_wal_publish: Arc<FailStore<SharedObjectStore>>,
    pub(crate) wal_gets: Arc<RecordingStore<SharedObjectStore>>,
    pub(crate) manifest_gets: Arc<RecordingStore<SharedObjectStore>>,
    pub(crate) hint_gets: Arc<RecordingStore<SharedObjectStore>>,
}

impl RuntimeStoreProbe {
    pub(crate) fn new(root: &Path, namespace_id: &NamespaceId) -> Self {
        let inner: SharedObjectStore =
            Arc::new(LocalFsStore::new(root).expect("create local-fs store"));
        let wal_gets = Arc::new(RecordingStore::new(
            inner,
            KeyPredicate::prefix(format!("namespaces/{namespace_id}/wal/")),
        ));
        let manifest_gets = Arc::new(RecordingStore::new(
            wal_gets.clone() as SharedObjectStore,
            KeyPredicate::prefix(format!("namespaces/{namespace_id}/manifests/")),
        ));
        let hint_gets = Arc::new(RecordingStore::new(
            manifest_gets.clone() as SharedObjectStore,
            KeyPredicate::hint(namespace_id),
        ));
        let fail_wal_publish = Arc::new(FailStore::new(
            hint_gets.clone() as SharedObjectStore,
            KeyPredicate::prefix(loonfs_objectstore::keys::wal_prefix(namespace_id)),
            OperationClass::PutCreateIfAbsent,
            InjectedError::PreconditionFailed,
        ));
        Self {
            store: fail_wal_publish.clone(),
            fail_wal_publish,
            wal_gets,
            manifest_gets,
            hint_gets,
        }
    }

    pub(crate) fn store(&self) -> SharedObjectStore {
        self.store.clone()
    }

    pub(crate) fn fail_wal_publish(&self) {
        self.fail_wal_publish.fail_all();
    }

    pub(crate) fn allow_wal_publish(&self) {
        self.fail_wal_publish.clear();
    }

    pub(crate) fn reset_wal_get_count(&self) {
        self.wal_gets.reset();
    }

    pub(crate) fn reset_control_get_counts(&self) {
        self.manifest_gets.reset();
        self.hint_gets.reset();
        self.reset_wal_get_count();
    }

    pub(crate) fn wal_get_count(&self) -> usize {
        self.wal_gets.count(OperationClass::Read)
    }

    pub(crate) fn manifest_get_count(&self) -> usize {
        self.manifest_gets.count(OperationClass::Read)
    }

    pub(crate) fn hint_get_count(&self) -> usize {
        self.hint_gets.count(OperationClass::Read)
    }
}
