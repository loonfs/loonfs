//! Content-object request accounting across staging and publication.

use bytes::Bytes;
use loonfs::content_tokens::ContentTokenError;
use loonfs::publish::{
    parse_mutation_path, CommitCandidate, CommitRequest, ContentPreparationError,
    FilesystemOperation, PreparedContent,
};
use loonfs::uploads::ResolvedUploadCompletion;
use loonfs::{
    CommitId, CoreError, DestinationBehavior, Error, ErrorCode, LoonFs, NamespaceId,
    PutFileOptions, RevisionNo, SharedObjectStore, Writable, CONTENT_READ_CHUNK_BYTES,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    BlockingStore, FailStore, InjectedError, KeyPredicate, RecordingStore,
};
use loonfs_types::ContentId;
use std::path::Path;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

#[derive(Debug, Clone, Copy)]
enum KeyClass {
    Content = 0,
    Other = 1,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct OperationCounts {
    head: usize,
    get: usize,
    put: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RequestCounts {
    by_class: [OperationCounts; 2],
    content_get_bytes: usize,
}

impl RequestCounts {
    fn operations(self, class: KeyClass) -> OperationCounts {
        self.by_class[class as usize]
    }
}

#[derive(Debug, Clone)]
struct RequestLog {
    store: SharedObjectStore,
    content: Arc<RecordingStore<SharedObjectStore>>,
    other: Arc<RecordingStore<SharedObjectStore>>,
}

impl RequestLog {
    fn new(root: &Path) -> Self {
        let inner: SharedObjectStore =
            Arc::new(LocalFsStore::new(root).expect("create local-fs store"));
        let other = Arc::new(RecordingStore::new(
            inner,
            KeyPredicate::new(|key| !is_content_key(key)),
        ));
        let content = Arc::new(RecordingStore::new(
            other.clone() as SharedObjectStore,
            KeyPredicate::content_blob(),
        ));
        Self {
            store: content.clone(),
            content,
            other,
        }
    }

    fn store(&self) -> SharedObjectStore {
        self.store.clone()
    }

    fn snapshot(&self) -> RequestCounts {
        let content = self.content.counts();
        let other = self.other.counts();
        let mut counts = RequestCounts::default();
        counts.by_class[KeyClass::Content as usize] = OperationCounts {
            head: content.heads,
            get: content.gets + content.gets_with_metadata,
            put: content.puts,
        };
        counts.by_class[KeyClass::Other as usize] = OperationCounts {
            head: other.heads,
            get: other.gets + other.gets_with_metadata,
            put: other.puts,
        };
        counts.content_get_bytes =
            usize::try_from(content.read_bytes).expect("test byte count should fit usize");
        counts
    }

    fn reset(&self) {
        self.content.reset();
        self.other.reset();
    }
}

struct TestHarness {
    _temp_dir: TempDir,
    recording: RequestLog,
    store: SharedObjectStore,
    namespace_id: NamespaceId,
    writer: LoonFs<Writable>,
}

impl TestHarness {
    async fn new(namespace: &str) -> Self {
        let temp_dir = tempdir().expect("tempdir");
        let recording = RequestLog::new(temp_dir.path());
        let store = recording.store();
        let namespace_id = NamespaceId::parse(namespace).expect("valid namespace id");
        let writer =
            build_initialized_writer(store.clone(), &namespace_id, "accounting-writer").await;
        Self {
            _temp_dir: temp_dir,
            recording,
            store,
            namespace_id,
            writer,
        }
    }

    async fn stage_content(&self, bytes: &[u8]) -> loonfs::ContentRef {
        loonfs_core::content::store_bytes_as_content(&self.store, &self.namespace_id, bytes)
            .await
            .expect("stage content")
            .into_content_ref()
    }
}

async fn build_initialized_writer(
    store: SharedObjectStore,
    namespace_id: &NamespaceId,
    writer_id: &str,
) -> LoonFs<Writable> {
    let writer = LoonFs::builder_with_store(store)
        .writer_id(writer_id)
        .inline_content(loonfs::InlineContentPolicy {
            inline_content_threshold_bytes: None,
            ..Default::default()
        })
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    writer
        .create_namespace(namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    let namespace = writer.open_namespace(namespace_id).expect("open namespace");
    namespace
        .create_directory("/catalog-warmup", &loonfs_test_support::test_actor())
        .await
        .expect("warm namespace catalog");
    writer
}

/// Full external preparation performs one content HEAD and one full GET.
async fn prepare_content(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    content_ref: &loonfs::ContentRef,
) -> PreparedContent {
    let catalog = loonfs_core::control::load_namespace_catalog_entry(store, namespace_id)
        .await
        .expect("load namespace catalog");
    loonfs_core::content::prepare_existing_content_ref(store, &catalog, content_ref.clone())
        .await
        .expect("prepare existing content")
}

fn put_request(commit_id: &str, path: &str, content_ref: loonfs::ContentRef) -> CommitRequest {
    CommitRequest::single(
        CommitId::parse(commit_id).expect("valid commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::PutFile {
            path: parse_mutation_path(path).expect("valid mutation path"),
            content_ref: Some(content_ref),
            inline_content: None,
            behavior: DestinationBehavior::NoReplace,
            expected_inode_id: None,
            expected_revision_no: None,
        },
    )
}

fn assert_content_counts(
    counts: RequestCounts,
    head: usize,
    get: usize,
    put: usize,
    bytes_read: usize,
) {
    assert_eq!(
        counts.operations(KeyClass::Content),
        OperationCounts { head, get, put }
    );
    assert_eq!(counts.content_get_bytes, bytes_read);
}

fn assert_content_not_prepared(error: impl Into<Error>, content_ref: &loonfs::ContentRef) {
    let error = error.into();
    assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
    assert!(
        matches!(
            error,
            Error::Core(CoreError::ContentPreparation(
                ContentPreparationError::ContentNotPrepared { ref content_id }
            )) if content_id == &content_ref.content_id
        ),
        "content-not-prepared error should carry the rejected digest"
    );
}

#[tokio::test]
async fn put_file_content_ref_validates_content_before_publication() {
    let harness = TestHarness::new("content-ref-validation").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let bytes = b"pre-staged content";
    let content_ref = harness.stage_content(bytes).await;
    harness.recording.reset();

    // The convenience method should validate the referenced content exactly
    // once before publishing it.
    namespace
        .put_file_content_ref("/file.txt", content_ref, &loonfs_test_support::test_actor())
        .await
        .expect("publish content ref");

    assert_content_counts(harness.recording.snapshot(), 0, 1, 1, bytes.len());
}

#[tokio::test]
async fn prepare_content_performs_one_content_put_and_no_reads() {
    let harness = TestHarness::new("prepare-file-bytes").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let bytes = b"parallel preparation primitive";
    harness.recording.reset();

    let prepared = namespace
        .prepare_content(bytes)
        .await
        .expect("prepare file bytes");

    let content_ref = prepared.content_ref();
    assert_eq!(content_ref.size_bytes, bytes.len() as u64);
    assert_eq!(
        content_ref.checksum,
        loonfs_types::Checksum::crc64nvme(bytes)
    );
    assert_content_counts(harness.recording.snapshot(), 0, 0, 1, 0);
}

#[tokio::test]
async fn put_file_prepared_performs_no_content_io() {
    let harness = TestHarness::new("put-file-prepared").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let prepared = namespace
        .prepare_content(b"already prepared")
        .await
        .expect("prepare file bytes");
    harness.recording.reset();

    namespace
        .put_file_prepared("/file.txt", prepared, &loonfs_test_support::test_actor())
        .await
        .expect("publish prepared file");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn prepared_content_for_another_store_is_rejected_without_content_io() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = RequestLog::new(temp_dir.path());
    let store = recording.store();
    let source = NamespaceId::parse("source-store").expect("source namespace id");
    let target = NamespaceId::parse("target-store").expect("target namespace id");
    let writer = build_initialized_writer(store.clone(), &source, "cross-store-writer").await;
    let source_writer = writer.open_namespace(&source).expect("open namespace");
    writer
        .create_namespace(&target, &loonfs_test_support::test_actor())
        .await
        .expect("create target namespace");
    let target_writer = writer.open_namespace(&target).expect("open namespace");
    target_writer
        .create_directory("/catalog-warmup", &loonfs_test_support::test_actor())
        .await
        .expect("warm target catalog");
    let prepared = source_writer
        .prepare_content(b"source-store-only")
        .await
        .expect("prepare source content");
    let content_ref = prepared.content_ref().clone();
    recording.reset();

    let error = target_writer
        .put_file_prepared("/file.txt", prepared, &loonfs_test_support::test_actor())
        .await
        .expect_err("another store must reject the admission");
    assert!(
        matches!(error, Error::Core(_)),
        "expected core content-preparation error"
    );
    let Error::Core(error) = error else {
        return;
    };

    assert_content_not_prepared(error, &content_ref);
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn independent_namespaces_sharing_a_store_reject_each_others_prepared_content() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = RequestLog::new(temp_dir.path());
    let store = recording.store();
    let source = NamespaceId::parse("shared-source").expect("source namespace id");
    let target = NamespaceId::parse("shared-target").expect("target namespace id");
    let writer = build_initialized_writer(store.clone(), &source, "shared-store-writer").await;
    let source_writer = writer.open_namespace(&source).expect("open namespace");
    writer
        .create_namespace(&target, &loonfs_test_support::test_actor())
        .await
        .expect("create independent target namespace");
    let target_writer = writer.open_namespace(&target).expect("open namespace");

    target_writer
        .create_directory("/catalog-warmup", &loonfs_test_support::test_actor())
        .await
        .expect("warm shared target catalog");
    let prepared = source_writer
        .prepare_content(b"independently shared")
        .await
        .expect("prepare through source namespace");
    recording.reset();

    let content_ref = prepared.content_ref().clone();
    let error = target_writer
        .put_file_prepared("/shared.txt", prepared, &loonfs_test_support::test_actor())
        .await
        .expect_err("shared-store target must reject source proof");

    assert_content_not_prepared(error, &content_ref);
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn fork_and_source_reject_each_others_prepared_content() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = RequestLog::new(temp_dir.path());
    let store = recording.store();
    let source = NamespaceId::parse("fork-source").expect("source namespace id");
    let fork = NamespaceId::parse("fork-target").expect("fork namespace id");
    let writer = build_initialized_writer(store, &source, "fork-sharing-writer").await;
    let source_writer = writer.open_namespace(&source).expect("open namespace");
    let prepared_for_fork = source_writer
        .prepare_content(b"prepared before fork")
        .await
        .expect("prepare through source namespace");
    writer
        .fork_namespace(&source, &fork, &loonfs_test_support::test_actor())
        .await
        .expect("fork namespace");
    let fork_writer = writer.open_namespace(&fork).expect("open namespace");
    fork_writer
        .create_directory("/fork-catalog-warmup", &loonfs_test_support::test_actor())
        .await
        .expect("warm fork catalog");
    recording.reset();

    let source_content_ref = prepared_for_fork.content_ref().clone();
    let error = fork_writer
        .put_file_prepared(
            "/from-source.txt",
            prepared_for_fork,
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect_err("fork must reject source proof");
    assert_content_not_prepared(error, &source_content_ref);
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);

    let prepared_for_source = fork_writer
        .prepare_content(b"prepared through fork")
        .await
        .expect("prepare through fork namespace");
    recording.reset();
    let fork_content_ref = prepared_for_source.content_ref().clone();
    let error = source_writer
        .put_file_prepared(
            "/from-fork.txt",
            prepared_for_source,
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect_err("source must reject fork proof");
    assert_content_not_prepared(error, &fork_content_ref);
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn prepare_content_ref_rejects_bytes_that_do_not_match_the_ref() {
    let harness = TestHarness::new("content-import-mismatch").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let content_ref = harness.stage_content(b"real bytes").await;
    let mut lying_ref = content_ref.clone();
    lying_ref.checksum = loonfs_types::Checksum::crc64nvme(b"other bytes");

    let error = namespace
        .prepare_content_ref(lying_ref)
        .await
        .expect_err("import must reject a ref whose checksum does not match the bytes");
    assert!(
        error.to_string().contains("content checksum mismatch"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn prepare_content_ref_accepts_a_matching_crc64nvme_ref() {
    let harness = TestHarness::new("content-import-crc64nvme").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let bytes = b"direct-uploaded bytes";
    let mut content_ref = harness.stage_content(bytes).await;
    content_ref.checksum = loonfs_types::Checksum::crc64nvme(bytes);

    let prepared = namespace
        .prepare_content_ref(content_ref)
        .await
        .expect("import must validate with the source ref's checksum algorithm");

    assert_eq!(prepared.content_ref().size_bytes, bytes.len() as u64);
    assert_eq!(
        prepared.content_ref().checksum,
        loonfs_types::Checksum::crc64nvme(bytes),
        "the destination may use its own checksum algorithm"
    );
}

#[tokio::test]
async fn prepare_content_ref_reads_large_sources_in_bounded_ranges() {
    let harness = TestHarness::new("content-import-ranges").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let bytes = vec![b'x'; CONTENT_READ_CHUNK_BYTES as usize + 17];
    let content_ref = harness.stage_content(&bytes).await;
    harness.recording.reset();

    let prepared = namespace
        .prepare_content_ref(content_ref)
        .await
        .expect("import a source larger than one read chunk");

    assert_eq!(prepared.content_ref().size_bytes, bytes.len() as u64);
    assert_eq!(
        prepared.content_ref().checksum,
        loonfs_types::Checksum::crc64nvme(&bytes)
    );
    assert_content_counts(harness.recording.snapshot(), 0, 2, 1, bytes.len());
}

#[tokio::test]
async fn prepare_content_ref_reads_once_and_prepared_publication_reads_nothing() {
    let harness = TestHarness::new("prepare-content-ref").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let bytes = b"externally staged content";
    let content_ref = harness.stage_content(bytes).await;
    harness.recording.reset();

    let prepared = namespace
        .prepare_content_ref(content_ref.clone())
        .await
        .expect("prepare content ref");

    assert_ne!(
        prepared.content_ref().content_id,
        content_ref.content_id,
        "import must mint a target-owned content identity"
    );
    assert_eq!(prepared.content_ref().checksum, content_ref.checksum);
    assert_eq!(prepared.content_ref().size_bytes, content_ref.size_bytes);
    assert_content_counts(harness.recording.snapshot(), 0, 1, 1, bytes.len());
    harness.recording.reset();

    namespace
        .put_file_prepared("/file.txt", prepared, &loonfs_test_support::test_actor())
        .await
        .expect("publish prepared content ref");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn proxied_upload_completion_proof_publishes_without_additional_content_io() {
    let harness = TestHarness::new("proxied-completion-proof").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let bytes = b"service proxied upload";
    let begin = namespace.create_upload().await.expect("begin upload");
    harness.recording.reset();

    namespace
        .put_upload_content(&begin.upload_id, bytes)
        .await
        .expect("upload content");
    let upload_counts = harness.recording.snapshot();
    assert_eq!(
        upload_counts.operations(KeyClass::Content),
        OperationCounts {
            head: 0,
            get: 0,
            put: 1,
        }
    );
    harness.recording.reset();

    let completed = namespace
        .complete_upload(&begin.upload_id, ResolvedUploadCompletion::KnownContent)
        .await
        .expect("complete upload with proof");
    let prepared = completed.prepared;
    assert_eq!(
        prepared.content_ref(),
        completed
            .response
            .content_ref()
            .expect("completed content ref")
    );
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
    harness.recording.reset();

    namespace
        .put_file_prepared(
            "/uploaded.txt",
            prepared,
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("publish uploaded content");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn direct_put_completion_avoids_blob_get_and_prepared_publish_uses_no_content_io() {
    let harness = TestHarness::new("direct-completion-proof").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let bytes = b"direct provider upload";
    let begin = namespace
        .create_direct_put_upload_target()
        .await
        .expect("begin direct put");
    harness.recording.reset();

    harness
        .store
        .put_if_absent(&begin.object_key, Bytes::copy_from_slice(bytes))
        .await
        .expect("drive direct provider upload");
    assert_content_counts(harness.recording.snapshot(), 0, 0, 1, 0);
    harness.recording.reset();

    let completed = namespace
        .complete_upload_for_mode(&begin.session.upload_id, |_| {
            Ok(loonfs::uploads::ResolvedUploadCompletion::DirectPut {
                content: loonfs::UploadContentClaim {
                    size_bytes: bytes.len() as u64,
                    checksum: loonfs_types::Checksum::crc64nvme(bytes),
                },
            })
        })
        .await
        .expect("complete direct put with proof");
    let content_ref = completed
        .response
        .content_ref()
        .expect("completed content ref")
        .clone();
    let prepared = completed.prepared;
    assert_eq!(completed.response.content_ref(), Some(&content_ref));
    let completion_counts = harness.recording.snapshot();
    assert_eq!(
        completion_counts.operations(KeyClass::Content),
        OperationCounts {
            head: 1,
            get: 0,
            put: 0,
        }
    );
    assert_eq!(completion_counts.content_get_bytes, 0);
    harness.recording.reset();

    namespace
        .put_file_prepared("/direct.txt", prepared, &loonfs_test_support::test_actor())
        .await
        .expect("publish direct uploaded content");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[derive(Debug, Clone, Copy)]
enum UnpreparedEntryPoint {
    Publisher,
    WriterCreateCommit,
}

#[tokio::test]
async fn an_unprepared_external_ref_fails_typed_without_content_io() {
    for entry_point in [
        UnpreparedEntryPoint::Publisher,
        UnpreparedEntryPoint::WriterCreateCommit,
    ] {
        for behavior in [DestinationBehavior::NoReplace, DestinationBehavior::Replace] {
            let harness = TestHarness::new("unprepared-ref").await;
            let namespace = harness
                .writer
                .open_namespace(&harness.namespace_id)
                .expect("open namespace");
            if behavior == DestinationBehavior::Replace {
                namespace
                    .put_file(
                        "/file.txt",
                        b"first revision",
                        &loonfs_test_support::test_actor(),
                    )
                    .await
                    .expect("seed file");
            }
            let content_ref = harness.stage_content(b"unprepared content").await;
            harness.recording.reset();

            let request = CommitRequest::single(
                CommitId::parse("unprepared-put").expect("valid commit id"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::PutFile {
                    path: parse_mutation_path("/file.txt").expect("valid mutation path"),
                    content_ref: Some(content_ref.clone()),
                    inline_content: None,
                    behavior,
                    expected_inode_id: None,
                    expected_revision_no: None,
                },
            );
            let error: Error = match entry_point {
                UnpreparedEntryPoint::Publisher => namespace
                    .commit_candidate(CommitCandidate::new(request))
                    .await
                    .expect_err("an unprepared ref must not publish"),
                UnpreparedEntryPoint::WriterCreateCommit => namespace
                    .commit(request)
                    .await
                    .expect_err("an unprepared ref must not publish"),
            };

            assert_content_not_prepared(error, &content_ref);
            assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_commit_after_concurrent_preparations_uses_no_publication_content_io() {
    let harness = TestHarness::new("prepared-explicit-many").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let preparations = [
        b"first prepared content" as &'static [u8],
        b"second prepared content",
        b"third prepared content",
    ]
    .into_iter()
    .map(|bytes| {
        let namespace = namespace.clone();
        tokio::spawn(async move { namespace.prepare_content(bytes).await })
    });
    let mut prepared = Vec::new();
    for preparation in preparations {
        prepared.push(
            preparation
                .await
                .expect("preparation task")
                .expect("prepare file bytes"),
        );
    }
    let first = prepared[0].content_ref().clone();
    let second = prepared[1].content_ref().clone();
    let third = prepared[2].content_ref().clone();
    harness.recording.reset();

    // One request, four puts, three distinct refs: two of the puts share a
    // ref, so one proof covers both.
    let put = |path: &str, content_ref: loonfs::ContentRef| FilesystemOperation::PutFile {
        path: parse_mutation_path(path).expect("valid mutation path"),
        content_ref: Some(content_ref),
        inline_content: None,
        behavior: DestinationBehavior::NoReplace,
        expected_inode_id: None,
        expected_revision_no: None,
    };
    namespace
        .commit_prepared(
            CommitRequest {
                preconditions: Vec::new(),
                commit_id: CommitId::parse("prepared-many-puts").expect("valid commit id"),
                actor_id: loonfs_test_support::test_actor(),
                subject: None,
                message: None,
                operations: vec![
                    put("/first.txt", first.clone()),
                    put("/first-copy.txt", first),
                    put("/second.txt", second),
                    put("/third.txt", third),
                ],
            },
            prepared,
        )
        .await
        .expect("publish prepared multi-operation request");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn restore_revision_uses_retained_metadata_without_content_io() {
    let harness = TestHarness::new("restore-retained").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let first = b"first revision";
    namespace
        .put_file("/file.txt", first, &loonfs_test_support::test_actor())
        .await
        .expect("put first revision");
    namespace
        .put_file_with_options(
            "/file.txt",
            b"second revision",
            &loonfs_test_support::test_actor(),
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                commit: loonfs_types::options::CommitOptions {
                    preconditions: Vec::new(),
                    commit_id: None,
                    message: None,
                },
                expected_inode_id: None,
                expected_revision_no: None,
            },
        )
        .await
        .expect("put second revision");
    harness.recording.reset();

    // Restore resolves content from retained namespace metadata, so
    // re-downloading the retained blob would prove nothing.
    namespace
        .commit(CommitRequest::single(
            CommitId::parse("restore-first-revision").expect("valid commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::RestoreRevision {
                path: parse_mutation_path("/file.txt").expect("valid mutation path"),
                source_revision_no: RevisionNo(1),
            },
        ))
        .await
        .expect("publish restore");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn put_file_publishes_without_reading_content() {
    let harness = TestHarness::new("put-bytes-admission").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    harness.recording.reset();

    namespace
        .put_file(
            "/file.txt",
            b"admitted content",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("put admitted bytes");

    // Staging performs the one content PUT. Its acknowledged write admits publication,
    // so neither staging nor publication needs a content HEAD or GET.
    assert_content_counts(harness.recording.snapshot(), 0, 0, 1, 0);
}

#[tokio::test]
async fn commit_id_replay_performs_no_content_operations() {
    let harness = TestHarness::new("receipt-replay").await;
    let content_ref = harness.stage_content(b"replayed content").await;
    let intent = put_request("replayed-put", "/file.txt", content_ref.clone());
    let prepared = prepare_content(&harness.store, &harness.namespace_id, &content_ref).await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let original = namespace
        .commit_candidate(CommitCandidate::prepared(intent.clone(), vec![prepared]))
        .await
        .expect("publish original put");
    harness.recording.reset();

    let replay = namespace
        .commit_candidate(CommitCandidate::new(intent))
        .await
        .expect("replay put");

    assert_eq!(replay, original);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn rejected_preparation_replays_durable_receipt_without_content_operations() {
    let harness = TestHarness::new("rejected-receipt-replay").await;
    let content_ref = harness.stage_content(b"replayed rejected content").await;
    let intent = put_request("rejected-replayed-put", "/file.txt", content_ref.clone());
    let prepared = prepare_content(&harness.store, &harness.namespace_id, &content_ref).await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    let original = namespace
        .commit_candidate(CommitCandidate::prepared(intent.clone(), vec![prepared]))
        .await
        .expect("publish original put");
    harness.recording.reset();

    let replay = namespace
        .commit_candidate(CommitCandidate::rejected(
            intent,
            ContentPreparationError::ContentToken(vec![(
                ContentId::generate(),
                ContentTokenError::Expired,
            )]),
        ))
        .await
        .expect("rejected preparation must replay the durable receipt");

    assert_eq!(replay, original);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn new_rejected_preparation_fails_before_path_planning_without_content_operations() {
    let harness = TestHarness::new("new-rejected-preparation").await;
    let namespace = harness
        .writer
        .open_namespace(&harness.namespace_id)
        .expect("open namespace");
    harness.recording.reset();
    let intent = CommitRequest::single(
        CommitId::parse("new-rejected").expect("valid commit id"),
        loonfs_test_support::test_actor(),
        None,
        FilesystemOperation::CreateDirectory {
            path: parse_mutation_path("/missing/child").expect("valid mutation path"),
            parents: false,
        },
    );

    let error = namespace
        .commit_candidate(CommitCandidate::rejected(
            intent,
            ContentPreparationError::ContentToken(vec![(
                ContentId::generate(),
                ContentTokenError::Expired,
            )]),
        ))
        .await
        .expect_err("new rejected preparation must fail");

    assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
    assert!(matches!(
        error,
        Error::Core(CoreError::ContentPreparation(
            ContentPreparationError::ContentToken(ref rejections)
        ))
            if matches!(rejections[..], [(_, ContentTokenError::Expired)])
    ));
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_duplicate_performs_no_additional_content_operations() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("in-flight-duplicate").expect("valid namespace id");
    let recording = RequestLog::new(temp_dir.path());
    let blocking = Arc::new(BlockingStore::matching(
        recording.store(),
        crate::common::data_wal_put_for(&namespace_id),
    ));
    let store: SharedObjectStore = blocking.clone();
    let writer = build_initialized_writer(store.clone(), &namespace_id, "duplicate-writer").await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let content_ref =
        loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"duplicate content")
            .await
            .expect("stage content")
            .into_content_ref();
    let intent = put_request("in-flight-put", "/file.txt", content_ref.clone());
    // Full preparation deliberately costs one content HEAD and one GET; do it
    // before the reset so this phase isolates publication and duplicate join.
    let prepared = prepare_content(&store, &namespace_id, &content_ref).await;
    recording.reset();

    blocking.block_next();
    let primary = {
        let namespace = namespace.clone();
        let intent = intent.clone();
        let prepared = prepared.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::prepared(intent, vec![prepared]))
                .await
        })
    };
    blocking.wait_until_blocked().await;
    let primary_counts = recording.snapshot();
    assert_content_counts(primary_counts, 0, 0, 0, 0);

    let mut duplicate =
        Box::pin(namespace.commit_candidate(CommitCandidate::prepared(intent, vec![prepared])));
    assert!(
        futures::poll!(duplicate.as_mut()).is_pending(),
        "the duplicate must join the in-flight primary"
    );
    blocking.release();

    let duplicate_receipt = duplicate.await.expect("duplicate receipt");
    let primary_receipt = primary
        .await
        .expect("primary task")
        .expect("primary receipt");
    assert_eq!(duplicate_receipt, primary_receipt);
    let completed_counts = recording.snapshot();
    assert_eq!(
        completed_counts.operations(KeyClass::Content),
        primary_counts.operations(KeyClass::Content)
    );
    assert_eq!(
        completed_counts.content_get_bytes,
        primary_counts.content_get_bytes
    );
}

#[tokio::test]
async fn stale_head_retry_preserves_content_admission() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("stale-head-retry").expect("valid namespace id");
    let recording = RequestLog::new(temp_dir.path());
    let conflicting = Arc::new(FailStore::matching(
        recording.store(),
        crate::common::data_wal_put_for(&namespace_id),
        InjectedError::PreconditionFailed,
    ));
    let store: SharedObjectStore = conflicting.clone();
    let writer = build_initialized_writer(store.clone(), &namespace_id, "retry-writer").await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let content_ref =
        loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"retry content")
            .await
            .expect("stage content")
            .into_content_ref();
    let intent = put_request("retry-put", "/file.txt", content_ref.clone());
    // Full preparation deliberately costs one content HEAD and one GET; do it
    // before the reset so this phase isolates publication retries.
    let prepared = prepare_content(&store, &namespace_id, &content_ref).await;
    recording.reset();
    conflicting.fail_next(1);

    namespace
        .commit_candidate(CommitCandidate::prepared(intent, vec![prepared]))
        .await
        .expect("publish after stale-head retry");

    assert_eq!(conflicting.attempts(), 2);
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_batch_publishes_admitted_put_and_rejects_unprepared_put_without_content_io() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("mixed-preparation").expect("valid namespace id");
    let recording = RequestLog::new(temp_dir.path());
    let blocking = Arc::new(BlockingStore::matching(
        recording.store(),
        crate::common::data_wal_put_for(&namespace_id),
    ));
    let store: SharedObjectStore = blocking.clone();
    let writer = build_initialized_writer(store.clone(), &namespace_id, "mixed-writer").await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let content_ref =
        loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"mixed content")
            .await
            .expect("stage content")
            .into_content_ref();
    // Full preparation deliberately costs one content HEAD and one GET; do it
    // before the reset so this phase isolates mixed-batch publication.
    let prepared = prepare_content(&store, &namespace_id, &content_ref).await;
    recording.reset();

    blocking.block_next();
    let blocker = {
        let namespace = namespace.clone();
        tokio::spawn(async move {
            namespace
                .commit_candidate(CommitCandidate::new(CommitRequest::single(
                    CommitId::parse("mixed-blocker").expect("valid commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::CreateDirectory {
                        path: parse_mutation_path("/hold").expect("valid mutation path"),
                        parents: false,
                    },
                )))
                .await
        })
    };
    blocking.wait_until_blocked().await;

    let mut admitted = Box::pin(namespace.commit_candidate(CommitCandidate::prepared(
        put_request("mixed-admitted", "/admitted.txt", content_ref.clone()),
        vec![prepared],
    )));
    assert!(
        futures::poll!(admitted.as_mut()).is_pending(),
        "admitted put must queue behind the blocked publication"
    );
    let mut unprepared = Box::pin(namespace.commit_candidate(CommitCandidate::new(put_request(
        "mixed-unprepared",
        "/unprepared.txt",
        content_ref.clone(),
    ))));
    assert!(
        futures::poll!(unprepared.as_mut()).is_pending(),
        "unprepared put must join the pending batch"
    );

    blocking.release();
    blocker
        .await
        .expect("blocker task")
        .expect("blocker publication");
    admitted.await.expect("admitted put publishes");
    let error = unprepared
        .await
        .expect_err("unprepared put must fail independently");

    assert_content_not_prepared(error, &content_ref);
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);
}

fn is_content_key(key: &str) -> bool {
    loonfs_objectstore::layout::parse_object_key(key).is_some_and(|key| {
        key.family() == loonfs_objectstore::layout::DurableObjectFamily::ContentBlob
    })
}
