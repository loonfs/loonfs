//! Content-object request accounting across staging and publication.

use bytes::Bytes;
use loonfs::content_tokens::ContentTokenError;
use loonfs::publish::{
    parse_mutation_path, CommitCandidate, CommitRequest, ContentPreparationError,
    FilesystemOperation, PreparedContent,
};
use loonfs::{
    BeginUploadRequest, CommitId, CompleteUploadRequest, CoreError, CreateDirectoryOptions,
    CreateNamespaceOptions, DestinationBehavior, ErrorCode, FsWriter, NamespaceId, PutFileOptions,
    RevisionNo, RuntimeError, SharedObjectStore,
};
use loonfs_api::wire::control::{encode_control_object, ControlObjectKind, HeadStateEnvelope};
use loonfs_api::{ContentId, ContentStoreId};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{
    BlockingStore, CountingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
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
    content: Arc<CountingStore<SharedObjectStore>>,
    other: Arc<CountingStore<SharedObjectStore>>,
}

impl RequestLog {
    fn new(root: &Path) -> Self {
        let inner: SharedObjectStore =
            Arc::new(LocalFsStore::new(root).expect("create local-fs store"));
        let other = Arc::new(CountingStore::new(
            inner,
            KeyPredicate::new(|key| !key.starts_with("content-stores/")),
        ));
        let content = Arc::new(CountingStore::new(
            other.clone() as SharedObjectStore,
            KeyPredicate::prefix("content-stores/"),
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
        let content = self.content.snapshot();
        let other = self.other.snapshot();
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
    writer: FsWriter,
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
            .content_ref
    }
}

async fn build_initialized_writer(
    store: SharedObjectStore,
    namespace_id: &NamespaceId,
    writer_id: &str,
) -> FsWriter {
    let writer = FsWriter::builder_with_store(store)
        .writer_id(writer_id)
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    // The immutable content-store descriptor shares the content-key prefix. Warm it before
    // resetting a phase so content counts describe blob staging and validation alone.
    writer
        .create_directory(
            namespace_id,
            "/catalog-warmup",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("warm namespace catalog");
    writer
}

/// Points a namespace at another namespace's content store, standing in for
/// a deployment that provisioned both against one shared keyspace. The
/// binding lives in the head, so this rewrites the head.
async fn bind_namespace_to_content_store(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    content_store_id: ContentStoreId,
) {
    let mut head = loonfs_core::control::load_namespace_head_control(store, namespace_id)
        .await
        .expect("load namespace head")
        .state;
    head.content_store_id = content_store_id;
    let envelope = HeadStateEnvelope::from_state(ControlObjectKind::WalHead, head)
        .expect("build namespace head");
    store
        .put_overwrite(
            &wal_head(namespace_id),
            Bytes::from(encode_control_object(&envelope).expect("encode namespace head")),
        )
        .await
        .expect("rebind the namespace head to the shared content store");
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
            content_ref,
            behavior: DestinationBehavior::NoReplace,
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

fn assert_content_not_prepared(error: CoreError, content_ref: &loonfs::ContentRef) {
    assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
    assert!(
        matches!(
            error,
            CoreError::ContentPreparation(ContentPreparationError::ContentNotPrepared {
                ref content_id
            }) if content_id == &content_ref.content_id
        ),
        "content-not-prepared error should carry the rejected digest"
    );
}

#[tokio::test]
async fn put_file_content_ref_validates_content_before_publication() {
    let harness = TestHarness::new("content-ref-validation").await;
    let bytes = b"pre-staged content";
    let content_ref = harness.stage_content(bytes).await;
    harness.recording.reset();

    // The convenience method should validate the referenced content exactly
    // once before publishing it.
    harness
        .writer
        .put_file_content_ref(
            &harness.namespace_id,
            "/file.txt",
            content_ref,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("publish content ref");

    assert_content_counts(harness.recording.snapshot(), 1, 1, 0, bytes.len());
}

#[tokio::test]
async fn prepare_file_bytes_performs_one_content_put_and_no_reads() {
    let harness = TestHarness::new("prepare-file-bytes").await;
    let bytes = b"parallel preparation primitive";
    harness.recording.reset();

    let prepared = harness
        .writer
        .prepare_file_bytes(&harness.namespace_id, bytes)
        .await
        .expect("prepare file bytes");

    let content_ref = prepared.content_ref();
    assert_eq!(content_ref.size_bytes, bytes.len() as u64);
    assert_eq!(content_ref.checksum, loonfs_api::Checksum::sha256(bytes));
    assert_content_counts(harness.recording.snapshot(), 0, 0, 1, 0);
}

#[tokio::test]
async fn put_file_prepared_performs_no_content_io() {
    let harness = TestHarness::new("put-file-prepared").await;
    let prepared = harness
        .writer
        .prepare_file_bytes(&harness.namespace_id, b"already prepared")
        .await
        .expect("prepare file bytes");
    harness.recording.reset();

    harness
        .writer
        .put_file_prepared(
            &harness.namespace_id,
            "/file.txt",
            prepared,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
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
    writer
        .create_namespace(&target, CreateNamespaceOptions::default())
        .await
        .expect("create target namespace");
    writer
        .create_directory(
            &target,
            "/catalog-warmup",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("warm target catalog");
    let prepared = writer
        .prepare_file_bytes(&source, b"source-store-only")
        .await
        .expect("prepare source content");
    let content_ref = prepared.content_ref().clone();
    recording.reset();

    let error = writer
        .put_file_prepared(
            &target,
            "/file.txt",
            prepared,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect_err("another store must reject the admission");
    assert!(
        matches!(error, RuntimeError::Core(_)),
        "expected core content-preparation error"
    );
    let RuntimeError::Core(error) = error else {
        return;
    };

    assert_content_not_prepared(error, &content_ref);
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn independent_namespaces_sharing_a_store_share_prepared_content() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = RequestLog::new(temp_dir.path());
    let store = recording.store();
    let source = NamespaceId::parse("shared-source").expect("source namespace id");
    let target = NamespaceId::parse("shared-target").expect("target namespace id");
    let writer = build_initialized_writer(store.clone(), &source, "shared-store-writer").await;
    writer
        .create_namespace(&target, CreateNamespaceOptions::default())
        .await
        .expect("create independent target namespace");
    let source_catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &source)
        .await
        .expect("load source catalog");
    bind_namespace_to_content_store(&store, &target, source_catalog.content_store_id().clone())
        .await;
    writer
        .create_directory(
            &target,
            "/catalog-warmup",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("warm shared target catalog");
    let prepared = writer
        .prepare_file_bytes(&source, b"independently shared")
        .await
        .expect("prepare through source namespace");
    recording.reset();

    writer
        .put_file_prepared(
            &target,
            "/shared.txt",
            prepared,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("shared-store target accepts source proof");

    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn fork_and_source_share_prepared_content_in_both_directions() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = RequestLog::new(temp_dir.path());
    let store = recording.store();
    let source = NamespaceId::parse("fork-source").expect("source namespace id");
    let fork = NamespaceId::parse("fork-target").expect("fork namespace id");
    let writer = build_initialized_writer(store, &source, "fork-sharing-writer").await;
    let prepared_for_fork = writer
        .prepare_file_bytes(&source, b"prepared before fork")
        .await
        .expect("prepare through source namespace");
    writer
        .fork_namespace(&source, &fork)
        .await
        .expect("fork namespace");
    writer
        .create_directory(
            &fork,
            "/fork-catalog-warmup",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("warm fork catalog");
    recording.reset();

    writer
        .put_file_prepared(
            &fork,
            "/from-source.txt",
            prepared_for_fork,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("fork accepts source proof");
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);

    let prepared_for_source = writer
        .prepare_file_bytes(&fork, b"prepared through fork")
        .await
        .expect("prepare through fork namespace");
    recording.reset();
    writer
        .put_file_prepared(
            &source,
            "/from-fork.txt",
            prepared_for_source,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("source accepts fork proof");
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn prepare_content_ref_reads_once_and_prepared_publication_reads_nothing() {
    let harness = TestHarness::new("prepare-content-ref").await;
    let bytes = b"externally staged content";
    let content_ref = harness.stage_content(bytes).await;
    harness.recording.reset();

    let prepared = harness
        .writer
        .prepare_content_ref(&harness.namespace_id, content_ref)
        .await
        .expect("prepare content ref");

    assert_content_counts(harness.recording.snapshot(), 1, 1, 0, bytes.len());
    harness.recording.reset();

    harness
        .writer
        .put_file_prepared(
            &harness.namespace_id,
            "/file.txt",
            prepared,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("publish prepared content ref");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn proxied_upload_completion_proof_publishes_without_additional_content_io() {
    let harness = TestHarness::new("proxied-completion-proof").await;
    let bytes = b"service proxied upload";
    let begin = harness
        .writer
        .begin_upload(&harness.namespace_id, BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    harness.recording.reset();

    let staged = harness
        .writer
        .upload_content(&harness.namespace_id, begin.upload_id(), bytes)
        .await
        .expect("upload content");
    let upload_counts = harness.recording.snapshot();
    // One blob PUT and nothing else: the namespace's content store is a
    // field in its head, so staging never reads the content-store keyspace.
    assert_eq!(
        upload_counts.operations(KeyClass::Content),
        OperationCounts {
            head: 0,
            get: 0,
            put: 1,
        }
    );
    harness.recording.reset();

    let completed = harness
        .writer
        .complete_upload_prepared(
            &harness.namespace_id,
            begin.upload_id(),
            &CompleteUploadRequest::for_content_ref(staged.content_ref),
        )
        .await
        .expect("complete upload with proof");
    let prepared = completed.prepared;
    assert_eq!(prepared.content_ref(), &completed.response.content_ref);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
    harness.recording.reset();

    harness
        .writer
        .put_file_prepared(
            &harness.namespace_id,
            "/uploaded.txt",
            prepared,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("publish uploaded content");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn direct_put_completion_avoids_blob_get_and_prepared_publish_uses_no_content_io() {
    let harness = TestHarness::new("direct-completion-proof").await;
    let bytes = b"direct provider upload";
    let begin = harness
        .writer
        .begin_direct_put_upload_target(
            &harness.namespace_id,
            loonfs::UploadContentClaim {
                size_bytes: bytes.len() as u64,
                checksum: loonfs_api::Checksum::sha256(bytes),
            },
        )
        .await
        .expect("begin direct put");
    let content_ref = begin.target.content_ref.clone();
    harness.recording.reset();

    harness
        .store
        .put_if_absent(&begin.target.object_key, Bytes::copy_from_slice(bytes))
        .await
        .expect("drive direct provider upload");
    assert_content_counts(harness.recording.snapshot(), 0, 0, 1, 0);
    harness.recording.reset();

    let completed = harness
        .writer
        .complete_upload_prepared(
            &harness.namespace_id,
            &begin.upload_id,
            &CompleteUploadRequest::for_content_ref(content_ref.clone()),
        )
        .await
        .expect("complete direct put with proof");
    let prepared = completed.prepared;
    assert_eq!(completed.response.content_ref, content_ref);
    // The session path reuses the runtime-resolved immutable store binding
    // and proves the provider write with one object HEAD; it never reads the
    // uploaded blob or reloads the content-store descriptor.
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

    harness
        .writer
        .put_file_prepared(
            &harness.namespace_id,
            "/direct.txt",
            prepared,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("publish direct uploaded content");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn plain_path_put_fails_unprepared_without_content_io() {
    let harness = TestHarness::new("plain-path-unprepared").await;
    let content_ref = harness.stage_content(b"unprepared path content").await;
    harness.recording.reset();

    let error = harness
        .writer
        .publisher()
        .submit_commit(
            harness.namespace_id.clone(),
            put_request("plain-unprepared-put", "/file.txt", content_ref.clone()),
        )
        .await
        .expect_err("plain path put must require prepared content");

    assert_content_not_prepared(error, &content_ref);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn writer_mutate_with_external_ref_fails_typed_without_content_io() {
    let harness = TestHarness::new("create-unprepared").await;
    let content_ref = harness.stage_content(b"unprepared create content").await;
    harness.recording.reset();

    // The writer surface wraps the same in-memory proof coverage the
    // publisher applies; publication never rescues an unprepared ref with
    // content I/O, and the caller still receives a typed core error.
    let error = harness
        .writer
        .commit(
            &harness.namespace_id,
            put_request("mutate-create", "/file.txt", content_ref.clone()),
        )
        .await
        .expect_err("unprepared create must fail");

    assert!(
        matches!(error, RuntimeError::Core(_)),
        "expected typed core error, got {error:?}"
    );
    let RuntimeError::Core(error) = error else {
        return;
    };
    assert_content_not_prepared(error, &content_ref);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn replacing_put_fails_unprepared_without_content_io() {
    let harness = TestHarness::new("replace-unprepared").await;
    harness
        .writer
        .put_file_bytes(
            &harness.namespace_id,
            "/file.txt",
            b"first revision",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("seed file");
    let content_ref = harness
        .stage_content(b"unprepared replacement content")
        .await;
    harness.recording.reset();

    // The existing file does not change proof coverage: a replacement's
    // external ref must already be prepared before publication.
    let error = harness
        .writer
        .publisher()
        .submit_commit(
            harness.namespace_id.clone(),
            CommitRequest::single(
                CommitId::parse("unprepared-replace").expect("valid commit id"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::PutFile {
                    path: parse_mutation_path("/file.txt").expect("valid mutation path"),
                    content_ref: content_ref.clone(),
                    behavior: DestinationBehavior::Replace,
                    expected_revision_no: None,
                },
            ),
        )
        .await
        .expect_err("unprepared replacement must fail");

    assert_content_not_prepared(error, &content_ref);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

/// The spawned preparations exercise concurrent use of one cloned writer before one commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_commit_after_concurrent_preparations_uses_no_publication_content_io() {
    let harness = TestHarness::new("prepared-explicit-many").await;
    let preparations = [
        b"first prepared content" as &'static [u8],
        b"second prepared content",
        b"third prepared content",
    ]
    .into_iter()
    .map(|bytes| {
        let writer = harness.writer.clone();
        let namespace_id = harness.namespace_id.clone();
        tokio::spawn(async move { writer.prepare_file_bytes(&namespace_id, bytes).await })
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
        content_ref,
        behavior: DestinationBehavior::NoReplace,
        expected_revision_no: None,
    };
    harness
        .writer
        .commit_prepared(
            &harness.namespace_id,
            CommitRequest {
                commit_id: CommitId::parse("prepared-many-puts").expect("valid commit id"),
                actor: loonfs_test_support::test_actor(),
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
    let first = b"first revision";
    harness
        .writer
        .put_file_bytes(
            &harness.namespace_id,
            "/file.txt",
            first,
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("put first revision");
    harness
        .writer
        .put_file_bytes(
            &harness.namespace_id,
            "/file.txt",
            b"second revision",
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                commit: loonfs_api::options::CommitOptions {
                    actor: loonfs_test_support::test_actor(),
                    commit_id: None,
                    message: None,
                },
                expected_revision_no: None,
            },
        )
        .await
        .expect("put second revision");
    harness.recording.reset();

    // Restore resolves content from retained namespace metadata, so
    // re-downloading the retained blob would prove nothing.
    harness
        .writer
        .commit(
            &harness.namespace_id,
            CommitRequest::single(
                CommitId::parse("restore-first-revision").expect("valid commit id"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::RestoreRevision {
                    path: parse_mutation_path("/file.txt").expect("valid mutation path"),
                    source_revision_no: RevisionNo(1),
                },
            ),
        )
        .await
        .expect("publish restore");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn put_file_bytes_publishes_without_reading_content() {
    let harness = TestHarness::new("put-bytes-admission").await;
    harness.recording.reset();

    harness
        .writer
        .put_file_bytes(
            &harness.namespace_id,
            "/file.txt",
            b"admitted content",
            PutFileOptions::new(loonfs_test_support::test_actor()),
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
    let original = harness
        .writer
        .publisher()
        .submit_commit_with_prepared_content(
            harness.namespace_id.clone(),
            intent.clone(),
            vec![prepared],
        )
        .await
        .expect("publish original put");
    harness.recording.reset();

    let replay = harness
        .writer
        .publisher()
        .submit_commit(harness.namespace_id.clone(), intent)
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
    let original = harness
        .writer
        .publisher()
        .submit_commit_with_prepared_content(
            harness.namespace_id.clone(),
            intent.clone(),
            vec![prepared],
        )
        .await
        .expect("publish original put");
    harness.recording.reset();

    let replay = harness
        .writer
        .publisher()
        .submit_candidate(
            harness.namespace_id.clone(),
            CommitCandidate::rejected(
                intent,
                ContentPreparationError::ContentToken(vec![(
                    ContentId::generate(),
                    ContentTokenError::Expired,
                )]),
            ),
        )
        .await
        .expect("rejected preparation must replay the durable receipt");

    assert_eq!(replay, original);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn new_rejected_preparation_fails_before_path_planning_without_content_operations() {
    let harness = TestHarness::new("new-rejected-preparation").await;
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

    let error = harness
        .writer
        .publisher()
        .submit_candidate(
            harness.namespace_id.clone(),
            CommitCandidate::rejected(
                intent,
                ContentPreparationError::ContentToken(vec![(
                    ContentId::generate(),
                    ContentTokenError::Expired,
                )]),
            ),
        )
        .await
        .expect_err("new rejected preparation must fail");

    assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
    assert!(matches!(
        error,
        CoreError::ContentPreparation(ContentPreparationError::ContentToken(ref rejections))
            if matches!(rejections[..], [(_, ContentTokenError::Expired)])
    ));
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

/// Exercises duplicate admission while the primary publication is stalled at its head CAS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_duplicate_performs_no_additional_content_operations() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("in-flight-duplicate").expect("valid namespace id");
    let recording = RequestLog::new(temp_dir.path());
    let blocking = Arc::new(BlockingStore::new(
        recording.store(),
        KeyPredicate::wal_head(&namespace_id),
        OperationClass::CompareAndSwap,
    ));
    let store: SharedObjectStore = blocking.clone();
    let writer = build_initialized_writer(store.clone(), &namespace_id, "duplicate-writer").await;
    let content_ref =
        loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"duplicate content")
            .await
            .expect("stage content")
            .content_ref;
    let intent = put_request("in-flight-put", "/file.txt", content_ref.clone());
    // Full preparation deliberately costs one content HEAD and one GET; do it
    // before the reset so this phase isolates publication and duplicate join.
    let prepared = prepare_content(&store, &namespace_id, &content_ref).await;
    recording.reset();

    blocking.block_next();
    let primary = {
        let registry = writer.publisher();
        let namespace_id = namespace_id.clone();
        let intent = intent.clone();
        let prepared = prepared.clone();
        tokio::spawn(async move {
            registry
                .submit_commit_with_prepared_content(namespace_id, intent, vec![prepared])
                .await
        })
    };
    blocking.wait_until_blocked().await;
    let primary_counts = recording.snapshot();
    assert_content_counts(primary_counts, 0, 0, 0, 0);

    let registry = writer.publisher();
    let mut duplicate = Box::pin(registry.submit_commit_with_prepared_content(
        namespace_id.clone(),
        intent,
        vec![prepared],
    ));
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
    let conflicting = Arc::new(FailStore::new(
        recording.store(),
        KeyPredicate::wal_head(&namespace_id),
        OperationClass::CompareAndSwap,
        InjectedError::PreconditionFailed,
    ));
    let store: SharedObjectStore = conflicting.clone();
    let writer = build_initialized_writer(store.clone(), &namespace_id, "retry-writer").await;
    let content_ref =
        loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"retry content")
            .await
            .expect("stage content")
            .content_ref;
    let intent = put_request("retry-put", "/file.txt", content_ref.clone());
    // Full preparation deliberately costs one content HEAD and one GET; do it
    // before the reset so this phase isolates publication retries.
    let prepared = prepare_content(&store, &namespace_id, &content_ref).await;
    recording.reset();
    conflicting.fail_next(1);

    writer
        .publisher()
        .submit_commit_with_prepared_content(namespace_id, intent, vec![prepared])
        .await
        .expect("publish after stale-head retry");

    assert_eq!(conflicting.attempts(), 2);
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);
}

/// Holds one publication at its head CAS so the next two candidates are
/// deterministically collected into the same batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_batch_publishes_admitted_put_and_rejects_unprepared_put_without_content_io() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("mixed-preparation").expect("valid namespace id");
    let recording = RequestLog::new(temp_dir.path());
    let blocking = Arc::new(BlockingStore::new(
        recording.store(),
        KeyPredicate::wal_head(&namespace_id),
        OperationClass::CompareAndSwap,
    ));
    let store: SharedObjectStore = blocking.clone();
    let writer = build_initialized_writer(store.clone(), &namespace_id, "mixed-writer").await;
    let content_ref =
        loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"mixed content")
            .await
            .expect("stage content")
            .content_ref;
    // Full preparation deliberately costs one content HEAD and one GET; do it
    // before the reset so this phase isolates mixed-batch publication.
    let prepared = prepare_content(&store, &namespace_id, &content_ref).await;
    recording.reset();

    blocking.block_next();
    let blocker = {
        let registry = writer.publisher();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_commit(
                    namespace_id,
                    CommitRequest::single(
                        CommitId::parse("mixed-blocker").expect("valid commit id"),
                        loonfs_test_support::test_actor(),
                        None,
                        FilesystemOperation::CreateDirectory {
                            path: parse_mutation_path("/hold").expect("valid mutation path"),
                            parents: false,
                        },
                    ),
                )
                .await
        })
    };
    blocking.wait_until_blocked().await;

    let registry = writer.publisher();
    let mut admitted = Box::pin(registry.submit_commit_with_prepared_content(
        namespace_id.clone(),
        put_request("mixed-admitted", "/admitted.txt", content_ref.clone()),
        vec![prepared],
    ));
    assert!(
        futures::poll!(admitted.as_mut()).is_pending(),
        "admitted put must queue behind the blocked publication"
    );
    let mut unprepared = Box::pin(registry.submit_commit(
        namespace_id.clone(),
        put_request("mixed-unprepared", "/unprepared.txt", content_ref.clone()),
    ));
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
