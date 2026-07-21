//! Content-object request accounting across staging and publication.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::content_tokens::ContentTokenError;
use loonfs::publish::{
    parse_mutation_path, ContentPreparationError, NamespaceMutation, NamespaceMutationCandidate,
    PathMutationIntent, PreparedContent,
};
use loonfs::{
    CommitId, CommitOp, CommitRequest, CoreError, CreateDirectoryOptions, CreateNamespaceOptions,
    DestinationBehavior, ErrorCode, FsWriter, InodeId, NamespaceId, PutFileOptions, RevisionNo,
    SharedObjectStore,
};
use loonfs_objectstore::keys::wal_head;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::{tempdir, TempDir};
use tokio::sync::Notify;
use tokio::time::{timeout, Duration};

#[derive(Debug, Clone, Copy)]
enum KeyClass {
    Content = 0,
    Other = 1,
}

impl KeyClass {
    fn of(key: &str) -> Self {
        if key.starts_with("content-stores/") {
            Self::Content
        } else {
            Self::Other
        }
    }
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

#[derive(Debug, Default)]
struct RequestLog {
    counts: Mutex<RequestCounts>,
}

impl RequestLog {
    fn record_head(&self, key: &str) {
        self.counts
            .lock()
            .expect("request log lock poisoned")
            .by_class[KeyClass::of(key) as usize]
            .head += 1;
    }

    fn record_get(&self, key: &str, bytes_read: usize) {
        let class = KeyClass::of(key);
        let mut counts = self.counts.lock().expect("request log lock poisoned");
        counts.by_class[class as usize].get += 1;
        if matches!(class, KeyClass::Content) {
            counts.content_get_bytes += bytes_read;
        }
    }

    fn record_put(&self, key: &str) {
        self.counts
            .lock()
            .expect("request log lock poisoned")
            .by_class[KeyClass::of(key) as usize]
            .put += 1;
    }

    fn snapshot(&self) -> RequestCounts {
        *self.counts.lock().expect("request log lock poisoned")
    }

    fn reset(&self) {
        *self.counts.lock().expect("request log lock poisoned") = RequestCounts::default();
    }
}

#[derive(Debug)]
struct RecordingStore {
    inner: LocalFsStore,
    log: RequestLog,
}

impl RecordingStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            log: RequestLog::default(),
        }
    }

    fn snapshot(&self) -> RequestCounts {
        self.log.snapshot()
    }

    fn reset(&self) {
        self.log.reset();
    }
}

#[async_trait]
impl ObjectStore for RecordingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.log.record_head(key);
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let result = self.inner.get(key, range).await;
        let bytes_read = result
            .as_ref()
            .ok()
            .and_then(|bytes| bytes.as_ref())
            .map_or(0, Bytes::len);
        self.log.record_get(key, bytes_read);
        result
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let result = self.inner.get_with_metadata(key).await;
        let bytes_read = result
            .as_ref()
            .ok()
            .and_then(|body| body.as_ref())
            .map_or(0, |body| body.bytes.len());
        self.log.record_get(key, bytes_read);
        result
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.log.record_put(key);
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
struct BlockingHeadCasStore {
    inner: Arc<RecordingStore>,
    head_key: String,
    block_next: AtomicBool,
    blocked: AtomicBool,
    released: AtomicBool,
    blocked_notify: Notify,
    release_notify: Notify,
}

impl BlockingHeadCasStore {
    fn new(inner: Arc<RecordingStore>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner,
            head_key: wal_head(namespace_id.as_str()),
            block_next: AtomicBool::new(false),
            blocked: AtomicBool::new(false),
            released: AtomicBool::new(false),
            blocked_notify: Notify::new(),
            release_notify: Notify::new(),
        }
    }

    fn arm_next_head_cas(&self) {
        self.released.store(false, Ordering::SeqCst);
        self.blocked.store(false, Ordering::SeqCst);
        self.block_next.store(true, Ordering::SeqCst);
    }

    async fn wait_for_blocked_head_cas(&self) {
        timeout(Duration::from_secs(10), async {
            while !self.blocked.load(Ordering::SeqCst) {
                let notified = self.blocked_notify.notified();
                if self.blocked.load(Ordering::SeqCst) {
                    break;
                }
                // Bounded wait: `notified` registers on first poll, so a notify
                // between the flag check and the await would otherwise be lost.
                let _ = timeout(Duration::from_millis(50), notified).await;
            }
        })
        .await
        .expect("a head CAS must reach the block");
    }

    fn release_head_cas(&self) {
        self.released.store(true, Ordering::SeqCst);
        self.release_notify.notify_waiters();
    }
}

#[async_trait]
impl ObjectStore for BlockingHeadCasStore {
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
        let should_block = key == self.head_key
            && matches!(mode, PutMode::CompareAndSwap { .. })
            && self.block_next.swap(false, Ordering::SeqCst);
        if should_block {
            self.blocked.store(true, Ordering::SeqCst);
            self.blocked_notify.notify_waiters();
            while !self.released.load(Ordering::SeqCst) {
                let notified = self.release_notify.notified();
                if self.released.load(Ordering::SeqCst) {
                    break;
                }
                // Bounded wait, matching publication.rs: a release landing between
                // the flag check and the await must not strand this writer.
                let _ = timeout(Duration::from_millis(50), notified).await;
            }
            self.blocked.store(false, Ordering::SeqCst);
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
struct HeadCasConflictStore {
    inner: Arc<RecordingStore>,
    head_key: String,
    fail_next: AtomicBool,
    attempts: AtomicUsize,
}

impl HeadCasConflictStore {
    fn new(inner: Arc<RecordingStore>, namespace_id: &NamespaceId) -> Self {
        Self {
            inner,
            head_key: wal_head(namespace_id.as_str()),
            fail_next: AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
        }
    }

    fn fail_next_head_cas(&self) {
        self.attempts.store(0, Ordering::SeqCst);
        self.fail_next.store(true, Ordering::SeqCst);
    }

    fn head_cas_attempts(&self) -> usize {
        self.attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ObjectStore for HeadCasConflictStore {
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
        if key == self.head_key && matches!(mode, PutMode::CompareAndSwap { .. }) {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self.fail_next.swap(false, Ordering::SeqCst) {
                return Err(ObjectStoreError::PreconditionFailed {
                    object_key: key.to_owned(),
                });
            }
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

struct TestHarness {
    _temp_dir: TempDir,
    recording: Arc<RecordingStore>,
    store: SharedObjectStore,
    namespace_id: NamespaceId,
    writer: FsWriter,
}

impl TestHarness {
    async fn new(namespace: &str) -> Self {
        let temp_dir = tempdir().expect("tempdir");
        let recording = Arc::new(RecordingStore::new(temp_dir.path()));
        let store: SharedObjectStore = recording.clone();
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
            CreateDirectoryOptions::default(),
        )
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
    loonfs_core::content::prepare_existing_content_ref(
        store,
        namespace_id,
        catalog.content_store_id(),
        content_ref.clone(),
    )
    .await
    .expect("prepare existing content")
}

fn put_intent(commit_id: &str, path: &str, content_ref: loonfs::ContentRef) -> PathMutationIntent {
    PathMutationIntent::PutFile {
        commit_id: CommitId::parse(commit_id).expect("valid commit id"),
        absolute_path: parse_mutation_path(path).expect("valid mutation path"),
        content_ref,
        behavior: DestinationBehavior::NoReplace,
    }
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
                ref content_ref_digest
            }) if content_ref_digest == &content_ref.digest
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

    // These global counts show the explicitly slow helper still validates
    // exactly once, but not where. The plain-candidate seam below proves the
    // publisher itself performs no content-key I/O, locating this read before
    // publication.
    harness
        .writer
        .put_file_content_ref(
            &harness.namespace_id,
            "/file.txt",
            content_ref,
            PutFileOptions::default(),
        )
        .await
        .expect("publish content ref");

    assert_content_counts(harness.recording.snapshot(), 1, 1, 0, bytes.len());
}

#[tokio::test]
async fn plain_path_put_fails_unprepared_without_content_io() {
    let harness = TestHarness::new("plain-path-unprepared").await;
    let content_ref = harness.stage_content(b"unprepared path content").await;
    harness.recording.reset();

    let error = harness
        .writer
        .publisher()
        .submit_path_intent(
            harness.namespace_id.clone(),
            put_intent("plain-unprepared-put", "/file.txt", content_ref.clone()),
        )
        .await
        .expect_err("plain path put must require prepared content");

    assert_content_not_prepared(error, &content_ref);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn explicit_create_file_fails_unprepared_without_content_io() {
    let harness = TestHarness::new("create-unprepared").await;
    let content_ref = harness.stage_content(b"explicit create content").await;
    harness.recording.reset();

    // Explicit commits use the same in-memory proof coverage as path puts;
    // publication never rescues an unprepared ref with content I/O.
    let error = harness
        .writer
        .publisher()
        .submit_commit(
            harness.namespace_id.clone(),
            CommitRequest {
                commit_id: CommitId::parse("explicit-create").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::CreateFile {
                    parent_inode_id: InodeId(1),
                    display_name: "file.txt".to_owned(),
                    content_ref: content_ref.clone(),
                }],
                message: None,
            },
        )
        .await
        .expect_err("unprepared explicit create must fail");

    assert_content_not_prepared(error, &content_ref);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn explicit_replace_file_fails_unprepared_without_content_io() {
    let harness = TestHarness::new("replace-unprepared").await;
    harness
        .writer
        .put_file_bytes(
            &harness.namespace_id,
            "/file.txt",
            b"first revision",
            PutFileOptions::default(),
        )
        .await
        .expect("seed file");
    let inode_id = harness
        .writer
        .reader()
        .stat_path(&harness.namespace_id, "/file.txt")
        .await
        .expect("stat seeded file")
        .inode_id;
    let content_ref = harness.stage_content(b"explicit replacement content").await;
    harness.recording.reset();

    // The existing inode does not change proof coverage: a replacement's
    // external ref must already be prepared before publication.
    let error = harness
        .writer
        .publisher()
        .submit_commit(
            harness.namespace_id.clone(),
            CommitRequest {
                commit_id: CommitId::parse("explicit-replace").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::ReplaceFile {
                    inode_id,
                    base_revision_no: RevisionNo(1),
                    content_ref: content_ref.clone(),
                }],
                message: None,
            },
        )
        .await
        .expect_err("unprepared explicit replacement must fail");

    assert_content_not_prepared(error, &content_ref);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn explicit_commit_with_prepared_distinct_and_repeated_refs_uses_no_publication_content_io() {
    let harness = TestHarness::new("prepared-explicit-many").await;
    let first = harness.stage_content(b"first prepared content").await;
    let second = harness.stage_content(b"second prepared content").await;
    let third = harness.stage_content(b"third prepared content").await;
    let prepared = vec![
        prepare_content(&harness.store, &harness.namespace_id, &first).await,
        prepare_content(&harness.store, &harness.namespace_id, &second).await,
        prepare_content(&harness.store, &harness.namespace_id, &third).await,
    ];
    harness.recording.reset();

    harness
        .writer
        .publisher()
        .submit_candidate(
            harness.namespace_id.clone(),
            NamespaceMutationCandidate::commit_prepared(
                CommitRequest {
                    commit_id: CommitId::parse("prepared-explicit-many").expect("valid commit id"),
                    preconditions: Vec::new(),
                    ops: vec![
                        CommitOp::CreateFile {
                            parent_inode_id: InodeId(1),
                            display_name: "first.txt".to_owned(),
                            content_ref: first.clone(),
                        },
                        CommitOp::CreateFile {
                            parent_inode_id: InodeId(1),
                            display_name: "first-copy.txt".to_owned(),
                            content_ref: first,
                        },
                        CommitOp::CreateFile {
                            parent_inode_id: InodeId(1),
                            display_name: "second.txt".to_owned(),
                            content_ref: second,
                        },
                        CommitOp::CreateFile {
                            parent_inode_id: InodeId(1),
                            display_name: "third.txt".to_owned(),
                            content_ref: third,
                        },
                    ],
                    message: None,
                },
                prepared,
            ),
        )
        .await
        .expect("publish prepared explicit commit");

    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn explicit_restore_revision_uses_retained_metadata_without_content_io() {
    let harness = TestHarness::new("restore-retained").await;
    let first = b"first revision";
    harness
        .writer
        .put_file_bytes(
            &harness.namespace_id,
            "/file.txt",
            first,
            PutFileOptions::default(),
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
                commit_id: None,
            },
        )
        .await
        .expect("put second revision");
    let inode_id = harness
        .writer
        .reader()
        .stat_path(&harness.namespace_id, "/file.txt")
        .await
        .expect("stat current file")
        .inode_id;
    harness.recording.reset();

    // Restore resolves content from retained namespace metadata, so
    // re-downloading the retained blob would prove nothing.
    harness
        .writer
        .commit_operations(
            &harness.namespace_id,
            CommitRequest {
                commit_id: CommitId::parse("explicit-restore").expect("valid commit id"),
                preconditions: Vec::new(),
                ops: vec![CommitOp::RestoreRevision {
                    inode_id,
                    source_revision_no: RevisionNo(1),
                    base_revision_no: RevisionNo(2),
                }],
                message: None,
            },
        )
        .await
        .expect("publish explicit restore");

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
            PutFileOptions::default(),
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
    let intent = put_intent("replayed-put", "/file.txt", content_ref.clone());
    let prepared = prepare_content(&harness.store, &harness.namespace_id, &content_ref).await;
    let original = harness
        .writer
        .publisher()
        .submit_path_intent_with_prepared_content(
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
        .submit_path_intent(harness.namespace_id.clone(), intent)
        .await
        .expect("replay put");

    assert_eq!(replay, original);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn rejected_preparation_replays_durable_receipt_without_content_operations() {
    let harness = TestHarness::new("rejected-receipt-replay").await;
    let content_ref = harness.stage_content(b"replayed rejected content").await;
    let intent = put_intent("rejected-replayed-put", "/file.txt", content_ref.clone());
    let prepared = prepare_content(&harness.store, &harness.namespace_id, &content_ref).await;
    let original = harness
        .writer
        .publisher()
        .submit_path_intent_with_prepared_content(
            harness.namespace_id.clone(),
            intent.clone(),
            vec![prepared],
        )
        .await
        .expect("publish original put");
    harness.recording.reset();

    let replay = harness
        .writer
        .publish_namespace_mutations_batch(
            &harness.namespace_id,
            vec![NamespaceMutationCandidate::rejected(
                NamespaceMutation::Path(intent),
                ContentPreparationError::ContentToken(ContentTokenError::Expired),
            )],
        )
        .await
        .pop()
        .expect("one replay result")
        .expect("rejected preparation must replay the durable receipt");

    assert_eq!(replay, original);
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

#[tokio::test]
async fn new_rejected_preparation_fails_before_path_planning_without_content_operations() {
    let harness = TestHarness::new("new-rejected-preparation").await;
    harness.recording.reset();
    let intent = PathMutationIntent::CreateDir {
        commit_id: CommitId::parse("new-rejected").expect("valid commit id"),
        absolute_path: parse_mutation_path("/missing/child").expect("valid mutation path"),
        parents: false,
    };

    let error = harness
        .writer
        .publish_namespace_mutations_batch(
            &harness.namespace_id,
            vec![NamespaceMutationCandidate::rejected(
                NamespaceMutation::Path(intent),
                ContentPreparationError::ContentToken(ContentTokenError::Expired),
            )],
        )
        .await
        .pop()
        .expect("one rejected result")
        .expect_err("new rejected preparation must fail");
    assert!(
        matches!(error, loonfs::Error::Core(_)),
        "expected core content-preparation error"
    );
    let loonfs::Error::Core(error) = error else {
        return;
    };

    assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
    assert!(matches!(
        error,
        CoreError::ContentPreparation(ContentPreparationError::ContentToken(
            ContentTokenError::Expired
        ))
    ));
    assert_content_counts(harness.recording.snapshot(), 0, 0, 0, 0);
}

/// Exercises duplicate admission while the primary publication is stalled at its head CAS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_duplicate_performs_no_additional_content_operations() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("in-flight-duplicate").expect("valid namespace id");
    let recording = Arc::new(RecordingStore::new(temp_dir.path()));
    let blocking = Arc::new(BlockingHeadCasStore::new(
        Arc::clone(&recording),
        &namespace_id,
    ));
    let store: SharedObjectStore = blocking.clone();
    let writer = build_initialized_writer(store.clone(), &namespace_id, "duplicate-writer").await;
    let content_ref =
        loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"duplicate content")
            .await
            .expect("stage content")
            .content_ref;
    let intent = put_intent("in-flight-put", "/file.txt", content_ref.clone());
    // Full preparation deliberately costs one content HEAD and one GET; do it
    // before the reset so this phase isolates publication and duplicate join.
    let prepared = prepare_content(&store, &namespace_id, &content_ref).await;
    recording.reset();

    blocking.arm_next_head_cas();
    let primary = {
        let registry = writer.publisher();
        let namespace_id = namespace_id.clone();
        let intent = intent.clone();
        let prepared = prepared.clone();
        tokio::spawn(async move {
            registry
                .submit_path_intent_with_prepared_content(namespace_id, intent, vec![prepared])
                .await
        })
    };
    blocking.wait_for_blocked_head_cas().await;
    let primary_counts = recording.snapshot();
    assert_content_counts(primary_counts, 0, 0, 0, 0);

    let registry = writer.publisher();
    let mut duplicate = Box::pin(registry.submit_path_intent_with_prepared_content(
        namespace_id.clone(),
        intent,
        vec![prepared],
    ));
    assert!(
        futures::poll!(duplicate.as_mut()).is_pending(),
        "the duplicate must join the in-flight primary"
    );
    blocking.release_head_cas();

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
    let recording = Arc::new(RecordingStore::new(temp_dir.path()));
    let conflicting = Arc::new(HeadCasConflictStore::new(
        Arc::clone(&recording),
        &namespace_id,
    ));
    let store: SharedObjectStore = conflicting.clone();
    let writer = build_initialized_writer(store.clone(), &namespace_id, "retry-writer").await;
    let content_ref =
        loonfs_core::content::store_bytes_as_content(&store, &namespace_id, b"retry content")
            .await
            .expect("stage content")
            .content_ref;
    let intent = put_intent("retry-put", "/file.txt", content_ref.clone());
    // Full preparation deliberately costs one content HEAD and one GET; do it
    // before the reset so this phase isolates publication retries.
    let prepared = prepare_content(&store, &namespace_id, &content_ref).await;
    recording.reset();
    conflicting.fail_next_head_cas();

    writer
        .publisher()
        .submit_path_intent_with_prepared_content(namespace_id, intent, vec![prepared])
        .await
        .expect("publish after stale-head retry");

    assert_eq!(conflicting.head_cas_attempts(), 2);
    assert_content_counts(recording.snapshot(), 0, 0, 0, 0);
}

/// Holds one publication at its head CAS so the next two candidates are
/// deterministically collected into the same batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_batch_publishes_admitted_put_and_rejects_unprepared_put_without_content_io() {
    let temp_dir = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("mixed-preparation").expect("valid namespace id");
    let recording = Arc::new(RecordingStore::new(temp_dir.path()));
    let blocking = Arc::new(BlockingHeadCasStore::new(
        Arc::clone(&recording),
        &namespace_id,
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

    blocking.arm_next_head_cas();
    let blocker = {
        let registry = writer.publisher();
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            registry
                .submit_path_intent(
                    namespace_id,
                    PathMutationIntent::CreateDir {
                        commit_id: CommitId::parse("mixed-blocker").expect("valid commit id"),
                        absolute_path: parse_mutation_path("/hold").expect("valid mutation path"),
                        parents: false,
                    },
                )
                .await
        })
    };
    blocking.wait_for_blocked_head_cas().await;

    let registry = writer.publisher();
    let mut admitted = Box::pin(registry.submit_path_intent_with_prepared_content(
        namespace_id.clone(),
        put_intent("mixed-admitted", "/admitted.txt", content_ref.clone()),
        vec![prepared],
    ));
    assert!(
        futures::poll!(admitted.as_mut()).is_pending(),
        "admitted put must queue behind the blocked publication"
    );
    let mut unprepared = Box::pin(registry.submit_path_intent(
        namespace_id.clone(),
        put_intent("mixed-unprepared", "/unprepared.txt", content_ref.clone()),
    ));
    assert!(
        futures::poll!(unprepared.as_mut()).is_pending(),
        "unprepared put must join the pending batch"
    );

    blocking.release_head_cas();
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
