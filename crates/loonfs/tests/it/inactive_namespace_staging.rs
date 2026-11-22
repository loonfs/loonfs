//! Content preparation for a namespace that isn't active: every way of
//! staging content, and every write that stages content, returns the
//! namespace's error and writes nothing under the pool's prefix, and the
//! same writes succeed once the namespace is active. Content at or under the
//! inline threshold is prepared without a check, and its commit is refused.

use bytes::Bytes;
use futures::StreamExt;
use loonfs::content_tokens::{mint_content_token, ContentToken};
use loonfs::publish::PreparedContent;
use loonfs::{
    ByteStream, CompleteUploadBody, LoonFs, Namespace, NamespaceId, SharedObjectStore, Writable,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{ObjectStore as _, ObjectStoreExtension};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{FailStore, InjectedError, KeyPredicate, OperationSelector};
use loonfs_types::{
    DisplayName, ErrorCode, ManifestNo, PoolId, RevisionNo, WriterId, ROOT_INODE_ID,
};
use std::sync::Arc;

const SECRET: &str = "content-token-secret";
const BYTES: &[u8] = b"bytes that stage as a content object";

struct Harness {
    _directory: tempfile::TempDir,
    failing: Arc<FailStore<LocalFsStore>>,
    store: SharedObjectStore,
    clock: Arc<ManualClock>,
    writer: LoonFs<Writable>,
}

/// Returns the inline content policy of a runtime that prepares no content
/// inline and stages every inline value that a commit includes.
fn staging_policy() -> loonfs::InlineContentPolicy {
    loonfs::InlineContentPolicy {
        inline_content_threshold_bytes: None,
        inline_content_wal_object_budget_bytes: 1,
        ..Default::default()
    }
}

/// Opens a writer runtime with `inline_content`, over a store that fails
/// the puts that `failing_keys` selects while failures are on.
async fn open(failing_keys: KeyPredicate, inline_content: loonfs::InlineContentPolicy) -> Harness {
    let directory = tempfile::tempdir().expect("tempdir");
    let failing = Arc::new(FailStore::new(
        LocalFsStore::new(directory.path()).expect("create local-fs store"),
        failing_keys,
        OperationSelector::Put,
        InjectedError::Transport("injected".into()),
    ));
    let store: SharedObjectStore = failing.clone();
    let clock = Arc::new(ManualClock::at_system_time());
    let writer = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(WriterId::parse("staging-writer").expect("writer id"))
        .wall_clock(clock.clone())
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            inline_content,
            ..Default::default()
        })
        .build()
        .await
        .expect("build writer");
    Harness {
        _directory: directory,
        failing,
        store,
        clock,
        writer,
    }
}

fn streamed(bytes: &'static [u8]) -> ByteStream {
    futures::stream::iter([Ok(Bytes::from_static(bytes))]).boxed()
}

/// Returns inline content that the publisher stages before the commit,
/// because the content is larger than the WAL object budget.
fn inline(harness: &Harness) -> PreparedContent {
    PreparedContent::inline(
        Bytes::from_static(BYTES),
        harness.store.checksum_algorithm(),
    )
}

/// Interrupts a create of `namespace_id` on the store of `harness`, whose
/// failing puts are the create's, and returns the namespace's pool.
async fn interrupt_create(harness: &Harness, namespace_id: &NamespaceId) -> PoolId {
    harness.failing.fail_all();
    harness
        .writer
        .create_namespace(namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect_err("the injected put fails");
    harness.failing.clear();
    loonfs_core::test_support::load_pool_id(harness.store.as_ref(), namespace_id).await
}

/// Uploads `BYTES` through an upload session of `namespace` and returns the
/// content token of the completed session.
async fn issue_token(harness: &Harness, namespace: &Namespace<Writable>) -> ContentToken {
    let actor = loonfs_test_support::test_actor();
    let upload = namespace
        .create_upload(&actor)
        .await
        .expect("create upload");
    namespace
        .put_upload_content(&upload.upload_id, BYTES, &actor)
        .await
        .expect("upload content");
    let completed = namespace
        .complete_upload(
            &upload.upload_id,
            CompleteUploadBody::ServiceProxied {},
            &actor,
        )
        .await
        .expect("complete upload");
    let evidence = completed.evidence.expect("evidence inside its window");
    mint_content_token(SECRET, &evidence, harness.clock.now_ms()).expect("mint")
}

/// Lists every object under the upload and content prefixes of `pool_id`.
async fn staged_objects(store: &SharedObjectStore, pool_id: &PoolId) -> Vec<String> {
    let mut keys = store
        .list_prefix(&loonfs_objectstore::keys::upload_session_prefix(pool_id))
        .await
        .expect("list upload records");
    keys.extend(
        store
            .list_prefix(&loonfs_objectstore::keys::content_prefix(pool_id))
            .await
            .expect("list content objects"),
    );
    keys
}

/// Runs every way of staging content and every write that stages content,
/// checks that each one returns `code`, and checks that none of them wrote
/// under the pool's prefix.
async fn assert_every_staging_is_refused(
    harness: &Harness,
    namespace: &Namespace<Writable>,
    pool_id: &PoolId,
    code: ErrorCode,
) {
    let actor = loonfs_test_support::test_actor();
    let before = staged_objects(&harness.store, pool_id).await;
    let name = DisplayName::parse("f").expect("display name");
    let results = [
        (
            "prepare_content",
            namespace.prepare_content(BYTES, &actor).await.err(),
        ),
        (
            "prepare_content_stream",
            namespace
                .prepare_content_stream(streamed(BYTES), &actor)
                .await
                .err(),
        ),
        (
            "put_file",
            namespace.put_file("/f", BYTES, &actor).await.err(),
        ),
        (
            "put_file_stream",
            namespace
                .put_file_stream("/f", streamed(BYTES), &actor)
                .await
                .err(),
        ),
        (
            "create_file_by_inode",
            namespace
                .create_file_by_inode(ROOT_INODE_ID, &name, BYTES, &actor)
                .await
                .err(),
        ),
        (
            "put_file_by_inode",
            namespace
                .put_file_by_inode(ROOT_INODE_ID, RevisionNo(1), BYTES, &actor)
                .await
                .err(),
        ),
        (
            "put_file_prepared with inline content",
            namespace
                .put_file_prepared("/f", inline(harness), &actor)
                .await
                .err(),
        ),
    ];
    for (operation, error) in results {
        let error = error.unwrap_or_else(|| panic!("{operation} should be refused"));
        assert_eq!(error.code(), code, "{operation}: {error}");
    }
    assert_eq!(
        staged_objects(&harness.store, pool_id).await,
        before,
        "a refused preparation writes nothing under the pool's prefix"
    );
}

/// Runs every write that stages content, prepares content in each way and
/// commits each value, and reads each file back.
async fn assert_every_staging_commits(harness: &Harness, namespace_id: &NamespaceId) {
    let actor = loonfs_test_support::test_actor();
    let namespace = harness
        .writer
        .open_namespace(namespace_id)
        .expect("open namespace");
    namespace
        .put_file("/put", BYTES, &actor)
        .await
        .expect("put a file");
    namespace
        .put_file_stream("/put-stream", streamed(BYTES), &actor)
        .await
        .expect("put a stream");
    let name = DisplayName::parse("by-inode").expect("display name");
    namespace
        .create_file_by_inode(ROOT_INODE_ID, &name, BYTES, &actor)
        .await
        .expect("create a file by inode");
    let inode_id = namespace.stat("/by-inode").await.expect("stat").inode_id;
    namespace
        .put_file_by_inode(inode_id, RevisionNo(1), BYTES, &actor)
        .await
        .expect("put a file by inode");
    let token = issue_token(harness, &namespace).await;
    let prepared: [(&str, PreparedContent); 4] = [
        (
            "/bytes",
            namespace
                .prepare_content(BYTES, &actor)
                .await
                .expect("prepare content"),
        ),
        (
            "/stream",
            namespace
                .prepare_content_stream(streamed(BYTES), &actor)
                .await
                .expect("prepare a stream"),
        ),
        (
            "/token",
            namespace
                .prepare_content_token(SECRET, &token)
                .await
                .expect("load the catalog")
                .expect("verify the token"),
        ),
        ("/inline", inline(harness)),
    ];
    for (path, content) in prepared {
        namespace
            .put_file_prepared(path, content, &actor)
            .await
            .expect("commit the prepared content");
    }
    for path in [
        "/put",
        "/put-stream",
        "/by-inode",
        "/bytes",
        "/stream",
        "/token",
        "/inline",
    ] {
        let read = harness
            .writer
            .namespace(namespace_id)
            .read_file(path)
            .await
            .expect("read the committed file");
        assert_eq!(read.bytes, BYTES, "{path}");
    }
}

/// Checks that staging content is refused while a create is interrupted,
/// before or after the namespace's member entry is written, and succeeds
/// once a retry finishes the create.
#[tokio::test]
async fn staging_waits_until_an_interrupted_create_finishes() {
    let namespace_id = namespace_id("half");
    let interruptions = [
        (
            "before the member entry",
            KeyPredicate::new(|key| key.contains("/members/")),
        ),
        (
            "after the member entry",
            KeyPredicate::exact(loonfs_objectstore::keys::metadata_manifest_object(
                &namespace_id,
                &ManifestNo(2),
            )),
        ),
    ];
    for (interruption, failing_keys) in interruptions {
        let harness = open(failing_keys, staging_policy()).await;
        let pool_id = interrupt_create(&harness, &namespace_id).await;
        let namespace = harness
            .writer
            .open_namespace(&namespace_id)
            .expect("open namespace");

        assert_every_staging_is_refused(
            &harness,
            &namespace,
            &pool_id,
            ErrorCode::NamespaceNotFound,
        )
        .await;
        assert!(
            staged_objects(&harness.store, &pool_id).await.is_empty(),
            "{interruption}"
        );

        harness
            .writer
            .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("the retry finishes the create");
        assert_every_staging_commits(&harness, &namespace_id).await;
    }
}

/// Checks that staging content is refused in a deleted namespace, and that
/// a token that the namespace issued before its deletion still verifies but
/// its commit is refused.
#[tokio::test]
async fn staging_is_refused_after_a_delete() {
    let harness = open(KeyPredicate::new(|_| false), staging_policy()).await;
    let namespace_id = namespace_id("deleted");
    let actor = loonfs_test_support::test_actor();
    harness
        .writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create namespace");
    let pool_id =
        loonfs_core::test_support::load_pool_id(harness.store.as_ref(), &namespace_id).await;
    let namespace = harness
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let token = issue_token(&harness, &namespace).await;
    namespace.delete().await.expect("delete namespace");

    assert_every_staging_is_refused(&harness, &namespace, &pool_id, ErrorCode::NamespaceDeleted)
        .await;

    let redeemed = namespace
        .prepare_content_token(SECRET, &token)
        .await
        .expect("load the catalog")
        .expect("verify the token");
    let error = namespace
        .put_file_prepared("/token", redeemed, &actor)
        .await
        .expect_err("a deleted namespace refuses the commit");
    assert_eq!(error.code(), ErrorCode::NamespaceDeleted, "{error}");
}

/// Checks that content at or under the default inline threshold is prepared
/// for a namespace that is being created, without a write, and that its
/// commit returns `namespace_not_found`.
#[tokio::test]
async fn inline_content_is_prepared_and_its_commit_is_refused_while_a_create_is_interrupted() {
    let namespace_id = namespace_id("half");
    let harness = open(
        KeyPredicate::exact(loonfs_objectstore::keys::metadata_manifest_object(
            &namespace_id,
            &ManifestNo(2),
        )),
        loonfs::InlineContentPolicy::default(),
    )
    .await;
    let pool_id = interrupt_create(&harness, &namespace_id).await;
    let namespace = harness
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let actor = loonfs_test_support::test_actor();

    let prepared = namespace
        .prepare_content(BYTES, &actor)
        .await
        .expect("prepare inline content");
    assert!(staged_objects(&harness.store, &pool_id).await.is_empty());
    let error = namespace
        .put_file_prepared("/f", prepared, &actor)
        .await
        .expect_err("an initializing namespace refuses the commit");
    assert_eq!(error.code(), ErrorCode::NamespaceNotFound, "{error}");
    assert!(staged_objects(&harness.store, &pool_id).await.is_empty());
}
