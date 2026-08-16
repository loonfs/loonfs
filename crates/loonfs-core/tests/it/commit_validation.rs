//! Publish-path validation: content admission, race guards evaluated against
//! what a request's earlier operations did, and the all-or-nothing rule that
//! makes a multi-operation request one commit.

#![allow(clippy::panic)]
// These integration tests use panic in unexpected match arms for precise diagnostics.

use crate::common::commit_split_support::*;
use crate::common::namespace_engine;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_api::{
    v0::{FilesystemChange, UploadSessionStatus},
    AbsolutePath, ChangeSeq, CommitId, DeleteDirectoryBehavior, DestinationBehavior, DisplayName,
    NamespaceId, RevisionNo,
};
use loonfs_core::content::{
    mint_content_token, store_bytes_as_content, verify_content_token, ContentTokenError,
};
use loonfs_core::limits::CONTENT_RECEIPT_TTL_MS;
use loonfs_core::publish::{CommitCandidate, CommitRequest, FilesystemOperation};
use loonfs_core::{Error as CoreError, ErrorCode};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::OperationClass;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;

/// A store that fails any read of the content-store keyspace, so a test can
/// assert that validation never went looking for content at all.
#[derive(Debug)]
struct ContentStoreAccessLimitStore {
    inner: LocalFsStore,
    content_store_accesses: AtomicUsize,
    max_content_store_accesses: usize,
}

impl ContentStoreAccessLimitStore {
    fn new(root: impl AsRef<Path>, max_content_store_accesses: usize) -> Self {
        Self {
            inner: LocalFsStore::new(root.as_ref()).expect("store"),
            content_store_accesses: AtomicUsize::new(0),
            max_content_store_accesses,
        }
    }

    fn content_store_access_count(&self) -> usize {
        self.content_store_accesses.load(Ordering::SeqCst)
    }

    fn record_content_store_access(&self, key: &str) -> Result<(), ObjectStoreError> {
        if !key.starts_with("content-stores/") {
            return Ok(());
        }

        let previous = self.content_store_accesses.fetch_add(1, Ordering::SeqCst);
        if previous >= self.max_content_store_accesses {
            return Err(ObjectStoreError::transport(
                key,
                "unexpected content-store descriptor access",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for ContentStoreAccessLimitStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.record_content_store_access(key)?;
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.record_content_store_access(key)?;
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

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_from_stream(prefix, start_after)
    }
}

fn put_file(absolute_path: &str, content_ref: loonfs_api::ContentRef) -> FilesystemOperation {
    FilesystemOperation::PutFile {
        path: AbsolutePath::parse(absolute_path).expect("path"),
        content_ref,
        behavior: DestinationBehavior::NoReplace,
        expected_revision_no: None,
    }
}

fn create_dir(absolute_path: &str) -> FilesystemOperation {
    FilesystemOperation::CreateDirectory {
        path: AbsolutePath::parse(absolute_path).expect("path"),
        parents: false,
    }
}

fn delete_path(absolute_path: &str) -> FilesystemOperation {
    FilesystemOperation::DeletePath {
        path: AbsolutePath::parse(absolute_path).expect("path"),
        behavior: DeleteDirectoryBehavior::NonRecursive,
        expected_inode_id: None,
    }
}

fn commit_id(value: &str) -> CommitId {
    CommitId::parse(value).expect("valid commit id")
}

#[tokio::test]
async fn path_put_file_without_admission_fails_without_reading_content() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"hello")
        .await
        .expect("stage content");

    store.reset();
    let responses = publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![CommitCandidate::new(CommitRequest::single(
            commit_id("put-cold-content"),
            loonfs_test_support::test_actor(),
            None,
            put_file("/docs/hello.txt", content.into_content_ref()),
        ))],
        &context,
    )
    .await;

    assert_eq!(
        responses[0]
            .as_ref()
            .expect_err("unadmitted path put must fail")
            .code(),
        ErrorCode::ContentNotPrepared
    );
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[tokio::test]
async fn path_batch_rejects_repeated_unadmitted_content_without_reading_it() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let content = store_bytes_as_content(&store, &namespace_id, b"shared")
        .await
        .expect("stage content");

    store.reset();
    let responses = publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![
            CommitCandidate::new(CommitRequest::single(
                commit_id("put-shared-a"),
                loonfs_test_support::test_actor(),
                None,
                put_file("/docs/a.txt", content.content_ref().clone()),
            )),
            CommitCandidate::new(CommitRequest::single(
                commit_id("put-shared-b"),
                loonfs_test_support::test_actor(),
                None,
                put_file("/docs/b.txt", content.into_content_ref()),
            )),
        ],
        &context,
    )
    .await;

    assert!(responses.iter().all(|response| {
        response
            .as_ref()
            .is_err_and(|error| error.code() == ErrorCode::ContentNotPrepared)
    }));
    assert_eq!(store.count(OperationClass::Read), 0);
}

#[tokio::test]
async fn valid_content_admission_skips_durable_content_validation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = content_blob_counting_store(temp_dir.path());
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();

    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    // A receipt exists only for a session the store already says completed,
    // so the token this test admits has to come from a real upload.
    let engine = namespace_engine(&store, &namespace_id, &context);
    let upload = engine
        .begin_upload(loonfs_api::v0::BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    engine
        .upload_content(upload.upload_id(), b"admitted")
        .await
        .expect("stage content");
    let completed = engine
        .complete_upload_prepared(upload.upload_id())
        .await
        .expect("complete upload");
    let content_ref = completed
        .response
        .content_ref()
        .expect("completed content ref")
        .clone();
    let token = mint_content_token(
        "test-content-token-secret",
        completed
            .receipt
            .as_ref()
            .expect("a completed session mints a receipt"),
        context.now_ms,
    )
    .expect("mint token");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared = verify_content_token(
        "test-content-token-secret",
        &catalog,
        &token,
        context.now_ms,
    )
    .expect("verify token");

    store.reset();
    let responses = publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![CommitCandidate::prepared(
            CommitRequest::single(
                commit_id("put-admitted-content"),
                loonfs_test_support::test_actor(),
                None,
                put_file("/docs/admitted.txt", content_ref),
            ),
            vec![prepared],
        )],
        &context,
    )
    .await;

    responses[0].as_ref().expect("admitted put commits");
    // A live admission is the fast path: no content read at all.
    assert_eq!(store.count(OperationClass::Read), 0);
}

/// A later candidate in one batch resolves against what the earlier one did,
/// so a delete of a path the earlier candidate renamed away finds nothing.
#[tokio::test]
async fn a_later_batch_candidate_observes_the_earlier_one() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/readme.txt",
        b"hello",
        &context,
        Some("seed-child-name-is"),
    )
    .await
    .expect("seed file");
    let file_inode = resolve_path(&store, &namespace_id, "/docs/readme.txt")
        .await
        .expect("resolve file")
        .inode_id;

    let responses = submit_commits_batch(
        &store,
        &namespace_id,
        vec![
            CommitRequest::single(
                commit_id("move-before-child-name-check"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::MovePath {
                    from_path: AbsolutePath::parse("/docs/readme.txt").expect("path"),
                    to_path: AbsolutePath::parse("/docs/moved.txt").expect("path"),
                    behavior: DestinationBehavior::NoReplace,
                },
            ),
            CommitRequest::single(
                commit_id("delete-with-stale-binding"),
                loonfs_test_support::test_actor(),
                None,
                FilesystemOperation::DeletePath {
                    path: AbsolutePath::parse("/docs/readme.txt").expect("path"),
                    behavior: DeleteDirectoryBehavior::NonRecursive,
                    expected_inode_id: Some(file_inode),
                },
            ),
        ],
        &context,
    )
    .await;

    assert_eq!(
        responses[0].as_ref().expect("rename").committed_seq,
        ChangeSeq(2)
    );
    let error = responses[1]
        .as_ref()
        .expect_err("the moved-away path no longer resolves");
    assert_eq!(error.code(), ErrorCode::PathNotFound);
}

/// The directory-empty rule is evaluated against what the earlier candidate
/// did, so a delete of a directory a earlier candidate just filled fails.
#[tokio::test]
async fn a_directory_delete_observes_an_earlier_batch_candidate() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    submit_operation(
        &store,
        &namespace_id,
        commit_id("seed-empty-dir"),
        create_dir("/docs"),
        &context,
    )
    .await
    .expect("seed docs");
    let content = store_bytes_as_content(&store, &namespace_id, b"child")
        .await
        .expect("stage content");

    let responses = submit_commits_batch(
        &store,
        &namespace_id,
        vec![
            CommitRequest::single(
                commit_id("create-child-before-empty-check"),
                loonfs_test_support::test_actor(),
                None,
                put_file("/docs/child.txt", content.into_content_ref()),
            ),
            CommitRequest::single(
                commit_id("delete-dir-with-stale-empty-check"),
                loonfs_test_support::test_actor(),
                None,
                delete_path("/docs"),
            ),
        ],
        &context,
    )
    .await;

    assert_eq!(
        responses[0].as_ref().expect("create child").committed_seq,
        ChangeSeq(2)
    );
    let error = responses[1]
        .as_ref()
        .expect_err("directory is no longer empty");
    assert_eq!(error.code(), ErrorCode::DirectoryNotEmpty);
}

#[tokio::test]
async fn mutation_paths_reject_invalid_display_names() {
    assert!(DisplayName::parse("a/b").is_err());
    assert!(DisplayName::parse(".").is_err());
}

#[tokio::test]
async fn restore_revision_does_not_revalidate_retained_content_before_publish() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");

    let first = store_bytes_as_content(&store, &namespace_id("demo"), b"first")
        .await
        .expect("stage first");
    submit_operation(
        &store,
        &namespace_id("demo"),
        commit_id("restore-create"),
        put_file("/restore.txt", first.content_ref().clone()),
        &context,
    )
    .await
    .expect("create file");

    let second = store_bytes_as_content(&store, &namespace_id("demo"), b"second")
        .await
        .expect("stage second");
    submit_operation(
        &store,
        &namespace_id("demo"),
        commit_id("restore-replace"),
        FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/restore.txt").expect("path"),
            content_ref: second.into_content_ref(),
            behavior: DestinationBehavior::Replace,
            expected_revision_no: None,
        },
        &context,
    )
    .await
    .expect("replace file");

    store
        .delete(first.object_key())
        .await
        .expect("delete first content");

    let response = submit_operation(
        &store,
        &namespace_id("demo"),
        commit_id("restore-missing-content"),
        FilesystemOperation::RestoreRevision {
            path: AbsolutePath::parse("/restore.txt").expect("path"),
            source_revision_no: RevisionNo(1),
        },
        &context,
    )
    .await
    .expect("restore trusts retained content metadata");
    assert_eq!(response.committed_seq, ChangeSeq(3));
}

#[tokio::test]
async fn metadata_only_mutation_does_not_validate_content_store_refs() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/delete-me.txt",
        b"hello",
        &context,
        Some("seed-metadata-only-delete"),
    )
    .await
    .expect("seed file");

    let guarded_store = ContentStoreAccessLimitStore::new(temp_dir.path(), 0);
    let response = submit_operation(
        &guarded_store,
        &namespace_id("demo"),
        commit_id("metadata-only-delete"),
        delete_path("/docs/delete-me.txt"),
        &context,
    )
    .await
    .expect("metadata-only delete should not perform content validation");

    assert_eq!(response.committed_seq, ChangeSeq(2));
    assert_eq!(
        guarded_store.content_store_access_count(),
        0,
        "the namespace's content store is a field in its head; metadata-only validation must not touch the content-store keyspace at all",
    );
}

/// Validation precedes content coverage, uniformly (owner decision with the
/// single-pass commit preparation): a put whose caller-supplied revision
/// guard is stale answers for the stale guard even when its content proof is
/// also missing, and nothing is read from the content store either way.
#[tokio::test]
async fn a_guarded_put_reports_the_stale_revision_before_missing_content_without_content_reads() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/replace.txt",
        b"first",
        &context,
        Some("seed-replace"),
    )
    .await
    .expect("seed replace target");

    let guarded_store = ContentStoreAccessLimitStore::new(temp_dir.path(), 0);
    let missing_content = content_ref("missing-content");
    let error = publish_namespace_commits_batch(
        &guarded_store,
        &namespace_id("demo"),
        vec![CommitCandidate::new(CommitRequest::single(
            commit_id("replace-stale-missing-content"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: AbsolutePath::parse("/docs/replace.txt").expect("path"),
                content_ref: missing_content.clone(),
                behavior: DestinationBehavior::Replace,
                expected_revision_no: Some(RevisionNo(99)),
            },
        ))],
        &context,
    )
    .await
    .into_iter()
    .next()
    .expect("one result")
    .expect_err("the stale revision guard should win before unprepared content");
    assert_eq!(error.code(), ErrorCode::StaleRevision);
    assert!(matches!(
        error,
        CoreError::CommitValidation(
            loonfs_core::commit::CommitValidationError::ReplaceFileBaseRevisionMismatch {
                expected: RevisionNo(99),
                actual: Some(RevisionNo(1)),
                ..
            }
        )
    ));
    assert_eq!(guarded_store.content_store_access_count(), 0);
}

#[tokio::test]
async fn restore_revision_missing_source_is_revision_not_found() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id("demo"), &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id("demo"),
        "/docs/restore.txt",
        b"first",
        &context,
        Some("seed-restore"),
    )
    .await
    .expect("seed restore target");

    let error = submit_operation(
        &store,
        &namespace_id("demo"),
        commit_id("restore-missing-source"),
        FilesystemOperation::RestoreRevision {
            path: AbsolutePath::parse("/docs/restore.txt").expect("path"),
            source_revision_no: RevisionNo(99),
        },
        &context,
    )
    .await
    .expect_err("missing restore source should fail");
    assert_eq!(error.code(), ErrorCode::RevisionNotFound);
}

/// A directory and the files under it land in one commit: the puts resolve
/// against the directory the first operation creates, and the change feed
/// reports one committed change whose events follow operation order.
#[tokio::test]
async fn a_batch_creates_a_directory_and_writes_into_it_in_one_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");
    let first = store_bytes_as_content(&store, &namespace_id, b"first")
        .await
        .expect("stage first");
    let second = store_bytes_as_content(&store, &namespace_id, b"second")
        .await
        .expect("stage second");

    let response = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("reports-batch"),
            actor: loonfs_test_support::test_actor(),
            message: Some("import reports".to_owned()),
            operations: vec![
                create_dir("/reports"),
                put_file("/reports/a.txt", first.content_ref().clone()),
                put_file("/reports/b.txt", second.content_ref().clone()),
            ],
        },
        &context,
    )
    .await
    .expect("the batch commits");
    assert_eq!(response.committed_seq, ChangeSeq(1));

    for path in ["/reports", "/reports/a.txt", "/reports/b.txt"] {
        resolve_path(&store, &namespace_id, path)
            .await
            .expect("every operation of the batch is visible");
    }

    let changes = list_changes_after(&store, &namespace_id, ChangeSeq(0))
        .await
        .expect("read the change feed");
    assert_eq!(changes.changes.len(), 1, "the batch is one logical commit");
    let change = &changes.changes[0];
    assert_eq!(change.commit_id, commit_id("reports-batch"));
    assert_eq!(change.message.as_deref(), Some("import reports"));
    let names = change
        .events
        .iter()
        .map(|event| match event {
            FilesystemChange::DirectoryCreated { display_name, .. }
            | FilesystemChange::FileCreated { display_name, .. } => {
                display_name.as_str().to_owned()
            }
            other => panic!("unexpected event: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["reports", "a.txt", "b.txt"]);
}

/// A request stops at its first failing operation and nothing it would have
/// written becomes visible. The same commit id then commits a corrected
/// batch, because the failed attempt left no receipt behind.
#[tokio::test]
async fn a_batch_that_stops_commits_nothing_and_names_the_operation() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    let error = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("half-good-batch"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                create_dir("/first"),
                delete_path("/missing"),
                create_dir("/third"),
            ],
        },
        &context,
    )
    .await
    .expect_err("the delete cannot resolve");
    assert_eq!(error.code(), ErrorCode::PathNotFound);
    assert_eq!(
        error
            .details()
            .expect("a stopped batch carries details")
            .operation_index,
        Some(1)
    );

    for path in ["/first", "/third"] {
        resolve_path(&store, &namespace_id, path)
            .await
            .expect_err("no operation of a stopped batch is visible");
    }

    let response = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("half-good-batch"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![create_dir("/first"), create_dir("/third")],
        },
        &context,
    )
    .await
    .expect("a corrected batch commits under the same commit id");
    assert_eq!(response.committed_seq, ChangeSeq(1));
}

/// Reusing a commit id replays the original receipt for the same request and
/// conflicts for a different one, and a one-operation request is the same
/// request as a one-element batch.
#[tokio::test]
async fn a_reused_commit_id_replays_the_receipt_or_conflicts() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    let batch = |commit: &str| CommitRequest {
        commit_id: commit_id(commit),
        actor: loonfs_test_support::test_actor(),
        message: None,
        operations: vec![create_dir("/a"), create_dir("/b")],
    };
    let first = submit_commit(&store, &namespace_id, batch("replayed-batch"), &context)
        .await
        .expect("the batch commits");

    let replayed = submit_commit(&store, &namespace_id, batch("replayed-batch"), &context)
        .await
        .expect("the same batch replays");
    assert_eq!(replayed.committed_seq, first.committed_seq);

    let error = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("replayed-batch"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![create_dir("/a"), create_dir("/c")],
        },
        &context,
    )
    .await
    .expect_err("different operations under the same commit id conflict");
    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);

    // The convenience form and a one-element list are the same request, so
    // either style replays the other's receipt.
    let convenience = submit_operation(
        &store,
        &namespace_id,
        commit_id("one-operation"),
        create_dir("/docs"),
        &context,
    )
    .await
    .expect("the convenience call commits");
    let as_batch = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("one-operation"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![create_dir("/docs")],
        },
        &context,
    )
    .await
    .expect("the one-element batch replays the same receipt");
    assert_eq!(as_batch.committed_seq, convenience.committed_seq);
}

/// Operation order is the contract: creating then deleting a path leaves it
/// gone, and deleting then creating leaves it present.
#[tokio::test]
async fn operation_order_decides_the_outcome() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");

    submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("create-then-delete"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![create_dir("/x"), delete_path("/x")],
        },
        &context,
    )
    .await
    .expect("create then delete commits");
    resolve_path(&store, &namespace_id, "/x")
        .await
        .expect_err("the delete ran after the create");

    submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("seed-y"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![create_dir("/y")],
        },
        &context,
    )
    .await
    .expect("seed a path to delete");
    submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("delete-then-create"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![delete_path("/y"), create_dir("/y")],
        },
        &context,
    )
    .await
    .expect("delete then create commits");
    resolve_path(&store, &namespace_id, "/y")
        .await
        .expect("the create ran after the delete");
}

/// A caller's revision guard is evaluated against the state its own
/// operation sees, which includes the revision an earlier operation of the
/// same request wrote.
#[tokio::test]
async fn a_revision_guard_observes_an_earlier_operation_of_the_same_request() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap namespace");
    write_file_bytes(
        &store,
        &namespace_id,
        "/docs/guarded.txt",
        b"first",
        &context,
        Some("seed-guarded"),
    )
    .await
    .expect("seed guarded file");
    let second = store_bytes_as_content(&store, &namespace_id, b"second")
        .await
        .expect("stage second");
    let third = store_bytes_as_content(&store, &namespace_id, b"third")
        .await
        .expect("stage third");

    let replace =
        |content_ref: loonfs_api::ContentRef, expected: u64| FilesystemOperation::PutFile {
            path: AbsolutePath::parse("/docs/guarded.txt").expect("path"),
            content_ref,
            behavior: DestinationBehavior::Replace,
            expected_revision_no: Some(RevisionNo(expected)),
        };

    // The second put guards on revision 2, which only exists because the
    // first put of this same request created it.
    submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("guarded-chain"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                replace(second.content_ref().clone(), 1),
                replace(third.content_ref().clone(), 2),
            ],
        },
        &context,
    )
    .await
    .expect("the second guard sees the first write");

    // Guarding on the revision the request started from is stale by the time
    // the second operation runs, so the whole request stops there.
    let error = submit_commit(
        &store,
        &namespace_id,
        CommitRequest {
            commit_id: commit_id("stale-guarded-chain"),
            actor: loonfs_test_support::test_actor(),
            message: None,
            operations: vec![
                replace(second.into_content_ref(), 3),
                replace(third.into_content_ref(), 3),
            ],
        },
        &context,
    )
    .await
    .expect_err("the second guard is stale once the first write lands");
    assert_eq!(error.code(), ErrorCode::StaleRevision);
    assert_eq!(
        error
            .details()
            .expect("a stopped batch carries details")
            .operation_index,
        Some(1)
    );
}

/// The end-to-end promise: a client that lost its commit response reads the
/// session again, gets a fresh receipt for bytes that never moved, and
/// publishes with it. The receipt it was holding is refused at admission
/// first, so the re-mint is doing real work rather than papering over a
/// token that would have been accepted anyway.
#[tokio::test]
async fn a_re_minted_receipt_publishes_after_the_first_one_expired() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = namespace_id("demo");
    let context = mutation_context();
    bootstrap_namespace(&store, &namespace_id, &context, false)
        .await
        .expect("bootstrap");
    let engine = namespace_engine(&store, &namespace_id, &context);
    let upload = engine
        .begin_upload(loonfs_api::v0::BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    let staged = engine
        .upload_content(upload.upload_id(), b"re-minted")
        .await
        .expect("stage content");
    let completed = engine
        .complete_upload_prepared(upload.upload_id())
        .await
        .expect("complete upload");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");

    // The receipt the completion handed back, past its life.
    let first = mint_content_token(
        "test-content-token-secret",
        completed.receipt.as_ref().expect("completion mints"),
        0,
    )
    .expect("mint");
    let refused = verify_content_token(
        "test-content-token-secret",
        &catalog,
        &first,
        CONTENT_RECEIPT_TTL_MS + 1,
    )
    .expect_err("an expired receipt is refused at admission");
    assert_eq!(refused, ContentTokenError::Expired);

    // Reading the session mints another one for the same durable bytes.
    let (status, receipt) = engine
        .read_upload_status(upload.upload_id())
        .await
        .expect("read upload status");
    match status.status {
        UploadSessionStatus::Completed { content_ref, .. } => {
            assert_eq!(content_ref, staged.content_ref);
        }
        other => panic!("expected a completed session, got {other:?}"),
    }
    let re_minted = mint_content_token(
        "test-content-token-secret",
        receipt.as_ref().expect("the status read re-mints"),
        CONTENT_RECEIPT_TTL_MS + 1,
    )
    .expect("re-mint");
    let prepared = verify_content_token(
        "test-content-token-secret",
        &catalog,
        &re_minted,
        CONTENT_RECEIPT_TTL_MS + 2,
    )
    .expect("a re-minted receipt is admitted");

    let responses = publish_namespace_commits_batch(
        &store,
        &namespace_id,
        vec![CommitCandidate::prepared(
            CommitRequest::single(
                commit_id("put-re-minted-content"),
                loonfs_test_support::test_actor(),
                None,
                put_file("/docs/re-minted.txt", staged.content_ref),
            ),
            vec![prepared],
        )],
        &context,
    )
    .await;
    responses
        .into_iter()
        .next()
        .expect("one response")
        .expect("publishing with a re-minted receipt succeeds");
}
