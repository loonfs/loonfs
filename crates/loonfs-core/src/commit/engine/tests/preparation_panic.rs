//! A panic in one commit's preparation, which fails that commit alone.

use super::*;
use crate::authorize::{Authorizer, ReadAccess};
use crate::wal::is_retryable_wal_publish;
use bytes::Bytes;
use loonfs_test_support::stores::{OperationKind, PanicStore};
use loonfs_types::options::AttributeInclusion;
use loonfs_types::{AbsolutePath, Checksum, DestinationBehavior, PathEntryKind, WriterId};

/// A store that panics on a content object's assembly while the test arms
/// it.
type Store = Arc<PanicStore<LocalFsStore>>;

fn request(commit_id: &str, operations: Vec<FilesystemOperation>) -> CommitIntent {
    CommitIntent {
        commit_id: CommitId::parse(commit_id).expect("commit id"),
        actor_id: loonfs_test_support::test_actor(),
        subject: None,
        message: None,
        preconditions: Vec::new(),
        operations,
    }
}

fn path(path: &str) -> AbsolutePath {
    AbsolutePath::parse(path).expect("path")
}

fn create_directory(at: &str) -> FilesystemOperation {
    FilesystemOperation::CreateDirectory {
        path: path(at),
        parents: false,
    }
}

fn append(at: &str, bytes: &[u8]) -> FilesystemOperation {
    FilesystemOperation::AppendFile {
        path: path(at),
        inline_content: bytes.to_vec(),
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

/// Creates a namespace with `/a`, whose bytes are in the WAL tail, and
/// `/b`, a copy of `/a` with one more byte appended to the chain they
/// share. An append to `/a` then starts a new chain from `/a`'s revision,
/// and its preparation writes `/a`'s bytes from the tail to a content
/// object, which the store panics on while it's armed.
async fn setup() -> (
    tempfile::TempDir,
    Store,
    NamespaceCommitEngine<Store>,
    WriterAt,
) {
    let directory = tempfile::tempdir().expect("directory");
    let store = Arc::new(PanicStore::panicking_on(
        LocalFsStore::new(directory.path()).expect("store"),
        |operation| matches!(operation.kind(), OperationKind::Assemble { .. }),
    ));
    let namespace_id = NamespaceId::parse("demo").expect("namespace");
    let context = WriterAt {
        writer_id: WriterId::parse("writer").expect("writer"),
        now_ms: 1_000,
    };
    crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
        .create()
        .await
        .expect("bootstrap");
    let mut engine = NamespaceCommitEngine::new(
        crate::test_support::namespace_store(Arc::clone(&store), &namespace_id),
        context.writer_id.clone(),
    );
    let value = InlineContent::new(
        ContentId::generate(),
        Bytes::from_static(b"hello"),
        loonfs_types::ChecksumAlgorithm::Crc64nvme,
    );
    let put = FilesystemOperation::PutFile {
        path: path("/a"),
        content: loonfs_types::FileContent::Ref(value.content_ref().clone()),
        behavior: DestinationBehavior::NoReplace,
        expected_inode_id: None,
        expected_revision_no: None,
    };
    let copy = FilesystemOperation::CopyPath {
        source_path: path("/a"),
        destination_path: path("/b"),
        precondition: Default::default(),
    };
    for candidate in [
        CommitCandidate::with_inline_content(request("put", vec![put]), Vec::new(), vec![value]),
        CommitCandidate::new(request("copy", vec![copy])),
        CommitCandidate::new(request("grow", vec![append("/b", b"!")])),
    ] {
        publish_one(&mut engine, &context, candidate)
            .await
            .expect("setup commit");
    }
    (directory, store, engine, context)
}

async fn publish_one(
    engine: &mut NamespaceCommitEngine<Store>,
    context: &WriterAt,
    candidate: CommitCandidate,
) -> Result<Commit> {
    engine
        .publish_batch([candidate], &context.clock())
        .await
        .results
        .pop()
        .expect("one result")
}

/// The commit whose preparation panics while the store is armed: it
/// creates `/made`, then appends to `/a`.
fn panicking() -> CommitCandidate {
    CommitCandidate::new(request(
        "panics",
        vec![create_directory("/made"), append("/a", b"?")],
    ))
}

fn assert_preparation_panicked(result: &Result<Commit>) {
    let error = result.as_ref().expect_err("the preparation panicked");
    assert_eq!(error.code(), ErrorCode::ServerError);
    assert!(!error.code().is_stale_plan());
    assert!(!is_retryable_wal_publish(result));
    assert_matches!(
        error,
        CoreError::CommitPreparationPanicked(panic) if panic.starts_with("injected panic")
    );
}

/// Checks that a panic in one commit's preparation fails that commit, and a
/// later candidate with its commit ID, and that the batch commits its other
/// candidates at consecutive sequences as if the failed commit had never
/// been in it. A candidate that needs the failed commit's directory doesn't
/// find it. The panic released the content write's permit, and the failed
/// commit, sent again, fails the same way while the store panics and
/// commits once the store stops.
#[tokio::test]
async fn a_panic_in_one_commits_preparation_fails_only_that_commit() {
    let (_directory, store, mut engine, context) = setup().await;
    let head = engine
        .namespace
        .load_current_metadata_view()
        .await
        .expect("view")
        .head()
        .seq;
    store.arm();
    let results = engine
        .publish_batch(
            [
                create_dir("before", "before"),
                panicking(),
                panicking(),
                CommitCandidate::new(request("dependent", vec![create_directory("/made/child")])),
                create_dir("after", "after"),
            ],
            &context.clock(),
        )
        .await
        .results;
    assert_eq!(store.attempts(), 1, "the panicking commit is prepared once");
    let committed = |index: usize| {
        results[index]
            .as_ref()
            .expect("a neighbor commits")
            .committed_seq
    };
    assert_eq!(committed(0), CommitSeq(head.0 + 1));
    assert_eq!(committed(4), CommitSeq(head.0 + 2));
    assert_preparation_panicked(&results[1]);
    assert_preparation_panicked(&results[2]);
    assert_eq!(
        results[3].as_ref().expect_err("no `/made`").code(),
        ErrorCode::PathNotFound
    );
    assert_eq!(
        engine.namespace.content_writes.counts(),
        crate::PermitCounts::default()
    );

    let wal_objects = wal_object_count(store.inner(), &engine.namespace.namespace_id).await;
    let alone = publish_one(&mut engine, &context, panicking()).await;
    assert_preparation_panicked(&alone);
    assert_eq!(
        wal_object_count(store.inner(), &engine.namespace.namespace_id).await,
        wal_objects,
        "a batch whose only commit panics writes no WAL object"
    );

    store.disarm();
    publish_one(&mut engine, &context, panicking())
        .await
        .expect("the same commit once the store stops panicking");
    let view = engine
        .namespace
        .load_current_metadata_view()
        .await
        .expect("view");
    let access = ReadAccess::live(Authorizer::Service);
    view.resolve_path("/made", AttributeInclusion::Omit, &access)
        .await
        .expect("the directory");
    let entry = view
        .resolve_path("/a", AttributeInclusion::Omit, &access)
        .await
        .expect("the file");
    assert_matches!(
        entry.kind,
        PathEntryKind::File { content_ref: Some(content_ref), .. }
            if content_ref.checksum == Checksum::crc64nvme(b"hello?")
    );
}
