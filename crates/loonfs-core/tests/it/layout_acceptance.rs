//! Namespace layout acceptance tests: numbered WAL objects make commits visible,
//! readers discover state without listing, and maintenance publishes manifests
//! without advancing the head sequence.

use crate::common::{mutation_context, namespace_engine, read_context};
use bytes::Bytes;
use loonfs_api::AbsolutePath;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_core::content::{prepare_existing_content_ref, store_bytes_as_content};
use loonfs_core::publish::{
    CommitCandidate, CommitRequest, FilesystemOperation, NamespaceCommitEngine, PublishTailOptions,
};
use loonfs_core::{gc_namespace, GcConfig};
use loonfs_core::{BootstrapOptions, MutationContext, ResolvedUploadCompletion};
use loonfs_objectstore::keys::wal_segment_prefix;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::{KeyPredicate, OperationClass, RecordingStore};
use tempfile::tempdir;

async fn put_file<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
    context: &MutationContext,
) {
    let content = store_bytes_as_content(store, namespace_id, bytes)
        .await
        .expect("stage content");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(store, namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared = prepare_existing_content_ref(store, &catalog, content.into_content_ref())
        .await
        .expect("prepare existing content");
    let content_ref = prepared.content_ref().clone();
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            vec![CommitCandidate::prepared(
                CommitRequest::single(
                    loonfs_api::CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(absolute_path).expect("path"),
                        content_ref,
                        behavior: loonfs_api::DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
                vec![prepared],
            )],
            context,
            &PublishTailOptions::default(),
        )
        .await
        .results
        .pop()
        .expect("one result")
        .expect("put file");
}

#[tokio::test]
async fn reads_commits_and_change_feed_never_list() {
    let temp_dir = tempdir().expect("tempdir");
    let store = RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = mutation_context("acceptance", 1_000);
    let engine = namespace_engine(&store, &namespace_id, &context);
    engine
        .bootstrap_namespace(BootstrapOptions::default())
        .await
        .expect("bootstrap");

    // Foreign junk in the segment collection must be invisible to every
    // hot path.
    store
        .put_if_absent(
            &format!("{}zz-junk.tmp", wal_segment_prefix(&namespace_id)),
            Bytes::from_static(b"junk"),
        )
        .await
        .expect("write junk");

    let baseline = store.count(OperationClass::List);
    let staged = engine.begin_upload().await.expect("begin upload");
    engine
        .upload_content(staged.upload_id(), b"uploaded\n")
        .await
        .expect("upload content");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");
    engine
        .complete_upload(
            &catalog,
            staged.upload_id(),
            ResolvedUploadCompletion::KnownContent,
        )
        .await
        .expect("complete upload");
    put_file(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
    )
    .await;

    let ctx = read_context(&store, &namespace_id).await;
    engine
        .resolve_path(
            "/docs/hello.txt",
            loonfs_api::options::StatPathOptions::default(),
            &ctx,
        )
        .await
        .expect("stat");
    engine
        .list_path_page(
            "/docs",
            loonfs_api::PageRequest {
                limit: loonfs_test_support::ids::page_limit(1024),
                cursor: None,
            },
            loonfs_api::options::ListPathEntriesOptions::default(),
            &ctx,
        )
        .await
        .expect("list directory");
    let bytes = engine
        .get_file("/docs/hello.txt", &ctx, None)
        .await
        .expect("read");
    assert_eq!(bytes.bytes, b"hello\n");
    let changes = engine
        .list_changes_after(ChangeSeq(0), loonfs_test_support::ids::page_limit(1024))
        .await
        .expect("change feed");
    assert!(!changes.changes.is_empty());

    assert_eq!(
        store.count(OperationClass::List),
        baseline,
        "read, commit, upload, and change-feed paths must not LIST"
    );
}

#[tokio::test]
async fn maintenance_preserves_namespace_identity_and_writer() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = mutation_context("acceptance", 1_000);
    let engine = namespace_engine(&store, &namespace_id, &context);
    engine
        .bootstrap_namespace(BootstrapOptions::default())
        .await
        .expect("bootstrap");
    put_file(
        &store,
        &namespace_id,
        "/docs/hello.txt",
        b"hello\n",
        &context,
    )
    .await;

    let before = loonfs_core::control::load_namespace_head_control(&store, &namespace_id)
        .await
        .expect("namespace state")
        .state;

    engine
        .create_checkpoint("test-pin".to_owned(), None)
        .await
        .expect("checkpoint");
    engine
        .advance_retention_floor()
        .await
        .expect("advance floor");
    gc_namespace(&store, &namespace_id, &GcConfig::default(), &context)
        .await
        .expect("gc pass");
    let staged = engine.begin_upload().await.expect("second upload");
    engine
        .upload_content(staged.upload_id(), b"more\n")
        .await
        .expect("second upload content");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");
    engine
        .complete_upload(
            &catalog,
            staged.upload_id(),
            ResolvedUploadCompletion::KnownContent,
        )
        .await
        .expect("second upload complete");

    let after = loonfs_core::control::load_namespace_head_control(&store, &namespace_id)
        .await
        .expect("namespace state")
        .state;
    assert_eq!(after.namespace_id, before.namespace_id);
    assert_eq!(after.content_store_id, before.content_store_id);
    assert_eq!(after.created_at_ms, before.created_at_ms);
    assert_eq!(after.fork_basis, before.fork_basis);
    assert_eq!(after.writer_epoch, before.writer_epoch);
    assert_eq!(after.writer, before.writer);
    assert_eq!(after.seq, before.seq);
    assert_eq!(after.head_commit_id, before.head_commit_id);
    assert_eq!(after.next_inode_id, before.next_inode_id);
}
