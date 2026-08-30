//! Structural acceptance tests for the namespace layout redesign (format
//! spec, "Final design summary"): live visibility comes only from the WAL
//! head, nothing correct depends on listing, and maintenance never touches
//! the head.

use crate::common::{mutation_context, namespace_engine, read_context};
use bytes::Bytes;
use loonfs_api::v0::BeginUploadRequest;
use loonfs_api::AbsolutePath;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_core::content::{prepare_existing_content_ref, store_bytes_as_content};
use loonfs_core::publish::{
    CommitCandidate, CommitRequest, FilesystemOperation, NamespaceCommitEngine, PublishTailOptions,
};
use loonfs_core::{gc_namespace, GcConfig};
use loonfs_core::{BootstrapOptions, MutationContext};
use loonfs_objectstore::keys::{wal_head, wal_segment_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass};
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
    let store = CountingStore::new(
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
    let staged = engine
        .begin_upload(BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    engine
        .upload_content(staged.upload_id(), b"uploaded\n")
        .await
        .expect("upload content");
    engine
        .complete_upload(staged.upload_id())
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
async fn maintenance_never_touches_the_wal_head() {
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

    let head_key = wal_head(&namespace_id);
    let before = store
        .get_with_metadata(&head_key)
        .await
        .expect("read head")
        .expect("head exists");

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
    let staged = engine
        .begin_upload(BeginUploadRequest::ServiceProxied {})
        .await
        .expect("second upload");
    engine
        .upload_content(staged.upload_id(), b"more\n")
        .await
        .expect("second upload content");
    engine
        .complete_upload(staged.upload_id())
        .await
        .expect("second upload complete");

    let after = store
        .get_with_metadata(&head_key)
        .await
        .expect("read head")
        .expect("head exists");
    assert_eq!(after.bytes, before.bytes, "head bytes must be unchanged");
    assert_eq!(
        after.metadata.etag, before.metadata.etag,
        "head object identity must be unchanged"
    );
}
