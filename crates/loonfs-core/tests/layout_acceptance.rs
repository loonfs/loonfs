//! Structural acceptance tests for the namespace layout redesign (format
//! spec, "Final design summary"): live visibility comes only from the WAL
//! head, nothing correct depends on listing, and maintenance never touches
//! the head.

mod common;

use bytes::Bytes;
use common::{mutation_context, namespace_engine, read_context};
use loonfs_api::v0::BeginUploadRequest;
use loonfs_api::AbsolutePath;
use loonfs_api::{ChangeSeq, NamespaceId};
use loonfs_core::content::{prepare_existing_content_ref, store_bytes_as_content};
use loonfs_core::gc::{gc_namespace, GcConfig};
use loonfs_core::publish::{
    NamespaceCommitEngine, NamespaceMutationCandidate, PathMutationIntent, PublishTailOptions,
};
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
    let prepared = prepare_existing_content_ref(store, &catalog, content.content_ref)
        .await
        .expect("prepare existing content");
    let content_ref = prepared.content_ref().clone();
    NamespaceCommitEngine::new(namespace_id.clone())
        .publish_batch(
            store,
            vec![NamespaceMutationCandidate::path_prepared(
                PathMutationIntent::PutFile {
                    commit_id: loonfs_api::CommitId::generate(),
                    absolute_path: AbsolutePath::parse(absolute_path).expect("path"),
                    content_ref,
                    behavior: loonfs_api::DestinationBehavior::NoReplace,
                },
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

/// Nothing correct depends on listing: reads, commits, and the change feed
/// complete without a single LIST, even with junk parked in the segment
/// collection (chain traversal never sees it).
#[tokio::test]
async fn reads_commits_and_change_feed_never_list() {
    let temp_dir = tempdir().expect("tempdir");
    let store = CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("store"),
        KeyPredicate::any(),
    );
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = mutation_context("acceptance", "wrs_acceptance", "acceptance/0.1.0", 1_000);
    let engine = namespace_engine(&store, &namespace_id, &context);
    engine
        .bootstrap_namespace(BootstrapOptions::default())
        .await
        .expect("bootstrap");

    // Foreign junk in the segment collection must be invisible to every
    // hot path.
    store
        .put_if_absent(
            &format!("{}zz-junk.tmp", wal_segment_prefix(namespace_id.as_str())),
            Bytes::from_static(b"junk"),
        )
        .await
        .expect("write junk");

    let baseline = store.count(OperationClass::List);
    let staged = engine
        .begin_upload(BeginUploadRequest::default())
        .await
        .expect("begin upload");
    let uploaded = engine
        .upload_content(&staged.upload_id, b"uploaded\n")
        .await
        .expect("upload content");
    engine
        .complete_upload(
            &staged.upload_id,
            &loonfs_api::v0::CompleteUploadRequest {
                content_ref: uploaded.content_ref,
            },
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
        .resolve_path("/docs/hello.txt", &ctx)
        .await
        .expect("stat");
    engine
        .list_path_page(
            "/docs",
            loonfs_api::PageRequest {
                limit: loonfs_test_support::ids::page_limit(1024),
                cursor: None,
            },
            &ctx,
        )
        .await
        .expect("list directory");
    let bytes = engine
        .read_file("/docs/hello.txt", &ctx, None)
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

/// The head changes only when commits land: checkpoint creation, root
/// publication, floor advancement, garbage collection, and the upload
/// workflow leave `wal/head.json` byte-identical.
#[tokio::test]
async fn maintenance_never_touches_the_wal_head() {
    let temp_dir = tempdir().expect("tempdir");
    let store = LocalFsStore::new(temp_dir.path()).expect("store");
    let namespace_id = NamespaceId::parse("demo").expect("namespace id");
    let context = mutation_context("acceptance", "wrs_acceptance", "acceptance/0.1.0", 1_000);
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

    let head_key = wal_head(namespace_id.as_str());
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
        .begin_upload(BeginUploadRequest::default())
        .await
        .expect("second upload");
    let uploaded = engine
        .upload_content(&staged.upload_id, b"more\n")
        .await
        .expect("second upload content");
    engine
        .complete_upload(
            &staged.upload_id,
            &loonfs_api::v0::CompleteUploadRequest {
                content_ref: uploaded.content_ref,
            },
        )
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
