//! What garbage collection can reach after an embedded convenience put.
//!
//! The convenience puts write their own content objects and write no upload
//! record, so until a commit references an object, only the object's age
//! protects it. These tests check that every object one of these puts writes
//! is either referenced by metadata or deleted once it's older than the grace
//! window.
//!
//! Garbage collection is driven through `PoolStore::gc` rather than
//! `Maintenance::gc_pool`, because the maintenance handle stamps its pass
//! from the wall clock and these deadlines are a day out. The store is the
//! same one the runtime wrote through, so the pass sees exactly what the
//! puts left behind.

use crate::common::{namespace, open_runtime_with_async, store, TestRuntime};
use loonfs::{ContentRef, NamespaceId, PoolGcResponse, PutFileOptions, SharedObjectStore};
use loonfs_core::limits::GC_GRACE_WINDOW_MS;
use loonfs_core::test_support::WriterAt;
use loonfs_objectstore::layout::DurableObjectFamily;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_test_support::stores::{
    FailStore, InjectedError, KeyPredicate, OperationSelector, RecordingStore,
};
use std::sync::Arc;
use tempfile::tempdir;

async fn open_staged_runtime(store: SharedObjectStore, writer_id: &str) -> TestRuntime {
    open_runtime_with_async(store, writer_id, |builder| {
        builder.config(loonfs::RuntimeConfig {
            inline_content: loonfs::InlineContentPolicy {
                inline_content_threshold_bytes: 0,
                ..Default::default()
            },
            ..loonfs::RuntimeConfig::default()
        })
    })
    .await
}

/// Runs one pass of the pool of `namespace_id` at a fabricated clock. The
/// provider stamped the runtime's objects from the real clock, so every
/// deadline here is measured from that.
async fn collect(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    now_ms: u64,
) -> PoolGcResponse {
    let pool_id = loonfs_core::test_support::load_pool_id(store, namespace_id).await;
    loonfs_core::test_support::pool_store(store.as_ref(), &pool_id)
        .gc(
            None,
            &WriterAt {
                writer_id: loonfs_types::WriterId::parse("reclamation-test").expect("writer id"),
                now_ms,
            }
            .clock(),
        )
        .await
        .expect("garbage collection")
}

/// Returns the key of the whole object of `content_ref`, under the prefix of
/// the pool of `namespace_id`.
async fn content_key(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    content_ref: &ContentRef,
) -> String {
    let pool_id = loonfs_core::test_support::load_pool_id(store, namespace_id).await;
    loonfs_objectstore::keys::content_blob(&pool_id, &content_ref.content_id)
}

async fn exists(store: &SharedObjectStore, key: &str) -> bool {
    store.head(key).await.expect("head object").is_some()
}

/// Every upload-session record under the pool's prefix.
async fn session_keys(store: &SharedObjectStore, namespace_id: &NamespaceId) -> Vec<String> {
    let pool_id = loonfs_core::test_support::load_pool_id(store, namespace_id).await;
    store
        .list_prefix(&loonfs_objectstore::keys::upload_session_prefix(&pool_id))
        .await
        .expect("list upload sessions")
}

#[tokio::test]
async fn content_prepared_and_never_published_is_reclaimed_at_the_grace_window() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let runtime = open_staged_runtime(store.clone(), "prepare-only").await;
    let namespace_id = namespace(&runtime).await;
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    // A published file: a revision keeps its content while the prepared
    // object is deleted once it's older than the grace window.
    namespace_writer
        .put_file(
            "/docs/live.txt",
            b"live",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("published put");

    let prepared = namespace_writer
        .prepare_content(b"never published")
        .await
        .expect("prepare content");
    let orphan_key = content_key(&store, &namespace_id, prepared.content_ref()).await;
    assert!(exists(&store, &orphan_key).await);

    let staged_at_ms = loonfs_core::time::current_time_ms();
    let inside = collect(&store, &namespace_id, staged_at_ms).await;
    assert_eq!(inside.deleted.content_objects, 0);
    assert!(
        exists(&store, &orphan_key).await,
        "inside the grace window a commit could still admit these bytes"
    );

    let past = collect(&store, &namespace_id, staged_at_ms + GC_GRACE_WINDOW_MS + 1).await;
    assert_eq!(
        past.deleted.content_objects, 1,
        "the prepared object is the one reclamation"
    );
    assert!(!exists(&store, &orphan_key).await);
    assert!(session_keys(&store, &namespace_id).await.is_empty());
}

#[tokio::test]
async fn a_published_put_keeps_its_content() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let runtime = open_staged_runtime(store.clone(), "published-put").await;
    let namespace_id = namespace(&runtime).await;
    let namespace = runtime.reader.namespace(&namespace_id);
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .put_file(
            "/docs/kept.txt",
            b"kept",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("published put");
    let content_ref = namespace
        .stat("/docs/kept.txt")
        .await
        .expect("stat file")
        .content_ref()
        .cloned()
        .expect("a file carries a content ref");
    let published_key = content_key(&store, &namespace_id, &content_ref).await;

    let report = collect(
        &store,
        &namespace_id,
        loonfs_core::time::current_time_ms() + GC_GRACE_WINDOW_MS + 1,
    )
    .await;

    assert_eq!(
        report.deleted.content_objects, 0,
        "metadata protects the bytes the moment the commit is made"
    );
    assert!(exists(&store, &published_key).await);
    assert_eq!(
        namespace
            .read_file("/docs/kept.txt")
            .await
            .expect("read the file back")
            .bytes,
        b"kept"
    );
}

#[tokio::test]
async fn a_conflicting_upload_is_reclaimed_and_the_published_content_survives() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let runtime = open_staged_runtime(store.clone(), "retrying-writer").await;
    let namespace_id = namespace(&runtime).await;
    let namespace = runtime.reader.namespace(&namespace_id);
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let options = || {
        let mut options = PutFileOptions::default();
        options.commit.commit_id =
            Some(loonfs::CommitId::parse("cmt_original").expect("valid commit id"));
        options
    };

    let first = namespace_writer
        .put_file_with_options(
            "/docs/retry.txt",
            b"same bytes",
            &loonfs_test_support::test_actor(),
            &options(),
        )
        .await
        .expect("first put");
    let committed_content_ref = namespace
        .stat("/docs/retry.txt")
        .await
        .expect("stat file")
        .content_ref()
        .cloned()
        .expect("a file carries a content ref");
    let committed_key = content_key(&store, &namespace_id, &committed_content_ref).await;

    let retry = namespace_writer
        .put_file_with_options(
            "/docs/retry.txt",
            b"same bytes",
            &loonfs_test_support::test_actor(),
            &options(),
        )
        .await
        .expect_err("the fresh upload conflicts with the committed request");
    assert_eq!(
        retry
            .details()
            .and_then(|details| details.committed)
            .map(|committed| committed.committed_seq),
        Some(first.committed_seq),
        "the conflict identifies the original commit"
    );
    let past_ms = loonfs_core::time::current_time_ms() + GC_GRACE_WINDOW_MS + 1;
    let report = collect(&store, &namespace_id, past_ms).await;
    assert_eq!(
        report.deleted.content_objects, 1,
        "exactly the duplicate the rerun staged"
    );
    assert!(
        exists(&store, &committed_key).await,
        "the object the commit named is referenced and stays"
    );
    assert_eq!(
        namespace
            .read_file("/docs/retry.txt")
            .await
            .expect("read the file back")
            .bytes,
        b"same bytes"
    );
}

#[tokio::test]
async fn staging_that_fails_leaves_no_upload_record() {
    let temp_dir = tempdir().expect("tempdir");
    // A refused precondition rather than a transport failure: the immutable
    // write retries the second for most of a minute before giving up, and
    // what this test is about is the state a failed staging write leaves,
    // not how long it takes to fail.
    let failing = Arc::new(FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::content_blob(),
        OperationSelector::Put,
        InjectedError::PreconditionFailed,
    ));
    let store: SharedObjectStore = failing.clone();
    let runtime = open_staged_runtime(store.clone(), "failing-writer").await;
    let namespace_id = namespace(&runtime).await;
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    failing.fail_all();
    namespace_writer
        .put_file(
            "/docs/lost.txt",
            b"never committed",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect_err("the content write fails");
    failing.clear();

    assert!(
        session_keys(&store, &namespace_id).await.is_empty(),
        "a failed embedded write leaves no upload record"
    );
}

#[tokio::test]
async fn a_put_writes_one_content_object_and_no_upload_record() {
    let temp_dir = tempdir().expect("tempdir");
    let recorded = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::new(|key| {
            loonfs_objectstore::layout::parse_object_key(key).is_some_and(|parsed| {
                matches!(
                    parsed.family(),
                    DurableObjectFamily::ContentBlob
                        | DurableObjectFamily::UploadSession
                        | DurableObjectFamily::UploadOutcome
                )
            })
        }),
    ));
    let store: SharedObjectStore = recorded.clone();
    let runtime = open_staged_runtime(store, "counted-writer").await;
    let namespace_id = namespace(&runtime).await;
    let namespace_writer = runtime
        .writer
        .open_namespace(&namespace_id)
        .expect("open namespace");

    recorded.reset();
    namespace_writer
        .put_file(
            "/docs/counted.txt",
            b"counted",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("put file");

    let counts = recorded.counts();
    assert_eq!(
        counts,
        loonfs_test_support::stores::StoreCounts {
            puts: 1,
            create_if_absent_puts: 1,
            written_bytes: 7,
            ..Default::default()
        },
        "the put writes its content object and no upload record"
    );
}
