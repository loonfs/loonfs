//! What garbage collection can reach after an embedded convenience put.
//!
//! The convenience puts stage their own bytes, so they are both ends of an
//! upload at once. They still open a session for the object they write,
//! because the session record is the only handle anything has on a content
//! object before metadata references it. These tests hold that to its
//! consequence: every blob one of these puts writes is eventually either
//! referenced by metadata or reclaimed, and none of them is immortal.
//!
//! Garbage collection is driven through `loonfs_core::gc_namespace` rather
//! than `FsAdmin::gc_namespace`, because the admin handle stamps its pass
//! from the wall clock and these deadlines are days out. The store is the
//! same one the runtime wrote through, so the pass sees exactly what the
//! puts left behind.

#![allow(clippy::panic)]
// Runtime integration tests use panic in helper assertions for precise diagnostics.

use crate::common::{open_runtime_async, store, TestRuntime};
use loonfs::{
    ContentRef, CreateNamespaceOptions, GcConfig, GcResponse, NamespaceId, PutFileOptions,
    SharedObjectStore,
};
use loonfs_core::limits::{CONTENT_RECLAMATION_GRACE_MS, UPLOAD_SESSION_LEASE_MS};
use loonfs_core::MutationContext;
use loonfs_objectstore::layout::DurableObjectFamily;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::stores::{
    CountingStore, FailStore, InjectedError, KeyPredicate, OperationClass,
};
use std::sync::Arc;
use tempfile::tempdir;

/// The grace a pass is configured with, well above the enforced floor so
/// the derived content grace is what decides these tests.
const GRACE_MS: u64 = 60 * 60 * 1000;

fn config() -> GcConfig {
    GcConfig {
        grace_window_ms: GRACE_MS,
        max_objects: None,
        cursor: None,
    }
}

/// Runs one pass at a fabricated clock. The runtime stamped its sessions
/// from the real clock, so every deadline here is measured from that.
async fn collect(store: &SharedObjectStore, namespace_id: &NamespaceId, now_ms: u64) -> GcResponse {
    loonfs_core::gc_namespace(
        store.as_ref(),
        namespace_id,
        &config(),
        &MutationContext {
            writer_id: "reclamation-test".to_owned(),
            now_ms,
        },
    )
    .await
    .expect("garbage collection")
}

async fn namespace(runtime: &TestRuntime) -> NamespaceId {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    runtime
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    namespace_id
}

/// The object key one staged reference lives at.
async fn content_key(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
    content_ref: &ContentRef,
) -> String {
    let content_store_id =
        loonfs::control::load_namespace_catalog_entry(store.as_ref(), namespace_id)
            .await
            .expect("load namespace catalog")
            .content_store_id()
            .clone();
    loonfs_objectstore::keys::content_blob(content_store_id.as_str(), &content_ref.content_id)
}

async fn exists(store: &SharedObjectStore, key: &str) -> bool {
    store.head(key).await.expect("head object").is_some()
}

/// Every upload-session record the namespace still holds.
async fn session_keys(store: &SharedObjectStore, namespace_id: &NamespaceId) -> Vec<String> {
    store
        .list_prefix(&loonfs_objectstore::keys::upload_session_prefix(
            namespace_id.as_str(),
        ))
        .await
        .expect("list upload sessions")
}

/// A prepared reference that never reaches a commit is not a leak. It is a
/// completed session's content that no metadata names, which is the one
/// thing content collection exists to reclaim.
#[tokio::test]
async fn content_prepared_and_never_published_is_reclaimed_with_its_session() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let runtime = open_runtime_async(store.clone(), "prepare-only").await;
    let namespace_id = namespace(&runtime).await;
    // A published file, so the reference scan has a root to read and the
    // verdict on the prepared object is "absent" rather than "unknown".
    runtime
        .writer
        .put_file_bytes(
            &namespace_id,
            "/docs/live.txt",
            b"live",
            PutFileOptions::default(),
        )
        .await
        .expect("published put");

    let prepared = runtime
        .writer
        .prepare_file_bytes(&namespace_id, b"never published")
        .await
        .expect("prepare content");
    let orphan_key = content_key(&store, &namespace_id, prepared.content_ref()).await;
    assert!(exists(&store, &orphan_key).await);

    let staged_at_ms = loonfs::current_time_ms().expect("wall clock");
    let inside = collect(&store, &namespace_id, staged_at_ms).await;
    assert_eq!(inside.deleted_content_objects, 0);
    assert!(
        exists(&store, &orphan_key).await,
        "inside the grace a receipt could still admit a commit for these bytes"
    );

    let past = collect(
        &store,
        &namespace_id,
        staged_at_ms + CONTENT_RECLAMATION_GRACE_MS + 1,
    )
    .await;
    assert_eq!(
        past.deleted_content_objects, 1,
        "the prepared object is the one reclamation"
    );
    assert_eq!(
        past.deleted_upload_sessions, 2,
        "both sessions have said everything they will say"
    );
    assert!(!exists(&store, &orphan_key).await);
    assert!(session_keys(&store, &namespace_id).await.is_empty());
}

/// Published content answers to metadata from then on. Its session record
/// still ages out, and taking that record must not take the bytes with it.
#[tokio::test]
async fn a_published_put_keeps_its_content_and_loses_only_the_session_record() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let runtime = open_runtime_async(store.clone(), "published-put").await;
    let namespace_id = namespace(&runtime).await;
    runtime
        .writer
        .put_file_bytes(
            &namespace_id,
            "/docs/kept.txt",
            b"kept",
            PutFileOptions::default(),
        )
        .await
        .expect("published put");
    let content_ref = runtime
        .reader
        .stat_path(&namespace_id, "/docs/kept.txt")
        .await
        .expect("stat file")
        .content_ref()
        .cloned()
        .expect("a file carries a content ref");
    let published_key = content_key(&store, &namespace_id, &content_ref).await;

    let report = collect(
        &store,
        &namespace_id,
        loonfs::current_time_ms().expect("wall clock") + CONTENT_RECLAMATION_GRACE_MS + 1,
    )
    .await;

    assert_eq!(report.deleted_upload_sessions, 1);
    assert_eq!(
        report.deleted_content_objects, 0,
        "metadata protects the bytes the moment the commit lands"
    );
    assert!(exists(&store, &published_key).await);
    assert_eq!(
        runtime
            .reader
            .get_file_bytes(&namespace_id, "/docs/kept.txt")
            .await
            .expect("read the file back")
            .bytes,
        b"kept"
    );
}

/// Re-running a put under a commit id that already committed stages a
/// second object and then reconciles against the first. The duplicate is
/// referenced by nothing, and this is what "not a leak" has to mean: it
/// goes, and the object the commit actually named stays.
#[tokio::test]
async fn a_retrys_duplicate_content_is_reclaimed_and_the_commit_it_matched_survives() {
    let temp_dir = tempdir().expect("tempdir");
    let store = store(temp_dir.path());
    let runtime = open_runtime_async(store.clone(), "retrying-writer").await;
    let namespace_id = namespace(&runtime).await;
    let options = || PutFileOptions {
        commit_id: Some(loonfs::CommitId::parse("cmt_reconciled").expect("valid commit id")),
        ..PutFileOptions::default()
    };

    let first = runtime
        .writer
        .put_file_bytes(&namespace_id, "/docs/retry.txt", b"same bytes", options())
        .await
        .expect("first put");
    let committed_content_ref = runtime
        .reader
        .stat_path(&namespace_id, "/docs/retry.txt")
        .await
        .expect("stat file")
        .content_ref()
        .cloned()
        .expect("a file carries a content ref");
    let committed_key = content_key(&store, &namespace_id, &committed_content_ref).await;

    let retry = runtime
        .writer
        .put_file_bytes(&namespace_id, "/docs/retry.txt", b"same bytes", options())
        .await
        .expect("the rerun reconciles against what the commit id landed");
    assert_eq!(
        retry.committed_seq, first.committed_seq,
        "the rerun reports the original commit, not a second one"
    );
    assert_eq!(
        session_keys(&store, &namespace_id).await.len(),
        2,
        "one session per staging write, published or not"
    );

    let report = collect(
        &store,
        &namespace_id,
        loonfs::current_time_ms().expect("wall clock") + CONTENT_RECLAMATION_GRACE_MS + 1,
    )
    .await;

    assert_eq!(report.deleted_upload_sessions, 2);
    assert_eq!(
        report.deleted_content_objects, 1,
        "exactly the duplicate the rerun staged"
    );
    assert!(
        exists(&store, &committed_key).await,
        "the object the commit named is referenced and stays"
    );
    assert_eq!(
        runtime
            .reader
            .get_file_bytes(&namespace_id, "/docs/retry.txt")
            .await
            .expect("read the file back")
            .bytes,
        b"same bytes"
    );
}

/// The session record is durable before the content write is attempted, so
/// a staging write that dies mid-flight leaves an owner behind rather than
/// nothing. What the owner buys is the expiry sweep: the record ages out of
/// its lease and takes whatever it was writing with it.
#[tokio::test]
async fn staging_that_fails_leaves_a_session_the_expiry_sweep_reclaims() {
    let temp_dir = tempdir().expect("tempdir");
    // A refused precondition rather than a transport failure: the immutable
    // write retries the second for most of a minute before giving up, and
    // what this test is about is the state a failed staging write leaves,
    // not how long it takes to fail.
    let failing = Arc::new(FailStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::content_blob(),
        OperationClass::Put,
        InjectedError::PreconditionFailed,
    ));
    let store: SharedObjectStore = failing.clone();
    let runtime = open_runtime_async(store.clone(), "failing-writer").await;
    let namespace_id = namespace(&runtime).await;

    failing.fail_all();
    runtime
        .writer
        .put_file_bytes(
            &namespace_id,
            "/docs/lost.txt",
            b"never lands",
            PutFileOptions::default(),
        )
        .await
        .expect_err("the content write fails");
    failing.clear();

    assert_eq!(
        session_keys(&store, &namespace_id).await.len(),
        1,
        "the session that would have owned the bytes is durable before they are written"
    );

    let staged_at_ms = loonfs::current_time_ms().expect("wall clock");
    let inside = collect(&store, &namespace_id, staged_at_ms).await;
    assert_eq!(
        inside.deleted_upload_sessions, 0,
        "the lease has not passed"
    );

    // Past the lease and its grace the session aborts; a grace after that
    // stamp the record itself is reaped.
    let aborted_at_ms = staged_at_ms + UPLOAD_SESSION_LEASE_MS + GRACE_MS + 1;
    collect(&store, &namespace_id, aborted_at_ms).await;
    let reaped = collect(&store, &namespace_id, aborted_at_ms + GRACE_MS + 1).await;
    assert_eq!(reaped.deleted_upload_sessions, 1);
    assert!(session_keys(&store, &namespace_id).await.is_empty());
}

/// What ownership costs, pinned so it stays deliberate: one create-if-absent
/// for the record that claims the content id, and one compare-and-swap to
/// freeze what was written under it. Reading the record for that swap's etag
/// is the third and last control operation a put adds.
#[tokio::test]
async fn a_put_pays_two_control_writes_for_the_session_that_owns_its_content() {
    let temp_dir = tempdir().expect("tempdir");
    let sessions = Arc::new(CountingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::family(DurableObjectFamily::UploadSession),
    ));
    let store: SharedObjectStore = sessions.clone();
    let runtime = open_runtime_async(store, "counted-writer").await;
    let namespace_id = namespace(&runtime).await;

    sessions.reset();
    runtime
        .writer
        .put_file_bytes(
            &namespace_id,
            "/docs/counted.txt",
            b"counted",
            PutFileOptions::default(),
        )
        .await
        .expect("put file");

    let counts = sessions.snapshot();
    assert_eq!(
        counts.create_if_absent_puts, 1,
        "one record opens, claiming the content id before the bytes exist"
    );
    assert_eq!(
        counts.compare_and_swaps, 1,
        "one swap freezes the reference the write produced"
    );
    assert_eq!(counts.overwrite_puts, 0, "a session is never clobbered");
    assert_eq!(
        counts.operations(OperationClass::Read),
        1,
        "the swap reads the etag it swaps on, and nothing else reads a session"
    );
    assert_eq!(
        counts.deletes, 0,
        "only collection removes a session record"
    );
}
