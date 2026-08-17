//! Cancellation semantics of the unified publication path: admitted work
//! is owned by the publication service's tasks, so cancelling callers —
//! one, or all of them — abandons only result delivery, never the
//! publication itself.

use futures::future::BoxFuture;
use loonfs::publish::{parse_mutation_path, CommitCandidate, CommitRequest, FilesystemOperation};
use loonfs::{
    BeginUploadRequest, CommitId, CommitResponse, CreateNamespaceOptions, DestinationBehavior,
    FsWriter, NamespaceId, PutFileOptions, SharedObjectStore,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{BlockingStore, KeyPredicate, OperationClass};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::time::{timeout, Duration};

struct ParkedPuts {
    store: Arc<BlockingStore<LocalFsStore>>,
    writer: Arc<FsWriter>,
    namespace_id: NamespaceId,
    first: tokio::task::JoinHandle<loonfs::Result<CommitResponse>>,
    /// A caller future whose publication is already admitted, batched
    /// behind the parked one. Awaiting it yields the publication's result;
    /// dropping it is the caller walking away from admitted work.
    second: BoxFuture<'static, loonfs::Result<CommitResponse>>,
}

/// Parks one put at the blocked head CAS with a second put admitted behind
/// it, so tests can cancel callers at both positions.
///
/// Both positions are arranged, not timed. The first put has provably been
/// admitted because its head CAS is observably in flight. The second put's
/// content is staged up front through the upload API, and its submission
/// future is polled exactly once: submission is synchronous through the
/// publisher's admission and its only await is the result channel, so a
/// pending first poll proves the candidate sits in the open batch behind
/// the parked publication. A settle sleep would leave a window where a
/// caller cancelled early was never admitted at all — and a publication
/// that never existed cannot land.
async fn park_two_puts(temp_dir: &Path) -> ParkedPuts {
    let namespace_id = NamespaceId::parse("parked").expect("valid namespace id");
    let store_impl = Arc::new(BlockingStore::new(
        LocalFsStore::new(temp_dir).expect("create local-fs store"),
        KeyPredicate::wal_head(&namespace_id),
        OperationClass::CompareAndSwap,
    ));
    let store: SharedObjectStore = store_impl.clone();
    let writer = Arc::new(
        FsWriter::builder_with_store(store.clone())
            .writer_id("parked-writer")
            .min_publish_interval_ms(0)
            .build()
            .await
            .expect("build writer"),
    );
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    // Stage the second put's content while the store is unblocked: once
    // callers start being cancelled, the only work still in flight is
    // publication, the thing under test.
    let upload = writer
        .begin_upload(&namespace_id, BeginUploadRequest::ServiceProxied {})
        .await
        .expect("begin upload");
    let staged = writer
        .upload_content(&namespace_id, upload.upload_id(), b"b")
        .await
        .expect("stage second put content");
    let catalog = loonfs_core::control::load_namespace_catalog_entry(&store, &namespace_id)
        .await
        .expect("load namespace catalog");
    let prepared =
        loonfs_core::content::prepare_existing_content_ref(&store, &catalog, staged.content_ref)
            .await
            .expect("prepare second put content");
    let prepared_content_ref = prepared.content_ref().clone();

    store_impl.arm();
    let first = {
        let writer = Arc::clone(&writer);
        let namespace_id = namespace_id.clone();
        tokio::spawn(async move {
            writer
                .put_file_bytes(
                    &namespace_id,
                    "/a.txt",
                    b"a",
                    PutFileOptions::new(loonfs_test_support::test_actor()),
                )
                .await
        })
    };
    store_impl.wait_until_blocked().await;

    // The publish task is parked inside the blocked CAS with its batch
    // already taken, so this submission deterministically opens the next
    // batch behind it.
    let registry = writer.publisher();
    let mut second: BoxFuture<'static, loonfs::Result<CommitResponse>> = {
        let namespace_id = namespace_id.clone();
        let request = CommitRequest::single(
            CommitId::parse("parked-second").expect("valid commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::PutFile {
                path: parse_mutation_path("/b.txt").expect("mutation path"),
                content_ref: prepared_content_ref,
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
            },
        );
        Box::pin(async move {
            registry
                .submit_candidate(
                    namespace_id,
                    CommitCandidate::prepared(request, vec![prepared]),
                )
                .await
        })
    };
    assert!(
        futures::poll!(second.as_mut()).is_pending(),
        "the second submission must be admitted and parked on its result channel"
    );

    ParkedPuts {
        store: store_impl,
        writer,
        namespace_id,
        first,
        second,
    }
}

/// Cancelling the caller whose publication is mid-flight abandons only its
/// result: the publication lands, and the caller queued behind it gets its
/// own durable result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_caller_does_not_cancel_admitted_publication() {
    let temp_dir = tempdir().expect("tempdir");
    let parked = park_two_puts(temp_dir.path()).await;

    parked.first.abort();
    let _ = parked.first.await;
    parked.store.release();

    let second = timeout(Duration::from_secs(15), parked.second)
        .await
        .expect("queued put must settle")
        .expect("queued put publishes normally");
    assert_eq!(second.namespace_id, parked.namespace_id);

    let reader = parked.writer.reader();
    reader
        .get_file_bytes(&parked.namespace_id, "/a.txt")
        .await
        .expect("the cancelled caller's admitted publication landed");
    reader
        .get_file_bytes(&parked.namespace_id, "/b.txt")
        .await
        .expect("the queued publication landed");
}

/// Even with every caller gone, admitted publications land and a drain
/// settles their tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_callers_cancelled_publication_still_lands() {
    let temp_dir = tempdir().expect("tempdir");
    let parked = park_two_puts(temp_dir.path()).await;

    parked.first.abort();
    let _ = parked.first.await;
    drop(parked.second);
    parked.store.release();

    // The writer-owned drain settles the publish tasks the callers left
    // behind.
    timeout(Duration::from_secs(15), parked.writer.shutdown())
        .await
        .expect("drain must settle")
        .expect("drain surfaces no panics");

    let reader = parked.writer.reader();
    reader
        .get_file_bytes(&parked.namespace_id, "/a.txt")
        .await
        .expect("the first admitted publication landed");
    reader
        .get_file_bytes(&parked.namespace_id, "/b.txt")
        .await
        .expect("the second admitted publication landed");
}
