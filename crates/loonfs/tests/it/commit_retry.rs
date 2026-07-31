//! Rerunning a put under a commit id that already committed.
//!
//! A commit's identity names *which* content object it wrote, so a rerun —
//! which necessarily stages a fresh object — is a different commit as far as
//! the publisher is concerned and it says so. What makes the rerun safe
//! anyway is the runtime reading back what that commit id actually
//! committed and comparing it against the bytes just staged. Nothing weaker
//! counts as agreement.

use crate::common::{open_runtime_async, store, TestRuntime};
use bytes::Bytes;
use futures::StreamExt;
use loonfs::{
    ByteStream, CommitId, CreateNamespaceOptions, DestinationBehavior, NamespaceId, PutFileOptions,
};
use loonfs_api::ErrorCode;
use tempfile::tempdir;

const PATH: &str = "/docs/retry.txt";

/// A payload delivered the way a large one is: in pieces, with nothing
/// holding it whole.
fn streamed(payload: &[u8], chunk_bytes: usize) -> ByteStream {
    let chunks: Vec<Bytes> = payload
        .chunks(chunk_bytes)
        .map(Bytes::copy_from_slice)
        .collect();
    futures::stream::iter(chunks.into_iter().map(Ok)).boxed()
}

fn options(commit_id: &CommitId) -> PutFileOptions {
    PutFileOptions {
        behavior: DestinationBehavior::Replace,
        commit_id: Some(commit_id.clone()),
        message: None,
        expected_revision_no: None,
    }
}

async fn namespace(runtime: &TestRuntime) -> NamespaceId {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    runtime
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    namespace_id
}

/// The retry that motivates all of this: the same command run twice with
/// the same `--commit-id`. The second run uploads again, conflicts, and
/// then reconciles to the commit that already landed.
#[tokio::test]
async fn rerunning_a_put_with_the_same_commit_id_replays_on_identical_bytes() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");

    let first = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect("first put");
    let rerun = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect("rerunning identical bytes is idempotent");

    assert_eq!(rerun, first);
    assert_eq!(
        runtime
            .reader
            .get_file_bytes(&namespace_id, PATH)
            .await
            .expect("read file")
            .bytes,
        b"stable bytes\n"
    );
}

/// Different bytes under the same commit id are a different operation, not
/// a retry, and the conflict stands.
#[tokio::test]
async fn different_bytes_under_a_used_commit_id_still_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");

    runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect("first put");
    let error = runtime
        .put_file_bytes(
            &namespace_id,
            PATH,
            b"different bytes\n",
            options(&commit_id),
        )
        .await
        .expect_err("different bytes are a different commit");

    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
    assert_eq!(
        runtime
            .reader
            .get_file_bytes(&namespace_id, PATH)
            .await
            .expect("read file")
            .bytes,
        b"stable bytes\n",
        "the refused rerun changed nothing"
    );
}

/// Same length, different bytes. Size alone cannot tell these apart, so
/// this is the case that proves the comparison reaches the content's
/// checksum rather than stopping at its length.
#[tokio::test]
async fn same_length_different_bytes_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");

    runtime
        .put_file_bytes(&namespace_id, PATH, b"aaaaaaaaaaaa\n", options(&commit_id))
        .await
        .expect("first put");
    let error = runtime
        .put_file_bytes(&namespace_id, PATH, b"bbbbbbbbbbbb\n", options(&commit_id))
        .await
        .expect_err("same length is not the same content");

    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
}

/// A commit id that never committed anything is not a retry of anything,
/// so a rerun against an unrelated id behaves like a first run.
#[tokio::test]
async fn an_unused_commit_id_is_an_ordinary_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;

    let first = runtime
        .put_file_bytes(
            &namespace_id,
            PATH,
            b"stable bytes\n",
            options(&CommitId::parse("first").expect("valid commit id")),
        )
        .await
        .expect("first put");
    let second = runtime
        .put_file_bytes(
            &namespace_id,
            PATH,
            b"stable bytes\n",
            options(&CommitId::parse("second").expect("valid commit id")),
        )
        .await
        .expect("a fresh commit id is a fresh commit");

    assert!(second.committed_seq > first.committed_seq);
}

/// The same reconciliation for a payload nobody held: rerunning a streamed
/// put with the same commit id and the same bytes replays the commit that
/// already landed. The evidence is what the pass measured and hashed,
/// because the bytes themselves are long gone.
#[tokio::test]
async fn rerunning_a_streamed_put_with_the_same_commit_id_replays_on_identical_bytes() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-stream").expect("valid commit id");
    let payload = vec![7u8; 300_000];

    let first = runtime
        .writer
        .put_file_stream(
            &namespace_id,
            PATH,
            streamed(&payload, 64 * 1024),
            options(&commit_id),
        )
        .await
        .expect("first streamed put");
    let rerun = runtime
        .writer
        .put_file_stream(
            &namespace_id,
            PATH,
            streamed(&payload, 7_919),
            options(&commit_id),
        )
        .await
        .expect("rerunning identical bytes is idempotent");

    assert_eq!(
        rerun, first,
        "the same bytes reconcile however the source chunked them"
    );
    assert_eq!(
        runtime
            .reader
            .get_file_bytes(&namespace_id, PATH)
            .await
            .expect("read file")
            .bytes,
        payload
    );
}

/// Different bytes under the same commit id are a different operation
/// whether or not anyone held them. Same length, too, so only the digest
/// can tell them apart.
#[tokio::test]
async fn different_streamed_bytes_under_a_used_commit_id_still_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-stream").expect("valid commit id");

    runtime
        .writer
        .put_file_stream(
            &namespace_id,
            PATH,
            streamed(&vec![7u8; 300_000], 64 * 1024),
            options(&commit_id),
        )
        .await
        .expect("first streamed put");
    let error = runtime
        .writer
        .put_file_stream(
            &namespace_id,
            PATH,
            streamed(&vec![9u8; 300_000], 64 * 1024),
            options(&commit_id),
        )
        .await
        .expect_err("different bytes are a different commit");

    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
    assert_eq!(
        runtime
            .reader
            .get_file_bytes(&namespace_id, PATH)
            .await
            .expect("read file")
            .bytes,
        vec![7u8; 300_000],
        "the refused rerun changed nothing"
    );
}

/// A streamed put and a buffered put of the same bytes are the same
/// operation and reconcile against each other: both hash the whole payload
/// themselves, so both produce the same trusted digest.
#[tokio::test]
async fn a_streamed_rerun_reconciles_against_a_buffered_first_run() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-either-way").expect("valid commit id");
    let payload = vec![3u8; 100_000];

    let first = runtime
        .put_file_bytes(&namespace_id, PATH, &payload, options(&commit_id))
        .await
        .expect("first buffered put");
    let rerun = runtime
        .writer
        .put_file_stream(
            &namespace_id,
            PATH,
            streamed(&payload, 8_192),
            options(&commit_id),
        )
        .await
        .expect("the same bytes, read differently, are the same commit");

    assert_eq!(rerun, first);
}
