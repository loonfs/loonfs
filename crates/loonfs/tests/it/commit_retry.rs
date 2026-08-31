//! Rerunning a put under a commit id that already committed.
//!
//! A commit's identity names *which* content object it wrote, so a rerun —
//! which necessarily stages a fresh object — is a different commit as far as
//! the publisher is concerned and it says so. What makes the rerun safe
//! anyway is the runtime reading back what that commit id actually
//! committed, rebuilding this request's fingerprint around the committed
//! reference, and requiring the whole value to match before it proves the
//! bytes agree. Nothing weaker counts as agreement.

use crate::common::{open_runtime_async, store, TestRuntime};
use bytes::Bytes;
use futures::StreamExt;
use loonfs::publish::{parse_mutation_path, CommitRequest, FilesystemOperation};
use loonfs::{
    ByteStream, ChangeSeq, CommitId, CreateDirectoryOptions, CreateNamespaceOptions,
    DestinationBehavior, ListChangesOptions, MaintenancePlan, NamespaceId, PutFileOptions,
    ReorganizeStepOutcome, RevisionNo,
};
use loonfs_api::ErrorCode;
use loonfs_api::{ActorId, ActorRef};
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
        commit: loonfs_api::options::CommitOptions {
            actor: loonfs_test_support::test_actor(),
            commit_id: Some(commit_id.clone()),
            message: None,
        },
        expected_inode_id: None,
        expected_revision_no: None,
    }
}

fn options_with_message(commit_id: &CommitId, message: Option<&str>) -> PutFileOptions {
    let mut options = options(commit_id);
    options.commit.message = message.map(ToOwned::to_owned);
    options
}

/// What the feed says the commit at a sequence was annotated with.
async fn feed_message(
    runtime: &TestRuntime,
    namespace_id: &NamespaceId,
    seq: ChangeSeq,
) -> Option<String> {
    let page = runtime
        .reader
        .list_changes(namespace_id, ChangeSeq(0), ListChangesOptions::default())
        .await
        .expect("list changes");
    page.changes
        .into_iter()
        .find(|change| change.committed_seq == seq)
        .expect("the committed change is on the feed")
        .message
}

async fn namespace(runtime: &TestRuntime) -> NamespaceId {
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    runtime
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    namespace_id
}

/// Advances retention and rebuilds the metadata runs that held a commit receipt.
async fn compact_receipt_past_horizon(
    runtime: &TestRuntime,
    namespace_id: &NamespaceId,
    committed_seq: ChangeSeq,
) -> ChangeSeq {
    // Nine rounds: the first flush builds the base run, and the next eight
    // accumulate the delta runs that reach the fold trigger
    // (`DEFAULT_MAX_CHECKPOINT_DELTA_RUNS`).
    let mut last_seq = committed_seq;
    for round in 0..9 {
        let filler = runtime
            .put_file_bytes(
                namespace_id,
                &format!("/docs/filler-{round}.txt"),
                b"filler\n",
                options(&CommitId::parse(format!("filler-{round}")).expect("valid commit id")),
            )
            .await
            .expect("filler put");
        last_seq = filler.committed_seq;
        runtime
            .create_checkpoint(namespace_id)
            .await
            .expect("create checkpoint");
    }
    let advanced = runtime
        .admin
        .run_maintenance(
            namespace_id,
            MaintenancePlan {
                advance_retention: true,
                ..MaintenancePlan::default()
            },
        )
        .await
        .expect("advance retention floor")
        .retention
        .expect("retention selected");
    assert!(
        advanced.retention_floor_seq > committed_seq,
        "the floor must pass the commit for this to test anything: floor {:?}, commit {:?}",
        advanced.retention_floor_seq,
        committed_seq
    );

    // The floor alone leaves the receipt answering. Drain reorganization so
    // the run families holding the receipt are rebuilt above the floor.
    let mut folded = false;
    for _ in 0..32 {
        let step = runtime
            .admin
            .run_maintenance(namespace_id, MaintenancePlan::metadata())
            .await
            .expect("upkeep step")
            .metadata_maintenance
            .expect("metadata selected");
        if matches!(step.reorganize, ReorganizeStepOutcome::NotNeeded) {
            break;
        }
        folded = true;
    }
    assert!(
        folded,
        "reorganization must actually rebuild runs for this to test anything"
    );

    last_seq
}

#[tokio::test]
async fn restart_replays_the_commit_actor_from_the_wal() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let actor = ActorRef::system(ActorId::parse("replay-worker").expect("actor id"));
    let committed = runtime
        .writer
        .create_directory(
            &namespace_id,
            "/replayed",
            CreateDirectoryOptions::new(actor.clone()),
        )
        .await
        .expect("commit attributed directory");
    drop(runtime);

    let reopened = open_runtime_async(store(temp_dir.path()), "writer-b").await;
    let page = reopened
        .reader
        .list_changes(&namespace_id, ChangeSeq(0), ListChangesOptions::default())
        .await
        .expect("replay change feed after restart");
    let change = page
        .changes
        .into_iter()
        .find(|change| change.committed_seq == committed.committed_seq)
        .expect("committed change");
    assert_eq!(change.committed_by, actor);
}

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

#[tokio::test]
async fn the_same_bytes_at_a_different_path_still_conflict() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");

    let first = runtime
        .put_file_bytes(
            &namespace_id,
            "/a.txt",
            b"stable bytes\n",
            options(&commit_id),
        )
        .await
        .expect("first put");
    let error = runtime
        .put_file_bytes(
            &namespace_id,
            "/b.txt",
            b"stable bytes\n",
            options(&commit_id),
        )
        .await
        .expect_err("the same bytes at another path are a different commit");

    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
    assert_eq!(
        runtime
            .get_path_entry(&namespace_id, "/b.txt")
            .await
            .expect_err("the refused rerun wrote nothing")
            .code(),
        ErrorCode::PathNotFound
    );
    let entry = runtime
        .get_path_entry(&namespace_id, "/a.txt")
        .await
        .expect("stat path");
    assert_eq!(
        entry.head_seq, first.committed_seq,
        "the refused rerun published no revision"
    );
}

#[tokio::test]
async fn a_changed_behavior_under_a_used_commit_id_still_conflicts() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");

    let first = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect("first put with replace behavior");
    let error = runtime
        .put_file_bytes(
            &namespace_id,
            PATH,
            b"stable bytes\n",
            PutFileOptions {
                behavior: DestinationBehavior::NoReplace,
                ..options(&commit_id)
            },
        )
        .await
        .expect_err("a changed behavior is a different commit");

    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
    let entry = runtime
        .get_path_entry(&namespace_id, PATH)
        .await
        .expect("stat path");
    assert_eq!(
        entry.head_seq, first.committed_seq,
        "the refused rerun published no revision"
    );
}

#[tokio::test]
async fn a_changed_expected_revision_under_a_used_commit_id_still_conflicts() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");

    let first = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect("first unguarded put");
    let observed = runtime
        .get_path_entry(&namespace_id, PATH)
        .await
        .expect("stat path");
    let error = runtime
        .put_file_bytes(
            &namespace_id,
            PATH,
            b"stable bytes\n",
            PutFileOptions {
                expected_inode_id: Some(observed.inode_id),
                expected_revision_no: Some(RevisionNo(1)),
                ..options(&commit_id)
            },
        )
        .await
        .expect_err("a guard the original never carried is a different commit");

    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
    let entry = runtime
        .get_path_entry(&namespace_id, PATH)
        .await
        .expect("stat path");
    assert_eq!(
        entry.head_seq, first.committed_seq,
        "the refused rerun published no revision"
    );
}

#[tokio::test]
async fn a_single_put_does_not_replay_a_multi_operation_commit() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-batch").expect("valid commit id");

    let prepared = runtime
        .writer
        .prepare_file_bytes(&namespace_id, b"stable bytes\n")
        .await
        .expect("prepare content");
    let content_ref = prepared.content_ref().clone();
    let first = runtime
        .writer
        .commit_prepared(
            &namespace_id,
            CommitRequest {
                commit_id: commit_id.clone(),
                actor: loonfs_test_support::test_actor(),
                message: None,
                operations: vec![
                    FilesystemOperation::PutFile {
                        path: parse_mutation_path(PATH).expect("path"),
                        content_ref,
                        behavior: DestinationBehavior::Replace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                    FilesystemOperation::CreateDirectory {
                        path: parse_mutation_path("/reports").expect("path"),
                        parents: true,
                    },
                ],
            },
            vec![prepared],
        )
        .await
        .expect("first two-operation commit");

    let error = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect_err("one put is not the two-operation commit that landed");

    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
    let entry = runtime
        .get_path_entry(&namespace_id, PATH)
        .await
        .expect("stat path");
    assert_eq!(
        entry.head_seq, first.committed_seq,
        "the refused rerun published no revision"
    );
}

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

#[tokio::test]
async fn rerunning_a_put_with_the_same_message_replays_on_identical_bytes() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");
    let options = || options_with_message(&commit_id, Some("import batch"));

    let first = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options())
        .await
        .expect("first put");
    let rerun = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options())
        .await
        .expect("rerunning an identical request is idempotent");

    assert_eq!(rerun, first);
    assert_eq!(
        feed_message(&runtime, &namespace_id, first.committed_seq).await,
        Some("import batch".to_owned())
    );
}

#[tokio::test]
async fn a_changed_message_under_a_used_commit_id_still_conflicts() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");

    let first = runtime
        .put_file_bytes(
            &namespace_id,
            PATH,
            b"stable bytes\n",
            options_with_message(&commit_id, Some("import batch")),
        )
        .await
        .expect("first put");
    let error = runtime
        .put_file_bytes(
            &namespace_id,
            PATH,
            b"stable bytes\n",
            options_with_message(&commit_id, Some("second thoughts")),
        )
        .await
        .expect_err("a changed message is a different commit");

    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
    assert_eq!(
        feed_message(&runtime, &namespace_id, first.committed_seq).await,
        Some("import batch".to_owned()),
        "the refused rerun did not rewrite the annotation that landed"
    );
    let entry = runtime
        .get_path_entry(&namespace_id, PATH)
        .await
        .expect("stat path");
    assert_eq!(
        entry.head_seq, first.committed_seq,
        "the refused rerun published no revision"
    );
}

#[tokio::test]
async fn an_empty_message_does_not_replay_an_absent_one() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");

    let first = runtime
        .put_file_bytes(
            &namespace_id,
            PATH,
            b"stable bytes\n",
            options_with_message(&commit_id, None),
        )
        .await
        .expect("first put");
    let error = runtime
        .put_file_bytes(
            &namespace_id,
            PATH,
            b"stable bytes\n",
            options_with_message(&commit_id, Some("")),
        )
        .await
        .expect_err("an empty message is not the absent message");

    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
    assert_eq!(
        feed_message(&runtime, &namespace_id, first.committed_seq).await,
        None
    );
}

#[tokio::test]
async fn a_changed_message_on_mkdir_still_conflicts() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-mkdir").expect("valid commit id");
    let options = |message: &str| CreateDirectoryOptions {
        commit: loonfs_api::options::CommitOptions {
            actor: loonfs_test_support::test_actor(),
            commit_id: Some(commit_id.clone()),
            message: Some(message.to_owned()),
        },
        parents: true,
    };

    let first = runtime
        .writer
        .create_directory(&namespace_id, "/pinned", options("one"))
        .await
        .expect("first mkdir");
    let replay = runtime
        .writer
        .create_directory(&namespace_id, "/pinned", options("one"))
        .await
        .expect("an identical retry replays");
    assert_eq!(replay, first);

    let error = runtime
        .writer
        .create_directory(&namespace_id, "/pinned", options("two"))
        .await
        .expect_err("a changed message is a different commit");
    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
}

#[tokio::test]
async fn a_changed_message_on_a_direct_commit_still_conflicts() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-commit").expect("valid commit id");
    let request = |message: &str| {
        CommitRequest::single(
            commit_id.clone(),
            loonfs_test_support::test_actor(),
            Some(message.to_owned()),
            FilesystemOperation::CreateDirectory {
                path: parse_mutation_path("/direct").expect("path"),
                parents: true,
            },
        )
    };

    let first = runtime
        .writer
        .create_commit(&namespace_id, request("one"))
        .await
        .expect("first commit");
    let replay = runtime
        .writer
        .create_commit(&namespace_id, request("one"))
        .await
        .expect("an identical retry replays");
    assert_eq!(replay, first);

    let error = runtime
        .writer
        .create_commit(&namespace_id, request("two"))
        .await
        .expect_err("a changed message is a different commit");
    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
}

#[tokio::test]
async fn a_retention_trimmed_commit_seq_leaves_the_conflict_standing() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");

    let first = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect("first put");

    // Pin the head, then give up incremental replay below it. The commit's
    // receipt still binds the id, so a rerun still conflicts and the
    // conflict still names where the commit landed — but the change feed no
    // longer answers for that sequence.
    runtime
        .create_checkpoint(&namespace_id)
        .await
        .expect("create checkpoint");
    let advanced = runtime
        .admin
        .run_maintenance(
            &namespace_id,
            MaintenancePlan {
                advance_retention: true,
                ..MaintenancePlan::default()
            },
        )
        .await
        .expect("advance retention floor")
        .retention
        .expect("retention selected");
    assert!(
        advanced.retention_floor_seq >= first.committed_seq,
        "the floor must cover the commit for this to test anything: floor {:?}, commit {:?}",
        advanced.retention_floor_seq,
        first.committed_seq
    );

    let error = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect_err("evidence that cannot be read cannot reconcile to success");

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

#[tokio::test]
async fn a_retry_past_the_receipt_horizon_commits_again() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-put").expect("valid commit id");

    let first = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect("first put");

    let last_seq = compact_receipt_past_horizon(&runtime, &namespace_id, first.committed_seq).await;

    // The live session's caches still answer the receipt (a conservative,
    // longer in-process window — a rerun in the same process still replays
    // or conflicts). The horizon is a durable-state fact, and the late
    // retry it governs is a cross-process event, so reopen on the same
    // store to read what the reorganization actually kept.
    drop(runtime);
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-b").await;

    // Same id, same bytes — but the receipt is gone, so this is not a
    // replay (which would return the original sequence without committing)
    // and not a conflict: it commits again.
    let retried = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect("a retry past the horizon is admitted as a new commit");
    assert!(
        retried.committed_seq > last_seq,
        "the late retry landed as a new commit: first {:?}, retry {:?}",
        first.committed_seq,
        retried.committed_seq
    );

    // The blast radius the spec documents: a duplicate revision of
    // identical content. The file still reads back the same bytes.
    assert_eq!(
        runtime
            .reader
            .get_file_bytes(&namespace_id, PATH)
            .await
            .expect("read file")
            .bytes,
        b"stable bytes\n",
    );
}

#[tokio::test]
async fn concurrent_retries_past_the_receipt_horizon_commit_once() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("concurrent-horizon-put").expect("valid commit id");

    let first = runtime
        .put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id))
        .await
        .expect("first put");
    let last_seq = compact_receipt_past_horizon(&runtime, &namespace_id, first.committed_seq).await;

    // Reopen so both retries observe the durable receipt horizon rather than
    // the conservative in-process cache.
    drop(runtime);
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-b").await;

    let (left, right) = tokio::join!(
        runtime.put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id)),
        runtime.put_file_bytes(&namespace_id, PATH, b"stable bytes\n", options(&commit_id)),
    );
    let left = left.expect("first late retry");
    let right = right.expect("second late retry");

    assert_eq!(left, right, "both retries resolve to the same commit");
    assert_eq!(
        left.committed_seq,
        ChangeSeq(last_seq.0 + 1),
        "only one retry advances the namespace head"
    );
    assert_eq!(
        runtime
            .admin
            .get_namespace_diagnostics(&namespace_id)
            .await
            .expect("namespace diagnostics")
            .head_seq,
        left.committed_seq,
        "the losing retry does not publish another commit"
    );
}

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
