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
use loonfs::publish::{parse_mutation_path, CommitRequest, FilesystemOperation};
use loonfs::{
    ByteStream, ChangeSeq, CommitId, CreateDirectoryOptions, CreateNamespaceOptions,
    DestinationBehavior, ListChangesOptions, MaintenanceStepKind, MaintenanceStepOptions,
    NamespaceId, PutFileOptions, ReorganizeStepOutcome,
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

fn options_with_message(commit_id: &CommitId, message: Option<&str>) -> PutFileOptions {
    PutFileOptions {
        message: message.map(ToOwned::to_owned),
        ..options(commit_id)
    }
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
        .find(|change| change.seq == seq)
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

/// The same bytes under the same message are the same commit, so the rerun
/// replays the sequence that already landed — the message being present
/// changes nothing about that.
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

/// The message is part of a commit's identity, so the same bytes under a
/// changed message are a different mutation and the conflict stands. The
/// bytes matching is exactly what makes this worth pinning: content
/// evidence alone would call this a successful retry and quietly keep the
/// original annotation.
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
        .stat_path(&namespace_id, PATH)
        .await
        .expect("stat path");
    assert_eq!(
        entry.head_seq, first.committed_seq,
        "the refused rerun published no revision"
    );
}

/// Absent and empty are different messages: the fingerprint serializes the
/// annotation as given, so `null` and `""` are different commits. The
/// reconciliation compares them the same way rather than folding both into
/// "no message".
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

/// The mutations that never uploaded anything reconcile nothing — the
/// publisher's own identity check is the whole answer — so a changed
/// message conflicts there whatever the put path does. This anchors that
/// the reconciliation added for put did not become the only thing keeping
/// the contract.
#[tokio::test]
async fn a_changed_message_on_mkdir_still_conflicts() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-mkdir").expect("valid commit id");
    let options = |message: &str| CreateDirectoryOptions {
        commit_id: Some(commit_id.clone()),
        message: Some(message.to_owned()),
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

/// The same anchor one layer down, where the caller builds the commit
/// itself: the request that reaches the publisher is identical apart from
/// its message, and that alone conflicts.
#[tokio::test]
async fn a_changed_message_on_a_direct_commit_still_conflicts() {
    let temp_dir = tempdir().expect("tempdir");
    let runtime = open_runtime_async(store(temp_dir.path()), "writer-a").await;
    let namespace_id = namespace(&runtime).await;
    let commit_id = CommitId::parse("pinned-commit").expect("valid commit id");
    let request = |message: &str| {
        CommitRequest::single(
            commit_id.clone(),
            Some(message.to_owned()),
            FilesystemOperation::CreateDirectory {
                path: parse_mutation_path("/direct").expect("path"),
                parents: true,
            },
        )
    };

    let first = runtime
        .writer
        .commit(&namespace_id, request("one"))
        .await
        .expect("first commit");
    let replay = runtime
        .writer
        .commit(&namespace_id, request("one"))
        .await
        .expect("an identical retry replays");
    assert_eq!(replay, first);

    let error = runtime
        .writer
        .commit(&namespace_id, request("two"))
        .await
        .expect_err("a changed message is a different commit");
    assert_eq!(error.code(), ErrorCode::CommitIdReuseConflict);
}

/// Reconciliation reads the feed at the sequence the conflict reported, so
/// once retention has surrendered the replay promise below that sequence
/// there is no committed content left to compare against. Identical bytes
/// and all, the conflict stands: a comparison that cannot be made is never
/// reported as success.
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
        .maintenance_step_namespace(
            &namespace_id,
            MaintenanceStepOptions {
                only: Some(MaintenanceStepKind::Retention),
                ..MaintenanceStepOptions::default()
            },
        )
        .await
        .expect("advance retention floor");
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

/// The idempotency horizon, pinned end to end: once the retention floor
/// passes a commit AND reorganization drops its receipt, the id no longer
/// replays — a late retry commits again as a new mutation. That is the
/// documented contract (api spec, "safe retry"): replay is guaranteed only
/// while the receipt lives, and receipts live exactly as long as retained
/// history. This test exists so the behavior stays deliberate; if it ever
/// starts failing because a late retry replays or errors instead, the spec
/// and this pin must move together.
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

    // Move the floor strictly past the pinned commit, and pile up enough
    // flushed runs that reorganization has real folding to do — receipts
    // are dropped when the runs holding them are rebuilt, and rebuilding
    // waits for enough L0 runs to accumulate.
    // Nine rounds: the first flush builds the base run, and the next eight
    // accumulate the L0 runs that reach the fold trigger
    // (`DEFAULT_MAX_CHECKPOINT_L0_RUNS`).
    let mut last_seq = first.committed_seq;
    for round in 0..9 {
        let filler = runtime
            .put_file_bytes(
                &namespace_id,
                &format!("/docs/filler-{round}.txt"),
                b"filler\n",
                options(&CommitId::parse(format!("filler-{round}")).expect("valid commit id")),
            )
            .await
            .expect("filler put");
        last_seq = filler.committed_seq;
        runtime
            .create_checkpoint(&namespace_id)
            .await
            .expect("create checkpoint");
    }
    let advanced = runtime
        .admin
        .maintenance_step_namespace(
            &namespace_id,
            MaintenanceStepOptions {
                only: Some(MaintenanceStepKind::Retention),
                ..MaintenanceStepOptions::default()
            },
        )
        .await
        .expect("advance retention floor");
    assert!(
        advanced.retention_floor_seq > first.committed_seq,
        "the floor must pass the commit for this to test anything: floor {:?}, commit {:?}",
        advanced.retention_floor_seq,
        first.committed_seq
    );

    // The floor alone leaves the receipt answering (the conflict test above
    // proves it). Drain reorganization until it reports nothing left, so
    // the run families holding the receipt have been rebuilt above the
    // floor.
    let mut folded = false;
    for _ in 0..32 {
        let step = runtime
            .admin
            .maintenance_step_namespace(
                &namespace_id,
                MaintenanceStepOptions {
                    only: Some(MaintenanceStepKind::Reorganize),
                    ..MaintenanceStepOptions::default()
                },
            )
            .await
            .expect("reorganize step");
        if matches!(step.reorganize, ReorganizeStepOutcome::NotNeeded) {
            break;
        }
        folded = true;
    }
    assert!(
        folded,
        "reorganization must actually rebuild runs for this to test anything"
    );

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
