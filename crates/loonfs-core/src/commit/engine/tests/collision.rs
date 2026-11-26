//! A writer session whose WAL put finds its number taken: what the session
//! reads before its next unit, and when that unit loads its view by
//! discovery instead.

use super::*;
use crate::wal::{is_retryable_wal_publish, prepare_wal_object, publish_wal_object};
use loonfs_test_support::stores::request_lines;
use loonfs_types::WalNo;

/// Publishes `candidate` through `engine`, and publishes it again while its
/// result is retryable, as the publisher's worker does. Returns the result
/// that the caller sees and the number of units.
async fn publish_settled<S: ObjectStore>(
    engine: &mut NamespaceCommitEngine<S>,
    writer: &WriterAt,
    candidate: CommitCandidate,
) -> (Result<Commit>, usize) {
    let clock = OperationClock::start(writer.now_ms, Arc::clone(&engine.namespace.timer));
    let mut units = 0;
    loop {
        units += 1;
        let result = engine
            .publish_batch([candidate.clone()], &clock)
            .await
            .results
            .remove(0);
        if !is_retryable_wal_publish(&result) || units == crate::limits::MAX_CONTENTION_ATTEMPTS {
            return (result, units);
        }
    }
}

fn wal(kind: &str, wal_no: u64) -> String {
    format!("{kind} demo WalObject {wal_no:020}")
}

/// Returns the lines of `operations` (see [`request_lines`]) with each run
/// of WAL GETs sorted, because a window's GETs finish in any order.
fn lines(operations: &[RecordedOperation]) -> Vec<String> {
    let mut lines = request_lines(operations);
    let get = |line: &String| line.starts_with("get demo WalObject");
    for run in lines.chunk_by_mut(|left, right| get(left) && get(right)) {
        run.sort();
    }
    lines
}

/// Checks that a quiet session whose next WAL number another process took
/// with a floor object reads the floor object and keeps its position. The
/// unit that collides reads the floor object and the next window, which is
/// empty, the next unit commits above the floor object with its put alone,
/// against the new floor, and the caller sees the commit.
#[tokio::test]
async fn a_session_that_collides_with_a_floor_object_reads_it_and_keeps_its_position() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    let before = engine.writer_position().expect("a position");
    let tip = before.read_basis().head.wal_no.0;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    let planned = namespace.load_current_manifest().await.expect("planned");
    namespace
        .publish_floor_object(&planned, CommitSeq(1))
        .await
        .expect("another process creates a floor object");

    timer.advance_ms(QUIET_HOUR_MS);
    store.reset();
    let (result, units) = publish_settled(&mut engine, &writer, create_dir("zeta", "zeta")).await;
    result.expect("the caller sees the commit");
    assert_eq!(units, 2);
    assert_eq!(
        lines(&store.take()),
        [
            wal("put", tip + 1),
            wal("get", tip + 1),
            wal("get", tip + 2),
            wal("get", tip + 3),
            wal("put", tip + 2),
        ]
    );
    let after = engine
        .writer_position()
        .expect("the session keeps its position");
    assert_eq!(
        after.read_basis().basis.manifest_no(),
        before.read_basis().basis.manifest_no()
    );
    assert_eq!(after.read_basis().head.wal_no, WalNo(tip + 2));
    assert_eq!(after.retention_floor_seq(), CommitSeq(1));
}

/// Checks that a quiet session whose tail another process folded, and whose
/// next WAL number that process then took with a floor object, reads the
/// floor object and keeps its position on its own manifest, which the fold
/// doesn't fold past once the position's tip is the floor object. The next
/// unit commits with its put alone, and a reader of the newest manifest
/// replays the commit.
#[tokio::test]
async fn a_session_that_collides_with_a_fold_then_an_advance_keeps_its_position() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    let before = engine.writer_position().expect("a position");
    let tip = before.read_basis().head.wal_no.0;
    let namespace = crate::test_support::namespace_store(&store, &namespace_id);
    namespace.fold_wal().await.expect("another process folds");
    let folded = namespace.load_current_manifest().await.expect("the fold");
    assert_eq!(folded.payload().folded_wal_no, WalNo(tip));
    namespace
        .publish_floor_object(&folded, CommitSeq(1))
        .await
        .expect("another process creates a floor object");

    timer.advance_ms(QUIET_HOUR_MS);
    store.reset();
    let (result, units) = publish_settled(&mut engine, &writer, create_dir("zeta", "zeta")).await;
    result.expect("the caller sees the commit");
    assert_eq!(units, 2);
    assert_eq!(
        lines(&store.take()),
        [
            wal("put", tip + 1),
            wal("get", tip + 1),
            wal("get", tip + 2),
            wal("get", tip + 3),
            wal("put", tip + 2),
        ]
    );
    let after = engine
        .writer_position()
        .expect("the session keeps its position");
    assert_eq!(
        after.read_basis().basis.manifest_no(),
        before.read_basis().basis.manifest_no()
    );
    assert_eq!(after.read_basis().head.wal_no, WalNo(tip + 2));
    let head = namespace.load_read_state().await.expect("the newest head");
    assert_eq!(head.wal_no, WalNo(tip + 2));
    assert_eq!(head.retention_floor_seq, CommitSeq(1));
}

/// Checks that a session whose put collides with an object of its own epoch
/// that doesn't continue its tip drops its position, and that its next
/// unit's discovery, from a manifest that doesn't fold the object, returns
/// `namespace_corrupt`.
#[tokio::test]
async fn a_session_that_collides_with_a_broken_head_runs_discovery() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
    let store = gated_store(&directory, KeyPredicate::new(|_| false));
    let (mut engine, _timer, writer) = writer_after_one_publish(&store, &namespace_id).await;
    let mut head = engine
        .writer_position()
        .expect("a position")
        .read_basis()
        .head
        .clone();
    let tip = head.wal_no.0;
    head.seq = CommitSeq(head.seq.0 + 5);
    let broken = prepare_wal_object(
        &head,
        head.writer_epoch,
        WalObjectBody::Floor {
            retention_floor_seq: CommitSeq(0),
        },
    )
    .expect("prepare");
    publish_wal_object(&store, &broken)
        .await
        .expect("an object that doesn't continue the tip");

    store.reset();
    let (result, units) = publish_settled(&mut engine, &writer, create_dir("zeta", "zeta")).await;
    let error = result.expect_err("the namespace is corrupt");
    assert_eq!(error.code(), ErrorCode::NamespaceCorrupt, "{error:?}");
    assert_eq!(units, 2);
    let lines = lines(&store.take());
    assert_eq!(
        lines[..3],
        [
            wal("put", tip + 1),
            wal("get", tip + 1),
            "get demo Boundary".to_owned()
        ],
        "{lines:#?}"
    );
}
