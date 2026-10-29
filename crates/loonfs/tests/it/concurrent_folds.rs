//! How often a writer folds its WAL tail while many sessions commit at once.
//! The store takes a set time for each request on the paused test clock, so
//! those times alone decide whether the writer's folds or its commits are
//! slower.

use futures::stream::{self, StreamExt};
use loonfs::metrics::{DefaultMetricsRecorder, MetricValue};
use loonfs::{
    DestinationBehavior, ErrorCode, InlineContentPolicy, LoonFs, Namespace, PutFileOptions,
    SharedObjectStore, Writable,
};
use loonfs_test_support::stores::{
    Intercept, InterceptStore, Interceptor, MemoryStore, OperationContext, OperationKind,
};
use loonfs_types::{ActorId, ChecksumAlgorithm, MonotonicTimer};
use std::sync::Arc;
use std::time::Duration;

/// Inline bytes that each turn appends to the session's transcript.
const APPEND_BYTES: usize = 4096;
/// Inline bytes of the state file that each turn replaces.
const STATE_BYTES: usize = 1024;

/// One run of the workload. Every session commits at the same time, and
/// each of its turns appends to its transcript and then replaces its state
/// file, as an agent session does.
#[derive(Debug, Clone, Copy)]
struct Shape {
    sessions: usize,
    turns: usize,
    /// How long the store takes for a WAL put.
    wal_put_elapsed_ms: u64,
    /// How long the store takes for every other request.
    request_elapsed_ms: u64,
}

impl Shape {
    /// How many times the run's inline bytes fill the fold threshold.
    fn fold_thresholds(self) -> u64 {
        let inline_bytes = self.sessions * self.turns * (APPEND_BYTES + STATE_BYTES);
        let fold_at_bytes = InlineContentPolicy::default().inline_content_fold_at_bytes;
        u64::try_from(inline_bytes / fold_at_bytes).expect("a test's threshold count fits in u64")
    }
}

/// A store that takes a set time for each request on the paused test clock,
/// so a slow request costs no wall time.
#[derive(Debug)]
struct StoreDelay {
    wal_put_elapsed_ms: u64,
    request_elapsed_ms: u64,
}

#[async_trait::async_trait]
impl Interceptor for StoreDelay {
    type Guard = ();

    #[expect(
        clippy::disallowed_methods,
        reason = "the delay stands in for provider latency on the paused test clock"
    )]
    async fn before(&self, context: &OperationContext<'_>) -> (Intercept, ()) {
        let wal_put =
            context.key().contains("/wal/") && matches!(context.kind(), OperationKind::Put { .. });
        let elapsed_ms = if wal_put {
            self.wal_put_elapsed_ms
        } else {
            self.request_elapsed_ms
        };
        tokio::time::sleep(Duration::from_millis(elapsed_ms)).await;
        (Intercept::Continue, ())
    }
}

/// The paused test clock as the writer's monotonic timer, so the writer's
/// publish pacing and the store's request times use one clock.
#[derive(Debug)]
struct PausedClock(tokio::time::Instant);

impl PausedClock {
    #[expect(
        clippy::disallowed_methods,
        reason = "the writer's timer starts at the paused test clock's current time"
    )]
    fn start() -> Self {
        Self(tokio::time::Instant::now())
    }
}

impl MonotonicTimer for PausedClock {
    #[expect(
        clippy::disallowed_methods,
        reason = "the writer's timer reads the paused test clock"
    )]
    fn monotonic_now_ms(&self) -> u64 {
        let elapsed = tokio::time::Instant::now().duration_since(self.0);
        u64::try_from(elapsed.as_millis()).expect("a test run fits in u64 milliseconds")
    }
}

/// Retries a write that the publisher refused because its queue was full,
/// as a client does.
///
/// A write at the write stop waits in the queue for the writer's fold, so
/// the publisher refuses it only after the write-stop wait.
async fn retry_refusals<T, Fut>(mut call: impl FnMut() -> Fut) -> loonfs::Result<T>
where
    Fut: std::future::Future<Output = loonfs::Result<T>>,
{
    for _ in 0..1_000 {
        match call().await {
            Err(error) if error.code() == ErrorCode::CommitQueueFull => {
                tokio::task::yield_now().await;
            }
            outcome => return outcome,
        }
    }
    panic!("the publisher refused one write 1000 times");
}

/// Runs one session's turns, one commit at a time.
async fn session_turns(
    namespace: &Namespace<Writable>,
    actor: &ActorId,
    session: usize,
    turns: usize,
) {
    let transcript = format!("/sessions/s-{session:03}/transcript.jsonl");
    let state = format!("/sessions/s-{session:03}/state.json");
    let replace = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..PutFileOptions::default()
    };
    retry_refusals(|| namespace.put_file(&transcript, b"{}\n", actor))
        .await
        .expect("create the transcript");
    for turn in 0..turns {
        let fill = b'a' + u8::try_from(turn % 26).expect("a letter");
        let line = vec![fill; APPEND_BYTES];
        retry_refusals(|| namespace.append_file(&transcript, &line, actor))
            .await
            .expect("append to the transcript");
        let body = vec![fill; STATE_BYTES];
        retry_refusals(|| namespace.put_file_with_options(&state, &body, actor, &replace))
            .await
            .expect("replace the state file");
    }
}

/// Runs every session's turns at once against one writer, and returns how
/// many folds the writer published.
async fn folds_for(shape: Shape) -> u64 {
    let store: SharedObjectStore = Arc::new(InterceptStore::with_interceptor(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        StoreDelay {
            wal_put_elapsed_ms: shape.wal_put_elapsed_ms,
            request_elapsed_ms: shape.request_elapsed_ms,
        },
    ));
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer: LoonFs<Writable> = LoonFs::builder_with_store(store)
        .writer_id(loonfs_test_support::ids::writer_id("concurrent-folds"))
        .monotonic_timer(Arc::new(PausedClock::start()))
        .metrics_recorder(recorder.clone())
        .build()
        .await
        .expect("build the writer");
    let actor = loonfs_test_support::test_actor();
    let namespace_id = loonfs_test_support::ids::namespace_id("sessions");
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create the namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open the namespace");
    stream::iter(0..shape.sessions)
        .map(|session| session_turns(&namespace, &actor, session, shape.turns))
        .buffer_unordered(shape.sessions)
        .collect::<Vec<()>>()
        .await;
    namespace
        .wait_for_fold()
        .await
        .expect("wait for the last fold");
    recorder
        .snapshot()
        .by_name("loonfs.publisher.wal_folds")
        .map(|entry| match entry.value {
            MetricValue::Counter(count) => count,
            ref other => panic!("expected a counter, found {other:?}"),
        })
        .sum()
}

/// Checks that a writer with many concurrent committers starts a fold only
/// once its WAL tail reaches the inline fold threshold, so it folds at most
/// once for each threshold of inline bytes, and that it folds once for each
/// threshold when its folds run faster than its commits. (Each WAL object
/// here batches enough commits that the inline threshold, not the WAL
/// object count, starts each fold.)
///
/// When every other request takes twice as long as a WAL put, each fold runs
/// slower than the commits, and the tail grows past the threshold while the
/// fold runs. The next fold then covers more than one threshold, so the same
/// commits take fewer folds. That's why the fold counts of two runs with
/// different store latency can't be compared.
#[tokio::test(start_paused = true)]
async fn concurrent_committers_fold_once_per_threshold_of_inline_bytes() {
    let slow_commits = |sessions, turns| Shape {
        sessions,
        turns,
        wal_put_elapsed_ms: 150,
        request_elapsed_ms: 5,
    };
    for shape in [slow_commits(32, 80), slow_commits(250, 10)] {
        let thresholds = shape.fold_thresholds();
        let folds = folds_for(shape).await;
        assert!(
            (thresholds - 1..=thresholds).contains(&folds),
            "{shape:?} published {folds} folds for {thresholds} fold thresholds"
        );
    }

    let slow_folds = Shape {
        wal_put_elapsed_ms: 20,
        request_elapsed_ms: 40,
        ..slow_commits(32, 80)
    };
    let thresholds = slow_folds.fold_thresholds();
    let folds = folds_for(slow_folds).await;
    assert!(
        (1..thresholds - 1).contains(&folds),
        "{slow_folds:?} published {folds} folds for {thresholds} fold thresholds, \
         but each fold should cover more than one threshold"
    );
}
