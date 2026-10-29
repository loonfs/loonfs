//! The instruments of the publication service.

use crate::metrics::labels::{LabeledCounters, MetricLabel};
use crate::metrics::{
    seconds_from_ms, CounterHandle, GaugeHandle, HistogramHandle, MetricsRecorder,
    LATENCY_SECONDS_BOUNDARIES, RESULT_ERROR, RESULT_OK, SMALL_COUNT_BOUNDARIES,
};
use std::sync::Arc;

/// The closed outcome vocabulary for a publication delivered to its caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublishOutcome {
    Ok,
    Error,
}

impl PublishOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => RESULT_OK,
            Self::Error => RESULT_ERROR,
        }
    }
}

impl MetricLabel for PublishOutcome {
    const VALUES: &'static [Self] = &[Self::Ok, Self::Error];

    fn as_str(self) -> &'static str {
        PublishOutcome::as_str(self)
    }
}

/// Publication instruments. Every label here is closed and known at
/// construction, so all of them register once.
pub(crate) struct PublisherInstruments {
    batches: Arc<dyn CounterHandle>,
    wal_folds: Arc<dyn CounterHandle>,
    wal_fold_seconds: Arc<dyn HistogramHandle>,
    write_stop_waits: Arc<dyn CounterHandle>,
    write_stop_wait_seconds: Arc<dyn HistogramHandle>,
    write_stop_refusals: Arc<dyn CounterHandle>,
    tail_replays: Arc<dyn CounterHandle>,
    batch_size: Arc<dyn HistogramHandle>,
    queue_depth: Arc<dyn GaugeHandle>,
    sessions_open: Arc<dyn GaugeHandle>,
    publishes: LabeledCounters<PublishOutcome>,
}

impl PublisherInstruments {
    pub(crate) fn register(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            batches: recorder.register_counter(
                "loonfs.publisher.batches",
                "Mutation batches taken for publication",
                &[],
            ),
            wal_folds: recorder.register_counter(
                "loonfs.publisher.wal_folds",
                "WAL tails folded by namespace publishers",
                &[],
            ),
            wal_fold_seconds: recorder.register_histogram(
                "loonfs.publisher.wal_fold_seconds",
                "Duration of a WAL-tail fold in seconds",
                &[],
                LATENCY_SECONDS_BOUNDARIES,
            ),
            write_stop_waits: recorder.register_counter(
                "loonfs.publisher.write_stop_waits",
                "Commits that waited in a namespace's queue at the write stop for a fold",
                &[],
            ),
            write_stop_wait_seconds: recorder.register_histogram(
                "loonfs.publisher.write_stop_wait_seconds",
                "Duration of a namespace queue's wait at the write stop in seconds",
                &[],
                LATENCY_SECONDS_BOUNDARIES,
            ),
            write_stop_refusals: recorder.register_counter(
                "loonfs.publisher.write_stop_refusals",
                "Commits refused because they waited for the whole write-stop wait",
                &[],
            ),
            tail_replays: recorder.register_counter(
                "loonfs.publisher.tail_replays",
                "Publishes that read the WAL tail from the store because the writer session had no position to start from",
                &[],
            ),
            batch_size: recorder.register_histogram(
                "loonfs.publisher.batch_size",
                "Candidates in one published batch",
                &[],
                SMALL_COUNT_BOUNDARIES,
            ),
            // Sampled rather than totalled: one process serves many
            // namespaces and each has its own queue, so this reads as the
            // depth of whichever publisher last admitted or took work.
            queue_depth: recorder.register_gauge(
                "loonfs.publisher.queue_depth",
                "Candidates queued at the namespace publisher that last admitted or took work",
                &[],
            ),
            sessions_open: recorder.register_gauge(
                "loonfs.publisher.sessions_open",
                "Namespace writer sessions that a handle holds or whose admitted work is still running",
                &[],
            ),
            publishes: LabeledCounters::register(
                recorder,
                "loonfs.publisher.publishes",
                "Publication results delivered to their callers",
                "result",
                &[],
            ),
        }
    }

    /// Reports the candidates a namespace has admitted but not yet taken.
    pub(crate) fn queue_depth(&self, queue_depth: usize) {
        self.queue_depth
            .set(i64::try_from(queue_depth).unwrap_or(i64::MAX));
    }

    pub(crate) fn sessions(&self, open: usize) {
        self.sessions_open
            .set(i64::try_from(open).unwrap_or(i64::MAX));
    }

    /// Reports one batch taken for publication.
    pub(crate) fn batch(&self, batch_size: usize) {
        self.batches.increment(1);
        self.batch_size.record(batch_size as f64);
    }

    /// Reports one WAL-tail fold on the publication path that published a
    /// manifest.
    pub(crate) fn wal_fold(&self) {
        self.wal_folds.increment(1);
    }

    pub(crate) fn wal_fold_duration(&self, elapsed_ms: u64) {
        self.wal_fold_seconds.record(seconds_from_ms(elapsed_ms));
    }

    /// Reports commits that waited in the queue at the write stop for the
    /// first time.
    pub(crate) fn write_stop_wait(&self, commits: u64) {
        self.write_stop_waits.increment(commits);
    }

    /// Reports one wait of a namespace's queue at the write stop.
    pub(crate) fn write_stop_wait_duration(&self, elapsed_ms: u64) {
        self.write_stop_wait_seconds
            .record(seconds_from_ms(elapsed_ms));
    }

    /// Reports commits that returned `maintenance_required` because they
    /// waited for the whole write-stop wait.
    pub(crate) fn write_stop_refusal(&self, commits: u64) {
        self.write_stop_refusals.increment(commits);
    }

    pub(crate) fn tail_replay(&self) {
        self.tail_replays.increment(1);
    }

    /// Reports one publication result delivered to its caller.
    pub(crate) fn publish(&self, outcome: PublishOutcome) {
        self.publishes.get(outcome).increment(1);
    }
}
