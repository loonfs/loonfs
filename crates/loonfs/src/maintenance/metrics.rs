//! The instruments of compaction units and garbage collections.

use crate::metrics::labels::{LabeledCounters, MetricLabel};
use crate::metrics::{
    seconds_from_ms, CounterHandle, HistogramHandle, MetricsRecorder, LATENCY_SECONDS_BOUNDARIES,
};
use crate::{GcResponse, PoolGcResponse};
use loonfs_core::CompactionUnitOutcome;
use std::sync::Arc;

/// The closed outcome vocabulary for a finished compaction unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionOutcome {
    Completed,
    Failed,
    Abandoned,
    Cancelled,
}

impl MetricLabel for CompactionOutcome {
    const VALUES: &'static [Self] = &[
        Self::Completed,
        Self::Failed,
        Self::Abandoned,
        Self::Cancelled,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
            Self::Cancelled => "cancelled",
        }
    }
}

/// One reclaimable family: its label, and the count a collection reports for it.
type GcKind<R> = (&'static str, fn(&R) -> u64);

/// Counts each deletion of a namespace's collection once, with snapshots grouped by
/// owner.
const GC_CATEGORIES: [GcKind<GcResponse>; 6] = [
    ("deleted_wal_objects", |gc| gc.deleted.wal_objects),
    ("deleted_metadata_segments", |gc| {
        gc.deleted.metadata_segments
    }),
    ("deleted_extension_segments", |gc| {
        gc.deleted.extension_segments
    }),
    ("deleted_manifests", |gc| gc.deleted.manifests),
    ("deleted_fork_snapshots", |gc| {
        gc.deleted_snapshots_by_owner.fork
    }),
    ("deleted_expired_snapshots", |gc| {
        gc.deleted_snapshots_by_owner.user
    }),
];

/// Counts each deletion of a pool's collection once.
const POOL_GC_CATEGORIES: [GcKind<PoolGcResponse>; 3] = [
    ("deleted_upload_sessions", |gc| gc.deleted.upload_sessions),
    ("deleted_content_objects", |gc| gc.deleted.content_objects),
    ("deleted_temporary_objects", |gc| {
        gc.deleted.temporary_objects
    }),
];

/// The gauges and durable totals for this process's compaction units.
pub(crate) struct CompactionInstruments {
    outcomes: LabeledCounters<CompactionOutcome>,
    outcome_seconds: Vec<Arc<dyn HistogramHandle>>,
    input_rows: Arc<dyn CounterHandle>,
    output_rows: Arc<dyn CounterHandle>,
    input_bytes: Arc<dyn CounterHandle>,
    output_bytes: Arc<dyn CounterHandle>,
}

impl CompactionInstruments {
    pub(crate) fn register(recorder: &dyn MetricsRecorder) -> Self {
        let rows = |direction: &'static str| {
            recorder.register_counter(
                "loonfs.maintenance.compaction_rows",
                "Rows processed by metadata compaction units",
                &[("direction", direction)],
            )
        };
        let bytes = |direction: &'static str| {
            recorder.register_counter(
                "loonfs.maintenance.compaction_bytes",
                "Bytes processed by metadata compaction units",
                &[("direction", direction)],
            )
        };
        Self {
            outcomes: LabeledCounters::register(
                recorder,
                "loonfs.maintenance.compactions",
                "Metadata compaction units by outcome",
                "outcome",
                &[],
            ),
            outcome_seconds: CompactionOutcome::VALUES
                .iter()
                .map(|outcome| {
                    recorder.register_histogram(
                        "loonfs.maintenance.compaction_seconds",
                        "Duration of a finished metadata compaction unit in seconds",
                        &[("outcome", outcome.as_str())],
                        LATENCY_SECONDS_BOUNDARIES,
                    )
                })
                .collect(),
            input_rows: rows("input"),
            output_rows: rows("output"),
            input_bytes: bytes("input"),
            output_bytes: bytes("output"),
        }
    }

    fn outcome_seconds(&self, outcome: CompactionOutcome) -> &Arc<dyn HistogramHandle> {
        &self.outcome_seconds[outcome.index()]
    }

    /// Reports one finished compaction unit and any window it compacted. A
    /// unit that found nothing to compact reports nothing.
    pub(crate) fn finished(&self, outcome: &crate::Result<CompactionUnitOutcome>, elapsed_ms: u64) {
        let (outcome, totals) = match outcome {
            Ok(CompactionUnitOutcome::NotNeeded) => return,
            Ok(CompactionUnitOutcome::Published {
                rows_read,
                rows_written,
                input_bytes,
                output_bytes,
                ..
            }) => (
                CompactionOutcome::Completed,
                Some((*rows_read, *rows_written, *input_bytes, *output_bytes)),
            ),
            Ok(CompactionUnitOutcome::Abandoned) => (CompactionOutcome::Abandoned, None),
            Ok(CompactionUnitOutcome::Cancelled) => (CompactionOutcome::Cancelled, None),
            Err(_) => (CompactionOutcome::Failed, None),
        };
        self.record(outcome, elapsed_ms, totals);
    }

    fn record(
        &self,
        outcome: CompactionOutcome,
        elapsed_ms: u64,
        totals: Option<(u64, u64, u64, u64)>,
    ) {
        self.outcomes.get(outcome).increment(1);
        self.outcome_seconds(outcome)
            .record(seconds_from_ms(elapsed_ms));
        let Some((input_rows, output_rows, input_bytes, output_bytes)) = totals else {
            return;
        };
        self.input_rows.increment(input_rows);
        self.output_rows.increment(output_rows);
        self.input_bytes.increment(input_bytes);
        self.output_bytes.increment(output_bytes);
    }
}

/// Collection instruments, one counter per reclaimable family.
pub(crate) struct GcInstruments {
    reclaimed: Vec<Arc<dyn CounterHandle>>,
    pool_reclaimed: Vec<Arc<dyn CounterHandle>>,
    retained: Arc<dyn CounterHandle>,
}

impl GcInstruments {
    pub(crate) fn register(recorder: &dyn MetricsRecorder) -> Self {
        let reclaimed = |category: &'static str| {
            recorder.register_counter(
                "loonfs.gc.reclaimed",
                "Objects one collection reclaimed, by family",
                &[("category", category)],
            )
        };
        Self {
            reclaimed: GC_CATEGORIES
                .iter()
                .map(|(category, _)| reclaimed(category))
                .collect(),
            pool_reclaimed: POOL_GC_CATEGORIES
                .iter()
                .map(|(category, _)| reclaimed(category))
                .collect(),
            retained: recorder.register_counter(
                "loonfs.gc.retained",
                "Candidates a collection retained rather than deleting",
                &[],
            ),
        }
    }

    /// Reports what one collection reclaimed and retained.
    ///
    /// Every runtime collection path records its counts here before its
    /// caller can drop the response.
    pub(crate) fn record_collection(&self, gc: &GcResponse) {
        for (category, count) in GC_CATEGORIES.iter().zip(self.reclaimed.iter()) {
            let reclaimed = (category.1)(gc);
            if reclaimed > 0 {
                count.increment(reclaimed);
            }
        }
        if gc.retained.total() > 0 {
            self.retained.increment(gc.retained.total());
        }
    }

    /// Reports what one collection of a pool reclaimed and retained.
    ///
    /// Every runtime collection path records its counts here before its
    /// caller can drop the response.
    pub(crate) fn record_pool_collection(&self, gc: &PoolGcResponse) {
        for (category, count) in POOL_GC_CATEGORIES.iter().zip(self.pool_reclaimed.iter()) {
            let reclaimed = (category.1)(gc);
            if reclaimed > 0 {
                count.increment(reclaimed);
            }
        }
        if gc.retained.total() > 0 {
            self.retained.increment(gc.retained.total());
        }
    }
}
