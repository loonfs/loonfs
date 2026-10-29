//! The instruments of compaction units and garbage collection passes.

use crate::metrics::labels::{LabeledCounters, MetricLabel};
use crate::metrics::{
    seconds_from_ms, CounterHandle, HistogramHandle, MetricsRecorder, LATENCY_SECONDS_BOUNDARIES,
};
use crate::GcResponse;
use loonfs_core::CompactionUnitOutcome;
use std::sync::Arc;

/// The closed outcome vocabulary for a finished compaction unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionOutcome {
    Completed,
    Failed,
    Abandoned,
    Cancelled,
    Fenced,
}

impl MetricLabel for CompactionOutcome {
    const VALUES: &'static [Self] = &[
        Self::Completed,
        Self::Failed,
        Self::Abandoned,
        Self::Cancelled,
        Self::Fenced,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
            Self::Cancelled => "cancelled",
            Self::Fenced => "fenced",
        }
    }
}

/// One reclaimable family: its label, and the count a pass reports for it.
type GcKind = (&'static str, fn(&GcResponse) -> u64);

/// Counts each deletion once, with pins grouped by owner.
const GC_CATEGORIES: [GcKind; 9] = [
    ("deleted_wal_objects", |gc| gc.deleted.wal_objects),
    ("deleted_metadata_segments", |gc| {
        gc.deleted.metadata_segments
    }),
    ("deleted_manifests", |gc| gc.deleted.manifests),
    ("deleted_fork_checkpoints", |gc| {
        gc.deleted_checkpoints_by_owner.fork
    }),
    ("deleted_expired_checkpoints", |gc| {
        gc.deleted_checkpoints_by_owner.user
    }),
    ("deleted_upload_sessions", |gc| gc.deleted.upload_sessions),
    ("deleted_content_objects", |gc| gc.deleted.content_objects),
    ("deleted_temporary_objects", |gc| {
        gc.deleted.temporary_objects
    }),
    ("deleted_retired_content_objects", |gc| {
        gc.deleted.retired_content_objects
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

    /// Reports one finished compaction unit and any merge it completed. A
    /// unit that found nothing to merge, or a window too large for its call,
    /// reports nothing.
    pub(crate) fn finished(&self, outcome: &crate::Result<CompactionUnitOutcome>, elapsed_ms: u64) {
        let (outcome, totals) = match outcome {
            Ok(CompactionUnitOutcome::NotNeeded | CompactionUnitOutcome::TooLarge { .. }) => return,
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
            Ok(CompactionUnitOutcome::Fenced) => (CompactionOutcome::Fenced, None),
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
    retained: Arc<dyn CounterHandle>,
}

impl GcInstruments {
    pub(crate) fn register(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            reclaimed: GC_CATEGORIES
                .iter()
                .map(|(category, _)| {
                    recorder.register_counter(
                        "loonfs.gc.reclaimed",
                        "Objects one collection pass reclaimed, by family",
                        &[("category", *category)],
                    )
                })
                .collect(),
            retained: recorder.register_counter(
                "loonfs.gc.retained",
                "Candidates a collection pass retained rather than deleting",
                &[],
            ),
        }
    }

    /// Reports what one collection pass reclaimed and retained.
    ///
    /// Every runtime collection path records at the shared maintenance pass before
    /// its caller can drop the response.
    pub(crate) fn pass(&self, gc: &GcResponse) {
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
}
