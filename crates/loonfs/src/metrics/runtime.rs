//! The instruments that one runtime reports through.

use super::labels::{LabeledCounters, MetricLabel};
use super::object_store::ObjectStoreInstruments;
use super::{CounterHandle, MetricsRecorder, NoopMetricsRecorder, ObjectStoreMetricsRecorder};
use crate::maintenance::metrics::{CompactionInstruments, GcInstruments};
use crate::publisher::metrics::PublisherInstruments;
use loonfs_core::DiffPage;
use loonfs_types::u64_from_usize;
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// Where a read took the head it started from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadHeadSource {
    /// The position of the namespace's writer session in this runtime.
    Position,
    /// The namespace's cached head, refreshed for the read.
    Refreshed,
    /// The namespace's head loaded from the store, because the read cache
    /// had none.
    Loaded,
    /// The namespace's cached head as it was, for an eventual read within
    /// its maximum staleness, with no request to confirm the head.
    Eventual,
}

impl MetricLabel for ReadHeadSource {
    const VALUES: &'static [Self] = &[
        Self::Position,
        Self::Refreshed,
        Self::Loaded,
        Self::Eventual,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Position => "position",
            Self::Refreshed => "refreshed",
            Self::Loaded => "loaded",
            Self::Eventual => "eventual",
        }
    }
}

/// Every instrument one runtime reports.
///
/// A runtime built without a recorder reports into a
/// [`NoopMetricsRecorder`].
pub(crate) struct RuntimeInstruments {
    /// The object-store instruments, present when the host gave the runtime
    /// a recorder. A runtime without one doesn't wrap its object store, so
    /// its object-store calls go to the store directly.
    object_store: Option<Arc<ObjectStoreInstruments>>,
    pub(crate) compactions: CompactionInstruments,
    pub(crate) publisher: PublisherInstruments,
    pub(crate) gc: GcInstruments,
    read_head_sources: LabeledCounters<ReadHeadSource>,
    diff_commits_scanned: Arc<dyn CounterHandle>,
    diff_touches_examined: Arc<dyn CounterHandle>,
    diff_entries_emitted: Arc<dyn CounterHandle>,
    diff_path_resolutions: Arc<dyn CounterHandle>,
}

impl RuntimeInstruments {
    /// Builds the instrument set a handle reports through, registering the
    /// fully-labeled ones now and the rest as their labels first appear.
    pub(crate) fn new(recorder: Option<Arc<dyn MetricsRecorder>>) -> Arc<Self> {
        let object_store = recorder
            .as_ref()
            .map(|recorder| Arc::new(ObjectStoreInstruments::new(Arc::clone(recorder))));
        let recorder = recorder.unwrap_or_else(|| Arc::new(NoopMetricsRecorder));
        Arc::new(Self {
            object_store,
            compactions: CompactionInstruments::register(recorder.as_ref()),
            publisher: PublisherInstruments::register(recorder.as_ref()),
            gc: GcInstruments::register(recorder.as_ref()),
            read_head_sources: LabeledCounters::register(
                recorder.as_ref(),
                "loonfs.read_head.source",
                "Reads by where they took the head they started from",
                "source",
                &[],
            ),
            diff_commits_scanned: recorder.register_counter(
                "loonfs.diff.commits_scanned",
                "Commits that diff pages read",
                &[],
            ),
            diff_touches_examined: recorder.register_counter(
                "loonfs.diff.touches_examined",
                "Inodes that the commits of diff pages touched, once for each commit",
                &[],
            ),
            diff_entries_emitted: recorder.register_counter(
                "loonfs.diff.entries_emitted",
                "Entries that diff pages returned",
                &[],
            ),
            diff_path_resolutions: recorder.register_counter(
                "loonfs.diff.path_resolutions",
                "Paths that diff pages resolved",
                &[],
            ),
        })
    }

    /// Records where one read took the head it started from.
    pub(crate) fn read_head(&self, source: ReadHeadSource) {
        self.read_head_sources.get(source).increment(1);
    }

    /// Records the work of one diff page.
    pub(crate) fn diff_page(&self, page: &DiffPage) {
        self.diff_commits_scanned.increment(page.commits_scanned);
        self.diff_touches_examined.increment(page.touches_examined);
        self.diff_entries_emitted
            .increment(u64_from_usize(page.response.entries.len()));
        self.diff_path_resolutions.increment(page.path_resolutions);
    }

    /// An object-store recorder that bridges samples into these
    /// instruments, or `None` for a runtime built without a recorder.
    pub(crate) fn object_store_recorder(&self) -> Option<Arc<dyn ObjectStoreMetricsRecorder>> {
        self.object_store
            .clone()
            .map(|instruments| instruments as Arc<dyn ObjectStoreMetricsRecorder>)
    }
}
