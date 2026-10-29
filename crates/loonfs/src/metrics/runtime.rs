//! The instruments that one runtime reports through.

use super::labels::{LabeledCounters, MetricLabel};
use super::object_store::ObjectStoreInstruments;
use super::{MetricsRecorder, NoopMetricsRecorder, ObjectStoreMetricsRecorder};
use crate::maintenance::metrics::{CompactionInstruments, GcInstruments};
use crate::publisher::metrics::PublisherInstruments;
use std::sync::Arc;

#[cfg(test)]
mod tests;

/// Where a read took the head it started from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadHeadSource {
    /// The position of the namespace's writer session in this runtime.
    Position,
    /// The namespace's cached head, confirmed or loaded for the read.
    Anchor,
    /// The namespace's cached head as it was, for an eventual read within
    /// its maximum staleness, with no request to confirm the head.
    Eventual,
}

impl MetricLabel for ReadHeadSource {
    const VALUES: &'static [Self] = &[Self::Position, Self::Anchor, Self::Eventual];

    fn as_str(self) -> &'static str {
        match self {
            Self::Position => "position",
            Self::Anchor => "anchor",
            Self::Eventual => "eventual",
        }
    }
}

/// Every instrument one runtime reports.
///
/// A runtime built without a recorder reports into a
/// [`NoopMetricsRecorder`].
pub(crate) struct RuntimeInstruments {
    /// Whether the host gave the runtime a recorder. A runtime without one
    /// doesn't wrap its object store, so its object-store calls go to the
    /// store directly.
    wraps_object_store: bool,
    object_store: Arc<ObjectStoreInstruments>,
    pub(crate) compactions: CompactionInstruments,
    pub(crate) publisher: PublisherInstruments,
    pub(crate) gc: GcInstruments,
    read_head_sources: LabeledCounters<ReadHeadSource>,
}

impl RuntimeInstruments {
    /// Builds the instrument set a handle reports through, registering the
    /// fully-labeled ones now and the rest as their labels first appear.
    pub(crate) fn new(recorder: Option<Arc<dyn MetricsRecorder>>) -> Arc<Self> {
        let wraps_object_store = recorder.is_some();
        let recorder = recorder.unwrap_or_else(|| Arc::new(NoopMetricsRecorder));
        Arc::new(Self {
            wraps_object_store,
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
            object_store: Arc::new(ObjectStoreInstruments::new(recorder)),
        })
    }

    /// Records where one read took the head it started from.
    pub(crate) fn read_head(&self, source: ReadHeadSource) {
        self.read_head_sources.get(source).increment(1);
    }

    /// An object-store recorder that bridges samples into these
    /// instruments, or `None` for a runtime built without a recorder.
    pub(crate) fn object_store_recorder(&self) -> Option<Arc<dyn ObjectStoreMetricsRecorder>> {
        if !self.wraps_object_store {
            return None;
        }
        Some(Arc::clone(&self.object_store) as Arc<dyn ObjectStoreMetricsRecorder>)
    }
}
