//! The maintenance capability of a writable runtime.

use crate::fs::{RuntimeCore, WriterBits, WriterIdentity};
use crate::publisher::PublisherRegistry;
use crate::Result;
use std::sync::Arc;

/// Explicit maintenance: namespace diagnostics, operator checkpoints, WAL
/// folds, metadata compaction, garbage collection, and retention.
///
/// Get one from [`LoonFs::maintenance`](crate::LoonFs::maintenance). Each
/// call runs in the caller's task; this value starts no background work.
/// Operations that mutate durable control state record the writer id it was
/// created with. Every value from one runtime shares that runtime's
/// compactor claim and execution budget with its writer sessions.
#[derive(Clone)]
pub struct Maintenance {
    pub(crate) core: RuntimeCore,
    pub(crate) publisher: PublisherRegistry,
    pub(crate) writer: Arc<WriterBits>,
    pub(crate) actor: WriterIdentity,
    /// A narrowed per-step row budget for the tests that need a family group
    /// whose base run no bounded step can compact. See
    /// [`Self::starve_compaction_row_budget`].
    #[cfg(test)]
    pub(crate) compaction_row_budget: Option<std::num::NonZeroUsize>,
    /// A narrowed per-segment row budget for the tests that need many
    /// compacted segments. See [`Self::narrow_segment_row_budget`].
    #[cfg(test)]
    pub(crate) segment_row_budget: Option<std::num::NonZeroUsize>,
}

impl Maintenance {
    pub(crate) fn new(
        core: RuntimeCore,
        publisher: PublisherRegistry,
        writer: Arc<WriterBits>,
        actor: WriterIdentity,
    ) -> Self {
        Self {
            core,
            publisher,
            writer,
            actor,
            #[cfg(test)]
            compaction_row_budget: None,
            #[cfg(test)]
            segment_row_budget: None,
        }
    }

    /// Narrows the rows one compaction step this value drives may decode, so
    /// a namespace a test can build in seconds ends up with a base run no
    /// bounded step can compact.
    ///
    /// Test-only, and the one shipped number that has to move to reach that
    /// state: planning, running, and publishing the job are the shipped path
    /// either way.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn starve_compaction_row_budget(
        mut self,
        max_decoded_input_rows_per_step: std::num::NonZeroUsize,
    ) -> Self {
        self.compaction_row_budget = Some(max_decoded_input_rows_per_step);
        self
    }

    /// Narrows the rows one segment this value's compaction writes may hold,
    /// so a namespace a test can build in seconds compacts into many
    /// segments.
    ///
    /// Test-only: folds keep the shipped segment shape, and planning and
    /// publishing are the shipped path either way.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn narrow_segment_row_budget(
        mut self,
        max_rows_per_segment: std::num::NonZeroUsize,
    ) -> Self {
        self.segment_row_budget = Some(max_rows_per_segment);
        self
    }

    /// Reads the runtime's wall clock as unix milliseconds.
    pub fn now_ms(&self) -> Result<u64> {
        self.core.now_ms()
    }

    // Maintenance operations live in `fs/maintenance.rs`.
}

/// Stops a maintenance call that takes it, such as
/// [`Maintenance::maintain_metadata_while_due`] or
/// [`Maintenance::compact_metadata_with_cancellation`]. Clones share one
/// state.
#[derive(Debug, Clone, Default)]
pub struct MaintenanceCancellation(loonfs_core::MetadataCompactionCancellation);

impl MaintenanceCancellation {
    /// Creates an uncancelled token.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancels the token.
    pub fn cancel(&self) {
        self.0.cancel();
    }

    /// Returns whether the token was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    /// Waits until the token is cancelled.
    pub async fn cancelled(&self) {
        self.0.cancelled().await;
    }

    pub(crate) fn metadata_compaction(&self) -> &loonfs_core::MetadataCompactionCancellation {
        &self.0
    }
}
