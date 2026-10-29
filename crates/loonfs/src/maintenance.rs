//! The maintenance capability of a writable runtime: namespace diagnostics,
//! operator checkpoints, WAL folds, metadata compaction, garbage
//! collection, and retention.
//!
//! Derived indexes are not here and not in this crate: `loonfs-grep`
//! builds and collects its own state through the runtime's public snapshot
//! calls, and its hosts drive it.

use crate::pagers::{decode_page_request, encode_next_cursor, CheckpointsPager};
use crate::publisher::{NamespaceChange, PublisherRegistry};
use crate::runtime_core::RuntimeCore;
use crate::trace::{self, operation_span};
use crate::{
    AdvanceRetentionFloorOptions, AdvanceRetentionResponse, Checkpoint, FoldWalResponse,
    ListCheckpointsResponse, MaintainMetadataOptions, MetadataMaintenanceResponse,
    NamespaceDiagnostics, NamespaceId, PinId, SetRetentionWindowResponse, WriterState,
};
use crate::{Error, Result};
use loonfs_core::control::NamespaceStorageDiagnostics;
use loonfs_core::{CheckpointPageCursor, MetadataCompactionCancellation};
use loonfs_types::{u64_from_usize, PageRequest, WriterId};
use std::num::NonZeroU64;
use std::sync::Arc;

mod compactor;
mod floor_acknowledgment;
pub(crate) mod metrics;
mod reports;
#[cfg(test)]
mod tests;

pub(crate) use compactor::{Compactor, TailFold};
pub use floor_acknowledgment::{
    AcknowledgeRetentionFloorOutcome, AcknowledgeRetentionFloorReport, RetentionFloors,
};
pub use reports::{
    MetadataCompactionOutcome, MetadataCompactionReport, MetadataPassReport, WalFoldStepOutcome,
};

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
    pub(crate) compactor: Compactor,
}

impl Maintenance {
    pub(crate) fn new(
        core: RuntimeCore,
        publisher: PublisherRegistry,
        writer: Arc<WriterState>,
        writer_id: WriterId,
    ) -> Self {
        Self {
            compactor: Compactor::new(core, publisher, writer, writer_id),
        }
    }

    /// Narrows the rows that a `metadata` pass this value drives admits in
    /// one compaction unit, so a namespace a test can build in seconds has a
    /// window too large for one pass.
    ///
    /// Test-only, and the one shipped number that has to move to reach that
    /// state: planning, running, and publishing the unit are the shipped path
    /// either way.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn starve_compaction_row_budget(
        mut self,
        max_decoded_input_rows_per_step: std::num::NonZeroUsize,
    ) -> Self {
        self.compactor.compaction_row_budget = Some(max_decoded_input_rows_per_step);
        self
    }

    /// Narrows the rows one segment this value's compaction writes may contain,
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
        self.compactor.segment_row_budget = Some(max_rows_per_segment);
        self
    }

    /// Reads the runtime's wall clock as unix milliseconds.
    pub fn now_ms(&self) -> u64 {
        self.compactor.core.now_ms()
    }

    /// Returns namespace state and storage details used by maintenance.
    pub async fn diagnostics(&self, namespace_id: &NamespaceId) -> Result<NamespaceDiagnostics> {
        trace::in_operation(
            operation_span!(self.compactor.core, "diagnostics", namespace_id),
            async {
                let _permit = self.compactor.core.read_permit().await;
                let diagnostics = self
                    .compactor
                    .core
                    .namespace_store(namespace_id)
                    .load_diagnostics()
                    .await?;
                let live_snapshots = self.count_live_snapshots(namespace_id).await?;
                Ok(Self::namespace_diagnostics(diagnostics, live_snapshots))
            },
        )
        .await
    }

    async fn count_live_snapshots(&self, namespace_id: &NamespaceId) -> Result<u64> {
        let now_ms = self.compactor.core.now_ms();
        let mut cursor = None;
        let mut live_snapshots = 0_u64;
        loop {
            let page = self
                .compactor
                .engine(namespace_id)
                .list_snapshots_page(PageRequest {
                    limit: loonfs_types::EffectiveLimit::MAX,
                    cursor,
                })
                .await
                .map_err(Error::from)?;
            let live = page
                .items
                .iter()
                .filter(|snapshot| snapshot.is_live(now_ms))
                .count();
            live_snapshots = live_snapshots.saturating_add(u64_from_usize(live));
            let Some(next_cursor) = page.next_cursor else {
                return Ok(live_snapshots);
            };
            cursor = Some(next_cursor);
        }
    }

    fn namespace_diagnostics(
        diagnostics: NamespaceStorageDiagnostics,
        live_snapshots: u64,
    ) -> NamespaceDiagnostics {
        NamespaceDiagnostics {
            namespace_id: diagnostics.namespace_id,
            created_at_ms: diagnostics.created_at_ms,
            created_by: diagnostics.created_by,
            fork_basis: diagnostics.fork_basis,
            head_seq: diagnostics.head_seq,
            retention_floor_seq: diagnostics.retention_floor_seq,
            acknowledged_floor_seq: diagnostics.acknowledged_floor_seq,
            current_manifest_no: Some(diagnostics.current_manifest_no),
            wal_tail_objects: diagnostics.wal_tail_objects,
            live_snapshots,
        }
    }

    /// Runs [`Self::maintain_metadata_with_options`] with the default
    /// thresholds.
    pub async fn maintain_metadata(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<MetadataPassReport> {
        self.maintain_metadata_with_options(namespace_id, &MaintainMetadataOptions::default())
            .await
    }

    /// Folds the WAL tail at `max_wal_tail_objects` WAL objects, at the
    /// runtime's inline fold size when this runtime's writer session records
    /// the tail's inline bytes, or once the tail's newest commit reaches the
    /// runtime's idle fold age on this handle's wall clock (see
    /// [`RuntimeConfig::idle_fold_after_ms`](crate::RuntimeConfig::idle_fold_after_ms)).
    /// Then runs one compaction unit whose window lists at most the run
    /// policy's `max_decoded_input_rows_per_step` rows, and reports a larger
    /// window as [`MetadataCompactionOutcome::MetadataCompactionRequired`]. A
    /// fold waits for a fold permit from the runtime's execution budget, and a
    /// unit that finds work waits for a compaction permit from it.
    pub async fn maintain_metadata_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &MaintainMetadataOptions,
    ) -> Result<MetadataPassReport> {
        trace::in_operation(
            operation_span!(self.compactor.core, "maintain_metadata", namespace_id),
            async {
                let step = self
                    .compactor
                    .metadata_step(
                        namespace_id,
                        TailFold::from_options(options),
                        options.compaction_policy,
                        &MetadataCompactionCancellation::default(),
                    )
                    .await?;
                Ok(step
                    .expect("a pass without a cancellation runs to its end")
                    .response)
            },
        )
        .await
    }

    /// Runs [`Self::maintain_metadata_while_due_with_options`] with the
    /// default thresholds.
    pub async fn maintain_metadata_while_due(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataMaintenanceResponse> {
        self.maintain_metadata_while_due_with_options(
            namespace_id,
            cancellation,
            &MaintainMetadataOptions::default(),
        )
        .await
    }

    /// Repeats [`Self::maintain_metadata_with_options`] while it finds
    /// compaction due. When a pass reports a window too large for it, runs
    /// [`Self::compact_metadata`].
    ///
    /// Stops when nothing is due, when another process has the compactor
    /// epoch, when `cancellation` is set, after three abandoned units in a
    /// row, or after it publishes 16 compaction units. Work still due after
    /// the 16th unit waits for the next call. A unit that loses its manifest
    /// put to another publication tries again over the manifest that won, so
    /// only an abandoned unit counts as a lost race. Setting `cancellation`
    /// ends a wait for a permit and drops a fold in progress at once, and a
    /// running compaction unit stops at its next block. A dropped call
    /// leaves what a crash would: unreferenced segments, or a manifest the
    /// runtime learns of the way it learns of another process's. Fails with
    /// the first error. Keeps no state between calls.
    ///
    /// The response reports the compaction units the call published, and an
    /// outcome. The outcome is [`MetadataMaintenanceOutcome::CaughtUp`] when
    /// nothing is due now and nothing can become due without a new commit
    /// under `options`, based on the state this call read. It is
    /// [`MetadataMaintenanceOutcome::Due`] at a work limit, after lost races,
    /// on fencing or cancellation, when a compaction unit does not finish,
    /// or while a tail waits for the idle fold age.
    pub async fn maintain_metadata_while_due_with_options(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
        options: &MaintainMetadataOptions,
    ) -> Result<MetadataMaintenanceResponse> {
        // Boxing erases the loop's nested step futures. Without it, a host
        // that awaits this call inside its own scheduling futures exceeds
        // rustc's type-recursion depth.
        Box::pin(self.compactor.metadata_while_due(
            namespace_id,
            cancellation.metadata_compaction(),
            TailFold::from_options(options),
            options.compaction_policy,
        ))
        .await
    }

    /// Runs one metadata compaction unit in the caller's task, whatever the
    /// size of its window.
    ///
    /// Use this when [`MetadataCompactionOutcome::MetadataCompactionRequired`]
    /// is reported, and repeat it while it publishes to compact every
    /// eligible group. The unit waits for a compaction permit from the
    /// runtime's execution budget.
    ///
    /// Cancelling `cancellation` ends the wait for a permit and reports the
    /// unit cancelled. A running unit stops at its next block.
    pub async fn compact_metadata(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataCompactionReport> {
        trace::in_operation(
            operation_span!(self.compactor.core, "compact_metadata", namespace_id),
            self.compactor
                .run_compaction_unit(namespace_id, cancellation.metadata_compaction()),
        )
        .await
    }

    /// Runs [`Self::gc_with_options`] with the default grace window.
    pub async fn gc(&self, namespace_id: &NamespaceId) -> Result<crate::GcResponse> {
        self.gc_with_options(namespace_id, &crate::GcOptions::default())
            .await
    }

    /// Runs one complete garbage-collection pass for one namespace.
    ///
    /// Every call rebuilds the current live roots and keeps no cursor. A pass
    /// runs only when asked. A deleted namespace is collected, which is how
    /// its objects are reclaimed; a namespace that does not exist returns
    /// `namespace_not_found`.
    pub async fn gc_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &crate::GcOptions,
    ) -> Result<crate::GcResponse> {
        trace::in_operation(
            operation_span!(self.compactor.core, "gc", namespace_id),
            async {
                let namespace = self.compactor.core.namespace_store(namespace_id);
                let report = namespace
                    .gc(
                        Some(self.compactor.core.completed_upload_sessions()),
                        options,
                        &namespace.start_clock(),
                    )
                    .await
                    .map_err(Error::from)?;
                // A caller may drop the response, so its counts survive only when
                // this shared pass records them here.
                self.compactor.core.instruments().gc.pass(&report);
                // A pass publishes no manifest, so the head didn't move. The
                // publisher keeps its position, and the cached head stays confirmed.
                // The collection grace covers the revalidation bound, so a head
                // inside its bound never lists a segment that a pass deleted
                // (format.md sections 11.2 and 11.4).
                self.compactor
                    .publisher
                    .invalidate_namespace(namespace_id, NamespaceChange::Unchanged)
                    .await;
                Ok(report)
            },
        )
        .await
    }

    /// Lists every pin as a checkpoint, in ascending id order, including
    /// fork pins and expired snapshots that garbage collection has not yet
    /// deleted. This is the inventory of what keeps manifests from
    /// collection. The cursor resumes a live listing.
    pub fn list_checkpoints(&self, namespace_id: &NamespaceId) -> CheckpointsPager {
        loonfs_types::Pager::with_state(
            (self.clone(), namespace_id.clone()),
            |(maintenance, namespace_id), request| async move {
                maintenance
                    .checkpoints_page(&namespace_id, decode_page_request(request)?)
                    .await
            },
        )
    }

    async fn checkpoints_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<CheckpointPageCursor>,
    ) -> Result<ListCheckpointsResponse> {
        trace::in_operation(
            operation_span!(self.compactor.core, "list_checkpoints", namespace_id),
            async {
                let _permit = self.compactor.core.read_permit().await;
                let page = self
                    .compactor
                    .engine(namespace_id)
                    .list_checkpoints_page(request)
                    .await
                    .map_err(Error::from)?;
                Ok(ListCheckpointsResponse {
                    namespace_id: namespace_id.clone(),
                    checkpoints: page.items,
                    next_cursor: encode_next_cursor(page.next_cursor.as_ref())?,
                })
            },
        )
        .await
    }

    /// Returns one existing pin of any owner, as the checkpoint listing
    /// returns it, including an expired snapshot that garbage collection has
    /// not yet deleted. A missing id returns `snapshot_not_found`.
    pub async fn get_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &PinId,
    ) -> Result<Checkpoint> {
        trace::in_operation(
            operation_span!(self.compactor.core, "get_checkpoint", namespace_id),
            async {
                let _permit = self.compactor.core.read_permit().await;
                self.compactor
                    .engine(namespace_id)
                    .get_checkpoint(checkpoint_id)
                    .await
                    .map_err(Error::from)
            },
        )
        .await
    }

    /// Folds the visible WAL tail into a new manifest.
    ///
    /// A namespace with no unfolded tail reports
    /// [`FoldWalOutcome::AlreadyCurrent`] and publishes nothing. The fold
    /// waits for a fold permit from the runtime's execution budget. This runs
    /// no compaction; [`Self::maintain_metadata`] folds and then runs one
    /// compaction unit.
    pub async fn fold_wal(&self, namespace_id: &NamespaceId) -> Result<FoldWalResponse> {
        trace::in_operation(
            operation_span!(self.compactor.core, "fold_wal", namespace_id),
            self.compactor.run_wal_fold(namespace_id),
        )
        .await
    }

    /// Runs [`Self::advance_retention_floor_with_options`] toward the folded
    /// manifest head.
    pub async fn advance_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AdvanceRetentionResponse> {
        self.advance_retention_floor_with_options(
            namespace_id,
            &AdvanceRetentionFloorOptions::default(),
        )
        .await
    }

    /// Advances the namespace retention floor toward `options.target` when a
    /// verified checkpoint makes it safe.
    ///
    /// Whatever the target, the floor doesn't pass a commit dated less than
    /// [`GC_DEFAULT_GRACE_WINDOW_MS`](crate::GC_DEFAULT_GRACE_WINDOW_MS)
    /// before the runtime's wall clock (format spec, "Advancing the retention
    /// floor"), and the response includes the floor that the advance reached.
    /// Advancing the floor abandons the replay history below it, and a later
    /// base compaction keeps, for each file, every revision above the floor
    /// and the newest revision at or below it. The runtime doesn't schedule
    /// an advance. The reference server makes one on each full maintenance
    /// pass, toward [`RetentionTarget::Window`](crate::RetentionTarget::Window)
    /// with its `retention_window_ms` as the default window, so a namespace
    /// with neither its own window nor that setting keeps its whole history.
    pub async fn advance_retention_floor_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &AdvanceRetentionFloorOptions,
    ) -> Result<AdvanceRetentionResponse> {
        trace::in_operation(
            operation_span!(self.compactor.core, "advance_retention_floor", namespace_id),
            async {
                let result = self
                    .compactor
                    .engine(namespace_id)
                    .advance_retention_floor(options.target)
                    .await
                    .map_err(Error::from);
                self.compactor
                    .publisher
                    .finish_publication(namespace_id, result)
                    .await
            },
        )
        .await
    }

    /// Sets the namespace's retention window.
    ///
    /// `None` clears the window, so a host's scheduled advance uses the
    /// host's own setting again.
    ///
    /// The call publishes a manifest that changes nothing else, and if the
    /// namespace already has the window, it publishes nothing. See
    /// [Advancing the retention
    /// floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor).
    pub async fn set_retention_window(
        &self,
        namespace_id: &NamespaceId,
        retention_window_ms: Option<NonZeroU64>,
    ) -> Result<SetRetentionWindowResponse> {
        trace::in_operation(
            operation_span!(self.compactor.core, "set_retention_window", namespace_id),
            async {
                let result = self
                    .compactor
                    .engine(namespace_id)
                    .set_retention_window(retention_window_ms)
                    .await
                    .map_err(Error::from);
                self.compactor
                    .publisher
                    .finish_publication(namespace_id, result)
                    .await
            },
        )
        .await
    }
}

/// Stops a maintenance call that takes it, such as
/// [`Maintenance::maintain_metadata_while_due`] or
/// [`Maintenance::compact_metadata`]. Clones share one
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
