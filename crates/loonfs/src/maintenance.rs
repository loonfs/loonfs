//! The maintenance capability of a writable runtime: namespace diagnostics,
//! operator snapshots, WAL folds, metadata compaction, garbage
//! collection, retention, and the publications of an extension's entry.
//!
//! Derived indexes aren't in this module or this crate. `loonfs-grep` builds
//! its own segments through the runtime's public reads and snapshot calls,
//! publishes its entry through [`Maintenance::publish_extension`], and its
//! hosts drive it.

use crate::pagers::{decode_page_request, encode_next_cursor, SnapshotRecordsPager};
use crate::publisher::{FoldWhen, NamespaceFold, PublisherRegistry};
use crate::runtime_core::RuntimeCore;
use crate::trace::{self, operation_span};
use crate::{
    AdvanceRetentionFloorOptions, AdvanceRetentionResponse, FoldWalResponse,
    ListSnapshotRecordsResponse, MaintainMetadataOptions, MetadataMaintenanceOutcome,
    MetadataMaintenanceResponse, NamespaceDiagnostics, NamespaceId, PoolId,
    SetRetentionWindowResponse, SnapshotId, SnapshotRecord, WriterState,
};
use crate::{Error, Result};
use loonfs_core::control::{LoadedManifest, NamespaceStorageDiagnostics};
use loonfs_core::{
    ExtensionBasis, ExtensionPublication, FloorObjectReport, MetadataCompactionCancellation,
    RetentionPlan, SnapshotRecordPageCursor,
};
use loonfs_types::format::manifest::ExtensionEntry;
use loonfs_types::{u64_from_usize, CommitSeq, ExtensionName, PageRequest, WriterId};
use std::num::NonZeroU64;
use std::sync::Arc;

mod compaction;
pub(crate) mod metrics;
mod reports;
#[cfg(test)]
mod tests;

pub use reports::{
    MaintainMetadataReport, MetadataCompactionOutcome, MetadataCompactionReport,
    MetadataMaintenanceReport,
};

/// Explicit maintenance: namespace diagnostics, operator snapshots, WAL
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
    pub(crate) writer: Arc<WriterState>,
    pub(crate) writer_id: WriterId,
    /// A narrowed per-segment row budget for the tests that need many
    /// compacted segments. See [`Self::narrow_segment_row_budget`].
    #[cfg(test)]
    segment_row_budget: Option<std::num::NonZeroUsize>,
}

impl Maintenance {
    /// Returns the maintenance of the runtime that `core`, `publisher`, and
    /// `writer` belong to, acting as `writer_id`.
    pub(crate) fn new(
        core: RuntimeCore,
        publisher: PublisherRegistry,
        writer: Arc<WriterState>,
        writer_id: WriterId,
    ) -> Self {
        Self {
            core,
            publisher,
            writer,
            writer_id,
            #[cfg(test)]
            segment_row_budget: None,
        }
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
        self.segment_row_budget = Some(max_rows_per_segment);
        self
    }

    /// Reads the runtime's wall clock as unix milliseconds.
    pub fn now_ms(&self) -> u64 {
        self.core.now_ms()
    }

    /// Returns namespace state and storage details used by maintenance.
    pub async fn diagnostics(&self, namespace_id: &NamespaceId) -> Result<NamespaceDiagnostics> {
        trace::in_operation(
            operation_span!(self.core, "diagnostics", namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let diagnostics = self
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
        let mut cursor = None;
        let mut live_snapshots = 0_u64;
        loop {
            let page = self
                .engine(namespace_id)
                .list_snapshots_page(PageRequest {
                    limit: loonfs_types::EffectiveLimit::MAX,
                    cursor,
                })
                .await
                .map_err(Error::from)?;
            live_snapshots = live_snapshots.saturating_add(u64_from_usize(page.items.len()));
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
    ) -> Result<MaintainMetadataReport> {
        self.maintain_metadata_with_options(namespace_id, &MaintainMetadataOptions::default())
            .await
    }

    /// Folds the WAL tail at `max_wal_tail_objects` WAL objects, at
    /// [`FOLD_AT_WAL_BYTES`](loonfs_core::limits::FOLD_AT_WAL_BYTES) of
    /// unfolded WAL bytes, or once the tail's newest commit reaches the
    /// runtime's idle fold age (see
    /// [`RuntimeConfig::idle_fold_after_ms`](crate::RuntimeConfig::idle_fold_after_ms)).
    /// The check reads the position of this runtime's writer session when
    /// the session has one, and otherwise the tail that it loads from the
    /// store.
    /// Then runs one size-tiered compaction unit when one is due, whatever
    /// the number of rows its window lists. A fold waits for a fold permit from the runtime's execution budget, and a
    /// unit that finds work waits for a compaction permit from it.
    pub async fn maintain_metadata_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &MaintainMetadataOptions,
    ) -> Result<MaintainMetadataReport> {
        trace::in_operation(
            operation_span!(self.core, "maintain_metadata", namespace_id),
            async {
                let (wal_fold, observed) = match self.fold_when_due(namespace_id, options).await? {
                    NamespaceFold::NotDue { observed, .. } => (None, observed),
                    NamespaceFold::Folded(folded) => (Some(folded.response), None),
                };
                let compaction = self
                    .compact_once(
                        namespace_id,
                        observed,
                        &MetadataCompactionCancellation::default(),
                    )
                    .await?;
                tracing::debug!(
                    ?wal_fold,
                    ?compaction,
                    "metadata maintenance step concluded"
                );
                Ok(MaintainMetadataReport {
                    namespace_id: namespace_id.clone(),
                    wal_fold,
                    compaction,
                })
            },
        )
        .await
    }

    /// Folds the namespace's WAL tail when a fold is due under `options`,
    /// through the namespace's one fold path.
    async fn fold_when_due(
        &self,
        namespace_id: &NamespaceId,
        options: &MaintainMetadataOptions,
    ) -> Result<NamespaceFold> {
        let when = FoldWhen::Due {
            max_wal_tail_objects: options.max_wal_tail_objects.get(),
        };
        self.publisher.fold_namespace(namespace_id, when).await
    }

    /// Runs [`Self::maintain_metadata_while_due_with_options`] with the
    /// default thresholds.
    pub async fn maintain_metadata_while_due(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataMaintenanceReport> {
        self.maintain_metadata_while_due_with_options(
            namespace_id,
            cancellation,
            &MaintainMetadataOptions::default(),
        )
        .await
    }

    /// Folds the WAL tail when a fold is due, as
    /// [`Self::maintain_metadata_with_options`] does, and then runs
    /// size-tiered compaction units while a unit is due. The fold
    /// waits for a fold of the namespace in progress, such as its writer
    /// session's, and each unit waits for a unit in progress, so the call
    /// never repeats that work.
    ///
    /// Stops when nothing is due, when another process has the compactor
    /// epoch, when `cancellation` is set, when the namespace's writer
    /// session in this runtime is closed or its namespace deleted, after three
    /// abandoned units in a row, or after it publishes 16 compaction units.
    /// Work still due after the 16th unit waits for the next call. A unit
    /// that loses its manifest put to another publication tries again over
    /// the manifest that won, so only an abandoned unit counts as a lost
    /// race. Setting `cancellation` ends a wait for a lock or a permit and
    /// drops a fold in progress at once, and a running compaction unit stops
    /// at its next block. A close or a delete of the session stops the call
    /// before its next unit. A dropped call leaves what a crash would:
    /// unreferenced segments, or a manifest the runtime learns of the way it
    /// learns of another process's. Fails with the first error. Keeps no
    /// state between calls.
    ///
    /// The response reports the compaction units the call published, and an
    /// outcome. The outcome is [`MetadataMaintenanceOutcome::CaughtUp`] when
    /// nothing is due now and nothing can become due without a new commit
    /// under `options`, based on the state this call read. It is
    /// [`MetadataMaintenanceOutcome::Due`] at a work limit, after lost races,
    /// on fencing or cancellation, when a compaction unit does not finish,
    /// or while a tail waits for the idle fold age. When the wait for the
    /// idle fold age is the only work left, the report says when the fold
    /// comes due.
    pub async fn maintain_metadata_while_due_with_options(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
        options: &MaintainMetadataOptions,
    ) -> Result<MetadataMaintenanceReport> {
        let cancellation = cancellation.metadata_compaction();
        let maintained = async {
            let fold = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Ok(MetadataMaintenanceReport {
                        response: MetadataMaintenanceResponse {
                            namespace_id: namespace_id.clone(),
                            outcome: MetadataMaintenanceOutcome::Due,
                            compaction_units: 0,
                            skipped: Vec::new(),
                        },
                        next_due_at_ms: None,
                    });
                }
                fold = self.fold_when_due(namespace_id, options) => fold?,
            };
            let (idle_fold_due_at_ms, observed) = match fold {
                NamespaceFold::NotDue {
                    idle_fold_due_at_ms,
                    observed,
                } => (idle_fold_due_at_ms, observed),
                NamespaceFold::Folded(_) => (None, None),
            };
            let mut response = self
                .compact_namespace(namespace_id, observed, cancellation)
                .await?;
            let next_due_at_ms = idle_fold_due_at_ms
                .filter(|_| response.outcome == MetadataMaintenanceOutcome::CaughtUp);
            if idle_fold_due_at_ms.is_some() {
                response.outcome = MetadataMaintenanceOutcome::Due;
            }
            Ok(MetadataMaintenanceReport {
                response,
                next_due_at_ms,
            })
        };
        // Boxing erases the loop's nested unit futures. Without it, a host
        // that awaits this call inside its own scheduling futures exceeds
        // rustc's type-recursion depth.
        Box::pin(trace::in_operation(
            operation_span!(self.core, "maintain_metadata_while_due", namespace_id),
            maintained,
        ))
        .await
    }

    /// Runs one metadata compaction unit in the caller's task when one is
    /// due under the namespace's run policy. The unit is the same size-tiered
    /// unit that [`Self::maintain_metadata`] runs after its fold check, and
    /// the call reports that no unit was needed when nothing is due.
    ///
    /// Repeat the call while it publishes to compact every due window. The
    /// unit waits for a compaction permit from the runtime's execution
    /// budget.
    ///
    /// Cancelling `cancellation` ends the wait for a permit and reports the
    /// unit cancelled. A running unit stops at its next block.
    pub async fn compact_metadata(
        &self,
        namespace_id: &NamespaceId,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataCompactionReport> {
        trace::in_operation(
            operation_span!(self.core, "compact_metadata", namespace_id),
            async {
                let compaction = self
                    .compact_once(namespace_id, None, cancellation.metadata_compaction())
                    .await?;
                Ok(MetadataCompactionReport {
                    namespace_id: namespace_id.clone(),
                    compaction,
                })
            },
        )
        .await
    }

    /// Runs one complete garbage collection of one namespace.
    ///
    /// Every call rebuilds the current live roots and keeps no cursor. A
    /// collection runs only when asked. A deleted namespace is collected, which is how
    /// it is retired; a namespace that does not exist returns
    /// `namespace_not_found`.
    pub async fn gc(&self, namespace_id: &NamespaceId) -> Result<crate::GcResponse> {
        trace::in_operation(operation_span!(self.core, "gc", namespace_id), async {
            let namespace = self.core.namespace_store(namespace_id);
            let report = namespace
                .gc(&namespace.start_clock())
                .await
                .map_err(Error::from)?;
            // A caller may drop the response, so its counts survive only when
            // this shared collection path records them here.
            self.core.instruments().gc.record_collection(&report);
            // A collection publishes no manifest, and its grace covers the
            // revalidation bound, so the session's position and the cached
            // head stay (format.md sections 11.2 and 11.4).
            Ok(report)
        })
        .await
    }

    /// Runs one complete garbage collection of one pool: its content objects,
    /// upload records, and temporary objects, with roots that it reads from
    /// every member of the pool.
    ///
    /// Every call rebuilds the members' roots and keeps no cursor. A pool
    /// without a pool record returns `pool_not_found`.
    pub async fn gc_pool(&self, pool_id: &PoolId) -> Result<crate::PoolGcResponse> {
        trace::in_operation(
            operation_span!(self.core, "gc_pool", pool_id = pool_id),
            async {
                let pool = self.core.pool_store(pool_id);
                let report = pool
                    .gc(
                        Some(self.core.completed_upload_sessions()),
                        &pool.start_clock(),
                    )
                    .await
                    .map_err(Error::from)?;
                // A caller may drop the response, so its counts survive only when
                // this shared collection path records them here. A pool's
                // collection publishes no manifest, so no namespace changes.
                self.core.instruments().gc.record_pool_collection(&report);
                Ok(report)
            },
        )
        .await
    }

    /// Lists every snapshot, in ascending id order, including
    /// fork snapshots and expired snapshots that garbage collection has not yet
    /// deleted. This is the inventory of what keeps manifests from
    /// collection. The cursor resumes a live listing.
    pub fn list_snapshot_records(&self, namespace_id: &NamespaceId) -> SnapshotRecordsPager {
        loonfs_types::Pager::with_state(
            (self.clone(), namespace_id.clone()),
            |(maintenance, namespace_id), request| async move {
                maintenance
                    .snapshot_records_page(&namespace_id, decode_page_request(request)?)
                    .await
            },
        )
    }

    async fn snapshot_records_page(
        &self,
        namespace_id: &NamespaceId,
        request: PageRequest<SnapshotRecordPageCursor>,
    ) -> Result<ListSnapshotRecordsResponse> {
        trace::in_operation(
            operation_span!(self.core, "list_snapshot_records", namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let page = self
                    .engine(namespace_id)
                    .list_snapshot_records_page(request)
                    .await
                    .map_err(Error::from)?;
                Ok(ListSnapshotRecordsResponse {
                    namespace_id: namespace_id.clone(),
                    snapshots: page.items,
                    next_cursor: encode_next_cursor(page.next_cursor.as_ref())?,
                })
            },
        )
        .await
    }

    /// Returns one existing snapshot of any owner, as the snapshot inventory
    /// returns it, including an expired snapshot that garbage collection has
    /// not yet deleted. A missing id returns `snapshot_not_found`.
    pub async fn get_snapshot_record(
        &self,
        namespace_id: &NamespaceId,
        snapshot_id: &SnapshotId,
    ) -> Result<SnapshotRecord> {
        trace::in_operation(
            operation_span!(self.core, "get_snapshot_record", namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.engine(namespace_id)
                    .get_snapshot_record(snapshot_id)
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
    ///
    /// The runtime runs one fold of a namespace at a time, so this fold waits
    /// for a fold in progress, such as the writer session's, instead of
    /// folding the same tail. It starts from the session's position when the
    /// runtime has a session for the namespace, so it reads no WAL objects.
    pub async fn fold_wal(&self, namespace_id: &NamespaceId) -> Result<FoldWalResponse> {
        trace::in_operation(
            operation_span!(self.core, "fold_wal", namespace_id),
            async {
                let folded = self
                    .publisher
                    .fold_namespace(namespace_id, FoldWhen::Always)
                    .await?;
                Ok(folded.folded().expect("a fold of any tail runs").response)
            },
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

    /// Advances the namespace retention floor toward `options.target` with a
    /// floor object in the WAL, and claims no writer epoch.
    ///
    /// Whatever the target, the floor doesn't pass a commit dated less than
    /// the grace window ([`GC_GRACE_WINDOW_MS`](crate::GC_GRACE_WINDOW_MS))
    /// before the runtime's wall clock (format spec, "Advancing the retention
    /// floor"), and the response includes the floor that the advance reached.
    /// Advancing the floor abandons the replay history below it, and a later
    /// base compaction keeps, for each file, every revision above the floor
    /// and the newest revision at or below it, and removes the rows that an
    /// expired deletion hides. The runtime doesn't schedule an advance. The
    /// reference server makes one on each maintenance sweep, toward
    /// [`RetentionTarget::Window`](crate::RetentionTarget::Window), so a
    /// namespace without a window keeps its whole history.
    ///
    /// The call plans the floor from this runtime's writer position when it
    /// has a current one, and otherwise from the current manifest and the
    /// WAL tail after that manifest. A floor that moves is recorded in a
    /// floor object at the next WAL number ([Advancing the retention
    /// floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor)).
    /// When this runtime has a writer session for the namespace, the floor
    /// object is one item of the session's queue, after the commits admitted
    /// before it, and the session's later commits are validated against the
    /// new floor. Otherwise, the call creates the floor object itself and
    /// opens no session, and it drops the position of a session that opened
    /// in this runtime meanwhile, so the fold after it reads the floor
    /// object from the store. A writer session in another runtime isn't fenced.
    /// Its next put collides with the floor object, and it plans again
    /// against the new floor.
    ///
    /// When no manifest records the floor of the view after the advance,
    /// including after an earlier advance whose fold failed, the call then
    /// folds the namespace, so the fold's manifest records the floor. The
    /// response's floor is the view's.
    pub async fn advance_retention_floor_with_options(
        &self,
        namespace_id: &NamespaceId,
        options: &AdvanceRetentionFloorOptions,
    ) -> Result<AdvanceRetentionResponse> {
        trace::in_operation(
            operation_span!(self.core, "advance_retention_floor", namespace_id),
            async {
                let store = self.core.namespace_store(namespace_id);
                let (planned, retention_floor_seq) = self.load_retention_view(namespace_id).await?;
                let plan = store
                    .plan_retention_floor(&planned, retention_floor_seq, options.target)
                    .await?;
                let report = match plan {
                    RetentionPlan::Stays(retention_floor_seq) => FloorObjectReport {
                        retention_floor_seq,
                        manifest_retention_floor_seq: planned.retention_floor_seq(),
                    },
                    RetentionPlan::Moves(target) => match self
                        .publisher
                        .submit_retention_advance(namespace_id, planned.clone(), target)
                        .await
                    {
                        Some(report) => report?,
                        None => {
                            let report = store.publish_floor_object(&planned, target).await?;
                            // A session that opened in this runtime after the
                            // check above keeps a position that may end below
                            // the floor object, and a fold from that position
                            // would miss the floor object.
                            self.publisher.drop_session_position(namespace_id).await;
                            report
                        }
                    },
                };
                if report.retention_floor_seq > report.manifest_retention_floor_seq {
                    self.publisher
                        .fold_namespace(namespace_id, FoldWhen::Always)
                        .await?;
                }
                Ok(AdvanceRetentionResponse {
                    namespace_id: namespace_id.clone(),
                    retention_floor_seq: report.retention_floor_seq,
                })
            },
        )
        .await
    }

    /// Returns the manifest and the retention floor of the view that a
    /// retention advance plans from: this runtime's writer position when it
    /// has a current one, and otherwise the current manifest and the WAL
    /// tail after that manifest.
    async fn load_retention_view(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<(LoadedManifest, CommitSeq)> {
        if let Some(position) = self.publisher.current_position(namespace_id) {
            let basis = loonfs_core::control::load_position_basis(
                self.core.store(),
                &self.core.metadata_segment_cache(),
                &position,
            )
            .await?;
            if let Some(basis) = basis {
                return Ok((basis.manifest, position.retention_floor_seq()));
            }
        }
        let anchor = self
            .core
            .namespace_store(namespace_id)
            .load_live_read_anchor(self.core.observation())
            .await?;
        Ok((
            anchor.current_manifest().clone(),
            anchor.retention_floor_seq(),
        ))
    }

    /// Loads the namespace's current manifest as the basis of an extension's
    /// change, and starts the clock of the change's publication.
    ///
    /// The extension plans its change from the basis's entry, writes its
    /// segments, and then publishes the change with
    /// [`Self::publish_extension`]. Returns `namespace_deleted` for a deleted
    /// namespace.
    pub async fn load_extension_basis(&self, namespace_id: &NamespaceId) -> Result<ExtensionBasis> {
        trace::in_operation(
            operation_span!(self.core, "load_extension_basis", namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.engine(namespace_id)
                    .load_extension_basis()
                    .await
                    .map_err(Error::from)
            },
        )
        .await
    }

    /// Publishes a change to the entry of extension `name` in the manifest
    /// after `basis`.
    ///
    /// The publication lists `entry` as the extension's entry, changes
    /// nothing else and claims no epoch, like a fold's. When another manifest
    /// follows `basis` first with the same entry of `name`, such as a fold or
    /// a compaction, the change is published over that manifest. When that
    /// manifest's entry differs, the call publishes nothing and returns
    /// [`ExtensionPublication::Superseded`], and the extension plans its next
    /// change from the current manifest. After a publication, this runtime's
    /// next read of the namespace starts from the published manifest or a
    /// newer one. See
    /// [Extensions](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#124-extensions).
    pub async fn publish_extension(
        &self,
        basis: &ExtensionBasis,
        name: &ExtensionName,
        entry: ExtensionEntry,
    ) -> Result<ExtensionPublication> {
        let namespace_id = &basis.manifest().namespace_id;
        trace::in_operation(
            operation_span!(self.core, "publish_extension", namespace_id),
            async {
                let result = self
                    .engine(namespace_id)
                    .publish_extension(basis, name, entry)
                    .await
                    .map_err(Error::from);
                self.publisher
                    .finish_publication(namespace_id, result)
                    .await
            },
        )
        .await
    }

    /// Sets the namespace's retention window.
    ///
    /// `None` clears the window, and a host doesn't advance the floor of a
    /// namespace without a window on a schedule. A window shorter than the
    /// grace window ([`GC_GRACE_WINDOW_MS`](crate::GC_GRACE_WINDOW_MS))
    /// returns [`Error::InvalidRequest`].
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
        crate::config::check_retention_window(retention_window_ms)?;
        trace::in_operation(
            operation_span!(self.core, "set_retention_window", namespace_id),
            async {
                let result = self
                    .engine(namespace_id)
                    .set_retention_window(retention_window_ms)
                    .await
                    .map_err(Error::from);
                self.publisher
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
