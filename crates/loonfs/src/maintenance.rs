//! The maintenance capability of a writable runtime that needs no writer
//! session: namespace diagnostics, the check of what maintenance a namespace
//! has due, the snapshot inventory, and garbage collection.
//!
//! Folds, metadata compaction, retention, and the publications of an
//! extension's entry publish manifests, so they're methods of the
//! namespace's writer session, [`Namespace<Writable>`](crate::Namespace).

use crate::pagers::{decode_page_request, encode_next_cursor, SnapshotRecordsPager};
use crate::publisher::{fold_due, FoldDue, WalTailState};
use crate::runtime_core::RuntimeCore;
use crate::trace::{self, operation_span};
use crate::{Error, Result};
use crate::{
    ListSnapshotRecordsResponse, MaintainMetadataOptions, NamespaceDiagnostics, NamespaceId,
    PoolId, SharedObjectStore, SnapshotId, SnapshotRecord,
};
use loonfs_core::control::NamespaceStorageDiagnostics;
use loonfs_core::{
    NamespaceReaderEngine, RetentionPlan, RuntimeReadContext, SnapshotRecordPageCursor,
};
use loonfs_types::{u64_from_usize, PageRequest};

pub(crate) mod metrics;
mod reports;
#[cfg(test)]
mod tests;

pub use reports::{
    MaintainMetadataReport, MetadataCompactionOutcome, MetadataCompactionReport,
    MetadataMaintenanceReport,
};

/// Explicit maintenance that needs no writer session: namespace
/// diagnostics, the check of what maintenance a namespace has due, the
/// snapshot inventory, and garbage collection.
///
/// Get one from [`LoonFs::maintenance`](crate::LoonFs::maintenance). Each
/// call runs in the caller's task; this value starts no background work, and
/// it publishes no manifest and writes no WAL object. Every value from one
/// runtime shares that runtime's execution budget with its writer sessions.
#[derive(Clone)]
pub struct Maintenance {
    pub(crate) core: RuntimeCore,
}

impl Maintenance {
    /// Returns the maintenance of the runtime that `core` belongs to.
    pub(crate) fn new(core: RuntimeCore) -> Self {
        Self { core }
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
                let anchor = self
                    .core
                    .namespace_store(namespace_id)
                    .load_read_anchor(self.core.observation())
                    .await?;
                let context = self.core.read_context(&anchor.held_basis(), None)?;
                let live_snapshots = self.count_live_snapshots(namespace_id, &context).await?;
                Ok(Self::namespace_diagnostics(
                    NamespaceStorageDiagnostics::from(&anchor),
                    live_snapshots,
                ))
            },
        )
        .await
    }

    /// Counts the live snapshots of the namespace that `context` reads.
    async fn count_live_snapshots(
        &self,
        namespace_id: &NamespaceId,
        context: &RuntimeReadContext,
    ) -> Result<u64> {
        let engine = self.engine(namespace_id);
        let administrator = engine
            .find_administrator_without_read()
            .expect("a maintenance engine has no subject, so it needs no read for the proof");
        let mut cursor = None;
        let mut live_snapshots = 0_u64;
        loop {
            let page = engine
                .list_snapshots_page(
                    &administrator,
                    context,
                    PageRequest {
                        limit: loonfs_types::EffectiveLimit::MAX,
                        cursor,
                    },
                )
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
            wal_tail_oldest_commit_at_ms: diagnostics.wal_tail_oldest_commit_at_ms,
            live_snapshots,
        }
    }

    /// Returns which maintenance the writer of `namespace_id` has due, from
    /// reads alone, so a host can claim only the namespaces with work due
    /// ("What a deployment uses" in the design note "One writer per
    /// namespace").
    ///
    /// The check loads the namespace's current manifest and WAL tail as a
    /// read does. A fold is due once the tail reaches the default fold
    /// thresholds of [`MaintainMetadataOptions`] or its oldest commit reaches
    /// the runtime's fold age. A compaction unit is due when the namespace's
    /// run policy selects one on the current manifest. An advance of the
    /// retention floor is due when an advance toward
    /// [`RetentionTarget::Window`](crate::RetentionTarget::Window) would move
    /// the floor, which reads commits upward from the floor only for a
    /// namespace with a window. The check writes nothing and claims nothing,
    /// and it returns `namespace_deleted` for a deleted namespace.
    pub async fn due_maintenance(&self, namespace_id: &NamespaceId) -> Result<DueMaintenance> {
        trace::in_operation(
            operation_span!(self.core, "due_maintenance", namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                let namespace = self.core.namespace_store(namespace_id);
                let anchor = namespace
                    .load_live_read_anchor(self.core.observation())
                    .await?;
                let fold = fold_due(
                    &self.core,
                    MaintainMetadataOptions::default()
                        .max_wal_tail_objects
                        .get(),
                    &WalTailState::from(&anchor),
                    self.core.now_ms(),
                ) == FoldDue::Now;
                let current = anchor.current_manifest();
                let compaction = self.engine(namespace_id).metadata_compaction_due(current);
                let retention_advance = matches!(
                    namespace
                        .plan_retention_floor(
                            current,
                            anchor.retention_floor_seq(),
                            crate::RetentionTarget::Window,
                        )
                        .await?,
                    RetentionPlan::Moves(_)
                );
                Ok(DueMaintenance {
                    fold,
                    compaction,
                    retention_advance,
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
    /// `namespace_not_found`. The call waits for a collection permit from the
    /// runtime's execution budget.
    pub async fn gc(&self, namespace_id: &NamespaceId) -> Result<crate::GcResponse> {
        trace::in_operation(operation_span!(self.core, "gc", namespace_id), async {
            let _permit = self.core.collection_permit().await;
            let namespace = self.core.namespace_store(namespace_id);
            let report = namespace
                .gc(&namespace.start_clock())
                .await
                .map_err(Error::from)?;
            // A caller may drop the response, so its counts survive only when
            // this shared collection path records them here.
            self.core.instruments().gc.record_collection(&report);
            // A collection publishes no manifest, so the session's position
            // and the cached head stay, and their next check reads the
            // manifest boundary that the collection raised (format.md
            // sections 11.2 and 11.4).
            Ok(report)
        })
        .await
    }

    /// Runs one complete garbage collection of one pool: its content objects
    /// and upload records, with roots that it reads from every member of the
    /// pool.
    ///
    /// Every call rebuilds the members' roots and keeps no cursor. A pool
    /// with nothing under its prefix returns `pool_not_found`. The call waits
    /// for a collection permit from the runtime's execution budget.
    pub async fn gc_pool(&self, pool_id: &PoolId) -> Result<crate::PoolGcResponse> {
        trace::in_operation(
            operation_span!(self.core, "gc_pool", pool_id = pool_id),
            async {
                let _permit = self.core.collection_permit().await;
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

    /// Returns an engine over `namespace_id` that reads in the service
    /// context, because maintenance acts in the service context whatever
    /// subject the runtime has.
    fn engine(&self, namespace_id: &NamespaceId) -> NamespaceReaderEngine<SharedObjectStore> {
        NamespaceReaderEngine::reader(self.core.namespace_store(namespace_id))
    }
}

/// Which maintenance the writer of one namespace has due, from
/// [`Maintenance::due_maintenance`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DueMaintenance {
    /// A fold of the WAL tail.
    pub fold: bool,
    /// A metadata compaction unit.
    pub compaction: bool,
    /// An advance of the retention floor under the namespace's retention
    /// window.
    pub retention_advance: bool,
}

impl DueMaintenance {
    /// Returns whether any maintenance is due.
    pub fn any(&self) -> bool {
        self.fold || self.compaction || self.retention_advance
    }
}

/// Stops a maintenance call that takes it, such as
/// [`Namespace::maintain_metadata_while_due`](crate::Namespace::maintain_metadata_while_due)
/// or [`Namespace::compact_metadata`](crate::Namespace::compact_metadata).
/// Clones share one state.
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
