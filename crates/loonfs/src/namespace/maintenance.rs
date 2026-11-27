//! The maintenance that a namespace's writer session publishes: WAL folds,
//! metadata compaction, retention, and the publications of an extension's
//! entry.
//!
//! Derived indexes aren't in this module or this crate. `loonfs-grep` builds
//! its own segments through the runtime's public reads and snapshot calls,
//! publishes its entry through [`Namespace::publish_extension`], and its
//! hosts drive it.

use crate::maintenance::{
    MaintainMetadataReport, MetadataCompactionReport, MetadataMaintenanceReport,
};
use crate::publisher::{FoldWhen, NamespaceFold};
use crate::trace::{self, operation_span};
use crate::{
    AdvanceRetentionFloorOptions, AdvanceRetentionResponse, Error, FoldWalResponse,
    MaintainMetadataOptions, MaintenanceCancellation, MetadataMaintenanceOutcome,
    MetadataMaintenanceResponse, Namespace, Result, RetentionWindow, SetRetentionWindowResponse,
    Writable,
};
use loonfs_core::control::LoadedManifest;
use loonfs_core::{
    ExtensionBasis, ExtensionPublication, FloorObjectReport, MetadataCompactionCancellation,
    RetentionPlan,
};
use loonfs_types::format::manifest::ExtensionEntry;
use loonfs_types::{CommitSeq, ExtensionName};

impl Namespace<Writable> {
    /// Runs [`Self::maintain_metadata_with_options`] with the default
    /// thresholds.
    pub async fn maintain_metadata(&self) -> Result<MaintainMetadataReport> {
        self.maintain_metadata_with_options(&MaintainMetadataOptions::default())
            .await
    }

    /// Folds the WAL tail at `max_wal_tail_objects` WAL objects, at
    /// [`FOLD_AT_WAL_BYTES`](loonfs_core::limits::FOLD_AT_WAL_BYTES) of
    /// unfolded WAL bytes, at a quarter of the inline tail limit in inline
    /// content bytes (see
    /// [`fold_at_inline_bytes`](loonfs_core::limits::fold_at_inline_bytes)),
    /// or once the tail's oldest commit reaches the runtime's fold age (see
    /// [`RuntimeConfig::fold_age_ms`](crate::RuntimeConfig::fold_age_ms)).
    /// Then runs one size-tiered compaction unit when one is due, whatever
    /// the number of rows its window lists.
    ///
    /// The checks read this session's position when it has one, and
    /// otherwise the current manifest and the tail from the store, and they
    /// claim nothing. A fold or a unit that is due runs as this session, so
    /// a session without its writer epoch claims it first (format spec,
    /// "Writer ownership"). A fold waits for a fold permit from the
    /// runtime's execution budget, and a unit that finds work waits for a
    /// compaction permit from it.
    pub async fn maintain_metadata_with_options(
        &self,
        options: &MaintainMetadataOptions,
    ) -> Result<MaintainMetadataReport> {
        trace::in_operation(
            operation_span!(self.core, "maintain_metadata", self.namespace_id),
            async {
                let (wal_fold, observed) = match self.fold_when_due(options).await? {
                    NamespaceFold::NotDue { observed, .. } => (None, observed),
                    NamespaceFold::Folded(folded) => (Some(folded.response), None),
                };
                let compaction = self
                    .session()
                    .compact_once(observed, &MetadataCompactionCancellation::default())
                    .await?;
                tracing::debug!(
                    ?wal_fold,
                    ?compaction,
                    "metadata maintenance step concluded"
                );
                Ok(MaintainMetadataReport {
                    namespace_id: self.namespace_id.clone(),
                    wal_fold,
                    compaction,
                })
            },
        )
        .await
    }

    /// Folds the namespace's WAL tail when a fold is due under `options`,
    /// through the namespace's one fold path.
    async fn fold_when_due(&self, options: &MaintainMetadataOptions) -> Result<NamespaceFold> {
        let when = FoldWhen::Due {
            max_wal_tail_objects: options.max_wal_tail_objects.get(),
        };
        self.session().fold(when).await
    }

    /// Runs [`Self::maintain_metadata_while_due_with_options`] with the
    /// default thresholds.
    pub async fn maintain_metadata_while_due(
        &self,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataMaintenanceReport> {
        self.maintain_metadata_while_due_with_options(
            cancellation,
            &MaintainMetadataOptions::default(),
        )
        .await
    }

    /// Folds the WAL tail when a fold is due, as
    /// [`Self::maintain_metadata_with_options`] does, and then runs
    /// size-tiered compaction units while a unit is due. The fold
    /// waits for a fold of the namespace in progress, such as the session's
    /// own, and each unit waits for a unit in progress, so the call never
    /// repeats that work.
    ///
    /// Stops when nothing is due, when `cancellation` is set, when this
    /// session is closed or its namespace deleted, after three abandoned
    /// units in a row, or after it publishes 16 compaction units. Work still
    /// due after the 16th unit waits for the next call. A unit that loses its
    /// manifest put to another publication tries again over the manifest
    /// that won, so only an abandoned unit counts as a lost race. Setting
    /// `cancellation` ends a wait for a lock or a permit and drops a fold in
    /// progress at once, and a running compaction unit stops at its next
    /// block. A close or a delete of the session stops the call before its
    /// next unit. A dropped call leaves what a crash would: unreferenced
    /// segments, or a manifest the runtime learns of the way it learns of
    /// another process's. Fails with the first error. Keeps no state between
    /// calls.
    ///
    /// The response reports the compaction units the call published, and an
    /// outcome. The outcome is [`MetadataMaintenanceOutcome::CaughtUp`] when
    /// nothing is due now and nothing can become due without a new commit
    /// under `options`, based on the state this call read. It is
    /// [`MetadataMaintenanceOutcome::Due`] at a work limit, after lost races,
    /// on fencing or cancellation, when a compaction unit does not finish,
    /// or while a tail waits for the fold age. When the wait for the fold
    /// age is the only work left, the report says when the fold comes due.
    pub async fn maintain_metadata_while_due_with_options(
        &self,
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
                            namespace_id: self.namespace_id.clone(),
                            outcome: MetadataMaintenanceOutcome::Due,
                            compaction_units: 0,
                            skipped: Vec::new(),
                        },
                        next_due_at_ms: None,
                    });
                }
                fold = self.fold_when_due(options) => fold?,
            };
            let (fold_due_at_ms, observed) = match fold {
                NamespaceFold::NotDue {
                    fold_due_at_ms,
                    observed,
                } => (fold_due_at_ms, observed),
                NamespaceFold::Folded(_) => (None, None),
            };
            let mut response = self
                .session()
                .compact_namespace(observed, cancellation)
                .await?;
            let next_due_at_ms =
                fold_due_at_ms.filter(|_| response.outcome == MetadataMaintenanceOutcome::CaughtUp);
            if fold_due_at_ms.is_some() {
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
            operation_span!(self.core, "maintain_metadata_while_due", self.namespace_id),
            maintained,
        ))
        .await
    }

    /// Runs one metadata compaction unit in the caller's task when one is
    /// due under the namespace's run policy. The unit is the same size-tiered
    /// unit that [`Self::maintain_metadata`] runs after its fold check, and
    /// the call reports that no unit was needed when nothing is due. A unit
    /// that is due runs as this session, so a session without its writer
    /// epoch claims it first.
    ///
    /// Repeat the call while it publishes to compact every due window. The
    /// unit waits for a compaction permit from the runtime's execution
    /// budget.
    ///
    /// Cancelling `cancellation` ends the wait for a permit and reports the
    /// unit cancelled. A running unit stops at its next block.
    pub async fn compact_metadata(
        &self,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MetadataCompactionReport> {
        trace::in_operation(
            operation_span!(self.core, "compact_metadata", self.namespace_id),
            async {
                let compaction = self
                    .session()
                    .compact_once(None, cancellation.metadata_compaction())
                    .await?;
                Ok(MetadataCompactionReport {
                    namespace_id: self.namespace_id.clone(),
                    compaction,
                })
            },
        )
        .await
    }

    /// Folds the visible WAL tail into a new manifest as this session, which
    /// claims the writer epoch first when it has none.
    ///
    /// A namespace with no unfolded tail reports
    /// [`FoldWalOutcome::AlreadyCurrent`](crate::FoldWalOutcome::AlreadyCurrent)
    /// and publishes nothing. The fold waits for a fold permit from the
    /// runtime's execution budget. This runs no compaction;
    /// [`Self::maintain_metadata`] folds and then runs one compaction unit.
    ///
    /// The runtime runs one fold of a namespace at a time, so this fold waits
    /// for a fold in progress, such as the session's own, instead of folding
    /// the same tail. It starts from the session's position when the session
    /// has one, so it reads no WAL objects.
    pub async fn fold_wal(&self) -> Result<FoldWalResponse> {
        trace::in_operation(
            operation_span!(self.core, "fold_wal", self.namespace_id),
            async {
                let folded = self.session().fold(FoldWhen::Always).await?;
                Ok(folded.folded().expect("a fold of any tail runs").response)
            },
        )
        .await
    }

    /// Runs [`Self::advance_retention_floor_with_options`] toward the folded
    /// manifest head.
    pub async fn advance_retention_floor(&self) -> Result<AdvanceRetentionResponse> {
        self.advance_retention_floor_with_options(&AdvanceRetentionFloorOptions::default())
            .await
    }

    /// Advances the namespace retention floor toward `options.target` with a
    /// floor object that this session puts in the WAL.
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
    /// When the session has a position, the call first checks it as a strong
    /// read does (one HEAD of the position manifest's successor and one read
    /// of the manifest boundary). The check moves the position onto a newer
    /// manifest of the same writer epoch, and drops the position when
    /// another writer has claimed the namespace. The call then plans the
    /// floor from the position when one is left, and otherwise from the
    /// current manifest's floor and head, and claims nothing to plan.
    ///
    /// A floor that moves is recorded in a floor object, one item of the
    /// session's queue, after the commits admitted before it, and the
    /// session's later commits are validated against the new floor. The
    /// session puts the floor object at the number after its position's tip,
    /// so a session without its writer epoch claims it first ([Advancing the
    /// retention
    /// floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor)).
    /// The put writes nothing when the view that the session loads already
    /// has a floor at or above the planned floor.
    ///
    /// The manifest that the advance plans from may be replaced, and its
    /// segments collected, after the check that found it. When the advance
    /// finds a segment of the manifest it planned from missing and that
    /// manifest isn't the current one, the call drops the position and the
    /// cached head and plans again from the newest manifest, up to
    /// `MAX_CONTENTION_ATTEMPTS` plans, after which it returns
    /// `store_contention`. A missing segment of the current manifest returns
    /// `namespace_corrupt`.
    ///
    /// When no manifest records the floor of the view after the advance,
    /// including after an earlier advance whose fold failed, the call then
    /// folds the namespace as this session, so the fold's manifest records
    /// the floor. The response's floor is the floor of the view after the
    /// advance, or, when the plan leaves the floor where it is, the floor
    /// that the call planned from (without a writer position, the current
    /// manifest's floor).
    pub async fn advance_retention_floor_with_options(
        &self,
        options: &AdvanceRetentionFloorOptions,
    ) -> Result<AdvanceRetentionResponse> {
        trace::in_operation(
            operation_span!(self.core, "advance_retention_floor", self.namespace_id),
            async {
                let report = self
                    .core
                    .run_again_after_replaced_manifest(|| self.publish_retention_floor(options))
                    .await?;
                if report.retention_floor_seq > report.manifest_retention_floor_seq {
                    self.session().fold(FoldWhen::Always).await?;
                }
                Ok(AdvanceRetentionResponse {
                    namespace_id: self.namespace_id.clone(),
                    retention_floor_seq: report.retention_floor_seq,
                })
            },
        )
        .await
    }

    /// Plans the retention floor of [`Self::advance_retention_floor_with_options`],
    /// puts a floor object through the session's queue when the floor moves,
    /// and returns the floors after the advance.
    ///
    /// The plan and the floor object's check of the planned manifest's
    /// segments read the manifest that the advance plans from. That manifest
    /// can be the session's position's, which a newer manifest may have
    /// replaced, and a collection may then have deleted its segments. A read
    /// that finds a segment missing then returns `manifest_replaced`, and the
    /// caller plans again from the newest manifest. A missing segment of the
    /// current manifest stays `namespace_corrupt`.
    async fn publish_retention_floor(
        &self,
        options: &AdvanceRetentionFloorOptions,
    ) -> Result<FloorObjectReport> {
        let store = self.core.namespace_store(&self.namespace_id);
        let (planned, retention_floor_seq) = self.load_retention_view().await?;
        let plan = store
            .plan_retention_floor(&planned, retention_floor_seq, options.target)
            .await?;
        match plan {
            RetentionPlan::Stays(retention_floor_seq) => Ok(FloorObjectReport {
                retention_floor_seq,
                manifest_retention_floor_seq: planned.retention_floor_seq(),
            }),
            RetentionPlan::Moves(target) => {
                self.session()
                    .submit_retention_advance(planned, target)
                    .await
            }
        }
    }

    /// Returns the manifest and the retention floor that a retention advance
    /// plans from. Those are the session's position after a strong read's
    /// check of it (see
    /// [`RuntimeCore::checked_writer_position`](crate::runtime_core::RuntimeCore::checked_writer_position)),
    /// or the current manifest of a live namespace and its floor when no
    /// position is left after the check or the position's manifest is missing
    /// from the store and at or below the manifest boundary.
    async fn load_retention_view(&self) -> Result<(LoadedManifest, CommitSeq)> {
        let store = self.core.namespace_store(&self.namespace_id);
        if let Some(position) = self
            .core
            .checked_writer_position(&self.namespace_id, None)
            .await?
        {
            match store.load_position_basis(&position).await? {
                Some(basis) => return Ok((basis.into_manifest(), position.retention_floor_seq())),
                None => self.core.forget_namespace_view(&self.namespace_id).await,
            }
        }
        let current = store
            .load_live_basis(self.core.observation())
            .await?
            .into_manifest();
        let retention_floor_seq = current.retention_floor_seq();
        Ok((current, retention_floor_seq))
    }

    /// Publishes a change to the entry of extension `name` in the manifest
    /// after `basis`, as this session, which claims the writer epoch first
    /// when it has none.
    ///
    /// The publication lists `entry` as the extension's entry and changes
    /// nothing else. When another manifest follows `basis` first with the
    /// same entry of `name`, such as a fold or a compaction, the change is
    /// published over that manifest. When that manifest's entry differs, the
    /// call publishes nothing and returns
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
        trace::in_operation(
            operation_span!(self.core, "publish_extension", self.namespace_id),
            async {
                let result = self.session().publish_extension(basis, name, entry).await;
                self.mode
                    .publisher
                    .finish_publication(&self.namespace_id, result)
                    .await
            },
        )
        .await
    }

    /// Sets the namespace's retention window as this session, which claims
    /// the writer epoch first when it has none.
    ///
    /// `None` clears the window, and a host doesn't advance the floor of a
    /// namespace without a window on a schedule.
    ///
    /// The call publishes a manifest over the session's position that
    /// changes nothing else, and if the namespace already has the window, it
    /// publishes nothing. See [Advancing the retention
    /// floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor).
    pub async fn set_retention_window(
        &self,
        retention_window_ms: Option<RetentionWindow>,
    ) -> Result<SetRetentionWindowResponse> {
        trace::in_operation(
            operation_span!(self.core, "set_retention_window", self.namespace_id),
            async {
                let result = self
                    .session()
                    .set_retention_window(retention_window_ms)
                    .await;
                self.mode
                    .publisher
                    .finish_publication(&self.namespace_id, result)
                    .await
            },
        )
        .await
    }
}

impl<M: crate::Mode> Namespace<M> {
    /// Loads the namespace's current manifest as the basis of an extension's
    /// change, and starts the clock of the change's publication. The load is
    /// a read, so it claims nothing, and a read-only handle loads it too.
    ///
    /// The extension plans its change from the basis's entry, writes its
    /// segments, and then publishes the change with
    /// [`Namespace::publish_extension`] on a writable handle. Returns `namespace_deleted` for a deleted
    /// namespace.
    pub async fn load_extension_basis(&self) -> Result<ExtensionBasis> {
        trace::in_operation(
            operation_span!(self.core, "load_extension_basis", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                loonfs_core::NamespaceReaderEngine::reader(
                    self.core.namespace_store(&self.namespace_id),
                )
                .load_extension_basis()
                .await
                .map_err(Error::from)
            },
        )
        .await
    }
}
