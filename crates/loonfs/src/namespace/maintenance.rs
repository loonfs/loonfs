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
use crate::publisher::AdmittedMaintenance;
use crate::publisher::{FoldWhen, NamespaceFold};
use crate::trace::{self, operation_span};
use crate::{
    AdvanceRetentionFloorOptions, AdvanceRetentionResponse, Error, FoldWalResponse,
    MaintainMetadataOptions, MaintenanceCancellation, MetadataMaintenanceOutcome,
    MetadataMaintenanceResponse, Namespace, ReadOnly, Result, RetentionWindow,
    SetRetentionWindowResponse, Writable,
};
use loonfs_core::control::LoadedManifest;
use loonfs_core::{ExtensionBasis, ExtensionPublication, RetentionPlan};
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
                let admitted = self.session().admit()?;
                let (wal_fold, observed) = match fold_when_due(&admitted, options).await? {
                    NamespaceFold::NotDue { observed, .. } => (None, observed),
                    NamespaceFold::Folded(folded) => (Some(folded.response), None),
                };
                let compaction = admitted
                    .compact_once(observed, &MaintenanceCancellation::default())
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
    /// due after the 16th unit waits for the next call. Each unit puts its
    /// manifest once, in its turn in the session's publication lane, and a
    /// unit whose put loses to another writer's claim returns
    /// `writer_fenced`. Setting
    /// `cancellation` ends a wait for a lock or a permit and drops a fold in
    /// progress at once, and a running compaction unit stops at its next
    /// block. A close, a fence or the runtime's shutdown sets the session's
    /// stop, which ends the same waits and stops a running unit at its next
    /// block, but lets a fold that has its fold permit run to its end, and
    /// the call then returns the session's refusal: `writer_fenced`,
    /// `writer_session_closed` or `shutting_down`. The close and the
    /// runtime's drain wait for the call to end. A unit whose position's
    /// manifest or segments a collection deleted drops the session's
    /// position and loads it again, so a session that another writer
    /// displaced returns `writer_fenced`. A dropped call leaves what a crash would: unreferenced
    /// segments, or a manifest the runtime learns of the way it learns of
    /// another process's. Fails with the first error. Keeps no state between
    /// calls.
    ///
    /// The response reports the compaction units the call published, and an
    /// outcome. The outcome is [`MetadataMaintenanceOutcome::CaughtUp`] when
    /// nothing is due now and nothing can become due without a new commit
    /// under `options`, based on the state this call read. A result that
    /// nothing is due is computed from the session's position and isn't
    /// confirmed against the store, so a held session with nothing due costs
    /// no request. A session that another writer displaced therefore returns
    /// that result until its next put, its next result that is confirmed (a
    /// window change, an index change or a retention advance), or its close,
    /// and the server closes an idle session after
    /// `idle_session_close_after_ms`. It is
    /// [`MetadataMaintenanceOutcome::Due`] at a work limit, after abandoned units,
    /// on cancellation, when a compaction unit does not finish,
    /// or while a tail waits for the fold age. When the wait for the fold
    /// age is the only work left, the report says when the fold comes due.
    pub async fn maintain_metadata_while_due_with_options(
        &self,
        cancellation: &MaintenanceCancellation,
        options: &MaintainMetadataOptions,
    ) -> Result<MetadataMaintenanceReport> {
        let maintained = async {
            let admitted = self.session().admit()?;
            // Only the host's token drops a fold in progress. The session's
            // stop ends the fold's waits, and a fold that has its permit runs
            // to its end (see `AdmittedMaintenance::fold`).
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
                fold = fold_when_due(&admitted, options) => fold?,
            };
            let (fold_due_at_ms, observed) = match fold {
                NamespaceFold::NotDue {
                    fold_due_at_ms,
                    observed,
                } => (fold_due_at_ms, observed),
                NamespaceFold::Folded(_) => (None, None),
            };
            let mut response = admitted.compact_namespace(observed, cancellation).await?;
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
                    .admit()?
                    .compact_once(None, cancellation)
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
                let folded = self.session().admit()?.fold(FoldWhen::Always).await?;
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

    /// Advances the namespace retention floor toward `options.target`, and
    /// records the new floor in a manifest that this session publishes.
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
    /// When the session has a position, the call plans the floor from the
    /// position, and then the session's check, the check that a strong read
    /// makes (one HEAD of the position manifest's successor and one read of
    /// the manifest boundary), confirms the position. When the check finds a
    /// newer manifest, the session settles its position and plans once more,
    /// and when another writer has claimed the namespace, the settle returns
    /// `writer_fenced`.
    /// Without a position, it plans from the current manifest's floor and
    /// head, and claims nothing to plan.
    ///
    /// A floor that moves is recorded in two steps of this session. The call
    /// first checks that every segment of the manifest it planned from exists
    /// (one HEAD each). It then takes the namespace's fold lock, folds the
    /// tail as an ordinary fold while commits continue, and then keeps the
    /// session's publication lane and pauses its commit queue, folds what
    /// arrived, and publishes one manifest that includes that fold and whose
    /// floor is the higher of the old floor and the target, even when
    /// nothing arrived. So commits pause only for the fold of what arrived
    /// and one manifest put, and the session's later commits are validated
    /// against the new floor. A session without its writer epoch claims it
    /// first ([Advancing the retention
    /// floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor)).
    ///
    /// The manifest that the advance plans from may be replaced, and its
    /// segments collected, after the check that found it. When the advance
    /// finds a segment of the manifest it planned from missing and that
    /// manifest isn't the current one, the call drops the cached head and
    /// plans again, a session's position behind such a manifest is loaded
    /// again from the store, and the call plans from the newest manifest, up to
    /// `MAX_CONTENTION_ATTEMPTS` plans, after which it returns
    /// `store_contention`. A missing segment of the current manifest returns
    /// `namespace_corrupt`.
    ///
    /// The response's floor is the floor of the manifest that records the
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
                let admitted = self.session().admit()?;
                let retention_floor_seq = self
                    .core
                    .run_again_after_replaced_manifest(|| {
                        self.raise_retention_floor(&admitted, options)
                    })
                    .await?;
                Ok(AdvanceRetentionResponse {
                    namespace_id: self.namespace_id.clone(),
                    retention_floor_seq,
                })
            },
        )
        .await
    }

    /// Plans the retention floor of [`Self::advance_retention_floor_with_options`],
    /// raises it through the session when it moves, and returns the floor
    /// after the advance.
    ///
    /// The plan and the check of the planned manifest's segments read the
    /// manifest that the advance plans from. That manifest can be the
    /// session's position's, which a newer manifest may have replaced, and a
    /// collection may then have deleted its segments. A read that finds a
    /// segment missing then returns `manifest_replaced`, and the caller plans
    /// again from the newest manifest. A missing segment of the current
    /// manifest stays `namespace_corrupt`.
    async fn raise_retention_floor(
        &self,
        admitted: &AdmittedMaintenance<'_>,
        options: &AdvanceRetentionFloorOptions,
    ) -> Result<CommitSeq> {
        let store = self.core.namespace_store(&self.namespace_id);
        let planned = self.load_retention_manifest(admitted).await?;
        let target = match store.plan_retention_floor(&planned, options.target).await? {
            RetentionPlan::Stays(retention_floor_seq) => return Ok(retention_floor_seq),
            RetentionPlan::Moves(target) => target,
        };
        store.verify_planned_segments(&planned).await?;
        admitted.advance_retention_floor(target).await
    }

    /// Returns the manifest that a retention advance plans from: the
    /// manifest of the session's position after the check that confirms it
    /// (see [`AdmittedMaintenance::load_confirmed`]), or the current manifest
    /// of a live namespace for a session without a position.
    async fn load_retention_manifest(
        &self,
        admitted: &AdmittedMaintenance<'_>,
    ) -> Result<LoadedManifest> {
        let store = self.core.namespace_store(&self.namespace_id);
        let newest = async {
            Ok(store
                .load_live_basis(self.core.observation())
                .await?
                .into_manifest())
        };
        admitted
            .load_confirmed(
                |position| {
                    let store = &store;
                    async move {
                        Ok(store
                            .load_publication_basis(&position)
                            .await?
                            .into_manifest())
                    }
                },
                newest,
            )
            .await
    }

    /// Publishes a change to the entry of extension `name` in the manifest
    /// after `basis`, as this session, which claims the writer epoch first
    /// when it has none.
    ///
    /// The publication lists `entry` as the extension's entry and changes
    /// nothing else. It is a publication of the session's lane, so it builds
    /// on the session's newest published manifest, such as the session's own
    /// fold, with one put. When that manifest has another entry, the call
    /// publishes nothing, and the session's check confirms its position
    /// before the call returns [`ExtensionPublication::Superseded`]. When a manifest that another
    /// writer published is in the way, the call returns `writer_fenced`. A
    /// `basis` loaded from another namespace returns `invalid_request` before
    /// anything is claimed. After a publication, this runtime's next read of
    /// the namespace starts from the published manifest or a newer one. See
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
                match self.session().admit() {
                    Ok(admitted) => admitted.publish_extension(basis, name, entry).await,
                    Err(error) => Err(error.into()),
                }
            },
        )
        .await
    }

    /// Sets the namespace's retention window as this session, which claims
    /// the writer epoch first when it has none and the window changes.
    ///
    /// `None` clears the window, and a host doesn't advance the floor of a
    /// namespace without a window on a schedule.
    ///
    /// The call publishes a manifest over the session's position that
    /// changes nothing else. If the position's manifest already has the
    /// window, it publishes nothing, and the session's check, the check that
    /// a strong read makes, confirms its position before the call returns, so
    /// a session that another writer displaced returns `writer_fenced`. A session without a
    /// position first reads the newest manifest, and when that manifest
    /// already has the window, it returns the window without a claim or a
    /// put. A session that is fenced, closed or shutting down returns its
    /// error before it sends anything.
    /// See [Advancing the retention
    /// floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor).
    pub async fn set_retention_window(
        &self,
        retention_window_ms: Option<RetentionWindow>,
    ) -> Result<SetRetentionWindowResponse> {
        trace::in_operation(
            operation_span!(self.core, "set_retention_window", self.namespace_id),
            async {
                match self.session().admit() {
                    Ok(admitted) => admitted.set_retention_window(retention_window_ms).await,
                    Err(error) => Err(error.into()),
                }
            },
        )
        .await
    }
}

impl Namespace<Writable> {
    /// Loads the manifest of this session's position as the basis of an
    /// extension's change, and starts the clock of the change's publication.
    ///
    /// The extension plans its change from the basis's entry, writes its
    /// segments, and then publishes the change with
    /// [`Self::publish_extension`], which builds on the same position, so the
    /// change and its publication start from the same manifest. The extension
    /// can return a result from the basis without a put, such as a refusal,
    /// so the session's check, the check that a strong read makes, confirms
    /// the position before the load returns, and a session that another
    /// writer displaced returns `writer_fenced`. A
    /// session without a position loads the newest manifest with a read, as
    /// a read-only handle does, and claims nothing: the publication claims
    /// the writer epoch first when there is something to publish. Returns
    /// `namespace_deleted` for a deleted namespace.
    pub async fn load_extension_basis(&self) -> Result<ExtensionBasis> {
        trace::in_operation(
            operation_span!(self.core, "load_extension_basis", self.namespace_id),
            async {
                let store = self.core.namespace_store(&self.namespace_id);
                self.session()
                    .admit()?
                    .load_confirmed(
                        |position| {
                            let store = &store;
                            async move { store.load_position_extension_basis(&position).await }
                        },
                        self.load_newest_extension_basis(),
                    )
                    .await
            },
        )
        .await
    }
}

impl Namespace<ReadOnly> {
    /// Loads the namespace's current manifest as the basis of an extension's
    /// change that a read plans, and starts the clock of the change's
    /// publication. The load is a read, so it claims nothing.
    ///
    /// A change that a writer session publishes loads its basis from the
    /// session's position instead when the session has one (see
    /// [`Namespace::<Writable>::load_extension_basis`](Namespace::load_extension_basis)).
    /// Returns `namespace_deleted` for a deleted namespace.
    pub async fn load_extension_basis(&self) -> Result<ExtensionBasis> {
        trace::in_operation(
            operation_span!(self.core, "load_extension_basis", self.namespace_id),
            self.load_newest_extension_basis(),
        )
        .await
    }
}

impl<M: crate::Mode> Namespace<M> {
    /// Loads the namespace's current manifest as an extension's basis with a
    /// read permit, which claims nothing.
    async fn load_newest_extension_basis(&self) -> Result<ExtensionBasis> {
        let _permit = self.core.read_permit().await;
        loonfs_core::NamespaceReaderEngine::reader(self.core.namespace_store(&self.namespace_id))
            .load_extension_basis()
            .await
            .map_err(Error::from)
    }
}

/// Folds the namespace's WAL tail when a fold is due under `options`,
/// through the namespace's one fold path, as `admitted` maintenance.
async fn fold_when_due(
    admitted: &AdmittedMaintenance<'_>,
    options: &MaintainMetadataOptions,
) -> Result<NamespaceFold> {
    let when = FoldWhen::Due {
        max_wal_tail_objects: options.max_wal_tail_objects.get(),
    };
    admitted.fold(when).await
}
