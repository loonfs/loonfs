//! Bounded metadata maintenance over the runtime maintenance handle.

use super::{
    MaintenanceCancellation, MaintenanceConclusion, MaintenanceJob, MaintenanceJobId,
    MaintenanceProbe, MaintenanceRunReport, NamespacePublication,
};
use crate::{
    ErrorCode, FsMaintenance, MetadataMaintenanceOptions, MetadataMaintenanceResponse, NamespaceId,
    ReorganizeStepOutcome, Result, RuntimeError, WalFlushStepOutcome,
};
use async_trait::async_trait;

/// Folds the WAL tail and runs bounded metadata compaction.
pub struct MetadataMaintenanceJob {
    maintenance: FsMaintenance,
    options: MetadataMaintenanceOptions,
}

impl MetadataMaintenanceJob {
    /// Creates a job with default metadata options.
    pub fn new(maintenance: FsMaintenance) -> Self {
        Self {
            maintenance,
            options: MetadataMaintenanceOptions::default(),
        }
    }

    /// Sets the metadata maintenance options.
    pub fn options(mut self, options: MetadataMaintenanceOptions) -> Self {
        self.options = options;
        self
    }
}

#[async_trait]
impl MaintenanceJob for MetadataMaintenanceJob {
    fn id(&self) -> MaintenanceJobId {
        MaintenanceJobId::METADATA
    }

    async fn run(
        &self,
        namespace_id: &NamespaceId,
        _cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport> {
        match self
            .maintenance
            .maintain_metadata_step(namespace_id, self.options.clone())
            .await
        {
            Ok((metadata, idle_fold_at_ms)) => {
                let mut report = MaintenanceRunReport::concluded(metadata_conclusion(&metadata));
                // A publication's wake can arrive before the tail is idle: the
                // clock moved back, or a later publication's hint was dropped.
                report.not_before_ms = idle_fold_at_ms;
                if metadata.reorganize == (ReorganizeStepOutcome::CompactionRequired {}) {
                    report.conclusion = MaintenanceConclusion::Blocked;
                    report.follow_up =
                        Some((MaintenanceJobId::METADATA_COMPACTION, namespace_id.clone()));
                }
                Ok(report)
            }
            Err(error) if metadata_has_nothing_to_maintain(&error) => Ok(
                MaintenanceRunReport::concluded(MaintenanceConclusion::NotEnabled),
            ),
            Err(error) => Err(error),
        }
    }

    async fn probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe> {
        match self
            .maintenance
            .metadata_probe(namespace_id, &self.options)
            .await
        {
            Ok(probe) => Ok(probe),
            Err(error) if metadata_has_nothing_to_maintain(&error) => Ok(MaintenanceProbe::Idle),
            Err(error) => Err(error),
        }
    }

    fn should_run_after_publication(&self, publication: &NamespacePublication) -> bool {
        self.options.fold_is_due(
            publication.wal_tail_segments,
            publication.wal_tail_inline_bytes,
        )
    }

    fn wake_after_publication_ms(&self, publication: &NamespacePublication) -> Option<u64> {
        (publication.committed_through_seq.is_some() && self.options.idle_fold_after_ms > 0)
            .then_some(self.options.idle_fold_after_ms)
    }

    fn should_run_after_fold(&self) -> bool {
        true
    }
}

fn metadata_has_nothing_to_maintain(error: &RuntimeError) -> bool {
    matches!(
        error.code(),
        ErrorCode::NamespaceNotFound | ErrorCode::NamespaceDeleted
    )
}

fn metadata_conclusion(step: &MetadataMaintenanceResponse) -> MaintenanceConclusion {
    let fold = match step.wal_flush {
        WalFlushStepOutcome::Flushed { .. } => Some(MaintenanceConclusion::Progressed),
        WalFlushStepOutcome::AlreadyPublished { .. }
        | WalFlushStepOutcome::RetriesExhausted { .. } => Some(MaintenanceConclusion::Superseded),
        WalFlushStepOutcome::NotNeeded => None,
    };
    let compaction = match step.reorganize {
        ReorganizeStepOutcome::UnitPublished {} => Some(MaintenanceConclusion::Progressed),
        ReorganizeStepOutcome::ManifestAdvanced {} | ReorganizeStepOutcome::Fenced {} => {
            Some(MaintenanceConclusion::Superseded)
        }
        ReorganizeStepOutcome::CompactionRequired {} => Some(MaintenanceConclusion::Blocked),
        ReorganizeStepOutcome::NotNeeded {} => None,
    };
    [fold, compaction]
        .into_iter()
        .flatten()
        .max_by_key(|conclusion| conclusion_precedence(*conclusion))
        .unwrap_or(MaintenanceConclusion::Idle)
}

fn conclusion_precedence(conclusion: MaintenanceConclusion) -> u8 {
    match conclusion {
        MaintenanceConclusion::Progressed => 3,
        MaintenanceConclusion::Superseded => 2,
        MaintenanceConclusion::Blocked => 1,
        MaintenanceConclusion::Idle | MaintenanceConclusion::NotEnabled => 0,
    }
}
