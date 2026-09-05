//! Bounded metadata maintenance over the runtime maintenance handle.

use super::{
    MaintenanceCancellation, MaintenanceConclusion, MaintenanceJob, MaintenanceJobId,
    MaintenanceProbe, MaintenanceRunReport,
};
use crate::{
    ErrorCode, FsMaintenance, MetadataMaintenanceOptions, MetadataMaintenanceResponse, NamespaceId,
    ReorganizeStepOutcome, Result, RuntimeError, WalFlushStepOutcome,
};
use async_trait::async_trait;

/// Flushes the WAL tail and runs bounded metadata reorganization.
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
        _continuation: Option<&str>,
        _cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport> {
        match self
            .maintenance
            .maintain_metadata(namespace_id, self.options.clone())
            .await
        {
            Ok(metadata) => {
                let mut report = MaintenanceRunReport::concluded(metadata_conclusion(&metadata));
                if metadata.reorganize == ReorganizeStepOutcome::CompactionRequired {
                    report.conclusion = MaintenanceConclusion::Blocked;
                    report.follow_up = Some(MaintenanceJobId::METADATA_COMPACTION);
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
    let flush = match step.wal_flush {
        WalFlushStepOutcome::Flushed { .. } => Some(MaintenanceConclusion::Progressed),
        WalFlushStepOutcome::AlreadyPublished { .. }
        | WalFlushStepOutcome::RetriesExhausted { .. } => Some(MaintenanceConclusion::Superseded),
        WalFlushStepOutcome::NotNeeded => None,
    };
    let reorganize = match step.reorganize {
        ReorganizeStepOutcome::UnitPublished => Some(MaintenanceConclusion::Progressed),
        ReorganizeStepOutcome::RootAdvanced => Some(MaintenanceConclusion::Superseded),
        ReorganizeStepOutcome::CompactionRequired => Some(MaintenanceConclusion::Blocked),
        ReorganizeStepOutcome::NotNeeded => None,
    };
    [flush, reorganize]
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
