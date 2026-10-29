//! [`Maintenance::acknowledge_retention_floor`], which acknowledges the
//! retention floor of a namespace whose writer commits nothing.

use crate::trace::{self, operation_span};
use crate::{ChangeSeq, Error, Maintenance, NamespaceId, Result};
use loonfs_core::control::LoadedManifest;

/// How one [`Maintenance::acknowledge_retention_floor`] call ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcknowledgeRetentionFloorOutcome {
    /// The current manifest had already acknowledged its retention floor,
    /// so the call published nothing.
    AlreadyAcknowledged,
    /// A fold of the WAL tail acknowledged the floor, because the tail had a
    /// WAL object that the writer validated against that floor, so the call
    /// wrote no fence.
    Folded,
    /// The call acquired the writer epoch, wrote a fence, and folded the
    /// fence.
    Fenced,
}

/// The retention floor and the acknowledged floor of one manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionFloors {
    /// The manifest's retention floor.
    pub retention_floor_seq: ChangeSeq,
    /// The manifest's acknowledged floor, which is at most its retention
    /// floor.
    pub acknowledged_floor_seq: ChangeSeq,
}

impl RetentionFloors {
    fn of(manifest: &LoadedManifest) -> Self {
        Self {
            retention_floor_seq: manifest.retention_floor_seq(),
            acknowledged_floor_seq: manifest.acknowledged_floor_seq(),
        }
    }

    fn is_acknowledged(self) -> bool {
        self.acknowledged_floor_seq >= self.retention_floor_seq
    }
}

/// The floors that one [`Maintenance::acknowledge_retention_floor`] call
/// found and left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcknowledgeRetentionFloorReport {
    /// The namespace whose floor the call acknowledged.
    pub namespace_id: NamespaceId,
    /// The floors of the current manifest that the call loaded first.
    pub before: RetentionFloors,
    /// The floors of the current manifest when the call returned.
    pub after: RetentionFloors,
    /// What the call did.
    pub outcome: AcknowledgeRetentionFloorOutcome,
}

impl Maintenance {
    /// Acknowledges the current manifest's retention floor, so that a base
    /// compaction can remove the rows of the deletions that the floor passed
    /// (format spec, "Advancing the retention floor").
    ///
    /// When the current manifest's acknowledged floor is at its retention
    /// floor, the call publishes nothing. Otherwise it folds the WAL tail,
    /// which acknowledges the floor when the writer committed after the
    /// floor moved. If the acknowledged floor is still lower, the call
    /// acquires the writer epoch under this handle's writer ID, writes a
    /// fence that records the floor, and folds again (format spec, "Writer
    /// ownership"). A writer session of the namespace, in this runtime or
    /// another, then returns `writer_fenced` to its next publication, and a
    /// later write opens a new session. Each fold waits for a fold permit
    /// from the runtime's execution budget.
    ///
    /// The runtime doesn't schedule this call. The reference server makes
    /// it on each full maintenance pass, before the pass's retention
    /// advance, so it acknowledges the floor that the previous pass set.
    pub async fn acknowledge_retention_floor(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<AcknowledgeRetentionFloorReport> {
        trace::in_operation(
            operation_span!(
                self.compactor.core,
                "acknowledge_retention_floor",
                namespace_id
            ),
            async {
                let before = RetentionFloors::of(
                    &self.compactor.load_live_basis(namespace_id).await?.manifest,
                );
                let report = |after, outcome| AcknowledgeRetentionFloorReport {
                    namespace_id: namespace_id.clone(),
                    before,
                    after,
                    outcome,
                };
                if before.is_acknowledged() {
                    return Ok(report(
                        before,
                        AcknowledgeRetentionFloorOutcome::AlreadyAcknowledged,
                    ));
                }
                self.compactor.run_wal_fold(namespace_id).await?;
                let folded = RetentionFloors::of(
                    &self.compactor.load_live_basis(namespace_id).await?.manifest,
                );
                if folded.is_acknowledged() {
                    return Ok(report(folded, AcknowledgeRetentionFloorOutcome::Folded));
                }
                let acquired = self
                    .compactor
                    .engine(namespace_id)
                    .acquire_writer_epoch()
                    .await
                    .map_err(Error::from);
                self.compactor
                    .publisher
                    .finish_publication(namespace_id, acquired)
                    .await?;
                self.compactor.run_wal_fold(namespace_id).await?;
                let fenced = RetentionFloors::of(
                    &self.compactor.load_live_basis(namespace_id).await?.manifest,
                );
                Ok(report(fenced, AcknowledgeRetentionFloorOutcome::Fenced))
            },
        )
        .await
    }
}
