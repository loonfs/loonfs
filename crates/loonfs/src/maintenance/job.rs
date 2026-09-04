//! Contracts and reports shared by registered maintenance jobs.

use crate::{ChangeSeq, NamespaceId, Result};
use async_trait::async_trait;
use std::fmt;

/// Stable identifier for a registered maintenance job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaintenanceJobId(&'static str);

impl MaintenanceJobId {
    /// Bounded metadata maintenance.
    pub const METADATA: Self = Self("metadata");
    /// Streaming metadata compaction.
    pub const METADATA_COMPACTION: Self = Self("metadata_compaction");
    /// Garbage collection.
    pub const GC: Self = Self("gc");

    /// Creates an extension job identifier.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// Returns the stable identifier.
    pub fn as_str(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for MaintenanceJobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Result category for one maintenance run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceConclusion {
    /// Durable work was published.
    Progressed,
    /// No work was due.
    Idle,
    /// Work remains but cannot proceed now.
    Blocked,
    /// Concurrent durable state replaced this attempt.
    Superseded,
    /// The job does not apply to this namespace.
    NotEnabled,
}

impl MaintenanceConclusion {
    /// Returns the wire and metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Progressed => "progressed",
            Self::Idle => "idle",
            Self::Blocked => "blocked",
            Self::Superseded => "superseded",
            Self::NotEnabled => "not_enabled",
        }
    }
}

/// Scheduling information returned by one maintenance run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceRunReport {
    /// How the run settled.
    pub conclusion: MaintenanceConclusion,
    /// Resume position for the next run.
    pub continuation: Option<String>,
    /// Earliest Unix millisecond for another run.
    pub not_before_ms: Option<u64>,
    /// A job this run wants scheduled for the same namespace.
    pub follow_up: Option<MaintenanceJobId>,
}

impl MaintenanceRunReport {
    /// Builds a report without continuation, deadline, or follow-up.
    pub fn concluded(conclusion: MaintenanceConclusion) -> Self {
        Self {
            conclusion,
            continuation: None,
            not_before_ms: None,
            follow_up: None,
        }
    }
}

/// State available to maintenance jobs after a publish attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespacePublication {
    /// Namespace whose publication was attempted.
    pub namespace_id: NamespaceId,
    /// Highest sequence committed by this attempt.
    pub committed_through_seq: Option<ChangeSeq>,
    /// WAL segments visible after the attempt.
    pub wal_tail_segments: u64,
}

/// Result of checking whether a job has durable work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceProbe {
    /// Work is due.
    Due,
    /// No work is due.
    Idle,
}

/// Cancellation shared with one running invocation.
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

/// One maintenance operation available to a registry.
#[async_trait]
pub trait MaintenanceJob: Send + Sync + 'static {
    /// Returns this job's stable identifier.
    fn id(&self) -> MaintenanceJobId;

    /// One run for one namespace. Reloads durable state, does bounded work
    /// where the job can, and reports. A long-running job checks
    /// `cancellation` between units of work.
    async fn run(
        &self,
        namespace_id: &NamespaceId,
        continuation: Option<&str>,
        cancellation: &MaintenanceCancellation,
    ) -> Result<MaintenanceRunReport>;

    /// Checks durable state without performing work.
    async fn probe(&self, namespace_id: &NamespaceId) -> Result<MaintenanceProbe>;

    /// Returns whether a publication should nudge this job.
    fn should_run_after_publication(&self, _publication: &NamespacePublication) -> bool {
        false
    }

    /// Returns whether a finished WAL-fold attempt should nudge this job.
    fn should_run_after_fold(&self) -> bool {
        false
    }
}
