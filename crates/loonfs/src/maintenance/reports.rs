//! What a metadata maintenance step and a compaction unit report.

use crate::{
    FoldWalResponse, ManifestNo, MetadataMaintenanceResponse, MetadataSkippedWork, NamespaceId,
};

/// What one [`Maintenance::maintain_metadata_while_due`](crate::Maintenance::maintain_metadata_while_due)
/// call did, and when its work next comes due.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataMaintenanceReport {
    /// The call's response, as the HTTP API returns it.
    pub response: MetadataMaintenanceResponse,
    /// When the tail's idle fold comes due, on the runtime's monotonic timer
    /// (see [`LoonFs::monotonic_timer`](crate::LoonFs::monotonic_timer)),
    /// when that fold is the only work left: the outcome is
    /// [`MetadataMaintenanceOutcome::Due`](crate::MetadataMaintenanceOutcome::Due)
    /// only because the tail waits for its age. `None` when nothing is due,
    /// or when other work is left.
    pub next_due_at_ms: Option<u64>,
}

/// What the WAL fold and the compaction unit of one metadata maintenance
/// step did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintainMetadataReport {
    /// The namespace that this step maintained.
    pub namespace_id: NamespaceId,
    /// The fold's response, or `None` when no fold was due.
    pub wal_fold: Option<FoldWalResponse>,
    /// What the compaction unit did.
    pub compaction: MetadataCompactionOutcome,
}

/// What one metadata compaction unit did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCompactionReport {
    /// The namespace that this job compacted.
    pub namespace_id: NamespaceId,
    /// The outcome of the compaction.
    pub compaction: MetadataCompactionOutcome,
}

/// The outcome of one metadata compaction unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCompactionOutcome {
    /// No eligible family group was available, so nothing was published.
    NotNeeded,
    /// The unit compacted the selected window and published the next manifest.
    Published {
        /// The manifest that the compaction published.
        manifest_no: ManifestNo,
        /// The rows that the compaction read.
        rows_read: u64,
        /// The rows that the compaction wrote.
        rows_written: u64,
        /// The input bytes that the compaction read.
        input_bytes: u64,
        /// The output bytes that the compaction wrote.
        output_bytes: u64,
        /// The output segments that the compaction wrote.
        output_segments: u64,
        /// The work that the compaction skipped at a limit, each reason once.
        skipped: Vec<MetadataSkippedWork>,
    },
    /// The run was cancelled, so the current manifest didn't change.
    Cancelled,
    /// The unit stopped because its inputs changed, time ran out, or every
    /// publication attempt lost.
    Abandoned,
}
