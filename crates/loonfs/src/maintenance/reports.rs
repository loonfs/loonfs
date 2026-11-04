//! What a metadata maintenance pass and a compaction unit report.

use crate::{FoldWalResponse, ManifestNo, NamespaceId};

/// What the WAL fold and the compaction unit of one metadata maintenance
/// pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataPassReport {
    /// The namespace that this pass maintained.
    pub namespace_id: NamespaceId,
    /// The fold's response, or `None` when no fold was due.
    pub wal_fold: Option<FoldWalResponse>,
    /// What the compaction unit did.
    pub compaction: MetadataCompactionOutcome,
}

/// What one metadata compaction unit did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCompactionReport {
    /// The namespace that this run compacted.
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
    },
    /// The selected window lists more rows than a metadata pass compacts, so
    /// nothing was published.
    ///
    /// [`Maintenance::compact_metadata`](crate::Maintenance::compact_metadata)
    /// compacts a window of any size, and
    /// [`Maintenance::maintain_metadata_while_due`](crate::Maintenance::maintain_metadata_while_due)
    /// runs it whenever a pass reports this outcome.
    MetadataCompactionRequired,
    /// The run was cancelled, so the current manifest didn't change.
    Cancelled,
    /// The unit stopped because its inputs changed, time ran out, or every
    /// publication attempt lost.
    Abandoned,
    /// Another process claimed the namespace's compactor epoch, so nothing
    /// was published.
    Fenced,
}
