//! What a metadata maintenance pass and a compaction unit report.

use crate::{ChangeSeq, ManifestNo, NamespaceId};

/// What the WAL fold step of a maintenance pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalFoldStepOutcome {
    /// The tail was below the threshold, so there was nothing to fold.
    NotNeeded,
    /// The step folded the WAL tail and published the next current manifest.
    Folded {
        /// The last sequence that the published manifest includes.
        manifest_head_seq: ChangeSeq,
    },
    /// The current manifest already included the captured WAL tail, so this
    /// step didn't publish a manifest.
    AlreadyPublished {
        /// The sequence that this step tried to fold through.
        attempted_seq: ChangeSeq,
        /// The number of the namespace's current manifest.
        current_manifest_no: ManifestNo,
    },
    /// Concurrent updates prevented every publication attempt.
    RetriesExhausted {
        /// The head sequence observed before the step ran.
        observed_head_seq: ChangeSeq,
    },
}

/// What the WAL fold and the compaction unit of one metadata maintenance
/// pass did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataPassReport {
    /// The namespace that this pass maintained.
    pub namespace_id: NamespaceId,
    /// What the WAL fold did.
    pub wal_fold: WalFoldStepOutcome,
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
    /// The unit merged the selected window and published the next manifest.
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
    /// The selected window lists more rows than a metadata pass merges, so
    /// nothing was published.
    ///
    /// [`Maintenance::compact_metadata`](crate::Maintenance::compact_metadata)
    /// merges a window of any size, and
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
