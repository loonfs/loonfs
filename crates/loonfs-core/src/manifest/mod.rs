//! Namespace manifests: loading and validation, publication, WAL folds,
//! retention, and statistics.
//!
//! A namespace manifest references the immutable metadata segment runs for one
//! namespace state. Folding, compaction, and retention publish manifests. The
//! segments themselves are in [`crate::segment`], and compaction is in
//! [`crate::compaction`].

pub(crate) mod error;
pub(crate) mod fold;
pub(crate) mod load;
pub(crate) mod publish;
pub(crate) mod published;
pub(crate) mod retention;
pub(crate) mod statistics;
#[cfg(test)]
pub(crate) mod tests;
pub(crate) mod validate;

pub use self::error::ManifestLoadError;
pub use self::fold::{next_run_no_after, FoldWalOutcome, FoldWalResponse, FoldedWalTail};
pub use self::load::load_position_basis;
pub(crate) use self::published::same_head;
pub use self::published::{PublicationReport, PublishedManifest};
pub use self::retention::RetentionTarget;
pub use self::statistics::NamespaceStatistics;

pub(crate) use self::load::{
    ensure_manifest_reference_matches, head_from_manifest, load_basis_metadata_segments,
    load_manifest_segments, load_namespace_manifest_envelope,
    load_namespace_manifest_envelope_if_present, metadata_basis_from_manifest, LoadedMetadataBasis,
};
