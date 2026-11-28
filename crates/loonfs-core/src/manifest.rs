//! Namespace manifests: loading and validation, publication, WAL folds,
//! retention, and statistics.
//!
//! A namespace manifest references the immutable metadata segment runs for one
//! namespace state, and each extension's entry. Folding, compaction,
//! retention, and extensions publish manifests. The
//! segments themselves are in [`crate::segment`], and compaction is in
//! [`crate::compaction`].

pub(crate) mod error;
pub(crate) mod extension;
pub(crate) mod fold;
pub(crate) mod fork_tail;
pub(crate) mod load;
pub(crate) mod publish;
pub(crate) mod published;
pub(crate) mod retention;
pub(crate) mod statistics;
#[cfg(test)]
pub(crate) mod tests;
pub(crate) mod validate;

pub use self::error::ManifestLoadError;
pub use self::extension::{ExtensionBasis, ExtensionPublication};
pub use self::fold::{
    FoldWalOutcome, FoldWalResponse, FoldWriteOutcome, FoldWrites, FoldedWalTail,
};
pub(crate) use self::fork_tail::ForkPointTail;
pub(crate) use self::published::move_read_basis;
pub use self::published::PublicationReport;
pub use self::retention::{FloorObjectReport, RetentionPlan, RetentionTarget};
pub use self::statistics::NamespaceStatistics;

#[cfg(any(test, feature = "test-support"))]
pub(crate) use self::load::metadata_basis_from_manifest;
pub(crate) use self::load::{
    ensure_manifest_reference_matches, head_from_manifest, load_namespace_manifest_envelope,
    load_referenced_manifest, manifest_base_state, project_manifest_tail, DecodedManifest,
    LoadedMetadataBasis,
};
