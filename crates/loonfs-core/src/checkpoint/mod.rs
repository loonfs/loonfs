//! Checkpoints, namespace manifests, and metadata SSTs.
//!
//! A namespace manifest names the immutable metadata SST runs that
//! materialize one namespace file-set version; a checkpoint pins one manifest
//! version for retention, forks, stable reads, and restore. Submodules follow
//! the manifest lifecycle:
//!
//! - [`create`] orchestrates checkpoint creation: project a manifest from the
//!   current basis, then publish it.
//! - [`build`] segments metadata rows and writes the immutable SST objects.
//! - [`publish`] writes manifest objects and advances `current_manifest_id`
//!   on the head by compare-and-swap.
//! - [`load`] and [`validate`] reconstruct a manifest's metadata state on the
//!   read side and check every durable artifact against its descriptors.
//! - [`scan`] answers verified row scans over loaded manifest tables.
//! - [`retention`] advances the retention floor behind verified progress.
//! - [`runs`] models the LSM run layout shared by all of the above, and
//!   [`cache`] holds decoded SST blocks keyed by content digest.

mod build;
mod cache;
mod create;
mod error;
mod load;
mod publish;
mod retention;
mod row;
mod runs;
mod scan;
#[cfg(test)]
mod tests;
mod validate;

pub use self::cache::{
    MetadataTableCache, MetadataTableCacheConfig, MetadataTableCacheStats, WalTailProjectionCache,
    WalTailProjectionCacheConfig, WalTailProjectionCacheKey, WalTailProjectionCacheStats,
};
pub use self::error::{ManifestLoadError, ManifestLoadErrorKind};
pub use self::runs::MetadataLsmPolicy;

pub(crate) use self::create::{build_initial_namespace_manifest, create_checkpoint};
pub(crate) use self::load::{
    load_namespace_manifest_envelope, load_verified_manifest_materialization,
    load_verified_manifest_tables_with_cache, manifest_basis_head,
};
pub(crate) use self::publish::write_namespace_manifest;
pub(crate) use self::retention::advance_retention_floor;
pub(crate) use self::scan::VerifiedMetadataTables;
