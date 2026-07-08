//! Checkpoints, namespace manifests, and metadata SSTs.
//!
//! A namespace manifest names the immutable metadata SST runs that
//! materialize one namespace file-set version; a checkpoint pins one manifest
//! version for retention, forks, stable reads, and restore. Submodules follow
//! the manifest lifecycle:
//!
//! - [`create`] orchestrates checkpoint creation: project a manifest from the
//!   current materialization, then publish it.
//! - [`build`] segments metadata rows and writes the immutable SST objects.
//! - [`publish`] writes manifest objects and advances `current_manifest_id`
//!   on the head by compare-and-swap.
//! - [`load`] and [`validate`] provide envelope-only loading, descriptor-only
//!   table verification, and explicit inspection materialization when callers
//!   truly need every metadata row.
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
pub(crate) mod record;
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
    DEFAULT_METADATA_TABLE_CACHE_DECODED_BYTES, DEFAULT_METADATA_TABLE_CACHE_MAX_BLOCKS,
};
pub use self::error::{ManifestLoadError, ManifestLoadFailureClass};
pub use self::runs::MetadataLsmPolicy;

pub(crate) use self::create::{
    build_initial_namespace_manifest, create_checkpoint, create_checkpoint_with_policy_and_owner,
};
pub(crate) use self::load::{
    head_from_manifest, load_namespace_manifest_envelope, load_verified_manifest_tables,
    load_verified_manifest_tables_with_cache,
};
pub(crate) use self::publish::write_namespace_manifest;
pub(crate) use self::record::{read_checkpoint_record, verify_checkpoint_basis};
pub(crate) use self::retention::advance_retention_floor;
pub(crate) use self::scan::{string_prefix_upper_bound, VerifiedMetadataTables};
