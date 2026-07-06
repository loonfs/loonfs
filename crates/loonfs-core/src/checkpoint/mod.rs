//! Checkpoints, namespace manifests, and metadata SSTs.
//!
//! Manifests name immutable metadata files. Checkpoints pin manifest versions
//! for retention, forks, stable reads, and restore.

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
