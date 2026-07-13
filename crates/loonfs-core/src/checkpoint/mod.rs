//! Checkpoints, namespace manifests, and metadata SSTs.
//!
//! A namespace manifest names the immutable metadata SST runs that
//! materialize one namespace file-set version; a checkpoint pins one manifest
//! version for retention, forks, stable reads, and restore. Submodules follow
//! the manifest lifecycle:
//!
//! - [`flush`] folds the visible WAL tail into metadata tables and
//!   advances `metadata/root.json` — the record-less latest-state
//!   maintenance path.
//! - [`create`] orchestrates checkpoint creation: flush, then pin
//!   the resulting manifest under one durable record.
//! - [`build`] segments metadata rows and writes the immutable SST objects.
//! - [`index_build`] builds the gram index in budgeted maintenance steps,
//!   publishing manifests that advance the `index.grams` watermark.
//! - [`publish`] writes manifest objects and advances `current_manifest_id`
//!   on the head by compare-and-swap.
//! - [`load`] and [`validate`] provide envelope-only loading and
//!   descriptor-only table verification (full-row inspection
//!   materialization is test-only).
//! - [`scan`] answers verified row scans over loaded manifest tables.
//! - [`retention`] advances the retention floor behind verified progress.
//! - [`runs`] models the LSM run layout shared by all of the above, and
//!   [`cache`] holds decoded SST blocks keyed by content digest.

mod build;
mod cache;
mod create;
mod error;
mod flush;
mod index_build;
mod load;
mod publish;
pub(crate) mod record;
mod release;
mod reorganize;
mod retention;
mod row;
mod runs;
mod scan;
#[cfg(test)]
pub(crate) mod tests;
mod validate;

pub use self::cache::{
    MetadataTableCache, MetadataTableCacheConfig, MetadataTableCacheStats, WalTailProjectionCache,
    WalTailProjectionCacheConfig, WalTailProjectionCacheKey, WalTailProjectionCacheStats,
    DEFAULT_METADATA_TABLE_CACHE_DECODED_BYTES, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
pub use self::error::{ManifestLoadError, ManifestLoadFailureClass};
pub use self::index_build::{
    GramIndexBuildOutcome, GramIndexBuildPolicy, GramIndexBuildReport, GramIndexDisableOutcome,
    GramIndexEnableOutcome, GramIndexFoldOutcome, GramIndexFoldReport,
};
pub use self::reorganize::{MetadataReorganizeOutcome, MetadataReorganizeReport};
pub(crate) use self::runs::MetadataLsmPolicy;

pub(crate) use self::create::{build_initial_namespace_manifest, create_checkpoint};
pub(crate) use self::flush::flush_wal;
pub(crate) use self::index_build::{
    build_grams_index_step, disable_grams_index, enable_grams_index, fold_grams_index_step,
    is_indexable_text_content,
};
pub(crate) use self::load::{
    head_from_manifest, load_namespace_manifest_envelope, load_verified_manifest_tables,
    load_verified_manifest_tables_with_cache,
};
pub(crate) use self::publish::write_namespace_manifest;
pub(crate) use self::record::{freshen_fork_checkpoint, read_checkpoint_record};
pub(crate) use self::release::release_checkpoint;
pub(crate) use self::reorganize::reorganize_metadata_step;
pub(crate) use self::retention::advance_retention_floor;
pub(crate) use self::scan::{string_prefix_upper_bound, VerifiedMetadataTables};
