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
//! - [`publish`] writes manifest objects and advances `metadata/root.json`
//!   by compare-and-swap.
//! - [`record`] and [`release`] manage durable checkpoint records, and
//!   [`files`] enumerates the files a record's manifest pins.
//! - [`load`] and [`validate`] provide envelope-only loading and
//!   descriptor-only table verification (full-row inspection
//!   materialization is test-only).
//! - [`scan`] answers verified row scans over loaded manifest tables, while
//!   [`row`] handles manifest-row encoding.
//! - [`reorganize`] compacts bounded family groups into new base runs.
//! - [`retention`] advances the retention floor behind verified progress.
//! - [`runs`] models the LSM run layout shared by all of the above, and
//!   [`cache`] holds decoded SST blocks keyed by content digest.

mod block_fetch;
mod block_load;
mod build;
mod cache;
mod create;
mod data_block_load;
mod error;
mod files;
mod flush;
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
pub use self::files::{CheckpointFile, CheckpointFilesPage, CheckpointFilesPageCursor};
pub use self::reorganize::{MetadataReorganizeOutcome, MetadataReorganizeReport};
pub(crate) use self::runs::MetadataLsmPolicy;

pub(crate) use self::create::create_checkpoint;
pub(crate) use self::files::list_checkpoint_files_page;
pub(crate) use self::flush::flush_wal;
pub(crate) use self::load::{
    head_from_manifest, load_basis_metadata_tables, load_namespace_manifest_envelope,
    load_namespace_manifest_envelope_if_present,
};
pub(crate) use self::record::{freshen_fork_checkpoint, read_checkpoint_record};
pub(crate) use self::release::release_checkpoint;
pub(crate) use self::reorganize::reorganize_metadata_step;
pub(crate) use self::retention::advance_retention_floor;
pub(crate) use self::scan::{Readahead, VerifiedMetadataTables};
