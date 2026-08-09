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
//! - [`record`], [`list`], and [`release`] manage durable checkpoint
//!   records, and [`files`] enumerates the files a record's manifest pins.
//! - [`load`] and [`validate`] provide envelope-only loading and
//!   descriptor-only table verification (full-row inspection
//!   materialization is test-only).
//! - [`scan`] answers verified row scans over loaded manifest tables, while
//!   [`row`] handles manifest-row encoding.
//! - [`reorganize`] compacts bounded family groups into new base runs, and
//!   [`streaming_compaction`] rebuilds a group whose bottom-anchored window
//!   no longer fits one of those steps — with [`compaction_retention`]
//!   holding the frozen floor's rules as streaming state and
//!   [`compaction_lease`] holding the running job's claim on its own output.
//! - [`retention`] advances the retention floor behind verified progress.
//! - [`runs`] models the LSM run layout shared by all of the above,
//!   [`cache`] holds decoded SST blocks keyed by content digest, and
//!   [`stored_block_cache`] defines the seam a node-local cache of the same
//!   blocks in their encoded form plugs into.

mod block_fetch;
mod block_load;
mod build;
mod cache;
mod compaction_lease;
mod compaction_retention;
mod create;
mod data_block_load;
mod error;
mod files;
mod flush;
mod list;
mod load;
mod publish;
pub(crate) mod record;
mod release;
mod reorganize;
mod retention;
mod row;
mod runs;
mod scan;
mod stored_block_cache;
mod streaming_compaction;
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
pub use self::reorganize::{
    MetadataCompactionView, MetadataReorganizeOutcome, MetadataReorganizeReport,
};
pub(crate) use self::runs::MetadataLsmPolicy;
pub use self::stored_block_cache::{
    StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
    StoredMetadataBlockKind,
};
pub use self::streaming_compaction::{
    MetadataCompactionCancellation, MetadataCompactionJobOutcome, MetadataCompactionSpec,
};

pub(crate) use self::compaction_lease::{read_compaction_lease_state, CompactionLeaseState};
pub(crate) use self::create::create_checkpoint;
pub(crate) use self::files::list_checkpoint_files_page;
pub(crate) use self::flush::flush_wal;
pub(crate) use self::list::list_checkpoints;
pub(crate) use self::load::{
    head_from_manifest, load_basis_metadata_tables, load_namespace_manifest_envelope,
    load_namespace_manifest_envelope_if_present, load_verified_manifest_tables,
};
pub(crate) use self::record::read_checkpoint_record;
pub(crate) use self::release::release_checkpoint;
pub(crate) use self::reorganize::reorganize_metadata_step;
pub(crate) use self::retention::advance_retention_floor;
pub(crate) use self::scan::{Readahead, VerifiedMetadataTables};
pub(crate) use self::streaming_compaction::run_metadata_compaction_job;
