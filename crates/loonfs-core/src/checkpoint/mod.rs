//! Checkpoints, namespace manifests, and metadata SSTs.
//!
//! A namespace manifest references the immutable metadata SST runs for one
//! namespace state. A checkpoint pins a manifest for retention, forks, stable
//! reads, or restore.
//!
//! The submodules cover these areas:
//! - Building and publishing manifests: [`build`], [`flush`], [`create`], and
//!   [`publish`].
//! - Managing checkpoint records: [`record`], [`list`], [`release`], and
//!   [`files`].
//! - Loading, validating, and scanning metadata: [`load`], [`validate`],
//!   [`scan`], and [`row`].
//! - Planning and running metadata merges: [`reorganize`],
//!   [`streaming_compaction`], [`compaction_merge`],
//!   [`compaction_retention`], [`compaction_output`], [`compaction_lease`],
//!   and [`frozen_floor`].
//! - Advancing the retention floor: [`retention`].
//! - Describing run layout and caching: [`runs`], [`cache`], and
//!   [`stored_block_cache`].

mod block_fetch;
mod block_load;
mod build;
pub(crate) mod cache;
mod compaction_lease;
mod compaction_merge;
mod compaction_output;
mod compaction_retention;
mod create;
mod data_block_load;
mod error;
mod files;
mod flush;
mod frozen_floor;
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
    MetadataTableCache, MetadataTableCacheConfig, MetadataTableCacheObserver,
    MetadataTableCacheStats, WalTailProjectionCache, WalTailProjectionCacheConfig,
    WalTailProjectionCacheKey, WalTailProjectionCacheObserver, WalTailProjectionCacheStats,
    DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES, DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
pub use self::error::{ManifestLoadError, ManifestLoadFailureClass};
pub use self::files::{CheckpointFile, CheckpointFilesPage, CheckpointFilesPageCursor};
pub use self::list::CheckpointPageCursor;
pub use self::reorganize::{
    FrozenBasePolicy, MetadataCompactionView, MetadataReorganizeOutcome, MetadataReorganizeReport,
};
pub use self::runs::MetadataFamilyGroup;
pub(crate) use self::runs::MetadataLsmPolicy;
pub use self::stored_block_cache::{
    StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
    StoredMetadataBlockKind,
};
pub use self::streaming_compaction::{
    MetadataCompactionCancellation, MetadataCompactionJobOutcome, MetadataCompactionSpec,
};

pub(crate) use self::compaction_lease::{claim_compaction_prefix, CompactionPrefixOwner};
pub(crate) use self::create::create_checkpoint;
pub(crate) use self::files::list_checkpoint_files_page;
pub(crate) use self::flush::flush_wal;
pub(crate) use self::list::list_checkpoints_page;
pub(crate) use self::load::{
    head_from_manifest, load_basis_metadata_tables, load_namespace_manifest_envelope,
    load_namespace_manifest_envelope_if_present, load_verified_manifest_tables,
    LoadedMetadataBasis,
};
pub(crate) use self::record::read_checkpoint_record;
pub(crate) use self::release::release_checkpoint;
pub(crate) use self::reorganize::reorganize_metadata_step;
pub(crate) use self::retention::advance_retention_floor;
pub(crate) use self::scan::{Readahead, VerifiedMetadataTables};
pub(crate) use self::streaming_compaction::run_metadata_compaction_job;
