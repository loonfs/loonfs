//! Checkpoints, namespace manifests, and metadata segments.
//!
//! A namespace manifest references the immutable metadata segment runs for one
//! namespace state. A checkpoint pins a manifest for retention, forks, stable
//! reads, or restore.

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
mod read_basis;
pub(crate) mod record;
mod release;
mod reorganize;
mod retention;
mod row;
mod runs;
mod scan;
mod snapshot;
mod stored_block_cache;
mod streaming_compaction;
#[cfg(test)]
pub(crate) mod tests;
mod validate;

pub use self::build::write_segments_in_waves;
pub use self::cache::{
    MetadataSegmentCache, MetadataSegmentCacheConfig, MetadataSegmentCacheStats,
    WalTailProjectionCache, WalTailProjectionCacheConfig, WalTailProjectionCacheKey,
    WalTailProjectionCacheStats, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
pub use self::compaction_merge::{
    refill_iterators, select_next_iterator, SegmentBlockLoader, SegmentRowIterator,
};
pub use self::error::{ManifestLoadError, ManifestLoadFailureClass};
pub use self::files::{CheckpointFile, CheckpointFilesPage, CheckpointFilesPageCursor};
pub use self::flush::{ensure_metadata_publication_budget, fold_wal_tail, next_run_no_after};
pub use self::list::CheckpointPageCursor;
pub use self::read_basis::{load_checkpoint_read_basis, CheckpointReadBasis};
pub use self::reorganize::{FrozenBasePolicy, MetadataReorganizeOutcome, MetadataReorganizeReport};
pub use self::runs::MetadataFamilyGroup;
pub(crate) use self::runs::MetadataLsmPolicy;
pub use self::snapshot::load_snapshot_read_basis;
pub use self::stored_block_cache::{
    StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
    StoredMetadataBlockKind,
};
pub use self::streaming_compaction::{
    MetadataCompactionCancellation, MetadataCompactionJobOutcome, MetadataCompactionSpec,
};

pub(crate) use self::compaction_lease::{
    claim_loaded_group_lease, load_group_lease, CompactionPrefixOwner, LoadedCompactionLease,
};
pub(crate) use self::create::create_checkpoint;
pub(crate) use self::data_block_load::DecodedRowWeight;
pub(crate) use self::files::list_checkpoint_files_page;
pub(crate) use self::flush::flush_wal;
pub(crate) use self::list::list_checkpoints_page;
pub(crate) use self::load::{
    head_from_manifest, load_basis_metadata_segments, load_namespace_manifest_envelope,
    load_namespace_manifest_envelope_if_present, load_verified_manifest_segments,
    LoadedMetadataBasis,
};
#[cfg(test)]
pub(crate) use self::publish::write_namespace_manifest;
pub(crate) use self::record::load_checkpoint_record;
pub(crate) use self::release::release_checkpoint;
pub use self::reorganize::metadata_maintenance_due;
pub(crate) use self::reorganize::reorganize_metadata_step;
pub(crate) use self::retention::advance_retention_floor;
pub(crate) use self::scan::{Readahead, VerifiedMetadataSegments};
pub(crate) use self::snapshot::{extend_snapshot_expiry, release_snapshot};
pub(crate) use self::streaming_compaction::run_metadata_compaction_job;

fn checkpoint_summary(
    record: loonfs_api::wire::control::CheckpointRecordState,
) -> loonfs_api::Checkpoint {
    let expires_at_ms = record.owner.expires_at_ms();
    let owner = match record.owner {
        loonfs_api::wire::control::CheckpointOwner::User { name, .. } => {
            loonfs_api::CheckpointOwnerSummary::User { name }
        }
        loonfs_api::wire::control::CheckpointOwner::Fork {
            target_namespace_id,
            ..
        } => loonfs_api::CheckpointOwnerSummary::Fork {
            target_namespace_id,
        },
        loonfs_api::wire::control::CheckpointOwner::Snapshot { name, .. } => {
            loonfs_api::CheckpointOwnerSummary::Snapshot { name }
        }
    };
    loonfs_api::Checkpoint {
        namespace_id: record.namespace_id,
        checkpoint_id: record.checkpoint_id,
        owner,
        created_at_ms: record.created_at_ms,
        expires_at_ms,
        checkpoint_seq: record.manifest.manifest_head_seq,
        manifest_no: record.manifest.manifest_no,
    }
}
