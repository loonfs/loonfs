//! LoonFS full-text grep subsystem and reference derived projection.
//!
//! Grep uses only public runtime APIs. Enabling it pins a checkpoint and
//! backfills files in bounded pages. Once active, it consumes the semantic
//! change feed from the checkpoint sequence.
//!
//! Each step publishes the segment list and watermark with one root
//! compare-and-swap. Failed publications leave unreferenced derived data for
//! grep garbage collection. A retention gap causes a new checkpoint and
//! backfill.
//!
//! Queries combine indexed postings with the unindexed tail, then verify each
//! candidate against current file contents. Grep state is stored under
//! `extensions/grep/` and does not affect filesystem availability.

mod cache;
pub mod codec;
mod config;
mod error;
mod index_read;
pub mod keyspace;
mod maintenance;
mod query;
mod reads;
pub mod root;
mod service;
mod worker;

pub use cache::{
    new_grep_block_cache, DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey,
    DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
pub use config::{GrepWorkerConfig, GrepWorkerConfigError};
pub use error::GrepError as Error;
pub use error::{GrepError, Result};
pub use maintenance::{GrepGcJob, GrepMaintenanceJob, GREP_GC_JOB, GREP_INDEX_JOB};
pub use reads::NamespaceReads;
pub use service::{GrepService, MAX_GREP_SCAN_FILES, MAX_GREP_TAIL_FILES};
pub use worker::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepBuildReport, GrepDisableOutcome, GrepEnableOutcome,
    GrepGcOptions, GrepGcReport, GrepReorganizeOutcome, GrepReorganizeReport, GrepWorker,
    GREP_BACKFILL_CHECKPOINT_TTL_MS, GREP_GC_GRACE_WINDOW_MS,
};
