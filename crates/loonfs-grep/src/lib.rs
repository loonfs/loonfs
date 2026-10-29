//! LoonFS full-text grep subsystem and reference derived projection.
//!
//! Grep uses only public runtime APIs. Enabling it creates a snapshot and
//! backfills files in bounded pages. Once active, it consumes the semantic
//! change feed from the snapshot's sequence.
//!
//! Each step publishes the segment list and watermark under the next manifest number. Failed publications leave unreferenced derived data for
//! grep garbage collection. A retention gap causes a new snapshot and
//! backfill.
//!
//! Queries combine indexed postings with the unindexed tail, then verify each
//! candidate against current file contents. Grep state is stored under
//! `extensions/grep/` and does not affect filesystem availability.

mod cache;
pub mod codec;
mod config;
mod error;
mod gc;
mod index_read;
pub mod keyspace;
pub mod manifest;
mod query;
mod reads;
mod segments;
mod service;
mod step_budget;
mod worker;

pub use cache::{
    DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
pub use config::{GrepWorkerConfig, GrepWorkerConfigError};
pub use error::{GrepError, Result};
pub use reads::NamespaceReads;
pub use service::{GrepService, MAX_GREP_SCAN_FILES, MAX_GREP_TAIL_FILES};
pub use step_budget::{GrepPassBudget, GrepPassBudgetStats, DEFAULT_MAX_CONCURRENT_GREP_STEPS};
pub use worker::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepDisableOutcome, GrepEnableOutcome,
    GrepReorganizeOutcome, GrepWorker,
};

pub use gc::GREP_GC_GRACE_WINDOW_MS;
pub use loonfs_types::GrepGcReport;
