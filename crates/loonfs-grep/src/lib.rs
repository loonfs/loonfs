//! LoonFS full-text grep subsystem and reference derived projection.
//!
//! Grep uses only public runtime APIs. Enabling it creates a snapshot and
//! backfills files in bounded pages. Once active, it consumes the semantic
//! change feed from the snapshot's sequence.
//!
//! Grep's state is its entry in the namespace manifest, which lists the
//! index's segments. A run of steps changes the entry in memory and
//! publishes the change in a manifest of its own, and core garbage
//! collection collects the segments that no kept manifest lists. A retention
//! gap causes a new snapshot and backfill.
//!
//! Queries combine indexed postings with the unindexed tail, then verify each
//! candidate against current file contents. Grep segments are stored under
//! `extensions/grep/` and don't affect filesystem availability.

mod cache;
pub mod codec;
mod config;
pub mod entry;
mod error;
mod index_read;
mod query;
mod reads;
mod segments;
mod service;
mod step_budget;
mod value_table;
mod worker;

pub use cache::{
    DecodedGrepBlock, GrepBlockCache, GrepBlockCacheKey, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
};
pub use config::{GrepWorkerConfig, GrepWorkerConfigError};
pub use error::{GrepError, Result};
pub use reads::NamespaceReads;
pub use service::{GrepService, MAX_GREP_SCAN_FILES, MAX_GREP_TAIL_FILES};
pub use step_budget::{GrepStepBudget, DEFAULT_MAX_CONCURRENT_GREP_STEPS};
pub use worker::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepDisableOutcome, GrepEnableOutcome, GrepProgress,
    GrepPublicationStats, GrepPublishOutcome, GrepReorganizeOutcome, GrepSteps, GrepWorker,
    DEFAULT_GREP_PUBLISH_INTERVAL_MS,
};
