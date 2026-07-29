//! LoonFS full-text grep subsystem.
//!
//! Grep durable state lives under each namespace's `extensions/grep/`
//! prefix and is independent of the namespace manifest. A missing or corrupt
//! grep root disables grep work for that namespace; it never affects core
//! filesystem operation. A [`GrepDriver`] owns the bounded [`GrepWorker`]
//! steps for exactly one namespace, runs continuously until caught up, and
//! then waits for an event-driven nudge.
//!
//! Everything grep knows about the filesystem it learns through supported
//! reads, collected in [`NamespaceReads`]: the files a checkpoint pins, the
//! semantic change feed after it, current state for a batch of inodes, and
//! content by reference. Core read and replay stay in `loonfs-core`, wire
//! vocabulary stays in `loonfs-api`, and embedded scheduling stays in
//! `loonfs`.

mod cache;
pub mod codec;
mod config;
mod driver;
mod error;
mod index_read;
pub mod keyspace;
mod query;
mod reads;
pub mod root;
mod service;
mod worker;

pub use config::{GrepWorkerConfig, GrepWorkerConfigError, DEFAULT_MAX_CONCURRENT_STEPS};
pub use driver::{
    GrepDriver, GrepDriverHandle, GrepDriverParked, GrepDriverState, GrepDriverTask,
    GrepStepLimiter,
};
pub use error::GrepError as Error;
pub use error::{GrepError, Result};
pub use reads::NamespaceReads;
pub use service::{
    GrepIndexSnapshot, GrepService, DEFAULT_GREP_PAGE_LIMIT, MAX_GREP_PAGE_LIMIT,
    MAX_GREP_SCAN_FILES, MAX_GREP_TAIL_FILES,
};
pub use worker::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepBuildReport, GrepDisableOutcome, GrepEnableOutcome,
    GrepGcReport, GrepReorganizeOutcome, GrepReorganizeReport, GrepWorker,
    GREP_BACKFILL_CHECKPOINT_TTL_MS, GREP_GC_GRACE_WINDOW_MS,
};
