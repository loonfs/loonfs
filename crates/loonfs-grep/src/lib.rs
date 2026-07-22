//! LoonFS full-text grep subsystem.
//!
//! Grep durable state lives under each namespace's `extensions/grep/`
//! prefix and is independent of the namespace manifest. A missing or corrupt
//! grep root disables grep work for that namespace; it never affects core
//! filesystem operation. [`GrepWorker`] registers in-process enablements
//! directly with its loop. Startup and periodic rediscovery must list the
//! whole `namespaces/` keyspace and are proportional to the store's total key
//! count—the price of the namespace-scoped layout until a namespace catalog
//! exists. [`GrepWorkerLoop`] keeps that scan rare and drives the same bounded
//! work for server-embedded and standalone hosts.

mod cache;
pub mod codec;
mod config;
mod index_read;
pub mod keyspace;
mod query;
pub mod root;
mod service;
mod worker;
mod worker_loop;

pub use config::{
    GrepWorkerConfig, GrepWorkerConfigError, DEFAULT_GREP_GC_INTERVAL_MS,
    DEFAULT_GREP_RESCAN_INTERVAL_MS, DEFAULT_GREP_STEP_INTERVAL_MS,
};
pub use service::{
    GrepIndexSnapshot, GrepService, DEFAULT_GREP_PAGE_LIMIT, MAX_GREP_PAGE_LIMIT,
    MAX_GREP_SCAN_FILES, MAX_GREP_TAIL_FILES,
};
pub use worker::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepBuildReport, GrepDisableOutcome, GrepEnableOutcome,
    GrepFoldOutcome, GrepFoldReport, GrepGcReport, GrepWorker, GREP_BACKFILL_CHECKPOINT_TTL_MS,
    GREP_GC_GRACE_WINDOW_MS,
};
pub use worker_loop::{
    GrepSweepReport, GrepWorkerLoop, GrepWorkerLoopShutdown, GrepWorkerRunOnceError,
};
