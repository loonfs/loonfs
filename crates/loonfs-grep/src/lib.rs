//! LoonFS full-text grep subsystem.
//!
//! Grep durable state lives under each namespace's `extensions/grep/`
//! prefix and is independent of the namespace manifest. A missing or corrupt
//! grep root disables grep work for that namespace; it never affects core
//! filesystem operation. A [`GrepDriver`] owns the bounded [`GrepWorker`]
//! steps for exactly one namespace, runs continuously until caught up, and
//! then waits for an event-driven nudge.

mod cache;
pub mod codec;
mod config;
mod driver;
mod error;
mod index_read;
pub mod keyspace;
mod query;
pub mod root;
mod service;
mod worker;

pub use config::{GrepWorkerConfig, GrepWorkerConfigError};
pub use driver::{GrepDriver, GrepDriverHandle, GrepDriverParked, GrepDriverState, GrepDriverTask};
pub use error::GrepError as Error;
pub use error::{GrepError, Result};
pub use service::{
    GrepIndexSnapshot, GrepService, DEFAULT_GREP_PAGE_LIMIT, MAX_GREP_PAGE_LIMIT,
    MAX_GREP_SCAN_FILES, MAX_GREP_TAIL_FILES,
};
pub use worker::{
    GramIndexBuildPolicy, GrepBuildOutcome, GrepBuildReport, GrepDisableOutcome, GrepEnableOutcome,
    GrepFoldOutcome, GrepFoldReport, GrepGcReport, GrepWorker, GREP_BACKFILL_CHECKPOINT_TTL_MS,
    GREP_GC_GRACE_WINDOW_MS,
};
