//! LoonFS full-text grep subsystem, and the reference derived projection.
//!
//! Grep is a first-party extension, not part of the runtime: it depends on
//! `loonfs` and `loonfs` knows nothing about it. Anything else that derives
//! its own data from a namespace — an embedding index, a symbol graph — can
//! be built the same way, from the same public surfaces. The shape:
//!
//! **Bootstrap from a checkpoint.** Enabling grep pins a checkpoint and
//! publishes a `Backfilling` root that names it. The backfill walks the
//! files that checkpoint pins, in inode order, a bounded page at a time,
//! remembering the last inode it consumed. The checkpoint is what makes
//! that walk resumable across steps and processes: it answers the same
//! state every time, whatever commits land meanwhile.
//!
//! **Semantic changes after it.** Once the walk finishes, the lifecycle
//! changes to `Active` and indexing continues from the checkpoint's sequence.
//! The feed reports what happened, not which bytes moved, so grep indexes
//! only the events that publish content and ignores moves, deletes, and
//! undeletes outright — the index is keyed by durable `(inode_id,
//! revision_no)`, which those events do not change.
//!
//! **Watermark by root publication.** Every step ends in one compare-and-swap
//! on grep's own root object, which advances the watermark and the segment
//! list together. Nothing is indexed until that CAS lands, so a crashed
//! step leaves unreferenced segments rather than a half-advanced cursor,
//! and a lost race is an outcome the caller retries.
//!
//! **Rebootstrap on a retention gap.** If the feed cursor falls below the
//! namespace's retention floor, or the pinned checkpoint stops pinning its
//! basis, the history the projection needs is gone. Grep does not guess: it
//! discards the incomplete projection, pins a fresh checkpoint, and starts
//! over.
//!
//! **Queries generate candidates; current state verifies them.** The index
//! narrows, it never decides. A query turns gram postings and the unindexed
//! tail into candidate inodes, then resolves each against current state and
//! runs the real pattern over the bytes that path publishes now. That is
//! what lets the index lag, hold postings for revisions that were replaced,
//! or miss a rename without ever returning a wrong match — and it is why
//! per-call reads suffice where a pinned snapshot once seemed necessary.
//!
//! **Three hosting modes, one protocol.** The reference server composes
//! grep in-process and registers [`GrepMaintenanceJob`] with the writer's
//! maintenance runner, nudged by the publish observer; a query-serving
//! deployment answers searches while a detached `loonfs admin run` host
//! registers that same job for the namespaces it was assigned; a one-shot
//! host (the CLI's `index-enable`) runs the job's steps itself, until the
//! index reaches a sequence it captured before it started. All three run
//! the identical durable protocol, so which one is running is a deployment
//! choice, not a format.
//!
//! **An extension owns its keyspace and its garbage.** Grep's durable state
//! lives under each namespace's `extensions/grep/` prefix and is
//! independent of the namespace manifest; a missing or corrupt grep root
//! disables grep for that namespace and never affects filesystem operation.
//! Grep collects that prefix itself
//! ([`GrepWorker::garbage_collect_namespace`]), because only grep knows
//! which of its objects are still reachable.
//!
//! Everything grep knows about the filesystem it learns through public
//! runtime calls, collected in [`NamespaceReads`]: the files a checkpoint
//! pins, the semantic change feed after it, current state for a batch of
//! inodes, directory pages, and content by reference. Wire vocabulary stays
//! in `loonfs-api`, and the filesystem stays in `loonfs`.

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

pub use cache::{GrepBlockCache, GrepBlockCacheStats, DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES};
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
