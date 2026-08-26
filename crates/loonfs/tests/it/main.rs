//! One binary for the crate's integration tests: every former
//! `tests/<name>.rs` file is a module here, so the suite links once and
//! runs its tests as threads instead of as separate processes.
//!
//! `tests/tracing_capture.rs` stays a binary of its own; the comment there
//! explains why it cannot share a process.

mod attributes;
mod attribution_rows;
mod binding_generation;
mod bulk_file_reads;
mod cache_seeding;
mod capability_conformance;
mod cold_stat_requests;
mod commit_retry;
mod common;
mod content_request_accounting;
mod direct_put;
mod handles;
mod immutable_view_inputs;
mod inode_reads;
mod invalidation;
mod maintenance;
mod metrics_instruments;
mod namespace_advance_observer;
mod pagination;
mod publication;
mod request_accounting;
mod runtime_config;
mod staged_content_reclamation;
mod streamed_put;
mod streamed_read;
mod undelete;
