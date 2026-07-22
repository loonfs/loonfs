//! LoonFS full-text grep subsystem.
//!
//! Grep durable state lives under a grep-owned keyspace and is independent
//! of the namespace manifest. A missing or corrupt grep root disables grep
//! work for that namespace; it must never affect core filesystem operation.
//! Query execution reads a caller-provided core snapshot; storage maintenance
//! and the standalone worker arrive in later changes.

mod cache;
pub mod codec;
mod index_read;
pub mod keyspace;
mod query;
pub mod root;
mod service;

pub use service::{
    is_indexable_text_content, GrepIndexSnapshot, GrepService, DEFAULT_GREP_PAGE_LIMIT,
    MAX_GREP_PAGE_LIMIT, MAX_GREP_SCAN_FILES, MAX_GREP_TAIL_FILES,
};

use std::io::Write as _;
use std::process::ExitCode;

/// Entry point for the standalone `loonfs-grep` binary.
pub fn main() -> ExitCode {
    let _ = writeln!(
        std::io::stderr().lock(),
        "usage: loonfs-grep (the standalone worker arrives in a later change)"
    );
    ExitCode::FAILURE
}
