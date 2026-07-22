//! LoonFS full-text grep subsystem.
//!
//! Grep durable state lives under a grep-owned keyspace and is independent
//! of the namespace manifest. A missing or corrupt grep root disables grep
//! work for that namespace; it must never affect core filesystem operation.
//! Query execution, storage maintenance, and the standalone worker arrive
//! in later changes.

pub mod codec;
pub mod keyspace;
pub mod root;

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
