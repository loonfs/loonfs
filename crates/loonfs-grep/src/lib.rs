//! LoonFS full-text grep subsystem.
//!
//! This first extraction step owns a byte-compatible copy of the gram
//! codec. Query execution, storage maintenance, and the standalone worker
//! arrive in later changes.

pub mod codec;

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
