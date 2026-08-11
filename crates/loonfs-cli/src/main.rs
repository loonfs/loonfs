//! The `loonfs` CLI binary: parse arguments, run the command, render the
//! result.

// The embedded runtime's composed async call graph is deep enough that
// rustc's future-layout computation exceeds its default query depth while
// building this binary. Raising the limit is the fix rustc prescribes.
#![recursion_limit = "256"]

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    loonfs_cli::main().await
}
