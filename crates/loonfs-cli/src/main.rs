//! The `loonfs` CLI binary: parse arguments, run the command, render the
//! result.

// The embedded runtime's composed async graph outgrew rustc's default
// future-layout query depth while building this binary. This is the raise
// rustc itself prescribes.
#![recursion_limit = "256"]

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    loonfs_cli::main().await
}
