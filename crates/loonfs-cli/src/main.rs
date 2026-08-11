//! The `loonfs` CLI binary: parse arguments, run the command, render the
//! result.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    loonfs_cli::main().await
}
