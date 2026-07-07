use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    loonfs_cli::main().await
}
