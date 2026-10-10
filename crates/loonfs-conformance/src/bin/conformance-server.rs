//! Runs the reference server until standard input closes.

use loonfs_conformance::server::{start_server, StoreShape};
use serde::Serialize;
use std::io::{Read, Write};

#[derive(Serialize)]
struct ServerInfo<'a> {
    base_url: &'a str,
    token: &'a str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let shape = match std::env::var("LOONFS_CONFORMANCE_SHAPE") {
        Ok(value) => match value.as_str() {
            "s3" => StoreShape::S3,
            "gcs" => StoreShape::Gcs,
            _ => {
                return Err(format!(
                    "invalid `LOONFS_CONFORMANCE_SHAPE` {value:?}: expected `s3` or `gcs`"
                )
                .into())
            }
        },
        Err(std::env::VarError::NotPresent) => StoreShape::default(),
        Err(error) => return Err(error.into()),
    };
    let server = start_server(shape).await?;
    let info = ServerInfo {
        base_url: &server.base_url,
        token: server.token,
    };
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &info)?;
    writeln!(stdout)?;
    stdout.flush()?;

    std::io::stdin().read_to_end(&mut Vec::new())?;
    Ok(())
}
