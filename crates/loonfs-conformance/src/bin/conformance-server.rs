use loonfs_conformance::server::start_server;
use serde::Serialize;
use std::io::{Read, Write};

#[derive(Serialize)]
struct ServerInfo<'a> {
    base_url: &'a str,
    token: &'a str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = start_server().await?;
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
