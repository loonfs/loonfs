//! Writes the generated OpenAPI document to `docs/specs/openapi.json`.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("docs/specs/openapi.json"));

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let mut json = loonfs_server::openapi_json_pretty()?;
    json.push('\n');
    std::fs::write(path, json)?;
    Ok(())
}
