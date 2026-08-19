//! Writes the generated OpenAPI document to `docs/specs/openapi.json`.

use std::path::PathBuf;

#[path = "loonfs-openapi/openapi_postprocess.rs"]
mod openapi_postprocess;

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

    let mut json = openapi_postprocess::openapi_json_pretty(&loonfs_server::openapi_document())?;
    json.push('\n');
    std::fs::write(path, json)?;
    Ok(())
}
