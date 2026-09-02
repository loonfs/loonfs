//! Writes the full OpenAPI document and the browser proxy document.

use std::path::Path;

mod openapi_postprocess;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let (Some(full_path), Some(proxy_path), None) =
        (arguments.next(), arguments.next(), arguments.next())
    else {
        return Err("usage: loonfs-openapi <openapi.json> <openapi-proxy.json>".into());
    };
    let (full, proxy) =
        openapi_postprocess::openapi_documents_pretty(&loonfs_server::openapi_document())?;
    write_json(Path::new(&full_path), full)?;
    write_json(Path::new(&proxy_path), proxy)?;
    Ok(())
}

fn write_json(path: &Path, mut json: String) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    json.push('\n');
    std::fs::write(path, json)?;
    Ok(())
}
