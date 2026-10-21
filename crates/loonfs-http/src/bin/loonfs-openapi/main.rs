//! Writes the OpenAPI document.

use std::path::Path;

mod openapi_postprocess;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args_os().skip(1);
    let (Some(path), None) = (arguments.next(), arguments.next()) else {
        return Err("usage: loonfs-openapi <openapi.json>".into());
    };
    let json = openapi_postprocess::openapi_json_pretty(&loonfs_http::openapi_document())?;
    write_json(Path::new(&path), json)?;
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
