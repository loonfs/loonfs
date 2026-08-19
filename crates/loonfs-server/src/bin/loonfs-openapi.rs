//! Writes the full OpenAPI document and the browser proxy document.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[path = "loonfs-openapi/openapi_postprocess.rs"]
mod openapi_postprocess;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (full_path, proxy_path) = output_paths(std::env::args_os().skip(1))?;
    let (full, proxy) =
        openapi_postprocess::openapi_documents_pretty(&loonfs_server::openapi_document())?;
    write_json(&full_path, full)?;
    write_json(&proxy_path, proxy)?;
    Ok(())
}

fn output_paths(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<(PathBuf, PathBuf), std::io::Error> {
    let mut full_path = None;
    let mut proxy_path = None;
    let mut arguments = arguments.into_iter();

    while let Some(argument) = arguments.next() {
        if argument == "--proxy-output" {
            let value = arguments
                .next()
                .ok_or_else(|| invalid_input("--proxy-output requires an output path"))?;
            if proxy_path.replace(PathBuf::from(value)).is_some() {
                return Err(invalid_input("--proxy-output may be set only once"));
            }
        } else if argument.to_string_lossy().starts_with('-') {
            return Err(invalid_input(format!(
                "unknown argument `{}`",
                argument.to_string_lossy()
            )));
        } else if full_path.replace(PathBuf::from(argument)).is_some() {
            return Err(invalid_input(
                "only one full-document output path is accepted",
            ));
        }
    }

    let full_path = full_path.unwrap_or_else(|| PathBuf::from("docs/specs/openapi.json"));
    let proxy_path = proxy_path.unwrap_or_else(|| default_proxy_path(&full_path));
    if full_path == proxy_path {
        return Err(invalid_input(
            "the full and proxy documents require different output paths",
        ));
    }
    Ok((full_path, proxy_path))
}

fn default_proxy_path(full_path: &Path) -> PathBuf {
    full_path.with_file_name("openapi-proxy.json")
}

fn invalid_input(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into())
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
