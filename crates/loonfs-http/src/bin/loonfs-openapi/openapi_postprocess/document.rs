//! Builds the full and browser proxy OpenAPI documents from the generated one.

use super::operations::{
    add_sdk_names, validate_operation_retry_classes, validate_pagination_metadata,
};
use super::proxy::{
    derive_proxy_paths, describe_proxy_document, prune_proxy_components, remove_proxy_security,
    retain_referenced_tags,
};
use super::OpenapiPostprocessError;
use serde::Serialize;

/// Generates OpenAPI JSON and applies the operation metadata.
pub(crate) fn openapi_json_pretty(
    document: &(impl Serialize + ?Sized),
) -> Result<String, OpenapiPostprocessError> {
    let mut document = serde_json::to_value(document)?;
    document["x-fern-global-headers"] = serde_json::json!([
        {"header": "Loonfs-Actor", "name": "actorId", "optional": true},
        {"header": "Loonfs-Subject", "name": "subjectId", "optional": true},
        {"header": "Loonfs-Principal-Scope", "name": "principalScope", "optional": true},
        {"header": "Loonfs-Principals", "name": "principals", "optional": true}
    ]);
    validate_operation_retry_classes(&document)?;
    add_sdk_names(&mut document)?;
    validate_pagination_metadata(&document)?;
    Ok(serde_json::to_string_pretty(&document)?)
}

/// Generates the full OpenAPI document and the browser proxy document.
pub(crate) fn openapi_documents_pretty(
    document: &(impl Serialize + ?Sized),
) -> Result<(String, String), OpenapiPostprocessError> {
    let full = openapi_json_pretty(document)?;
    let proxy = proxy_openapi_json_pretty(&full)?;
    Ok((full, proxy))
}

/// Builds the browser proxy document from the full OpenAPI JSON.
pub(crate) fn proxy_openapi_json_pretty(
    full_document: &str,
) -> Result<String, OpenapiPostprocessError> {
    let mut document = serde_json::from_str(full_document)?;
    describe_proxy_document(&mut document)?;
    derive_proxy_paths(&mut document)?;
    remove_proxy_security(&mut document)?;
    retain_referenced_tags(&mut document)?;
    prune_proxy_components(&mut document)?;
    Ok(serde_json::to_string_pretty(&document)?)
}
