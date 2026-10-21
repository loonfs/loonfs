//! Builds the published OpenAPI document from the generated one.

use super::operations::{
    add_sdk_names, validate_operation_retry_classes, validate_pagination_metadata,
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
