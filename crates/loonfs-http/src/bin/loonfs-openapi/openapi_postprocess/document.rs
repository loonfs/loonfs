//! Builds the published OpenAPI document from the generated one.

use super::operations::{
    add_pagination_metadata, add_request_id_response_headers, add_retry_metadata,
    normalize_optional_request_bodies,
};
use super::OpenapiPostprocessError;
use loonfs_types::headers;
use serde::Serialize;

/// Generates OpenAPI JSON and applies the operation metadata.
pub(crate) fn openapi_json_pretty(
    document: &(impl Serialize + ?Sized),
) -> Result<String, OpenapiPostprocessError> {
    let mut document = serde_json::to_value(document)?;
    document["x-fern-global-headers"] = serde_json::json!([
        {"header": headers::ACTOR, "name": "actorId", "optional": true},
        {"header": headers::PRINCIPALS, "name": "principals", "optional": true},
        {"header": headers::CONTEXT, "name": "loonfsContext", "optional": true}
    ]);
    add_retry_metadata(&mut document)?;
    add_pagination_metadata(&mut document)?;
    normalize_optional_request_bodies(&mut document)?;
    add_request_id_response_headers(&mut document)?;
    Ok(serde_json::to_string_pretty(&document)?)
}
