//! OpenAPI document rewrites that utoipa cannot express at the handler or schema.

pub(crate) mod operations;
pub(crate) mod proxy;
mod value;

use operations::{add_sdk_names, validate_operation_retry_classes, validate_pagination_metadata};
use proxy::{
    derive_proxy_paths, describe_proxy_document, prune_proxy_components, remove_proxy_security,
    retain_referenced_tags,
};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenapiPostprocessError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("openapi operation `{operation_id}` has no retry classification")]
    MissingRetryClassification { operation_id: String },
    #[error("openapi operation `{operation_id}` has no SDK name")]
    MissingSdkName { operation_id: String },
    #[error("openapi SDK-named operation `{operation_id}` does not appear in the document")]
    MissingSdkNameOperation { operation_id: String },
    #[error("openapi pagination operation `{operation_id}` has no `cursor` query parameter")]
    MissingPaginationCursorParameter { operation_id: String },
    #[error("openapi pagination operation `{operation_id}` has no 200 response component schema")]
    MissingPaginationResponseSchema { operation_id: String },
    #[error("openapi pagination operation `{operation_id}` response has no `{property}` property")]
    MissingPaginationResponseProperty {
        operation_id: String,
        property: String,
    },
    #[error(
        "openapi pagination operation `{operation_id}` response: expected exactly one array property"
    )]
    InvalidPaginationArrayProperties { operation_id: String },
    #[error(
        "openapi operation `{operation_id}` has a `cursor` query parameter but no pagination metadata entry"
    )]
    MissingPaginationMetadata { operation_id: String },
    #[error("invalid openapi document at `{location}`")]
    InvalidDocument { location: String },
    #[error("proxy operation `{operation_id}` does not appear in the full OpenAPI document")]
    MissingProxyOperation { operation_id: String },
    #[error(
        "proxy operation `{operation_id}` appears more than once in the full OpenAPI document"
    )]
    DuplicateProxyOperation { operation_id: String },
    #[error("proxy operation `{operation_id}` has no `namespace_id` path parameter")]
    MissingProxyNamespaceParameter { operation_id: String },
    #[error("proxy document references a missing OpenAPI component `{reference}`")]
    MissingProxyComponent { reference: String },
}

/// Generates OpenAPI JSON and applies the operation metadata.
pub(crate) fn openapi_json_pretty(
    document: &(impl Serialize + ?Sized),
) -> Result<String, OpenapiPostprocessError> {
    let mut document = serde_json::to_value(document)?;
    document["x-fern-global-headers"] = serde_json::json!([
        {"header": "Loonfs-Actor", "name": "actorId", "optional": true},
        {"header": "Loonfs-Subject", "name": "subjectId", "optional": true},
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
