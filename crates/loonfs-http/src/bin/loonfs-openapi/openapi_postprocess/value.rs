//! Shared JSON document traversal for OpenAPI post-processing passes.

use super::OpenapiPostprocessError;
use serde_json::{Map, Value};

pub(super) const HTTP_METHODS: &[&str] = &["get", "post", "put", "delete", "patch"];

type OperationMut<'a> = (String, &'a mut Map<String, Value>);

pub(super) fn invalid_document(location: impl Into<String>) -> OpenapiPostprocessError {
    OpenapiPostprocessError::InvalidDocument {
        location: location.into(),
    }
}

pub(super) fn operations_mut(
    document: &mut Value,
) -> Result<Vec<OperationMut<'_>>, OpenapiPostprocessError> {
    let paths = document
        .get_mut("paths")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid_document("paths"))?;
    let mut operations = Vec::new();

    for (path, path_item) in paths {
        let path_item = path_item
            .as_object_mut()
            .ok_or_else(|| invalid_document(format!("paths.{path}")))?;
        for (method, operation) in path_item {
            if !HTTP_METHODS.contains(&method.as_str()) {
                continue;
            }
            let operation = operation
                .as_object_mut()
                .ok_or_else(|| invalid_document(format!("paths.{path}.{method}")))?;
            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_document(format!("paths.{path}.{method}.operationId")))?
                .to_owned();
            operations.push((operation_id, operation));
        }
    }

    Ok(operations)
}

pub(super) fn component_schemas(
    document: &Value,
) -> Result<&Map<String, Value>, OpenapiPostprocessError> {
    document
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .ok_or_else(|| invalid_document("components.schemas"))
}

pub(super) fn decode_json_pointer_segment(segment: &str) -> String {
    segment.replace("~1", "/").replace("~0", "~")
}

pub(super) fn component_schema_for_reference<'a>(
    schemas: &'a Map<String, Value>,
    reference: &str,
) -> Option<&'a Value> {
    schemas.get(&component_schema_name(reference)?)
}

pub(super) fn component_schema_name(reference: &str) -> Option<String> {
    reference
        .strip_prefix("#/components/schemas/")
        .map(decode_json_pointer_segment)
}
