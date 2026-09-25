//! Shared JSON document traversal for OpenAPI post-processing passes.

use super::OpenapiPostprocessError;
use serde_json::{Map, Value};

pub(super) const HTTP_METHODS: &[&str] = &["get", "post", "put", "delete", "patch"];

type Operation<'a> = (&'a str, &'a Map<String, Value>);
type OperationMut<'a> = (String, &'a mut Map<String, Value>);

pub(super) fn invalid_document(location: impl Into<String>) -> OpenapiPostprocessError {
    OpenapiPostprocessError::InvalidDocument {
        location: location.into(),
    }
}

pub(super) fn operations(document: &Value) -> Result<Vec<Operation<'_>>, OpenapiPostprocessError> {
    let paths = document
        .get("paths")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid_document("paths"))?;
    let mut operations = Vec::new();

    for (path, path_item) in paths {
        let path_item = path_item
            .as_object()
            .ok_or_else(|| invalid_document(format!("paths.{path}")))?;
        for &method in HTTP_METHODS {
            let Some(operation) = path_item.get(method) else {
                continue;
            };
            let operation = operation
                .as_object()
                .ok_or_else(|| invalid_document(format!("paths.{path}.{method}")))?;
            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_document(format!("paths.{path}.{method}.operationId")))?;
            operations.push((operation_id, operation));
        }
    }

    Ok(operations)
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

pub(super) fn collect_component_references(value: &Value, references: &mut Vec<String>) {
    match value {
        Value::String(value) => {
            if value.starts_with("#/components/") {
                references.push(value.clone());
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_component_references(value, references);
            }
        }
        Value::Object(object) => {
            for value in object.values() {
                collect_component_references(value, references);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

pub(super) fn component_reference_parts(reference: &str) -> Option<(String, String)> {
    let mut parts = reference.strip_prefix("#/components/")?.split('/');
    let component_type = decode_json_pointer_segment(parts.next()?);
    let component_name = decode_json_pointer_segment(parts.next()?);
    Some((component_type, component_name))
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
