//! Nullable schema normalization for requests and responses.

use super::value::{
    collect_component_references, component_reference_parts, invalid_document, operations,
    operations_mut,
};
use super::OpenapiPostprocessError;
use serde_json::Value;
use std::collections::BTreeSet;

pub(super) fn normalize_optional_schemas(value: &mut Value) -> Result<(), OpenapiPostprocessError> {
    match value {
        Value::Object(object) => {
            let required = object
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<BTreeSet<_>>();
            if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
                for (name, schema) in properties {
                    if let Some(replacement) = nullable_alternative(schema).cloned() {
                        if required.contains(name.as_str()) {
                            return Err(invalid_document(format!(
                                "required property {name} has an optional schema"
                            )));
                        }
                        *schema = replacement;
                    }
                }
            }

            if let Some(request_body) = object.get_mut("requestBody").and_then(Value::as_object_mut)
            {
                let required = request_body.get("required") == Some(&Value::Bool(true));
                if let Some(content) = request_body
                    .get_mut("content")
                    .and_then(Value::as_object_mut)
                {
                    for media_type in content.values_mut() {
                        let Some(schema) = media_type
                            .as_object_mut()
                            .and_then(|media_type| media_type.get_mut("schema"))
                        else {
                            continue;
                        };
                        if let Some(replacement) = nullable_alternative(schema).cloned() {
                            if required {
                                return Err(invalid_document(
                                    "required request body has an optional schema",
                                ));
                            }
                            *schema = replacement;
                        }
                    }
                }
            }

            for child in object.values_mut() {
                normalize_optional_schemas(child)?;
            }
        }
        Value::Array(values) => {
            for child in values {
                normalize_optional_schemas(child)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn nullable_alternative(value: &Value) -> Option<&Value> {
    let object = value.as_object()?;
    if object.len() != 1 {
        return None;
    }
    let alternatives = object.get("oneOf")?.as_array()?;
    if alternatives.len() != 2 {
        return None;
    }

    match (
        is_null_schema(&alternatives[0]),
        is_null_schema(&alternatives[1]),
    ) {
        (true, false) => Some(&alternatives[1]),
        (false, true) => Some(&alternatives[0]),
        (true, true) | (false, false) => None,
    }
}

fn is_null_schema(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.len() == 1 && object.get("type").and_then(Value::as_str) == Some("null")
}

/// Removes `null` from optional fields used in responses.
///
/// The server omits absent response fields. Request-only schemas keep `null`
/// because serde accepts it. Shared request and response schemas use the
/// response rule while keeping the field optional.
pub(super) fn drop_null_from_response_schemas(
    document: &mut Value,
) -> Result<(), OpenapiPostprocessError> {
    let mut pending = Vec::new();
    collect_response_references(document, &mut pending)?;
    let mut reachable = BTreeSet::new();

    while let Some(reference) = pending.pop() {
        let Some((component_type, component_name)) = component_reference_parts(&reference) else {
            continue;
        };
        if !reachable.insert((component_type.clone(), component_name.clone())) {
            continue;
        }
        let component = document
            .get("components")
            .and_then(|components| components.get(&component_type))
            .and_then(Value::as_object)
            .and_then(|entries| entries.get(&component_name))
            .ok_or_else(|| {
                invalid_document(format!("components.{component_type}.{component_name}"))
            })?;
        collect_component_references(component, &mut pending);
    }

    for responses in operation_responses_mut(document)? {
        drop_null_from_optional_properties(responses);
    }

    let components = document
        .as_object_mut()
        .and_then(|document| document.get_mut("components"))
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid_document("components"))?;
    for (component_type, entries) in components {
        let entries = entries
            .as_object_mut()
            .ok_or_else(|| invalid_document(format!("components.{component_type}")))?;
        for (component_name, component) in entries {
            if reachable.contains(&(component_type.clone(), component_name.clone())) {
                drop_null_from_optional_properties(component);
            }
        }
    }
    Ok(())
}

/// Collects component references from every operation response.
fn collect_response_references(
    document: &Value,
    references: &mut Vec<String>,
) -> Result<(), OpenapiPostprocessError> {
    for (operation_id, operation) in operations(document)? {
        let responses = operation
            .get("responses")
            .ok_or_else(|| invalid_document(format!("operations.{operation_id}.responses")))?;
        collect_component_references(responses, references);
    }
    Ok(())
}

/// Returns every operation's `responses` object for in-place updates.
fn operation_responses_mut(
    document: &mut Value,
) -> Result<Vec<&mut Value>, OpenapiPostprocessError> {
    operations_mut(document)?
        .into_iter()
        .map(|(operation_id, operation)| {
            operation
                .get_mut("responses")
                .ok_or_else(|| invalid_document(format!("operations.{operation_id}.responses")))
        })
        .collect()
}

fn drop_null_from_optional_properties(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let required = object
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<BTreeSet<_>>();
            if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
                for (name, schema) in properties {
                    if required.contains(name.as_str()) {
                        continue;
                    }
                    let Some(replacement) = non_null_type(schema).cloned() else {
                        continue;
                    };
                    let schema = schema
                        .as_object_mut()
                        .expect("schema with a type array should be an object");
                    schema.insert("type".to_owned(), replacement);
                    schema.remove("nullable");
                }
            }

            for child in object.values_mut() {
                drop_null_from_optional_properties(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                drop_null_from_optional_properties(child);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

/// Returns `X` when the schema type is `["X", "null"]`.
fn non_null_type(value: &Value) -> Option<&Value> {
    let types = value.get("type")?.as_array()?;
    if types.len() != 2 {
        return None;
    }

    match (
        types[0].as_str() == Some("null"),
        types[1].as_str() == Some("null"),
    ) {
        (true, false) => Some(&types[1]),
        (false, true) => Some(&types[0]),
        (true, true) | (false, false) => None,
    }
}
