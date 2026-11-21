//! Operation retry and pagination metadata rewrites.

use super::value::{component_schema_for_reference, component_schemas, operations_mut};
use super::OpenapiPostprocessError;
use serde_json::{Map, Value};

/// Checks that every operation has a retry class, and disables the
/// generated SDK retries of every `not_idempotent` operation.
pub(crate) fn add_retry_metadata(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    for (operation_id, operation) in operations_mut(document)? {
        let Some(retry_class) = operation.get("x-loonfs-retry").and_then(Value::as_str) else {
            return Err(OpenapiPostprocessError::MissingRetryClassification { operation_id });
        };
        if retry_class == "not_idempotent" {
            operation.insert(
                "x-fern-retries".to_owned(),
                serde_json::json!({"disabled": true}),
            );
        }
    }
    Ok(())
}

/// Adds the Fern pagination extension to every operation with a `cursor`
/// query parameter, naming the one array property of its response.
pub(crate) fn add_pagination_metadata(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let schemas = component_schemas(document)?.clone();

    for (operation_id, operation) in operations_mut(document)? {
        if !has_query_parameter(operation, "cursor") {
            continue;
        }
        let results_property = pagination_results_property(&schemas, operation, &operation_id)?;
        operation.insert(
            "x-fern-pagination".to_owned(),
            serde_json::json!({
                "cursor": "$request.cursor",
                "next_cursor": "$response.next_cursor",
                "results": format!("$response.{results_property}"),
            }),
        );
    }

    Ok(())
}

/// Unwraps `utoipa`'s `Option<T>` request-body schema (`oneOf: [null, T]`) into
/// `T` with `required: false`: an omitted HTTP body is accepted, while JSON
/// `null` is not.
pub(crate) fn normalize_optional_request_bodies(
    document: &mut Value,
) -> Result<(), OpenapiPostprocessError> {
    for (_, operation) in operations_mut(document)? {
        let Some(request_body) = operation
            .get_mut("requestBody")
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        let is_optional = request_body
            .get("required")
            .and_then(Value::as_bool)
            .is_none_or(|required| !required);
        if !is_optional {
            continue;
        }
        request_body.insert("required".to_owned(), Value::Bool(false));
        let Some(content) = request_body
            .get_mut("content")
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        for media_type in content.values_mut() {
            let Some(schema) = media_type.get_mut("schema") else {
                continue;
            };
            if let Some(inner) = unwrap_nullable_one_of(schema) {
                *schema = inner;
            }
        }
    }
    Ok(())
}

fn unwrap_nullable_one_of(schema: &Value) -> Option<Value> {
    let variants = schema.get("oneOf")?.as_array()?;
    if variants.len() != 2 {
        return None;
    }
    let is_null = |v: &Value| v.get("type").and_then(Value::as_str) == Some("null");
    if is_null(&variants[0]) && !is_null(&variants[1]) {
        Some(variants[1].clone())
    } else if is_null(&variants[1]) && !is_null(&variants[0]) {
        Some(variants[0].clone())
    } else {
        None
    }
}

/// Declares the `x-request-id` header in `components.headers.RequestId` and
/// attaches a `$ref` to it on every non-reference response object.
pub(crate) fn add_request_id_response_headers(
    document: &mut Value,
) -> Result<(), OpenapiPostprocessError> {
    const HEADER_NAME: &str = "x-request-id";
    const HEADER_REF: &str = "#/components/headers/RequestId";

    let components = document
        .as_object_mut()
        .and_then(|root| root.get_mut("components"))
        .and_then(Value::as_object_mut);
    if let Some(components) = components {
        let headers = components
            .entry("headers".to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(headers) = headers.as_object_mut() {
            headers.insert(
                "RequestId".to_owned(),
                serde_json::json!({
                    "description": "Unique request identifier echoed in error envelopes and server logs",
                    "schema": { "type": "string" }
                }),
            );
        }
        if let Some(responses) = components
            .get_mut("responses")
            .and_then(Value::as_object_mut)
        {
            for response in responses.values_mut() {
                attach_header_ref(response, HEADER_NAME, HEADER_REF);
            }
        }
    }

    for (_, operation) in operations_mut(document)? {
        if let Some(responses) = operation
            .get_mut("responses")
            .and_then(Value::as_object_mut)
        {
            for response in responses.values_mut() {
                attach_header_ref(response, HEADER_NAME, HEADER_REF);
            }
        }
    }
    Ok(())
}

fn attach_header_ref(response: &mut Value, name: &str, reference: &str) {
    let Some(response) = response.as_object_mut() else {
        return;
    };
    if response.contains_key("$ref") {
        return;
    }
    let headers = response
        .entry("headers".to_owned())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Some(headers) = headers.as_object_mut() {
        headers
            .entry(name.to_owned())
            .or_insert_with(|| serde_json::json!({ "$ref": reference }));
    }
}

fn has_query_parameter(operation: &Map<String, Value>, parameter_name: &str) -> bool {
    operation
        .get("parameters")
        .and_then(Value::as_array)
        .is_some_and(|parameters| {
            parameters.iter().any(|parameter| {
                parameter.get("in").and_then(Value::as_str) == Some("query")
                    && parameter.get("name").and_then(Value::as_str) == Some(parameter_name)
            })
        })
}

fn pagination_results_property(
    schemas: &Map<String, Value>,
    operation: &Map<String, Value>,
    operation_id: &str,
) -> Result<String, OpenapiPostprocessError> {
    let response_schema = operation
        .get("responses")
        .and_then(|responses| responses.get("200"))
        .and_then(|response| response.get("content"))
        .and_then(|content| content.get("application/json"))
        .and_then(|content| content.get("schema"))
        .and_then(|schema| schema.get("$ref"))
        .and_then(Value::as_str)
        .and_then(|reference| component_schema_for_reference(schemas, reference))
        .and_then(Value::as_object)
        .ok_or_else(
            || OpenapiPostprocessError::MissingPaginationResponseSchema {
                operation_id: operation_id.to_owned(),
            },
        )?;
    let properties = response_schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(
            || OpenapiPostprocessError::MissingPaginationResponseSchema {
                operation_id: operation_id.to_owned(),
            },
        )?;

    if !properties.contains_key("next_cursor") {
        return Err(OpenapiPostprocessError::MissingPaginationResponseProperty {
            operation_id: operation_id.to_owned(),
            property: "next_cursor".to_owned(),
        });
    }

    let mut array_properties = properties.iter().filter_map(|(name, property)| {
        (property.get("type").and_then(Value::as_str) == Some("array")).then_some(name)
    });
    let Some(results_property) = array_properties.next() else {
        return Err(OpenapiPostprocessError::InvalidPaginationArrayProperties {
            operation_id: operation_id.to_owned(),
        });
    };
    if array_properties.next().is_some() {
        return Err(OpenapiPostprocessError::InvalidPaginationArrayProperties {
            operation_id: operation_id.to_owned(),
        });
    }

    Ok(results_property.to_owned())
}
