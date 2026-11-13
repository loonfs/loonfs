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
