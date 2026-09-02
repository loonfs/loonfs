//! Browser proxy document derivation and component pruning.

use super::value::{
    collect_component_references, component_reference_parts, invalid_document, operations,
    HTTP_METHODS,
};
use super::OpenapiPostprocessError;
use serde_json::{Map, Value};
use std::collections::BTreeSet;

const NAMESPACE_PATH_PREFIX: &str = "/v0/namespaces/{namespace_id}";
const NAMESPACE_ALIAS_PATH_PREFIX: &str = "/v0/namespace-aliases/{namespace_alias}";

/// Operations included in the browser proxy document. Keep this table sorted.
pub(crate) const PROXY_OPERATIONS: &[&str] = &[
    "abort_upload",
    "complete_upload",
    "create_commit",
    "create_download",
    "create_snapshot",
    "create_upload",
    "extend_snapshot",
    "get_capabilities",
    "get_file_bytes",
    "get_path_entry",
    "get_upload",
    "grep",
    "list_changes",
    "list_file_revisions",
    "list_path_entries",
    "list_snapshots",
    "list_trash",
    "put_upload_content",
    "release_snapshot",
    "sign_upload_parts",
];

pub(super) fn describe_proxy_document(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let info = document
        .as_object_mut()
        .and_then(|document| document.get_mut("info"))
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid_document("info"))?;
    info.insert(
        "title".to_owned(),
        Value::String("LoonFS Browser Proxy API".to_owned()),
    );
    info.insert(
        "description".to_owned(),
        Value::String("API for browser clients that access namespaces by alias.".to_owned()),
    );
    Ok(())
}

pub(super) fn derive_proxy_paths(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let paths = document
        .as_object_mut()
        .and_then(|document| document.get_mut("paths"))
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid_document("paths"))?;
    let mut derived_paths = Map::new();
    let mut found_operations = BTreeSet::new();

    for (path, path_item) in paths.iter() {
        let path_item = path_item
            .as_object()
            .ok_or_else(|| invalid_document(format!("paths.{path}")))?;
        let mut derived_path_item = Map::new();
        let derived_path = match path.strip_prefix(NAMESPACE_PATH_PREFIX) {
            Some(suffix) => format!("{NAMESPACE_ALIAS_PATH_PREFIX}{suffix}"),
            None => path.clone(),
        };
        let mut retained_operation = false;

        for (field, value) in path_item {
            if !HTTP_METHODS.contains(&field.as_str()) {
                derived_path_item.insert(field.clone(), value.clone());
                continue;
            }

            let operation = value
                .as_object()
                .ok_or_else(|| invalid_document(format!("paths.{path}.{field}")))?;
            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_document(format!("paths.{path}.{field}.operationId")))?;
            if !PROXY_OPERATIONS.contains(&operation_id) {
                continue;
            }
            if !found_operations.insert(operation_id.to_owned()) {
                return Err(OpenapiPostprocessError::DuplicateProxyOperation {
                    operation_id: operation_id.to_owned(),
                });
            }
            retained_operation = true;

            let mut operation = value.clone();
            let operation_object = operation
                .as_object_mut()
                .ok_or_else(|| invalid_document(format!("paths.{path}.{field}")))?;
            operation_object.remove("security");
            if path.starts_with(NAMESPACE_PATH_PREFIX)
                && rewrite_namespace_parameters(operation_object, operation_id)? == 0
            {
                return Err(OpenapiPostprocessError::MissingProxyNamespaceParameter {
                    operation_id: operation_id.to_owned(),
                });
            }
            derived_path_item.insert(field.clone(), operation);
        }

        if retained_operation {
            derived_paths.insert(derived_path, Value::Object(derived_path_item));
        }
    }

    for &operation_id in PROXY_OPERATIONS {
        if !found_operations.contains(operation_id) {
            return Err(OpenapiPostprocessError::MissingProxyOperation {
                operation_id: operation_id.to_owned(),
            });
        }
    }
    *paths = derived_paths;
    Ok(())
}

fn rewrite_namespace_parameters(
    operation: &mut Map<String, Value>,
    operation_id: &str,
) -> Result<usize, OpenapiPostprocessError> {
    let Some(parameters) = operation.get_mut("parameters") else {
        return Ok(0);
    };
    let parameters = parameters
        .as_array_mut()
        .ok_or_else(|| invalid_document(format!("operations.{operation_id}.parameters")))?;
    let mut rewritten = 0;

    for (index, parameter) in parameters.iter_mut().enumerate() {
        let parameter = parameter.as_object_mut().ok_or_else(|| {
            invalid_document(format!("operations.{operation_id}.parameters.{index}"))
        })?;
        if parameter.get("in").and_then(Value::as_str) != Some("path")
            || parameter.get("name").and_then(Value::as_str) != Some("namespace_id")
        {
            continue;
        }
        parameter.insert(
            "name".to_owned(),
            Value::String("namespace_alias".to_owned()),
        );
        parameter.insert(
            "description".to_owned(),
            Value::String("Application namespace alias".to_owned()),
        );
        parameter.insert("schema".to_owned(), serde_json::json!({"type": "string"}));
        rewritten += 1;
    }
    Ok(rewritten)
}

pub(super) fn remove_proxy_security(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let document = document
        .as_object_mut()
        .ok_or_else(|| invalid_document("$"))?;
    document.remove("security");
    if let Some(components) = document.get_mut("components") {
        components
            .as_object_mut()
            .ok_or_else(|| invalid_document("components"))?
            .remove("securitySchemes");
    }
    Ok(())
}

/// Retains only tag definitions that a retained operation references, so a
/// tag whose operations were all pruned does not survive as a dead entry.
pub(super) fn retain_referenced_tags(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let mut referenced = BTreeSet::new();
    for (operation_id, operation) in operations(document)? {
        let Some(tags) = operation.get("tags") else {
            continue;
        };
        let tags = tags
            .as_array()
            .ok_or_else(|| invalid_document(format!("operations.{operation_id}.tags")))?;
        for (index, tag) in tags.iter().enumerate() {
            let tag = tag.as_str().ok_or_else(|| {
                invalid_document(format!("operations.{operation_id}.tags.{index}"))
            })?;
            referenced.insert(tag.to_owned());
        }
    }
    let Some(tags) = document.get_mut("tags") else {
        return Ok(());
    };
    let tags = tags
        .as_array_mut()
        .ok_or_else(|| invalid_document("tags"))?;
    for (index, tag) in tags.iter().enumerate() {
        if tag.get("name").and_then(Value::as_str).is_none() {
            return Err(invalid_document(format!("tags.{index}.name")));
        }
    }
    tags.retain(|tag| {
        tag.get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| referenced.contains(name))
    });
    Ok(())
}

pub(super) fn prune_proxy_components(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let paths = document
        .get("paths")
        .ok_or_else(|| invalid_document("paths"))?;
    let mut pending = Vec::new();
    collect_component_references(paths, &mut pending);
    let mut referenced = BTreeSet::new();

    while let Some(reference) = pending.pop() {
        let Some((component_type, component_name)) = component_reference_parts(&reference) else {
            continue;
        };
        if !referenced.insert((component_type.clone(), component_name.clone())) {
            continue;
        }
        let component = document
            .get("components")
            .and_then(|components| components.get(&component_type))
            .and_then(Value::as_object)
            .and_then(|components| components.get(&component_name))
            .ok_or_else(|| OpenapiPostprocessError::MissingProxyComponent {
                reference: reference.clone(),
            })?;
        collect_component_references(component, &mut pending);
    }

    let components = document
        .as_object_mut()
        .and_then(|document| document.get_mut("components"))
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid_document("components"))?;
    for (component_type, entries) in components.iter_mut() {
        let entries = entries
            .as_object_mut()
            .ok_or_else(|| invalid_document(format!("components.{component_type}")))?;
        entries.retain(|component_name, _| {
            referenced.contains(&(component_type.clone(), component_name.clone()))
        });
    }
    components.retain(|_, entries| {
        entries
            .as_object()
            .is_some_and(|entries| !entries.is_empty())
    });
    Ok(())
}
