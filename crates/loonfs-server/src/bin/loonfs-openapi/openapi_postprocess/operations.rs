//! Operation metadata validation and SDK naming rewrites.

use super::value::{component_schema_for_reference, component_schemas, operations, operations_mut};
use super::OpenapiPostprocessError;
use serde_json::{Map, Value};
use std::collections::BTreeSet;

pub(crate) const OPERATION_SDK_NAMES: &[(&str, SdkName)] = &[
    (
        "abort_upload",
        SdkName {
            group: &["uploads"],
            method: "abort",
            request: Some("AbortUploadRequest"),
        },
    ),
    (
        "complete_upload",
        SdkName {
            group: &["uploads"],
            method: "complete",
            request: Some("CompleteUploadRequest"),
        },
    ),
    (
        "create_checkpoint",
        SdkName {
            group: &["maintenance", "checkpoints"],
            method: "create",
            request: None,
        },
    ),
    (
        "create_commit",
        SdkName {
            group: &["commits"],
            method: "create",
            request: None,
        },
    ),
    (
        "create_download",
        SdkName {
            group: &["files"],
            method: "createDownload",
            request: None,
        },
    ),
    (
        "create_download_by_inode",
        SdkName {
            group: &["inodes"],
            method: "createDownload",
            request: Some("CreateDownloadByInodeRequest"),
        },
    ),
    (
        "create_namespace",
        SdkName {
            group: &["namespaces"],
            method: "create",
            request: None,
        },
    ),
    (
        "create_snapshot",
        SdkName {
            group: &["snapshots"],
            method: "create",
            request: None,
        },
    ),
    (
        "create_upload",
        SdkName {
            group: &["uploads"],
            method: "create",
            request: Some("CreateUploadRequest"),
        },
    ),
    (
        "delete_namespace",
        SdkName {
            group: &["namespaces"],
            method: "delete",
            request: Some("DeleteNamespaceRequest"),
        },
    ),
    (
        "disable_grep_index",
        SdkName {
            group: &["maintenance", "grepIndex"],
            method: "disable",
            request: Some("DisableGrepIndexRequest"),
        },
    ),
    (
        "enable_grep_index",
        SdkName {
            group: &["maintenance", "grepIndex"],
            method: "enable",
            request: Some("EnableGrepIndexRequest"),
        },
    ),
    (
        "extend_snapshot",
        SdkName {
            group: &["snapshots"],
            method: "extend",
            request: None,
        },
    ),
    (
        "fork_namespace",
        SdkName {
            group: &["namespaces"],
            method: "fork",
            request: None,
        },
    ),
    (
        "gc_grep_index",
        SdkName {
            group: &["maintenance", "grepIndex"],
            method: "gc",
            request: None,
        },
    ),
    (
        "get_capabilities",
        SdkName {
            group: &["capabilities"],
            method: "retrieve",
            request: None,
        },
    ),
    (
        "get_file_bytes",
        SdkName {
            group: &["files"],
            method: "content",
            request: Some("GetFileBytesRequest"),
        },
    ),
    (
        "get_file_revision_bytes_by_inode",
        SdkName {
            group: &["inodes"],
            method: "content",
            request: Some("GetFileRevisionBytesByInodeRequest"),
        },
    ),
    (
        "get_grep_index",
        SdkName {
            group: &["maintenance", "grepIndex"],
            method: "retrieve",
            request: Some("GetGrepIndexRequest"),
        },
    ),
    (
        "get_inode",
        SdkName {
            group: &["inodes"],
            method: "retrieve",
            request: Some("GetInodeRequest"),
        },
    ),
    (
        "get_namespace",
        SdkName {
            group: &["namespaces"],
            method: "retrieve",
            request: Some("GetNamespaceRequest"),
        },
    ),
    (
        "get_namespace_diagnostics",
        SdkName {
            group: &["maintenance", "diagnostics"],
            method: "retrieve",
            request: Some("GetNamespaceDiagnosticsRequest"),
        },
    ),
    (
        "get_path_entry",
        SdkName {
            group: &["files"],
            method: "retrieve",
            request: Some("GetPathEntryRequest"),
        },
    ),
    (
        "get_upload",
        SdkName {
            group: &["uploads"],
            method: "retrieve",
            request: Some("GetUploadRequest"),
        },
    ),
    (
        "grep",
        SdkName {
            group: &["files"],
            method: "grep",
            request: Some("GrepRequest"),
        },
    ),
    (
        "list_changes",
        SdkName {
            group: &["changes"],
            method: "list",
            request: Some("ListChangesRequest"),
        },
    ),
    (
        "list_checkpoints",
        SdkName {
            group: &["maintenance", "checkpoints"],
            method: "list",
            request: Some("ListCheckpointsRequest"),
        },
    ),
    (
        "list_file_revisions",
        SdkName {
            group: &["files"],
            method: "listRevisions",
            request: Some("ListFileRevisionsRequest"),
        },
    ),
    (
        "list_file_revisions_by_inode",
        SdkName {
            group: &["inodes"],
            method: "listRevisions",
            request: Some("ListFileRevisionsByInodeRequest"),
        },
    ),
    (
        "list_inode_children",
        SdkName {
            group: &["inodes"],
            method: "listChildren",
            request: Some("ListInodeChildrenRequest"),
        },
    ),
    (
        "list_path_entries",
        SdkName {
            group: &["files"],
            method: "list",
            request: Some("ListPathEntriesRequest"),
        },
    ),
    (
        "list_snapshots",
        SdkName {
            group: &["snapshots"],
            method: "list",
            request: Some("ListSnapshotsRequest"),
        },
    ),
    (
        "list_trash",
        SdkName {
            group: &["trash"],
            method: "list",
            request: Some("ListTrashRequest"),
        },
    ),
    (
        "probe_store",
        SdkName {
            group: &["maintenance", "store"],
            method: "probe",
            request: None,
        },
    ),
    (
        "put_upload_content",
        SdkName {
            group: &["uploads"],
            method: "putContent",
            request: None,
        },
    ),
    (
        "release_checkpoint",
        SdkName {
            group: &["maintenance", "checkpoints"],
            method: "release",
            request: Some("ReleaseCheckpointRequest"),
        },
    ),
    (
        "release_snapshot",
        SdkName {
            group: &["snapshots"],
            method: "release",
            request: Some("ReleaseSnapshotRequest"),
        },
    ),
    (
        "run_maintenance",
        SdkName {
            group: &["maintenance", "runs"],
            method: "create",
            request: None,
        },
    ),
    (
        "sign_upload_parts",
        SdkName {
            group: &["uploads"],
            method: "signParts",
            request: None,
        },
    ),
];

/// Operations served over HTTP but left out of generated SDKs. Keep this table sorted.
pub(crate) const SDK_EXCLUDED_OPERATIONS: &[&str] = &["get_health", "get_metrics", "get_readiness"];

/// The SDK surface of one operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SdkName {
    pub(crate) group: &'static [&'static str],
    pub(crate) method: &'static str,
    pub(crate) request: Option<&'static str>,
}

pub(super) fn validate_operation_retry_classes(
    document: &Value,
) -> Result<(), OpenapiPostprocessError> {
    for (operation_id, operation) in operations(document)? {
        if operation
            .get("x-loonfs-retry")
            .and_then(Value::as_str)
            .is_none()
        {
            return Err(OpenapiPostprocessError::MissingRetryClassification {
                operation_id: operation_id.to_owned(),
            });
        }
    }
    Ok(())
}

/// Adds the Fern naming extensions to every operation and hides the excluded ones.
pub(super) fn add_sdk_names(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let mut seen_operations = BTreeSet::new();

    for (operation_id, operation) in operations_mut(document)? {
        seen_operations.insert(operation_id.clone());

        if SDK_EXCLUDED_OPERATIONS.contains(&operation_id.as_str()) {
            operation.insert("x-fern-ignore".to_owned(), Value::Bool(true));
            continue;
        }

        let sdk_name = OPERATION_SDK_NAMES
            .iter()
            .find_map(|(candidate, sdk_name)| (*candidate == operation_id).then_some(*sdk_name))
            .ok_or_else(|| OpenapiPostprocessError::MissingSdkName {
                operation_id: operation_id.clone(),
            })?;
        operation.insert(
            "x-fern-sdk-group-name".to_owned(),
            Value::Array(
                sdk_name
                    .group
                    .iter()
                    .map(|segment| Value::String((*segment).to_owned()))
                    .collect(),
            ),
        );
        operation.insert(
            "x-fern-sdk-method-name".to_owned(),
            Value::String(sdk_name.method.to_owned()),
        );
        if let Some(request) = sdk_name.request {
            operation.insert(
                "x-fern-request-name".to_owned(),
                Value::String(request.to_owned()),
            );
        }
    }

    for &(operation_id, _) in OPERATION_SDK_NAMES {
        if !seen_operations.contains(operation_id) {
            return Err(OpenapiPostprocessError::MissingSdkNameOperation {
                operation_id: operation_id.to_owned(),
            });
        }
    }
    for &operation_id in SDK_EXCLUDED_OPERATIONS {
        if !seen_operations.contains(operation_id) {
            return Err(OpenapiPostprocessError::MissingSdkNameOperation {
                operation_id: operation_id.to_owned(),
            });
        }
    }

    Ok(())
}

pub(super) fn validate_pagination_metadata(
    document: &Value,
) -> Result<(), OpenapiPostprocessError> {
    let schemas = component_schemas(document)?;

    for (operation_id, operation) in operations(document)? {
        let has_cursor = has_query_parameter(operation, "cursor");
        let Some(metadata) = operation.get("x-fern-pagination") else {
            if has_cursor {
                return Err(OpenapiPostprocessError::MissingPaginationMetadata {
                    operation_id: operation_id.to_owned(),
                });
            }
            continue;
        };
        if !has_cursor {
            return Err(OpenapiPostprocessError::MissingPaginationCursorParameter {
                operation_id: operation_id.to_owned(),
            });
        }

        let results_property = validate_pagination_response(schemas, operation, operation_id)?;
        let expected = serde_json::json!({
            "cursor": "$request.cursor",
            "next_cursor": "$response.next_cursor",
            "results": format!("$response.{results_property}"),
        });
        if metadata != &expected {
            return Err(super::value::invalid_document(format!(
                "operations.{operation_id}.x-fern-pagination"
            )));
        }
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

fn validate_pagination_response(
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
