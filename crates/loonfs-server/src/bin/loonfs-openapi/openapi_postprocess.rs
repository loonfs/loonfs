use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

type Map = IndexMap<String, Value>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum Value {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Value>),
    Object(Map),
}

/// Generated SDKs use operation IDs as method names. Keep this list sorted.
pub(crate) const OPENAPI_OPERATION_IDS: &[&str] = &[
    "abort_upload",
    "apply_commit",
    "begin_download",
    "begin_download_by_inode",
    "begin_upload",
    "capabilities",
    "complete_upload",
    "create_checkpoint",
    "create_namespace",
    "delete_namespace",
    "disable_grep_index",
    "enable_grep_index",
    "fork_namespace",
    "gc_grep_index",
    "get_file_bytes",
    "get_file_revision_bytes_by_inode",
    "get_grep_index_status",
    "get_metrics",
    "get_namespace",
    "get_namespace_diagnostics",
    "get_upload_status",
    "grep",
    "health",
    "list_changes",
    "list_checkpoints",
    "list_file_revisions",
    "list_file_revisions_by_inode",
    "list_path_entries",
    "list_trash",
    "maintenance_step",
    "probe_store",
    "readiness",
    "release_checkpoint",
    "sign_upload_parts",
    "stat_inode",
    "stat_path",
    "upload_content",
];

/// Whether a proxy operation uses a mount path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProxyOperationScope {
    Unscoped,
    Mount,
}

/// Operations included in the browser proxy document. Keep this table sorted.
pub(crate) const PROXY_OPERATIONS: &[(&str, ProxyOperationScope)] = &[
    ("abort_upload", ProxyOperationScope::Mount),
    ("apply_commit", ProxyOperationScope::Mount),
    ("begin_download", ProxyOperationScope::Mount),
    ("begin_upload", ProxyOperationScope::Mount),
    ("capabilities", ProxyOperationScope::Unscoped),
    ("complete_upload", ProxyOperationScope::Mount),
    ("get_file_bytes", ProxyOperationScope::Mount),
    ("get_upload_status", ProxyOperationScope::Mount),
    ("grep", ProxyOperationScope::Mount),
    ("list_changes", ProxyOperationScope::Mount),
    ("list_file_revisions", ProxyOperationScope::Mount),
    ("list_path_entries", ProxyOperationScope::Mount),
    ("list_trash", ProxyOperationScope::Mount),
    ("sign_upload_parts", ProxyOperationScope::Mount),
    ("stat_path", ProxyOperationScope::Mount),
    ("upload_content", ProxyOperationScope::Mount),
];

/// Retry behavior for generated SDKs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryClass {
    Safe,
    Replay,
    NewAttempt,
}

/// Pagination fields for cursor list operations. Keep this table sorted.
///
/// Most entries page through the opaque `cursor` and `next_cursor` pair.
/// `list_changes` pages through its sequence fields: `next_after_seq` is
/// absent on the final page, so a pager stops at the snapshot head.
///
/// `gc_grep_index` is excluded: its cursor resumes a maintenance pass across
/// calls and does not paginate results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PaginationOperation {
    pub(crate) operation_id: &'static str,
    pub(crate) cursor_parameter: &'static str,
    pub(crate) next_field: &'static str,
    pub(crate) results_field: &'static str,
}

pub(crate) const PAGINATION_OPERATIONS: &[PaginationOperation] = &[
    PaginationOperation {
        operation_id: "grep",
        cursor_parameter: "cursor",
        next_field: "next_cursor",
        results_field: "matches",
    },
    PaginationOperation {
        operation_id: "list_changes",
        cursor_parameter: "after_seq",
        next_field: "next_after_seq",
        results_field: "changes",
    },
    PaginationOperation {
        operation_id: "list_checkpoints",
        cursor_parameter: "cursor",
        next_field: "next_cursor",
        results_field: "checkpoints",
    },
    PaginationOperation {
        operation_id: "list_file_revisions",
        cursor_parameter: "cursor",
        next_field: "next_cursor",
        results_field: "revisions",
    },
    PaginationOperation {
        operation_id: "list_file_revisions_by_inode",
        cursor_parameter: "cursor",
        next_field: "next_cursor",
        results_field: "revisions",
    },
    PaginationOperation {
        operation_id: "list_path_entries",
        cursor_parameter: "cursor",
        next_field: "next_cursor",
        results_field: "entries",
    },
    PaginationOperation {
        operation_id: "list_trash",
        cursor_parameter: "cursor",
        next_field: "next_cursor",
        results_field: "entries",
    },
];

impl RetryClass {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Replay => "replay",
            Self::NewAttempt => "new_attempt",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenapiPostprocessError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("OpenAPI operation `{operation_id}` has no retry classification")]
    MissingRetryClassification { operation_id: String },
    #[error("OpenAPI pagination operation `{operation_id}` does not appear in the document")]
    MissingPaginationOperation { operation_id: String },
    #[error("OpenAPI pagination operation `{operation_id}` has no `{parameter}` query parameter")]
    MissingPaginationCursorParameter {
        operation_id: String,
        parameter: &'static str,
    },
    #[error("OpenAPI pagination operation `{operation_id}` has no 200 response component schema")]
    MissingPaginationResponseSchema { operation_id: String },
    #[error("OpenAPI pagination operation `{operation_id}` response has no `{property}` property")]
    MissingPaginationResponseProperty {
        operation_id: String,
        property: String,
    },
    #[error(
        "OpenAPI pagination operation `{operation_id}` response property `{results_field}` is not an array"
    )]
    PaginationResultsNotArray {
        operation_id: String,
        results_field: String,
    },
    #[error(
        "OpenAPI operation `{operation_id}` has a `{parameter}` pagination query parameter but no pagination metadata entry"
    )]
    MissingPaginationMetadata {
        operation_id: String,
        parameter: String,
    },
    #[error("OpenAPI pagination metadata cannot read `{location}`")]
    InvalidPaginationDocument { location: &'static str },
    #[error("OpenAPI path item `{path}` is not an object")]
    InvalidPaginationPathItem { path: String },
    #[error("OpenAPI operation `{method} {path}` is not an object or has no operation ID")]
    InvalidPaginationOperation { method: &'static str, path: String },
    #[error("OpenAPI union variant `{schema_name}` conflicts with an existing component schema")]
    UnionVariantSchemaCollision { schema_name: String },
    #[error("proxy operation `{operation_id}` does not appear in the full OpenAPI document")]
    MissingProxyOperation { operation_id: String },
    #[error(
        "proxy operation `{operation_id}` appears more than once in the full OpenAPI document"
    )]
    DuplicateProxyOperation { operation_id: String },
    #[error("proxy operation `{operation_id}` has an unexpected path `{path}`")]
    UnexpectedProxyPath { operation_id: String, path: String },
    #[error("proxy operation `{operation_id}` has no `namespace_id` path parameter")]
    MissingProxyNamespaceParameter { operation_id: String },
    #[error("proxy document references a missing OpenAPI component `{reference}`")]
    MissingProxyComponent { reference: String },
}

/// Retry class for each public operation.
pub(crate) const OPERATION_RETRY_CLASSES: &[(&str, RetryClass)] = &[
    ("abort_upload", RetryClass::Safe),
    ("apply_commit", RetryClass::Replay),
    ("begin_download", RetryClass::Safe),
    ("begin_download_by_inode", RetryClass::Safe),
    ("begin_upload", RetryClass::NewAttempt),
    ("capabilities", RetryClass::Safe),
    ("complete_upload", RetryClass::Replay),
    ("create_checkpoint", RetryClass::NewAttempt),
    ("create_namespace", RetryClass::NewAttempt),
    ("delete_namespace", RetryClass::NewAttempt),
    ("disable_grep_index", RetryClass::Safe),
    ("enable_grep_index", RetryClass::Safe),
    ("fork_namespace", RetryClass::NewAttempt),
    ("gc_grep_index", RetryClass::NewAttempt),
    ("get_file_bytes", RetryClass::Safe),
    ("get_file_revision_bytes_by_inode", RetryClass::Safe),
    ("get_grep_index_status", RetryClass::Safe),
    ("get_metrics", RetryClass::Safe),
    ("get_namespace", RetryClass::Safe),
    ("get_namespace_diagnostics", RetryClass::Safe),
    ("get_upload_status", RetryClass::Safe),
    ("grep", RetryClass::Safe),
    ("health", RetryClass::Safe),
    ("list_changes", RetryClass::Safe),
    ("list_checkpoints", RetryClass::Safe),
    ("list_file_revisions", RetryClass::Safe),
    ("list_file_revisions_by_inode", RetryClass::Safe),
    ("list_path_entries", RetryClass::Safe),
    ("list_trash", RetryClass::Safe),
    ("maintenance_step", RetryClass::NewAttempt),
    ("probe_store", RetryClass::NewAttempt),
    ("readiness", RetryClass::Safe),
    ("release_checkpoint", RetryClass::Safe),
    ("sign_upload_parts", RetryClass::Safe),
    ("stat_inode", RetryClass::Safe),
    ("stat_path", RetryClass::Safe),
    ("upload_content", RetryClass::Safe),
];

impl Value {
    fn get(&self, name: &str) -> Option<&Self> {
        self.as_object()?.get(name)
    }

    fn as_array(&self) -> Option<&Vec<Self>> {
        match self {
            Self::Array(values) => Some(values),
            _ => None,
        }
    }

    fn as_object(&self) -> Option<&Map> {
        match self {
            Self::Object(object) => Some(object),
            _ => None,
        }
    }

    fn as_object_mut(&mut self) -> Option<&mut Map> {
        match self {
            Self::Object(object) => Some(object),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }
}

/// Generates OpenAPI JSON, applies schema fixes, and preserves field order.
pub(crate) fn openapi_json_pretty(
    document: &(impl Serialize + ?Sized),
) -> Result<String, OpenapiPostprocessError> {
    let derived = serde_json::to_string(document)?;
    let mut document = serde_json::from_str(&derived)?;
    normalize_optional_schemas(&mut document);
    add_union_discriminators(&mut document);
    extract_union_variants(&mut document)?;
    add_operation_retry_classes(&mut document)?;
    add_pagination_metadata(&mut document)?;
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
    describe_proxy_document(&mut document);
    derive_proxy_paths(&mut document)?;
    remove_proxy_security(&mut document);
    retain_referenced_tags(&mut document);
    prune_proxy_components(&mut document)?;
    Ok(serde_json::to_string_pretty(&document)?)
}

const HTTP_METHODS: &[&str] = &["get", "post", "put", "delete", "patch"];
const NAMESPACE_PATH_PREFIX: &str = "/v0/namespaces/{namespace_id}";
const MOUNT_PATH_PREFIX: &str = "/v0/mounts/{mount}";

fn describe_proxy_document(document: &mut Value) {
    let info = document
        .as_object_mut()
        .and_then(|document| document.get_mut("info"))
        .and_then(Value::as_object_mut)
        .expect("OpenAPI document has no info object");
    info.insert(
        "title".to_owned(),
        Value::String("LoonFS Browser Proxy API".to_owned()),
    );
    info.insert(
        "description".to_owned(),
        Value::String(
            "API for browser clients that access namespaces through application mounts.".to_owned(),
        ),
    );
}

fn derive_proxy_paths(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let paths = document
        .as_object_mut()
        .and_then(|document| document.get_mut("paths"))
        .and_then(Value::as_object_mut)
        .expect("OpenAPI document has no paths object");
    let mut derived_paths = Map::new();
    let mut found_operations = BTreeSet::new();

    for (path, path_item) in paths.iter() {
        let path_item = path_item
            .as_object()
            .expect("OpenAPI path item is not an object");
        let mut derived_path_item = Map::new();
        let mut derived_path = None;

        for (field, value) in path_item {
            if !HTTP_METHODS.contains(&field.as_str()) {
                derived_path_item.insert(field.clone(), value.clone());
                continue;
            }

            let operation_id = value
                .get("operationId")
                .and_then(Value::as_str)
                .expect("OpenAPI operation has no operationId");
            let Some((_, scope)) = PROXY_OPERATIONS
                .iter()
                .find(|(candidate, _)| *candidate == operation_id)
            else {
                continue;
            };
            if !found_operations.insert(operation_id.to_owned()) {
                return Err(OpenapiPostprocessError::DuplicateProxyOperation {
                    operation_id: operation_id.to_owned(),
                });
            }

            let operation_path = proxy_path(operation_id, path, *scope)?;
            if let Some(existing) = &derived_path {
                assert_eq!(
                    existing, &operation_path,
                    "one path item changed proxy scope"
                );
            } else {
                derived_path = Some(operation_path);
            }

            let mut operation = value.clone();
            operation
                .as_object_mut()
                .expect("OpenAPI operation is not an object")
                .shift_remove("security");
            if *scope == ProxyOperationScope::Mount
                && rewrite_namespace_parameters(&mut operation) == 0
            {
                return Err(OpenapiPostprocessError::MissingProxyNamespaceParameter {
                    operation_id: operation_id.to_owned(),
                });
            }
            derived_path_item.insert(field.clone(), operation);
        }

        if let Some(derived_path) = derived_path {
            derived_paths.insert(derived_path, Value::Object(derived_path_item));
        }
    }

    for (operation_id, _) in PROXY_OPERATIONS {
        if !found_operations.contains(*operation_id) {
            return Err(OpenapiPostprocessError::MissingProxyOperation {
                operation_id: (*operation_id).to_owned(),
            });
        }
    }
    *paths = derived_paths;
    Ok(())
}

fn proxy_path(
    operation_id: &str,
    path: &str,
    scope: ProxyOperationScope,
) -> Result<String, OpenapiPostprocessError> {
    match scope {
        ProxyOperationScope::Unscoped => Ok(path.to_owned()),
        ProxyOperationScope::Mount => path
            .strip_prefix(NAMESPACE_PATH_PREFIX)
            .map(|suffix| format!("{MOUNT_PATH_PREFIX}{suffix}"))
            .ok_or_else(|| OpenapiPostprocessError::UnexpectedProxyPath {
                operation_id: operation_id.to_owned(),
                path: path.to_owned(),
            }),
    }
}

fn rewrite_namespace_parameters(operation: &mut Value) -> usize {
    let Some(parameters) = operation
        .as_object_mut()
        .and_then(|operation| operation.get_mut("parameters"))
        .and_then(|parameters| match parameters {
            Value::Array(parameters) => Some(parameters),
            _ => None,
        })
    else {
        return 0;
    };
    let mut rewritten = 0;

    for parameter in parameters {
        let Some(parameter) = parameter.as_object_mut() else {
            continue;
        };
        if parameter.get("in").and_then(Value::as_str) != Some("path")
            || parameter.get("name").and_then(Value::as_str) != Some("namespace_id")
        {
            continue;
        }
        parameter.insert("name".to_owned(), Value::String("mount".to_owned()));
        parameter.insert(
            "description".to_owned(),
            Value::String("Application mount name".to_owned()),
        );
        parameter.insert(
            "schema".to_owned(),
            Value::Object(IndexMap::from([(
                "type".to_owned(),
                Value::String("string".to_owned()),
            )])),
        );
        rewritten += 1;
    }
    rewritten
}

fn remove_proxy_security(document: &mut Value) {
    let document = document
        .as_object_mut()
        .expect("OpenAPI document is not an object");
    document.shift_remove("security");
    if let Some(components) = document
        .get_mut("components")
        .and_then(Value::as_object_mut)
    {
        components.shift_remove("securitySchemes");
    }
}

/// Retains only tag definitions that a retained operation references, so a
/// tag whose operations were all pruned does not survive as a dead entry.
fn retain_referenced_tags(document: &mut Value) {
    let mut referenced = BTreeSet::new();
    if let Some(paths) = document.get("paths").and_then(Value::as_object) {
        for path_item in paths.values() {
            let Some(path_item) = path_item.as_object() else {
                continue;
            };
            for (field, operation) in path_item {
                if !HTTP_METHODS.contains(&field.as_str()) {
                    continue;
                }
                let Some(tags) = operation.get("tags").and_then(Value::as_array) else {
                    continue;
                };
                for tag in tags {
                    if let Some(tag) = tag.as_str() {
                        referenced.insert(tag.to_owned());
                    }
                }
            }
        }
    }
    let Some(tags) = document
        .as_object_mut()
        .and_then(|document| document.get_mut("tags"))
        .and_then(|tags| match tags {
            Value::Array(tags) => Some(tags),
            _ => None,
        })
    else {
        return;
    };
    tags.retain(|tag| {
        tag.get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| referenced.contains(name))
    });
}

fn prune_proxy_components(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let mut pending = Vec::new();
    collect_component_references(
        document
            .get("paths")
            .expect("OpenAPI document has no paths object"),
        &mut pending,
    );
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
        .expect("OpenAPI document has no components object");
    components.retain(|component_type, entries| {
        let Some(entries) = entries.as_object_mut() else {
            return false;
        };
        entries.retain(|component_name, _| {
            referenced.contains(&(component_type.clone(), component_name.clone()))
        });
        !entries.is_empty()
    });
    Ok(())
}

fn collect_component_references(value: &Value, references: &mut Vec<String>) {
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

fn component_reference_parts(reference: &str) -> Option<(String, String)> {
    let mut parts = reference.strip_prefix("#/components/")?.split('/');
    let component_type = decode_json_pointer_segment(parts.next()?);
    let component_name = decode_json_pointer_segment(parts.next()?);
    Some((component_type, component_name))
}

fn decode_json_pointer_segment(segment: &str) -> String {
    segment.replace("~1", "/").replace("~0", "~")
}

/// Adds `x-loonfs-retry` to every OpenAPI operation.
fn add_operation_retry_classes(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    assert_eq!(
        OPENAPI_OPERATION_IDS.len(),
        OPERATION_RETRY_CLASSES.len(),
        "operation IDs and retry classes must have the same length"
    );
    for (registered, (classified, _)) in OPENAPI_OPERATION_IDS
        .iter()
        .zip(OPERATION_RETRY_CLASSES.iter())
    {
        assert_eq!(
            registered, classified,
            "operation IDs and retry classes must use the same order"
        );
    }

    let paths = document
        .as_object_mut()
        .and_then(|document| document.get_mut("paths"))
        .and_then(Value::as_object_mut)
        .expect("OpenAPI document has no paths object");

    for path_item in paths.values_mut() {
        let path_item = path_item
            .as_object_mut()
            .expect("OpenAPI path item is not an object");
        for method in ["get", "post", "put", "delete", "patch"] {
            let Some(operation) = path_item.get_mut(method) else {
                continue;
            };
            let operation = operation
                .as_object_mut()
                .expect("OpenAPI operation is not an object");
            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .expect("OpenAPI operation has no operationId");
            let retry_class = OPERATION_RETRY_CLASSES
                .iter()
                .find_map(|(candidate, retry_class)| {
                    (*candidate == operation_id).then_some(*retry_class)
                })
                .ok_or_else(|| OpenapiPostprocessError::MissingRetryClassification {
                    operation_id: operation_id.to_owned(),
                })?;
            operation.insert(
                "x-loonfs-retry".to_owned(),
                Value::String(retry_class.as_str().to_owned()),
            );
        }
    }
    Ok(())
}

/// Adds `x-fern-pagination` to each cursor list operation.
fn add_pagination_metadata(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    validate_pagination_metadata(document)?;

    let paths = document
        .as_object_mut()
        .and_then(|document| document.get_mut("paths"))
        .and_then(Value::as_object_mut)
        .ok_or(OpenapiPostprocessError::InvalidPaginationDocument { location: "paths" })?;

    for (path, path_item) in paths {
        let path_item = path_item.as_object_mut().ok_or_else(|| {
            OpenapiPostprocessError::InvalidPaginationPathItem { path: path.clone() }
        })?;
        for &method in HTTP_METHODS {
            let Some(operation) = path_item.get_mut(method) else {
                continue;
            };
            let operation = operation.as_object_mut().ok_or_else(|| {
                OpenapiPostprocessError::InvalidPaginationOperation {
                    method,
                    path: path.clone(),
                }
            })?;
            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| OpenapiPostprocessError::InvalidPaginationOperation {
                    method,
                    path: path.clone(),
                })?;
            let Some(metadata) = pagination_operation(&operation_id) else {
                continue;
            };

            let mut extension = Map::new();
            extension.insert(
                "cursor".to_owned(),
                Value::String(format!("$request.{}", metadata.cursor_parameter)),
            );
            extension.insert(
                "next_cursor".to_owned(),
                Value::String(format!("$response.{}", metadata.next_field)),
            );
            extension.insert(
                "results".to_owned(),
                Value::String(format!("$response.{}", metadata.results_field)),
            );
            operation.insert("x-fern-pagination".to_owned(), Value::Object(extension));
        }
    }

    Ok(())
}

fn validate_pagination_metadata(document: &Value) -> Result<(), OpenapiPostprocessError> {
    let schemas = document
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .ok_or(OpenapiPostprocessError::InvalidPaginationDocument {
            location: "components.schemas",
        })?;
    let paths = document
        .get("paths")
        .and_then(Value::as_object)
        .ok_or(OpenapiPostprocessError::InvalidPaginationDocument { location: "paths" })?;
    let mut found_operations = BTreeSet::new();

    for (path, path_item) in paths {
        let path_item = path_item.as_object().ok_or_else(|| {
            OpenapiPostprocessError::InvalidPaginationPathItem { path: path.clone() }
        })?;
        for &method in HTTP_METHODS {
            let Some(operation) = path_item.get(method) else {
                continue;
            };
            let operation = operation.as_object().ok_or_else(|| {
                OpenapiPostprocessError::InvalidPaginationOperation {
                    method,
                    path: path.clone(),
                }
            })?;
            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .ok_or_else(|| OpenapiPostprocessError::InvalidPaginationOperation {
                    method,
                    path: path.clone(),
                })?;
            let pagination_parameter = ["cursor", "after_seq"]
                .into_iter()
                .find(|name| has_query_parameter(operation, name));
            let Some(metadata) = pagination_operation(operation_id) else {
                if let Some(parameter) = pagination_parameter {
                    return Err(OpenapiPostprocessError::MissingPaginationMetadata {
                        operation_id: operation_id.to_owned(),
                        parameter: parameter.to_owned(),
                    });
                }
                continue;
            };
            found_operations.insert(metadata.operation_id);

            if !has_query_parameter(operation, metadata.cursor_parameter) {
                return Err(OpenapiPostprocessError::MissingPaginationCursorParameter {
                    operation_id: operation_id.to_owned(),
                    parameter: metadata.cursor_parameter,
                });
            }
            validate_pagination_response(schemas, operation, metadata)?;
        }
    }

    for metadata in PAGINATION_OPERATIONS {
        if !found_operations.contains(metadata.operation_id) {
            return Err(OpenapiPostprocessError::MissingPaginationOperation {
                operation_id: metadata.operation_id.to_owned(),
            });
        }
    }

    Ok(())
}

fn pagination_operation(operation_id: &str) -> Option<&'static PaginationOperation> {
    PAGINATION_OPERATIONS
        .iter()
        .find(|metadata| metadata.operation_id == operation_id)
}

fn has_query_parameter(operation: &Map, parameter_name: &str) -> bool {
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
    schemas: &Map,
    operation: &Map,
    metadata: &PaginationOperation,
) -> Result<(), OpenapiPostprocessError> {
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
                operation_id: metadata.operation_id.to_owned(),
            },
        )?;
    let properties = response_schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(
            || OpenapiPostprocessError::MissingPaginationResponseSchema {
                operation_id: metadata.operation_id.to_owned(),
            },
        )?;

    for property in [metadata.next_field, metadata.results_field] {
        if !properties.contains_key(property) {
            return Err(OpenapiPostprocessError::MissingPaginationResponseProperty {
                operation_id: metadata.operation_id.to_owned(),
                property: property.to_owned(),
            });
        }
    }
    if properties
        .get(metadata.results_field)
        .and_then(|property| property.get("type"))
        .and_then(Value::as_str)
        != Some("array")
    {
        return Err(OpenapiPostprocessError::PaginationResultsNotArray {
            operation_id: metadata.operation_id.to_owned(),
            results_field: metadata.results_field.to_owned(),
        });
    }

    Ok(())
}

/// Removes `null` from schemas for values that are omitted when absent.
fn normalize_optional_schemas(document: &mut Value) {
    normalize_optional_schemas_in(document);
}

/// Adds discriminators that utoipa 5.5 omits from tagged unions.
fn add_union_discriminators(document: &mut Value) {
    visit(document, &mut Vec::new());
}

/// Moves inline union variants into `components.schemas`.
fn extract_union_variants(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    struct UnionRewrite {
        union_name: String,
        variants: Vec<Value>,
        mapping: Map,
    }

    let schemas = document
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .expect("OpenAPI document has no component schemas object");
    let mut extracted = Map::new();
    let mut rewrites = Vec::new();

    for (union_name, union) in schemas {
        let Some(variants) = union.get("oneOf").and_then(Value::as_array) else {
            continue;
        };
        let Some(discriminator) = union.get("discriminator").and_then(Value::as_object) else {
            continue;
        };
        let property_name = discriminator
            .get("propertyName")
            .and_then(Value::as_str)
            .expect("OpenAPI discriminator has no propertyName");
        let mut mapping = Map::new();
        let mut references = Vec::with_capacity(variants.len());

        for variant in variants {
            if let Some(reference) = variant.get("$ref").and_then(Value::as_str) {
                let schema = component_schema_for_reference(schemas, reference)
                    .expect("OpenAPI union variant does not reference a component schema");
                let tag = fixed_discriminator_value(schema, property_name)
                    .expect("OpenAPI union variant has no fixed discriminator value");
                mapping.insert(tag, Value::String(reference.to_owned()));
                references.push(variant.clone());
                continue;
            }

            let tag = fixed_discriminator_value(variant, property_name)
                .expect("OpenAPI union variant has no fixed discriminator value");
            let schema_name = variant
                .get("title")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{union_name}{}", pascal_case(&tag)));
            let reference = component_schema_reference(&schema_name);
            let mut schema = variant.clone();
            schema
                .as_object_mut()
                .expect("OpenAPI union variant is not an object")
                .shift_remove("title");

            register_extracted_schema(schemas, &mut extracted, &schema_name, schema)?;
            mapping.insert(tag, Value::String(reference.clone()));
            references.push(Value::Object(IndexMap::from([(
                "$ref".to_owned(),
                Value::String(reference),
            )])));
        }

        rewrites.push(UnionRewrite {
            union_name: union_name.clone(),
            variants: references,
            mapping,
        });
    }

    let schemas = document
        .as_object_mut()
        .and_then(|document| document.get_mut("components"))
        .and_then(Value::as_object_mut)
        .and_then(|components| components.get_mut("schemas"))
        .and_then(Value::as_object_mut)
        .expect("OpenAPI document has no component schemas object");
    for rewrite in rewrites {
        let union = schemas
            .get_mut(&rewrite.union_name)
            .and_then(Value::as_object_mut)
            .expect("OpenAPI union component is not an object");
        union.insert("oneOf".to_owned(), Value::Array(rewrite.variants));
        union
            .get_mut("discriminator")
            .and_then(Value::as_object_mut)
            .expect("OpenAPI union discriminator is not an object")
            .insert("mapping".to_owned(), Value::Object(rewrite.mapping));
    }
    schemas.extend(extracted);
    Ok(())
}

fn register_extracted_schema(
    schemas: &Map,
    extracted: &mut Map,
    schema_name: &str,
    schema: Value,
) -> Result<(), OpenapiPostprocessError> {
    if let Some(existing) = schemas
        .get(schema_name)
        .or_else(|| extracted.get(schema_name))
    {
        if existing != &schema {
            return Err(OpenapiPostprocessError::UnionVariantSchemaCollision {
                schema_name: schema_name.to_owned(),
            });
        }
        return Ok(());
    }

    extracted.insert(schema_name.to_owned(), schema);
    Ok(())
}

fn fixed_discriminator_value(variant: &Value, property_name: &str) -> Option<String> {
    fixed_required_properties(variant)?
        .into_iter()
        .find_map(|(name, value)| (name == property_name).then_some(value))
}

fn component_schema_for_reference<'a>(schemas: &'a Map, reference: &str) -> Option<&'a Value> {
    let encoded_name = reference.strip_prefix("#/components/schemas/")?;
    let schema_name = encoded_name.replace("~1", "/").replace("~0", "~");
    schemas.get(&schema_name)
}

fn component_schema_reference(schema_name: &str) -> String {
    format!(
        "#/components/schemas/{}",
        schema_name.replace('~', "~0").replace('/', "~1")
    )
}

fn pascal_case(value: &str) -> String {
    value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut characters = part.chars();
            let first = characters
                .next()
                .expect("filtered discriminator part is not empty");
            first.to_ascii_uppercase().to_string() + characters.as_str()
        })
        .collect()
}

fn normalize_optional_schemas_in(value: &mut Value) {
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
                        assert!(
                            !required.contains(name.as_str()),
                            "property `{name}` is required but has an Option schema"
                        );
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
                            assert!(!required, "required request body has an Option schema");
                            *schema = replacement;
                        }
                    }
                }
            }

            for child in object.values_mut() {
                normalize_optional_schemas_in(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                normalize_optional_schemas_in(child);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
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

fn visit(value: &mut Value, path: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(discriminator) = discriminator_for(object, path) {
                object.insert("discriminator".to_owned(), discriminator);
            }

            for (name, child) in object {
                path.push(name.clone());
                visit(child, path);
                path.pop();
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter_mut().enumerate() {
                path.push(index.to_string());
                visit(child, path);
                path.pop();
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn discriminator_for(object: &Map, path: &[String]) -> Option<Value> {
    let variants = object.get("oneOf")?.as_array()?;
    if variants.is_empty() {
        return None;
    }

    let fixed_properties = variants
        .iter()
        .map(fixed_required_properties)
        .collect::<Option<Vec<_>>>()?;
    let common_names = fixed_properties.iter().skip(1).fold(
        fixed_properties[0]
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<BTreeSet<_>>(),
        |common, properties| {
            let names = properties
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<BTreeSet<_>>();
            common.intersection(&names).copied().collect()
        },
    );
    let property_name = common_names.iter().copied().next()?;
    if common_names.len() != 1 {
        return None;
    }

    let values = fixed_properties
        .iter()
        .map(|properties| {
            properties
                .iter()
                .find_map(|(name, value)| (name == property_name).then_some(value.as_str()))
        })
        .collect::<Option<Vec<_>>>()?;
    if values.iter().copied().collect::<BTreeSet<_>>().len() != variants.len() {
        return None;
    }

    let one_of_pointer = format!("{}/oneOf", json_pointer(path));
    let mapping: Map = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            (
                value.to_owned(),
                Value::String(format!("#{one_of_pointer}/{index}")),
            )
        })
        .collect();

    Some(Value::Object(IndexMap::from([
        (
            "propertyName".to_owned(),
            Value::String(property_name.to_owned()),
        ),
        ("mapping".to_owned(), Value::Object(mapping)),
    ])))
}

fn fixed_required_properties(variant: &Value) -> Option<Vec<(String, String)>> {
    let variant = variant.as_object()?;
    let required = variant.get("required")?.as_array()?;
    let properties = variant.get("properties")?.as_object()?;

    Some(
        required
            .iter()
            .filter_map(Value::as_str)
            .filter_map(|name| {
                fixed_string(properties.get(name)?).map(|value| (name.to_owned(), value.to_owned()))
            })
            .collect(),
    )
}

fn fixed_string(schema: &Value) -> Option<&str> {
    if let Some(value) = schema.get("const").and_then(Value::as_str) {
        return Some(value);
    }

    let values = schema.get("enum")?.as_array()?;
    if values.len() == 1 {
        values[0].as_str()
    } else {
        None
    }
}

fn json_pointer(path: &[String]) -> String {
    path.iter().fold(String::new(), |mut pointer, segment| {
        pointer.push('/');
        pointer.push_str(&segment.replace('~', "~0").replace('/', "~1"));
        pointer
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ordered(value: serde_json::Value) -> Value {
        serde_json::from_value(value).expect("decode ordered JSON value")
    }

    fn unordered(value: &Value) -> serde_json::Value {
        serde_json::to_value(value).expect("encode unordered JSON value")
    }

    #[test]
    fn extracts_named_and_derived_union_variants() {
        let mut document = ordered(serde_json::json!({
            "components": {
                "schemas": {
                    "Choice": {
                        "oneOf": [
                            {
                                "title": "ChoiceFirst",
                                "required": ["kind"],
                                "properties": {"kind": {"enum": ["first"]}}
                            },
                            {
                                "required": ["kind", "value"],
                                "properties": {
                                    "kind": {"const": "not_needed"},
                                    "value": {"type": "string"}
                                }
                            }
                        ]
                    }
                }
            }
        }));

        add_union_discriminators(&mut document);
        extract_union_variants(&mut document).expect("extract union variants");
        let actual = unordered(&document);
        assert_eq!(
            actual.pointer("/components/schemas/Choice/discriminator"),
            Some(&serde_json::json!({
                "propertyName": "kind",
                "mapping": {
                    "first": "#/components/schemas/ChoiceFirst",
                    "not_needed": "#/components/schemas/ChoiceNotNeeded"
                }
            }))
        );
        assert_eq!(
            actual.pointer("/components/schemas/Choice/oneOf"),
            Some(&serde_json::json!([
                {"$ref": "#/components/schemas/ChoiceFirst"},
                {"$ref": "#/components/schemas/ChoiceNotNeeded"}
            ]))
        );
        assert_eq!(
            actual.pointer("/components/schemas/ChoiceFirst"),
            Some(&serde_json::json!({
                "required": ["kind"],
                "properties": {"kind": {"enum": ["first"]}}
            }))
        );
        assert_eq!(
            actual.pointer("/components/schemas/ChoiceNotNeeded"),
            Some(&serde_json::json!({
                "required": ["kind", "value"],
                "properties": {
                    "kind": {"const": "not_needed"},
                    "value": {"type": "string"}
                }
            }))
        );

        let once = document.clone();
        add_union_discriminators(&mut document);
        extract_union_variants(&mut document).expect("extract union variants again");
        assert_eq!(document, once);
    }

    #[test]
    fn rejects_a_different_schema_under_the_variant_name() {
        let mut document = ordered(serde_json::json!({
            "components": {
                "schemas": {
                    "Choice": {
                        "oneOf": [{
                            "title": "ChoiceFirst",
                            "required": ["kind"],
                            "properties": {"kind": {"const": "first"}}
                        }]
                    },
                    "ChoiceFirst": {"type": "string"}
                }
            }
        }));

        add_union_discriminators(&mut document);
        let error = extract_union_variants(&mut document)
            .expect_err("different component content should fail generation");
        assert!(matches!(
            error,
            OpenapiPostprocessError::UnionVariantSchemaCollision { schema_name }
                if schema_name == "ChoiceFirst"
        ));
    }

    #[test]
    fn reuses_identical_schema_under_the_variant_name() {
        let variant = serde_json::json!({
            "required": ["kind"],
            "properties": {"kind": {"const": "first"}}
        });
        let mut titled_variant = variant.clone();
        titled_variant
            .as_object_mut()
            .expect("variant object")
            .insert("title".to_owned(), serde_json::json!("ChoiceFirst"));
        let mut document = ordered(serde_json::json!({
            "components": {
                "schemas": {
                    "Choice": {"oneOf": [titled_variant]},
                    "ChoiceFirst": variant
                }
            }
        }));

        add_union_discriminators(&mut document);
        extract_union_variants(&mut document).expect("reuse identical component schema");
        assert_eq!(
            unordered(&document).pointer("/components/schemas/Choice/oneOf/0/$ref"),
            Some(&serde_json::json!("#/components/schemas/ChoiceFirst"))
        );
    }

    #[test]
    fn missing_retry_classification_returns_a_named_error() {
        let mut document = ordered(serde_json::json!({
            "paths": {
                "/future": {
                    "post": {"operationId": "future_operation"}
                }
            }
        }));

        let error = add_operation_retry_classes(&mut document)
            .expect_err("unknown operation should fail generation");
        assert!(matches!(
            error,
            OpenapiPostprocessError::MissingRetryClassification { operation_id }
                if operation_id == "future_operation"
        ));
    }

    #[test]
    fn missing_pagination_operation_returns_a_named_error() {
        let mut document = ordered(serde_json::json!({
            "components": {"schemas": {}},
            "paths": {}
        }));

        let error = add_pagination_metadata(&mut document)
            .expect_err("missing pagination operation should fail generation");
        let expected_operation_id = PAGINATION_OPERATIONS[0].operation_id;
        assert!(matches!(
            error,
            OpenapiPostprocessError::MissingPaginationOperation { operation_id }
                if operation_id == expected_operation_id
        ));
    }

    #[test]
    fn unregistered_cursor_operation_returns_a_named_error() {
        let mut document = ordered(serde_json::json!({
            "components": {"schemas": {}},
            "paths": {
                "/future": {
                    "get": {
                        "operationId": "future_list",
                        "parameters": [{"name": "cursor", "in": "query"}]
                    }
                }
            }
        }));
        let error = add_pagination_metadata(&mut document)
            .expect_err("unregistered cursor operation should fail generation");
        assert!(matches!(
            error,
            OpenapiPostprocessError::MissingPaginationMetadata {
                operation_id,
                parameter,
            } if operation_id == "future_list" && parameter == "cursor"
        ));
    }

    #[test]
    fn replaces_an_optional_property_null_union() {
        let mut document = ordered(serde_json::json!({
            "type": "object",
            "properties": {
                "child": {
                    "oneOf": [
                        {"type": "null"},
                        {"$ref": "#/components/schemas/Child"}
                    ]
                }
            }
        }));

        normalize_optional_schemas(&mut document);
        let actual = unordered(&document);
        assert_eq!(
            actual.pointer("/properties/child"),
            Some(&serde_json::json!({"$ref": "#/components/schemas/Child"}))
        );
    }

    #[test]
    fn replaces_an_optional_request_body_null_union() {
        let mut document = ordered(serde_json::json!({
            "requestBody": {
                "content": {
                    "application/json": {
                        "schema": {
                            "oneOf": [
                                {"type": "null"},
                                {"$ref": "#/components/schemas/Request"}
                            ]
                        }
                    }
                }
            }
        }));

        normalize_optional_schemas(&mut document);
        let actual = unordered(&document);
        assert_eq!(
            actual.pointer("/requestBody/content/application~1json/schema"),
            Some(&serde_json::json!({
                "$ref": "#/components/schemas/Request"
            }))
        );
    }

    #[test]
    #[should_panic(expected = "property `child` is required but has an Option schema")]
    fn refuses_to_rewrite_a_required_property() {
        let mut document = ordered(serde_json::json!({
            "type": "object",
            "required": ["child"],
            "properties": {
                "child": {
                    "oneOf": [
                        {"type": "null"},
                        {"$ref": "#/components/schemas/Child"}
                    ]
                }
            }
        }));

        normalize_optional_schemas(&mut document);
    }

    #[test]
    fn leaves_a_one_of_without_one_shared_fixed_tag_unchanged() {
        let mut document = ordered(serde_json::json!({
            "oneOf": [
                {"required": ["left"], "properties": {"left": {"enum": ["a"]}}},
                {"required": ["right"], "properties": {"right": {"enum": ["b"]}}}
            ]
        }));

        add_union_discriminators(&mut document);
        assert!(document.get("discriminator").is_none());
    }
}
