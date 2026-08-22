use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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

/// Operations included in the browser proxy document. Keep this table sorted.
pub(crate) const PROXY_OPERATIONS: &[&str] = &[
    "abort_upload",
    "complete_upload",
    "create_commit",
    "create_download",
    "create_upload",
    "get_capabilities",
    "get_file_bytes",
    "get_path_entry",
    "get_upload",
    "grep",
    "list_changes",
    "list_file_revisions",
    "list_path_entries",
    "list_trash",
    "put_upload_content",
    "sign_upload_parts",
];

/// Retry behavior for generated SDKs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryClass {
    Safe,
    Replay,
    NewAttempt,
}

/// Cursor list operations supported by generated SDK pagination. Keep this table sorted.
///
/// `list_changes` is excluded because the pinned Go generator cannot combine
/// its required `after_seq` request value with its optional `next_after_seq`
/// response value.
pub(crate) const PAGINATION_OPERATIONS: &[&str] = &[
    "grep",
    "list_checkpoints",
    "list_file_revisions",
    "list_file_revisions_by_inode",
    "list_path_entries",
    "list_trash",
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
    #[error("OpenAPI pagination operation `{operation_id}` has no `cursor` query parameter")]
    MissingPaginationCursorParameter { operation_id: String },
    #[error("OpenAPI pagination operation `{operation_id}` has no 200 response component schema")]
    MissingPaginationResponseSchema { operation_id: String },
    #[error("OpenAPI pagination operation `{operation_id}` response has no `{property}` property")]
    MissingPaginationResponseProperty {
        operation_id: String,
        property: String,
    },
    #[error(
        "OpenAPI pagination operation `{operation_id}` response: expected exactly one array property"
    )]
    InvalidPaginationArrayProperties { operation_id: String },
    #[error(
        "OpenAPI operation `{operation_id}` has a `cursor` query parameter but no pagination metadata entry"
    )]
    MissingPaginationMetadata { operation_id: String },
    #[error("OpenAPI pagination metadata cannot read `{location}`")]
    InvalidPaginationDocument { location: &'static str },
    #[error("OpenAPI path item `{path}` is not an object")]
    InvalidPaginationPathItem { path: String },
    #[error("OpenAPI operation `{method} {path}` is not an object or has no operation ID")]
    InvalidPaginationOperation { method: &'static str, path: String },
    #[error("OpenAPI union variant `{schema_name}` conflicts with an existing component schema")]
    UnionVariantSchemaCollision { schema_name: String },
    #[error("OpenAPI composite `{schema_name}` combines two discriminated unions")]
    UnionCompositeHasTwoUnions { schema_name: String },
    #[error("OpenAPI composite `{schema_name}` defines property `{property}` twice")]
    UnionCompositeDuplicateProperty {
        schema_name: String,
        property: String,
    },
    #[error(
        "OpenAPI union `{union_name}` is flattened into `{schema_name}` and referenced elsewhere"
    )]
    SharedUnionComposite {
        schema_name: String,
        union_name: String,
    },
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

/// Retry class for each public operation.
pub(crate) const OPERATION_RETRY_CLASSES: &[(&str, RetryClass)] = &[
    ("abort_upload", RetryClass::Safe),
    ("complete_upload", RetryClass::Replay),
    ("create_checkpoint", RetryClass::NewAttempt),
    ("create_commit", RetryClass::Replay),
    ("create_download", RetryClass::Safe),
    ("create_download_by_inode", RetryClass::Safe),
    ("create_namespace", RetryClass::NewAttempt),
    ("create_upload", RetryClass::NewAttempt),
    ("delete_namespace", RetryClass::NewAttempt),
    ("disable_grep_index", RetryClass::Safe),
    ("enable_grep_index", RetryClass::Safe),
    ("fork_namespace", RetryClass::NewAttempt),
    ("gc_grep_index", RetryClass::NewAttempt),
    ("get_capabilities", RetryClass::Safe),
    ("get_file_bytes", RetryClass::Safe),
    ("get_file_revision_bytes_by_inode", RetryClass::Safe),
    ("get_grep_index", RetryClass::Safe),
    ("get_health", RetryClass::Safe),
    ("get_inode", RetryClass::Safe),
    ("get_metrics", RetryClass::Safe),
    ("get_namespace", RetryClass::Safe),
    ("get_namespace_diagnostics", RetryClass::Safe),
    ("get_path_entry", RetryClass::Safe),
    ("get_readiness", RetryClass::Safe),
    ("get_upload", RetryClass::Safe),
    ("grep", RetryClass::Safe),
    ("list_changes", RetryClass::Safe),
    ("list_checkpoints", RetryClass::Safe),
    ("list_file_revisions", RetryClass::Safe),
    ("list_file_revisions_by_inode", RetryClass::Safe),
    ("list_path_entries", RetryClass::Safe),
    ("list_trash", RetryClass::Safe),
    ("probe_store", RetryClass::NewAttempt),
    ("put_upload_content", RetryClass::Safe),
    ("release_checkpoint", RetryClass::Safe),
    ("run_maintenance", RetryClass::NewAttempt),
    ("sign_upload_parts", RetryClass::Safe),
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

    fn as_array_mut(&mut self) -> Option<&mut Vec<Self>> {
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
    drop_null_from_response_schemas(&mut document);
    add_union_discriminators(&mut document, &mut Vec::new());
    extract_union_variants(&mut document)?;
    merge_union_composites(&mut document)?;
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
const NAMESPACE_ALIAS_PATH_PREFIX: &str = "/v0/namespace-aliases/{namespace_alias}";

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
        Value::String("API for browser clients that access namespaces by alias.".to_owned()),
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

            let operation_id = value
                .get("operationId")
                .and_then(Value::as_str)
                .expect("OpenAPI operation has no operationId");
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
            operation
                .as_object_mut()
                .expect("OpenAPI operation is not an object")
                .shift_remove("security");
            if path.starts_with(NAMESPACE_PATH_PREFIX)
                && rewrite_namespace_parameters(&mut operation) == 0
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
        parameter.insert(
            "name".to_owned(),
            Value::String("namespace_alias".to_owned()),
        );
        parameter.insert(
            "description".to_owned(),
            Value::String("Application namespace alias".to_owned()),
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
    let paths = document
        .as_object_mut()
        .and_then(|document| document.get_mut("paths"))
        .and_then(Value::as_object_mut)
        .expect("OpenAPI document has no paths object");

    for path_item in paths.values_mut() {
        let path_item = path_item
            .as_object_mut()
            .expect("OpenAPI path item is not an object");
        for &method in HTTP_METHODS {
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
    let pagination_fields = validate_pagination_metadata(document)?;

    let Some(paths) = document
        .as_object_mut()
        .and_then(|document| document.get_mut("paths"))
        .and_then(Value::as_object_mut)
    else {
        return Ok(());
    };

    for path_item in paths.values_mut() {
        let Some(path_item) = path_item.as_object_mut() else {
            continue;
        };
        for &method in HTTP_METHODS {
            let Some(operation) = path_item.get_mut(method) else {
                continue;
            };
            let Some(operation) = operation.as_object_mut() else {
                continue;
            };
            let Some(operation_id) = operation.get("operationId").and_then(Value::as_str) else {
                continue;
            };
            let Some(results_property) = pagination_fields.get(operation_id) else {
                continue;
            };

            let mut extension = Map::new();
            extension.insert(
                "cursor".to_owned(),
                Value::String("$request.cursor".to_owned()),
            );
            extension.insert(
                "next_cursor".to_owned(),
                Value::String("$response.next_cursor".to_owned()),
            );
            extension.insert(
                "results".to_owned(),
                Value::String(format!("$response.{results_property}")),
            );
            operation.insert("x-fern-pagination".to_owned(), Value::Object(extension));
        }
    }

    Ok(())
}

fn validate_pagination_metadata(
    document: &Value,
) -> Result<BTreeMap<String, String>, OpenapiPostprocessError> {
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
    let mut pagination_fields = BTreeMap::new();

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
            let has_cursor = has_query_parameter(operation, "cursor");
            if !PAGINATION_OPERATIONS.contains(&operation_id) {
                if has_cursor {
                    return Err(OpenapiPostprocessError::MissingPaginationMetadata {
                        operation_id: operation_id.to_owned(),
                    });
                }
                continue;
            }

            if !has_cursor {
                return Err(OpenapiPostprocessError::MissingPaginationCursorParameter {
                    operation_id: operation_id.to_owned(),
                });
            }
            let results_property = validate_pagination_response(schemas, operation, operation_id)?;
            pagination_fields.insert(operation_id.to_owned(), results_property);
        }
    }

    for &operation_id in PAGINATION_OPERATIONS {
        if !pagination_fields.contains_key(operation_id) {
            return Err(OpenapiPostprocessError::MissingPaginationOperation {
                operation_id: operation_id.to_owned(),
            });
        }
    }

    Ok(pagination_fields)
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

/// Planned rewrite for an `allOf` that contains a discriminated union.
struct UnionComposite {
    composite_name: String,
    union_name: String,
    /// Variant references for the replacement `oneOf`.
    one_of: Vec<Value>,
    /// Component names for the variants that receive envelope fields.
    variant_names: Vec<String>,
    discriminator: Value,
    envelope: UnionEnvelope,
    /// Components that may be removed after the rewrite.
    merged_names: Vec<String>,
}

/// Non-union fields from an `allOf` composite.
#[derive(Default)]
struct UnionEnvelope {
    properties: Map,
    required: Vec<String>,
}

impl UnionEnvelope {
    /// Adds one composite member's fields in document order.
    fn extend(
        &mut self,
        composite_name: &str,
        member: &Value,
    ) -> Result<(), OpenapiPostprocessError> {
        if let Some(properties) = member.get("properties").and_then(Value::as_object) {
            for (name, schema) in properties {
                insert_new_property(composite_name, &mut self.properties, name, schema.clone())?;
            }
        }
        for name in required_names(member) {
            if !self.required.contains(&name) {
                self.required.push(name);
            }
        }
        Ok(())
    }
}

/// Rewrites `allOf: [union, envelope]` as a top-level discriminated `oneOf`.
/// Each variant receives the envelope fields. This preserves the wire schema
/// while avoiding generator bugs that discard either half of `allOf`.
fn merge_union_composites(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let schemas = document
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .expect("OpenAPI document has no component schemas object");
    let mut composites = Vec::new();

    for (composite_name, composite) in schemas {
        if let Some(composite) = plan_union_composite(schemas, composite_name, composite)? {
            composites.push(composite);
        }
    }
    if composites.is_empty() {
        return Ok(());
    }
    reject_shared_unions(document, &composites)?;

    let mut merged_names = BTreeSet::new();
    let schemas = document
        .as_object_mut()
        .and_then(|document| document.get_mut("components"))
        .and_then(Value::as_object_mut)
        .and_then(|components| components.get_mut("schemas"))
        .and_then(Value::as_object_mut)
        .expect("OpenAPI document has no component schemas object");

    for composite in composites {
        for variant_name in &composite.variant_names {
            let variant = schemas
                .get_mut(variant_name)
                .expect("OpenAPI union variant component is missing");
            merge_envelope(&composite.composite_name, variant, &composite.envelope)?;
        }

        let schema = schemas
            .get_mut(&composite.composite_name)
            .and_then(Value::as_object_mut)
            .expect("OpenAPI composite component is not an object");
        for field in ["allOf", "properties", "required", "type"] {
            schema.shift_remove(field);
        }
        schema.insert("oneOf".to_owned(), Value::Array(composite.one_of));
        schema.insert("discriminator".to_owned(), composite.discriminator);
        merged_names.extend(composite.merged_names);
    }

    remove_unreferenced_components(document, &merged_names);
    Ok(())
}

/// Plans a union-composite rewrite, or returns `None` for other schemas.
fn plan_union_composite(
    schemas: &Map,
    composite_name: &str,
    composite: &Value,
) -> Result<Option<UnionComposite>, OpenapiPostprocessError> {
    let Some(members) = composite.get("allOf").and_then(Value::as_array) else {
        return Ok(None);
    };
    let mut union = None;
    let mut envelope = UnionEnvelope::default();
    let mut merged_names = Vec::new();

    for member in members {
        let referenced = member
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(component_schema_name)
            .and_then(|name| Some((schemas.get(&name)?, name)));
        match referenced {
            Some((schema, name)) if is_discriminated_union(schema) => {
                if union.is_some() {
                    return Err(OpenapiPostprocessError::UnionCompositeHasTwoUnions {
                        schema_name: composite_name.to_owned(),
                    });
                }
                merged_names.push(name.clone());
                union = Some((schema, name));
            }
            Some((schema, name)) => {
                merged_names.push(name);
                envelope.extend(composite_name, schema)?;
            }
            None => envelope.extend(composite_name, member)?,
        }
    }

    let Some((union, union_name)) = union else {
        return Ok(None);
    };
    let one_of = union
        .get("oneOf")
        .and_then(Value::as_array)
        .expect("discriminated union has a oneOf array")
        .clone();
    let variant_names = one_of
        .iter()
        .map(|variant| {
            variant
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(component_schema_name)
                .expect("extracted union variant references a component schema")
        })
        .collect();

    Ok(Some(UnionComposite {
        composite_name: composite_name.to_owned(),
        union_name,
        one_of,
        variant_names,
        discriminator: union
            .get("discriminator")
            .expect("discriminated union has a discriminator")
            .clone(),
        envelope,
        merged_names,
    }))
}

/// Returns whether a schema is a discriminated `oneOf`.
fn is_discriminated_union(schema: &Value) -> bool {
    schema.get("oneOf").and_then(Value::as_array).is_some() && schema.get("discriminator").is_some()
}

/// Adds the envelope fields to one union variant.
fn merge_envelope(
    composite_name: &str,
    variant: &mut Value,
    envelope: &UnionEnvelope,
) -> Result<(), OpenapiPostprocessError> {
    let variant = variant
        .as_object_mut()
        .expect("OpenAPI union variant is not an object");
    let properties = variant
        .entry("properties".to_owned())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .expect("OpenAPI union variant properties is not an object");
    for (name, schema) in &envelope.properties {
        insert_new_property(composite_name, properties, name, schema.clone())?;
    }

    let required = variant
        .entry("required".to_owned())
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .expect("OpenAPI union variant required is not an array");
    for name in &envelope.required {
        if !required.iter().any(|value| value.as_str() == Some(name)) {
            required.push(Value::String(name.clone()));
        }
    }
    Ok(())
}

fn insert_new_property(
    composite_name: &str,
    properties: &mut Map,
    name: &str,
    schema: Value,
) -> Result<(), OpenapiPostprocessError> {
    if properties.contains_key(name) {
        return Err(OpenapiPostprocessError::UnionCompositeDuplicateProperty {
            schema_name: composite_name.to_owned(),
            property: name.to_owned(),
        });
    }
    properties.insert(name.to_owned(), schema);
    Ok(())
}

/// Rejects a union referenced outside the composite being rewritten. Reusing
/// its modified variants elsewhere would add fields that are not present there.
fn reject_shared_unions(
    document: &Value,
    composites: &[UnionComposite],
) -> Result<(), OpenapiPostprocessError> {
    let mut references = Vec::new();
    collect_component_references(document, &mut references);

    for composite in composites {
        let reference = component_schema_reference(&composite.union_name);
        let readers = references
            .iter()
            .filter(|candidate| **candidate == reference)
            .count();
        if readers > 1 {
            return Err(OpenapiPostprocessError::SharedUnionComposite {
                schema_name: composite.composite_name.clone(),
                union_name: composite.union_name.clone(),
            });
        }
    }
    Ok(())
}

/// Removes merged components that are no longer referenced.
fn remove_unreferenced_components(document: &mut Value, merged_names: &BTreeSet<String>) {
    let mut references = Vec::new();
    collect_component_references(document, &mut references);
    let referenced = references.into_iter().collect::<BTreeSet<_>>();

    let schemas = document
        .as_object_mut()
        .and_then(|document| document.get_mut("components"))
        .and_then(Value::as_object_mut)
        .and_then(|components| components.get_mut("schemas"))
        .and_then(Value::as_object_mut)
        .expect("OpenAPI document has no component schemas object");
    schemas.retain(|name, _| {
        !merged_names.contains(name) || referenced.contains(&component_schema_reference(name))
    });
}

/// Returns a schema's required property names in document order.
fn required_names(schema: &Value) -> Vec<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn fixed_discriminator_value(variant: &Value, property_name: &str) -> Option<String> {
    fixed_required_properties(variant)?
        .into_iter()
        .find_map(|(name, value)| (name == property_name).then_some(value))
}

fn component_schema_for_reference<'a>(schemas: &'a Map, reference: &str) -> Option<&'a Value> {
    schemas.get(&component_schema_name(reference)?)
}

fn component_schema_name(reference: &str) -> Option<String> {
    reference
        .strip_prefix("#/components/schemas/")
        .map(decode_json_pointer_segment)
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

fn normalize_optional_schemas(value: &mut Value) {
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
                normalize_optional_schemas(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                normalize_optional_schemas(child);
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

/// Removes `null` from optional fields used in responses.
///
/// The server omits absent response fields. Request-only schemas keep `null`
/// because serde accepts it. Shared request and response schemas use the
/// response rule while keeping the field optional.
fn drop_null_from_response_schemas(document: &mut Value) {
    let mut pending = Vec::new();
    collect_response_references(document, &mut pending);
    let mut reachable = BTreeSet::new();

    while let Some(reference) = pending.pop() {
        let Some((component_type, component_name)) = component_reference_parts(&reference) else {
            continue;
        };
        if !reachable.insert((component_type.clone(), component_name.clone())) {
            continue;
        }
        let Some(component) = document
            .get("components")
            .and_then(|components| components.get(&component_type))
            .and_then(Value::as_object)
            .and_then(|entries| entries.get(&component_name))
        else {
            continue;
        };
        collect_component_references(component, &mut pending);
    }

    for responses in operation_responses_mut(document) {
        drop_null_from_optional_properties(responses);
    }

    let Some(components) = document
        .as_object_mut()
        .and_then(|document| document.get_mut("components"))
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    for (component_type, entries) in components {
        let Some(entries) = entries.as_object_mut() else {
            continue;
        };
        for (component_name, component) in entries {
            if reachable.contains(&(component_type.clone(), component_name.clone())) {
                drop_null_from_optional_properties(component);
            }
        }
    }
}

/// Collects component references from every operation response.
fn collect_response_references(document: &Value, references: &mut Vec<String>) {
    let Some(paths) = document.get("paths").and_then(Value::as_object) else {
        return;
    };

    for path_item in paths.values() {
        let Some(path_item) = path_item.as_object() else {
            continue;
        };
        for method in HTTP_METHODS {
            let Some(responses) = path_item
                .get(*method)
                .and_then(|operation| operation.get("responses"))
            else {
                continue;
            };
            collect_component_references(responses, references);
        }
    }
}

/// Returns every operation's `responses` object for in-place updates.
fn operation_responses_mut(document: &mut Value) -> Vec<&mut Value> {
    let Some(paths) = document
        .as_object_mut()
        .and_then(|document| document.get_mut("paths"))
        .and_then(Value::as_object_mut)
    else {
        return Vec::new();
    };

    paths
        .values_mut()
        .filter_map(Value::as_object_mut)
        .flat_map(|path_item| {
            path_item
                .iter_mut()
                .filter(|(method, _)| HTTP_METHODS.contains(&method.as_str()))
                .filter_map(|(_, operation)| {
                    operation
                        .as_object_mut()
                        .and_then(|operation| operation.get_mut("responses"))
                })
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
                        .expect("schema with a type array is an object");
                    schema.insert("type".to_owned(), replacement);
                    schema.shift_remove("nullable");
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

fn add_union_discriminators(value: &mut Value, path: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            if let Some(discriminator) = discriminator_for(object, path) {
                object.insert("discriminator".to_owned(), discriminator);
            }

            for (name, child) in object {
                path.push(name.clone());
                add_union_discriminators(child, path);
                path.pop();
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter_mut().enumerate() {
                path.push(index.to_string());
                add_union_discriminators(child, path);
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

        add_union_discriminators(&mut document, &mut Vec::new());
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
        add_union_discriminators(&mut document, &mut Vec::new());
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

        add_union_discriminators(&mut document, &mut Vec::new());
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

        add_union_discriminators(&mut document, &mut Vec::new());
        extract_union_variants(&mut document).expect("reuse identical component schema");
        assert_eq!(
            unordered(&document).pointer("/components/schemas/Choice/oneOf/0/$ref"),
            Some(&serde_json::json!("#/components/schemas/ChoiceFirst"))
        );
    }

    /// Extracts union variants and then rewrites their composites.
    fn extract_and_merge(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
        add_union_discriminators(document, &mut Vec::new());
        extract_union_variants(document).expect("extract union variants");
        merge_union_composites(document)
    }

    #[test]
    fn merges_an_inline_envelope_into_every_union_variant() {
        let mut document = ordered(serde_json::json!({
            "components": {
                "schemas": {
                    "Session": {
                        "allOf": [
                            {"$ref": "#/components/schemas/SessionStatus"},
                            {
                                "type": "object",
                                "required": ["upload_id"],
                                "properties": {"upload_id": {"type": "string"}}
                            }
                        ],
                        "description": "One upload session."
                    },
                    "SessionStatus": {
                        "oneOf": [
                            {
                                "title": "SessionStatusOpen",
                                "required": ["status"],
                                "properties": {"status": {"const": "open"}}
                            },
                            {
                                "title": "SessionStatusDone",
                                "required": ["status"],
                                "properties": {"status": {"const": "done"}}
                            }
                        ]
                    }
                }
            }
        }));

        extract_and_merge(&mut document).expect("merge union composites");
        let actual = unordered(&document);
        assert_eq!(
            actual.pointer("/components/schemas/Session"),
            Some(&serde_json::json!({
                "description": "One upload session.",
                "oneOf": [
                    {"$ref": "#/components/schemas/SessionStatusOpen"},
                    {"$ref": "#/components/schemas/SessionStatusDone"}
                ],
                "discriminator": {
                    "propertyName": "status",
                    "mapping": {
                        "open": "#/components/schemas/SessionStatusOpen",
                        "done": "#/components/schemas/SessionStatusDone"
                    }
                }
            }))
        );
        assert_eq!(
            actual.pointer("/components/schemas/SessionStatusOpen"),
            Some(&serde_json::json!({
                "required": ["status", "upload_id"],
                "properties": {
                    "status": {"const": "open"},
                    "upload_id": {"type": "string"}
                }
            }))
        );
        assert_eq!(
            actual.pointer("/components/schemas/SessionStatusDone/properties/upload_id"),
            Some(&serde_json::json!({"type": "string"}))
        );
        assert_eq!(
            actual.pointer("/components/schemas/SessionStatus"),
            None,
            "the union component is unreferenced after the composite is rewritten"
        );
    }

    #[test]
    fn merges_a_referenced_envelope_and_drops_its_component() {
        let mut document = ordered(serde_json::json!({
            "components": {
                "schemas": {
                    // The union does not have to come first.
                    "Entry": {
                        "allOf": [
                            {"$ref": "#/components/schemas/EntryAttributes"},
                            {"$ref": "#/components/schemas/EntryKind"},
                            {
                                "type": "object",
                                "required": ["path"],
                                "properties": {"path": {"type": "string"}}
                            }
                        ]
                    },
                    "EntryKind": {
                        "oneOf": [{
                            "title": "EntryDirectory",
                            "required": ["inode_kind"],
                            "properties": {"inode_kind": {"const": "dir"}}
                        }]
                    },
                    "EntryAttributes": {
                        "type": "object",
                        "properties": {"attributes": {"type": "string"}}
                    }
                }
            }
        }));

        extract_and_merge(&mut document).expect("merge union composites");
        let actual = unordered(&document);
        assert_eq!(
            actual.pointer("/components/schemas/EntryDirectory"),
            Some(&serde_json::json!({
                "required": ["inode_kind", "path"],
                "properties": {
                    "inode_kind": {"const": "dir"},
                    "attributes": {"type": "string"},
                    "path": {"type": "string"}
                }
            }))
        );
        assert_eq!(actual.pointer("/components/schemas/EntryKind"), None);
        assert_eq!(actual.pointer("/components/schemas/EntryAttributes"), None);
    }

    #[test]
    fn leaves_a_composite_without_a_union_unchanged() {
        let mut document = ordered(serde_json::json!({
            "components": {
                "schemas": {
                    "CreateCheckpointResponse": {
                        "allOf": [
                            {"$ref": "#/components/schemas/Checkpoint"},
                            {
                                "type": "object",
                                "required": ["namespace_id"],
                                "properties": {"namespace_id": {"type": "string"}}
                            }
                        ]
                    },
                    "Checkpoint": {
                        "type": "object",
                        "required": ["checkpoint_id"],
                        "properties": {"checkpoint_id": {"type": "string"}}
                    }
                }
            }
        }));
        let before = document.clone();

        merge_union_composites(&mut document).expect("leave the composite alone");
        assert_eq!(document, before);
    }

    #[test]
    fn rejects_a_composite_that_redefines_a_variant_property() {
        let mut document = ordered(serde_json::json!({
            "components": {
                "schemas": {
                    "Session": {
                        "allOf": [
                            {"$ref": "#/components/schemas/SessionStatus"},
                            {
                                "type": "object",
                                "required": ["status"],
                                "properties": {"status": {"type": "string"}}
                            }
                        ]
                    },
                    "SessionStatus": {
                        "oneOf": [{
                            "title": "SessionStatusOpen",
                            "required": ["status"],
                            "properties": {"status": {"const": "open"}}
                        }]
                    }
                }
            }
        }));

        let error = extract_and_merge(&mut document)
            .expect_err("a redefined property should fail generation");
        assert!(matches!(
            error,
            OpenapiPostprocessError::UnionCompositeDuplicateProperty {
                schema_name,
                property,
            } if schema_name == "Session" && property == "status"
        ));
    }

    #[test]
    fn rejects_a_union_a_second_schema_also_reads() {
        let mut document = ordered(serde_json::json!({
            "components": {
                "schemas": {
                    "Session": {
                        "allOf": [
                            {"$ref": "#/components/schemas/SessionStatus"},
                            {
                                "type": "object",
                                "required": ["upload_id"],
                                "properties": {"upload_id": {"type": "string"}}
                            }
                        ]
                    },
                    "SessionStatus": {
                        "oneOf": [{
                            "title": "SessionStatusOpen",
                            "required": ["status"],
                            "properties": {"status": {"const": "open"}}
                        }]
                    },
                    "SessionAudit": {
                        "type": "object",
                        "properties": {"status": {"$ref": "#/components/schemas/SessionStatus"}}
                    }
                }
            }
        }));

        let error =
            extract_and_merge(&mut document).expect_err("a shared union should fail generation");
        assert!(matches!(
            error,
            OpenapiPostprocessError::SharedUnionComposite {
                schema_name,
                union_name,
            } if schema_name == "Session" && union_name == "SessionStatus"
        ));
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
        let expected_operation_id = PAGINATION_OPERATIONS[0];
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
            OpenapiPostprocessError::MissingPaginationMetadata { operation_id }
                if operation_id == "future_list"
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

    /// One request schema, one response schema, and one schema both reach.
    fn request_and_response_document() -> Value {
        ordered(serde_json::json!({
            "paths": {
                "/things": {
                    "post": {
                        "requestBody": {
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ThingRequest"}
                                }
                            }
                        },
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {
                                        "schema": {"$ref": "#/components/schemas/ThingResponse"}
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "components": {
                "schemas": {
                    "Shared": {
                        "type": "object",
                        "properties": {
                            "label": {"type": ["string", "null"]}
                        }
                    },
                    "ThingRequest": {
                        "type": "object",
                        "properties": {
                            "cursor": {"type": ["string", "null"]},
                            "shared": {"$ref": "#/components/schemas/Shared"}
                        }
                    },
                    "ThingResponse": {
                        "type": "object",
                        "required": ["count"],
                        "properties": {
                            "count": {"type": "integer"},
                            "next_cursor": {"type": ["string", "null"], "nullable": true},
                            "shared": {"$ref": "#/components/schemas/Shared"}
                        }
                    }
                }
            }
        }))
    }

    #[test]
    fn drops_the_null_type_from_an_optional_response_property() {
        let mut document = request_and_response_document();

        drop_null_from_response_schemas(&mut document);
        let actual = unordered(&document);
        assert_eq!(
            actual.pointer("/components/schemas/ThingResponse/properties/next_cursor"),
            Some(&serde_json::json!({"type": "string"}))
        );
    }

    #[test]
    fn drops_the_null_type_from_a_schema_a_request_and_a_response_share() {
        let mut document = request_and_response_document();

        drop_null_from_response_schemas(&mut document);
        let actual = unordered(&document);
        assert_eq!(
            actual.pointer("/components/schemas/Shared/properties/label"),
            Some(&serde_json::json!({"type": "string"}))
        );
    }

    #[test]
    fn keeps_the_null_type_on_a_request_only_property() {
        let mut document = request_and_response_document();

        drop_null_from_response_schemas(&mut document);
        let actual = unordered(&document);
        assert_eq!(
            actual.pointer("/components/schemas/ThingRequest/properties/cursor"),
            Some(&serde_json::json!({"type": ["string", "null"]}))
        );
    }

    #[test]
    fn rewrites_only_optional_response_properties() {
        let mut document = ordered(serde_json::json!({
            "paths": {
                "/things": {
                    "get": {
                        "responses": {
                            "200": {
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "type": "object",
                                            "required": ["marker"],
                                            "properties": {
                                                "marker": {"type": ["string", "null"]}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }));

        drop_null_from_response_schemas(&mut document);
        let actual = unordered(&document);
        assert_eq!(
            actual.pointer(
                "/paths/~1things/get/responses/200/content/application~1json/schema/properties/marker"
            ),
            Some(&serde_json::json!({"type": ["string", "null"]}))
        );
    }

    #[test]
    fn leaves_a_one_of_without_one_shared_fixed_tag_unchanged() {
        let mut document = ordered(serde_json::json!({
            "oneOf": [
                {"required": ["left"], "properties": {"left": {"enum": ["a"]}}},
                {"required": ["right"], "properties": {"right": {"enum": ["b"]}}}
            ]
        }));

        add_union_discriminators(&mut document, &mut Vec::new());
        assert!(document.get("discriminator").is_none());
    }
}
