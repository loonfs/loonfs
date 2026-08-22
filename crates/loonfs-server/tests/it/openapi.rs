#![allow(clippy::panic)]

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[path = "../../src/bin/loonfs-openapi/openapi_postprocess.rs"]
mod openapi_postprocess;

const OPENAPI_JSON_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/specs/openapi.json");
const PROXY_OPENAPI_JSON_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/specs/openapi-proxy.json"
);
const OPENAPI_PATH_SOURCES: &[&str] = &[
    include_str!("../../src/http/mod.rs"),
    include_str!("../../src/http/handlers_downloads.rs"),
    include_str!("../../src/http/handlers_filesystem.rs"),
    include_str!("../../src/http/handlers_inodes.rs"),
    include_str!("../../src/http/handlers_namespace.rs"),
    include_str!("../../src/http/handlers_query.rs"),
    include_str!("../../src/http/handlers_store.rs"),
    include_str!("../../src/http/handlers_uploads.rs"),
];

#[test]
fn openapi_static_files_are_current() {
    let (mut full, mut proxy) =
        openapi_postprocess::openapi_documents_pretty(&loonfs_server::openapi_document())
            .expect("generate full and proxy OpenAPI JSON");
    full.push('\n');
    proxy.push('\n');

    assert_eq!(
        full,
        std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
        "docs/specs/openapi.json is stale; rerun `cargo run -p loonfs-server --features openapi --bin loonfs-openapi -- docs/specs/openapi.json docs/specs/openapi-proxy.json`"
    );
    assert_eq!(
        proxy,
        std::fs::read_to_string(PROXY_OPENAPI_JSON_PATH)
            .expect("read static proxy openapi json"),
        "docs/specs/openapi-proxy.json is stale; rerun `cargo run -p loonfs-server --features openapi --bin loonfs-openapi -- docs/specs/openapi.json docs/specs/openapi-proxy.json`"
    );
}

#[test]
fn every_one_of_is_a_discriminated_non_null_union() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let mut unions = Vec::new();
    collect_one_of_objects(&spec, &mut unions);
    let schemas = spec
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .expect("openapi schemas object");
    assert!(
        !unions.is_empty(),
        "the generated document has no oneOf schemas"
    );

    for union in unions {
        let variants = union
            .get("oneOf")
            .and_then(Value::as_array)
            .expect("collected object has a oneOf array");
        assert!(
            variants
                .iter()
                .all(|variant| variant.get("type").and_then(Value::as_str) != Some("null")),
            "oneOf contains a null alternative: {union:?}"
        );

        let discriminator = union
            .get("discriminator")
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("oneOf has no discriminator: {union:?}"));
        let property_name = discriminator
            .get("propertyName")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("oneOf discriminator has no propertyName: {union:?}"));
        let mapping = discriminator
            .get("mapping")
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("oneOf discriminator has no mapping: {union:?}"));
        assert_eq!(
            mapping.len(),
            variants.len(),
            "oneOf discriminator mapping is incomplete: {union:?}"
        );
        for variant in variants {
            let variant_ref = variant
                .get("$ref")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("discriminated oneOf member is not a $ref: {variant:?}"));
            let variant = component_schema_for_ref(schemas, variant_ref).unwrap_or_else(|| {
                panic!("discriminated oneOf member does not reference a component: {variant_ref}")
            });
            let tag_schema = variant
                .get("properties")
                .and_then(Value::as_object)
                .and_then(|properties| properties.get(property_name))
                .unwrap_or_else(|| {
                    panic!("oneOf variant has no discriminator property: {variant:?}")
                });
            let tag = tag_schema
                .get("const")
                .and_then(Value::as_str)
                .or_else(|| {
                    let values = tag_schema.get("enum")?.as_array()?;
                    let [value] = values.as_slice() else {
                        return None;
                    };
                    value.as_str()
                })
                .unwrap_or_else(|| {
                    panic!("oneOf variant has no fixed discriminator value: {variant:?}")
                });
            assert_eq!(
                mapping.get(tag).and_then(Value::as_str),
                Some(variant_ref),
                "oneOf discriminator mapping does not reference the `{tag}` variant: {union:?}"
            );
        }
        for mapping_ref in mapping.values().map(|value| {
            value
                .as_str()
                .unwrap_or_else(|| panic!("oneOf discriminator mapping is not a $ref: {union:?}"))
        }) {
            assert!(
                component_schema_for_ref(schemas, mapping_ref).is_some(),
                "oneOf discriminator mapping does not reference a component: {mapping_ref}"
            );
        }
    }
}

/// Checks success and error response schemas, including components used only
/// by `ApiError` and `ErrorDetails`.
#[test]
fn no_schema_a_response_reaches_admits_null() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let mut visited = BTreeSet::new();
    let mut properties = 0usize;

    for (location, response) in documented_responses(&spec) {
        assert_no_null_property(&spec, response, &location, &mut visited, &mut properties);
    }

    for schema_name in ["ApiError", "ErrorDetails", "GcResponse", "GrepMatch"] {
        assert!(
            visited.contains(&format!("#/components/schemas/{schema_name}")),
            "the walk never reached `{schema_name}`"
        );
    }
    assert!(
        properties > 0,
        "the walk reached no response schema properties"
    );
}

/// Fields that the server always includes in these response types.
const ALWAYS_SERIALIZED_RESPONSE_FIELDS: &[(&str, &str)] = &[
    ("GcResponse", "budget_exhausted"),
    ("GcResponse", "content_reclamation_deferred"),
    ("GcResponse", "deleted_content_objects"),
    ("GcResponse", "deleted_upload_sessions"),
    ("GcResponse", "released_expired_checkpoints"),
    ("GcResponse", "released_missing_basis_checkpoints"),
    ("GcResponse", "retained"),
    ("GrepMatch", "line_truncated"),
];

#[test]
fn always_serialized_response_fields_are_required() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let schemas = spec
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .expect("openapi schemas object");

    for (schema_name, field) in ALWAYS_SERIALIZED_RESPONSE_FIELDS {
        let schema = schemas
            .get(*schema_name)
            .unwrap_or_else(|| panic!("openapi document has no `{schema_name}` schema"));
        assert!(
            required_fields(schema).contains(field),
            "`{schema_name}.{field}` is written on every response but the document marks it optional"
        );
    }
}

#[test]
fn openapi_documents_current_server_paths() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths object");

    for (path, method) in [
        ("/health", "get"),
        ("/readiness", "get"),
        ("/metrics", "get"),
        ("/v0/capabilities", "get"),
        ("/v0/namespaces", "post"),
        ("/v0/namespaces/{namespace_id}", "get"),
        ("/v0/namespaces/{namespace_id}", "delete"),
        ("/v0/namespaces/{namespace_id}/forks", "post"),
        ("/v0/admin/namespaces/{namespace_id}/diagnostics", "get"),
        ("/v0/namespaces/{namespace_id}/filesystem/entries", "get"),
        ("/v0/namespaces/{namespace_id}/filesystem/entry", "get"),
        ("/v0/namespaces/{namespace_id}/filesystem/content", "get"),
        ("/v0/namespaces/{namespace_id}/filesystem/downloads", "post"),
        ("/v0/namespaces/{namespace_id}/filesystem/revisions", "get"),
        ("/v0/namespaces/{namespace_id}/inodes/{inode_id}", "get"),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            "get",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
            "get",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
            "post",
        ),
        ("/v0/namespaces/{namespace_id}/commits", "post"),
        ("/v0/namespaces/{namespace_id}/uploads", "post"),
        (
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
            "put",
        ),
        (
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete",
            "post",
        ),
        ("/v0/namespaces/{namespace_id}/changes", "get"),
        ("/v0/admin/namespaces/{namespace_id}/checkpoints", "post"),
        ("/v0/admin/namespaces/{namespace_id}/checkpoints", "get"),
        (
            "/v0/admin/namespaces/{namespace_id}/checkpoints/{checkpoint_id}/release",
            "post",
        ),
        (
            "/v0/admin/namespaces/{namespace_id}/maintenance/run",
            "post",
        ),
        (
            "/v0/admin/namespaces/{namespace_id}/grep/index/enable",
            "post",
        ),
        (
            "/v0/admin/namespaces/{namespace_id}/grep/index/disable",
            "post",
        ),
        ("/v0/admin/namespaces/{namespace_id}/grep/index/gc", "post"),
        ("/v0/admin/store/probe", "post"),
    ] {
        assert_path_method(paths, path, method);
    }

    assert!(!paths.contains_key("/openapi.json"));
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/filesystem/entries",
        "get",
        &["path", "limit", "cursor"],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/changes",
        "get",
        &["after_seq", "limit"],
    );
    assert_query_params(
        paths,
        "/v0/admin/namespaces/{namespace_id}/checkpoints",
        "get",
        &["limit", "cursor"],
    );

    let mut namespace_scoped_operations = 0;
    for (path, path_item) in paths {
        if !path.contains("/namespaces/{") {
            continue;
        }
        assert!(
            path.contains("{namespace_id}"),
            "namespace-scoped OpenAPI path uses the retired parameter name: `{path}`"
        );
        assert!(!path.contains("{namespace}"));
        for method in ["get", "post", "put", "delete", "patch"] {
            let Some(operation) = path_item.get(method) else {
                continue;
            };
            namespace_scoped_operations += 1;
            let path_parameter_names = operation
                .get("parameters")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|parameter| parameter.get("in").and_then(Value::as_str) == Some("path"))
                .filter_map(|parameter| parameter.get("name").and_then(Value::as_str))
                .collect::<BTreeSet<_>>();
            assert!(
                path_parameter_names.contains("namespace_id"),
                "`{method} {path}` does not declare `namespace_id` as a path parameter"
            );
            assert!(!path_parameter_names.contains("namespace"));
        }
    }
    assert_eq!(namespace_scoped_operations, 31);

    for (path, method, parameter, schema_name) in [
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
            "get",
            "revision_no",
            "RevisionNo",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
            "post",
            "revision_no",
            "RevisionNo",
        ),
    ] {
        assert_path_parameter_schema(paths, path, method, parameter, schema_name);
    }

    for (path, method) in [
        ("/v0/namespaces/{namespace_id}/inodes/{inode_id}", "get"),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            "get",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
            "get",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
            "post",
        ),
    ] {
        let parameter = path_parameter(paths, path, method, "inode_id");
        assert_eq!(
            parameter.pointer("/schema/type").and_then(Value::as_str),
            Some("string")
        );
        assert_eq!(
            parameter.pointer("/schema/pattern").and_then(Value::as_str),
            Some(loonfs_api::public_inode_id::PATTERN)
        );
        assert_eq!(
            parameter.get("example").and_then(Value::as_str),
            Some(loonfs_api::public_inode_id::EXAMPLE)
        );
    }

    for (path, method, operation_id) in [
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}",
            "get",
            "get_inode",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            "get",
            "list_file_revisions_by_inode",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
            "get",
            "get_file_revision_bytes_by_inode",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
            "post",
            "create_download_by_inode",
        ),
    ] {
        let actual = paths
            .get(path)
            .and_then(|path_item| path_item.get(method))
            .and_then(|operation| operation.get("operationId"))
            .and_then(Value::as_str);
        assert_eq!(
            actual,
            Some(operation_id),
            "unexpected operation id for `{method} {path}`"
        );
    }
}

#[test]
fn openapi_operation_ids_match_the_public_registry() {
    const REGISTRY_MESSAGE: &str = "operation IDs define generated SDK method names; update OPERATION_RETRY_CLASSES only when renaming a public method";

    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths object");
    let mut operation_ids = paths
        .values()
        .filter_map(Value::as_object)
        .flat_map(|path_item| {
            ["get", "post", "put", "delete", "patch"]
                .into_iter()
                .filter_map(|method| path_item.get(method))
        })
        .map(|operation| {
            operation
                .get("operationId")
                .and_then(Value::as_str)
                .expect("OpenAPI operation has an operationId")
        })
        .collect::<Vec<_>>();
    let path_macro_count = OPENAPI_PATH_SOURCES
        .iter()
        .map(|source| source.matches("utoipa::path(").count())
        .sum::<usize>();
    let explicit_operation_id_count = OPENAPI_PATH_SOURCES
        .iter()
        .map(|source| source.matches("operation_id = \"").count())
        .sum::<usize>();
    assert_eq!(
        explicit_operation_id_count, path_macro_count,
        "every utoipa path macro must set an explicit operation ID"
    );
    assert_eq!(
        path_macro_count,
        operation_ids.len(),
        "the registered operations and documented path macros must match"
    );
    let unique_operation_ids = operation_ids.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        unique_operation_ids.len(),
        operation_ids.len(),
        "duplicate operationIds found; {REGISTRY_MESSAGE}"
    );

    let registered = openapi_postprocess::OPERATION_RETRY_CLASSES
        .iter()
        .map(|(operation_id, _)| *operation_id)
        .collect::<Vec<_>>();
    operation_ids.sort_unstable();
    assert_eq!(
        operation_ids, registered,
        "operationIds changed; {REGISTRY_MESSAGE}"
    );
}

#[test]
fn proxy_operation_ids_match_the_allowlist() {
    let registered = openapi_postprocess::OPERATION_RETRY_CLASSES
        .iter()
        .map(|(operation_id, _)| *operation_id)
        .collect::<BTreeSet<_>>();
    let allowlisted = openapi_postprocess::PROXY_OPERATIONS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        allowlisted.len(),
        openapi_postprocess::PROXY_OPERATIONS.len(),
        "proxy operation IDs must be unique"
    );
    assert!(
        allowlisted.is_subset(&registered),
        "proxy operation IDs must be registered public operations"
    );
    assert!(
        openapi_postprocess::PROXY_OPERATIONS
            .windows(2)
            .all(|pair| pair[0] < pair[1]),
        "proxy operations must stay sorted"
    );

    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(PROXY_OPENAPI_JSON_PATH).expect("read static proxy openapi json"),
    )
    .expect("parse proxy openapi json");
    let published = spec["paths"]
        .as_object()
        .expect("proxy openapi paths object")
        .values()
        .filter_map(Value::as_object)
        .flat_map(|path_item| {
            ["get", "post", "put", "delete", "patch"]
                .into_iter()
                .filter_map(|method| path_item.get(method))
        })
        .map(|operation| {
            operation["operationId"]
                .as_str()
                .expect("proxy operation has an operation ID")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(published, allowlisted);
}

#[test]
fn proxy_paths_use_namespace_aliases_and_declare_no_security() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(PROXY_OPENAPI_JSON_PATH).expect("read static proxy openapi json"),
    )
    .expect("parse proxy openapi json");
    assert_eq!(spec["info"]["title"], "LoonFS Browser Proxy API");
    assert_eq!(
        spec["info"]["description"],
        "API for browser clients that access namespaces by alias."
    );
    assert!(spec.get("security").is_none());
    assert!(spec.pointer("/components/securitySchemes").is_none());
    assert!(values_named(&spec, "security").next().is_none());

    let tag_names = spec["tags"]
        .as_array()
        .expect("proxy tags array")
        .iter()
        .filter_map(|tag| tag["name"].as_str())
        .collect::<BTreeSet<_>>();
    // `get_capabilities` keeps the `system` tag in the proxy document. The
    // path checks below verify that other system routes are excluded.
    assert!(!tag_names.contains("admin"));
    let referenced_tags = spec["paths"]
        .as_object()
        .expect("proxy openapi paths object")
        .values()
        .flat_map(|path_item| path_item.as_object().expect("proxy path item").values())
        .flat_map(|operation| operation["tags"].as_array().into_iter().flatten())
        .filter_map(|tag| tag.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        tag_names, referenced_tags,
        "every declared proxy tag must be referenced by a retained operation"
    );

    let paths = spec["paths"]
        .as_object()
        .expect("proxy openapi paths object");
    for (path, path_item) in paths {
        assert!(
            !path.contains("namespace_id"),
            "proxy path contains namespace_id: `{path}`"
        );
        assert!(!path.starts_with("/v0/admin/"));
        assert!(!matches!(
            path.as_str(),
            "/health" | "/readiness" | "/metrics"
        ));

        for operation in ["get", "post", "put", "delete", "patch"]
            .into_iter()
            .filter_map(|method| path_item.get(method))
        {
            let operation_id = operation["operationId"]
                .as_str()
                .expect("proxy operation ID");
            if operation_id == "get_capabilities" {
                assert_eq!(path, "/v0/capabilities");
                continue;
            }
            assert!(path.starts_with("/v0/namespace-aliases/{namespace_alias}/"));
            let namespace_alias = operation["parameters"]
                .as_array()
                .expect("proxy operation parameters")
                .iter()
                .find(|parameter| {
                    parameter["in"] == "path" && parameter["name"] == "namespace_alias"
                })
                .expect("proxy operation namespace alias parameter");
            assert_eq!(
                namespace_alias["description"],
                "Application namespace alias"
            );
            assert_eq!(
                namespace_alias["schema"],
                serde_json::json!({"type": "string"})
            );
        }
    }
}

#[test]
fn proxy_components_are_exactly_the_referenced_closure() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(PROXY_OPENAPI_JSON_PATH).expect("read static proxy openapi json"),
    )
    .expect("parse proxy openapi json");
    let components = spec["components"]
        .as_object()
        .expect("proxy components object");
    let referenced = referenced_components(&spec);
    let published = components
        .iter()
        .flat_map(|(component_type, entries)| {
            entries
                .as_object()
                .unwrap_or_else(|| {
                    panic!("proxy component group `{component_type}` must be an object")
                })
                .keys()
                .map(move |component_name| (component_type.clone(), component_name.clone()))
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(published, referenced);

    let schemas = components["schemas"]
        .as_object()
        .expect("proxy schemas object");
    for reference in string_values(&spec)
        .filter_map(Value::as_str)
        .filter_map(|reference| reference.strip_prefix("#/components/schemas/"))
    {
        let schema_name = decode_json_pointer_segment(reference);
        assert!(
            schemas.contains_key(&schema_name),
            "proxy schema reference is missing: `{schema_name}`"
        );
    }
}

#[test]
fn openapi_document_generation_is_byte_stable() {
    let first = openapi_postprocess::openapi_documents_pretty(&loonfs_server::openapi_document())
        .expect("generate full and proxy OpenAPI JSON");
    let second = openapi_postprocess::openapi_documents_pretty(&loonfs_server::openapi_document())
        .expect("generate full and proxy OpenAPI JSON again");
    assert_eq!(first, second);
    assert_eq!(
        openapi_postprocess::proxy_openapi_json_pretty(&first.0)
            .expect("derive proxy OpenAPI JSON again"),
        first.1
    );
}

#[test]
fn every_registered_operation_publishes_its_retry_class() {
    let generated = openapi_postprocess::openapi_json_pretty(&loonfs_server::openapi_document())
        .expect("generate openapi json");
    let spec: Value = serde_json::from_str(&generated).expect("parse generated openapi json");
    let published = operations_by_id(&spec)
        .into_iter()
        .map(|(operation_id, operation)| {
            (
                operation_id,
                operation
                    .get("x-loonfs-retry")
                    .and_then(Value::as_str)
                    .expect("OpenAPI operation has an x-loonfs-retry extension"),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let expected = openapi_postprocess::OPERATION_RETRY_CLASSES
        .iter()
        .map(|(operation_id, retry_class)| (*operation_id, retry_class.as_str()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(published, expected);
}

#[test]
fn cursor_list_operations_publish_the_registered_pagination_metadata() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let operations = operations_by_id(&spec);
    let published = operations
        .iter()
        .filter_map(|(&operation_id, &operation)| {
            operation
                .get("x-fern-pagination")
                .cloned()
                .map(|extension| (operation_id, extension))
        })
        .collect::<BTreeMap<_, _>>();
    let schemas = spec
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("openapi schemas object");
    let expected = openapi_postprocess::PAGINATION_OPERATIONS
        .iter()
        .map(|&operation_id| {
            let operation = operations
                .get(operation_id)
                .unwrap_or_else(|| panic!("missing pagination operation `{operation_id}`"));
            let response_reference = operation
                .pointer("/responses/200/content/application~1json/schema/$ref")
                .and_then(Value::as_str)
                .expect("pagination response component reference");
            let properties = component_schema_for_ref(schemas, response_reference)
                .and_then(|schema| schema.get("properties"))
                .and_then(Value::as_object)
                .expect("pagination response properties");
            let array_properties = properties
                .iter()
                .filter_map(|(name, property)| {
                    (property.get("type").and_then(Value::as_str) == Some("array"))
                        .then_some(name.as_str())
                })
                .collect::<Vec<_>>();
            let [results_property] = array_properties.as_slice() else {
                panic!("pagination operation `{operation_id}` must have one array property");
            };
            (
                operation_id,
                serde_json::json!({
                    "cursor": "$request.cursor",
                    "next_cursor": "$response.next_cursor",
                    "results": format!("$response.{results_property}"),
                }),
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(published, expected);
}

#[test]
fn proxy_cursor_list_operations_keep_pagination_metadata() {
    let full: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let proxy: Value = serde_json::from_str(
        &std::fs::read_to_string(PROXY_OPENAPI_JSON_PATH).expect("read static proxy openapi json"),
    )
    .expect("parse proxy openapi json");
    let full_operations = operations_by_id(&full);
    let proxy_operations = operations_by_id(&proxy);
    let table_operation_ids = openapi_postprocess::PAGINATION_OPERATIONS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let expected_proxy_operations = openapi_postprocess::PROXY_OPERATIONS
        .iter()
        .copied()
        .filter(|operation_id| table_operation_ids.contains(operation_id))
        .collect::<BTreeSet<_>>();
    let mut actual_proxy_operations = BTreeSet::new();

    for (operation_id, proxy_operation) in proxy_operations {
        if !table_operation_ids.contains(operation_id) {
            continue;
        }
        let full_operation = full_operations
            .get(operation_id)
            .unwrap_or_else(|| panic!("missing full operation `{operation_id}`"));
        assert_eq!(
            proxy_operation.get("x-fern-pagination"),
            full_operation.get("x-fern-pagination"),
            "proxy pagination metadata differs for `{operation_id}`"
        );
        actual_proxy_operations.insert(operation_id);
    }

    assert_eq!(actual_proxy_operations, expected_proxy_operations);
}

#[test]
fn openapi_publishes_namespace_diagnostics_in_the_admin_plane() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let operation = &spec["paths"]["/v0/admin/namespaces/{namespace_id}/diagnostics"]["get"];
    assert_eq!(operation["operationId"], "get_namespace_diagnostics");
    assert_eq!(operation["tags"], serde_json::json!(["admin"]));
    assert_eq!(
        operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/NamespaceDiagnostics"
    );
}

#[test]
fn openapi_query_parameters_publish_the_runtime_grammar() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths object");

    for (path, method) in [
        ("/v0/namespaces/{namespace_id}/filesystem/entries", "get"),
        ("/v0/namespaces/{namespace_id}/filesystem/revisions", "get"),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            "get",
        ),
        ("/v0/namespaces/{namespace_id}/filesystem/trash", "get"),
        ("/v0/admin/namespaces/{namespace_id}/checkpoints", "get"),
        ("/v0/namespaces/{namespace_id}/changes", "get"),
        ("/v0/namespaces/{namespace_id}/grep", "get"),
    ] {
        let parameter = query_parameter(paths, path, method, "limit");
        assert_eq!(parameter.get("required"), Some(&Value::Bool(false)));
        let schema = parameter.get("schema").expect("limit schema");
        assert_eq!(schema.get("type").and_then(Value::as_str), Some("integer"));
        assert_eq!(schema.get("format").and_then(Value::as_str), Some("int32"));
        assert_eq!(schema.get("minimum").and_then(Value::as_u64), Some(1));
        assert_eq!(schema.get("maximum").and_then(Value::as_u64), Some(1000));
        assert_eq!(schema.get("default").and_then(Value::as_u64), Some(1000));
    }

    for (path, default) in [
        ("/v0/namespaces/{namespace_id}/filesystem/entry", true),
        ("/v0/namespaces/{namespace_id}/filesystem/entries", false),
        ("/v0/namespaces/{namespace_id}/inodes/{inode_id}", true),
    ] {
        let parameter = query_parameter(paths, path, "get", "include_attributes");
        assert_eq!(parameter.get("required"), Some(&Value::Bool(false)));
        let schema = parameter.get("schema").expect("include_attributes schema");
        assert_eq!(schema.get("type").and_then(Value::as_str), Some("boolean"));
        assert_eq!(
            schema.get("default").and_then(Value::as_bool),
            Some(default)
        );
    }

    let grep_path = "/v0/namespaces/{namespace_id}/grep";
    for name in ["case_insensitive", "allow_scan", "allow_stale"] {
        let parameter = query_parameter(paths, grep_path, "get", name);
        assert_eq!(parameter.get("required"), Some(&Value::Bool(false)));
        let schema = parameter.get("schema").expect("grep boolean schema");
        assert_eq!(schema.get("type").and_then(Value::as_str), Some("boolean"));
        assert_eq!(schema.get("default").and_then(Value::as_bool), Some(false));
    }
    let pattern = query_parameter(paths, grep_path, "get", "pattern");
    assert_eq!(pattern.get("required"), Some(&Value::Bool(true)));
    assert!(pattern
        .get("description")
        .and_then(Value::as_str)
        .is_some_and(|description| description.contains("1024 bytes")));

    for (path, method, parameter_name, schema_name, required) in [
        (
            "/v0/namespaces/{namespace_id}/changes",
            "get",
            "after_seq",
            "ChangeSeq",
            true,
        ),
        (
            "/v0/namespaces/{namespace_id}/filesystem/content",
            "get",
            "revision_no",
            "RevisionNo",
            false,
        ),
        (
            "/v0/namespaces/{namespace_id}",
            "delete",
            "expected_head_seq",
            "ChangeSeq",
            false,
        ),
    ] {
        let parameter = query_parameter(paths, path, method, parameter_name);
        assert_eq!(parameter.get("required"), Some(&Value::Bool(required)));
        let expected_ref = format!("#/components/schemas/{schema_name}");
        assert_eq!(
            parameter.pointer("/schema/$ref").and_then(Value::as_str),
            Some(expected_ref.as_str())
        );
    }

    for (path, method, parameter_name) in [
        (
            "/v0/namespaces/{namespace_id}/filesystem/entries",
            "get",
            "path",
        ),
        (
            "/v0/namespaces/{namespace_id}/filesystem/entry",
            "get",
            "path",
        ),
        (
            "/v0/namespaces/{namespace_id}/filesystem/content",
            "get",
            "path",
        ),
        (
            "/v0/namespaces/{namespace_id}/filesystem/revisions",
            "get",
            "path",
        ),
        (
            "/v0/namespaces/{namespace_id}/filesystem/entries",
            "get",
            "cursor",
        ),
        (
            "/v0/namespaces/{namespace_id}/filesystem/revisions",
            "get",
            "cursor",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            "get",
            "cursor",
        ),
        (
            "/v0/namespaces/{namespace_id}/filesystem/trash",
            "get",
            "cursor",
        ),
        (
            "/v0/admin/namespaces/{namespace_id}/checkpoints",
            "get",
            "cursor",
        ),
    ] {
        let parameter = query_parameter(paths, path, method, parameter_name);
        assert_eq!(
            parameter.pointer("/schema/type").and_then(Value::as_str),
            Some("string")
        );
    }
}

#[test]
fn openapi_names_tagged_one_of_alternatives() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let schemas = spec
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .expect("openapi schemas object");

    for (schema_name, expected_names) in [
        (
            "AuthoritativePathEntry",
            &[
                "AuthoritativePathEntryDirectory",
                "AuthoritativePathEntryFile",
            ][..],
        ),
        (
            "BeginUploadRequest",
            &[
                "BeginUploadServiceProxied",
                "BeginUploadDirectPut",
                "BeginUploadDirectMultipart",
            ][..],
        ),
        (
            "BeginUploadResponse",
            &[
                "BeginUploadResponseServiceProxied",
                "BeginUploadResponseDirectPut",
                "BeginUploadResponseDirectMultipart",
            ][..],
        ),
        (
            "FilesystemChange",
            &[
                "FilesystemChangeDirectoryCreated",
                "FilesystemChangeFileCreated",
                "FilesystemChangeContentChanged",
                "FilesystemChangeMoved",
                "FilesystemChangeDeleted",
                "FilesystemChangeUndeleted",
                "FilesystemChangeAttributesChanged",
            ][..],
        ),
        (
            "FilesystemOperation",
            &[
                "FilesystemOperationCreateDirectory",
                "FilesystemOperationPutFile",
                "FilesystemOperationDeletePath",
                "FilesystemOperationMovePath",
                "FilesystemOperationCopyPath",
                "FilesystemOperationUndelete",
                "FilesystemOperationRestoreRevision",
                "FilesystemOperationUpdateAttributes",
            ][..],
        ),
        (
            "ObjectTransferAccess",
            &["ObjectTransferAccessPresignedUrl"][..],
        ),
        (
            "UploadSessionResponse",
            &[
                "UploadSessionStatusOpen",
                "UploadSessionStatusCompleted",
                "UploadSessionStatusAborted",
            ][..],
        ),
        (
            "CheckpointOwnerSummary",
            &["CheckpointOwnerUser", "CheckpointOwnerFork"][..],
        ),
        (
            "CompleteUploadRequest",
            &[
                "CompleteUploadServiceProxied",
                "CompleteUploadDirectPut",
                "CompleteUploadDirectMultipart",
            ][..],
        ),
        (
            "GrepIndexStatusResponse",
            &[
                "GrepIndexLifecycleDisabled",
                "GrepIndexLifecycleBackfilling",
                "GrepIndexLifecycleActive",
            ][..],
        ),
        (
            "ReorganizeStepOutcome",
            &[
                "ReorganizeStepOutcomeNotNeeded",
                "ReorganizeStepOutcomeUnitPublished",
                "ReorganizeStepOutcomeCompactionStarted",
                "ReorganizeStepOutcomeCompactionRunning",
                "ReorganizeStepOutcomeCompactionAtCapacity",
                "ReorganizeStepOutcomeCompactionRequired",
                "ReorganizeStepOutcomeSuperseded",
            ][..],
        ),
        (
            "WalFlushStepOutcome",
            &[
                "WalFlushStepOutcomeNotNeeded",
                "WalFlushStepOutcomeFlushed",
                "WalFlushStepOutcomeSuperseded",
                "WalFlushStepOutcomeRaceLost",
            ][..],
        ),
    ] {
        let names = one_of_schema_names(schemas, schema_name);
        assert_eq!(
            names, expected_names,
            "unexpected oneOf schema names for `{schema_name}`"
        );
    }
}

/// Responses that flatten a Rust enum, and the fields their envelope adds to
/// every variant.
const UNION_COMPOSITE_ENVELOPES: &[(&str, &[&str])] = &[
    (
        "AuthoritativePathEntry",
        &[
            "namespace_id",
            "path",
            "inode_id",
            "created_by",
            "created_at_ms",
            "head_seq",
        ],
    ),
    (
        "UploadSessionResponse",
        &["mode", "namespace_id", "upload_id"],
    ),
    (
        "GrepIndexStatusResponse",
        &["namespace_id", "next_run_ordinal", "reorganize_pending"],
    ),
];

/// A response that flattens an enum is one discriminated union, never an
/// `allOf` wrapped around one. SDK generators keep the envelope half of such
/// an `allOf` and drop the union half, so the generated type would lose every
/// field the variants carry.
#[test]
fn no_openapi_schema_wraps_an_all_of_around_a_one_of() {
    for document_path in [OPENAPI_JSON_PATH, PROXY_OPENAPI_JSON_PATH] {
        let spec: Value = serde_json::from_str(
            &std::fs::read_to_string(document_path).expect("read static openapi json"),
        )
        .expect("parse openapi json");
        let schemas = spec
            .pointer("/components/schemas")
            .and_then(Value::as_object)
            .expect("openapi schemas object");
        let mut composites = Vec::new();
        collect_all_of_members(&spec, "openapi", &mut composites);

        for (location, members) in composites {
            for member in members {
                let member = match member.get("$ref").and_then(Value::as_str) {
                    Some(reference) => {
                        component_schema_for_ref(schemas, reference).unwrap_or_else(|| {
                            panic!("{location} references a missing component: {reference}")
                        })
                    }
                    None => member,
                };
                assert!(
                    member.get("oneOf").is_none(),
                    "{location} in {document_path} wraps an allOf around a oneOf"
                );
            }
        }

        for (composite_name, envelope) in UNION_COMPOSITE_ENVELOPES {
            // The proxy document publishes only the operations it serves.
            let Some(composite) = schemas.get(*composite_name) else {
                continue;
            };
            let discriminator = composite
                .get("discriminator")
                .and_then(Value::as_object)
                .unwrap_or_else(|| panic!("{composite_name} has no discriminator"));
            assert!(discriminator.contains_key("propertyName"));
            assert!(discriminator.contains_key("mapping"));

            for variant_name in one_of_schema_names(schemas, composite_name) {
                let variant = schemas
                    .get(variant_name)
                    .unwrap_or_else(|| panic!("{variant_name} schema"));
                let required = required_fields(variant);
                for field in *envelope {
                    assert!(
                        required.contains(field),
                        "{variant_name} in {document_path} must require `{field}`"
                    );
                }
            }
        }
    }
}

#[test]
fn openapi_documents_string_id_contracts_without_dead_schemas() {
    let raw = std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json");
    let spec: Value = serde_json::from_str(&raw).expect("parse openapi json");
    let schemas = spec
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .expect("openapi schemas object");
    let content_id = schemas.get("ContentId").expect("ContentId schema");

    assert_eq!(
        content_id.get("pattern").and_then(Value::as_str),
        Some(r"^con_[0-9a-f]{32}$")
    );
    assert_eq!(
        content_id.get("example").and_then(Value::as_str),
        Some("con_9f2a6c0e4b7d4a90b13f0d8c5e6a2b41")
    );
    assert!(!schemas.contains_key("ContentStoreId"));
    assert!(!schemas.contains_key("FilesystemChangeCreated"));
    assert!(
        !raw.contains(r#""created""#),
        "retired creation-event kind remains in OpenAPI"
    );

    let api_spec = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/specs/api.md"
    ))
    .expect("read API spec");
    assert!(
        !api_spec.contains("`created`"),
        "retired creation-event kind remains in api.md"
    );
}

#[test]
fn openapi_caps_public_ordinals_and_uses_string_inode_ids() {
    let raw = std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json");
    let spec: Value = serde_json::from_str(&raw).expect("parse openapi json");
    let schemas = spec
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .expect("openapi schemas object");

    for name in [
        "RevisionNo",
        "ChangeSeq",
        "AttributeRevisionNo",
        "ManifestNo",
        "WriterEpoch",
    ] {
        let schema = schemas.get(name).unwrap_or_else(|| panic!("{name} schema"));
        assert_eq!(schema.get("type").and_then(Value::as_str), Some("integer"));
        assert_eq!(schema.get("format").and_then(Value::as_str), Some("int64"));
        assert_eq!(schema.get("minimum").and_then(Value::as_u64), Some(0));
        assert_eq!(
            schema.get("maximum").and_then(Value::as_u64),
            Some(loonfs_api::MAX_PUBLIC_INTEGER),
            "wrong public maximum for {name}"
        );
    }

    assert!(
        !schemas.contains_key("InodeId"),
        "public inode fields use inline string schemas"
    );

    for variant in one_of_schema_names(schemas, "GrepIndexStatusResponse") {
        let schema = schemas
            .get(variant)
            .unwrap_or_else(|| panic!("{variant} schema"));
        assert_eq!(
            schema
                .pointer("/properties/next_run_ordinal/maximum")
                .and_then(Value::as_u64),
            Some(loonfs_api::MAX_PUBLIC_INTEGER),
            "next_run_ordinal must use the public maximum in {variant}"
        );
    }
}

#[test]
fn openapi_describes_inode_ids_as_non_null_strings() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let schemas = spec
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("openapi schemas object");

    fn schema_matches(
        schema: &Value,
        schemas: &serde_json::Map<String, Value>,
        predicate: fn(&Value) -> bool,
    ) -> bool {
        if predicate(schema) {
            return true;
        }
        if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
            if let Some(name) = reference.strip_prefix("#/components/schemas/") {
                return schemas
                    .get(name)
                    .is_some_and(|schema| schema_matches(schema, schemas, predicate));
            }
        }
        ["allOf", "anyOf", "oneOf"]
            .into_iter()
            .filter_map(|key| schema.get(key).and_then(Value::as_array))
            .flatten()
            .any(|schema| schema_matches(schema, schemas, predicate))
    }

    fn assert_public_inode_schema(
        schema: &Value,
        schemas: &serde_json::Map<String, Value>,
        location: &str,
    ) {
        assert!(
            schema_matches(schema, schemas, |schema| {
                schema.get("type").and_then(Value::as_str) == Some("string")
                    && schema.get("pattern").and_then(Value::as_str)
                        == Some(loonfs_api::public_inode_id::PATTERN)
            }),
            "inode ID at {location} must use the public string format: {schema}"
        );
        assert!(
            !schema_matches(schema, schemas, |schema| {
                schema.get("type").and_then(Value::as_str) == Some("integer")
            }),
            "inode ID at {location} must not accept integers: {schema}"
        );
        assert!(
            !schema_matches(schema, schemas, |schema| {
                schema.get("type").and_then(Value::as_str) == Some("null")
            }),
            "inode ID at {location} must not accept null: {schema}"
        );
    }

    fn inspect(value: &Value, schemas: &serde_json::Map<String, Value>, location: &str) {
        match value {
            Value::Object(object) => {
                if let Some(properties) = object.get("properties").and_then(Value::as_object) {
                    for (name, schema) in properties {
                        if name.ends_with("inode_id") {
                            assert_public_inode_schema(
                                schema,
                                schemas,
                                &format!("{location}.{name}"),
                            );
                        }
                    }
                }
                if object.get("in").and_then(Value::as_str) == Some("path") {
                    if let Some(name) = object.get("name").and_then(Value::as_str) {
                        if name.ends_with("inode_id") {
                            let schema = object.get("schema").expect("path parameter schema");
                            assert_public_inode_schema(schema, schemas, location);
                        }
                    }
                }
                for (key, child) in object {
                    inspect(child, schemas, &format!("{location}.{key}"));
                }
            }
            Value::Array(values) => {
                for (index, child) in values.iter().enumerate() {
                    inspect(child, schemas, &format!("{location}[{index}]"));
                }
            }
            _ => {}
        }
    }

    inspect(&spec, schemas, "openapi");
}

#[test]
fn openapi_reuses_the_one_checksum_and_upload_claim_shapes() {
    let raw = std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json");
    for dead_name in [
        "StorageChecksum",
        "storage_checksum",
        "whole_file_sha256",
        "DirectPutContentClaim",
        "DirectMultipartContentClaim",
        "absolute_path",
        "root_inode_id",
        "deleted_at_seq",
        "ValidatedContentToken",
        "validated_content_token",
    ] {
        assert!(
            !raw.contains(dead_name),
            "dead public checksum name `{dead_name}` remains in OpenAPI"
        );
    }

    let spec: Value = serde_json::from_str(&raw).expect("parse openapi json");
    let schemas = spec
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("openapi schemas object");
    assert!(schemas.contains_key("Checksum"));
    assert!(schemas.contains_key("UploadContentClaim"));
    assert!(schemas.contains_key("ContentToken"));
    assert!(schemas.contains_key("UploadMode"));
    assert!(schemas.contains_key("UploadSessionResponse"));
    for retired_response in [
        "CompleteUploadResponse",
        "AbortUploadResponse",
        "UploadStatusResponse",
    ] {
        assert!(!schemas.contains_key(retired_response));
    }

    assert_eq!(
        schemas
            .get("UploadMode")
            .and_then(|schema| schema.get("enum"))
            .and_then(Value::as_array)
            .map(Vec::as_slice),
        Some(
            &[
                Value::String("service_proxied".to_owned()),
                Value::String("direct_put".to_owned()),
                Value::String("direct_multipart".to_owned()),
            ][..]
        )
    );
    let session_response = schemas
        .get("UploadSessionResponse")
        .expect("UploadSessionResponse schema");
    assert_eq!(
        session_response
            .pointer("/discriminator/propertyName")
            .and_then(Value::as_str),
        Some("status"),
        "the upload session union tags on `status`"
    );
    for variant_name in one_of_schema_names(schemas, "UploadSessionResponse") {
        let variant = schemas
            .get(variant_name)
            .unwrap_or_else(|| panic!("{variant_name} schema"));
        let encoded =
            serde_json::to_string(variant).expect("serialize upload session variant schema");
        assert!(encoded.contains(r#""status""#));
        assert!(!encoded.contains(r#""state""#));
    }

    for (path, method) in [
        ("/v0/namespaces/{namespace_id}/uploads/{upload_id}", "get"),
        (
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete",
            "post",
        ),
        (
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/abort",
            "post",
        ),
    ] {
        assert_eq!(
            spec.get("paths")
                .and_then(|paths| paths.get(path))
                .and_then(|path| path.get(method))
                .and_then(|operation| operation
                    .pointer("/responses/200/content/application~1json/schema/$ref"))
                .and_then(Value::as_str),
            Some("#/components/schemas/UploadSessionResponse")
        );
    }

    let checksum_ref = Value::String("#/components/schemas/Checksum".to_owned());
    let checksum_ref_count = values_named(&spec, "$ref")
        .filter(|reference| *reference == &checksum_ref)
        .count();
    assert!(
        checksum_ref_count >= 4,
        "public checksum-bearing shapes should reuse Checksum; found {checksum_ref_count} refs"
    );

    let multipart = schemas
        .get("DirectMultipartUpload")
        .expect("DirectMultipartUpload schema");
    assert!(required_fields(multipart).contains("checksum_algorithm"));

    let completion_variants = schemas
        .get("CompleteUploadRequest")
        .and_then(|schema| schema.get("oneOf"))
        .and_then(Value::as_array)
        .expect("completion variants");
    assert_eq!(completion_variants.len(), 3);
    for (variant_ref, (schema_name, mode)) in completion_variants.iter().zip([
        ("CompleteUploadServiceProxied", "service_proxied"),
        ("CompleteUploadDirectPut", "direct_put"),
        ("CompleteUploadDirectMultipart", "direct_multipart"),
    ]) {
        let reference = variant_ref
            .get("$ref")
            .and_then(Value::as_str)
            .expect("completion variant component ref");
        assert_eq!(
            reference.strip_prefix("#/components/schemas/"),
            Some(schema_name)
        );
        let variant = component_schema_for_ref(schemas, reference)
            .expect("completion variant component schema");
        assert_eq!(
            variant
                .pointer("/properties/mode/enum/0")
                .and_then(Value::as_str),
            Some(mode)
        );
        assert!(required_fields(variant).contains("mode"));
        let properties = variant
            .get("properties")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        for retired_name in ["completion", "content_ref"] {
            assert!(
                !properties.contains_key(retired_name),
                "retired completion request field `{retired_name}` remains in OpenAPI"
            );
        }
    }
    let multipart = completion_variants[2]
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|reference| component_schema_for_ref(schemas, reference))
        .expect("multipart completion variant component schema");
    let multipart_required = required_fields(multipart);
    assert!(multipart_required.contains("content"));
    assert!(multipart_required.contains("parts"));
    assert!(!schemas.contains_key("CompleteKnownContentUploadRequest"));
    assert!(!schemas.contains_key("CompleteMultipartUploadRequest"));

    for properties in values_named(&spec, "properties").filter_map(Value::as_object) {
        for retired_name in [
            "absolute_path",
            "root_inode_id",
            "deleted_at_seq",
            "validated_content_token",
        ] {
            assert!(
                !properties.contains_key(retired_name),
                "retired public field `{retired_name}` remains in an OpenAPI schema"
            );
        }
        assert!(
            !properties.contains_key("crc64nvme"),
            "crc64nvme must be an algorithm value, never a raw public field"
        );
    }

    let content_token_ref = Value::String("#/components/schemas/ContentToken".to_owned());
    let content_token_ref_count = values_named(&spec, "$ref")
        .filter(|reference| *reference == &content_token_ref)
        .count();
    assert_eq!(
        content_token_ref_count, 2,
        "the unified upload session and commit should share one ContentToken schema"
    );
}

#[test]
fn openapi_flattens_the_path_entry_attribute_projection() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let schemas = spec
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("openapi schemas object");
    assert!(!schemas.contains_key("AuthoritativeAttributes"));
    assert!(
        !schemas.contains_key("AttributesProjection"),
        "the attribute fields are flattened into each path entry variant"
    );

    for variant_name in one_of_schema_names(schemas, "AuthoritativePathEntry") {
        let variant = schemas
            .get(variant_name)
            .unwrap_or_else(|| panic!("{variant_name} schema"));
        let properties = variant
            .get("properties")
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("{variant_name} properties"));
        let required = required_fields(variant);
        for attribute_field in [
            "attributes",
            "attributes_revision_no",
            "attributes_updated_at_ms",
            "attributes_updated_by",
        ] {
            assert!(
                properties.contains_key(attribute_field),
                "{variant_name} must carry `{attribute_field}` at the top level"
            );
            assert!(
                !required.contains(attribute_field),
                "path entries may omit attribute fields when attributes are not requested"
            );
        }
    }
}

#[test]
fn openapi_documents_delete_path_behavior() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let schemas = spec
        .get("components")
        .and_then(|components| components.get("schemas"))
        .and_then(Value::as_object)
        .expect("openapi schemas object");
    let delete_schema = schemas
        .get("FilesystemOperationDeletePath")
        .expect("FilesystemOperationDeletePath component schema");

    assert!(!delete_schema
        .get("required")
        .and_then(Value::as_array)
        .expect("delete required fields")
        .iter()
        .any(|field| field.as_str() == Some("behavior")));
    assert_eq!(
        delete_schema
            .pointer("/properties/behavior/$ref")
            .and_then(Value::as_str),
        Some("#/components/schemas/DeleteDirectoryBehavior")
    );
}

fn assert_path_method(paths: &serde_json::Map<String, Value>, path: &str, method: &str) {
    let path_item = paths
        .get(path)
        .unwrap_or_else(|| panic!("missing OpenAPI path `{path}`"));
    assert!(
        path_item.get(method).is_some(),
        "missing OpenAPI method `{method}` for `{path}`"
    );
}

fn values_named<'a>(value: &'a Value, name: &'a str) -> Box<dyn Iterator<Item = &'a Value> + 'a> {
    match value {
        Value::Object(object) => Box::new(
            object.get(name).into_iter().chain(
                object
                    .values()
                    .flat_map(move |value| values_named(value, name)),
            ),
        ),
        Value::Array(values) => Box::new(
            values
                .iter()
                .flat_map(move |value| values_named(value, name)),
        ),
        _ => Box::new(std::iter::empty()),
    }
}

fn string_values(value: &Value) -> Box<dyn Iterator<Item = &Value> + '_> {
    match value {
        Value::String(_) => Box::new(std::iter::once(value)),
        Value::Object(object) => Box::new(object.values().flat_map(string_values)),
        Value::Array(values) => Box::new(values.iter().flat_map(string_values)),
        _ => Box::new(std::iter::empty()),
    }
}

fn referenced_components(spec: &Value) -> BTreeSet<(String, String)> {
    let components = spec["components"]
        .as_object()
        .expect("proxy components object");
    let mut pending = string_values(&spec["paths"])
        .filter_map(Value::as_str)
        .filter(|value| value.starts_with("#/components/"))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut referenced = BTreeSet::new();

    while let Some(reference) = pending.pop() {
        let Some((component_type, component_name)) = component_reference_parts(&reference) else {
            continue;
        };
        if !referenced.insert((component_type.clone(), component_name.clone())) {
            continue;
        }
        let component = components
            .get(&component_type)
            .and_then(Value::as_object)
            .and_then(|entries| entries.get(&component_name))
            .unwrap_or_else(|| panic!("missing proxy component reference `{reference}`"));
        pending.extend(
            string_values(component)
                .filter_map(Value::as_str)
                .filter(|value| value.starts_with("#/components/"))
                .map(str::to_owned),
        );
    }
    referenced
}

fn component_reference_parts(reference: &str) -> Option<(String, String)> {
    let mut parts = reference.strip_prefix("#/components/")?.split('/');
    Some((
        decode_json_pointer_segment(parts.next()?),
        decode_json_pointer_segment(parts.next()?),
    ))
}

fn decode_json_pointer_segment(segment: &str) -> String {
    segment.replace("~1", "/").replace("~0", "~")
}

fn collect_one_of_objects<'a>(
    value: &'a Value,
    unions: &mut Vec<&'a serde_json::Map<String, Value>>,
) {
    match value {
        Value::Object(object) => {
            if object.get("oneOf").and_then(Value::as_array).is_some() {
                unions.push(object);
            }
            for child in object.values() {
                collect_one_of_objects(child, unions);
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_one_of_objects(child, unions);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

/// Collects each `allOf` member list with its document location.
fn collect_all_of_members<'a>(
    value: &'a Value,
    location: &str,
    composites: &mut Vec<(String, &'a Vec<Value>)>,
) {
    match value {
        Value::Object(object) => {
            if let Some(members) = object.get("allOf").and_then(Value::as_array) {
                composites.push((location.to_owned(), members));
            }
            for (name, child) in object {
                collect_all_of_members(child, &format!("{location}.{name}"), composites);
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                collect_all_of_members(child, &format!("{location}[{index}]"), composites);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

/// Returns every response with its operation ID and status.
fn documented_responses(spec: &Value) -> Vec<(String, &Value)> {
    let mut responses = Vec::new();

    for (operation_id, operation) in operations_by_id(spec) {
        let Some(statuses) = operation.get("responses").and_then(Value::as_object) else {
            continue;
        };
        for (status, response) in statuses {
            responses.push((format!("{operation_id} {status}"), response));
        }
    }

    responses
}

/// Checks a response and its referenced components for nullable properties.
/// Optional response fields are omitted rather than encoded as `null`.
fn assert_no_null_property<'a>(
    spec: &'a Value,
    value: &'a Value,
    location: &str,
    visited: &mut BTreeSet<String>,
    properties: &mut usize,
) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                if visited.insert(reference.to_owned()) {
                    let component = component_for_ref(spec, reference).unwrap_or_else(|| {
                        panic!("{location} references a missing component: {reference}")
                    });
                    assert_no_null_property(spec, component, reference, visited, properties);
                }
            }

            if let Some(schema_properties) = object.get("properties").and_then(Value::as_object) {
                for (name, schema) in schema_properties {
                    *properties += 1;
                    assert!(
                        !admits_null(schema),
                        "response property `{name}` in {location} is typed null; \
                         optional response fields are omitted when absent"
                    );
                    assert!(
                        schema.get("nullable") != Some(&Value::Bool(true)),
                        "response property `{name}` in {location} is marked nullable; \
                         optional response fields are omitted when absent"
                    );
                }
            }

            for child in object.values() {
                assert_no_null_property(spec, child, location, visited, properties);
            }
        }
        Value::Array(values) => {
            for child in values {
                assert_no_null_property(spec, child, location, visited, properties);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn admits_null(schema: &Value) -> bool {
    schema
        .get("type")
        .and_then(Value::as_array)
        .is_some_and(|types| types.iter().any(|entry| entry.as_str() == Some("null")))
}

fn component_for_ref<'a>(spec: &'a Value, reference: &str) -> Option<&'a Value> {
    let (component_type, component_name) = component_reference_parts(reference)?;
    spec.get("components")?
        .get(component_type.as_str())?
        .get(component_name.as_str())
}

fn required_fields(schema: &Value) -> BTreeSet<&str> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

fn operations_by_id(spec: &Value) -> BTreeMap<&str, &Value> {
    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths object");
    let mut operations = BTreeMap::new();

    for path_item in paths.values() {
        let path_item = path_item.as_object().expect("openapi path item object");
        for method in ["get", "post", "put", "delete", "patch"] {
            let Some(operation) = path_item.get(method) else {
                continue;
            };
            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .expect("OpenAPI operation has an operationId");
            assert!(
                operations.insert(operation_id, operation).is_none(),
                "duplicate operationId `{operation_id}`"
            );
        }
    }

    operations
}

fn component_schema_for_ref<'a>(
    schemas: &'a serde_json::Map<String, Value>,
    reference: &str,
) -> Option<&'a Value> {
    let schema_name = reference.strip_prefix("#/components/schemas/")?;
    schemas.get(schema_name)
}

fn one_of_schema_names<'a>(
    schemas: &'a serde_json::Map<String, Value>,
    schema_name: &str,
) -> Vec<&'a str> {
    schemas
        .get(schema_name)
        .and_then(|schema| schema.get("oneOf"))
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing oneOf for schema `{schema_name}`"))
        .iter()
        .map(|schema| {
            schema
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix("#/components/schemas/"))
                .unwrap_or_else(|| panic!("missing oneOf component ref in schema `{schema_name}`"))
        })
        .collect()
}

fn assert_query_params(
    paths: &serde_json::Map<String, Value>,
    path: &str,
    method: &str,
    expected: &[&str],
) {
    let operation = paths
        .get(path)
        .and_then(|path_item| path_item.get(method))
        .unwrap_or_else(|| panic!("missing OpenAPI operation `{method} {path}`"));
    let params = operation
        .get("parameters")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("missing parameters for `{method} {path}`"));
    let query_names = params
        .iter()
        .filter(|param| param.get("in").and_then(Value::as_str) == Some("query"))
        .filter_map(|param| param.get("name").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();

    for name in expected {
        assert!(
            query_names.contains(name),
            "missing query parameter `{name}` for `{method} {path}`"
        );
    }
}

fn query_parameter<'a>(
    paths: &'a serde_json::Map<String, Value>,
    path: &str,
    method: &str,
    parameter_name: &str,
) -> &'a Value {
    paths
        .get(path)
        .and_then(|path_item| path_item.get(method))
        .and_then(|operation| operation.get("parameters"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|parameter| {
            parameter.get("in").and_then(Value::as_str) == Some("query")
                && parameter.get("name").and_then(Value::as_str) == Some(parameter_name)
        })
        .unwrap_or_else(|| {
            panic!("missing query parameter `{parameter_name}` for `{method} {path}`")
        })
}

fn assert_path_parameter_schema(
    paths: &serde_json::Map<String, Value>,
    path: &str,
    method: &str,
    parameter_name: &str,
    schema_name: &str,
) {
    let operation = paths
        .get(path)
        .and_then(|path_item| path_item.get(method))
        .unwrap_or_else(|| panic!("missing OpenAPI operation `{method} {path}`"));
    let parameter = operation
        .get("parameters")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|parameter| {
            parameter.get("in").and_then(Value::as_str) == Some("path")
                && parameter.get("name").and_then(Value::as_str) == Some(parameter_name)
        })
        .unwrap_or_else(|| {
            panic!("missing path parameter `{parameter_name}` for `{method} {path}`")
        });
    let expected_ref = format!("#/components/schemas/{schema_name}");
    assert_eq!(
        parameter.pointer("/schema/$ref").and_then(Value::as_str),
        Some(expected_ref.as_str()),
        "path parameter `{parameter_name}` for `{method} {path}` does not use `{schema_name}`",
    );
}

fn path_parameter<'a>(
    paths: &'a serde_json::Map<String, Value>,
    path: &str,
    method: &str,
    parameter_name: &str,
) -> &'a Value {
    paths
        .get(path)
        .and_then(|path_item| path_item.get(method))
        .and_then(|operation| operation.get("parameters"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|parameter| {
            parameter.get("in").and_then(Value::as_str) == Some("path")
                && parameter.get("name").and_then(Value::as_str) == Some(parameter_name)
        })
        .unwrap_or_else(|| {
            panic!("missing path parameter `{parameter_name}` for `{method} {path}`")
        })
}
