//! Generated HTTP schema and operation contracts.

use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

#[path = "../../src/bin/loonfs-openapi/openapi_postprocess/mod.rs"]
mod openapi_postprocess;

use openapi_postprocess::operations::{
    add_pagination_metadata, add_retry_metadata, add_sdk_names, OPERATION_SDK_NAMES,
    SDK_EXCLUDED_OPERATIONS,
};
use openapi_postprocess::OpenapiPostprocessError;
use std::assert_matches;

const OPENAPI_JSON_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/specs/openapi.json");
const OPENAPI_PATH_SOURCES: &[&str] = &[
    include_str!("../../src/http/openapi/host.rs"),
    include_str!("../../src/http/handlers_downloads.rs"),
    include_str!("../../src/http/handlers_filesystem.rs"),
    include_str!("../../src/http/handlers_inodes.rs"),
    include_str!("../../src/http/handlers_maintenance.rs"),
    include_str!("../../src/http/handlers_namespace.rs"),
    include_str!("../../src/http/handlers_query.rs"),
    include_str!("../../src/http/handlers_snapshots.rs"),
    include_str!("../../src/http/handlers_store.rs"),
    include_str!("../../src/http/handlers_uploads.rs"),
];

#[test]
fn missing_retry_classification_returns_a_named_error() {
    let mut document = serde_json::json!({
        "paths": {
            "/future": {
                "post": {"operationId": "future_operation"}
            }
        }
    });

    let error =
        add_retry_metadata(&mut document).expect_err("unknown operation should fail generation");
    assert_matches!(
        error,
        OpenapiPostprocessError::MissingRetryClassification { operation_id }
            if operation_id == "future_operation"
    );
}

#[test]
fn missing_sdk_name_returns_a_named_error() {
    let mut document = serde_json::json!({
        "paths": {
            "/future": {
                "post": {"operationId": "future_operation"}
            }
        }
    });

    let error = add_sdk_names(&mut document).expect_err("unknown operation should fail generation");
    assert_matches!(
        error,
        OpenapiPostprocessError::MissingSdkName { operation_id }
            if operation_id == "future_operation"
    );
}

#[test]
fn sdk_name_tables_are_sorted_and_disjoint() {
    let named = OPERATION_SDK_NAMES
        .iter()
        .map(|(operation_id, _)| *operation_id)
        .collect::<Vec<_>>();
    let excluded = SDK_EXCLUDED_OPERATIONS.to_vec();

    let mut sorted_named = named.clone();
    sorted_named.sort_unstable();
    assert_eq!(named, sorted_named, "SDK name table must stay sorted");

    let mut sorted_excluded = excluded.clone();
    sorted_excluded.sort_unstable();
    assert_eq!(
        excluded, sorted_excluded,
        "SDK exclusion table must stay sorted"
    );

    let named_set = named.iter().copied().collect::<BTreeSet<_>>();
    let excluded_set = excluded.iter().copied().collect::<BTreeSet<_>>();
    assert!(named_set.is_disjoint(&excluded_set));
}

#[test]
fn sdk_method_names_are_unique_within_each_group() {
    let methods = OPERATION_SDK_NAMES
        .iter()
        .map(|(_, sdk_name)| (sdk_name.group, sdk_name.method))
        .collect::<BTreeSet<_>>();
    assert_eq!(methods.len(), OPERATION_SDK_NAMES.len());
}

#[test]
fn cursor_operation_without_a_page_response_returns_a_named_error() {
    let mut document = serde_json::json!({
        "components": {"schemas": {}},
        "paths": {
            "/future": {
                "get": {
                    "operationId": "future_list",
                    "parameters": [{"name": "cursor", "in": "query"}]
                }
            }
        }
    });
    let error = add_pagination_metadata(&mut document)
        .expect_err("a cursor operation without a page response should fail generation");
    assert_matches!(
        error,
        OpenapiPostprocessError::MissingPaginationResponseSchema { operation_id }
            if operation_id == "future_list"
    );
}

#[test]
fn openapi_static_files_are_current() {
    let mut full = openapi_postprocess::openapi_json_pretty(&loonfs_http::openapi_document())
        .expect("generate OpenAPI JSON");
    full.push('\n');

    assert_eq!(
        full,
        std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
        "docs/specs/openapi.json is stale; rerun `cargo run -p loonfs-http --features openapi --bin loonfs-openapi -- docs/specs/openapi.json`"
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

    for schema_name in [
        "ErrorResponse",
        "ErrorDetails",
        "RunMaintenanceResponseGc",
        "GrepMatch",
        "PinId",
    ] {
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
    ("DeletedObjectCounts", "content_objects"),
    ("DeletedObjectCounts", "upload_sessions"),
    ("RunMaintenanceResponseGc", "deleted"),
    ("RunMaintenanceResponseGc", "deleted_checkpoints_by_owner"),
    ("RunMaintenanceResponseGc", "retained"),
    ("GrepMatch", "line_truncated"),
    ("DeletedCheckpointsByOwner", "user"),
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
        ("/v0/namespaces/{namespace_id}/snapshots", "post"),
        ("/v0/namespaces/{namespace_id}/snapshots", "get"),
        (
            "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend",
            "post",
        ),
        (
            "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}",
            "delete",
        ),
        (
            "/v0/maintenance/namespaces/{namespace_id}/diagnostics",
            "get",
        ),
        ("/v0/namespaces/{namespace_id}/filesystem/entries", "get"),
        ("/v0/namespaces/{namespace_id}/filesystem/entry", "get"),
        ("/v0/namespaces/{namespace_id}/filesystem/content", "get"),
        ("/v0/namespaces/{namespace_id}/filesystem/downloads", "post"),
        ("/v0/namespaces/{namespace_id}/filesystem/revisions", "get"),
        ("/v0/namespaces/{namespace_id}/inodes/{inode_id}", "get"),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
            "get",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
            "get",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/downloads",
            "post",
        ),
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
        (
            "/v0/maintenance/namespaces/{namespace_id}/checkpoints",
            "get",
        ),
        (
            "/v0/maintenance/namespaces/{namespace_id}/checkpoints/{checkpoint_id}",
            "get",
        ),
        ("/v0/maintenance/namespaces/{namespace_id}/runs", "post"),
        (
            "/v0/maintenance/namespaces/{namespace_id}/grep/index/enable",
            "post",
        ),
        (
            "/v0/maintenance/namespaces/{namespace_id}/grep/index/disable",
            "post",
        ),
        ("/v0/maintenance/store/probe", "post"),
    ] {
        assert_path_method(paths, path, method);
    }

    assert!(!paths.contains_key("/openapi.json"));
    assert!(!paths.contains_key("/v0/maintenance/namespaces/{namespace_id}/grep/index/gc"));
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/filesystem/entries",
        "get",
        &[
            "path",
            "limit",
            "cursor",
            "snapshot_id",
            "consistency",
            "max_staleness_ms",
        ],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/filesystem/entry",
        "get",
        &["path", "snapshot_id", "consistency", "max_staleness_ms"],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/filesystem/content",
        "get",
        &[
            "path",
            "revision_no",
            "snapshot_id",
            "consistency",
            "max_staleness_ms",
        ],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/filesystem/downloads",
        "post",
        &[],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
        "get",
        &["limit", "cursor", "consistency", "max_staleness_ms"],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
        "get",
        &["snapshot_id", "consistency", "max_staleness_ms"],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
        "get",
        &["consistency", "max_staleness_ms"],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/downloads",
        "post",
        &[],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
        "post",
        &[],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/changes",
        "get",
        &[
            "after_seq",
            "limit",
            "snapshot_id",
            "consistency",
            "max_staleness_ms",
        ],
    );
    assert_query_params(
        paths,
        "/v0/maintenance/namespaces/{namespace_id}/checkpoints",
        "get",
        &["limit", "cursor"],
    );
    assert_query_params(
        paths,
        "/v0/namespaces/{namespace_id}/snapshots",
        "get",
        &["limit", "cursor"],
    );

    for (path, path_item) in paths {
        if !path.contains("/namespaces/{") {
            continue;
        }
        assert!(
            path.contains("{namespace_id}"),
            "namespace-scoped OpenAPI path must use `namespace_id`: `{path}`"
        );
        for method in ["get", "post", "put", "delete", "patch"] {
            let Some(operation) = path_item.get(method) else {
                continue;
            };
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

    for (path, request_name) in [
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/downloads",
            "CreateDownloadByInodeRequest",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
            "CreateRevisionDownloadByInodeRequest",
        ),
    ] {
        let inode_download = &paths[path]["post"];
        assert_eq!(
            inode_download.pointer("/requestBody/content/application~1json/schema/$ref"),
            Some(&json!(format!("#/components/schemas/{request_name}")))
        );
        assert!(inode_download.get("x-fern-request-name").is_none());
        assert!(spec["components"]["schemas"].get(request_name).is_some());
    }

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
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
            "get",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
            "get",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/downloads",
            "post",
        ),
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
            Some(loonfs_types::public_inode_id::PATTERN)
        );
        assert_eq!(
            parameter.get("example").and_then(Value::as_str),
            Some(loonfs_types::public_inode_id::EXAMPLE)
        );
    }

    for (path, method, operation_id) in [
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}",
            "get",
            "get_inode",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
            "get",
            "list_inode_children",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
            "get",
            "get_file_content_by_inode",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/downloads",
            "post",
            "create_download_by_inode",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            "get",
            "list_file_revisions_by_inode",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
            "get",
            "get_file_revision_content_by_inode",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
            "post",
            "create_revision_download_by_inode",
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
    const REGISTRY_MESSAGE: &str = "operation IDs are the wire registry; add or rename an operation in OPERATION_SDK_NAMES or SDK_EXCLUDED_OPERATIONS";

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

    let registered = registered_operation_ids();
    operation_ids.sort_unstable();
    assert_eq!(
        operation_ids.into_iter().collect::<BTreeSet<_>>(),
        registered,
        "operationIds changed; {REGISTRY_MESSAGE}"
    );
}

#[test]
fn openapi_document_generation_is_byte_stable() {
    let first = openapi_postprocess::openapi_json_pretty(&loonfs_http::openapi_document())
        .expect("generate OpenAPI JSON");
    let second = openapi_postprocess::openapi_json_pretty(&loonfs_http::openapi_document())
        .expect("generate OpenAPI JSON again");
    assert_eq!(first, second);
}

#[test]
fn every_registered_operation_publishes_its_sdk_name() {
    let generated =
        openapi_postprocess::document::openapi_json_pretty(&loonfs_http::openapi_document())
            .expect("generate openapi json");
    let spec: Value = serde_json::from_str(&generated).expect("parse generated openapi json");

    for (operation_id, operation) in operations_by_id(&spec) {
        if SDK_EXCLUDED_OPERATIONS.contains(&operation_id) {
            assert_eq!(operation.get("x-fern-ignore"), Some(&json!(true)));
            for extension in [
                "x-fern-sdk-group-name",
                "x-fern-sdk-method-name",
                "x-fern-request-name",
            ] {
                assert!(
                    operation.get(extension).is_none(),
                    "excluded operation `{operation_id}` has `{extension}`"
                );
            }
            continue;
        }

        let sdk_name = OPERATION_SDK_NAMES
            .iter()
            .find_map(|(candidate, sdk_name)| (*candidate == operation_id).then_some(sdk_name))
            .unwrap_or_else(|| panic!("operation `{operation_id}` has no SDK name"));
        assert_eq!(
            operation.get("x-fern-sdk-group-name"),
            Some(&json!(sdk_name.group)),
            "unexpected SDK group for `{operation_id}`"
        );
        assert_eq!(
            operation.get("x-fern-sdk-method-name"),
            Some(&json!(sdk_name.method)),
            "unexpected SDK method for `{operation_id}`"
        );
        let expected_request = sdk_name.request.map(|request| json!(request));
        assert_eq!(
            operation.get("x-fern-request-name"),
            expected_request.as_ref(),
            "unexpected SDK request name for `{operation_id}`"
        );
        assert!(
            operation.get("x-fern-ignore").is_none(),
            "named operation `{operation_id}` is ignored"
        );
    }
}

#[test]
fn non_idempotent_operations_disable_generated_sdk_retries() {
    let generated =
        openapi_postprocess::document::openapi_json_pretty(&loonfs_http::openapi_document())
            .expect("generate openapi json");
    let spec: Value = serde_json::from_str(&generated).expect("parse generated openapi json");
    let operations = operations_by_id(&spec);

    for (operation_id, operation) in operations {
        let retry_class = operation
            .get("x-loonfs-retry")
            .and_then(Value::as_str)
            .expect("operation retry class");
        let fern_retries = operation.get("x-fern-retries");
        match retry_class {
            "not_idempotent" => {
                assert_eq!(fern_retries, Some(&serde_json::json!({"disabled": true})));
            }
            "idempotent" | "replayable" => assert_eq!(fern_retries, None),
            retry_class => {
                panic!("operation `{operation_id}` has invalid retry class `{retry_class}`")
            }
        }
    }
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
    let expected = operations
        .iter()
        .filter(|(_, operation)| {
            operation
                .get("parameters")
                .and_then(Value::as_array)
                .is_some_and(|parameters| {
                    parameters.iter().any(|parameter| {
                        parameter.get("in").and_then(Value::as_str) == Some("query")
                            && parameter.get("name").and_then(Value::as_str) == Some("cursor")
                    })
                })
        })
        .map(|(&operation_id, &operation)| {
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
fn openapi_publishes_namespace_diagnostics_in_the_maintenance_api_group() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let operation = &spec["paths"]["/v0/maintenance/namespaces/{namespace_id}/diagnostics"]["get"];
    assert_eq!(operation["operationId"], "get_namespace_diagnostics");
    assert_eq!(operation["tags"], serde_json::json!(["maintenance"]));
    assert_eq!(
        operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/NamespaceDiagnostics"
    );
    let diagnostics = &spec["components"]["schemas"]["NamespaceDiagnostics"];
    assert_eq!(
        required_fields(diagnostics),
        BTreeSet::from([
            "acknowledged_floor_seq",
            "created_at_ms",
            "created_by",
            "head_seq",
            "live_snapshots",
            "namespace_id",
            "retention_floor_seq",
            "wal_tail_objects",
        ])
    );
}

/// Checks that each proxied content route documents the optional `Range`
/// header, its 206 and 416 responses, and the `Accept-Ranges` header of a
/// whole read.
#[test]
fn content_routes_publish_the_range_header_and_its_responses() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    for path in [
        "/v0/namespaces/{namespace_id}/filesystem/content",
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
    ] {
        let operation = &spec["paths"][path]["get"];
        let headers: Vec<_> = operation["parameters"]
            .as_array()
            .expect("parameters")
            .iter()
            .filter(|parameter| parameter["in"] == "header")
            .collect();
        assert_eq!(headers.len(), 1, "{path}");
        assert_eq!(headers[0]["name"], "Range", "{path}");
        assert_eq!(headers[0]["required"], false, "{path}");
        assert_eq!(headers[0]["schema"], json!({"type": "string"}), "{path}");
        let responses = &operation["responses"];
        assert!(
            responses["200"]["headers"]["Accept-Ranges"].is_object(),
            "{path}"
        );
        assert!(
            responses["206"]["headers"]["Content-Range"].is_object(),
            "{path}"
        );
        assert_eq!(
            responses["416"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ErrorResponse",
            "{path}"
        );
    }
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
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
            "get",
        ),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            "get",
        ),
        ("/v0/namespaces/{namespace_id}/filesystem/trash", "get"),
        (
            "/v0/maintenance/namespaces/{namespace_id}/checkpoints",
            "get",
        ),
        ("/v0/namespaces/{namespace_id}/snapshots", "get"),
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
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
            false,
        ),
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

    for path in [
        "/v0/namespaces/{namespace_id}/filesystem/entry",
        "/v0/namespaces/{namespace_id}/filesystem/entries",
        "/v0/namespaces/{namespace_id}/filesystem/content",
        "/v0/namespaces/{namespace_id}/filesystem/trash",
        "/v0/namespaces/{namespace_id}/filesystem/revisions",
        "/v0/namespaces/{namespace_id}/changes",
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}",
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
        "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
    ] {
        let consistency = query_parameter(paths, path, "get", "consistency");
        assert_eq!(consistency.get("required"), Some(&Value::Bool(false)));
        assert_eq!(
            consistency.pointer("/schema/$ref").and_then(Value::as_str),
            Some("#/components/schemas/ReadConsistency")
        );
        let max_staleness = query_parameter(paths, path, "get", "max_staleness_ms");
        assert_eq!(max_staleness.get("required"), Some(&Value::Bool(false)));
        let schema = max_staleness
            .get("schema")
            .expect("max_staleness_ms schema");
        assert_eq!(schema.get("type").and_then(Value::as_str), Some("integer"));
        assert_eq!(schema.get("minimum").and_then(Value::as_u64), Some(0));
        assert_eq!(
            schema.get("maximum").and_then(Value::as_u64),
            Some(loonfs::engine::READ_REVALIDATION_BOUND_MS)
        );
    }

    let schemas = &spec["components"]["schemas"];
    for request_name in [
        "CreateDownloadRequest",
        "CreateDownloadByInodeRequest",
        "CreateRevisionDownloadByInodeRequest",
    ] {
        let properties = &schemas[request_name]["properties"];
        assert_eq!(
            properties["consistency"]["$ref"],
            json!("#/components/schemas/ReadConsistency"),
            "{request_name}"
        );
        let max_staleness = &properties["max_staleness_ms"];
        assert_eq!(max_staleness["type"], json!("integer"), "{request_name}");
        assert_eq!(max_staleness["minimum"], json!(0), "{request_name}");
        assert_eq!(
            max_staleness["maximum"],
            json!(loonfs::engine::READ_REVALIDATION_BOUND_MS),
            "{request_name}"
        );
    }
    assert_eq!(
        schemas["ReadConsistency"]["enum"],
        json!(["eventual", "strong"])
    );
    assert_eq!(
        schemas["ReadConsistency"].get("default"),
        None,
        "the default is the server's `consistency` setting, so the schema declares none"
    );

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
            "/v0/maintenance/namespaces/{namespace_id}/checkpoints",
            "get",
            "cursor",
        ),
        ("/v0/namespaces/{namespace_id}/snapshots", "get", "cursor"),
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
            "NamespaceAccess",
            &["NamespaceAccessUnrestricted", "NamespaceAccessAcl"][..],
        ),
        (
            "NamespaceAccessMode",
            &["NamespaceAccessModeUnrestricted", "NamespaceAccessModeAcl"][..],
        ),
        (
            "RunMaintenanceRequest",
            &[
                "RunMaintenanceRequestMetadata",
                "RunMaintenanceRequestGc",
                "RunMaintenanceRequestRetention",
                "RunMaintenanceRequestRetentionWindow",
                "RunMaintenanceRequestRecoverAdministrator",
            ][..],
        ),
        (
            "RunMaintenanceResponse",
            &[
                "RunMaintenanceResponseMetadata",
                "RunMaintenanceResponseGc",
                "RunMaintenanceResponseRetention",
                "RunMaintenanceResponseRetentionWindow",
                "RunMaintenanceResponseRecoverAdministrator",
            ][..],
        ),
        ("PathEntry", &["PathEntryDirectory", "PathEntryFile"][..]),
        (
            "CreateUploadBody",
            &[
                "CreateUploadBodyServiceProxied",
                "CreateUploadBodyDirectPut",
                "CreateUploadBodyDirectMultipart",
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
                "FilesystemChangeAccessChanged",
            ][..],
        ),
        (
            "FilesystemOperation",
            &[
                "FilesystemOperationCreateDirectory",
                "FilesystemOperationCreateDirectoryByInode",
                "FilesystemOperationPutFile",
                "FilesystemOperationCreateFileByInode",
                "FilesystemOperationPutFileRevisionByInode",
                "FilesystemOperationAppendFile",
                "FilesystemOperationAppendFileByInode",
                "FilesystemOperationDeletePath",
                "FilesystemOperationDeleteByInode",
                "FilesystemOperationMovePath",
                "FilesystemOperationMoveByInode",
                "FilesystemOperationCopyPath",
                "FilesystemOperationCopyByInode",
                "FilesystemOperationUndelete",
                "FilesystemOperationRestoreRevision",
                "FilesystemOperationRestoreRevisionByInode",
                "FilesystemOperationUpdateAttributes",
                "FilesystemOperationUpdateAttributesByInode",
                "FilesystemOperationUpdateAccess",
                "FilesystemOperationUpdateAccessByInode",
            ][..],
        ),
        (
            "ObjectTransferAccess",
            &["ObjectTransferAccessPresignedUrl"][..],
        ),
        (
            "UploadSession",
            &[
                "UploadSessionStatusOpen",
                "UploadSessionStatusCompleted",
                "UploadSessionStatusAborted",
            ][..],
        ),
        (
            "CheckpointOwner",
            &["CheckpointOwnerUser", "CheckpointOwnerFork"][..],
        ),
        (
            "CompleteUploadBody",
            &[
                "CompleteUploadBodyServiceProxied",
                "CompleteUploadBodyDirectPut",
                "CompleteUploadBodyDirectMultipart",
            ][..],
        ),
        (
            "GrepIndex",
            &[
                "GrepIndexStatusDisabled",
                "GrepIndexStatusBackfilling",
                "GrepIndexStatusActive",
            ][..],
        ),
        (
            "StoreProbeCheck",
            &[
                "StoreProbeCheckPassed",
                "StoreProbeCheckUnsupported",
                "StoreProbeCheckFailed",
            ][..],
        ),
    ] {
        let names = one_of_schema_names(schemas, schema_name);
        for name in &names {
            assert!(
                schemas[*name]["description"]
                    .as_str()
                    .is_some_and(|text| !text.is_empty()),
                "variant schema `{name}` must have a description"
            );
        }
        assert_eq!(
            names, expected_names,
            "unexpected oneOf schema names for `{schema_name}`"
        );
    }

    let user_owner = schemas
        .get("CheckpointOwnerUser")
        .expect("user checkpoint-owner schema");
    assert!(!required_fields(user_owner).contains("expires_at_ms"));
}

/// Responses that flatten a Rust enum, and the fields their envelope adds to
/// every variant.
const UNION_COMPOSITE_ENVELOPES: &[(&str, &[&str])] = &[
    (
        "PathEntry",
        &[
            "namespace_id",
            "path",
            "inode_id",
            "created_by",
            "created_at_ms",
            "head_seq",
        ],
    ),
    ("UploadSession", &["mode", "namespace_id", "upload_id"]),
    (
        "GrepIndex",
        &["namespace_id", "next_run_no", "reorganize_pending"],
    ),
    ("StoreProbeCheck", &["name"]),
];

#[test]
fn no_openapi_schema_wraps_an_all_of_around_a_one_of() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
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
                "{location} wraps an allOf around a oneOf"
            );
        }
    }

    for (composite_name, envelope) in UNION_COMPOSITE_ENVELOPES {
        let composite = schemas
            .get(*composite_name)
            .unwrap_or_else(|| panic!("{composite_name} schema"));
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
                    "{variant_name} must require `{field}`"
                );
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
    let referenced = referenced_components(&spec);
    for name in schemas.keys() {
        assert!(
            referenced.contains(&("schemas".to_owned(), name.clone())),
            "OpenAPI schema `{name}` is not referenced by a path"
        );
    }
    let content_id = schemas.get("ContentId").expect("ContentId schema");

    assert_eq!(
        content_id.get("pattern").and_then(Value::as_str),
        Some(r"^con_[0-7][0-9a-hjkmnp-tv-z]{25}$")
    );
    assert_eq!(
        content_id.get("example").and_then(Value::as_str),
        Some("con_4z59p0wjvx9a8b2frdhhf6mat1")
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
        "AttributesRevisionNo",
        "ManifestNo",
        "RunNo",
        "WriterEpoch",
    ] {
        let schema = schemas.get(name).unwrap_or_else(|| panic!("{name} schema"));
        assert_eq!(schema.get("type").and_then(Value::as_str), Some("integer"));
        assert_eq!(schema.get("format").and_then(Value::as_str), Some("int64"));
        assert_eq!(schema.get("minimum").and_then(Value::as_u64), Some(0));
        assert_eq!(
            schema.get("maximum").and_then(Value::as_u64),
            Some(loonfs_types::MAX_PUBLIC_INTEGER),
            "wrong public maximum for {name}"
        );
    }

    let inode_id = schemas.get("InodeId").expect("InodeId schema");
    assert_eq!(inode_id.get("type").and_then(Value::as_str), Some("string"));
    assert_eq!(
        inode_id.get("pattern").and_then(Value::as_str),
        Some(loonfs_types::public_inode_id::PATTERN)
    );
    assert_eq!(
        inode_id.get("example").and_then(Value::as_str),
        Some(loonfs_types::public_inode_id::EXAMPLE)
    );

    // Each flattened status variant must use the capped run number schema.
    for variant in one_of_schema_names(schemas, "GrepIndex") {
        let schema = schemas
            .get(variant)
            .unwrap_or_else(|| panic!("{variant} schema"));
        assert_eq!(
            schema
                .pointer("/properties/next_run_no/$ref")
                .and_then(Value::as_str),
            Some("#/components/schemas/RunNo"),
            "next_run_no must use the capped run number schema in {variant}"
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
                        == Some(loonfs_types::public_inode_id::PATTERN)
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
    let spec: Value = serde_json::from_str(&raw).expect("parse openapi json");
    let schemas = spec
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("openapi schemas object");
    assert!(schemas.contains_key("Checksum"));
    assert!(schemas.contains_key("UploadContentClaim"));
    assert!(schemas.contains_key("ContentToken"));
    assert!(schemas.contains_key("UploadMode"));
    assert!(schemas.contains_key("UploadSession"));

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
    let session_response = schemas.get("UploadSession").expect("UploadSession schema");
    assert_eq!(
        session_response
            .pointer("/discriminator/propertyName")
            .and_then(Value::as_str),
        Some("status"),
        "the upload session union tags on `status`"
    );
    for variant_name in one_of_schema_names(schemas, "UploadSession") {
        let variant = schemas
            .get(variant_name)
            .unwrap_or_else(|| panic!("{variant_name} schema"));
        let encoded =
            serde_json::to_string(variant).expect("serialize upload session variant schema");
        assert!(encoded.contains(r#""status""#));
        assert!(!encoded.contains(r#""state""#));
    }

    for (path, method) in [
        ("/v0/namespaces/{namespace_id}/uploads", "post"),
        (
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
            "put",
        ),
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
            Some("#/components/schemas/UploadSession")
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

    let open = schemas
        .get("UploadSessionStatusOpen")
        .expect("open session schema");
    let required = required_fields(open);
    assert!(open["properties"].get("checksum_algorithm").is_some());
    assert!(required.contains("checksum_algorithm"));
    for field in ["access", "part_size_bytes", "content_ref"] {
        assert!(
            open["properties"].get(field).is_some(),
            "open session includes `{field}`"
        );
        assert!(
            !required.contains(field),
            "open session leaves `{field}` optional"
        );
    }
    assert!(required_fields(
        schemas
            .get("CreateUploadBodyDirectMultipart")
            .expect("CreateUploadBodyDirectMultipart schema")
    )
    .contains("mode"));
    let completion_variants = schemas
        .get("CompleteUploadBody")
        .and_then(|schema| schema.get("oneOf"))
        .and_then(Value::as_array)
        .expect("completion variants");
    assert_eq!(completion_variants.len(), 3);
    for (variant_ref, (schema_name, mode)) in completion_variants.iter().zip([
        ("CompleteUploadBodyServiceProxied", "service_proxied"),
        ("CompleteUploadBodyDirectPut", "direct_put"),
        ("CompleteUploadBodyDirectMultipart", "direct_multipart"),
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
    }
    let multipart = completion_variants[2]
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|reference| component_schema_for_ref(schemas, reference))
        .expect("multipart completion variant component schema");
    let multipart_required = required_fields(multipart);
    assert!(multipart_required.contains("content"));
    assert!(multipart_required.contains("parts"));

    for properties in values_named(&spec, "properties").filter_map(Value::as_object) {
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
fn access_id_schemas_exclude_commas() {
    use utoipa::PartialSchema as _;

    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json"),
    )
    .expect("parse openapi json");
    let schemas = spec
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("openapi schemas object");
    let subject_schema =
        serde_json::to_value(loonfs_types::SubjectId::schema()).expect("subject id schema");
    assert_eq!(
        subject_schema.get("pattern").and_then(Value::as_str),
        Some(r"^[\x21-\x2B\x2D-\x7E]{1,256}$"),
        "SubjectId pattern"
    );
    for name in ["PrincipalId", "PrincipalScope"] {
        assert_eq!(
            schemas[name].get("pattern").and_then(Value::as_str),
            Some(r"^[\x21-\x2B\x2D-\x7E]{1,256}$"),
            "{name} pattern"
        );
    }
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
    assert!(
        !schemas.contains_key("AttributesProjection"),
        "the attribute fields are flattened into each path entry variant"
    );

    for variant_name in one_of_schema_names(schemas, "PathEntry") {
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
        .expect("openapi components object");
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
            .unwrap_or_else(|| panic!("missing component reference `{reference}`"));
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

fn registered_operation_ids() -> BTreeSet<&'static str> {
    OPERATION_SDK_NAMES
        .iter()
        .map(|(operation_id, _)| *operation_id)
        .chain(SDK_EXCLUDED_OPERATIONS.iter().copied())
        .collect()
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

#[test]
fn actor_headers_are_global_and_only_attributed_operations_require_them() {
    let full = openapi_postprocess::openapi_json_pretty(&loonfs_http::openapi_document())
        .expect("generate document");
    let full: Value = serde_json::from_str(&full).expect("full document");
    assert_eq!(
        full["x-fern-global-headers"],
        json!([
        {"header": "Loonfs-Actor", "name": "actorId", "optional": true},
        {"header": "Loonfs-Subject", "name": "subjectId", "optional": true},
        {"header": "Loonfs-Principal-Scope", "name": "principalScope", "optional": true},
        {"header": "Loonfs-Principals", "name": "principals", "optional": true}
        ])
    );
    let mut required = BTreeSet::new();
    for path in full["paths"].as_object().expect("paths").values() {
        for operation in path.as_object().expect("path").values() {
            let Some(id) = operation["operationId"].as_str() else {
                continue;
            };
            if let Some(actor) = operation.get("x-loonfs-actor") {
                assert_eq!(actor, "required");
                required.insert(id);
            }
            if let Some(parameters) = operation["parameters"].as_array() {
                assert!(!parameters.iter().any(|parameter| {
                    parameter["in"] == "header"
                        && parameter["name"]
                            .as_str()
                            .is_some_and(|name| name.eq_ignore_ascii_case("Loonfs-Actor"))
                }));
            }
        }
    }
    assert_eq!(
        required,
        BTreeSet::from(["create_commit", "create_namespace", "fork_namespace"])
    );
}
