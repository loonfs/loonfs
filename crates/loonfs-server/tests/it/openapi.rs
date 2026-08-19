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
        "docs/specs/openapi.json is stale; rerun `cargo run -p loonfs-server --features openapi --bin loonfs-openapi -- --proxy-output docs/specs/openapi-proxy.json docs/specs/openapi.json`"
    );
    assert_eq!(
        proxy,
        std::fs::read_to_string(PROXY_OPENAPI_JSON_PATH)
            .expect("read static proxy openapi json"),
        "docs/specs/openapi-proxy.json is stale; rerun `cargo run -p loonfs-server --features openapi --bin loonfs-openapi -- --proxy-output docs/specs/openapi-proxy.json docs/specs/openapi.json`"
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
        ("/v0/namespaces/{namespace_id}/filesystem/list", "get"),
        ("/v0/namespaces/{namespace_id}/filesystem/stat", "get"),
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
            "/v0/admin/namespaces/{namespace_id}/maintenance/step",
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
        "/v0/namespaces/{namespace_id}/filesystem/list",
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
            "stat_inode",
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
            "begin_download_by_inode",
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
    const REGISTRY_MESSAGE: &str = "operation IDs define generated SDK method names; update OPENAPI_OPERATION_IDS only when renaming a public method";

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

    operation_ids.sort_unstable();
    assert_eq!(
        operation_ids,
        openapi_postprocess::OPENAPI_OPERATION_IDS,
        "operationIds changed; {REGISTRY_MESSAGE}"
    );
}

#[test]
fn proxy_operation_ids_match_the_allowlist() {
    let registered = openapi_postprocess::OPENAPI_OPERATION_IDS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let allowlisted = openapi_postprocess::PROXY_OPERATIONS
        .iter()
        .map(|(operation_id, _)| *operation_id)
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
            .all(|pair| pair[0].0 < pair[1].0),
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
fn proxy_paths_use_mounts_and_declare_no_security() {
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(PROXY_OPENAPI_JSON_PATH).expect("read static proxy openapi json"),
    )
    .expect("parse proxy openapi json");
    assert_eq!(spec["info"]["title"], "LoonFS Browser Proxy API");
    assert_eq!(
        spec["info"]["description"],
        "API for browser clients that access namespaces through application mounts."
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
    assert!(!tag_names.contains("system"));
    assert!(!tag_names.contains("admin"));

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
            let scope = openapi_postprocess::PROXY_OPERATIONS
                .iter()
                .find_map(|(candidate, scope)| (*candidate == operation_id).then_some(*scope))
                .expect("published proxy operation is allowlisted");
            match scope {
                openapi_postprocess::ProxyOperationScope::Unscoped => {
                    assert_eq!(path, "/v0/capabilities");
                }
                openapi_postprocess::ProxyOperationScope::Mount => {
                    assert!(path.starts_with("/v0/mounts/{mount}/"));
                    let mount = operation["parameters"]
                        .as_array()
                        .expect("proxy operation parameters")
                        .iter()
                        .find(|parameter| parameter["in"] == "path" && parameter["name"] == "mount")
                        .expect("proxy operation mount parameter");
                    assert_eq!(mount["description"], "Application mount name");
                    assert_eq!(mount["schema"], serde_json::json!({"type": "string"}));
                }
            }
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
    let registered = openapi_postprocess::OPENAPI_OPERATION_IDS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let classified = openapi_postprocess::OPERATION_RETRY_CLASSES
        .iter()
        .map(|(operation_id, _)| *operation_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        openapi_postprocess::OPENAPI_OPERATION_IDS.len(),
        registered.len(),
        "operation IDs must be unique"
    );
    assert_eq!(
        openapi_postprocess::OPERATION_RETRY_CLASSES.len(),
        classified.len(),
        "each operation must have one retry class"
    );
    assert_eq!(classified, registered);

    let generated = openapi_postprocess::openapi_json_pretty(&loonfs_server::openapi_document())
        .expect("generate openapi json");
    let spec: Value = serde_json::from_str(&generated).expect("parse generated openapi json");
    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths object");
    let mut published = BTreeMap::new();
    for path_item in paths.values().filter_map(Value::as_object) {
        for method in ["get", "post", "put", "delete", "patch"] {
            let Some(operation) = path_item.get(method) else {
                continue;
            };
            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .expect("OpenAPI operation has an operationId");
            let retry_class = operation
                .get("x-loonfs-retry")
                .and_then(Value::as_str)
                .expect("OpenAPI operation has an x-loonfs-retry extension");
            assert!(
                published.insert(operation_id, retry_class).is_none(),
                "duplicate operationId `{operation_id}`"
            );
        }
    }

    let expected = openapi_postprocess::OPERATION_RETRY_CLASSES
        .iter()
        .map(|(operation_id, retry_class)| (*operation_id, retry_class.as_str()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(published, expected);
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
        ("/v0/namespaces/{namespace_id}/filesystem/list", "get"),
        ("/v0/namespaces/{namespace_id}/filesystem/revisions", "get"),
        (
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            "get",
        ),
        ("/v0/namespaces/{namespace_id}/filesystem/trash", "get"),
        ("/v0/admin/namespaces/{namespace_id}/checkpoints", "get"),
        ("/v0/namespaces/{namespace_id}/changes", "get"),
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
        ("/v0/namespaces/{namespace_id}/filesystem/stat", true),
        ("/v0/namespaces/{namespace_id}/filesystem/list", false),
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
            "/v0/namespaces/{namespace_id}/filesystem/list",
            "get",
            "path",
        ),
        (
            "/v0/namespaces/{namespace_id}/filesystem/stat",
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
            "/v0/namespaces/{namespace_id}/filesystem/list",
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
            "AuthoritativePathEntryKind",
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
                "FsOpCreateDirectory",
                "FsOpPutFile",
                "FsOpDeletePath",
                "FsOpMovePath",
                "FsOpCopyPath",
                "FsOpUndelete",
                "FsOpRestoreRevision",
                "FsOpUpdateAttributes",
            ][..],
        ),
        (
            "ObjectTransferAccess",
            &["ObjectTransferAccessPresignedUrl"][..],
        ),
        (
            "UploadSessionStatus",
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
            "GrepIndexLifecycle",
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
        "ManifestId",
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

    assert_eq!(
        schemas
            .get("GrepIndexStatusResponse")
            .and_then(|schema| schema.get("allOf"))
            .and_then(Value::as_array)
            .and_then(|schemas| schemas.iter().find_map(|schema| schema.get("properties")))
            .and_then(|properties| properties.get("next_run_ordinal"))
            .and_then(|schema| schema.get("maximum"))
            .and_then(Value::as_u64),
        Some(loonfs_api::MAX_PUBLIC_INTEGER),
        "next_run_ordinal must use the public maximum"
    );
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
    assert!(schemas.contains_key("UploadSessionStatus"));
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
    let session_response_refs: BTreeSet<_> = session_response
        .get("allOf")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|schema| schema.get("$ref").and_then(Value::as_str))
        .collect();
    assert!(session_response_refs.contains("#/components/schemas/UploadSessionStatus"));
    assert!(serde_json::to_string(
        schemas
            .get("UploadSessionStatus")
            .expect("UploadSessionStatus schema")
    )
    .expect("serialize UploadSessionStatus schema")
    .contains(r#""status""#));
    assert!(!serde_json::to_string(
        schemas
            .get("UploadSessionStatus")
            .expect("UploadSessionStatus schema")
    )
    .expect("serialize UploadSessionStatus schema")
    .contains(r#""state""#));

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

    let projection = schemas
        .get("AttributesProjection")
        .expect("AttributesProjection schema");
    let projection_properties = projection
        .get("properties")
        .and_then(Value::as_object)
        .expect("attribute projection properties");
    assert!(projection_properties.contains_key("attributes_revision_no"));
    assert!(projection_properties.contains_key("attributes"));
    let required = required_fields(projection);
    assert!(required.contains("attributes_revision_no"));
    assert!(required.contains("attributes"));

    let path_entry = schemas
        .get("AuthoritativePathEntry")
        .expect("AuthoritativePathEntry schema");
    let flattened_projection_ref = path_entry
        .get("allOf")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|schema| schema.get("$ref").and_then(Value::as_str))
        .any(|reference| reference == "#/components/schemas/AttributesProjection");
    assert!(
        flattened_projection_ref,
        "path entries must flatten AttributesProjection at the top level"
    );
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
        .get("FsOpDeletePath")
        .expect("FsOpDeletePath component schema");

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

fn required_fields(schema: &Value) -> BTreeSet<&str> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
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
