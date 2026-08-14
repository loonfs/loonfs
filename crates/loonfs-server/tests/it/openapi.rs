#![allow(clippy::panic)]

use serde_json::Value;
use std::collections::BTreeSet;

const OPENAPI_JSON_PATH: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/specs/openapi.json");

#[test]
fn openapi_static_file_is_current() {
    let mut generated = loonfs_server::openapi_json_pretty().expect("generate openapi json");
    generated.push('\n');
    let committed = std::fs::read_to_string(OPENAPI_JSON_PATH).expect("read static openapi json");

    assert_eq!(
        generated, committed,
        "docs/specs/openapi.json is stale; rerun `cargo run -p loonfs-server --features openapi --bin loonfs-openapi -- docs/specs/openapi.json`"
    );
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
    assert_eq!(namespace_scoped_operations, 30);

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
fn openapi_operation_ids_are_the_frozen_public_registry() {
    const REGISTRY_MESSAGE: &str = "the operationId registry is a frozen public contract; record any deliberate rename in crates/loonfs-server/tests/it/openapi.rs::openapi_operation_ids_are_the_frozen_public_registry";

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
    let unique_operation_ids = operation_ids.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        unique_operation_ids.len(),
        operation_ids.len(),
        "duplicate operationIds found; {REGISTRY_MESSAGE}"
    );

    operation_ids.sort_unstable();
    assert_eq!(
        operation_ids,
        [
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
            "grep",
            "grep_index_status",
            "health",
            "list_changes",
            "list_checkpoints",
            "list_file_revisions",
            "list_file_revisions_by_inode",
            "list_path_entries",
            "list_trash",
            "maintenance_step",
            "namespace_status",
            "probe_store",
            "read_upload_status",
            "readiness",
            "release_checkpoint",
            "serve_metrics",
            "sign_upload_parts",
            "stat_inode",
            "stat_path",
            "upload_content",
        ],
        "operationIds changed; {REGISTRY_MESSAGE}"
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

    for (schema_name, expected_titles) in [
        (
            "AuthoritativePathEntryKind",
            &[
                "AuthoritativePathEntryDirectory",
                "AuthoritativePathEntryFile",
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
    ] {
        let titles = one_of_titles(schemas, schema_name);
        assert_eq!(
            titles, expected_titles,
            "unexpected oneOf titles for `{schema_name}`"
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
fn openapi_describes_inode_ids_as_strings_with_correct_nullability() {
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
        nullable: bool,
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
        assert_eq!(
            schema_matches(schema, schemas, |schema| {
                schema.get("type").and_then(Value::as_str) == Some("null")
            }),
            nullable,
            "wrong inode ID nullability at {location}: {schema}"
        );
    }

    fn inspect(value: &Value, schemas: &serde_json::Map<String, Value>, location: &str) {
        match value {
            Value::Object(object) => {
                if let Some(properties) = object.get("properties").and_then(Value::as_object) {
                    let required = object.get("required").and_then(Value::as_array);
                    for (name, schema) in properties {
                        if name.ends_with("inode_id") {
                            let is_required = required.is_some_and(|required| {
                                required.iter().any(|entry| entry.as_str() == Some(name))
                            });
                            assert_public_inode_schema(
                                schema,
                                schemas,
                                !is_required,
                                &format!("{location}.{name}"),
                            );
                        }
                    }
                }
                if object.get("in").and_then(Value::as_str) == Some("path") {
                    if let Some(name) = object.get("name").and_then(Value::as_str) {
                        if name.ends_with("inode_id") {
                            let schema = object.get("schema").expect("path parameter schema");
                            assert_public_inode_schema(schema, schemas, false, location);
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
    assert_eq!(
        completion_variants,
        &[
            serde_json::json!({
                "$ref": "#/components/schemas/CompleteKnownContentUploadRequest"
            }),
            serde_json::json!({
                "$ref": "#/components/schemas/CompleteMultipartUploadRequest"
            }),
        ]
    );
    let known_completion = schemas
        .get("CompleteKnownContentUploadRequest")
        .expect("known-content completion schema");
    assert!(known_completion
        .get("properties")
        .and_then(Value::as_object)
        .is_none_or(serde_json::Map::is_empty));

    let multipart_completion = schemas
        .get("CompleteMultipartUploadRequest")
        .expect("multipart completion schema");
    let required = required_fields(multipart_completion);
    assert!(required.contains("content"));
    assert!(required.contains("parts"));

    for request_schema in [known_completion, multipart_completion] {
        let properties = request_schema
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
        .filter_map(|schema| schema.get("oneOf").and_then(Value::as_array))
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
        .get("FilesystemOperation")
        .and_then(|schema| schema.get("oneOf"))
        .and_then(Value::as_array)
        .and_then(|schemas| {
            schemas.iter().find(|schema| {
                schema.get("title").and_then(Value::as_str) == Some("FsOpDeletePath")
            })
        })
        .expect("FsOpDeletePath oneOf schema");

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

fn required_fields(schema: &Value) -> BTreeSet<&str> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

fn one_of_titles<'a>(
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
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_else(|| panic!("missing oneOf title in schema `{schema_name}`"))
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
