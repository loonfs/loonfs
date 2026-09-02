//! OpenAPI document rewrites that utoipa cannot express at the handler or schema.

mod nullable;
mod operations;
pub(crate) mod proxy;
mod unions;
mod value;

use nullable::{drop_null_from_response_schemas, normalize_optional_schemas};
use operations::{add_sdk_names, validate_operation_retry_classes, validate_pagination_metadata};
#[cfg(test)]
pub(crate) use operations::{OPERATION_SDK_NAMES, SDK_EXCLUDED_OPERATIONS};
use proxy::{
    derive_proxy_paths, describe_proxy_document, prune_proxy_components, remove_proxy_security,
    retain_referenced_tags,
};
use serde::Serialize;
#[cfg(test)]
use serde_json::Value;
use unions::{add_union_discriminators, extract_union_variants, merge_union_composites};

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpenapiPostprocessError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("openapi operation `{operation_id}` has no retry classification")]
    MissingRetryClassification { operation_id: String },
    #[error("openapi operation `{operation_id}` has no SDK name")]
    MissingSdkName { operation_id: String },
    #[error("openapi SDK-named operation `{operation_id}` does not appear in the document")]
    MissingSdkNameOperation { operation_id: String },
    #[error("openapi pagination operation `{operation_id}` has no `cursor` query parameter")]
    MissingPaginationCursorParameter { operation_id: String },
    #[error("openapi pagination operation `{operation_id}` has no 200 response component schema")]
    MissingPaginationResponseSchema { operation_id: String },
    #[error("openapi pagination operation `{operation_id}` response has no `{property}` property")]
    MissingPaginationResponseProperty {
        operation_id: String,
        property: String,
    },
    #[error(
        "openapi pagination operation `{operation_id}` response: expected exactly one array property"
    )]
    InvalidPaginationArrayProperties { operation_id: String },
    #[error(
        "openapi operation `{operation_id}` has a `cursor` query parameter but no pagination metadata entry"
    )]
    MissingPaginationMetadata { operation_id: String },
    #[error("invalid openapi document at `{location}`")]
    InvalidDocument { location: String },
    #[error("openapi union variant `{schema_name}` conflicts with an existing component schema")]
    UnionVariantSchemaCollision { schema_name: String },
    #[error("openapi composite `{schema_name}` combines two discriminated unions")]
    UnionCompositeHasTwoUnions { schema_name: String },
    #[error("openapi composite `{schema_name}` defines property `{property}` twice")]
    UnionCompositeDuplicateProperty {
        schema_name: String,
        property: String,
    },
    #[error(
        "openapi union `{union_name}` is flattened into `{schema_name}` and referenced elsewhere"
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

/// Generates OpenAPI JSON and applies the required schema and operation rewrites.
pub(crate) fn openapi_json_pretty(
    document: &(impl Serialize + ?Sized),
) -> Result<String, OpenapiPostprocessError> {
    let derived = serde_json::to_string(document)?;
    let mut document = serde_json::from_str(&derived)?;
    normalize_optional_schemas(&mut document)?;
    drop_null_from_response_schemas(&mut document)?;
    add_union_discriminators(&mut document, &mut Vec::new());
    extract_union_variants(&mut document)?;
    merge_union_composites(&mut document)?;
    validate_operation_retry_classes(&document)?;
    add_sdk_names(&mut document)?;
    validate_pagination_metadata(&document)?;
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
    describe_proxy_document(&mut document)?;
    derive_proxy_paths(&mut document)?;
    remove_proxy_security(&mut document)?;
    retain_referenced_tags(&mut document)?;
    prune_proxy_components(&mut document)?;
    Ok(serde_json::to_string_pretty(&document)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn extracts_named_and_derived_union_variants() {
        let mut document = serde_json::json!({
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
        });

        add_union_discriminators(&mut document, &mut Vec::new());
        extract_union_variants(&mut document).expect("extract union variants");
        let actual = &document;
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
        let mut document = serde_json::json!({
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
        });

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
        let mut document = serde_json::json!({
            "components": {
                "schemas": {
                    "Choice": {"oneOf": [titled_variant]},
                    "ChoiceFirst": variant
                }
            }
        });

        add_union_discriminators(&mut document, &mut Vec::new());
        extract_union_variants(&mut document).expect("reuse identical component schema");
        assert_eq!(
            document.pointer("/components/schemas/Choice/oneOf/0/$ref"),
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
        let mut document = serde_json::json!({
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
        });

        extract_and_merge(&mut document).expect("merge union composites");
        let actual = &document;
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
        let mut document = serde_json::json!({
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
        });

        extract_and_merge(&mut document).expect("merge union composites");
        let actual = &document;
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
        let mut document = serde_json::json!({
            "components": {
                "schemas": {
                    "ItemEnvelope": {
                        "allOf": [
                            {"$ref": "#/components/schemas/Item"},
                            {
                                "type": "object",
                                "required": ["owner_id"],
                                "properties": {"owner_id": {"type": "string"}}
                            }
                        ]
                    },
                    "Item": {
                        "type": "object",
                        "required": ["item_id"],
                        "properties": {"item_id": {"type": "string"}}
                    }
                }
            }
        });
        let before = document.clone();

        merge_union_composites(&mut document).expect("leave the composite alone");
        assert_eq!(document, before);
    }

    #[test]
    fn rejects_a_composite_that_redefines_a_variant_property() {
        let mut document = serde_json::json!({
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
        });

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
        let mut document = serde_json::json!({
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
        });

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
        let document = serde_json::json!({
            "paths": {
                "/future": {
                    "post": {"operationId": "future_operation"}
                }
            }
        });

        let error = validate_operation_retry_classes(&document)
            .expect_err("unknown operation should fail generation");
        assert!(matches!(
            error,
            OpenapiPostprocessError::MissingRetryClassification { operation_id }
                if operation_id == "future_operation"
        ));
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

        let error =
            add_sdk_names(&mut document).expect_err("unknown operation should fail generation");
        assert!(matches!(
            error,
            OpenapiPostprocessError::MissingSdkName { operation_id }
                if operation_id == "future_operation"
        ));
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
    fn unregistered_cursor_operation_returns_a_named_error() {
        let document = serde_json::json!({
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
        let error = validate_pagination_metadata(&document)
            .expect_err("unregistered cursor operation should fail generation");
        assert!(matches!(
            error,
            OpenapiPostprocessError::MissingPaginationMetadata { operation_id }
                if operation_id == "future_list"
        ));
    }

    #[test]
    fn replaces_an_optional_property_null_union() {
        let mut document = serde_json::json!({
            "type": "object",
            "properties": {
                "child": {
                    "oneOf": [
                        {"type": "null"},
                        {"$ref": "#/components/schemas/Child"}
                    ]
                }
            }
        });

        normalize_optional_schemas(&mut document).expect("normalize optional property");
        let actual = &document;
        assert_eq!(
            actual.pointer("/properties/child"),
            Some(&serde_json::json!({"$ref": "#/components/schemas/Child"}))
        );
    }

    #[test]
    fn replaces_an_optional_request_body_null_union() {
        let mut document = serde_json::json!({
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
        });

        normalize_optional_schemas(&mut document).expect("normalize optional request body");
        let actual = &document;
        assert_eq!(
            actual.pointer("/requestBody/content/application~1json/schema"),
            Some(&serde_json::json!({
                "$ref": "#/components/schemas/Request"
            }))
        );
    }

    #[test]
    fn refuses_to_rewrite_a_required_property() {
        let mut document = serde_json::json!({
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
        });

        let error = normalize_optional_schemas(&mut document)
            .expect_err("required optional schema should fail generation");
        assert!(matches!(
            error,
            OpenapiPostprocessError::InvalidDocument { location }
                if location == "required property child has an optional schema"
        ));
    }

    /// One request schema, one response schema, and one schema both reach.
    fn request_and_response_document() -> Value {
        serde_json::json!({
            "paths": {
                "/things": {
                    "post": {
                        "operationId": "create_thing",
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
        })
    }

    #[test]
    fn drops_the_null_type_from_an_optional_response_property() {
        let mut document = request_and_response_document();

        drop_null_from_response_schemas(&mut document).expect("normalize response schemas");
        let actual = &document;
        assert_eq!(
            actual.pointer("/components/schemas/ThingResponse/properties/next_cursor"),
            Some(&serde_json::json!({"type": "string"}))
        );
    }

    #[test]
    fn drops_the_null_type_from_a_schema_a_request_and_a_response_share() {
        let mut document = request_and_response_document();

        drop_null_from_response_schemas(&mut document).expect("normalize response schemas");
        let actual = &document;
        assert_eq!(
            actual.pointer("/components/schemas/Shared/properties/label"),
            Some(&serde_json::json!({"type": "string"}))
        );
    }

    #[test]
    fn keeps_the_null_type_on_a_request_only_property() {
        let mut document = request_and_response_document();

        drop_null_from_response_schemas(&mut document).expect("normalize response schemas");
        let actual = &document;
        assert_eq!(
            actual.pointer("/components/schemas/ThingRequest/properties/cursor"),
            Some(&serde_json::json!({"type": ["string", "null"]}))
        );
    }

    #[test]
    fn rewrites_only_optional_response_properties() {
        let mut document = serde_json::json!({
            "paths": {
                "/things": {
                    "get": {
                        "operationId": "get_things",
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
            },
            "components": {}
        });

        drop_null_from_response_schemas(&mut document).expect("normalize response schemas");
        let actual = &document;
        assert_eq!(
            actual.pointer(
                "/paths/~1things/get/responses/200/content/application~1json/schema/properties/marker"
            ),
            Some(&serde_json::json!({"type": ["string", "null"]}))
        );
    }

    #[test]
    fn leaves_a_one_of_without_one_shared_fixed_tag_unchanged() {
        let mut document = serde_json::json!({
            "oneOf": [
                {"required": ["left"], "properties": {"left": {"enum": ["a"]}}},
                {"required": ["right"], "properties": {"right": {"enum": ["b"]}}}
            ]
        });

        add_union_discriminators(&mut document, &mut Vec::new());
        assert!(document.get("discriminator").is_none());
    }
}
