//! Named schemas for the tagged HTTP payloads.
//!
//! Utoipa derives fields from the wire types. These declarations choose the tag
//! and the few types whose fields belong to every variant. Assembly is local to
//! each declared type; it never searches a document for unions or guesses tags.

use super::*;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use utoipa::openapi::{schema::Schema, RefOr};
use utoipa::ToSchema;

/// Installs the named HTTP unions in an OpenAPI component collection.
///
/// Panics if a declaration disagrees with the derived wire schema. This runs
/// when building the static specification, not when serving an HTTP request.
pub fn register(schemas: &mut BTreeMap<String, RefOr<Schema>>) {
    let mut named = NamedSchemas::default();
    named.tagged::<BeginUploadRequest>("mode");
    named.tagged::<BeginUploadResponse>("mode");
    named.tagged::<CheckpointOwnerSummary>("kind");
    named.tagged::<FilesystemChange>("kind");
    named.tagged::<FilesystemOperation>("kind");
    named.tagged::<MetadataCompactionOutcome>("outcome");
    named.tagged::<ObjectTransferAccess>("kind");
    named.tagged::<ReorganizeStepOutcome>("outcome");
    named.tagged::<RunMaintenanceRequest>("kind");
    named.tagged::<RunMaintenanceResponse>("kind");
    named.tagged::<CompleteUploadRequest>("mode");
    named.tagged::<WalFlushStepOutcome>("outcome");
    named.composite::<PathEntry, PathEntryKind>("inode_kind");
    named.composite::<UploadSession, UploadSessionStatus>("status");
    named.composite::<GrepIndex, GrepIndexLifecycle>("status");

    // These are implementation fields flattened into the named payloads. The
    // document's reference-integrity test catches accidental independent reuse.
    for name in named.flattened.into_iter().chain(named.roots) {
        schemas.remove(&name);
    }
    for (name, schema) in named.schemas {
        let schema = serde_json::from_value(schema).expect("valid assembled HTTP schema");
        assert!(
            schemas
                .get(&name)
                .is_none_or(|existing| existing == &schema),
            "conflicting HTTP schema: {name}"
        );
        schemas.insert(name, schema);
    }
}

#[derive(Default)]
struct NamedSchemas {
    schemas: BTreeMap<String, Value>,
    flattened: BTreeSet<String>,
    roots: BTreeSet<String>,
}

impl NamedSchemas {
    fn tagged<T: ToSchema>(&mut self, tag: &str) {
        let source = Source::of::<T>();
        self.union(&T::name(), tag, source.schema.clone(), &source, None);
    }

    fn composite<T: ToSchema, Kind: ToSchema>(&mut self, tag: &str) {
        let source = Source::of::<T>();
        let mut envelope = source.schema.clone();
        let members = envelope["allOf"].as_array_mut().expect("composite fields");
        let kind_ref = reference(&Kind::name());
        let index = members
            .iter()
            .position(|s| s["$ref"] == kind_ref)
            .expect("declared flattened union");
        members.remove(index);
        let fields = self.flatten(envelope, &source);
        let mut union = serde_json::to_value(Kind::schema()).expect("derived union");
        for variant in union["oneOf"].as_array_mut().expect("kind variants") {
            if variant.get("title").is_none() {
                let value = variant["properties"][tag]["enum"][0]
                    .as_str()
                    .expect("kind tag");
                variant["title"] = json!(format!("{}{}", Kind::name(), pascal_case(value)));
            }
        }
        // The public payload owns the description and all variant fields.
        union["description"] = source.schema["description"].clone();
        self.union(&T::name(), tag, union, &source, Some(&fields));
        self.flattened.insert(Kind::name().into_owned());
    }

    fn union(
        &mut self,
        name: &str,
        tag: &str,
        mut union: Value,
        source: &Source,
        fields: Option<&Value>,
    ) {
        let variants = union
            .as_object_mut()
            .expect("union object")
            .remove("oneOf")
            .expect("tagged union");
        let mut references = Vec::new();
        let mut mapping = Map::new();
        for variant in variants.as_array().expect("union variants") {
            let mut variant = self.flatten(variant.clone(), source);
            let values = variant["properties"][tag]["enum"]
                .as_array()
                .expect("fixed tag");
            assert_eq!(values.len(), 1, "{name}: tag must have one value");
            let value = values[0].as_str().expect("string tag").to_owned();
            assert!(
                variant["required"]
                    .as_array()
                    .expect("required fields")
                    .contains(&json!(tag)),
                "{name}: tag must be required"
            );
            let title = variant
                .as_object_mut()
                .expect("variant object")
                .remove("title");
            let variant_name = title
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_else(|| format!("{name}{}", pascal_case(&value)));
            if let Some(fields) = fields {
                merge_fields(&mut variant, fields);
            }
            let reference = reference(&variant_name);
            assert!(
                mapping.insert(value, json!(reference)).is_none(),
                "{name}: duplicate tag"
            );
            references.push(json!({"$ref": reference}));
            assert!(
                self.schemas.insert(variant_name, variant).is_none(),
                "duplicate variant name"
            );
        }
        self.roots.insert(name.to_owned());
        union["oneOf"] = json!(references);
        union["discriminator"] = json!({"propertyName": tag, "mapping": mapping});
        assert!(
            self.schemas.insert(name.to_owned(), union).is_none(),
            "duplicate union name"
        );
    }

    fn flatten(&mut self, mut schema: Value, source: &Source) -> Value {
        let Some(members) = schema
            .as_object_mut()
            .expect("schema object")
            .remove("allOf")
        else {
            return schema;
        };
        schema["type"] = json!("object");
        for member in members.as_array().expect("composite members") {
            let member = if let Some(name) = member["$ref"]
                .as_str()
                .and_then(|r| r.strip_prefix("#/components/schemas/"))
            {
                self.flattened.insert(name.to_owned());
                source
                    .dependencies
                    .get(name)
                    .expect("derived component dependency")
            } else {
                member
            };
            assert!(
                member.get("allOf").is_none() && member.get("oneOf").is_none(),
                "flattened member must be an object"
            );
            merge_fields(&mut schema, member);
        }
        schema
    }
}

struct Source {
    schema: Value,
    dependencies: BTreeMap<String, Value>,
}

impl Source {
    fn of<T: ToSchema>() -> Self {
        let mut dependencies = Vec::new();
        T::schemas(&mut dependencies);
        Self {
            schema: serde_json::to_value(T::schema()).expect("derived schema"),
            dependencies: dependencies
                .into_iter()
                .map(|(name, schema)| {
                    (
                        name,
                        serde_json::to_value(schema).expect("derived dependency"),
                    )
                })
                .collect(),
        }
    }
}

fn merge_fields(target: &mut Value, source: &Value) {
    let target = target.as_object_mut().expect("object schema");
    let properties = target
        .entry("properties")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .expect("properties");
    if let Some(fields) = source["properties"].as_object() {
        for (name, field) in fields {
            assert!(
                properties.insert(name.clone(), field.clone()).is_none(),
                "duplicate flattened property: {name}"
            );
        }
    }
    let required = target
        .entry("required")
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .expect("required fields");
    if let Some(fields) = source["required"].as_array() {
        for field in fields {
            if !required.contains(field) {
                required.push(field.clone());
            }
        }
    }
}

fn reference(name: &str) -> String {
    format!("#/components/schemas/{name}")
}

fn pascal_case(value: &str) -> String {
    value
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|c| c.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::PartialSchema;

    #[test]
    fn omission_is_defined_by_the_field_without_document_rewrites() {
        let response =
            serde_json::to_value(ListPathEntriesResponse::schema()).expect("derived schema");
        assert_eq!(response["properties"]["next_cursor"]["type"], "string");
        assert!(!response["required"]
            .as_array()
            .expect("derived schema")
            .contains(&json!("next_cursor")));
        let request = serde_json::to_value(CommitRequest::schema()).expect("derived schema");
        assert_eq!(
            request["properties"]["message"]["type"],
            json!(["string", "null"])
        );
    }

    #[test]
    fn assembling_a_payload_does_not_change_its_kind_type() {
        let kind = serde_json::to_value(PathEntryKind::schema()).expect("derived schema");
        let mut named = NamedSchemas::default();
        named.composite::<PathEntry, PathEntryKind>("inode_kind");
        assert!(named.schemas["PathEntryFile"]["properties"]
            .get("namespace_id")
            .is_some());
        assert!(kind["oneOf"][1]["properties"].get("namespace_id").is_none());
        assert_eq!(
            serde_json::to_value(PathEntryKind::schema()).expect("derived schema"),
            kind
        );
    }

    #[test]
    #[should_panic(expected = "fixed tag")]
    fn a_wrong_declared_tag_fails_generation() {
        NamedSchemas::default().tagged::<PathEntryKind>("kind");
    }
}
