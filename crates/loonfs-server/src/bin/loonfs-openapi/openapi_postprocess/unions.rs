//! Discriminated union extraction and composite rewrites.

use super::value::{
    collect_component_references, component_schema_for_reference, component_schema_name,
    component_schema_reference, component_schemas, component_schemas_mut, invalid_document,
};
use super::OpenapiPostprocessError;
use serde_json::{Map, Value};
use std::collections::BTreeSet;

pub(super) fn extract_union_variants(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    struct UnionRewrite {
        union_name: String,
        variants: Vec<Value>,
        mapping: Map<String, Value>,
    }

    let schemas = component_schemas(document)?;
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
            .ok_or_else(|| {
                invalid_document(format!(
                    "components.schemas.{union_name}.discriminator.propertyName"
                ))
            })?;
        let mut mapping = Map::new();
        let mut references = Vec::with_capacity(variants.len());

        for variant in variants {
            if let Some(reference) = variant.get("$ref").and_then(Value::as_str) {
                let schema =
                    component_schema_for_reference(schemas, reference).ok_or_else(|| {
                        invalid_document(format!("components.schemas.{union_name}.oneOf.$ref"))
                    })?;
                let tag = fixed_discriminator_value(schema, property_name).ok_or_else(|| {
                    invalid_document(format!("components.schemas.{union_name}.oneOf"))
                })?;
                mapping.insert(tag, Value::String(reference.to_owned()));
                references.push(variant.clone());
                continue;
            }

            let tag = fixed_discriminator_value(variant, property_name).ok_or_else(|| {
                invalid_document(format!("components.schemas.{union_name}.oneOf"))
            })?;
            let schema_name = variant
                .get("title")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{union_name}{}", pascal_case(&tag)));
            let reference = component_schema_reference(&schema_name);
            let mut schema = variant.clone();
            schema
                .as_object_mut()
                .ok_or_else(|| invalid_document(format!("components.schemas.{union_name}.oneOf")))?
                .remove("title");

            register_extracted_schema(schemas, &mut extracted, &schema_name, schema)?;
            mapping.insert(tag, Value::String(reference.clone()));
            references.push(serde_json::json!({"$ref": reference}));
        }

        rewrites.push(UnionRewrite {
            union_name: union_name.clone(),
            variants: references,
            mapping,
        });
    }

    let schemas = component_schemas_mut(document)?;
    for rewrite in rewrites {
        let union = schemas
            .get_mut(&rewrite.union_name)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                invalid_document(format!("components.schemas.{}", rewrite.union_name))
            })?;
        union.insert("oneOf".to_owned(), Value::Array(rewrite.variants));
        union
            .get_mut("discriminator")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                invalid_document(format!(
                    "components.schemas.{}.discriminator",
                    rewrite.union_name
                ))
            })?
            .insert("mapping".to_owned(), Value::Object(rewrite.mapping));
    }
    schemas.extend(extracted);
    Ok(())
}

fn register_extracted_schema(
    schemas: &Map<String, Value>,
    extracted: &mut Map<String, Value>,
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
    properties: Map<String, Value>,
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
pub(super) fn merge_union_composites(document: &mut Value) -> Result<(), OpenapiPostprocessError> {
    let schemas = component_schemas(document)?;
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
    let schemas = component_schemas_mut(document)?;

    for composite in composites {
        for variant_name in &composite.variant_names {
            let variant = schemas
                .get_mut(variant_name)
                .ok_or_else(|| invalid_document(format!("components.schemas.{variant_name}")))?;
            merge_envelope(&composite.composite_name, variant, &composite.envelope)?;
        }

        let schema = schemas
            .get_mut(&composite.composite_name)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                invalid_document(format!("components.schemas.{}", composite.composite_name))
            })?;
        for field in ["allOf", "properties", "required", "type"] {
            schema.remove(field);
        }
        schema.insert("oneOf".to_owned(), Value::Array(composite.one_of));
        schema.insert("discriminator".to_owned(), composite.discriminator);
        merged_names.extend(composite.merged_names);
    }

    remove_unreferenced_components(document, &merged_names)?;
    Ok(())
}

/// Plans a union-composite rewrite, or returns `None` for other schemas.
fn plan_union_composite(
    schemas: &Map<String, Value>,
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
        let referenced = match member.get("$ref") {
            Some(reference) => {
                let reference = reference.as_str().ok_or_else(|| {
                    invalid_document(format!("components.schemas.{composite_name}.allOf.$ref"))
                })?;
                let name = component_schema_name(reference).ok_or_else(|| {
                    invalid_document(format!("components.schemas.{composite_name}.allOf.$ref"))
                })?;
                let schema = schemas
                    .get(&name)
                    .ok_or_else(|| invalid_document(format!("components.schemas.{name}")))?;
                Some((schema, name))
            }
            None => None,
        };
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
        .ok_or_else(|| invalid_document(format!("components.schemas.{union_name}.oneOf")))?
        .clone();
    let variant_names = one_of
        .iter()
        .map(|variant| {
            variant
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(component_schema_name)
                .ok_or_else(|| {
                    invalid_document(format!("components.schemas.{union_name}.oneOf.$ref"))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Some(UnionComposite {
        composite_name: composite_name.to_owned(),
        union_name,
        one_of,
        variant_names,
        discriminator: union
            .get("discriminator")
            .ok_or_else(|| {
                invalid_document(format!("components.schemas.{composite_name}.discriminator"))
            })?
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
        .ok_or_else(|| invalid_document(format!("components.schemas.{composite_name}.oneOf")))?;
    let properties = variant
        .entry("properties".to_owned())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| {
            invalid_document(format!(
                "components.schemas.{composite_name}.oneOf.properties"
            ))
        })?;
    for (name, schema) in &envelope.properties {
        insert_new_property(composite_name, properties, name, schema.clone())?;
    }

    let required = variant
        .entry("required".to_owned())
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| {
            invalid_document(format!(
                "components.schemas.{composite_name}.oneOf.required"
            ))
        })?;
    for name in &envelope.required {
        if !required.iter().any(|value| value.as_str() == Some(name)) {
            required.push(Value::String(name.clone()));
        }
    }
    Ok(())
}

fn insert_new_property(
    composite_name: &str,
    properties: &mut Map<String, Value>,
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
fn remove_unreferenced_components(
    document: &mut Value,
    merged_names: &BTreeSet<String>,
) -> Result<(), OpenapiPostprocessError> {
    let mut references = Vec::new();
    collect_component_references(document, &mut references);
    let referenced = references.into_iter().collect::<BTreeSet<_>>();

    let schemas = component_schemas_mut(document)?;
    schemas.retain(|name, _| {
        !merged_names.contains(name) || referenced.contains(&component_schema_reference(name))
    });
    Ok(())
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

fn pascal_case(value: &str) -> String {
    value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut characters = part.chars();
            let first = characters
                .next()
                .expect("filtered discriminator part should not be empty");
            first.to_ascii_uppercase().to_string() + characters.as_str()
        })
        .collect()
}

pub(super) fn add_union_discriminators(value: &mut Value, path: &mut Vec<String>) {
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

fn discriminator_for(object: &Map<String, Value>, path: &[String]) -> Option<Value> {
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
    let mapping: Map<String, Value> = values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            (
                value.to_owned(),
                Value::String(format!("#{one_of_pointer}/{index}")),
            )
        })
        .collect();

    Some(serde_json::json!({
        "propertyName": property_name,
        "mapping": mapping,
    }))
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
