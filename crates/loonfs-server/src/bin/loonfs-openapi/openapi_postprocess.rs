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

/// Serializes and normalizes one generated OpenAPI document without changing
/// the field order produced by utoipa.
pub(crate) fn openapi_json_pretty(
    document: &(impl Serialize + ?Sized),
) -> Result<String, serde_json::Error> {
    let derived = serde_json::to_string(document)?;
    let mut document = serde_json::from_str(&derived)?;
    normalize_optional_schemas(&mut document);
    add_union_discriminators(&mut document);
    serde_json::to_string_pretty(&document)
}

/// Renders omitted `Option<T>` values as optional, non-null schemas.
fn normalize_optional_schemas(document: &mut Value) {
    normalize_optional_schemas_in(document);
}

/// Adds metadata that utoipa 5.5 cannot derive for internally tagged enums.
fn add_union_discriminators(document: &mut Value) {
    visit(document, &mut Vec::new());
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
    fn derives_a_complete_mapping_from_fixed_required_properties() {
        let mut document = ordered(serde_json::json!({
            "components": {
                "schemas": {
                    "Choice": {
                        "oneOf": [
                            {
                                "required": ["kind"],
                                "properties": {"kind": {"enum": ["first"]}}
                            },
                            {
                                "required": ["kind", "value"],
                                "properties": {
                                    "kind": {"const": "second"},
                                    "value": {"type": "string"}
                                }
                            }
                        ]
                    }
                }
            }
        }));

        add_union_discriminators(&mut document);
        let actual = unordered(&document);
        assert_eq!(
            actual.pointer("/components/schemas/Choice/discriminator"),
            Some(&serde_json::json!({
                "propertyName": "kind",
                "mapping": {
                    "first": "#/components/schemas/Choice/oneOf/0",
                    "second": "#/components/schemas/Choice/oneOf/1"
                }
            }))
        );

        let once = document.clone();
        add_union_discriminators(&mut document);
        assert_eq!(document, once);
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
