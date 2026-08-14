//! Converts inode IDs between internal numbers and public API strings.
//!
//! Public IDs use the form `ino_<number>`. They are scoped to a namespace and
//! should be treated as identifiers rather than numbers.

use crate::InodeId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Prefix used by every public inode ID.
pub const PREFIX: &str = "ino_";

/// OpenAPI pattern for public inode IDs.
pub const PATTERN: &str = r"^ino_[1-9][0-9]*$";

/// Example public inode ID used in OpenAPI.
pub const EXAMPLE: &str = "ino_123";

/// OpenAPI description for public inode IDs.
pub const DESCRIPTION: &str = "Stable inode ID within a namespace";

const INVALID_REASON: &str = "must use `ino_` followed by a nonzero u64 without leading zeroes";

/// Explains why a string is not a valid public inode ID.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid inode id {value:?}: {reason}")]
pub struct PublicInodeIdError {
    value: String,
    reason: String,
}

impl PublicInodeIdError {
    /// Returns the rejected input.
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Returns the validation rule that the input failed.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// Converts an inode ID to its public API string.
pub fn encode(id: InodeId) -> String {
    format!("{PREFIX}{}", id.0)
}

/// Parses and validates a public inode ID.
pub fn decode(value: &str) -> Result<InodeId, PublicInodeIdError> {
    let Some(suffix) = value.strip_prefix(PREFIX) else {
        return Err(invalid(value));
    };
    if suffix.is_empty()
        || suffix.starts_with('0')
        || !suffix.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(invalid(value));
    }

    suffix
        .parse::<u64>()
        .map(InodeId)
        .map_err(|_| invalid(value))
}

fn invalid(value: &str) -> PublicInodeIdError {
    PublicInodeIdError {
        value: value.to_owned(),
        reason: INVALID_REASON.to_owned(),
    }
}

/// Serializes an inode ID as a public API string.
pub fn serialize<S>(id: &InodeId, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    encode(*id).serialize(serializer)
}

/// Deserializes an inode ID from its public API string.
pub fn deserialize<'de, D>(deserializer: D) -> Result<InodeId, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    decode(&value).map_err(serde::de::Error::custom)
}

/// Serde support for optional public inode ID fields.
pub mod option {
    use super::{decode, encode};
    use crate::InodeId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    /// Serializes an optional inode ID as a public API string.
    pub fn serialize<S>(id: &Option<InodeId>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        id.map(encode).serialize(serializer)
    }

    /// Deserializes an optional inode ID from its public API string.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<InodeId>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<String>::deserialize(deserializer)?
            .map(|value| decode(&value))
            .transpose()
            .map_err(serde::de::Error::custom)
    }
}

/// Builds the OpenAPI schema for a public inode ID.
#[cfg(feature = "openapi")]
#[allow(deprecated)]
pub fn schema() -> utoipa::openapi::schema::Object {
    utoipa::openapi::schema::Object::builder()
        .schema_type(utoipa::openapi::schema::Type::String)
        .pattern(Some(PATTERN))
        .example(Some(serde_json::json!(EXAMPLE)))
        .description(Some(DESCRIPTION))
        .build()
}

/// Builds the OpenAPI schema for an optional public inode ID.
#[cfg(feature = "openapi")]
pub fn optional_schema() -> utoipa::openapi::schema::Schema {
    utoipa::openapi::schema::OneOf::builder()
        .item(
            utoipa::openapi::schema::Object::builder()
                .schema_type(utoipa::openapi::schema::Type::Null),
        )
        .item(schema())
        .build()
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_rejected(value: &str) {
        let error = decode(value).expect_err("invalid public inode id");

        assert_eq!(error.value(), value);
        assert_eq!(error.reason(), INVALID_REASON);
        assert_eq!(
            error.to_string(),
            format!("invalid inode id {value:?}: {INVALID_REASON}")
        );
    }

    #[test]
    fn decode_accepts_public_inode_id_grammar() {
        for (value, expected) in [
            ("ino_1", InodeId(1)),
            ("ino_42", InodeId(42)),
            ("ino_18446744073709551615", InodeId(u64::MAX)),
        ] {
            assert_eq!(decode(value).expect("valid public inode id"), expected);
            assert_eq!(encode(expected), value);
        }
    }

    macro_rules! rejection_test {
        ($name:ident, $value:literal) => {
            #[test]
            fn $name() {
                assert_rejected($value);
            }
        };
    }

    rejection_test!(decode_rejects_zero, "ino_0");
    rejection_test!(decode_rejects_leading_zero, "ino_01");
    rejection_test!(decode_rejects_sign, "ino_-1");
    rejection_test!(decode_rejects_uppercase_prefix, "INO_27");
    rejection_test!(decode_rejects_mixed_case_prefix, "Ino_27");
    rejection_test!(decode_rejects_missing_prefix, "27");
    rejection_test!(decode_rejects_empty_suffix, "ino_");
    rejection_test!(decode_rejects_suffix_trailing_text, "ino_27x");
    rejection_test!(decode_rejects_leading_whitespace, " ino_27");
    rejection_test!(decode_rejects_trailing_whitespace, "ino_27 ");
    rejection_test!(decode_rejects_empty_string, "");
    rejection_test!(decode_rejects_u64_overflow, "ino_18446744073709551616");

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct RequiredField {
        #[serde(with = "crate::public_inode_id")]
        inode_id: InodeId,
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct OptionalField {
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::public_inode_id::option"
        )]
        inode_id: Option<InodeId>,
    }

    #[test]
    fn serde_adapter_uses_only_the_string_form() {
        let value = RequiredField {
            inode_id: InodeId(42),
        };

        assert_eq!(
            serde_json::to_string(&value).expect("serialize public inode id"),
            r#"{"inode_id":"ino_42"}"#
        );
        assert_eq!(
            serde_json::from_str::<RequiredField>(r#"{"inode_id":"ino_42"}"#)
                .expect("deserialize public inode id"),
            value
        );
        assert!(serde_json::from_str::<RequiredField>(r#"{"inode_id":42}"#).is_err());
    }

    #[test]
    fn optional_serde_adapter_composes_with_skip_serializing_if() {
        let present = OptionalField {
            inode_id: Some(InodeId(42)),
        };
        let absent = OptionalField { inode_id: None };

        assert_eq!(
            serde_json::to_string(&present).expect("serialize present public inode id"),
            r#"{"inode_id":"ino_42"}"#
        );
        assert_eq!(
            serde_json::to_string(&absent).expect("serialize absent public inode id"),
            "{}"
        );
        assert_eq!(
            serde_json::from_str::<OptionalField>(r#"{"inode_id":"ino_42"}"#)
                .expect("deserialize present public inode id"),
            present
        );
        assert_eq!(
            serde_json::from_str::<OptionalField>(r#"{"inode_id":null}"#)
                .expect("deserialize null public inode id"),
            absent
        );
        assert_eq!(
            serde_json::from_str::<OptionalField>("{}").expect("deserialize missing inode id"),
            absent
        );
        assert!(serde_json::from_str::<OptionalField>(r#"{"inode_id":42}"#).is_err());
    }

    #[cfg(feature = "openapi")]
    #[test]
    fn openapi_schema_uses_shared_metadata() {
        let schema = serde_json::to_value(schema()).expect("serialize public inode-id schema");

        assert_eq!(schema["type"], "string");
        assert_eq!(schema["pattern"], PATTERN);
        assert_eq!(schema["example"], EXAMPLE);
        assert_eq!(schema["description"], DESCRIPTION);
    }
}
